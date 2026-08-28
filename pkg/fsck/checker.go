package fsck

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/scrub"
)

// MetadataStore is the interface required by the checker.
type MetadataStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)
}

// Finding represents a single check result.
type Finding struct {
	Level   string // "error", "warning", "info"
	Message string
	Details map[string]any
}

type Checker struct {
	Store    MetadataStore
	Findings []Finding

	// DeviceSize bounds where an extent and an arena may legitimately live.
	// Zero means the run was not told the device size — offline fsck is often
	// pointed at etcd alone — and both range checks are skipped rather than run
	// against a guessed limit.
	DeviceSize uint64
}

func New(store MetadataStore) *Checker {
	return &Checker{Store: store}
}

func (c *Checker) Run(ctx context.Context) []Finding {
	c.Findings = nil

	// The consistency checks themselves live in pkg/scrub and are shared: this
	// is the offline front end, the scrubber is the online one.  They were
	// implemented twice, with different thresholds and severities, which is
	// exactly how an offline check and an online check come to disagree about
	// whether a filesystem is healthy.
	snap, err := scrub.Scan(ctx, c.Store)
	if err != nil {
		c.Findings = append(c.Findings, Finding{
			Level:   "error",
			Message: fmt.Sprintf("cannot read the filesystem: %v", err),
		})
		return c.Findings
	}

	c.checkInodesDecodable(ctx)
	c.checkDirentsReferenced(ctx, snap)
	c.report("warning", scrub.CheckUnreferencedInodes(snap))
	c.report("warning", scrub.CheckNlinkConsistency(snap))
	c.report("warning", scrub.CheckOrphanExtents(snap))
	c.report("warning", scrub.CheckDeadExtents(snap))
	c.report("error", scrub.CheckExtentCollisions(snap))
	c.report("error", scrub.CheckRangeValidity(snap, c.DeviceSize))
	c.report("error", scrub.CheckGenerationConsistency(snap))
	c.checkArenaBoundaries(ctx)
	c.checkArenaOrphans(ctx)

	return c.Findings
}

// report records a scrub check's findings at the given severity.
func (c *Checker) report(level string, results []scrub.Result) {
	for _, r := range results {
		c.Findings = append(c.Findings, Finding{Level: level, Message: r.Detail})
	}
}

func (c *Checker) ErrorCount() int {
	n := 0
	for _, f := range c.Findings {
		if f.Level == "error" {
			n++
		}
	}
	return n
}

func (c *Checker) WarningCount() int {
	n := 0
	for _, f := range c.Findings {
		if f.Level == "warning" {
			n++
		}
	}
	return n
}

// ---- checks that are fsck's own ----

func (c *Checker) checkInodesDecodable(ctx context.Context) {
	kvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixInode)
	for _, kv := range kvs {
		if len(kv.Value) < 72 {
			c.Findings = append(c.Findings, Finding{
				Level:   "error",
				Message: fmt.Sprintf("corrupt inode: %s (len=%d)", string(kv.Key), len(kv.Value)),
			})
		}
	}
}

// checkDirentsReferenced reports names whose inode is missing — the mirror of
// the scrubber's unreferenced-inode check, and the one condition that makes a
// path resolve to nothing.
func (c *Checker) checkDirentsReferenced(ctx context.Context, snap *scrub.Snapshot) {
	kvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixDirent)
	for _, kv := range kvs {
		ino := metadata.DecodeUint64(kv.Value)
		if ino == 0 {
			continue
		}
		if _, alive := snap.Inodes[ino]; !alive {
			c.Findings = append(c.Findings, Finding{
				Level:   "error",
				Message: fmt.Sprintf("dirent %s points to missing inode %d", string(kv.Key), ino),
			})
		}
	}
}

func (c *Checker) checkArenaBoundaries(ctx context.Context) {
	kvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixArena)
	if len(kvs) == 0 {
		c.Findings = append(c.Findings, Finding{
			Level:   "info",
			Message: "no arena keys found",
		})
	}
	for _, kv := range kvs {
		node, id, ok := metadata.ParseArenaKey(string(kv.Key))
		if !ok {
			c.Findings = append(c.Findings, Finding{
				Level:   "error",
				Message: fmt.Sprintf("malformed arena ownership key %q", string(kv.Key)),
			})
			continue
		}
		if c.DeviceSize > 0 && (id+1)*arenaSizeBytes > c.DeviceSize {
			c.Findings = append(c.Findings, Finding{
				Level: "error",
				Message: fmt.Sprintf("node %s holds arena %d, which ends past the %d byte device",
					node, id, c.DeviceSize),
			})
		}
	}
}

// checkArenaOrphans reports arena space no node can ever reissue.
//
// Every arena ID the allocator has handed out is below the arena_alloc_log
// high-water mark, and must be either owned by a node or sitting in the free
// pool.  An ID in neither is lost space: nothing will re-adopt it on restart
// and nothing will claim it from the pool.  An ID in both is worse — the free
// pool would hand a second node into a range someone still owns.
//
// Reported, not repaired: deciding an arena is truly unowned needs the
// operator's knowledge of which nodes are permanently gone.
func (c *Checker) checkArenaOrphans(ctx context.Context) {
	// The counter holds the next unissued ID, so every arena ever handed out
	// is below it.
	counter, _ := c.Store.Get(ctx, metadata.PrefixArenaLog)
	highWater := metadata.DecodeUint64(counter)

	owners := make(map[uint64]string)
	kvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixArena)
	for _, kv := range kvs {
		if node, id, ok := metadata.ParseArenaKey(string(kv.Key)); ok {
			owners[id] = node
		}
	}

	free := make(map[uint64]bool)
	freeKvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixFreeArena)
	for _, kv := range freeKvs {
		id, err := strconv.ParseUint(strings.TrimPrefix(string(kv.Key), metadata.PrefixFreeArena), 10, 64)
		if err == nil {
			free[id] = true
		}
	}

	for id := uint64(0); id < highWater; id++ {
		node, owned := owners[id]
		switch {
		case owned && free[id]:
			c.Findings = append(c.Findings, Finding{
				Level: "error",
				Message: fmt.Sprintf("arena %d is in the free pool while %s still owns it",
					id, node),
			})
		case !owned && !free[id]:
			c.Findings = append(c.Findings, Finding{
				Level: "warning",
				Message: fmt.Sprintf("arena %d is orphaned: owned by no node and not in the free pool",
					id),
			})
		}
	}
}

// ---- helpers ----

// arenaSizeBytes mirrors arena.ArenaSizeBytes.  It is duplicated rather than
// imported because pkg/arena depends on the metadata store, and fsck is meant
// to run against etcd alone.
const arenaSizeBytes = 1 << 30
