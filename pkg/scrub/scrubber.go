// Package scrub implements the continuous background verification scrubber.
//
// The scrubber periodically cross-checks etcd metadata against stored state
// to detect:
//  1. Extent collisions — two inodes claiming the same disk offset
//  2. Range violations — extent outside its owning arena's range
//  3. Orphan extents — allocated blocks with no inode reference
//  4. Generation mismatches — extent stamped with wrong fencing generation
//  5. Nlink inconsistencies — inode nlink doesn't match dirent count
//  6. Unreferenced inodes — inode records no directory entry names
//
// Anomalies are logged and emitted as metrics.  The scrubber can reclaim
// safe anomalies (orphans) automatically.  Unsafe anomalies (collisions,
// range violations) are alerted but not auto-remediated.
package scrub

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/metrics"
)

// MetadataStore is the slice of the metadata store the scrubber needs: it
// reads keys and deletes the orphans it is allowed to reclaim, and nothing
// else.  Declaring only that keeps the scrubber testable against a stub and
// makes the blast radius of a scrub bug obvious from its dependencies.
//
// The delete goes through Txn rather than a plain Delete so it can be made
// conditional on the record not having changed since the scan read it.
type MetadataStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)
	Txn(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, error)
}

// Result records a single scrub finding.
type Result struct {
	Type    string // "collision", "orphan", "range", "generation", "nlink"
	Detail  string
	Ino     uint64
	DiskOff uint64
	Length  uint64 // length of the disk range, when the finding refers to one
	Key     string // etcd key the finding refers to, when it is a single key
	ModRev  int64  // revision Key was at when the finding was made, when it refers to one
	AutoFix bool   // true if the scrubber can safely remediate
}

// Scrubber runs continuous verification of filesystem invariants.
type Scrubber struct {
	store     MetadataStore
	log       Logger
	nodeID    string
	interval  time.Duration
	rateLimit float64 // max fraction of foreground I/O bandwidth to use
	reclaimer Reclaimer

	// deviceSize bounds where an extent may legitimately live.  Zero means the
	// scrubber was not told, and the range check is skipped rather than run
	// against a guessed limit.
	deviceSize uint64

	// locks answers whether a reclaim would race a read on this node.  Nil in
	// an offline check, where there is no daemon to race — see InodeLocks.
	locks InodeLocks

	mu           sync.Mutex
	lastRun      time.Time
	anomalies    []Result
	totalChecked int64

	// held are the disk ranges of extents this pass has already deleted but
	// could not free, because the inode was locked here between the delete and
	// the free.  They are retried at the start of every later pass: the record
	// naming them is gone, so nothing but this list can give them back, and
	// nothing else can reach them in the meantime.
	held []heldRange
}

// heldRange is one reclaimed disk range waiting for its inode to go quiet.
type heldRange struct {
	ino     uint64
	diskOff uint64
	length  uint64
}

// Reclaimer returns a disk range to the block allocator, and reports which
// ranges this node is entitled to reclaim.  Implemented by pkg/arena.Allocator.
//
// Owns is what keeps reclamation from leaking across nodes.  The free list is
// per-process and in-memory, so an orphaned extent inside a peer's arena has to
// be left to that peer: deleting the extent record here would remove the only
// reference its bitmap is rebuilt from, and those blocks would stay marked
// allocated there until the peer restarts.  Each node reclaims its own arenas,
// and every arena has exactly one owner, so every orphan is still covered.
type Reclaimer interface {
	Free(diskOff, size uint64)
	Owns(diskOff uint64) bool
}

// InodeLocks reports whether this node still has an inode's lock cached, which
// is the question the reclaim path has to ask before it hands blocks back.
//
// A scrub pass takes no inode lock — one per inode across a whole-keyspace scan
// would be its own scalability problem — so it can run alongside a read on this
// node that has already resolved an extent and not yet issued its device read.
// Deleting that extent's record is harmless to such a reader; freeing its blocks
// is not, because the allocator may hand them to another file before the read
// lands. The gap is real without a further conspiracy: the reader's own lock
// key can have been deleted by a lease expiry this node has not observed, a
// peer can have taken the inode and buried that extent, and the pass then sees
// it as dead through no fault of its own.
//
// The scrubber's revision-conditional delete does not close this. That
// comparison exists so two passes cannot free the same extent twice; it says
// nothing about a reader that resolved the extent before the delete.
//
// So the answer is a local map lookup and not a round trip: an entry in the
// lock cache means an operation on this node may still be holding something it
// resolved, and the blocks wait.
type InodeLocks interface {
	// Holds reports whether anything is cached for this inode's lock.
	Holds(ino uint64) bool
}

// Logger is the logging interface used by the scrubber.
type Logger interface {
	Warn(msg string, args ...any)
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// New creates a Scrubber.
func New(store MetadataStore, nodeID string, interval time.Duration, log Logger) *Scrubber {
	return &Scrubber{
		store:     store,
		log:       log,
		nodeID:    nodeID,
		interval:  interval,
		rateLimit: 0.1, // default: 10% of I/O bandwidth
	}
}

// SetDeviceSize tells the scrubber how large the shared device is, so its
// range check uses the same number the allocator does rather than a hardcoded
// copy of it.
func (s *Scrubber) SetDeviceSize(bytes uint64) {
	s.deviceSize = bytes
}

// SetReclaimer attaches the block allocator whose space the scrubber may
// return on an auto-fix.  Without one the scrubber still deletes orphaned
// extent records, but their blocks stay allocated.
func (s *Scrubber) SetReclaimer(r Reclaimer) {
	s.reclaimer = r
}

// SetInodeLocks attaches this node's lock cache, so a reclaim can tell whether
// an operation here may still be reading the extent it is about to free.
// See InodeLocks.
func (s *Scrubber) SetInodeLocks(l InodeLocks) {
	s.locks = l
}

// lockedHere reports whether this node has the inode's lock cached.  Without a
// lock cache attached there is no daemon serving reads, and so nothing to race.
func (s *Scrubber) lockedHere(ino uint64) bool {
	return s.locks != nil && s.locks.Holds(ino)
}

// Run starts the scrub loop.  Blocks until ctx is cancelled.
func (s *Scrubber) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.log.Info("scrubber started", "interval", s.interval)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("scrubber stopped")
			return
		case <-ticker.C:
			s.RunScrubPass(ctx)
		}
	}
}

// runScrubPass executes a single full scrub cycle.
func (s *Scrubber) RunScrubPass(ctx context.Context) {
	s.mu.Lock()
	s.lastRun = time.Now()
	s.mu.Unlock()

	s.log.Info("scrub pass starting")

	// Ranges an earlier pass deleted but could not free get another chance
	// before anything new is scanned.
	s.releaseHeldRanges()

	snap, err := s.Scan(ctx)
	if err != nil {
		s.log.Error("scrub pass aborted", "error", err)
		return
	}

	collisions := s.CheckExtentCollisions(snap)
	orphans := s.CheckOrphanExtents(snap)
	dead := s.CheckDeadExtents(snap)
	rangeV := s.CheckRangeValidity(snap)
	genM := s.CheckGenerationConsistency(snap)
	nlinkV := s.CheckNlinkConsistency(snap)
	unref := s.CheckUnreferencedInodes(snap)

	s.record(collisions, orphans, dead, rangeV, genM, nlinkV, unref)

	// Reclaim only what this node owns.  An orphan in a peer's arena is that
	// peer's to clean up, and one in a free-pool arena is cleaned up by whoever
	// claims it next — the claimer marks it live from the extent record that is
	// still there, then its own scrub pass finds it orphaned and reclaims it.
	// So every orphan is still covered, without a node ever deleting the record
	// another node's bitmap is rebuilt from.
	reclaimable := make([]Result, 0, len(orphans)+len(dead))
	reclaimable = append(reclaimable, orphans...)
	reclaimable = append(reclaimable, dead...)

	for _, r := range reclaimable {
		if !r.AutoFix {
			continue
		}
		if s.reclaimer == nil || !s.reclaimer.Owns(r.DiskOff) {
			continue
		}
		// An operation on this node may still be holding this extent as it
		// resolved it, and freeing the blocks under it is what would let a
		// later allocation overwrite what that read is about to fetch.  The
		// finding is simply re-made next pass, by which time the inode is
		// usually quiet.  See InodeLocks.
		if s.lockedHere(r.Ino) {
			s.log.Info("scrub auto-fix: inode is locked here, extent left for a later pass",
				"ino", r.Ino, "key", r.Key)
			continue
		}
		s.log.Info("scrub auto-fix: reclaiming extent", "type", r.Type, "detail", r.Detail)
		// Delete first: the blocks must stop being reachable through metadata
		// before they can be handed to another allocation, or a reader
		// resolving the extent could land on data already overwritten.
		//
		// Conditional on the record still being at the revision the scan read
		// it at, because the pass runs against a snapshot taken some time ago
		// and takes no inode lock.  An unconditional delete succeeds even on a
		// key that is already gone, so a truncate that reclaimed this extent in
		// between — freeing its blocks, which the allocator may since have
		// handed to another file — would be followed by a second free of the
		// same range from here, and two files would own the same blocks.  A
		// finding that loses the comparison is simply re-found next pass.
		ok, err := s.store.Txn(ctx,
			[]clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(r.Key), "=", r.ModRev)},
			[]clientv3.Op{clientv3.OpDelete(r.Key)}, nil)
		if err != nil {
			s.log.Error("scrub auto-fix: extent not deleted, blocks not reclaimed",
				"key", r.Key, "error", err)
			continue
		}
		if !ok {
			s.log.Info("scrub auto-fix: extent changed since the scan, left alone", "key", r.Key)
			continue
		}
		if r.Length > 0 {
			s.freeOrHold(r.Ino, r.DiskOff, r.Length)
		}
	}

	metrics.ScrubPasses.Inc()
	metrics.ScrubLastRun.SetToCurrentTime()
	for typ, found := range map[string]int{
		"collision": len(collisions), "orphan": len(orphans), "dead": len(dead),
		"range": len(rangeV), "generation": len(genM), "nlink": len(nlinkV),
		"unreferenced": len(unref),
	} {
		metrics.ScrubAnomalies.WithLabelValues(typ).Add(float64(found))
	}

	total := len(collisions) + len(orphans) + len(dead) + len(rangeV) + len(genM) + len(nlinkV) + len(unref)
	if total > 0 {
		s.log.Warn("scrub found anomalies", "count", total, "collisions", len(collisions),
			"orphans", len(orphans), "dead", len(dead), "range", len(rangeV),
			"generation", len(genM), "nlink", len(nlinkV), "unreferenced", len(unref))
	} else {
		s.log.Info("scrub pass clean")
	}
}

// record folds a pass's findings into the anomaly list.
//
// The list is deduplicated by type and key and capped: a permanent anomaly is
// re-found by every pass, so an append-only list grew without bound for as long
// as the daemon ran — one finding every 30 seconds, forever, for a condition an
// operator may well have decided to live with.  Keeping the newest is the right
// end to keep: the oldest entries describe passes whose findings have since
// been re-reported anyway.
func (s *Scrubber) record(passes ...[]Result) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[string]bool, len(s.anomalies))
	for _, r := range s.anomalies {
		seen[r.Type+"\x00"+r.Key] = true
	}
	for _, pass := range passes {
		for _, r := range pass {
			id := r.Type + "\x00" + r.Key
			if seen[id] {
				continue
			}
			seen[id] = true
			s.anomalies = append(s.anomalies, r)
		}
	}
	if excess := len(s.anomalies) - maxAnomalies; excess > 0 {
		s.anomalies = append(s.anomalies[:0], s.anomalies[excess:]...)
	}
	s.totalChecked++
}

// maxAnomalies bounds the retained findings.  Large enough that a real burst is
// still legible in full, small enough that a permanently unfixable anomaly
// cannot consume the process.
const maxAnomalies = 1000

// ---- the pass snapshot ----

// Snapshot is everything one scrub pass reads, gathered once.
//
// Each check used to scan the key space it needed for itself, so a pass read
// the whole extent space five times and the inode space twice, and the orphan
// check additionally issued one Get per extent to ask whether its inode
// existed — a question the inode scan already answers.
type Snapshot struct {
	Extents []metadata.Extent
	// Inodes holds every inode record, by inode number.
	Inodes map[uint64]*metadata.InodeRecord
	// DirentRefs counts the directory entries pointing at each inode.
	DirentRefs map[uint64]uint32
	// SubdirRefs counts the subdirectories each directory holds, which is what
	// its link count is made of beyond its own ".".
	SubdirRefs map[uint64]uint32
	// Generations is each node's current fencing generation.
	Generations map[string]uint64
}

// Reader is the read side of the store: all a snapshot and every check needs.
// fsck runs the same checks over the same snapshot through it.
type Reader interface {
	GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)
}

// Scan reads the whole key space once, for every check in a pass to share.
func (s *Scrubber) Scan(ctx context.Context) (*Snapshot, error) {
	return Scan(ctx, s.store)
}

// Scan reads the whole key space once.
func Scan(ctx context.Context, store Reader) (*Snapshot, error) {
	extKvs, err := store.GetPrefix(ctx, metadata.PrefixExtent)
	if err != nil {
		return nil, fmt.Errorf("scan extents: %w", err)
	}
	inodeKvs, err := store.GetPrefix(ctx, metadata.PrefixInode)
	if err != nil {
		return nil, fmt.Errorf("scan inodes: %w", err)
	}
	direntKvs, err := store.GetPrefix(ctx, metadata.PrefixDirent)
	if err != nil {
		return nil, fmt.Errorf("scan dirents: %w", err)
	}
	genKvs, err := store.GetPrefix(ctx, metadata.PrefixGen)
	if err != nil {
		return nil, fmt.Errorf("scan generations: %w", err)
	}

	snap := &Snapshot{
		Extents:     metadata.DecodeExtents(extKvs),
		Inodes:      make(map[uint64]*metadata.InodeRecord, len(inodeKvs)),
		DirentRefs:  make(map[uint64]uint32, len(direntKvs)),
		SubdirRefs:  make(map[uint64]uint32, len(direntKvs)),
		Generations: make(map[string]uint64, len(genKvs)),
	}
	for _, kv := range inodeKvs {
		if rec := metadata.DecodeInode(kv.Value); rec != nil {
			snap.Inodes[rec.Ino] = rec
		}
	}
	// Second pass over the dirents, after the inodes: whether an entry names a
	// directory is a property of the record it points at.
	for _, kv := range direntKvs {
		child := metadata.DecodeUint64(kv.Value)
		snap.DirentRefs[child]++
		parent, _, ok := metadata.ParseDirentKey(string(kv.Key))
		if !ok {
			continue
		}
		if rec := snap.Inodes[child]; rec != nil && rec.Mode&metadata.S_IFMT == metadata.ModeDir {
			snap.SubdirRefs[parent]++
		}
	}
	for _, kv := range genKvs {
		n := uint64(0)
		_, _ = fmt.Sscanf(string(kv.Value), "%d", &n)
		snap.Generations[string(kv.Key[len(metadata.PrefixGen):])] = n
	}
	return snap, nil
}

// ---- scrub checks ----

// CheckExtentCollisions detects two extents whose device ranges overlap.
//
// Overlap, not an identical starting offset: two extents sharing a disk_off is
// only the most obvious case, and comparing offsets for equality missed every
// partial overlap — which is the same corruption, one byte over.
func (s *Scrubber) CheckExtentCollisions(snap *Snapshot) []Result { return CheckExtentCollisions(snap) }

func CheckExtentCollisions(snap *Snapshot) []Result {
	byStart := append([]metadata.Extent(nil), snap.Extents...)
	sort.Slice(byStart, func(i, j int) bool { return byStart[i].DiskOff < byStart[j].DiskOff })

	results := make([]Result, 0, len(byStart))
	for i := 1; i < len(byStart); i++ {
		prev, cur := byStart[i-1], byStart[i]
		// Sorted by start, so only the immediately preceding extent can be the
		// one that reaches into this one — except where several overlap, and
		// there each adjacent pair is reported in turn.
		if prev.DiskOff+prev.Length <= cur.DiskOff {
			continue
		}
		if prev.Key == cur.Key {
			continue
		}
		results = append(results, Result{
			Type: "collision",
			Detail: fmt.Sprintf("extent %s (ino %d) at %d+%d overlaps %s (ino %d) at %d+%d",
				cur.Key, cur.Ino(), cur.DiskOff, cur.Length,
				prev.Key, prev.Ino(), prev.DiskOff, prev.Length),
			Ino:     cur.Ino(),
			DiskOff: cur.DiskOff,
			Key:     cur.Key,
		})
	}
	return results
}

// CheckOrphanExtents detects allocated extents with no inode reference.
func (s *Scrubber) CheckOrphanExtents(snap *Snapshot) []Result { return CheckOrphanExtents(snap) }

func CheckOrphanExtents(snap *Snapshot) []Result {
	results := make([]Result, 0, len(snap.Extents))
	for _, ext := range snap.Extents {
		if _, alive := snap.Inodes[ext.Ino()]; alive {
			continue
		}
		// The disk range is carried along, not just the key: deleting the
		// extent record makes the metadata clean but leaves the blocks behind
		// it marked allocated forever.  Reclaiming them is what makes deletion
		// actually return space.
		results = append(results, Result{
			Type:    "orphan",
			Detail:  fmt.Sprintf("extent %s has no inode reference", ext.Key),
			Ino:     ext.Ino(),
			DiskOff: ext.DiskOff,
			Length:  ext.Length,
			Key:     ext.Key,
			ModRev:  ext.ModRevision,
			AutoFix: true,
		})
	}
	return results
}

// CheckDeadExtents detects extents of a *live* inode that nothing can read any
// more: those left entirely past the file's size by a truncate, and those a
// later write has fully covered.
//
// The orphan check cannot see either of them — it looks for extents whose inode
// is gone, and here the inode is very much alive.  Without this the blocks
// behind them stay allocated for the lifetime of the file.
//
// Both cases arise on the node that performed the operation as well, but that
// node reclaims what it owns inline (see ipc.Service.reclaimCovered).  What
// reaches here is the cross-node remainder: a truncate or overwrite issued from
// one node against bytes sitting in another node's arena, which only the owner
// may reclaim.
func (s *Scrubber) CheckDeadExtents(snap *Snapshot) []Result { return CheckDeadExtents(snap) }

func CheckDeadExtents(snap *Snapshot) []Result {
	byIno := make(map[uint64][]metadata.Extent)
	for _, ext := range snap.Extents {
		byIno[ext.Ino()] = append(byIno[ext.Ino()], ext)
	}

	results := make([]Result, 0, len(snap.Extents))
	for ino, extents := range byIno {
		rec, alive := snap.Inodes[ino]
		if !alive {
			continue // no inode: the orphan check owns this one
		}
		for _, ext := range extents {
			reason := deadReason(ext, rec.Size, extents)
			if reason == "" {
				continue
			}
			results = append(results, Result{
				Type:    "dead",
				Detail:  fmt.Sprintf("extent %s is unreachable: %s", ext.Key, reason),
				Ino:     ino,
				DiskOff: ext.DiskOff,
				Length:  ext.Length,
				Key:     ext.Key,
				ModRev:  ext.ModRevision,
				AutoFix: true,
			})
		}
	}
	return results
}

// deadReason explains why ext can no longer be read, or "" if it still can.
//
// An extent only partly past the end of the file is left alone: trimming it is
// a rewrite rather than a delete, and the surviving head is still live data.
// Its tail is reclaimed when the file is deleted or fully overwritten.
func deadReason(ext metadata.Extent, size uint64, siblings []metadata.Extent) string {
	if ext.LogOff >= size {
		return fmt.Sprintf("it starts at %d, past the file size of %d", ext.LogOff, size)
	}
	for _, other := range siblings {
		if other.Supersedes(ext) {
			return fmt.Sprintf("extent %s overwrote it", other.Key)
		}
	}
	return ""
}

// CheckRangeValidity detects extents that do not fit on the device.
//
// Skipped entirely when the device size is unknown: the previous version
// compared against a hardcoded 1 TiB, which was neither the device's size nor
// the limit fsck used.
func (s *Scrubber) CheckRangeValidity(snap *Snapshot) []Result {
	return CheckRangeValidity(snap, s.deviceSize)
}

// CheckRangeValidity reports extents that do not fit on a device of the given
// size.  A zero size means the caller does not know it, and the check is
// skipped rather than run against a guessed limit.
func CheckRangeValidity(snap *Snapshot, deviceSize uint64) []Result {
	if deviceSize == 0 {
		return nil
	}

	results := make([]Result, 0, len(snap.Extents))
	for _, ext := range snap.Extents {
		if ext.DiskOff+ext.Length > deviceSize {
			results = append(results, Result{
				Type: "range",
				Detail: fmt.Sprintf("extent %s disk_off=%d+%d is past the end of the %d byte device",
					ext.Key, ext.DiskOff, ext.Length, deviceSize),
				Ino:     ext.Ino(),
				DiskOff: ext.DiskOff,
				Key:     ext.Key,
			})
		}
	}
	return results
}

// CheckGenerationConsistency detects extents stamped with a generation their
// writer has never reached.
//
// The stamp is the writer's fencing generation at commit time, and every commit
// is guarded by that generation, so an extent can never legitimately carry one
// above its writer's current value.  One that does means the guard let a write
// through it should have rejected, or the record was written outside the daemon.
//
// It deliberately does *not* flag extents stamped below the current generation.
// Those are simply older than the node's last fence, which is what every extent
// written before a fence looks like.  The check used to compare against the
// maximum generation across the whole cluster, so one node ever being fenced
// turned every extent written by every other node into an anomaly.
func (s *Scrubber) CheckGenerationConsistency(snap *Snapshot) []Result {
	return CheckGenerationConsistency(snap)
}

func CheckGenerationConsistency(snap *Snapshot) []Result {
	results := make([]Result, 0, len(snap.Extents))
	for _, ext := range snap.Extents {
		current, known := snap.Generations[ext.Node]
		if !known || ext.Gen <= current {
			continue
		}
		results = append(results, Result{
			Type: "generation",
			Detail: fmt.Sprintf("extent %s stamped gen=%d, but %s has only reached %d",
				ext.Key, ext.Gen, ext.Node, current),
			Ino:     ext.Ino(),
			DiskOff: ext.DiskOff,
			Key:     ext.Key,
		})
	}
	return results
}

// CheckNlinkConsistency verifies every inode's nlink matches what the namespace
// says it should be: its dirent count for a file, and 2 plus its subdirectories
// for a directory.
func (s *Scrubber) CheckNlinkConsistency(snap *Snapshot) []Result { return CheckNlinkConsistency(snap) }

func CheckNlinkConsistency(snap *Snapshot) []Result {
	results := make([]Result, 0, len(snap.Inodes))
	for ino, rec := range snap.Inodes {
		expected := expectedNlink(rec, snap.DirentRefs[ino], snap.SubdirRefs[ino])
		if rec.Nlink != expected {
			results = append(results, Result{
				Type:   "nlink",
				Detail: fmt.Sprintf("ino=%d nlink=%d dirents=%d", ino, rec.Nlink, expected),
				Ino:    ino,
				Key:    metadata.InodeKey(ino),
			})
		}
	}
	return results
}

// CheckUnreferencedInodes detects inode records that no directory entry names.
//
// Nothing can reach such an inode: it does not appear in any listing, and its
// extents are invisible to the orphan check, which looks for extents whose
// inode is *missing* rather than unreachable.  Every creating operation is a
// single transaction, so this should never appear; when it does, it is either a
// leak from an older write path or genuine corruption.
//
// It is reported, never auto-fixed.  Deleting an inode is not reversible, and
// the blocks behind it are reclaimed by the orphan check once it goes — so an
// operator, or fsck, decides.
func (s *Scrubber) CheckUnreferencedInodes(snap *Snapshot) []Result {
	return CheckUnreferencedInodes(snap)
}

func CheckUnreferencedInodes(snap *Snapshot) []Result {
	results := make([]Result, 0, len(snap.Inodes))
	for ino := range snap.Inodes {
		// The root has no dirent by construction — it is the directory every
		// path starts from, so nothing names it.
		if ino == metadata.RootIno || snap.DirentRefs[ino] > 0 {
			continue
		}
		results = append(results, Result{
			Type:   "unreferenced",
			Detail: fmt.Sprintf("inode %d has no directory entry", ino),
			Ino:    ino,
			Key:    metadata.InodeKey(ino),
		})
	}
	return results
}

// expectedNlink is the link count an inode should carry given how many dirents
// point at it.
//
// For a directory the answer is fixed rather than counted. Directories cannot
// be hard-linked, and this filesystem does not model the ".." link each
// subdirectory would contribute to its parent, so every directory keeps the
// count it was created with — counting dirents instead would flag every
// directory in the filesystem.
// expectedNlink is what an inode's link count should read.
//
// A directory is referred to by its own ".", by its entry in its parent, and by
// the ".." of every subdirectory it holds. The first two are the 2 it is
// created with; the third is why the count moves at all. A filesystem written
// before directory counts were maintained reports 2 everywhere, so this check
// is also what finds those, and fsck is what repairs them.
func expectedNlink(rec *metadata.InodeRecord, dirents, subdirs uint32) uint32 {
	if rec.Mode&metadata.S_IFMT == metadata.ModeDir {
		return metadata.InitialNlink(rec.Mode) + subdirs
	}
	return dirents
}

// Stats returns scrub statistics.
func (s *Scrubber) Stats() (passes int64, anomalies int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalChecked, len(s.anomalies)
}

// Anomalies returns all detected anomalies.
func (s *Scrubber) Anomalies() []Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Result{}, s.anomalies...)
}

// LastRun returns the time of the last scrub pass.
func (s *Scrubber) LastRun() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRun
}

// freeOrHold returns a deleted extent's blocks to the allocator, unless the
// inode became locked here while the delete was in flight.
//
// The check above the delete narrows this window; it cannot close it, because a
// read can begin between that check and the transaction committing.  So the
// question is asked again on the far side of the delete, where the answer is
// the one that matters: the record is already gone, so nothing else can reach
// these blocks, and holding them costs a retry rather than a leak.
func (s *Scrubber) freeOrHold(ino, diskOff, length uint64) {
	if !s.lockedHere(ino) {
		s.reclaimer.Free(diskOff, length)
		return
	}
	s.mu.Lock()
	s.held = append(s.held, heldRange{ino: ino, diskOff: diskOff, length: length})
	depth := len(s.held)
	s.mu.Unlock()
	s.log.Info("scrub auto-fix: inode locked here after the delete, blocks held back",
		"ino", ino, "disk_off", diskOff, "waiting", depth)
}

// releaseHeldRanges gives back the blocks of extents an earlier pass deleted
// while their inode was locked here, for every inode that has since gone quiet.
//
// An inode that stays locked keeps its ranges held indefinitely, which is the
// safe direction: the blocks are unreachable — their extent records are gone —
// so nothing can hand them out, and a restart rebuilds the allocator's bitmap
// from the records that remain, which is to say without them.
func (s *Scrubber) releaseHeldRanges() {
	if s.reclaimer == nil {
		return
	}
	s.mu.Lock()
	held := s.held
	s.held = nil
	s.mu.Unlock()

	var still []heldRange
	for _, h := range held {
		if s.lockedHere(h.ino) {
			still = append(still, h)
			continue
		}
		s.reclaimer.Free(h.diskOff, h.length)
	}

	if len(still) == 0 {
		return
	}
	s.mu.Lock()
	s.held = append(still, s.held...)
	depth := len(s.held)
	s.mu.Unlock()
	s.log.Info("scrub auto-fix: blocks still held back by locked inodes", "ranges", depth)
}
