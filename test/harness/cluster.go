package harness

import (
	"context"
	"time"

	"github.com/etcfs/etcfs/pkg/metadata"
)

type Cluster struct {
	Store *MockStore
	Nodes []*Simulator
}

func NewCluster(nodes int) *Cluster {
	store := NewMockStore()
	c := &Cluster{Store: store}
	for i := 0; i < nodes; i++ {
		c.Nodes = append(c.Nodes, NewSimulatorWithStore(int64(9000+i), store))
	}
	return c
}

func (c *Cluster) FreshGetAttr(ctx context.Context, ino uint64) *metadata.InodeRecord {
	val, _ := c.Store.Get(ctx, metadata.InodeKey(ino))
	if val == nil {
		return nil
	}
	return metadata.DecodeInode(val)
}

func (c *Cluster) FreshLookup(ctx context.Context, parent uint64, name string) uint64 {
	val, _ := c.Store.Get(ctx, metadata.DirentKey(parent, name))
	if val == nil {
		return 0
	}
	return metadata.DecodeUint64(val)
}

func (c *Cluster) FreshListDir(ctx context.Context, parent uint64) []string {
	prefix := metadata.DirentPrefix(parent)
	kvs, _ := c.Store.GetPrefix(ctx, prefix)
	names := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		names = append(names, string(kv.Key[len(prefix):]))
	}
	return names
}

// harnessLockKey is a single fixed holder key, standing in for the real lock
// API: these tests only need one contended key, not the shared/exclusive
// bookkeeping metadata.AcquireLock provides.
func harnessLockKey(ino uint64) string {
	return metadata.LockKey(ino, metadata.LockExclusive, "harness")
}

func (c *Cluster) tryAcquireLock(ctx context.Context, ino uint64) bool {
	return c.Store.CASPut(ctx, harnessLockKey(ino), []byte("locked"))
}

func (c *Cluster) releaseLock(ctx context.Context, ino uint64) {
	_ = c.Store.Delete(ctx, harnessLockKey(ino))
}

func (c *Cluster) createDirIfMissing(ctx context.Context, parent uint64, name string, ino uint64) {
	if c.FreshLookup(ctx, parent, name) != 0 {
		return
	}
	rec := &metadata.InodeRecord{
		Ino: ino, Mode: metadata.ModeDir | 0755, Nlink: 1, Blksize: 4096,
		Atime: time.Now(), Mtime: time.Now(), Ctime: time.Now(),
	}
	_, _ = c.Store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
	_, _ = c.Store.Put(ctx, metadata.DirentKey(parent, name), metadata.EncodeUint64(ino))
}

func (c *Cluster) checkAllInvariants() int {
	total := 0
	for _, n := range c.Nodes {
		total += n.checkInvariants()
	}
	return total
}

const arenaBlockSize = 4096
const arenaSizeBytes = 1 << 30

func (c *Cluster) AddNode() {
	seed := int64(9000 + len(c.Nodes))
	sim := NewSimulatorWithStore(seed, c.Store)
	c.Nodes = append(c.Nodes, sim)
}

func (c *Cluster) RemoveNode(idx int) {
	if idx < 0 || idx >= len(c.Nodes) {
		return
	}
	c.Nodes = append(c.Nodes[:idx], c.Nodes[idx+1:]...)
}

func (c *Cluster) PopulateExtents(ctx context.Context, inoStart, fileCount uint64, arenaID uint64) {
	diskStart := arenaID * arenaSizeBytes
	for i := uint64(0); i < fileCount; i++ {
		ino := inoStart + i
		diskOff := diskStart + i*arenaBlockSize
		ext := metadata.Extent{DiskOff: diskOff, Length: arenaBlockSize, Gen: 1}
		_, _ = c.Store.Put(ctx, metadata.ExtentKey(ino, 0), []byte(ext.Encode()))

		rec := &metadata.InodeRecord{
			Ino: ino, Mode: 0100644, Nlink: 1,
			Size: arenaBlockSize, Blksize: arenaBlockSize,
			Atime: time.Now(), Mtime: time.Now(), Ctime: time.Now(),
		}
		_, _ = c.Store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
	}
}

func (c *Cluster) ArenaDiskStart(arenaID uint64) uint64 {
	return arenaID * arenaSizeBytes
}

func (c *Cluster) ArenaDiskEnd(arenaID uint64) uint64 {
	return (arenaID + 1) * arenaSizeBytes
}

func (c *Cluster) CountExtentsInArena(ctx context.Context, arenaID uint64) int {
	diskStart := c.ArenaDiskStart(arenaID)
	diskEnd := c.ArenaDiskEnd(arenaID)
	count := 0
	kvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixExtent)
	for _, ext := range metadata.DecodeExtents(kvs) {
		if ext.WithinDisk(diskStart, diskEnd) {
			count++
		}
	}
	return count
}
