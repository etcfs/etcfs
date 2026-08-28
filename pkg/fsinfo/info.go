package fsinfo

import (
	"context"
	"fmt"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/etcfs/etcfs/pkg/arena"
	"github.com/etcfs/etcfs/pkg/metadata"
)

type MetadataStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)
}

type Info struct {
	TotalInodes      uint64
	TotalFiles       uint64
	TotalDirs        uint64
	TotalSize        uint64
	TotalExtents     uint64
	TotalLocks       uint64
	TotalDirents     uint64
	ArenaCount       uint64
	MemberCount      uint64
	ArenaUtilization map[string]float64
}

func Collect(ctx context.Context, store MetadataStore) (*Info, error) {
	info := &Info{
		ArenaUtilization: make(map[string]float64),
	}

	inodeKvs, _ := store.GetPrefix(ctx, "inode:")
	info.TotalInodes = uint64(len(inodeKvs))

	for _, kv := range inodeKvs {
		rec := metadata.DecodeInode(kv.Value)
		if rec != nil {
			if rec.Mode&metadata.ModeDir != 0 {
				info.TotalDirs++
			} else {
				info.TotalFiles++
			}
			info.TotalSize += rec.Size
		}
	}

	extKvs, _ := store.GetPrefix(ctx, metadata.PrefixExtent)
	info.TotalExtents = uint64(len(extKvs))

	direntKvs, _ := store.GetPrefix(ctx, "dirent:")
	info.TotalDirents = uint64(len(direntKvs))

	lockKvs, _ := store.GetPrefix(ctx, "lock:")
	info.TotalLocks = uint64(len(lockKvs))

	arenaKvs, _ := store.GetPrefix(ctx, metadata.PrefixArena)
	info.ArenaCount = uint64(len(arenaKvs))
	info.ArenaUtilization = arenaUtilization(arenaKvs, metadata.DecodeExtents(extKvs))

	memKvs, _ := store.GetPrefix(ctx, "membership:")
	info.MemberCount = uint64(len(memKvs))

	return info, nil
}

// arenaUtilization reports, per node, the fraction of the space in the arenas
// it owns that live extents occupy.
//
// Which arena an extent belongs to follows from its offset alone: arenas are
// fixed-size and laid out back to back from offset 0, the same mapping
// pkg/arena uses to derive DiskStart from an arena ID.
func arenaUtilization(arenaKvs []*mvccpb.KeyValue, extents []metadata.Extent) map[string]float64 {
	usedBlocks := make(map[uint64]uint64)
	for _, ext := range extents {
		id := ext.DiskOff / arena.ArenaSizeBytes
		usedBlocks[id] += (ext.Length + arena.BlockSize - 1) / arena.BlockSize
	}

	used := make(map[string]uint64)
	owned := make(map[string]uint64)
	for _, kv := range arenaKvs {
		node, id, ok := metadata.ParseArenaKey(string(kv.Key))
		if !ok {
			continue
		}
		owned[node]++
		used[node] += usedBlocks[id]
	}

	util := make(map[string]float64, len(owned))
	for node, count := range owned {
		util[node] = float64(used[node]) / float64(count*arena.BlocksPerArena)
	}
	return util
}

func (i *Info) String() string {
	return fmt.Sprintf(
		"inodes=%d files=%d dirs=%d size=%d extents=%d dirents=%d locks=%d arenas=%d members=%d",
		i.TotalInodes, i.TotalFiles, i.TotalDirs, i.TotalSize,
		i.TotalExtents, i.TotalDirents, i.TotalLocks,
		i.ArenaCount, i.MemberCount,
	)
}
