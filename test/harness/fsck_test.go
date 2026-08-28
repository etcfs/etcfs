package harness

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/etcfs/etcfs/pkg/fsck"
	"github.com/etcfs/etcfs/pkg/fsinfo"
	"github.com/etcfs/etcfs/pkg/metadata"
)

// ---- C11.10: fsck offline check ----

func TestFsck_CleanFilesystem(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()

	cluster.createDirIfMissing(ctx, 1, "fsck-root", 60000)

	for i := 0; i < 20; i++ {
		ino := uint64(61000) + uint64(i)
		name := fmt.Sprintf("f-%02d", i)
		rec := &metadata.InodeRecord{
			Ino: ino, Mode: 0100644, Nlink: 1,
			Size: 4096, Blksize: 4096,
		}
		_, _ = cluster.Store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
		_, _ = cluster.Store.Put(ctx, metadata.DirentKey(60000, name), metadata.EncodeUint64(ino))
		extKey := fmt.Sprintf("extent:%d/0", ino)
		diskOff := uint64(i * 4096)
		extVal := fmt.Sprintf("0,%d,%d,1", diskOff, 4096)
		_, _ = cluster.Store.Put(ctx, extKey, []byte(extVal))
	}

	chk := fsck.New(cluster.Store)
	findings := chk.Run(ctx)

	errs := chk.ErrorCount()
	warns := chk.WarningCount()

	t.Logf("fsck: %d errors, %d warnings, %d total findings", errs, warns, len(findings))
	assert.Zero(t, errs, "clean filesystem should have zero errors")
}

func TestFsck_CorruptFilesystem(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()

	// Create a dirent pointing to a missing inode
	cluster.createDirIfMissing(ctx, 1, "broken", 62000)
	_ = cluster.Store.Delete(ctx, metadata.InodeKey(62000))

	chk := fsck.New(cluster.Store)
	findings := chk.Run(ctx)

	// The dir inode at 62000 was deleted but a dirent may point to it
	errs := chk.ErrorCount()
	total := len(findings)
	t.Logf("corrupt fsck: %d errors, %d findings", errs, total)
	assert.NotZero(t, total, "corrupt filesystem should produce findings")
}

func TestFsck_OrphanExtentDetected(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()

	// Create extent with no inode reference
	extKey := "extent:99999/0"
	extVal := "0,8192,4096,1,0,node-A"
	_, _ = cluster.Store.Put(ctx, extKey, []byte(extVal))

	chk := fsck.New(cluster.Store)
	findings := chk.Run(ctx)

	warns := chk.WarningCount()
	t.Logf("orphan fsck: %d warnings, %d findings", warns, len(findings))
	assert.GreaterOrEqual(t, warns, 1, "orphan extent should produce a warning")
}

func TestFsck_NlinkMismatchDetected(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()

	// Create inode with nlink=1 but 2 dirents
	rec := &metadata.InodeRecord{
		Ino: 63000, Mode: 0100644, Nlink: 1, Size: 4096, Blksize: 4096,
	}
	_, _ = cluster.Store.Put(ctx, metadata.InodeKey(63000), metadata.EncodeInode(rec))
	cluster.createDirIfMissing(ctx, 1, "nlink-test", 64000)
	_, _ = cluster.Store.Put(ctx, metadata.DirentKey(64000, "link1"), metadata.EncodeUint64(63000))
	_, _ = cluster.Store.Put(ctx, metadata.DirentKey(64000, "link2"), metadata.EncodeUint64(63000))

	chk := fsck.New(cluster.Store)
	findings := chk.Run(ctx)

	warns := chk.WarningCount()
	t.Logf("nlink fsck: %d warnings, %d findings", warns, len(findings))
	assert.GreaterOrEqual(t, warns, 1, "nlink mismatch should produce a warning")
}

// ---- C11.11: fs-info correctness ----

func TestFsInfo_Correctness(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	// Create known state: 5 files, 2 directories, with known sizes
	cluster.createDirIfMissing(ctx, 1, "inforoot", 65000)

	for i := 0; i < 5; i++ {
		ino := uint64(66000) + uint64(i)
		name := fmt.Sprintf("fi-%d", i)
		rec := &metadata.InodeRecord{
			Ino: ino, Mode: 0100644, Nlink: 1, Size: 4096, Blksize: 4096,
		}
		_, _ = store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
		_, _ = store.Put(ctx, metadata.DirentKey(65000, name), metadata.EncodeUint64(ino))
	}

	// Add a second directory
	recDir := &metadata.InodeRecord{
		Ino: 65100, Mode: metadata.ModeDir | 0755, Nlink: 1, Blksize: 4096,
	}
	_, _ = store.Put(ctx, metadata.InodeKey(65100), metadata.EncodeInode(recDir))
	// Register inode_range to simulate membership-like data
	_, _ = store.Put(ctx, "inode_range:test-node", []byte("0,99999"))

	info, err := fsinfo.Collect(ctx, cluster.Store)
	require.NoError(t, err)

	assert.Equal(t, uint64(7), info.TotalInodes, "5 files + 2 dirs = 7 inodes") // 65000 + 65100 + 5 files + 1 root?
	// Actually: 65000 (dir), 65100 (dir), 5 files, root (ino 1) = 8? Wait root isn't created.
	// We created: 65000 dir, 65100 dir, 5 files = 7 inodes total

	assert.Equal(t, uint64(5), info.TotalFiles)
	assert.GreaterOrEqual(t, info.TotalDirs, uint64(1))
	assert.Greater(t, info.TotalSize, uint64(0))
	assert.Greater(t, info.TotalDirents, uint64(0))

	t.Logf("fs-info: %s", info.String())
}
