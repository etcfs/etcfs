//go:build integration
// +build integration

// Integration tests for the scrubber's orphan-extent block reclamation.
//
// Requires a running etcd. Start one with:
//
//	docker run -d -p 2379:2379 quay.io/coreos/etcd:v3.5.18 \
//	  /usr/local/bin/etcd --data-dir=/etcd-data \
//	  --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://0.0.0.0:2379
//
// Run with:
//
//	ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration -count=1 ./...
package scrub

import (
	"context"
	"testing"
	"time"

	"github.com/etcfs/etcfs/pkg/arena"
	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/test/etcdtest"
)

type testLogger struct{}

func (testLogger) Warn(string, ...any)  {}
func (testLogger) Info(string, ...any)  {}
func (testLogger) Error(string, ...any) {}

func testStore(t *testing.T, nodeID string) *metadata.Store {
	t.Helper()
	return metadata.NewStore(etcdtest.Client(t), nodeID)
}

// allocOne reserves a single block and returns its device offset.  Allocate
// answers with runs because a request may be spread over several; a one-block
// request never is.
func allocOne(t *testing.T, a *arena.Allocator, what string) uint64 {
	t.Helper()
	runs, err := a.Allocate(arena.BlockSize)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if len(runs) != 1 {
		t.Fatalf("%s: one block came back as %d runs", what, len(runs))
	}
	return runs[0].DiskOff
}

// Deleting an unlinked file's dangling extent key must also return its
// blocks to the allocator — otherwise disk space leaks on every deletion.
func TestIntegration_OrphanReclaimReturnsBlocksToAllocator(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	alloc := arena.NewAllocator("node-A", store)

	if _, err := alloc.AcquireArena(ctx); err != nil {
		t.Fatalf("acquire arena: %v", err)
	}
	off := allocOne(t, alloc, "allocate")
	// No inode record for ino 99 — this is what makes the extent an orphan:
	// AtomicUnlink already removed it, but the extent key survived.
	if err := store.AppendExtent(ctx, 99, 0, off, arena.BlockSize, 1); err != nil {
		t.Fatalf("append orphan extent: %v", err)
	}

	before := alloc.LiveRatio()

	s := New(store, "node-A", time.Hour, testLogger{})
	s.SetReclaimer(alloc)
	s.RunScrubPass(ctx)

	after := alloc.LiveRatio()
	if after >= before {
		t.Fatalf("live ratio did not drop after orphan reclaim: before=%f after=%f", before, after)
	}

	// The freed block must be reachable by a fresh allocation, not just
	// unmarked — Free and Allocate share one bitmap.
	got := allocOne(t, alloc, "allocate after reclaim")
	if got != off {
		t.Fatalf("reclaimed block %d not reissued, got %d instead", off, got)
	}

	kvs, err := store.GetPrefix(ctx, metadata.PrefixExtent)
	if err != nil {
		t.Fatalf("get extents: %v", err)
	}
	if len(kvs) != 0 {
		t.Fatalf("orphan extent key not deleted, %d remain", len(kvs))
	}
}

// A node that owns no arena must leave the orphan alone rather than delete it.
//
// The extent record is the only thing the owning node's in-memory bitmap is
// rebuilt from, so deleting it from here would strand those blocks as allocated
// on that node until it restarted.  Reporting without reclaiming is the correct
// degraded behaviour; the owner's own pass does the cleanup.
func TestIntegration_OrphanInForeignArenaIsReportedNotDeleted(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	owner := arena.NewAllocator("node-A", store)

	if _, err := owner.AcquireArena(ctx); err != nil {
		t.Fatalf("acquire arena: %v", err)
	}
	off := allocOne(t, owner, "allocate")
	if err := store.AppendExtent(ctx, 99, 0, off, arena.BlockSize, 1); err != nil {
		t.Fatalf("append orphan extent: %v", err)
	}

	// node-B holds no arena, so the range belongs to a peer as far as it knows.
	bystander := arena.NewAllocator("node-B", metadata.NewStore(store.Client(), "node-B"))
	s := New(store, "node-B", time.Hour, testLogger{})
	s.SetReclaimer(bystander)
	s.RunScrubPass(ctx)

	kvs, err := store.GetPrefix(ctx, metadata.PrefixExtent)
	if err != nil {
		t.Fatalf("get extents: %v", err)
	}
	if len(kvs) != 1 {
		t.Fatalf("orphan extent in another node's arena was deleted, %d remain", len(kvs))
	}

	anomalies := s.Anomalies()
	if len(anomalies) == 0 {
		t.Fatal("orphan in another node's arena was not reported at all")
	}
}

// An arena emptied by deletion must go back to the global pool, or the space
// stays reserved to this node for as long as the process lives.
func TestIntegration_EmptiedArenaReturnsToFreePool(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	alloc := arena.NewAllocator("node-A", store)

	ar, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("acquire arena: %v", err)
	}
	off := allocOne(t, alloc, "allocate")

	// Still in use: nothing to release yet.
	released, err := alloc.ReleaseEmptyArenas(ctx)
	if err != nil {
		t.Fatalf("release with a live block: %v", err)
	}
	if len(released) != 0 {
		t.Fatalf("arena released while a block was still allocated: %v", released)
	}

	alloc.Free(off, arena.BlockSize)

	released, err = alloc.ReleaseEmptyArenas(ctx)
	if err != nil {
		t.Fatalf("release after free: %v", err)
	}
	if len(released) != 1 || released[0] != ar.ID {
		t.Fatalf("emptied arena %d not released, got %v", ar.ID, released)
	}
	if alloc.ArenaCount() != 0 {
		t.Fatalf("released arena still in the local free list (%d remain)", alloc.ArenaCount())
	}

	owner, err := store.Get(ctx, metadata.ArenaOwnerKey("node-A", ar.ID))
	if err != nil {
		t.Fatalf("read ownership: %v", err)
	}
	if owner != nil {
		t.Fatal("ownership record survived the release")
	}
	free, err := store.Get(ctx, metadata.FreeArenaKey(ar.ID))
	if err != nil {
		t.Fatalf("read free pool: %v", err)
	}
	if free == nil {
		t.Fatalf("arena %d released but never landed in the free pool", ar.ID)
	}
}

// seedInode writes a minimal inode record with the given size.
func seedInode(t *testing.T, store *metadata.Store, ino, size uint64) {
	t.Helper()
	rec := &metadata.InodeRecord{Ino: ino, Size: size, Mode: 0o100644, Nlink: 1}
	if _, err := store.Put(context.Background(), metadata.InodeKey(ino),
		metadata.EncodeInode(rec)); err != nil {
		t.Fatalf("seed inode %d: %v", ino, err)
	}
}

// A truncate issued from another node leaves the extents past EOF in place —
// only the arena's owner may remove them, because the record is what the
// owner's bitmap is rebuilt from.  The owner's scrub pass is what finally
// reclaims the space, and without this check it never would: the inode is
// alive, so the orphan check does not see these extents at all.
func TestIntegration_ExtentsPastEOFAreReclaimedByTheOwner(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	alloc := arena.NewAllocator("node-A", store)

	if _, err := alloc.AcquireArena(ctx); err != nil {
		t.Fatalf("acquire arena: %v", err)
	}
	kept := allocOne(t, alloc, "allocate kept")
	dropped := allocOne(t, alloc, "allocate dropped")

	// A two-block file, then truncated to one block by some other node: the
	// size says 4096, but the second extent is still recorded.
	seedInode(t, store, 77, arena.BlockSize)
	if err := store.AppendExtent(ctx, 77, 0, kept, arena.BlockSize, 1); err != nil {
		t.Fatalf("append kept extent: %v", err)
	}
	if err := store.AppendExtent(ctx, 77, arena.BlockSize, dropped, arena.BlockSize, 1); err != nil {
		t.Fatalf("append past-EOF extent: %v", err)
	}

	s := New(store, "node-A", time.Hour, testLogger{})
	s.SetReclaimer(alloc)
	s.RunScrubPass(ctx)

	kvs, err := store.GetPrefix(ctx, metadata.ExtentPrefix(77))
	if err != nil {
		t.Fatalf("get extents: %v", err)
	}
	if len(kvs) != 1 {
		t.Fatalf("want only the live extent left, got %d", len(kvs))
	}

	// The reclaimed block must be handed out again, not merely unreferenced.
	if got := allocOne(t, alloc, "allocate after reclaim"); got != dropped {
		t.Fatalf("block %d past EOF was not returned to the free list, got %d", dropped, got)
	}
}

// The same for an overwrite: a later extent fully covering an earlier one makes
// the earlier one's blocks dead while the file itself lives on.
func TestIntegration_SupersededExtentsAreReclaimedByTheOwner(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	alloc := arena.NewAllocator("node-A", store)

	if _, err := alloc.AcquireArena(ctx); err != nil {
		t.Fatalf("acquire arena: %v", err)
	}
	first := allocOne(t, alloc, "allocate first write")
	second := allocOne(t, alloc, "allocate overwrite")

	seedInode(t, store, 88, arena.BlockSize)
	if err := store.AppendExtent(ctx, 88, 0, first, arena.BlockSize, 1); err != nil {
		t.Fatalf("append first extent: %v", err)
	}
	if err := store.AppendExtent(ctx, 88, 0, second, arena.BlockSize, 1); err != nil {
		t.Fatalf("append overwriting extent: %v", err)
	}

	// Before the scrub, a read must already resolve to the newer write.
	extents, err := store.GetExtents(ctx, 88)
	if err != nil {
		t.Fatalf("get extents: %v", err)
	}
	if extents[0].DiskOff != second {
		t.Fatalf("read would resolve to disk_off %d, want the newer %d",
			extents[0].DiskOff, second)
	}

	s := New(store, "node-A", time.Hour, testLogger{})
	s.SetReclaimer(alloc)
	s.RunScrubPass(ctx)

	kvs, err := store.GetPrefix(ctx, metadata.ExtentPrefix(88))
	if err != nil {
		t.Fatalf("get extents: %v", err)
	}
	if len(kvs) != 1 {
		t.Fatalf("want only the surviving extent, got %d", len(kvs))
	}
	if got := allocOne(t, alloc, "allocate after reclaim"); got != first {
		t.Fatalf("overwritten block %d was not returned to the free list, got %d", first, got)
	}
}

// A middle split writes two records and must not resurrect the buried middle.
// Both halves carry the parent's sequence, so a later write covering either of
// them still outranks it — which is what makes the split safe to do at all.
func TestIntegration_MiddleSplitHalvesKeepTheParentSequence(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")

	const bs = arena.BlockSize
	parent := metadata.Extent{
		Key: metadata.ExtentKey(55, 0), Chunk: 0, Seq: 0,
		LogOff: 0, DiskOff: 4 << 30, Length: 4 * bs, Gen: 1,
	}
	head, tail := metadata.SplitAround(parent, bs, 2*bs)
	if head == nil || tail == nil {
		t.Fatalf("middle split produced head=%v tail=%v", head, tail)
	}

	// Store them as the write path would: head under the parent's key, tail
	// under a fresh one, and the overwriting extent under another.
	writer := metadata.Extent{LogOff: bs, DiskOff: 5 << 30, Length: bs, Gen: 1, Seq: 1}
	for key, ext := range map[string]metadata.Extent{
		parent.Key:                *head,
		metadata.ExtentKey(55, 9): *tail,
		metadata.ExtentKey(55, 1): writer,
	} {
		if _, err := store.Put(ctx, key, []byte(ext.Encode())); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	got, err := store.GetExtents(ctx, 55)
	if err != nil {
		t.Fatalf("get extents: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 extents, got %d", len(got))
	}

	// Reading each region must land on the right extent: the head and tail for
	// the bytes they still own, and the overwriting write for the middle.
	for _, c := range []struct {
		at      uint64
		diskOff uint64
		what    string
	}{
		{0, head.DiskOff, "head"},
		{bs, writer.DiskOff, "overwritten middle"},
		{2 * bs, tail.DiskOff, "tail"},
	} {
		var resolved *metadata.Extent
		for i := range got {
			if got[i].LogOff <= c.at && c.at < got[i].End() {
				resolved = &got[i]
				break
			}
		}
		if resolved == nil {
			t.Errorf("%s: offset %d covered by no extent", c.what, c.at)
			continue
		}
		if resolved.DiskOff != c.diskOff {
			t.Errorf("%s: offset %d resolves to disk_off %d, want %d",
				c.what, c.at, resolved.DiskOff, c.diskOff)
		}
	}

	// The tail's fresh key must not have made it look newer than the write.
	if tail.Seq != parent.Seq {
		t.Errorf("tail sequence %d, want the parent's %d", tail.Seq, parent.Seq)
	}
}
