//go:build integration
// +build integration

// Integration tests for the arena allocator against a real etcd cluster.
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
package arena

import (
	"context"
	"fmt"
	"testing"

	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/test/etcdtest"
)

// allocOne reserves a single block and returns its device offset.  Allocate
// answers with runs because a request may be spread over several; a one-block
// request never is, so these tests can keep asserting on one offset.
func allocOne(t *testing.T, a *Allocator, what string) uint64 {
	t.Helper()
	runs, err := a.Allocate(BlockSize)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if len(runs) != 1 {
		t.Fatalf("%s: one block came back as %d runs", what, len(runs))
	}
	return runs[0].DiskOff
}

func testStore(t *testing.T, nodeID string) *metadata.Store {
	t.Helper()
	return metadata.NewStore(etcdtest.Client(t), nodeID)
}

// A node must never adopt another node's arena on restart.
//
// This is the allocator half of the Kleppmann stale-write hazard: if
// Reconstruct pulls in arenas owned by other nodes, two live nodes hand out
// the same disk offset and both extent commits succeed, because neither node
// is fenced and the generation guard has nothing to reject.
func TestIntegration_ReconstructDoesNotAdoptForeignArenas(t *testing.T) {
	ctx := context.Background()
	storeA := testStore(t, "node-A")
	storeB := testStore(t, "node-B")

	allocA := NewAllocator("node-A", storeA)
	allocB := NewAllocator("node-B", storeB)

	arenaA, err := allocA.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("node-A acquire: %v", err)
	}
	arenaB, err := allocB.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("node-B acquire: %v", err)
	}
	if arenaA.ID == arenaB.ID {
		t.Fatalf("two nodes were handed the same arena ID %d", arenaA.ID)
	}

	// node-A restarts and rebuilds its free-list from etcd.
	restartedA := NewAllocator("node-A", storeA)
	if err := restartedA.Reconstruct(ctx); err != nil {
		t.Fatalf("node-A reconstruct: %v", err)
	}

	if got := restartedA.ArenaCount(); got != 1 {
		t.Fatalf("node-A recovered %d arenas, want exactly its own 1", got)
	}
	for _, ar := range restartedA.arenas {
		if ar.ID == arenaB.ID {
			t.Fatalf("node-A adopted node-B's arena %d — foreign arena in free-list", ar.ID)
		}
	}

	// The offsets node-A hands out must stay outside node-B's byte range.
	off := allocOne(t, restartedA, "node-A allocate after restart")
	if off >= arenaB.DiskStart && off < arenaB.DiskEnd {
		t.Fatalf("node-A allocated disk offset %d inside node-B's arena [%d,%d)",
			off, arenaB.DiskStart, arenaB.DiskEnd)
	}
}

// A node holding several arenas must recover all of them after a restart, not
// just the last one acquired. Before arena:<node_id>/<arena_id> replaced the
// single arena:<node_id> record, the second AcquireArena silently overwrote
// the first record and its arena was never re-adopted — a permanent leak on
// any node writing more than one arena's worth of data.
func TestIntegration_ReconstructRecoversAllOwnedArenas(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	alloc := NewAllocator("node-A", store)

	first, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	second, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if first.ID == second.ID {
		t.Fatalf("acquired the same arena twice: %d", first.ID)
	}

	restarted := NewAllocator("node-A", store)
	if err := restarted.Reconstruct(ctx); err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if got := restarted.ArenaCount(); got != 2 {
		t.Fatalf("recovered %d arenas after restart, want 2 (arenas %d and %d)", got, first.ID, second.ID)
	}
	seen := map[uint64]bool{}
	for _, ar := range restarted.arenas {
		seen[ar.ID] = true
	}
	if !seen[first.ID] || !seen[second.ID] {
		t.Fatalf("recovered arenas %v, want both %d and %d", seen, first.ID, second.ID)
	}
}

// ReleaseArena must return every arena a node owns, not just one — the same
// leak as above but on the release path (node leave or fencing reclaim).
func TestIntegration_ReleaseArenaReleasesAllOwned(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	alloc := NewAllocator("node-A", store)

	first, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	second, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}

	released, err := store.ReleaseArena(ctx, "node-A")
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if len(released) != 2 {
		t.Fatalf("released %v, want both %d and %d", released, first.ID, second.ID)
	}

	restarted := NewAllocator("node-A", store)
	if err := restarted.Reconstruct(ctx); err != nil {
		t.Fatalf("reconstruct after release: %v", err)
	}
	if got := restarted.ArenaCount(); got != 0 {
		t.Fatalf("node-A recovered %d arenas after releasing all of them, want 0", got)
	}
}

// A malformed ownership record must not be read as arena 0, which the node
// probably does not own.  Compaction used to write an ASCII "id=%d" value here.
func TestIntegration_MalformedArenaRecordIsIgnored(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")

	if _, err := store.Put(ctx, metadata.ArenaOwnerKey("node-A", 7), []byte("id=7")); err != nil {
		t.Fatalf("seed malformed record: %v", err)
	}

	alloc := NewAllocator("node-A", store)
	if err := alloc.Reconstruct(ctx); err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if got := alloc.ArenaCount(); got != 0 {
		t.Fatalf("adopted %d arenas from a malformed record, want 0", got)
	}
}

// Arena ID 0 is a real arena — the global counter starts there — so a node
// owning it must recover it rather than treating 0 as "no record".
func TestIntegration_ArenaZeroIsRecovered(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")

	alloc := NewAllocator("node-A", store)
	first, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if first.ID != 0 {
		t.Skipf("counter did not start at 0 (got %d); test assumes a clean store", first.ID)
	}

	restarted := NewAllocator("node-A", store)
	if err := restarted.Reconstruct(ctx); err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if got := restarted.ArenaCount(); got != 1 {
		t.Fatalf("recovered %d arenas, want 1 (arena 0 must not be mistaken for 'no record')", got)
	}
}

// A freed arena must be reused before the counter is bumped for a new one —
// otherwise ClaimFreeArena is dead code and space never comes back.
func TestIntegration_ReleasedArenaIsReused(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	alloc := NewAllocator("node-A", store)

	first, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	released, err := store.ReleaseArena(ctx, "node-A")
	if err != nil || len(released) != 1 || released[0] != first.ID {
		t.Fatalf("release: released=%v err=%v (want [%d])", released, err, first.ID)
	}

	second, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("re-acquire got arena %d, want the freed arena %d back", second.ID, first.ID)
	}
}

// A recycled arena is not empty: its previous owner's live extents must be
// marked allocated before the new owner writes, or the new owner can hand out
// a block that still holds another inode's data.
func TestIntegration_RecycledArenaKeepsLiveExtentsMarked(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, "node-A")
	alloc := NewAllocator("node-A", store)

	first, err := alloc.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	off := allocOne(t, alloc, "allocate")
	// A live extent for an inode that still exists — the scrubber would not
	// reclaim it, and neither should a recycling node overwrite it.
	if _, err := store.Put(ctx, metadata.InodeKey(42), []byte("stub-inode")); err != nil {
		t.Fatalf("seed inode: %v", err)
	}
	if err := store.AppendExtent(ctx, 42, 0, off, BlockSize, 1); err != nil {
		t.Fatalf("append extent: %v", err)
	}

	if released, err := store.ReleaseArena(ctx, "node-A"); err != nil || len(released) != 1 {
		t.Fatalf("release: released=%v err=%v", released, err)
	}

	recycler := NewAllocator("node-B", metadata.NewStore(store.Client(), "node-B"))
	recycled, err := recycler.AcquireArena(ctx)
	if err != nil {
		t.Fatalf("node-B acquire: %v", err)
	}
	if recycled.ID != first.ID {
		t.Skipf("arena %d was not recycled to node-B (got %d) — pool had another candidate", first.ID, recycled.ID)
	}

	// Allocating BlocksPerArena-1 more blocks must never return the offset the
	// old extent still occupies.
	for i := 0; i < 4; i++ {
		got := allocOne(t, recycler, fmt.Sprintf("node-B allocate %d", i))
		if got == off {
			t.Fatalf("node-B was handed disk_off=%d, which node-A's live extent still occupies", got)
		}
	}
}
