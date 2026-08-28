package harness

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/etcfs/etcfs/pkg/membership"
	"github.com/etcfs/etcfs/pkg/metadata"
)

func registerArena(ctx context.Context, store *MockStore, arenaID uint64) {
	key := metadata.ArenaOwnerKey(fmt.Sprintf("preexisting-%d", arenaID), arenaID)
	_, _ = store.Put(ctx, key, metadata.EncodeUint64(arenaID))
}

// nodeArena returns the key and value of the single arena a node holds under
// the new arena:<node_id>/<arena_id> layout — every test here joins a node
// exactly once, so it never holds more than one.
func nodeArena(ctx context.Context, store *MockStore, nodeID string) (key string, val []byte) {
	kvs, _ := store.GetPrefix(ctx, metadata.ArenaNodePrefix(nodeID))
	if len(kvs) == 0 {
		return "", nil
	}
	return string(kvs[0].Key), kvs[0].Value
}

// ---- C10.5: Elastic join — new node ----

func TestElastic_JoinNewNode(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	store := cluster.Store

	mgr := membership.New(store, "node-3")

	// Join: registers membership, acquires arena
	err := mgr.Join(ctx)
	require.NoError(t, err)

	// Verify membership key exists
	memVal, _ := store.Get(ctx, metadata.MembershipKey("node-3"))
	assert.NotNil(t, memVal)

	// Verify arena was acquired for node-3
	_, arenaVal := nodeArena(ctx, store, "node-3")
	assert.NotNil(t, arenaVal)

	// Existing nodes should be unaffected
	assert.True(t, mgr.IsMember(ctx, "node-3"))
	assert.Zero(t, cluster.checkAllInvariants())
	_ = cluster
}

// ---- C10.6: Elastic join — warm cache time ----

func TestElastic_WarmCacheTime(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	cluster.createDirIfMissing(ctx, 1, "existing-dir", 50000)
	registerArena(ctx, store, 30)

	mgr := membership.New(store, "node-4")
	err := mgr.Join(ctx)
	require.NoError(t, err)

	assert.True(t, mgr.IsMember(ctx, "node-4"))

	entries := cluster.FreshListDir(ctx, 50000)
	assert.NotNil(t, entries)

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C10.7: Elastic leave — graceful ----

func TestElastic_LeaveGraceful(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	store := cluster.Store

	mgr := membership.New(store, "node-exit")
	_ = mgr.Join(ctx)
	require.True(t, mgr.IsMember(ctx, "node-exit"))

	_, arenaVal := nodeArena(ctx, store, "node-exit")
	assert.NotNil(t, arenaVal)

	err := mgr.LeaveGraceful(ctx)
	require.NoError(t, err)

	assert.False(t, mgr.IsMember(ctx, "node-exit"))

	_, arenaVal = nodeArena(ctx, store, "node-exit")
	assert.Nil(t, arenaVal, "arena should be released")

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C10.8: Elastic leave — ungraceful (SIGKILL) ----

func TestElastic_LeaveUngraceful(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	store := cluster.Store

	mgr := membership.New(store, "node-killed")
	_ = mgr.Join(ctx)
	require.True(t, mgr.IsMember(ctx, "node-killed"))

	_, arenaVal := nodeArena(ctx, store, "node-killed")
	assert.NotNil(t, arenaVal)

	mgr.LeaveUngraceful(ctx)

	assert.False(t, mgr.IsMember(ctx, "node-killed"))
	_, arenaVal = nodeArena(ctx, store, "node-killed")
	assert.Nil(t, arenaVal)

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C10.9: Arena rebalancing — manual advisory ----

func TestElastic_RebalanceArena(t *testing.T) {
	cluster := NewCluster(3)
	ctx := t.Context()
	store := cluster.Store

	mgrA := membership.New(store, "node-A")
	_ = mgrA.Join(ctx)

	mgrB := membership.New(store, "node-B")
	_ = mgrB.Join(ctx)

	_, arenaAVal := nodeArena(ctx, store, "node-A")
	require.NotNil(t, arenaAVal)
	arenaAID := metadata.DecodeUint64(arenaAVal)

	_, arenaBVal := nodeArena(ctx, store, "node-B")
	require.NotNil(t, arenaBVal)

	// RebalanceArena requires the source to already be fenced (see the
	// function's doc comment for why this is the one case reassignment is
	// actually safe) — bump node-A's generation first, as the fencing
	// controller would after a confirmed fence.
	_, err := store.BumpGeneration(ctx, "node-A", 0)
	require.NoError(t, err)

	err = mgrA.RebalanceArena(ctx, "node-A", "node-B", arenaAID)
	require.NoError(t, err)

	_, arenaAValAfter := nodeArena(ctx, store, "node-A")
	assert.Nil(t, arenaAValAfter)

	_, arenaBValAfter := nodeArena(ctx, store, "node-B")
	assert.Equal(t, arenaAID, metadata.DecodeUint64(arenaBValAfter))

	assert.Zero(t, cluster.checkAllInvariants())
	_ = arenaBVal
}

// TestElastic_RebalanceArenaRejectsUnfencedSource is the actual point of the
// guard added to RebalanceArena: moving an arena away from a live, healthy
// node is the one case Kleppmann's stale-write analysis identifies as
// unclosable by any check (both nodes would be unfenced, so nothing has
// grounds to reject it) — so the function must refuse it outright rather than
// perform it and hope. See the doc comment on RebalanceArena for the full
// argument.
func TestElastic_RebalanceArenaRejectsUnfencedSource(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	store := cluster.Store

	mgrA := membership.New(store, "live-node")
	require.NoError(t, mgrA.Join(ctx))
	mgrB := membership.New(store, "target-node")
	require.NoError(t, mgrB.Join(ctx))

	_, arenaVal := nodeArena(ctx, store, "live-node")
	require.NotNil(t, arenaVal)
	arenaID := metadata.DecodeUint64(arenaVal)

	// live-node was never fenced (generation stays 0) — the rebalance must
	// be rejected, and the arena must stay exactly where it was.
	err := mgrA.RebalanceArena(ctx, "live-node", "target-node", arenaID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not been fenced")

	_, stillThere := nodeArena(ctx, store, "live-node")
	assert.Equal(t, arenaVal, stillThere, "arena must be untouched after a rejected rebalance")

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- C10.11: Global arena pool under contention ----

func TestElastic_ArenaPoolContention(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	const nodes = 4
	var wg sync.WaitGroup
	arenas := make(map[string]uint64)
	var mu sync.Mutex

	for i := 0; i < nodes; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("contend-%d", idx)
			mgr := membership.New(store, nodeID)
			_ = mgr.Join(ctx)

			_, arenaVal := nodeArena(ctx, store, nodeID)
			if arenaVal != nil {
				mu.Lock()
				arenas[nodeID] = metadata.DecodeUint64(arenaVal)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	assert.Len(t, arenas, nodes, "all nodes should get an arena")
	// Ensure no duplicate arenas
	seen := make(map[uint64]bool)
	for _, id := range arenas {
		assert.False(t, seen[id], "arena %d should be unique", id)
		seen[id] = true
	}

	assert.Zero(t, cluster.checkAllInvariants())
	_ = cluster
}

// ---- Additional Tests ----

func TestElastic_MultipleJoinLeaveCycles(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	store := cluster.Store

	for cycle := 0; cycle < 5; cycle++ {
		nodeID := fmt.Sprintf("cycle-%d", cycle)
		mgr := membership.New(store, nodeID)

		_ = mgr.Join(ctx)
		assert.True(t, mgr.IsMember(ctx, nodeID))

		_ = mgr.LeaveGraceful(ctx)
		assert.False(t, mgr.IsMember(ctx, nodeID))
	}

	assert.Zero(t, cluster.checkAllInvariants())
	_ = cluster
}

// TestElastic_ConcurrentJoin is the in-memory equivalent of
// scripts/test/chaos-elastic-concurrent.sh: several nodes join at the same
// time, against the same shared store, instead of one after another. The
// chaos script proves this against real containers/EC2 in minutes; this
// proves the same property against MockStore in milliseconds, so a
// regression here is caught by `go test ./...` rather than only by someone
// remembering to run the chaos script.
//
// Scope note: this asserts arena disjointness only. It previously also
// asserted non-overlapping per-node inode ranges via ReserveInodeRange, but
// that was dead code — no production path ever called it. Inode allocation
// is a single global CAS-retried counter (Service.allocInode ->
// Store.NextCounter), which is a method on *metadata.Store and so is not
// reachable from MockStore; concurrent inode allocation therefore has no
// harness-level coverage and is exercised only by
// pkg/metadata/integration_test.go's TestIntegration_CounterIsUniqueUnderConcurrency
// against real etcd, and by the chaos script's 20-way concurrent create.
func TestElastic_ConcurrentJoin(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	const nodes = 5
	var wg sync.WaitGroup

	type joinResult struct {
		nodeID  string
		arena   uint64
		joinErr error
	}
	results := make([]joinResult, nodes)

	for i := 0; i < nodes; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("concurrent-join-%d", idx)
			mgr := membership.New(store, nodeID)

			joinErr := mgr.Join(ctx)

			_, arenaVal := nodeArena(ctx, store, nodeID)
			var arena uint64
			if arenaVal != nil {
				arena = metadata.DecodeUint64(arenaVal)
			}

			results[idx] = joinResult{nodeID: nodeID, arena: arena, joinErr: joinErr}
		}(i)
	}
	wg.Wait()

	// No arena may be handed to two nodes — that is the hazard a broken CAS
	// retry produces, and the one that previously let a restarting node adopt
	// a live peer's disk range (see kleppmann-stale-write-analysis.md).
	seenArenas := make(map[uint64]bool)
	for _, r := range results {
		require.NoError(t, r.joinErr, "join must succeed for %s", r.nodeID)
		assert.False(t, seenArenas[r.arena], "arena %d handed to more than one node", r.arena)
		seenArenas[r.arena] = true
	}

	assert.Zero(t, cluster.checkAllInvariants())
}

func TestElastic_RebalanceIdempotent(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	mgrA := membership.New(store, "src")
	_ = mgrA.Join(ctx)
	mgrB := membership.New(store, "dst")
	_ = mgrB.Join(ctx)

	_, arenaAVal := nodeArena(ctx, store, "src")
	arenaAID := metadata.DecodeUint64(arenaAVal)

	_, err := store.BumpGeneration(ctx, "src", 0)
	require.NoError(t, err)

	require.NoError(t, mgrA.RebalanceArena(ctx, "src", "dst", arenaAID))

	// Rebalancing again should fail (src no longer has the arena) — a
	// different reason than the fencing guard, which "src" still passes
	// (its generation only ever increases, so it stays fenced).
	err = mgrA.RebalanceArena(ctx, "src", "dst", arenaAID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not hold it")

	assert.Zero(t, cluster.checkAllInvariants())
}

// ---- Fault injection during join/leave ----
//
// These are the cheap, deterministic equivalent of two of the four
// fault-injection scenarios exercised by the chaos scripts: killing a
// joining node before its first write, and bumping a leaving node's
// generation mid-leave. The other two (partitioning a joining node mid-join,
// killing a survivor while a different node is mid-join) have no meaningful
// equivalent against MockStore/membership.Manager — partitioning mid-join
// needs a real self-fencing watchdog process to observe, and killing a
// survivor needs a real daemon crash affecting a real FUSE mount, neither of
// which the harness's Join()/LeaveGraceful() abstraction models. Both are
// chaos-script-only.

// TestElastic_JoinInterruptedBeforeArena is the harness equivalent of the
// join-interrupted chaos scenario: a node's daemon dies after registering
// membership but before its first
// write (arena acquisition is lazy — see pkg/arena/allocator.go,
// AcquireArena is only called from handleWriteBlock, not at startup — so
// "before FUSE mount" and "before any write" are the same instant from the
// allocator's perspective). Manager.Join() bundles membership registration
// and arena acquisition in one call with no way to stop between them from
// outside the package, so this simulates the interruption the same way
// registerMembership itself would: writing the membership key directly via
// the public Store, then simply never calling AcquireArena.
func TestElastic_JoinInterruptedBeforeArena(t *testing.T) {
	cluster := NewCluster(2)
	ctx := t.Context()
	store := cluster.Store

	const nodeID = "half-joined"
	_, err := store.Put(ctx, metadata.MembershipKey(nodeID), []byte(`{"joined":1}`))
	require.NoError(t, err)

	// The crash happens here — no AcquireArena call ever happens.

	_, arenaVal := nodeArena(ctx, store, nodeID)
	assert.Nil(t, arenaVal, "a node that never reached its first write must not hold an arena")

	// The rest of the cluster must be completely unaffected: another node
	// joining and creating files does not observe the half-joined node at
	// all (it has no arena, so nothing depends on reclaiming from it).
	mgr := membership.New(store, "node-other")
	require.NoError(t, mgr.Join(ctx))
	assert.True(t, mgr.IsMember(ctx, "node-other"))

	assert.Zero(t, cluster.checkAllInvariants())
}

// TestElastic_GenerationBumpDuringGracefulLeave is the harness equivalent
// of the generation-bump-during-leave chaos scenario: a node's fencing
// generation is bumped (as the fencing controller
// would on a lease expiry) while it is in the middle of leaving gracefully.
//
// Caveat, stated plainly: MockStore has no SetGuard/guard-enforcement
// concept — unlike the production metadata.Store, LeaveGraceful's Delete/Put
// calls here are never rejected by a generation mismatch, because nothing in
// the harness checks one. This test can only verify the structural
// invariant (no orphaned arena, membership key gone, cluster consistent
// afterward), not that the bump actually blocks the leaving node's own
// writes — that guard-rejection behavior is real in production
// (verified by scripts/test/chaos-fencing-namespace.sh) but is not modeled
// by this harness type at all.
func TestElastic_GenerationBumpDuringGracefulLeave(t *testing.T) {
	cluster := NewCluster(1)
	ctx := t.Context()
	store := cluster.Store

	const nodeID = "leaving-node"
	mgr := membership.New(store, nodeID)
	require.NoError(t, mgr.Join(ctx))

	_, arenaVal := nodeArena(ctx, store, nodeID)
	require.NotNil(t, arenaVal, "node must hold an arena before it can leave")

	// Fencing controller bumps the generation mid-leave.
	_, err := store.BumpGeneration(ctx, nodeID, 0)
	require.NoError(t, err)

	err = mgr.LeaveGraceful(ctx)
	require.NoError(t, err)

	assert.False(t, mgr.IsMember(ctx, nodeID), "membership key must be gone after leave")
	_, arenaVal = nodeArena(ctx, store, nodeID)
	assert.Nil(t, arenaVal, "arena must not be orphaned — released back to the free pool")

	assert.Zero(t, cluster.checkAllInvariants())
}
