//go:build integration
// +build integration

// Fencing-guard integration tests.
//
// These need a real etcd because the whole point of the guard is the atomicity
// of the comparison and the mutation inside one etcd transaction — a mock that
// evaluates them separately would pass while the real thing raced.
//
// Run with:
//
//	ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration -count=1 -v ./pkg/metadata/ -run Guard
package metadata

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// guardedStore returns a store whose transactions are guarded against the
// generation it starts at, mirroring what the IPC service installs at startup.
func guardedStore(t *testing.T, nodeID string) (*Store, context.Context) {
	t.Helper()
	store := testStore(t, nodeID)
	ctx := context.Background()

	gen, err := store.EnsureGenerationKey(ctx, nodeID)
	require.NoError(t, err)

	store.SetGuard(func() (clientv3.Cmp, uint64, bool) {
		return WithGenerationGuard(nodeID, gen), gen, true
	})
	return store, ctx
}

// fence bumps the node's generation the way the fencing controller would,
// invalidating any guard captured before the bump.
func fence(t *testing.T, store *Store, nodeID string) {
	t.Helper()
	current, err := store.GetGeneration(context.Background(), nodeID)
	require.NoError(t, err)
	_, err = store.BumpGeneration(context.Background(), nodeID, current)
	require.NoError(t, err)
}

func TestGuard_NamespaceOpsSucceedWhenNotFenced(t *testing.T) {
	store, ctx := guardedStore(t, "guard-node")

	_, err := store.AtomicCreateFile(ctx, 1, "file", 100, 0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)

	_, err = store.AtomicCreateDir(ctx, 1, "dir", 101, 0755, 1000, 1000)
	require.NoError(t, err)

	require.NoError(t, store.AtomicRename(ctx, 1, "file", 1, "renamed", 100, 0))
	require.NoError(t, store.AtomicUnlink(ctx, 1, "renamed"))
}

// Each namespace mutation must be rejected once the node is fenced.  Before
// this guard existed, every one of these succeeded on a fenced node while the
// data path was correctly blocked.
func TestGuard_NamespaceOpsRejectedWhenFenced(t *testing.T) {
	store, ctx := guardedStore(t, "guard-node")

	// Seed entries to delete/rename *before* fencing, so the fenced-path
	// assertions fail on the guard rather than on a missing key.
	_, err := store.AtomicCreateFile(ctx, 1, "victim", 200, 0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)

	fence(t, store, "guard-node")

	t.Run("create", func(t *testing.T) {
		_, err := store.AtomicCreateFile(ctx, 1, "new", 201, 0644, 1000, 1000, CreateExtra{})
		assert.ErrorIs(t, err, ErrFenced)
	})

	t.Run("mkdir", func(t *testing.T) {
		_, err := store.AtomicCreateDir(ctx, 1, "newdir", 202, 0755, 1000, 1000)
		assert.ErrorIs(t, err, ErrFenced)
	})

	t.Run("rename", func(t *testing.T) {
		assert.ErrorIs(t, store.AtomicRename(ctx, 1, "victim", 1, "moved", 200, 0), ErrFenced)
	})

	t.Run("unlink", func(t *testing.T) {
		assert.ErrorIs(t, store.AtomicUnlink(ctx, 1, "victim"), ErrFenced)
	})

	t.Run("inode put", func(t *testing.T) {
		// setattr/symlink/mknod write inode records through Put, not Txn.
		_, err := store.Put(ctx, InodeKey(200), []byte("mutated"))
		assert.ErrorIs(t, err, ErrFenced)
	})

	t.Run("delete", func(t *testing.T) {
		assert.ErrorIs(t, store.Delete(ctx, InodeKey(200)), ErrFenced)
	})

	t.Run("delete prefix", func(t *testing.T) {
		_, err := store.DeletePrefix(ctx, PrefixInode)
		assert.ErrorIs(t, err, ErrFenced)
	})
}

// A fenced node must not be able to erase the evidence either: the entry it
// failed to unlink is still there afterwards.
func TestGuard_FencedNodeLeavesStateIntact(t *testing.T) {
	store, ctx := guardedStore(t, "guard-node")

	_, err := store.AtomicCreateFile(ctx, 1, "keeper", 300, 0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)

	fence(t, store, "guard-node")

	assert.ErrorIs(t, store.AtomicUnlink(ctx, 1, "keeper"), ErrFenced)

	ino, err := store.LookupDirent(ctx, 1, "keeper")
	require.NoError(t, err)
	assert.Equal(t, uint64(300), ino, "fenced unlink must not have removed the entry")
}

// An ordinary CAS failure must stay distinguishable from a fence — they demand
// opposite responses, retry versus stop.
func TestGuard_CASFailureIsNotReportedAsFenced(t *testing.T) {
	store, ctx := guardedStore(t, "guard-node")

	_, err := store.AtomicCreateFile(ctx, 1, "dup", 400, 0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)

	// Same name again: fails on the caller's own comparison, not the guard.
	_, err = store.AtomicCreateFile(ctx, 1, "dup", 401, 0644, 1000, 1000, CreateExtra{})
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrFenced)
	assert.ErrorIs(t, err, ErrExists)
}

// The first fence of a node that has already started (and so already ran
// EnsureGenerationKey, which creates gen:<node> at "0") must succeed.
//
// Regression test: BumpGeneration used to require the generation key to be
// entirely absent when expectedOld was 0, but EnsureGenerationKey runs at
// every node's startup and creates that key at "0" before the node serves
// anything — so the key always already existed by the time a real fence
// happened, and every first-ever fence was silently rejected.
func TestGuard_FirstFenceOfInitialisedNodeSucceeds(t *testing.T) {
	store := testStore(t, "fresh-node")
	ctx := context.Background()

	gen, err := store.EnsureGenerationKey(ctx, "fresh-node")
	require.NoError(t, err)
	require.Equal(t, uint64(0), gen)

	newGen, err := store.BumpGeneration(ctx, "fresh-node", gen)
	require.NoError(t, err, "fencing a freshly-initialised node must succeed")
	assert.Equal(t, uint64(1), newGen)
}

// The control-plane paths must stay usable on a fenced node, or the fencing
// controller could not fence a node twice and a restart could not recover.
func TestGuard_ControlPlaneRemainsUnguarded(t *testing.T) {
	store, ctx := guardedStore(t, "guard-node")

	fence(t, store, "guard-node")

	// A second fence must still work.
	current, err := store.GetGeneration(ctx, "guard-node")
	require.NoError(t, err)
	_, err = store.BumpGeneration(ctx, "guard-node", current)
	assert.NoError(t, err, "BumpGeneration must not be blocked by the guard")

	// Re-establishing the generation key must still work.
	_, err = store.EnsureGenerationKey(ctx, "guard-node")
	assert.NoError(t, err, "EnsureGenerationKey must not be blocked by the guard")
}

// A store with no guard installed behaves exactly as before.  Bootstrap tools
// (seed-etcd, fsck) rely on this.
func TestGuard_UnguardedStoreUnaffected(t *testing.T) {
	store := testStore(t, "plain-node")
	ctx := context.Background()

	require.NoError(t, store.PutGeneration(ctx, "plain-node", 7))

	_, err := store.AtomicCreateFile(ctx, 1, "plain", 500, 0644, 1000, 1000, CreateExtra{})
	assert.NoError(t, err, "unguarded store must not consult the generation")
}

// Failing closed matters more than failing usefully: a mutation issued before
// the generation is known must be refused, not silently let through.
func TestGuard_UnavailableGuardFailsClosed(t *testing.T) {
	store := testStore(t, "uninit-node")
	ctx := context.Background()

	store.SetGuard(func() (clientv3.Cmp, uint64, bool) {
		return clientv3.Cmp{}, 0, false
	})

	_, err := store.AtomicCreateFile(ctx, 1, "nope", 600, 0644, 1000, 1000, CreateExtra{})
	assert.ErrorIs(t, err, ErrGuardUnavailable)

	_, err = store.Put(ctx, InodeKey(600), []byte("nope"))
	assert.ErrorIs(t, err, ErrGuardUnavailable)
}

// A fenced node must not pull arenas out of the free pool: the claim is
// rejected and the arena stays free for a node that may still write.
func TestGuard_ClaimFreeArenaRejectedWhenFenced(t *testing.T) {
	store, ctx := guardedStore(t, "claim-node")

	_, err := store.Put(ctx, ArenaOwnerKey("other-node", 9), EncodeUint64(9))
	require.NoError(t, err)
	won, err := store.ReleaseArenaID(ctx, "other-node", 9)
	require.NoError(t, err)
	require.True(t, won)

	fence(t, store, "claim-node")

	_, ok, err := store.ClaimFreeArena(ctx, "claim-node")
	assert.ErrorIs(t, err, ErrFenced)
	assert.False(t, ok)

	free, err := store.Get(ctx, FreeArenaKey(9))
	require.NoError(t, err)
	assert.NotNil(t, free, "a rejected claim must leave the arena in the pool")
	owner, err := store.Get(ctx, ArenaOwnerKey("claim-node", 9))
	require.NoError(t, err)
	assert.Nil(t, owner, "a rejected claim must not record ownership")
}
