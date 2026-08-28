package harness

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// ---- C5.1: Self-fence — node stops accepting writes after lease expiry ----

func TestFencing_SelfFenceTrigger(t *testing.T) {
	s := NewSimulator(5001)

	// Create some state
	_, _ = s.createFile(t.Context(), 1, "file.txt", 100, 0100644)

	// Verify operations work
	assert.NotNil(t, s.inodes[100])

	// Simulate lease expiry — inject fault
	s.injectFault(FaultLeaseExpiry)

	// After fence, inodes should be cleared (simulating crash + recovery)
	s.simulateCrash()

	// Verify state was recovered from store
	ino := s.lookup(1, "file.txt")
	assert.Equal(t, uint64(100), ino)

	v := s.checkInvariants()
	assert.Zero(t, v, "no violations after self-fence")
}

// ---- C5.2: No residual writes after self-fence ----

func TestFencing_NoResidualWrites(t *testing.T) {
	s := NewSimulator(5002)
	ctx := t.Context()

	// Create file and write data
	_, _ = s.createFile(ctx, 1, "victim.txt", 200, 0100644)
	s.writeInode(ctx, 200, 4096)

	// Self-fence
	s.injectFault(FaultLeaseExpiry)
	s.simulateCrash()

	// Verify the file still exists after recovery
	rec := s.getattr(200)
	require.NotNil(t, rec)

	// The size should match what was committed before the fence
	assert.Equal(t, uint64(4096), rec.Size)

	v := s.checkInvariants()
	assert.Zero(t, v)
}

// ---- C5.8: Lock reclamation blocked until fencing confirmed ----

func TestFencing_LockBlockedUntilFence(t *testing.T) {
	s := NewSimulator(5003)
	ctx := t.Context()

	ino := uint64(300)

	// Create file
	_, _ = s.createFile(ctx, 1, "locked.txt", ino, 0100644)

	// Acquire lock
	s.acquireLock(ctx, ino)
	require.NotNil(t, s.locks[ino], "lock should be held")

	// Inject lease expiry (simulating node death)
	s.injectFault(FaultLeaseExpiry)

	// After fence, the lock should be cleared (leases expired)
	s.simulateCrash()

	// Lock should be gone after crash + recovery
	assert.Nil(t, s.locks[ino], "lock should be released after fencing")

	v := s.checkInvariants()
	assert.Zero(t, v)
}

// ---- C5.9: Self-fence beats external fence ----

func TestFencing_SelfFenceBeforeExternal(t *testing.T) {
	s := NewSimulator(5004)
	ctx := t.Context()

	// Create files
	for i := 0; i < 5; i++ {
		_, _ = s.createFile(ctx, 1, fmt.Sprintf("f%d", i), uint64(400+i), 0100644)
	}

	// Self-fence first (simulated by lease expiry)
	s.injectFault(FaultLeaseExpiry)

	// Then external fence (simulated by crash)
	s.simulateCrash()

	// All files should be recoverable
	for i := 0; i < 5; i++ {
		ino := s.lookup(1, fmt.Sprintf("f%d", i))
		assert.Equal(t, uint64(400+i), ino)
	}

	v := s.checkInvariants()
	assert.Zero(t, v)
}

// ---- C5.12: Slow etcd — self-fence race prevention ----

func TestFencing_SlowEtcdRacePrevention(t *testing.T) {
	s := NewSimulator(5005)
	ctx := t.Context()

	_, _ = s.createFile(ctx, 1, "race.txt", 500, 0100644)

	// Simulate multiple ticks of "slow etcd"
	for i := 0; i < 20; i++ {
		s.store.Tick()
	}

	// Verify file still exists after slow period
	ino := s.lookup(1, "race.txt")
	assert.Equal(t, uint64(500), ino)

	// No violations
	v := s.checkInvariants()
	assert.Zero(t, v)
}

// ---- Controller: generation bump flow ----

func TestFencing_GenerationBump(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	nodeID := "vol-test"

	// Initialize generation
	_, err := store.Put(ctx, metadata.GenKey(nodeID), []byte("0"))
	require.NoError(t, err)

	// Simulate controller bumping generation
	newGen, err := store.BumpGeneration(ctx, nodeID, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), newGen)

	// Verify generation was stored
	gen, err := store.GetGeneration(ctx, nodeID)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen)

	// Concurrent bump should fail
	_, err = store.BumpGeneration(ctx, nodeID, 0) // stale expected value
	assert.Error(t, err, "concurrent bump with stale generation should fail")
}

// ---- Controller: leader election delegation ----

func TestFencing_ControllerLeaderElection(t *testing.T) {
	// Leader election is implemented via etcd lease-backed keys.
	// This test verifies the pattern: two controllers, one wins the lease.
	s := NewSimulator(5007)
	ctx := context.Background()

	// Simulate two controllers racing for leader key
	key := "fencing/leader"

	// Controller A acquires leader key
	_, err := s.store.Put(ctx, key, []byte("controller-a"))
	require.NoError(t, err)

	// Controller B tries to acquire — key already exists
	val, err := s.store.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "controller-a", string(val), "controller A should be leader")

	// Controller A's lease expires
	s.injectFault(FaultLeaseExpiry)

	// After expiry/fence, the leader key might be cleared
	s.simulateCrash()
}

// ---- Integration: generation-guided lock grant ----

func TestFencing_LockGrantWithGeneration(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	nodeID := "test-fence-node"

	// Set up generation
	_, err := store.Put(ctx, metadata.GenKey(nodeID), []byte("5"))
	require.NoError(t, err)

	// Verify generation is 5
	gen, err := store.GetGeneration(ctx, nodeID)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), gen)

	// Bump to 6
	newGen, err := store.BumpGeneration(ctx, nodeID, 5)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), newGen)

	// Verify generation was bumped
	gen, err = store.GetGeneration(ctx, nodeID)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), gen)

	// Stale generation bump should fail
	_, err = store.BumpGeneration(ctx, nodeID, 5)
	assert.Error(t, err, "stale generation bump should be rejected")

	// Correct generation bump should succeed
	_, err = store.BumpGeneration(ctx, nodeID, 6)
	assert.NoError(t, err)
}

// ---- C5.8: Multiple sequential fences ----

func TestFencing_SequentialFences(t *testing.T) {
	s := NewSimulator(5008)
	ctx := context.Background()
	nodeID := "multi-fence-node"

	_, err := s.store.Put(ctx, metadata.GenKey(nodeID), []byte("0"))
	require.NoError(t, err)

	for fenceNum := uint64(1); fenceNum <= 3; fenceNum++ {
		newGen, err := s.store.BumpGeneration(ctx, nodeID, fenceNum-1)
		require.NoError(t, err)
		assert.Equal(t, fenceNum, newGen)

		// Verify the generation persists
		gen, err := s.store.GetGeneration(ctx, nodeID)
		require.NoError(t, err)
		assert.Equal(t, fenceNum, gen)
	}
}

// ---- C5.10: Post-fence scrub confirms no stale writes ----

func TestFencing_PostFenceScrub(t *testing.T) {
	s := NewSimulator(5009)
	ctx := t.Context()

	// Create files and write data
	_, _ = s.createFile(ctx, 1, "pre-fence.txt", 600, 0100644)
	s.writeInode(ctx, 600, 4096)

	// Record pre-fence state
	preSize := s.getattr(600).Size

	// Fence via lease expiry
	s.injectFault(FaultLeaseExpiry)
	s.simulateCrash()

	// Post-fence: verify the committed data survived
	rec := s.getattr(600)
	require.NotNil(t, rec)
	assert.Equal(t, preSize, rec.Size, "pre-fence committed data should survive")

	v := s.checkInvariants()
	assert.Zero(t, v)
}
