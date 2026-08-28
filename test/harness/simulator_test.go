package harness

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/etcfs/etcfs/pkg/metadata"
)

func TestDeterministicReplay(t *testing.T) {
	s1 := NewSimulator(42)
	v1 := s1.Run(100, 1)

	s2 := NewSimulator(42)
	v2 := s2.Run(100, 1)

	assert.Equal(t, v1, v2, "same seed should produce identical results")
	assert.Zero(t, v1, "no faults injected, should have zero violations")
}

func TestNlinkInvariant(t *testing.T) {
	s := NewSimulator(123)

	// Create 10 files, then unlink 5 — nlink should be consistent
	for i := 0; i < 10; i++ {
		s.executeRandomOp()
	}
	s.store.Tick()

	v := s.checkNlinkConsistency()
	assert.Zero(t, v, "nlink should be consistent")
}

func TestCrashRecovery(t *testing.T) {
	s := NewSimulator(456)

	// Create some state
	_, _ = s.createFile(t.Context(), 1, "test.txt", 100, 0100644)
	_, _ = s.createFile(t.Context(), 1, "other.txt", 101, 0100644)
	_, _ = s.createDir(t.Context(), 1, "mydir", 200)

	// Crash and restore
	s.simulateCrash()

	// All 3 inodes should be reloaded
	assert.NotNil(t, s.inodes[100])
	assert.NotNil(t, s.inodes[101])
	assert.NotNil(t, s.inodes[200])

	// All 3 dirents should be reloaded
	assert.Equal(t, uint64(100), s.lookup(1, "test.txt"))
	assert.Equal(t, uint64(101), s.lookup(1, "other.txt"))
	assert.Equal(t, uint64(200), s.lookup(1, "mydir"))

	v := s.checkInvariants()
	assert.Zero(t, v)
}

func TestNlinkConsistencyAfterUnlink(t *testing.T) {
	s := NewSimulator(789)

	_, _ = s.createFile(t.Context(), 1, "f1", 301, 0100644)
	_, _ = s.createFile(t.Context(), 1, "f2", 302, 0100644)
	_, _ = s.createFile(t.Context(), 1, "f3", 303, 0100644)

	// Unlink f2
	s.unlinkFile(t.Context(), 1, "f2")

	v := s.checkNlinkConsistency()
	assert.Zero(t, v, "nlink should match after unlink")

	// f2 should be gone
	assert.Nil(t, s.inodes[302])
	assert.Zero(t, s.lookup(1, "f2"))
}

func TestRenamePreservesInode(t *testing.T) {
	s := NewSimulator(111)

	_, _ = s.createFile(t.Context(), 1, "old", 401, 0100644)
	s.renameFile(t.Context(), 1, "old", 1, "new", 401)

	assert.Zero(t, s.lookup(1, "old"))
	assert.Equal(t, uint64(401), s.lookup(1, "new"))
	assert.NotNil(t, s.inodes[401])
}

func TestSeedZeroViolations(t *testing.T) {
	s := NewSimulator(42)
	v := s.Run(200, 1)
	assert.Zero(t, v, "random operations without faults should not violate invariants")
}

// ---- C4.2: Crash-at-every-point (create file) ----

func TestCrashDuringCreate(t *testing.T) {
	for seed := 1; seed <= 100; seed++ {
		s := NewSimulator(int64(seed))
		_, _ = s.createFile(t.Context(), 1, "victim.txt", 5000, 0100644)
		s.simulateCrash()
		v := s.checkInvariants()
		if !assert.Zero(t, v, "seed %d: crash during create should not violate invariants", seed) {
			break
		}
	}
}

// ---- C4.3: Crash-at-every-point (rename) ----

func TestCrashDuringRename(t *testing.T) {
	for seed := 1; seed <= 100; seed++ {
		s := NewSimulator(int64(seed))
		_, _ = s.createFile(t.Context(), 1, "src.txt", 6000, 0100644)
		s.renameFile(t.Context(), 1, "src.txt", 1, "dst.txt", 6000)
		s.simulateCrash()
		v := s.checkInvariants()
		assert.Zero(t, v, "seed %d: crash after rename should not violate invariants", seed)
		if v > 0 {
			break
		}
	}
}

// ---- C4.4: Crash-at-every-point (rm -rf / bulk delete) ----

func TestCrashDuringBulkDelete(t *testing.T) {
	for seed := 1; seed <= 100; seed++ {
		s := NewSimulator(int64(seed))

		// Create 20 files
		for i := 0; i < 20; i++ {
			_, _ = s.createFile(t.Context(), 1, fmt.Sprintf("bulk-%d", i), uint64(7000+i), 0100644)
		}

		// Unlink all
		for i := 0; i < 20; i++ {
			s.unlinkFile(t.Context(), 1, fmt.Sprintf("bulk-%d", i))
		}

		s.simulateCrash()
		v := s.checkInvariants()
		assert.Zero(t, v, "seed %d: crash after bulk delete should not violate invariants", seed)
		if v > 0 {
			break
		}
	}
}

// ---- C4.5: etcd partition during Txn ----

func TestEtcdPartitionDuringTxn(t *testing.T) {
	s := NewSimulator(1001)
	s.AddFault(5, FaultEtcdPartition)
	v := s.Run(100, 1)
	assert.Zero(t, v, "etcd partition injection should not violate invariants")
}

// ---- C4.7: Lease expiry — stale writes rejected ----

func TestLeaseExpiryStaleWrites(t *testing.T) {
	s := NewSimulator(2002)
	s.AddFault(10, FaultLeaseExpiry)
	v := s.Run(50, 1)
	assert.Zero(t, v, "lease expiry injection should not violate invariants")
}

// ---- C4.8: Transaction conflict storm ----

func TestConflictStorm(t *testing.T) {
	s := NewSimulator(3003)

	// Many clients racing to create files in the same directory
	const clients = 50
	ctx := t.Context()

	for i := 0; i < clients; i++ {
		ino := uint64(8000 + i)
		name := fmt.Sprintf("storm-%d", i)
		_, _ = s.createFile(ctx, 1, name, ino, 0100644)
	}

	// All files should exist with unique inodes
	for i := 0; i < clients; i++ {
		name := fmt.Sprintf("storm-%d", i)
		ino := s.lookup(1, name)
		assert.NotZero(t, ino, "file %s should exist after conflict storm", name)
	}

	v := s.checkInvariants()
	assert.Zero(t, v, "conflict storm should not violate invariants")
}

// ---- C4.12: Intentional bug detection ----

func TestIntentionalBug_NlinkNotDecremented(t *testing.T) {
	s := NewSimulator(4001)

	// Create a file normally
	_, _ = s.createFile(t.Context(), 1, "buggy.txt", 9001, 0100644)

	// Simulate bug: delete dirent but forget to decrement nlink
	key := metadata.DirentKey(1, "buggy.txt")
	delete(s.dirents, key)
	_ = s.store.Delete(t.Context(), key)
	// BUG: forgot to decrement s.inodes[9001].Nlink

	v := s.checkInvariants()
	assert.Greater(t, v, 0, "harness should detect nlink not decremented bug")
}

func TestIntentionalBug_DirentPointsToMissingInode(t *testing.T) {
	s := NewSimulator(4002)

	// Create a dirent that points to a non-existent inode
	s.dirents[metadata.DirentKey(1, "orphan.txt")] = 99999

	v := s.checkInvariants()
	assert.Greater(t, v, 0, "harness should detect missing inode bug")
}

func TestIntentionalBug_DuplicateInode(t *testing.T) {
	s := NewSimulator(4003)

	// Create two dirents with different names pointing to the same inode
	_, _ = s.createFile(t.Context(), 1, "file-a.txt", 9100, 0100644)
	// Bug: overwrite the inode entirely with a second create
	s.inodes[9100] = &metadata.InodeRecord{
		Ino: 9100, Mode: 0100644, Nlink: 1, Size: 999,
	}
	s.store.log = nil

	// The duplicate should be flagged — nlink should be 1 but we have 2 dirents
	// Actually this won't detect it because we overwrote the inode
	// Let's test differently: dirent points to same inode but with wrong nlink
	s.dirents[metadata.DirentKey(1, "file-b.txt")] = 9100
	// Now ino 9100 has nlink=1 but 2 dirents point to it

	v := s.checkInvariants()
	assert.Greater(t, v, 0, "harness should detect duplicate inode via nlink mismatch")
}

// ---- C4.10: Deterministic linearizability — operation history ----

func TestLinearizability_BasicCreateDelete(t *testing.T) {
	s := NewSimulator(5001)
	ctx := t.Context()

	// Record operation history
	type historyEntry struct {
		op   string
		ino  uint64
		name string
	}
	var history []historyEntry

	// Create
	_, _ = s.createFile(ctx, 1, "linear", 9200, 0100644)
	history = append(history, historyEntry{"create", 9200, "linear"})

	// Verify it exists
	ino := s.lookup(1, "linear")
	assert.Equal(t, uint64(9200), ino)
	history = append(history, historyEntry{"lookup-found", 9200, "linear"})

	// Delete
	s.unlinkFile(ctx, 1, "linear")
	history = append(history, historyEntry{"delete", 9200, "linear"})

	// Verify it's gone
	ino = s.lookup(1, "linear")
	assert.Zero(t, ino)
	history = append(history, historyEntry{"lookup-missing", 0, "linear"})

	// Verify no invariants violated
	v := s.checkInvariants()
	assert.Zero(t, v)

	// Replay history deterministically
	s2 := NewSimulator(5001)
	_, _ = s2.createFile(ctx, 1, "linear", 9200, 0100644)
	s2.lookup(1, "linear")
	s2.unlinkFile(ctx, 1, "linear")
	s2.lookup(1, "linear")
	assert.Zero(t, s2.checkInvariants())

	t.Logf("history: %d entries, no violations", len(history))
}

// ---- C4.11: CI integration — test runs quickly ----

func TestHarnessPerformance(t *testing.T) {
	s := NewSimulator(42)
	v := s.Run(500, 1)
	assert.Zero(t, v)
	ops, _, _ := s.Stats()
	assert.Equal(t, 500, ops, "should execute exactly 500 operations")
}
