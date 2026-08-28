//go:build integration
// +build integration

// Integration tests for the metadata layer against a real etcd cluster.
//
// Requires a running etcd cluster.  Start one with:
//
//	docker compose -f deploy/docker/docker-compose.yml up -d etcd1 etcd2 etcd3
//
// Run tests with:
//
//	ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration -count=1 ./... -run Integration
package metadata

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/etcfs/etcfs/test/etcdtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// testStore returns a Store on a client scoped to this test's own etcd key
// space, so suites running in parallel cannot delete each other's records.
func testStore(t *testing.T, nodeID string) *Store {
	t.Helper()
	return NewStore(etcdtest.Client(t), nodeID)
}

// ---- C1.1: Schema validation ----

func TestIntegration_SchemaKeyFormats(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()

	// Write a key in each family and verify we can read it back
	_, err := store.Put(ctx, InodeKey(1), []byte("test-inode"))
	require.NoError(t, err)

	_, err = store.Put(ctx, DirentKey(1, "hello"), EncodeUint64(42))
	require.NoError(t, err)

	_, err = store.Put(ctx, LockKey(1, LockShared, "1"), []byte("test"))
	require.NoError(t, err)

	_, err = store.Put(ctx, GenKey("node-1"), []byte("5"))
	require.NoError(t, err)

	// Verify reads
	v, err := store.Get(ctx, InodeKey(1))
	require.NoError(t, err)
	assert.Equal(t, "test-inode", string(v))

	v, err = store.Get(ctx, DirentKey(1, "hello"))
	require.NoError(t, err)
	assert.Equal(t, uint64(42), DecodeUint64(v))
}

// ---- C1.2: Atomic dirent create (concurrent) ----

func TestIntegration_AtomicDirentCreate(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(1)

	const concurrent = 20
	errCh := make(chan error, concurrent)
	var created int32

	// Create parent inode
	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			name := fmt.Sprintf("file-%d", id)
			err := store.CreateDirent(ctx, parent, name, uint64(id+10))
			if err == nil {
				atomic.AddInt32(&created, 1)
			}
			errCh <- err
		}(i)
	}
	wg.Wait()
	close(errCh)

	// Each file should have been created exactly once
	assert.Equal(t, int32(concurrent), created, "all %d concurrent creates should succeed", concurrent)

	// Verify all entries exist
	entries, err := store.ListDirents(ctx, parent)
	require.NoError(t, err)
	assert.Len(t, entries, concurrent, "should have %d directory entries", concurrent)
}

// ---- C1.3: Atomic cross-directory rename ----

func TestIntegration_AtomicRename(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()

	parent := uint64(10001)
	ino := uint64(42)

	_, err := store.CreateInode(ctx, parent, ModeDir|0755, 0, 0)
	require.NoError(t, err)

	// Create source file
	err = store.CreateDirent(ctx, parent, "old-name", ino)
	require.NoError(t, err)

	// Rename
	err = store.AtomicRename(ctx, parent, "old-name", parent, "new-name", ino, 0)
	require.NoError(t, err)

	// Old name should not exist
	oldIno, err := store.LookupDirent(ctx, parent, "old-name")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), oldIno, "old name should not exist after rename")

	// New name should point to the inode
	newIno, err := store.LookupDirent(ctx, parent, "new-name")
	require.NoError(t, err)
	assert.Equal(t, ino, newIno, "new name should point to same inode")
}

// ---- C1.5: Lease-backed lock acquire/release ----

func TestIntegration_LockAcquireRelease(t *testing.T) {
	store := testStore(t, "node-a")
	ctx := context.Background()
	ino := uint64(100)

	// Acquire exclusive lock
	holder, err := store.AcquireLock(ctx, ino, LockExclusive, 10*time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, holder)

	// Verify lock exists
	locked, err := store.IsLocked(ctx, ino)
	require.NoError(t, err)
	assert.True(t, locked)

	// Release
	released, err := store.ReleaseLock(ctx, ino, LockExclusive, holder)
	require.NoError(t, err)
	require.True(t, released, "the lock key should still have been there to release")

	// Verify the holder's key is gone
	locked, err = store.IsLocked(ctx, ino)
	require.NoError(t, err)
	assert.False(t, locked)
}

// ---- C1.6: Lease expiry releases lock ----

// TestIntegration_LockLeaseExpiry simulates a holder that dies outright: its
// client goes away, so nothing renews the lease and etcd expires it.
//
// The holder gets its own client precisely so it can be killed.  Cancelling
// the acquisition context would not do this — the keepalive stream is
// deliberately not bound to that context (see AcquireLock), because a lock
// that lapsed while its holder still believed it held it is the exact hazard
// locking exists to prevent.
func TestIntegration_LockLeaseExpiry(t *testing.T) {
	observer := testStore(t, "node-observer")
	ino := uint64(200)

	// A client of its own, so it can be killed — but in the same test key
	// space as the observer, or neither would see the other's keys.
	holderCli := etcdtest.Client(t)
	holder := NewStore(holderCli, "node-b")

	token, err := holder.AcquireLock(context.Background(), ino, LockExclusive, 3*time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	locked, err := observer.IsLocked(context.Background(), ino)
	require.NoError(t, err)
	require.True(t, locked, "lock should be held while the holder is alive")

	// The holder dies: no further keepalives reach etcd.
	require.NoError(t, holderCli.Close())

	t.Log("waiting for lease expiry (3s TTL + grace)...")
	require.Eventually(t, func() bool {
		l, ierr := observer.IsLocked(context.Background(), ino)
		return ierr == nil && !l
	}, 15*time.Second, 500*time.Millisecond, "lock should be auto-deleted after lease expiry")
}

// TestIntegration_LockSurvivesAcquisitionContextCancel pins the reason
// AcquireLock does not hand the caller's context to KeepAlive.
//
// The data path acquires this lock under a request-scoped context with a
// deadline (see internal/ipc.lockInode).  If the keepalive stream were bound
// to that context, every lock held past the request deadline would stop being
// renewed and lapse at its TTL with the holder none the wiser — a silent
// stale-holder bug.  Against that earlier behaviour this test fails: the lock
// is gone well before the assertion runs.
func TestIntegration_LockSurvivesAcquisitionContextCancel(t *testing.T) {
	store := testStore(t, "node-c")
	ino := uint64(210)

	acquireCtx, cancel := context.WithCancel(context.Background())
	holder, err := store.AcquireLock(acquireCtx, ino, LockExclusive, 3*time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, holder)

	// The context that acquired the lock goes away; the lock must not.
	cancel()

	// Comfortably past the 3s TTL — a keepalive bound to acquireCtx would have
	// stopped renewing at cancel() and the lease would have lapsed by now.
	time.Sleep(7 * time.Second)

	locked, err := store.IsLocked(context.Background(), ino)
	require.NoError(t, err)
	assert.True(t, locked, "lock must outlive the context used to acquire it")

	mustReleaseLock(t, store, context.Background(), ino, LockExclusive, holder)
}

// ---- C1.7: Fencing generation CAS ----

func TestIntegration_GenerationBump(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	nodeID := "fenced-node-1"

	// Initialise generation
	_, err := store.EnsureGenerationKey(ctx, nodeID)
	require.NoError(t, err)

	gen, err := store.GetGeneration(ctx, nodeID)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), gen)

	// Bump generation
	newGen, err := store.BumpGeneration(ctx, nodeID, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), newGen)

	// Verify
	gen, err = store.GetGeneration(ctx, nodeID)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen)

	// Attempt to bump with stale expected value should fail
	_, err = store.BumpGeneration(ctx, nodeID, 0) // expected 0, actual 1
	assert.Error(t, err, "stale generation bump should fail")

	// Correct bump should succeed
	newGen, err = store.BumpGeneration(ctx, nodeID, 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), newGen)
}

// ---- C1.8: Inode number allocation ----

func TestIntegration_InodeAllocation(t *testing.T) {
	store := testStore(t, "node-1")
	ctx := context.Background()

	// The first number handed out is FirstUsableIno: 0 is never valid and 1 is
	// the root directory.
	ino, err := store.NextCounter(ctx, KeyInodeAllocCounter, FirstUsableIno)
	require.NoError(t, err)
	assert.Equal(t, FirstUsableIno, ino)

	next, err := store.NextCounter(ctx, KeyInodeAllocCounter, FirstUsableIno)
	require.NoError(t, err)
	assert.Equal(t, FirstUsableIno+1, next)
}

func TestIntegration_ReserveCounterHandsOutDisjointBlocks(t *testing.T) {
	store := testStore(t, "node-1")
	ctx := context.Background()

	const block = 1024
	first, err := store.ReserveCounter(ctx, KeyInodeAllocCounter, FirstUsableIno, block)
	require.NoError(t, err)
	assert.Equal(t, FirstUsableIno, first)

	// The whole block is reserved, not just its first number: the next caller
	// starts past the end of it or two nodes hand out the same inode.
	second, err := store.ReserveCounter(ctx, KeyInodeAllocCounter, FirstUsableIno, block)
	require.NoError(t, err)
	assert.Equal(t, first+block, second)

	// A single value still follows the block rather than reusing it.
	next, err := store.NextCounter(ctx, KeyInodeAllocCounter, FirstUsableIno)
	require.NoError(t, err)
	assert.Equal(t, second+block, next)
}

func TestIntegration_CounterIsUniqueUnderConcurrency(t *testing.T) {
	store := testStore(t, "node-1")
	ctx := context.Background()

	const n = 16
	results := make(chan uint64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := store.NextCounter(ctx, KeyInodeAllocCounter, FirstUsableIno)
			if err == nil {
				results <- v
			}
		}()
	}
	wg.Wait()
	close(results)

	seen := make(map[uint64]bool)
	for v := range results {
		assert.False(t, seen[v], "handed out %d twice", v)
		seen[v] = true
	}
	assert.Equal(t, n, len(seen), "every concurrent caller should get a distinct number")
}

// ---- C1.9: Watch delivery ----

func TestIntegration_WatchDelivery(t *testing.T) {
	store := testStore(t, "test-node")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	parent := uint64(99)
	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	// Start watching the directory prefix
	watchCh := store.Watch(ctx, DirentPrefix(parent), clientv3.WithPrefix())

	// Create files in a separate goroutine
	go func() {
		for i := 0; i < 10; i++ {
			_ = store.CreateDirent(context.Background(), parent, fmt.Sprintf("watch-%d", i), uint64(i+500))
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Collect at least one batch of events
	eventCount := 0
	timeout := time.After(15 * time.Second)
loop:
	for {
		select {
		case resp, ok := <-watchCh:
			if !ok {
				break loop
			}
			eventCount += len(resp.Events)
			t.Logf("received %d watch events (total: %d)", len(resp.Events), eventCount)
			if eventCount >= 10 {
				break loop
			}
		case <-timeout:
			break loop
		case <-ctx.Done():
			break loop
		}
	}

	assert.GreaterOrEqual(t, eventCount, 1, "should receive at least one watch event")
}

// ---- C1.11: Transaction conflict storm ----

func TestIntegration_TransactionConflictStorm(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()

	const concurrent = 50
	const key = "storm/counter"
	var successes int32

	// Initialise counter
	_, err := store.Put(ctx, key, EncodeUint64(0))
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// A flat 20-attempt budget with uniform backoff is not enough at
			// this fan-out: with 50 writers on one key a loser can lose every
			// race and exhaust the budget on a loaded runner. Retry until the
			// deadline instead, backing off exponentially so the herd thins.
			deadline := time.Now().Add(60 * time.Second)
			for backoff := time.Millisecond; time.Now().Before(deadline); {
				value, err := store.Get(ctx, key)
				if err != nil {
					continue
				}
				current := DecodeUint64(value)
				next := current + 1

				cmp := clientv3.Compare(clientv3.Value(key), "=", string(EncodeUint64(current)))
				op := clientv3.OpPut(key, string(EncodeUint64(next)))

				ok, err := store.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{op}, nil)
				if err == nil && ok {
					atomic.AddInt32(&successes, 1)
					return
				}
				time.Sleep(time.Duration(rand.Int63n(int64(backoff))) + time.Millisecond)
				if backoff < 200*time.Millisecond {
					backoff *= 2
				}
			}
		}()
	}
	wg.Wait()

	// Every goroutine should eventually succeed
	assert.Equal(t, int32(concurrent), successes, "all concurrent increments should eventually succeed")

	// Verify final value
	final, err := store.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, uint64(concurrent), DecodeUint64(final), "counter should equal number of successes")
}

// ---- C1.12: Large extent map ----

func TestIntegration_LargeExtentMap(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	ino := uint64(77777)

	const totalExtents = 200
	for i := 0; i < totalExtents; i++ {
		err := store.AppendExtent(ctx, ino,
			uint64(i)*4096,         // logical offset
			uint64(i)*4096+1000000, // disk offset
			4096,                   // length
			1)                      // generation
		require.NoError(t, err, "append extent %d", i)
	}

	// Read all extents back
	extents, err := store.GetExtents(ctx, ino)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(extents), totalExtents, "should have at least %d extents", totalExtents)

	// Verify first and last.  More than ten chunks exist, so this also
	// covers that GetExtents orders by logical offset and not by key.
	assert.Equal(t, uint64(0), extents[0].LogOff)
	assert.Equal(t, uint64(1000000), extents[0].DiskOff)
	assert.Equal(t, uint64((totalExtents-1)*4096), extents[totalExtents-1].LogOff)
}

// ---- Additional: Inode CRUD ----

func TestIntegration_InodeCRUD(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()

	// Create
	rec, err := store.CreateInode(ctx, 42, 0644, 1000, 1000)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), rec.Ino)
	assert.Equal(t, uint32(0644), rec.Mode)

	// Get
	rec, err = store.GetInode(ctx, 42)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, uint32(0644), rec.Mode)

	// Create duplicate should fail
	_, err = store.CreateInode(ctx, 42, 0644, 1000, 1000)
	assert.Error(t, err)

	// A new inode starts with one link; unlinking the only name that refers to
	// it is what removes the record.
	assert.EqualValues(t, 1, rec.Nlink)
	require.NoError(t, store.CreateDirent(ctx, RootIno, "inode-42", 42))
	require.NoError(t, store.AtomicUnlink(ctx, RootIno, "inode-42"))

	rec, err = store.GetInode(ctx, 42)
	require.NoError(t, err)
	assert.Nil(t, rec)
}

func TestIntegration_AtomicCreateFile(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(1)

	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	rec, err := store.AtomicCreateFile(ctx, parent, "test.txt", 100, 0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)
	assert.Equal(t, uint64(100), rec.Ino)
	assert.Equal(t, uint32(0644), rec.Mode)

	// Verify dirent exists
	ino, err := store.LookupDirent(ctx, parent, "test.txt")
	require.NoError(t, err)
	assert.Equal(t, uint64(100), ino)

	// Verify inode exists
	rec, err = store.GetInode(ctx, 100)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, uint32(1), rec.Nlink)
}

func TestIntegration_AtomicUnlinkFile(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(1)

	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	_, err = store.AtomicCreateFile(ctx, parent, "to-delete.txt", 200, 0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)

	err = store.AtomicUnlink(ctx, parent, "to-delete.txt")
	require.NoError(t, err)

	// Dirent should be gone
	ino, err := store.LookupDirent(ctx, parent, "to-delete.txt")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), ino)

	// Inode should be deleted (nlink reached 0)
	rec, err := store.GetInode(ctx, 200)
	require.NoError(t, err)
	assert.Nil(t, rec, "inode should be deleted when nlink reaches 0")
}

// ---- Concurrent test helper ----

func TestIntegration_ConcurrentCreatesNoCollision(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(1)

	_, err := store.CreateInode(ctx, parent, 0755|uint32(1<<31), 0, 0)
	require.NoError(t, err)

	const workers = 32
	const filesPerWorker = 10
	errCh := make(chan error, workers*filesPerWorker)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for f := 0; f < filesPerWorker; f++ {
				name := fmt.Sprintf("w%d-f%d", workerID, f)
				ino := uint64(workerID*1000 + f + 10000)
				_, err := store.AtomicCreateFile(ctx, parent, name, ino, 0644, 1000, 1000, CreateExtra{})
				errCh <- err
			}
		}(w)
	}
	wg.Wait()
	close(errCh)

	errors := 0
	for err := range errCh {
		if err != nil {
			t.Logf("create error: %v", err)
			errors++
		}
	}

	assert.Equal(t, 0, errors, "all concurrent creates should succeed without collisions")

	entries, err := store.ListDirents(ctx, parent)
	require.NoError(t, err)
	assert.Len(t, entries, workers*filesPerWorker)
}

// ---- Write operations ----

func TestIntegration_CreateFile(t *testing.T) {
	store := testStore(t, "phase3-create")
	ctx := context.Background()
	parent := uint64(3001)

	_, err := store.CreateInode(ctx, parent, 0755|ModeDir, 0, 0)
	require.NoError(t, err)

	// Create a file via AtomicCreateFile (Go handler path)
	rec, err := store.AtomicCreateFile(ctx, parent, "newfile.txt", 3010, 0100644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)
	assert.Equal(t, uint64(3010), rec.Ino)
	assert.Equal(t, uint32(0100644), rec.Mode)
	assert.Equal(t, uint32(1), rec.Nlink)

	// Verify dirent exists
	ino, err := store.LookupDirent(ctx, parent, "newfile.txt")
	require.NoError(t, err)
	assert.Equal(t, uint64(3010), ino)
}

func TestIntegration_Mkdir(t *testing.T) {
	store := testStore(t, "phase3-mkdir")
	ctx := context.Background()
	parent := uint64(3020)

	_, err := store.CreateInode(ctx, parent, 0755|ModeDir, 0, 0)
	require.NoError(t, err)

	rec, err := store.AtomicCreateDir(ctx, parent, "newdir", 3030, ModeDir|0755, 1000, 1000)
	require.NoError(t, err)
	assert.Equal(t, uint64(3030), rec.Ino)
	assert.Equal(t, uint32(2), rec.Nlink) // . and ..

	ino, err := store.LookupDirent(ctx, parent, "newdir")
	require.NoError(t, err)
	assert.Equal(t, uint64(3030), ino)
}

func TestIntegration_Unlink(t *testing.T) {
	store := testStore(t, "phase3-unlink")
	ctx := context.Background()
	parent := uint64(3100)

	_, err := store.CreateInode(ctx, parent, 0755|ModeDir, 0, 0)
	require.NoError(t, err)
	_, err = store.AtomicCreateFile(ctx, parent, "todelete.txt", 3101, 0100644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)

	err = store.AtomicUnlink(ctx, parent, "todelete.txt")
	require.NoError(t, err)

	ino, err := store.LookupDirent(ctx, parent, "todelete.txt")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), ino)

	rec, err := store.GetInode(ctx, 3101)
	require.NoError(t, err)
	assert.Nil(t, rec, "inode deleted when nlink reaches 0")
}

func TestIntegration_Rmdir(t *testing.T) {
	store := testStore(t, "phase3-rmdir")
	ctx := context.Background()
	parent := uint64(3120)

	_, err := store.CreateInode(ctx, parent, 0755|ModeDir, 0, 0)
	require.NoError(t, err)
	_, err = store.AtomicCreateDir(ctx, parent, "emptydir", 3121, ModeDir|0755, 1000, 1000)
	require.NoError(t, err)

	err = store.AtomicUnlink(ctx, parent, "emptydir")
	require.NoError(t, err)

	ino, err := store.LookupDirent(ctx, parent, "emptydir")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), ino)
}

func TestIntegration_Rename(t *testing.T) {
	store := testStore(t, "phase3-rename")
	ctx := context.Background()
	parent := uint64(3130)

	_, err := store.CreateInode(ctx, parent, 0755|ModeDir, 0, 0)
	require.NoError(t, err)
	_, err = store.AtomicCreateFile(ctx, parent, "oldname.txt", 3131, 0100644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)

	err = store.AtomicRename(ctx, parent, "oldname.txt", parent, "newname.txt", 3131, 0)
	require.NoError(t, err)

	oldIno, err := store.LookupDirent(ctx, parent, "oldname.txt")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), oldIno)

	newIno, err := store.LookupDirent(ctx, parent, "newname.txt")
	require.NoError(t, err)
	assert.Equal(t, uint64(3131), newIno)
}

func TestIntegration_WriteInodeSize(t *testing.T) {
	store := testStore(t, "phase3-write")
	ctx := context.Background()

	// Create inode
	_, err := store.CreateInode(ctx, 3201, 0100644, 1000, 1000)
	require.NoError(t, err)

	// Simulate write: update size
	rec, err := store.GetInode(ctx, 3201)
	require.NoError(t, err)
	rec.Size = 4096
	_, err = store.Put(ctx, InodeKey(3201), EncodeInode(rec))
	require.NoError(t, err)

	rec, err = store.GetInode(ctx, 3201)
	require.NoError(t, err)
	assert.Equal(t, uint64(4096), rec.Size)
}

func TestIntegration_Symlink(t *testing.T) {
	store := testStore(t, "phase3-symlink")
	ctx := context.Background()
	parent := uint64(3300)

	_, err := store.CreateInode(ctx, parent, 0755|ModeDir, 0, 0)
	require.NoError(t, err)

	// Create symlink inode
	_, err = store.CreateInode(ctx, 3301, ModeSymlink|0777, 1000, 1000)
	require.NoError(t, err)

	// Store target
	_, err = store.Put(ctx, InodeSymlinkKey(3301), []byte("target.txt"))
	require.NoError(t, err)

	// Create dirent
	err = store.CreateDirent(ctx, parent, "mylink", 3301)
	require.NoError(t, err)

	// Verify
	ino, err := store.LookupDirent(ctx, parent, "mylink")
	require.NoError(t, err)
	assert.Equal(t, uint64(3301), ino)

	target, err := store.Get(ctx, InodeSymlinkKey(3301))
	require.NoError(t, err)
	assert.Equal(t, "target.txt", string(target))
}

func TestIntegration_Link(t *testing.T) {
	store := testStore(t, "phase3-link")
	ctx := context.Background()
	parent := uint64(3400)

	_, err := store.CreateInode(ctx, parent, 0755|ModeDir, 0, 0)
	require.NoError(t, err)
	_, err = store.AtomicCreateFile(ctx, parent, "original.txt", 3401, 0100644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)

	// Hard link
	_, err = store.AtomicLink(ctx, 3401, parent, "hardlink.txt")
	require.NoError(t, err)

	rec, err := store.GetInode(ctx, 3401)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), rec.Nlink)

	// Both dirents point to same inode
	ino1, _ := store.LookupDirent(ctx, parent, "original.txt")
	ino2, _ := store.LookupDirent(ctx, parent, "hardlink.txt")
	assert.Equal(t, uint64(3401), ino1)
	assert.Equal(t, uint64(3401), ino2)
}

func TestIntegration_FsyncDurability(t *testing.T) {
	store := testStore(t, "phase3-fsync")
	ctx := context.Background()

	_, err := store.CreateInode(ctx, 3501, 0100644, 1000, 1000)
	require.NoError(t, err)

	// Simulate write + fsync: update then verify
	rec, err := store.GetInode(ctx, 3501)
	require.NoError(t, err)
	rec.Size = 8192
	_, err = store.Put(ctx, InodeKey(3501), EncodeInode(rec))
	require.NoError(t, err)

	// Verify data persists (simulating fsync durability)
	rec2, err := store.GetInode(ctx, 3501)
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), rec2.Size)
}

func TestIntegration_TruncateToZero(t *testing.T) {
	store := testStore(t, "phase3-truncate")
	ctx := context.Background()

	_, err := store.CreateInode(ctx, 3601, 0100644, 1000, 1000)
	require.NoError(t, err)

	// Write some data
	rec, err := store.GetInode(ctx, 3601)
	require.NoError(t, err)
	rec.Size = 4096
	_, err = store.Put(ctx, InodeKey(3601), EncodeInode(rec))
	require.NoError(t, err)

	// Truncate to 0
	rec.Size = 0
	_, err = store.Put(ctx, InodeKey(3601), EncodeInode(rec))
	require.NoError(t, err)

	rec, err = store.GetInode(ctx, 3601)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), rec.Size)
}

func TestIntegration_DeepMkdir(t *testing.T) {
	store := testStore(t, "phase3-deepdir")
	ctx := context.Background()
	parent := uint64(3700)

	_, err := store.CreateInode(ctx, parent, 0755|ModeDir, 0, 0)
	require.NoError(t, err)

	current := parent
	nextIno := uint64(3710)
	for depth := 0; depth < 5; depth++ {
		rec, err := store.AtomicCreateDir(ctx, current, fmt.Sprintf("d%d", depth),
			nextIno, ModeDir|0755, 1000, 1000)
		require.NoError(t, err)
		assert.Equal(t, uint32(2), rec.Nlink)
		current = nextIno
		nextIno++
	}

	assert.Equal(t, uint64(3715), nextIno)
}

// ---- rename over an existing target ----

// renameFixture seeds a parent directory holding one file per name given.
func renameFixture(t *testing.T, s *Store, parent uint64, names ...string) map[string]uint64 {
	t.Helper()
	ctx := context.Background()
	inos := make(map[string]uint64, len(names))
	for i, name := range names {
		ino := parent*1000 + uint64(i) + 1
		_, err := s.AtomicCreateFile(ctx, parent, name, ino, ModeFile|0644, 1000, 1000, CreateExtra{})
		require.NoError(t, err, "seed %s", name)
		inos[name] = ino
	}
	return inos
}

// Replacing a name is an unlink of whatever was there. Leaving that out is what
// orphaned the target: its inode stayed in etcd with nothing pointing at it.
func TestIntegration_RenameOverFileUnlinksTheTarget(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent = 8100

	inos := renameFixture(t, s, parent, "src", "victim")

	require.NoError(t, s.AtomicRename(ctx, parent, "src", parent, "victim", inos["src"], 0))

	got, err := s.LookupDirent(ctx, parent, "victim")
	require.NoError(t, err)
	assert.Equal(t, inos["src"], got, "the name must now resolve to the source inode")

	gone, err := s.LookupDirent(ctx, parent, "src")
	require.NoError(t, err)
	assert.Zero(t, gone, "the source name must be gone")

	victim, err := s.GetInode(ctx, inos["victim"])
	require.NoError(t, err)
	assert.Nil(t, victim, "the replaced inode must be deleted, not orphaned")
}

// A replaced file with another link left keeps its inode; only the count drops.
func TestIntegration_RenameOverHardlinkedFileOnlyDropsNlink(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent = 8200

	inos := renameFixture(t, s, parent, "src", "victim")
	_, err := s.AtomicLink(ctx, inos["victim"], parent, "victim-link")
	require.NoError(t, err)

	require.NoError(t, s.AtomicRename(ctx, parent, "src", parent, "victim", inos["src"], 0))

	victim, err := s.GetInode(ctx, inos["victim"])
	require.NoError(t, err)
	require.NotNil(t, victim, "an inode with a surviving link must not be deleted")
	assert.EqualValues(t, 1, victim.Nlink)
}

func TestIntegration_RenameRejectsUnsupportedAndIllegalCases(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent = 8300

	inos := renameFixture(t, s, parent, "src", "taken")

	// Exchange is not implemented, and must not silently degrade to a rename
	// that deletes the source and overwrites the target.
	err := s.AtomicRename(ctx, parent, "src", parent, "taken", inos["src"], RenameExchange)
	assert.ErrorIs(t, err, ErrInvalid)

	err = s.AtomicRename(ctx, parent, "src", parent, "taken", inos["src"], RenameNoReplace)
	assert.ErrorIs(t, err, ErrExists)

	// Both names survive an aborted rename.
	for _, name := range []string{"src", "taken"} {
		got, lerr := s.LookupDirent(ctx, parent, name)
		require.NoError(t, lerr)
		assert.NotZero(t, got, "%s must survive the rejected rename", name)
	}

	// A file may not replace a directory, nor a directory a file.
	const dirIno = 8399
	_, err = s.AtomicCreateDir(ctx, parent, "adir", dirIno, 0755, 1000, 1000)
	require.NoError(t, err)

	err = s.AtomicRename(ctx, parent, "src", parent, "adir", inos["src"], 0)
	assert.ErrorIs(t, err, ErrIsDir)

	err = s.AtomicRename(ctx, parent, "adir", parent, "src", dirIno, 0)
	assert.ErrorIs(t, err, ErrNotDir)
}

func TestIntegration_RenameRejectsNonEmptyDirectoryTarget(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent, srcDir, dstDir = 8400, 8401, 8402

	_, err := s.AtomicCreateDir(ctx, parent, "src", srcDir, 0755, 1000, 1000)
	require.NoError(t, err)
	_, err = s.AtomicCreateDir(ctx, parent, "dst", dstDir, 0755, 1000, 1000)
	require.NoError(t, err)
	_, err = s.AtomicCreateFile(ctx, dstDir, "occupant", 8403, ModeFile|0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)

	err = s.AtomicRename(ctx, parent, "src", parent, "dst", srcDir, 0)
	assert.ErrorIs(t, err, ErrNotEmpty)

	// Emptying it makes the same rename legal.
	require.NoError(t, s.AtomicUnlink(ctx, dstDir, "occupant"))
	require.NoError(t, s.AtomicRename(ctx, parent, "src", parent, "dst", srcDir, 0))
}

// Moving a directory beneath itself detaches its whole subtree: the entries
// remain, but no path from the root reaches them.
func TestIntegration_RenameRejectsDirectoryIntoItsOwnSubtree(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent, top, mid, deep = 8500, 8501, 8502, 8503

	_, err := s.AtomicCreateDir(ctx, parent, "top", top, 0755, 1000, 1000)
	require.NoError(t, err)
	_, err = s.AtomicCreateDir(ctx, top, "mid", mid, 0755, 1000, 1000)
	require.NoError(t, err)
	_, err = s.AtomicCreateDir(ctx, mid, "deep", deep, 0755, 1000, 1000)
	require.NoError(t, err)

	// Directly into its own child, and into a grandchild.
	assert.ErrorIs(t, s.AtomicRename(ctx, parent, "top", top, "loop", top, 0), ErrInvalid)
	assert.ErrorIs(t, s.AtomicRename(ctx, parent, "top", deep, "loop", top, 0), ErrInvalid)

	// The directory is still where it was.
	got, err := s.LookupDirent(ctx, parent, "top")
	require.NoError(t, err)
	assert.EqualValues(t, top, got)

	// Moving a *sibling* subtree in is fine — it is not an ancestor of itself.
	_, err = s.AtomicCreateDir(ctx, parent, "other", 8504, 0755, 1000, 1000)
	require.NoError(t, err)
	assert.NoError(t, s.AtomicRename(ctx, parent, "other", deep, "other", 8504, 0))
}

// ---- initial link counts ----

// A dirent points at every inode these paths create, so each must be stored
// with a link count that says so. Symlinks, device nodes and hardlink targets
// all go through CreateInode, which used to write 0.
func TestIntegration_CreatedInodesCarryTheirLinkCount(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent = 9100

	_, err := s.AtomicCreateDir(ctx, RootIno, "nlink-parent", parent, 0755, 1000, 1000)
	require.NoError(t, err)

	cases := []struct {
		name  string
		ino   uint64
		mode  uint32
		nlink uint32
	}{
		{"symlink", 9101, ModeSymlink | 0777, 1},
		{"chardev", 9102, 0020000 | 0644, 1},
		{"regular", 9103, ModeFile | 0644, 1},
		{"subdir", 9104, ModeDir | 0755, 2},
	}
	for _, c := range cases {
		_, cerr := s.CreateInode(ctx, c.ino, c.mode, 1000, 1000)
		require.NoError(t, cerr, c.name)
		require.NoError(t, s.CreateDirent(ctx, parent, c.name, c.ino))

		rec, gerr := s.GetInode(ctx, c.ino)
		require.NoError(t, gerr, c.name)
		require.NotNil(t, rec, c.name)
		assert.Equal(t, c.nlink, rec.Nlink, "%s link count", c.name)
	}

	// A hard link raises the count, and unlinking one name lowers it again
	// without removing the inode.
	_, err = s.AtomicLink(ctx, 9103, parent, "regular-link")
	require.NoError(t, err)
	rec, err := s.GetInode(ctx, 9103)
	require.NoError(t, err)
	assert.EqualValues(t, 2, rec.Nlink)

	require.NoError(t, s.AtomicUnlink(ctx, parent, "regular-link"))
	rec, err = s.GetInode(ctx, 9103)
	require.NoError(t, err)
	require.NotNil(t, rec, "an inode with a surviving link must not be deleted")
	assert.EqualValues(t, 1, rec.Nlink)
}

// ---- shared locks ----

// Two readers must be able to hold the same inode at once. They could not
// before: the lock lived in one key, and the shared-join path decided whether
// to reuse it by parsing the mode out of the value with Sscanf, which never
// matched — so every reader took the "no lock exists" branch and the second one
// was refused.
func TestIntegration_SharedLocksAreHeldConcurrently(t *testing.T) {
	ctx := context.Background()
	first := testStore(t, "reader-1")
	second := NewStore(first.Client(), "reader-2")
	const ino = 9200

	holderA, err := first.AcquireLock(ctx, ino, LockShared, 10*time.Second)
	require.NoError(t, err)
	holderB, err := second.AcquireLock(ctx, ino, LockShared, 10*time.Second)
	require.NoError(t, err, "a second reader must not be refused")
	require.NotEqual(t, holderA, holderB, "each holder needs its own key")

	rec, err := first.GetLockInfo(ctx, ino)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, string(LockShared), rec.Mode)
	assert.ElementsMatch(t, []string{"reader-1", "reader-2"}, rec.Holders)

	// One reader leaving must not drop the lock for the other. Revoking the
	// single shared key is exactly what the old scheme did.
	mustReleaseLock(t, first, ctx, ino, LockShared, holderA)

	rec, err = second.GetLockInfo(ctx, ino)
	require.NoError(t, err)
	require.NotNil(t, rec, "the surviving reader still holds the lock")
	assert.Equal(t, []string{"reader-2"}, rec.Holders)

	mustReleaseLock(t, second, ctx, ino, LockShared, holderB)
	locked, err := second.IsLocked(ctx, ino)
	require.NoError(t, err)
	assert.False(t, locked, "the lock is gone once the last holder leaves")
}

func TestIntegration_SharedAndExclusiveLocksExcludeEachOther(t *testing.T) {
	ctx := context.Background()
	reader := testStore(t, "reader")
	writer := NewStore(reader.Client(), "writer")

	// A reader blocks a writer.
	const sharedFirst = 9210
	readHolder, err := reader.AcquireLock(ctx, sharedFirst, LockShared, 10*time.Second)
	require.NoError(t, err)
	_, err = writer.AcquireLock(ctx, sharedFirst, LockExclusive, 2*time.Second)
	assert.ErrorIs(t, err, ErrConflict, "a writer must wait for readers")

	mustReleaseLock(t, reader, ctx, sharedFirst, LockShared, readHolder)
	writeHolder, err := writer.AcquireLock(ctx, sharedFirst, LockExclusive, 10*time.Second)
	require.NoError(t, err, "the writer proceeds once the reader is gone")
	mustReleaseLock(t, writer, ctx, sharedFirst, LockExclusive, writeHolder)

	// And a writer blocks both readers and other writers.
	const exclusiveFirst = 9211
	writeHolder, err = writer.AcquireLock(ctx, exclusiveFirst, LockExclusive, 10*time.Second)
	require.NoError(t, err)

	_, err = reader.AcquireLock(ctx, exclusiveFirst, LockShared, 2*time.Second)
	assert.ErrorIs(t, err, ErrConflict, "a reader must not join an exclusive hold")
	_, err = reader.AcquireLock(ctx, exclusiveFirst, LockExclusive, 2*time.Second)
	assert.ErrorIs(t, err, ErrConflict, "two writers must not both hold it")

	rec, err := reader.GetLockInfo(ctx, exclusiveFirst)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, string(LockExclusive), rec.Mode)

	mustReleaseLock(t, writer, ctx, exclusiveFirst, LockExclusive, writeHolder)
}

// A failed acquisition must not leave its lease behind holding a key.
func TestIntegration_RefusedLockLeavesNothingBehind(t *testing.T) {
	ctx := context.Background()
	writer := testStore(t, "writer")
	other := NewStore(writer.Client(), "other")
	const ino = 9220

	holder, err := writer.AcquireLock(ctx, ino, LockExclusive, 10*time.Second)
	require.NoError(t, err)

	_, err = other.AcquireLock(ctx, ino, LockExclusive, 10*time.Second)
	require.ErrorIs(t, err, ErrConflict)

	rec, err := writer.GetLockInfo(ctx, ino)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, []string{"writer"}, rec.Holders, "the refused acquirer left no key")

	mustReleaseLock(t, writer, ctx, ino, LockExclusive, holder)
}

// ---- lost-update protection on nlink ----

// Concurrent hard links to one inode must all be counted. Before the inode was
// pinned to the revision it was read at, the transaction only proved the record
// existed, so every writer read the same count and the last one to commit
// silently erased the rest.
func TestIntegration_ConcurrentHardLinksAllCount(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent, ino, links = 9300, 9301, 16

	_, err := s.AtomicCreateDir(ctx, RootIno, "nlink-race", parent, 0755, 1000, 1000)
	require.NoError(t, err)
	_, err = s.AtomicCreateFile(ctx, parent, "target", ino, ModeFile|0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, links)
	for i := 0; i < links; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, lerr := s.AtomicLink(ctx, ino, parent, fmt.Sprintf("link-%d", i))
			errs <- lerr
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}

	rec, err := s.GetInode(ctx, ino)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.EqualValues(t, 1+links, rec.Nlink, "every concurrent link must be counted")

	entries, err := s.ListDirents(ctx, parent)
	require.NoError(t, err)
	assert.Len(t, entries, 1+links, "and every name must exist")
}

// The mirror case: concurrent unlinks of many names for one inode must land on
// exactly zero and delete it. Each used to read the same count and write the
// same lower value, leaving the inode referenced by nothing but never freed.
func TestIntegration_ConcurrentUnlinksReachZeroAndFreeTheInode(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent, ino, names = 9400, 9401, 12

	_, err := s.AtomicCreateDir(ctx, RootIno, "unlink-race", parent, 0755, 1000, 1000)
	require.NoError(t, err)
	_, err = s.AtomicCreateFile(ctx, parent, "name-0", ino, ModeFile|0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)
	for i := 1; i < names; i++ {
		_, lerr := s.AtomicLink(ctx, ino, parent, fmt.Sprintf("name-%d", i))
		require.NoError(t, lerr)
	}

	rec, err := s.GetInode(ctx, ino)
	require.NoError(t, err)
	require.EqualValues(t, names, rec.Nlink)

	var wg sync.WaitGroup
	errs := make(chan error, names)
	for i := 0; i < names; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- s.AtomicUnlink(ctx, parent, fmt.Sprintf("name-%d", i))
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}

	rec, err = s.GetInode(ctx, ino)
	require.NoError(t, err)
	assert.Nil(t, rec, "the inode must be deleted once the last name is gone")

	entries, err := s.ListDirents(ctx, parent)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// A rename replacing a target reads that target's link count before writing it
// back, so it has to abort if the count moves underneath it.
func TestIntegration_RenameOverTargetPinsTheVictimInode(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent, srcIno, victimIno = 9500, 9501, 9502

	_, err := s.AtomicCreateDir(ctx, RootIno, "rename-pin", parent, 0755, 1000, 1000)
	require.NoError(t, err)
	_, err = s.AtomicCreateFile(ctx, parent, "src", srcIno, ModeFile|0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)
	_, err = s.AtomicCreateFile(ctx, parent, "victim", victimIno, ModeFile|0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)

	// Racing hard links against the replacement: whichever order they land in,
	// the victim's count must stay consistent with the names pointing at it.
	var wg sync.WaitGroup
	wg.Add(2)
	var renameErr error
	go func() {
		defer wg.Done()
		renameErr = s.AtomicRename(ctx, parent, "src", parent, "victim", srcIno, 0)
	}()
	go func() {
		defer wg.Done()
		_, _ = s.AtomicLink(ctx, victimIno, parent, "victim-link")
	}()
	wg.Wait()

	if renameErr != nil {
		// Aborting is the correct outcome when the victim moved first.
		assert.ErrorIs(t, renameErr, ErrConflict)
		return
	}

	// The rename won. Its own name now resolves to the source, and the victim
	// survives only if the concurrent link got in first.
	got, err := s.LookupDirent(ctx, parent, "victim")
	require.NoError(t, err)
	assert.EqualValues(t, srcIno, got)

	victim, err := s.GetInode(ctx, victimIno)
	require.NoError(t, err)
	linked, err := s.LookupDirent(ctx, parent, "victim-link")
	require.NoError(t, err)
	if linked == 0 {
		assert.Nil(t, victim, "no surviving name, so the inode must be gone")
	} else {
		require.NotNil(t, victim, "a surviving name means the inode must remain")
		assert.EqualValues(t, 1, victim.Nlink)
	}
}

// ---- atomic creation ----

// Every creating operation publishes the inode and the name that reaches it in
// one transaction. Symlink, mknod and link used to be two or three round trips,
// so a failure between them left an inode nothing could name — invisible to the
// orphan check, which looks for extents without inodes, not the reverse.
func TestIntegration_CreationsPublishInodeAndNameTogether(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent = 9500

	_, err := s.AtomicCreateDir(ctx, RootIno, "atomic-create", parent, 0755, 1000, 1000)
	require.NoError(t, err)

	link, err := s.AtomicCreateSymlink(ctx, parent, "link", 9501, "../target", 1000, 1000)
	require.NoError(t, err)
	assert.EqualValues(t, len("../target"), link.Size)
	target, err := s.Get(ctx, InodeSymlinkKey(9501))
	require.NoError(t, err)
	assert.Equal(t, "../target", string(target))

	node, err := s.AtomicCreateNode(ctx, parent, "dev", 9502, 0020000|0644, 0x0103, 1000, 1000)
	require.NoError(t, err)
	assert.EqualValues(t, 0x0103, node.Rdev)

	for name, ino := range map[string]uint64{"link": 9501, "dev": 9502} {
		got, lerr := s.LookupDirent(ctx, parent, name)
		require.NoError(t, lerr)
		assert.Equal(t, ino, got, name)
		rec, gerr := s.GetInode(ctx, ino)
		require.NoError(t, gerr)
		require.NotNil(t, rec, name)
		assert.EqualValues(t, 1, rec.Nlink, "%s link count", name)
	}

	// A create that loses the race for the name writes nothing at all: its
	// inode number stays unused rather than becoming an unreachable record.
	_, err = s.AtomicCreateSymlink(ctx, parent, "link", 9503, "elsewhere", 1000, 1000)
	require.ErrorIs(t, err, ErrExists)
	rec, err := s.GetInode(ctx, 9503)
	require.NoError(t, err)
	assert.Nil(t, rec, "a refused create must leave no inode behind")
}

// A hard link that cannot take its name must not leave the link count raised:
// the count and the dirent commit together or not at all.
func TestIntegration_RefusedLinkLeavesNlinkAlone(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent = 9600

	_, err := s.AtomicCreateDir(ctx, RootIno, "atomic-link", parent, 0755, 1000, 1000)
	require.NoError(t, err)
	_, err = s.AtomicCreateFile(ctx, parent, "file", 9601, ModeFile|0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)
	_, err = s.AtomicCreateFile(ctx, parent, "taken", 9602, ModeFile|0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)

	_, err = s.AtomicLink(ctx, 9601, parent, "taken")
	require.ErrorIs(t, err, ErrExists)

	rec, err := s.GetInode(ctx, 9601)
	require.NoError(t, err)
	assert.EqualValues(t, 1, rec.Nlink, "a refused link must not raise the count")

	// A hard link to a directory would let the namespace form a cycle no unlink
	// can break.
	_, err = s.AtomicLink(ctx, parent, parent, "self")
	require.ErrorIs(t, err, ErrPerm)
}

// ---- rmdir ----

// Emptiness has to be asserted by the transaction that removes the directory.
// Checked beforehand and committed afterwards, another node can create an entry
// in between, and the subtree is stranded: the parent's name is gone and
// nothing reaches the children.
func TestIntegration_RmdirRefusesADirectoryFilledUnderIt(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent, child = 9700, 9701

	_, err := s.AtomicCreateDir(ctx, RootIno, "rmdir-parent", parent, 0755, 1000, 1000)
	require.NoError(t, err)
	_, err = s.AtomicCreateDir(ctx, parent, "victim", child, 0755, 1000, 1000)
	require.NoError(t, err)

	require.NoError(t, s.CreateDirent(ctx, child, "surprise", 9702))
	require.ErrorIs(t, s.AtomicRmdir(ctx, parent, "victim"), ErrNotEmpty)

	rec, err := s.GetInode(ctx, child)
	require.NoError(t, err)
	require.NotNil(t, rec, "a refused rmdir must leave the directory in place")

	require.NoError(t, s.RemoveDirent(ctx, child, "surprise"))
	require.NoError(t, s.AtomicRmdir(ctx, parent, "victim"))

	rec, err = s.GetInode(ctx, child)
	require.NoError(t, err)
	assert.Nil(t, rec, "an emptied directory is removed outright, not decremented")

	// A file is not a directory, whatever the caller asked for.
	_, err = s.AtomicCreateFile(ctx, parent, "file", 9703, ModeFile|0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)
	require.ErrorIs(t, s.AtomicRmdir(ctx, parent, "file"), ErrNotDir)
}

// A symlink's target lives in a key of its own, which nothing else references
// and no check looks for. Unlinking the symlink has to take it along.
func TestIntegration_UnlinkRemovesTheSymlinkTarget(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, "node-A")
	const parent = 9800

	_, err := s.AtomicCreateDir(ctx, RootIno, "symlink-unlink", parent, 0755, 1000, 1000)
	require.NoError(t, err)
	_, err = s.AtomicCreateSymlink(ctx, parent, "link", 9801, "../target", 1000, 1000)
	require.NoError(t, err)

	require.NoError(t, s.AtomicUnlink(ctx, parent, "link"))

	target, err := s.Get(ctx, InodeSymlinkKey(9801))
	require.NoError(t, err)
	assert.Nil(t, target, "the target key must go with the inode")
}

// ---- Directory timestamps ----

// Adding or removing an entry has to mark the containing directory changed:
// every namespace operation used to leave the parent record untouched, so
// anything that watches a directory's mtime (make, rsync) never saw new files.
func TestIntegration_NamespaceOpsTouchTheParentDirectory(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(1)

	_, err := store.CreateInode(ctx, parent, ModeDir|0755, 0, 0)
	require.NoError(t, err)

	// Backdate the directory so the update is visible despite the one-second
	// resolution of the stored timestamps.
	backdate := func() {
		rec, err := store.GetInode(ctx, parent)
		require.NoError(t, err)
		rec.Mtime = time.Now().Add(-time.Hour)
		rec.Ctime = rec.Mtime
		_, err = store.Put(ctx, InodeKey(parent), EncodeInode(rec))
		require.NoError(t, err)
	}
	assertTouched := func(what string) {
		rec, err := store.GetInode(ctx, parent)
		require.NoError(t, err)
		assert.WithinDuration(t, time.Now(), rec.Mtime, time.Minute, "%s left the parent mtime alone", what)
		assert.WithinDuration(t, time.Now(), rec.Ctime, time.Minute, "%s left the parent ctime alone", what)
	}

	backdate()
	_, err = store.AtomicCreateFile(ctx, parent, "f", 900, ModeFile|0644, 0, 0, CreateExtra{})
	require.NoError(t, err)
	assertTouched("create")

	backdate()
	_, err = store.AtomicLink(ctx, 900, parent, "f-link")
	require.NoError(t, err)
	assertTouched("link")

	backdate()
	require.NoError(t, store.AtomicRename(ctx, parent, "f-link", parent, "f-moved", 900, 0))
	assertTouched("rename")

	backdate()
	require.NoError(t, store.AtomicUnlink(ctx, parent, "f-moved"))
	assertTouched("unlink")

	// Removing one of two names is a status change of the file itself.
	rec, err := store.GetInode(ctx, 900)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), rec.Ctime, time.Minute, "unlink left the target ctime alone")
}

// A directory is referred to by the ".." of every subdirectory it holds, so its
// link count moves as subdirectories arrive and leave. It used to be fixed at
// 2 for the directory's whole life.
func TestIntegration_DirectoryNlinkCountsSubdirectories(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(1)

	_, err := store.CreateInode(ctx, parent, ModeDir|0755, 0, 0)
	require.NoError(t, err)

	nlink := func(ino uint64) uint32 {
		rec, err := store.GetInode(ctx, ino)
		require.NoError(t, err)
		require.NotNil(t, rec)
		return rec.Nlink
	}

	_, err = store.AtomicCreateDir(ctx, parent, "a", 800, 0755, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), nlink(parent), "parent gained a subdirectory")
	assert.Equal(t, uint32(2), nlink(800), "a new directory holds none of its own")

	// A file is not a subdirectory and must leave the count alone — this is
	// also the path that must stay free of a pin on the parent.
	_, err = store.AtomicCreateFile(ctx, parent, "f", 801, ModeFile|0644, 0, 0, CreateExtra{})
	require.NoError(t, err)
	assert.Equal(t, uint32(3), nlink(parent), "a file changed the parent's count")

	_, err = store.AtomicCreateDir(ctx, 800, "b", 802, 0755, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), nlink(800))
	assert.Equal(t, uint32(3), nlink(parent), "a grandchild moved the grandparent's count")

	// Moving a directory takes its reference with it.
	require.NoError(t, store.AtomicRename(ctx, 800, "b", parent, "b", 802, 0))
	assert.Equal(t, uint32(2), nlink(800), "the old parent kept the reference")
	assert.Equal(t, uint32(4), nlink(parent), "the new parent did not gain it")

	require.NoError(t, store.AtomicRmdir(ctx, parent, "b"))
	assert.Equal(t, uint32(3), nlink(parent), "rmdir left the count behind")

	// Renaming within one directory moves nothing.
	require.NoError(t, store.AtomicRename(ctx, parent, "a", parent, "a2", 800, 0))
	assert.Equal(t, uint32(3), nlink(parent))
}

// Concurrent mkdirs in one directory each have to land: they contend on the
// parent's record, and a lost increment would be a permanently wrong count.
func TestIntegration_ConcurrentMkdirKeepsTheParentCount(t *testing.T) {
	store := testStore(t, "test-node")
	ctx := context.Background()
	parent := uint64(1)

	_, err := store.CreateInode(ctx, parent, ModeDir|0755, 0, 0)
	require.NoError(t, err)

	const concurrent = 8
	var wg sync.WaitGroup
	errs := make(chan error, concurrent)
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := store.AtomicCreateDir(ctx, parent, fmt.Sprintf("d-%d", id), uint64(850+id), 0755, 0, 0)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	rec, err := store.GetInode(ctx, parent)
	require.NoError(t, err)
	assert.Equal(t, uint32(2+concurrent), rec.Nlink, "an increment was lost to contention")
}

// mustReleaseLock releases a lock the test just took, asserting both that the
// call succeeded and that the key was still there to release — the second is
// what tells a genuine release apart from a lease that had already dropped it.
func mustReleaseLock(t *testing.T, s *Store, ctx context.Context, ino uint64, mode LockMode, holder string) {
	t.Helper()
	released, err := s.ReleaseLock(ctx, ino, mode, holder)
	require.NoError(t, err)
	require.True(t, released, "the lock key was already gone")
}
