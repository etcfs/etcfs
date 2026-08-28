package ipc

import (
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/internal/config"
	"github.com/etcfs/etcfs/pkg/metadata"
)

func testLogger() *config.Logger { return config.NewLogger(0) }

// The whole point of caching a lock is that a second acquisition of a mode the
// cached one already satisfies costs no etcd round trip — and that an exclusive
// lock satisfies a read, so a read-modify-write sequence does not flap the key
// between modes at a Raft commit each way.
func TestCoveredLockModes(t *testing.T) {
	cases := []struct {
		held, want metadata.LockMode
		covered    bool
	}{
		{metadata.LockExclusive, metadata.LockExclusive, true},
		{metadata.LockExclusive, metadata.LockShared, true},
		{metadata.LockShared, metadata.LockShared, true},
		{metadata.LockShared, metadata.LockExclusive, false},
	}
	for _, c := range cases {
		if got := covers(c.held, c.want); got != c.covered {
			t.Errorf("covers(%s, %s) = %v, want %v", c.held, c.want, got, c.covered)
		}
	}
}

// A released lock has to leave the entry acquirable again, in either mode.  The
// hold is now a node-local RWMutex rather than an etcd key, so a leaked unlock
// deadlocks every later operation on that inode instead of merely costing a
// round trip.
func TestLocalLockReleaseIsIdempotent(t *testing.T) {
	e := &lockEntry{ino: 7}

	lk := &heldLock{e: e, mode: metadata.LockExclusive}
	e.rw.Lock()

	lk.Release()
	lk.Release() // a deferred Release next to an explicit one must be harmless

	if !e.rw.TryLock() {
		t.Fatal("exclusive lock still held after release")
	}
	e.rw.Unlock()
}

// A shared hold must release as a reader: unlocking it as a writer panics, and
// releasing only one of two concurrent readers must leave the other's hold
// standing.
func TestSharedLocksReleaseIndependently(t *testing.T) {
	e := &lockEntry{ino: 7}

	first := &heldLock{e: e, mode: metadata.LockShared}
	second := &heldLock{e: e, mode: metadata.LockShared}
	e.rw.RLock()
	e.rw.RLock()

	first.Release()
	if e.rw.TryLock() {
		t.Fatal("exclusive lock taken while a reader still holds the inode")
	}

	second.Release()
	if !e.rw.TryLock() {
		t.Fatal("inode still locked after the last reader released")
	}
	e.rw.Unlock()
}

// An eviction must not take a lock out of the cache while an operation is using
// it: the entry is the node-local exclusion, so dropping a busy one would let a
// second operation acquire a fresh entry for the same inode and run alongside.
func TestEvictionSkipsBusyEntries(t *testing.T) {
	s := &Service{locks: newLockMap(func(es []*lockEntry, _ string) []*lockEntry { return es })}

	// The oldest entry is the one eviction would pick first, and it is busy.
	busy := &lockEntry{ino: 1, lastUsed: time.Unix(0, 0)}
	busy.rw.Lock()
	s.locks.entries[1] = busy
	for ino := uint64(2); ino <= lockCacheMax; ino++ {
		s.locks.entries[ino] = &lockEntry{ino: ino, lastUsed: time.Unix(int64(ino), 0)}
	}

	s.locks.mu.Lock()
	s.locks.evictLocked()
	s.locks.mu.Unlock()

	if _, ok := s.locks.entries[1]; !ok {
		t.Fatal("an inode with an operation in flight was evicted from the lock cache")
	}
	if len(s.locks.entries) >= lockCacheMax {
		t.Fatalf("cache still at %d entries, eviction made no room", len(s.locks.entries))
	}
	// A sweep gives up a whole batch, so the next lockEvictBatch-1 inodes cost
	// no commit at all — which is the point of batching them.
	if want := lockCacheMax - lockEvictBatch; len(s.locks.entries) != want {
		t.Fatalf("cache at %d entries after one sweep, want %d", len(s.locks.entries), want)
	}
	busy.rw.Unlock()
}

// A recall must demote the entry, not remove it.  Removing it lets the next
// caller build a second entry for the same inode and take a different mutex,
// so two of this node's own operations would run against one inode believing
// each holds it — the exclusion the cached etcd key no longer provides.
func TestRecallKeepsTheEntryInTheCache(t *testing.T) {
	s := &Service{locks: newLockMap(func(es []*lockEntry, _ string) []*lockEntry { return es }), log: testLogger()}
	e := s.locks.entryFor(7)

	s.recallLock(7)

	if got := s.locks.entryFor(7); got != e {
		t.Fatal("recall replaced the cache entry; the node-local lock no longer excludes anything")
	}
	if e.holder != "" {
		t.Fatal("recall left the etcd lock key in place")
	}
}

// A recall waits out the minimum hold time before taking the lock away, so
// contention on one inode cannot turn every operation into a recall.
func TestRecallHonoursTheMinimumHoldTime(t *testing.T) {
	s := &Service{locks: newLockMap(func(es []*lockEntry, _ string) []*lockEntry { return es }), log: testLogger()}
	e := s.locks.entryFor(7)
	e.acquiredAt = time.Now()

	start := time.Now()
	s.recallLock(7)
	if elapsed := time.Since(start); elapsed < minHoldTime {
		t.Fatalf("recall yielded after %v, before the %v minimum hold", elapsed, minHoldTime)
	}
}

// An operation holding an entry that has since been evicted must not proceed
// on it: the entry excludes nothing once it is out of the cache.
func TestEvictedEntryIsNotCurrent(t *testing.T) {
	s := &Service{locks: newLockMap(func(es []*lockEntry, _ string) []*lockEntry { return es })}
	e := s.locks.entryFor(7)

	if !s.locks.isCurrent(e) {
		t.Fatal("a freshly cached entry reports as stale")
	}

	s.locks.mu.Lock()
	delete(s.locks.entries, 7)
	s.locks.mu.Unlock()

	if s.locks.isCurrent(e) {
		t.Fatal("an evicted entry still reports as the cache's entry for its inode")
	}
}

// A session that ends takes every key written under it with it, so the caches
// those keys vouched for have to go too — but only those.  An inode
// re-acquired under a fresh session holds a live key, and dropping it with the
// dead one would discard writes that are still publishable.
func TestSessionLossDropsOnlyTheCachesUnderTheDeadLease(t *testing.T) {
	s := &Service{locks: newLockMap(func(es []*lockEntry, _ string) []*lockEntry { return es }), log: testLogger()}

	const dead, live = clientv3.LeaseID(11), clientv3.LeaseID(12)
	stale := s.locks.entryFor(7)
	stale.holder, stale.lease = "11-a", dead
	stale.meta, stale.metaFor = &inodeMeta{}, "11-a"

	fresh := s.locks.entryFor(8)
	fresh.holder, fresh.lease = "12-a", live
	fresh.meta, fresh.metaFor = &inodeMeta{}, "12-a"

	s.dropCachesForLease(dead)

	if stale.holder != "" || stale.meta != nil {
		t.Fatal("a cache under the dead session's lease survived it")
	}
	if fresh.holder == "" || fresh.meta == nil {
		t.Fatal("a cache under a live session's lease was dropped with the dead one")
	}
}

// The hold is the whole of the hysteresis trade: it grows while a peer keeps
// asking for an inode the moment this node has taken it, and decays once the
// asking stops. Both directions matter — the first is what stops six writers
// to one file paying a round trip per operation, and the second is what stops
// an inode contended once from making every later peer wait for it.
func TestHandoverHoldGrowsUnderContentionAndDecaysAfterIt(t *testing.T) {
	e := &lockEntry{ino: 1}

	// Acquired just now, so every recall arrives inside the current hold.
	e.acquiredAt = time.Now()
	var hold time.Duration
	for i := 0; i < 8; i++ {
		_, hold = e.handoverHold()
	}
	if hold != maxHoldTime {
		t.Errorf("hold = %s after sustained contention, want the %s ceiling", hold, maxHoldTime)
	}

	// The recalls now arrive well after the hold has passed, which is what a
	// workload that has stopped fighting over the inode looks like.
	e.acquiredAt = time.Now().Add(-time.Second)
	for i := 0; i < 8; i++ {
		_, hold = e.handoverHold()
	}
	if hold != minHoldTime {
		t.Errorf("hold = %s after the contention ended, want the %s floor", hold, minHoldTime)
	}
}

func TestHandoverHoldStartsAtTheFloor(t *testing.T) {
	e := &lockEntry{ino: 1, acquiredAt: time.Now().Add(-time.Second)}
	held, hold := e.handoverHold()
	if hold != minHoldTime {
		t.Errorf("hold = %s on a lock nothing has contended, want %s", hold, minHoldTime)
	}
	if held < time.Second {
		t.Errorf("held = %s, want at least the second since it was acquired", held)
	}
}
