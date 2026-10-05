package ipc

import (
	"sort"
	"sync"
	"time"
)

// lockMap is the set of inodes this node has a cached lock entry for.
//
// Only the membership of that set lives here — which inode has which entry,
// which entry is the current one, and which entry to evict when the set is
// full. What a cached lock *means* (when its key may be given up, what must be
// published first, what a recall has to assert) stays on the Service in
// lockcache.go: that is the protocol, and it is not a map operation.
//
// The eviction victims are dropped through the callback rather than here,
// because dropping a lock publishes whatever the entry has buffered and can
// fail — and a failure has to leave the entry in the set.
type lockMap struct {
	mu      sync.Mutex
	entries map[uint64]*lockEntry

	// evicting marks a sweep in flight, which runs with mu dropped. See
	// evictLocked.
	evicting bool

	// drop publishes the entries' buffers and gives up their etcd keys, in one
	// transaction, returning those whose keys are gone. It runs with each
	// entry's write lock held and the set's mutex held.
	drop func(es []*lockEntry, trigger string) []*lockEntry
}

func newLockMap(drop func([]*lockEntry, string) []*lockEntry) *lockMap {
	return &lockMap{entries: make(map[uint64]*lockEntry), drop: drop}
}

// entryFor returns the entry for an inode, creating it if needed.
func (m *lockMap) entryFor(ino uint64) *lockEntry {
	m.mu.Lock()
	defer m.mu.Unlock()

	e := m.entries[ino]
	if e == nil {
		m.evictLocked()
		// evictLocked drops the set's mutex while it gives the keys up, so the
		// inode may have been inserted by someone else in the meantime.
		// Overwriting it would leave two entries for one inode, one of them
		// holding an etcd key nothing would ever release.
		if e = m.entries[ino]; e == nil {
			e = &lockEntry{ino: ino}
			m.entries[ino] = e
		}
	}
	e.lastUsed = time.Now()
	return e
}

// lookup returns the entry for an inode, or nil if it has none.
func (m *lockMap) lookup(ino uint64) *lockEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries[ino]
}

// isCurrent reports whether e is still the set's entry for its inode.  An
// operation that has taken e's local lock must check this before proceeding:
// an entry evicted out from under it excludes nothing, because the next caller
// builds a fresh entry and takes a different mutex.
func (m *lockMap) isCurrent(e *lockEntry) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries[e.ino] == e
}

// all returns the current entries, for the sweeps that walk every one of them.
// A snapshot rather than a held mutex: the sweeps take entry locks, and holding
// the set's mutex across that would block every acquisition behind them.
func (m *lockMap) all() []*lockEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := make([]*lockEntry, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, e)
	}
	return entries
}

// drain empties the set and returns what it held.
func (m *lockMap) drain() []*lockEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := make([]*lockEntry, 0, len(m.entries))
	for ino, e := range m.entries {
		entries = append(entries, e)
		delete(m.entries, ino)
	}
	return entries
}

// evictLocked makes room in the set by giving up its least recently used
// locks, a batch at a time.
//
// A batch rather than one victim, because giving a key up is a Raft commit and
// one transaction of lockEvictBatch deletes costs what one delete costs.  A
// workload with a far larger working set than the cache — an unpacking archive
// touches 80,000 inodes against 4,096 entries — otherwise evicts one inode per
// new one and pays that commit per file.  Between sweeps the set sits under its
// bound by up to a batch, which is what the bound already tolerates in the
// other direction.
//
// **The set's mutex is released while the keys are given up**, and that is not
// an optimisation.  Yielding a key invalidates the inode's kernel pages, which
// is a synchronous round trip to the FUSE daemon and a notify into the kernel;
// a batch of them is that cost lockEvictBatch times over.  This mutex is taken
// by every operation on the node (entryFor, lookup, isCurrent), so holding it
// across the batch stalls the whole node for the length of the sweep — measured
// as a 1.25x loss on an untar, which is more than batching the commits wins.
// One victim at a time hid it, because one round trip is short.
//
// The victims stay in the set while they are dropped, holding their own write
// locks.  An operation that arrives for one of those inodes finds the entry,
// blocks on its lock, and on waking sees the entry is no longer current and
// starts again on the one that replaced it — the eviction race lockInode
// already handles.  Removing them from the set first would instead let that
// operation build a *second* entry for an inode this node still holds the key
// for, and its own exclusive acquisition would then be blocked by that key
// forever.
//
// Only an entry no operation currently holds can go, so a full set of busy
// inodes grows past the bound rather than blocking; the bound is a target, not
// an invariant.
// forget removes an entry from the set, for an inode that can never be reached
// again — one whose last name and last descriptor are both gone.
//
// Eviction is the only other way an entry leaves, and it is driven by pressure
// rather than by liveness, so without this a deleted inode's entry can sit in
// the set for as long as the set has room. That is not free: the scrubber
// declines to reclaim the extents such an inode orphaned while an entry for it
// is here, so the blocks stay out of the free list for exactly as long.
//
// Only the entry the caller was working with is removed: a lookup that raced
// this and built a fresh one is a different object, and deleting it would leave
// that caller running under an entry nothing else can find.
func (m *lockMap) forget(e *lockEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries[e.ino] == e {
		delete(m.entries, e.ino)
	}
}

func (m *lockMap) evictLocked() {
	if len(m.entries) < lockCacheMax || m.evicting {
		return
	}
	victims := m.claimVictimsLocked(lockEvictBatch)
	if len(victims) == 0 {
		return
	}

	// One sweep at a time: a second one entering while this has the mutex
	// dropped would pick its own victims and pay its own stall for room this
	// one is already making.
	m.evicting = true
	m.mu.Unlock()

	released := m.drop(victims, "eviction")
	for _, e := range victims {
		e.unlockExclusive()
	}

	m.mu.Lock()
	m.evicting = false
	// Entries whose keys could not be given up — their writes are unpublished,
	// or their pages are not invalidated — are not returned and stay in the set
	// with their buffers reachable by the flush interval.
	for _, e := range released {
		delete(m.entries, e.ino)
	}
}

// claimVictimsLocked takes the write lock on up to n of the least recently used
// entries and returns them, oldest first.
//
// The candidates are ordered before any of them is locked, so the sweep costs
// one pass and one sort of the set rather than a linear scan per victim.
//
// TryLock rather than Lock: an entry with an operation in flight is skipped,
// never waited for.  Evicting it would let the next caller build a second entry
// for the same inode and run alongside the operation this one is excluding.
func (m *lockMap) claimVictimsLocked(n int) []*lockEntry {
	candidates := make([]*lockEntry, 0, len(m.entries))
	for _, e := range m.entries {
		candidates = append(candidates, e)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastUsed.Before(candidates[j].lastUsed)
	})

	victims := make([]*lockEntry, 0, n)
	for _, e := range candidates {
		if len(victims) == n {
			break
		}
		if e.tryLockExclusive() {
			victims = append(victims, e)
		}
	}
	return victims
}

// recallSet names the inodes with a recall already in flight, so that a burst
// of want events for one inode starts one yield rather than a race between
// several for the same key.
type recallSet struct {
	mu  sync.Mutex
	set map[uint64]bool
}

func newRecallSet() *recallSet { return &recallSet{set: make(map[uint64]bool)} }

// begin claims the right to recall an inode, reporting false if one is already
// in flight.
func (r *recallSet) begin(ino uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.set[ino] {
		return false
	}
	r.set[ino] = true
	return true
}

func (r *recallSet) end(ino uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.set, ino)
}
