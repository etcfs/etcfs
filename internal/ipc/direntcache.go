package ipc

import (
	"context"
	"sync"
	"time"

	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/metrics"
)

// Directory name sets, for answering a lookup of a name that is not there.
//
// A build probing an include path, a package manager looking for an optional
// config, a linker walking a library search path: all of them ask for names
// that mostly do not exist, and each of those questions used to be a
// linearizable point read of etcd.  The kernel caches the *answer* — a negative
// entry is cacheable, see negativeEntryResp — but only after it has been asked
// once, and a fresh directory of a thousand absent names costs a thousand round
// trips before any of that helps.
//
// One range read over the directory's `dirent:` prefix answers every one of
// them.  The set is kept live by the same watch that invalidates the kernel's
// dentries, so a name a peer creates lands here as it lands there, and this
// node's own mutations update it as they happen.
//
// Only *absences* are answered from it.  A name the set says is present still
// goes to etcd, so a set that has fallen behind can never invent a file or
// return the wrong inode — the worst it can do is report a name as absent for
// as long as a negative dentry would have been cached anyway.  That is the same
// staleness, from the same watch, under the same bound.
//
// Two things arm and disarm it, and both are the rule the page cache already
// follows: nothing is cached that nothing can invalidate.  The set is only used
// while the dirent watch is delivering, and everything is dropped when that
// watch has to skip forward.

const (
	// direntCacheMaxDirs bounds how many directories are remembered at once.
	direntCacheMaxDirs = 64

	// direntCacheMaxDirNames is the largest directory worth remembering.  Above
	// it the set would cost more memory than the lookups it saves, so the
	// directory is marked as such and its misses go back to being point reads —
	// which is what the whole filesystem did before this existed.
	direntCacheMaxDirNames = 4096

	// direntCacheMaxNames caps the names held across every directory at once.
	// The per-directory cap bounds one wide directory; on its own it bounds
	// nothing, because sixty-four of them multiply it.
	direntCacheMaxNames = 65536
)

// direntSet is one directory's names as of one range read, plus the deltas
// applied to it since.
type direntSet struct {
	names  map[string]struct{}
	filled time.Time

	// mine are names this node created and has not yet seen come back on the
	// watch.  A watch delta may not take one out of the set.
	//
	// The watch delivers in revision order, but a local mutation is applied when
	// it is *acknowledged*, which is neither.  So a peer's DELETE of a name,
	// committed before this node created that same name, can still arrive after
	// the create has been answered — and applying it would have this node report
	// a file the caller had just made as absent, for as long as the kernel keeps
	// the negative entry.  Holding the name until its own PUT arrives closes
	// that, and costs nothing when it is wrong: a name reported present is
	// looked up in etcd like any other.
	mine map[string]struct{}

	// tooBig marks a directory past the per-directory cap.  It holds no names:
	// the entry exists only so that the next miss does not read the whole
	// directory again to reach the same conclusion.
	tooBig bool
}

// direntCache remembers directory name sets.  Every field is guarded by mu.
type direntCache struct {
	// ttl bounds how long a set is trusted without the watch having said
	// anything, and is the entry timeout for exactly that reason: it is the same
	// window the kernel's own negative dentry gets, invalidated by the same
	// watch.
	ttl time.Duration

	mu    sync.Mutex
	armed bool
	dirs  map[uint64]*direntSet
	names int

	// filling counts the deltas that have arrived for a directory since its
	// range read began.  A read describes one revision, and a change committed
	// after that revision but delivered before the read returns would be lost by
	// installing the result — so a fill that saw any delta is discarded rather
	// than installed, and the next miss tries again.
	filling map[uint64]int
}

func newDirentCache(ttl time.Duration) *direntCache {
	return &direntCache{
		ttl:     ttl,
		dirs:    make(map[uint64]*direntSet),
		filling: make(map[uint64]int),
	}
}

// arm allows the cache to be consulted, which is sound only while the dirent
// watch is delivering.  Called every time that watch is established.
func (c *direntCache) arm() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = true
}

// reset drops everything and stops the cache being consulted, for a watch that
// had to skip forward: the sets describe directories as they were before
// changes nothing will ever deliver.
func (c *direntCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = false
	c.dirs = make(map[uint64]*direntSet)
	c.filling = make(map[uint64]int)
	c.names = 0
}

// absent reports whether the cache can say, on its own, that a name is not in a
// directory.  False means "ask etcd", which covers both a name that is there
// and a directory nothing is known about.
func (c *direntCache) absent(parent uint64, name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.armed {
		return false
	}
	d := c.dirs[parent]
	if d == nil || d.tooBig || time.Since(d.filled) > c.ttl {
		return false
	}
	_, present := d.names[name]
	return !present
}

// beginFill claims the right to read a directory, reporting false when the
// cache already has it or another fill is in flight.
func (c *direntCache) beginFill(parent uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.armed {
		return false
	}
	if _, busy := c.filling[parent]; busy {
		return false
	}
	if d := c.dirs[parent]; d != nil && time.Since(d.filled) <= c.ttl {
		return false
	}
	c.filling[parent] = 0
	return true
}

// abandonFill gives up a claimed fill without installing anything, for a read
// that failed.  Installing what a failed read returned would be installing an
// empty directory, which is the one answer this cache must never invent.
func (c *direntCache) abandonFill(parent uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.filling, parent)
}

// endFill installs what the range read found, unless a change to the directory
// was delivered while it was in flight.
func (c *direntCache) endFill(parent uint64, entries []metadata.DirentEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	deltas, filling := c.filling[parent]
	delete(c.filling, parent)
	if !c.armed || !filling || deltas > 0 {
		return
	}

	c.dropLocked(parent)
	if len(entries) > direntCacheMaxDirNames {
		c.dirs[parent] = &direntSet{filled: time.Now(), tooBig: true}
		c.evictLocked()
		return
	}

	names := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		names[e.Name] = struct{}{}
	}
	c.dirs[parent] = &direntSet{names: names, mine: map[string]struct{}{}, filled: time.Now()}
	c.names += len(names)
	c.evictLocked()
}

// created records a name this node has just made, and holds it against any
// watch delta until the watch confirms it. See direntSet.mine.
func (c *direntCache) created(parent uint64, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notedLocked(parent)
	if d := c.mutableLocked(parent); d != nil {
		c.insertLocked(d, name)
		d.mine[name] = struct{}{}
	}
}

// deleted records a name this node has just removed, so a lookup of it is
// answered here instead of costing a round trip to be told the same.
func (c *direntCache) deleted(parent uint64, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notedLocked(parent)
	if d := c.mutableLocked(parent); d != nil {
		delete(d.mine, name)
		c.removeLocked(d, name)
	}
}

// observed applies what the dirent watch reported about a name.
//
// A name this node created is only taken out of the set by its own PUT, never
// by a delta: the PUT is proof the create is behind us, and any DELETE still to
// arrive is either older than the create or will be followed by its own event.
func (c *direntCache) observed(parent uint64, name string, present bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notedLocked(parent)
	d := c.mutableLocked(parent)
	if d == nil {
		return
	}
	if present {
		delete(d.mine, name)
		c.insertLocked(d, name)
		return
	}
	if _, held := d.mine[name]; held {
		return
	}
	c.removeLocked(d, name)
}

// mutableLocked returns the set a delta may be applied to, or nil when there is
// none: a directory nothing is cached for has no set for a name to be missing
// from, and one past the size cap holds no names at all.
func (c *direntCache) mutableLocked(parent uint64) *direntSet {
	d := c.dirs[parent]
	if d == nil || d.tooBig {
		return nil
	}
	return d
}

func (c *direntCache) insertLocked(d *direntSet, name string) {
	if _, present := d.names[name]; !present {
		d.names[name] = struct{}{}
		c.names++
	}
}

func (c *direntCache) removeLocked(d *direntSet, name string) {
	if _, present := d.names[name]; present {
		delete(d.names, name)
		c.names--
	}
}

// notedLocked records that a directory changed while a fill of it was in
// flight, so that fill is discarded rather than installed over the change.
func (c *direntCache) notedLocked(parent uint64) {
	if _, filling := c.filling[parent]; filling {
		c.filling[parent]++
	}
}

// dropLocked forgets one directory.
func (c *direntCache) dropLocked(parent uint64) {
	if d := c.dirs[parent]; d != nil {
		c.names -= len(d.names)
		delete(c.dirs, parent)
	}
}

// evictLocked drops the least recently filled directories until both caps hold.
//
// ponytail: a scan for the oldest per eviction, which is bounded by
// direntCacheMaxDirs and runs only on a fill. A heap is the upgrade if that
// bound ever rises.
func (c *direntCache) evictLocked() {
	for len(c.dirs) > direntCacheMaxDirs || c.names > direntCacheMaxNames {
		var oldest uint64
		var at time.Time
		for parent, d := range c.dirs {
			if at.IsZero() || d.filled.Before(at) {
				oldest, at = parent, d.filled
			}
		}
		if at.IsZero() {
			return
		}
		c.dropLocked(oldest)
	}
}

// direntsAbsent reports whether this node can answer that a name is not in a
// directory without asking etcd.
func (s *Service) direntsAbsent(parent uint64, name string) bool {
	if !s.dirents.absent(parent, name) {
		metrics.DirentCache.WithLabelValues("miss").Inc()
		return false
	}
	metrics.DirentCache.WithLabelValues("hit").Inc()
	return true
}

// prefetchDirents reads a directory's names into the cache, so the misses after
// this one are answered locally.
//
// Called after a lookup etcd has just reported as absent, which is the point
// where a second miss in the same directory is likely and the read has already
// been shown to be worth making. A failure is ignored: the cache is an
// optimisation, and the lookup it follows has already been answered.
func (s *Service) prefetchDirents(ctx context.Context, parent uint64) {
	if !s.dirents.beginFill(parent) {
		return
	}
	entries, err := s.store.ListDirents(ctx, parent)
	if err != nil {
		s.dirents.abandonFill(parent)
		s.log.Debug("dirent prefetch failed; misses in this directory keep costing a read",
			"parent", parent, "error", err)
		return
	}
	s.dirents.endFill(parent, entries)
}
