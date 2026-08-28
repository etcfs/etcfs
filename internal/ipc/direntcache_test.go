package ipc

import (
	"fmt"
	"testing"
	"time"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// The cache answers one question — "this name is not in this directory" — and
// every test here is about a way that answer could be wrong. A wrong absence is
// an ENOENT for a file that exists, cached by the kernel for the entry timeout,
// so the bar is that it is never given on anything but a set the watch is
// keeping current.

func filled(t *testing.T, c *direntCache, parent uint64, names ...string) {
	t.Helper()
	c.arm()
	if !c.beginFill(parent) {
		t.Fatalf("could not claim a fill of directory %d", parent)
	}
	entries := make([]metadata.DirentEntry, 0, len(names))
	for i, n := range names {
		entries = append(entries, metadata.DirentEntry{Name: n, Ino: uint64(100 + i)})
	}
	c.endFill(parent, entries)
}

func TestDirentCacheAnswersAbsenceOnlyForAFilledDirectory(t *testing.T) {
	c := newDirentCache(time.Minute)

	// Nothing known: every question goes to etcd.
	if c.absent(1, "anything") {
		t.Error("an unfilled directory answered a lookup; it knows no names to be missing from")
	}

	filled(t, c, 1, "present")

	if !c.absent(1, "missing") {
		t.Error("a name the directory does not hold was not answered as absent")
	}
	if c.absent(1, "present") {
		t.Error("a name the directory holds was answered as absent")
	}
}

func TestDirentCacheIsNotConsultedUntilTheWatchIsDelivering(t *testing.T) {
	c := newDirentCache(time.Minute)

	// Disarmed: a fill cannot even be claimed, so nothing can be cached that
	// nothing would invalidate.
	if c.beginFill(1) {
		t.Fatal("a directory was filled while the dirent watch was not delivering")
	}

	filled(t, c, 1, "present")
	if !c.absent(1, "missing") {
		t.Fatal("the cache did not answer while armed")
	}

	// A watch that had to skip forward drops everything: the set describes the
	// directory as it was before changes nothing will ever deliver.
	c.reset()
	if c.absent(1, "missing") {
		t.Error("the cache answered from a set the watch had stopped maintaining")
	}
}

func TestDirentCacheStopsAnsweringWhenTheSetExpires(t *testing.T) {
	c := newDirentCache(10 * time.Millisecond)
	filled(t, c, 1, "present")

	if !c.absent(1, "missing") {
		t.Fatal("a fresh set did not answer")
	}
	time.Sleep(20 * time.Millisecond)
	if c.absent(1, "missing") {
		t.Error("an expired set still answered; nothing bounds a watch that stopped delivering")
	}
}

// A name this node has just made must never be reported absent, whatever the
// watch delivers next. A peer's DELETE of the same name, committed *before* the
// create, is delivered afterwards — the watch orders by revision and the local
// mutation is applied when it is acknowledged, which is neither.
func TestDirentCacheHoldsANameThisNodeCreatedAgainstAStaleDelete(t *testing.T) {
	c := newDirentCache(time.Minute)
	filled(t, c, 1)

	c.created(1, "just-made")
	if c.absent(1, "just-made") {
		t.Fatal("a name this node created was reported absent immediately")
	}

	c.observed(1, "just-made", false) // the peer's older DELETE, delivered late
	if c.absent(1, "just-made") {
		t.Error("a stale DELETE removed a name this node had already acknowledged creating")
	}

	// Its own PUT arrives and releases the hold; a DELETE after that is a real
	// removal and does apply.
	c.observed(1, "just-made", true)
	c.observed(1, "just-made", false)
	if !c.absent(1, "just-made") {
		t.Error("a genuine removal after the create was confirmed did not apply")
	}
}

func TestDirentCacheFollowsThisNodesOwnRemovals(t *testing.T) {
	c := newDirentCache(time.Minute)
	filled(t, c, 1, "doomed")

	if c.absent(1, "doomed") {
		t.Fatal("a name in the directory was reported absent")
	}
	c.deleted(1, "doomed")
	if !c.absent(1, "doomed") {
		t.Error("a name this node unlinked still costs a round trip to be told it is gone")
	}
}

// A fill describes one revision. A change committed after that revision but
// delivered before the read returns would be lost by installing the result, so
// the fill is discarded instead and the next miss tries again.
func TestDirentCacheDiscardsAFillOvertakenByAChange(t *testing.T) {
	c := newDirentCache(time.Minute)
	c.arm()

	if !c.beginFill(1) {
		t.Fatal("could not claim the fill")
	}
	c.observed(1, "appeared", true) // committed after the read, delivered during it
	c.endFill(1, []metadata.DirentEntry{{Name: "old", Ino: 2}})

	if c.absent(1, "appeared") {
		t.Error("a fill that a change overtook was installed, hiding the change")
	}
	if c.absent(1, "old") {
		t.Error("the discarded fill left the directory cached anyway")
	}
}

func TestDirentCacheRefusesADirectoryPastTheCap(t *testing.T) {
	c := newDirentCache(time.Minute)
	names := make([]string, direntCacheMaxDirNames+1)
	for i := range names {
		names[i] = fmt.Sprintf("name-%d", i)
	}
	filled(t, c, 1, names...)

	if c.absent(1, "nothing-like-that") {
		t.Error("a directory past the cap was cached; its misses should stay point reads")
	}
	// And it is not read again on the next miss, which is the whole reason the
	// refusal is remembered rather than simply not stored.
	if c.beginFill(1) {
		t.Error("a directory known to be past the cap was read again")
	}
}

func TestDirentCacheEvictsToStayWithinItsCaps(t *testing.T) {
	c := newDirentCache(time.Minute)
	for i := 0; i <= direntCacheMaxDirs; i++ {
		filled(t, c, uint64(i+1), "a", "b")
	}
	if len(c.dirs) > direntCacheMaxDirs {
		t.Errorf("cached %d directories, cap is %d", len(c.dirs), direntCacheMaxDirs)
	}
	// The most recent fill survives; the count is what the eviction is for.
	if !c.absent(uint64(direntCacheMaxDirs+1), "missing") {
		t.Error("the newest directory was evicted rather than the oldest")
	}
}

func TestDirentCacheNameCountTracksTheSets(t *testing.T) {
	c := newDirentCache(time.Minute)
	filled(t, c, 1, "a", "b")
	if c.names != 2 {
		t.Fatalf("names = %d after a fill of two, want 2", c.names)
	}
	c.created(1, "c")
	c.observed(1, "c", true) // idempotent: the watch echo of the same create
	if c.names != 3 {
		t.Errorf("names = %d after one create, want 3", c.names)
	}
	c.deleted(1, "a")
	c.observed(1, "a", false)
	if c.names != 2 {
		t.Errorf("names = %d after one unlink, want 2", c.names)
	}
	c.reset()
	if c.names != 0 {
		t.Errorf("names = %d after a reset, want 0", c.names)
	}
}
