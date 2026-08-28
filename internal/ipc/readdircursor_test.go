package ipc

import (
	"fmt"
	"testing"
	"time"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// A cursor is only ever consulted for the request that continues exactly where
// the last reply stopped. Anything else has to miss, because the name it holds
// says nothing about where some other position begins — answering from it would
// start the page in a place the caller did not ask for.
func TestDirCursorResumesOnlyWhereTheLastReplyStopped(t *testing.T) {
	c := newDirCursors()
	c.record(7, 24, "entry-000023")

	if _, found := c.resumeAt(7, 24); !found {
		t.Error("the request continuing the scan did not resume")
	}
	for _, offset := range []uint64{0, 23, 25, 48} {
		if _, found := c.resumeAt(7, offset); found {
			t.Errorf("offset %d resumed from a cursor recorded for 24", offset)
		}
	}
	if _, found := c.resumeAt(8, 24); found {
		t.Error("a different directory resumed from this one's cursor")
	}
}

// An offset of zero is the start of a listing by definition, so it must read
// from the beginning even if a cursor happens to be recorded at zero.
func TestDirCursorNeverResumesAtOffsetZero(t *testing.T) {
	c := newDirCursors()
	c.record(7, 0, "somewhere")
	if _, found := c.resumeAt(7, 0); found {
		t.Fatal("offset 0 resumed instead of reading the directory from the start")
	}
}

// A listing that ended has no name to continue from, and recording one would
// leave a cursor pointing at a page that does not exist.
func TestDirCursorIgnoresAnEmptyPage(t *testing.T) {
	c := newDirCursors()
	c.record(7, 24, "")
	if _, found := c.resumeAt(7, 24); found {
		t.Fatal("an empty page recorded a cursor")
	}
}

// An abandoned scan must not keep a cursor alive forever, and a resumed one
// must not be answered from a name recorded long enough ago that the directory
// is unrecognisable.
func TestDirCursorExpires(t *testing.T) {
	c := newDirCursors()
	c.m[7] = dirCursor{offset: 24, name: "stale", used: time.Now().Add(-2 * dirCursorTTL)}

	if _, found := c.resumeAt(7, 24); found {
		t.Fatal("a cursor older than its lifetime was used")
	}
}

// The map is bounded, and reaching the bound must cost the least recently used
// directory rather than the one being scanned right now.
func TestDirCursorEvictsTheLeastRecentlyUsed(t *testing.T) {
	c := newDirCursors()
	for i := 0; i < dirCursorMax; i++ {
		c.record(uint64(i+1), 10, fmt.Sprintf("name-%d", i))
	}
	if len(c.m) != dirCursorMax {
		t.Fatalf("tracking %d directories, want %d", len(c.m), dirCursorMax)
	}

	// Touch the first one so it is no longer the oldest, then overflow.
	if _, found := c.resumeAt(1, 10); !found {
		t.Fatal("the directory just recorded did not resume")
	}
	c.record(uint64(dirCursorMax+1), 10, "newcomer")

	if len(c.m) > dirCursorMax {
		t.Errorf("tracking %d directories, over the %d cap", len(c.m), dirCursorMax)
	}
	if _, found := c.resumeAt(1, 10); !found {
		t.Error("the most recently used directory was evicted")
	}
	if _, found := c.resumeAt(uint64(dirCursorMax+1), 10); !found {
		t.Error("the newly recorded directory was not kept")
	}
}

// Re-recording a directory already tracked must not count against the cap, or a
// long scan of one directory would evict everything else one page at a time.
func TestDirCursorReplacingDoesNotEvict(t *testing.T) {
	c := newDirCursors()
	for i := 0; i < dirCursorMax; i++ {
		c.record(uint64(i+1), 10, fmt.Sprintf("name-%d", i))
	}
	c.record(1, 20, "next-page")

	if len(c.m) != dirCursorMax {
		t.Fatalf("tracking %d directories, want %d", len(c.m), dirCursorMax)
	}
	if _, found := c.resumeAt(1, 20); !found {
		t.Error("the replaced cursor was not the one kept")
	}
}

// pageLimit decides how many entries to ask etcd for, and it may only ever
// overshoot: fetching more than fits costs one comparison each, while fetching
// fewer silently shortens the listing into another round trip.
func TestPageLimitNeverUndershootsWhatFits(t *testing.T) {
	for _, size := range []uint32{4096, 8192, 32768, 131072} {
		for _, plus := range []bool{false, true} {
			limit := pageLimit(size, plus)

			// The most entries that could fit is one per shortest possible
			// entry, which is what pageLimit returns; check against the widest
			// framing truncateToBuffer would charge for a one-character name.
			entries := make([]metadata.DirentEntry, limit+16)
			for i := range entries {
				entries[i] = metadata.DirentEntry{Name: "a"}
			}
			if fits := len(truncateToBuffer(entries, size, plus)); int64(fits) > limit {
				t.Errorf("size=%d plus=%v: %d entries fit but only %d were fetched",
					size, plus, fits, limit)
			}
		}
	}
}

// A buffer too small for even one entry still has to make progress: an empty
// reply is how a listing ends, so answering one for a buffer that is merely
// tight would truncate the directory.
func TestPageLimitAlwaysFetchesAtLeastOne(t *testing.T) {
	if got := pageLimit(1, true); got < 1 {
		t.Errorf("pageLimit(1, true) = %d, want at least 1", got)
	}
	if got := pageLimit(0, true); got != 0 {
		t.Errorf("pageLimit(0, true) = %d, want 0 meaning no limit", got)
	}
}
