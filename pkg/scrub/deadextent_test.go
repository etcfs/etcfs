package scrub

import (
	"fmt"
	"testing"

	"github.com/etcfs/etcfs/pkg/metadata"
)

func ext(seq, logOff, length uint64) metadata.Extent {
	return metadata.Extent{
		Key:   metadata.ExtentKey(1, seq),
		Chunk: seq,
		Seq:   seq,
		// Disk offsets do not matter to deadReason; keep them distinct so a
		// mix-up shows up as a failure rather than a coincidence.
		DiskOff: seq * length,
		LogOff:  logOff,
		Length:  length,
	}
}

// An extent left entirely past the file's size by a truncate is unreachable —
// the kernel clamps reads to the inode size — but nothing else in the scrubber
// notices, because the inode is still very much alive.
func TestDeadReasonFindsExtentsPastEOF(t *testing.T) {
	live := ext(0, 0, 4096)
	past := ext(1, 8192, 4096)
	all := []metadata.Extent{live, past}

	if r := deadReason(past, 4096, all); r == "" {
		t.Error("extent starting past the file size was treated as live")
	}
	if r := deadReason(live, 4096, all); r != "" {
		t.Errorf("live extent reported dead: %s", r)
	}
}

// An extent only partly past the end still holds live bytes at its head, so it
// must survive: reclaiming it would take the head with it.
func TestDeadReasonKeepsPartiallyTruncatedExtent(t *testing.T) {
	e := ext(0, 0, 8192)
	if r := deadReason(e, 4096, []metadata.Extent{e}); r != "" {
		t.Errorf("extent straddling the new EOF reported dead: %s", r)
	}
}

// A later write that fully covers an earlier one makes the earlier extent's
// blocks dead — this is the overwrite leak, invisible to the orphan check.
func TestDeadReasonFindsSupersededExtents(t *testing.T) {
	old := ext(1, 0, 4096)
	newer := ext(2, 0, 4096)
	all := []metadata.Extent{newer, old}

	if r := deadReason(old, 4096, all); r == "" {
		t.Error("overwritten extent was treated as live")
	}
	if r := deadReason(newer, 4096, all); r != "" {
		t.Errorf("the overwriting extent was reported dead: %s", r)
	}
}

// A partial overwrite leaves both extents readable, so neither may be
// reclaimed; the read path resolves them by chunk order instead.
func TestDeadReasonKeepsPartiallyOverwrittenExtent(t *testing.T) {
	old := ext(1, 0, 8192)
	newer := ext(2, 0, 4096)
	all := []metadata.Extent{newer, old}

	if r := deadReason(old, 8192, all); r != "" {
		t.Errorf("partly overwritten extent reported dead: %s", r)
	}
}

// A directory's link count is not its dirent count: it is its own ".", its
// entry in its parent, and the ".." of every subdirectory it holds. Counting
// dirents instead would report every directory in the filesystem as
// inconsistent.
func TestExpectedNlink(t *testing.T) {
	dir := &metadata.InodeRecord{Mode: metadata.ModeDir | 0755, Nlink: 2}
	if got := expectedNlink(dir, 1, 0); got != 2 {
		t.Errorf("empty directory: expected %d, want 2", got)
	}
	if got := expectedNlink(dir, 0, 0); got != 2 {
		t.Errorf("root directory with no dirent: expected %d, want 2", got)
	}
	if got := expectedNlink(dir, 1, 3); got != 5 {
		t.Errorf("directory holding three subdirectories: expected %d, want 5", got)
	}

	file := &metadata.InodeRecord{Mode: metadata.ModeFile | 0644, Nlink: 3}
	if got := expectedNlink(file, 3, 0); got != 3 {
		t.Errorf("hard-linked file: expected %d, want its 3 dirents", got)
	}
	if got := expectedNlink(file, 0, 0); got != 0 {
		t.Errorf("unlinked file: expected %d, want 0", got)
	}
}

// A permanent anomaly is re-found by every pass, so an append-only list grew by
// one entry every 30 seconds for as long as the daemon ran.
func TestAnomalyListDeduplicatesAndStaysBounded(t *testing.T) {
	s := &Scrubber{}
	stuck := []Result{{Type: "generation", Key: "extent:1/0"}}

	for i := 0; i < 50; i++ {
		s.record(stuck)
	}
	if got := len(s.Anomalies()); got != 1 {
		t.Errorf("the same finding was retained %d times", got)
	}

	overflow := make([]Result, maxAnomalies+10)
	for i := range overflow {
		overflow[i] = Result{Type: "orphan", Key: fmt.Sprintf("extent:2/%d", i)}
	}
	s.record(overflow)
	if got := len(s.Anomalies()); got != maxAnomalies {
		t.Errorf("anomaly list held %d entries, want the %d cap", got, maxAnomalies)
	}
	// The cap keeps the newest: the oldest findings have been re-reported since.
	last := s.Anomalies()[maxAnomalies-1]
	if last.Key != fmt.Sprintf("extent:2/%d", maxAnomalies+9) {
		t.Errorf("newest finding was evicted: last key is %s", last.Key)
	}
}
