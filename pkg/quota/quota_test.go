package quota

import (
	"testing"
	"time"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// tree builds the dirent key/target pair lists a prefix scan would produce.
type entry struct {
	parent uint64
	name   string
	target uint64
}

func build(entries []entry) *Tree {
	keys := make([]string, len(entries))
	targets := make([]uint64, len(entries))
	for i, e := range entries {
		keys[i] = metadata.DirentKey(e.parent, e.name)
		targets[i] = e.target
	}
	return BuildTree(keys, targets)
}

func dir(ino uint64) *metadata.InodeRecord {
	return &metadata.InodeRecord{Ino: ino, Mode: metadata.ModeDir | 0755}
}

func file(ino, size uint64) *metadata.InodeRecord {
	return &metadata.InodeRecord{Ino: ino, Mode: metadata.ModeFile | 0644, Size: size}
}

// Usage must include files nested arbitrarily deep, and must not charge the
// quota root itself — the root is the container, not something stored in it.
func TestComputeSumsWholeSubtree(t *testing.T) {
	// 10/ -> a (100 bytes), sub/ -> b (250 bytes)
	tree := build([]entry{
		{10, "a", 11},
		{10, "sub", 12},
		{12, "b", 13},
	})
	inodes := map[uint64]*metadata.InodeRecord{
		10: dir(10), 11: file(11, 100), 12: dir(12), 13: file(13, 250),
	}
	got := Compute(tree, inodes, map[uint64]Limits{10: {Bytes: 1000}})
	if len(got) != 1 {
		t.Fatalf("got %d usages, want 1", len(got))
	}
	u := got[0]
	if u.Bytes != 350 {
		t.Errorf("Bytes = %d, want 350", u.Bytes)
	}
	// The two files plus the one nested directory; the root is not counted.
	if u.Inodes != 3 {
		t.Errorf("Inodes = %d, want 3", u.Inodes)
	}
	if u.OverBytes() {
		t.Error("reported over limit at 350 of 1000")
	}
}

// A directory's own Size must not be charged as data: on a filesystem where a
// directory record carries a nonzero size, counting it would inflate every
// subtree's byte usage by the number of directories in it.
func TestComputeDoesNotChargeDirectorySizes(t *testing.T) {
	tree := build([]entry{{10, "sub", 11}})
	sub := dir(11)
	sub.Size = 4096
	inodes := map[uint64]*metadata.InodeRecord{10: dir(10), 11: sub}

	got := Compute(tree, inodes, map[uint64]Limits{10: {}})
	if got[0].Bytes != 0 {
		t.Errorf("Bytes = %d, want 0 — a directory's size is not data", got[0].Bytes)
	}
	if got[0].Inodes != 1 {
		t.Errorf("Inodes = %d, want 1", got[0].Inodes)
	}
}

// A hard link reaching the same inode twice within one root must be charged
// once, not once per name: the walk is over inodes, and double-counting would
// let a user inflate their own usage by linking a file to itself repeatedly.
func TestComputeChargesAHardLinkOnceWithinARoot(t *testing.T) {
	tree := build([]entry{
		{10, "one", 11},
		{10, "two", 11},
	})
	inodes := map[uint64]*metadata.InodeRecord{10: dir(10), 11: file(11, 100)}

	got := Compute(tree, inodes, map[uint64]Limits{10: {}})
	if got[0].Bytes != 100 {
		t.Errorf("Bytes = %d, want 100", got[0].Bytes)
	}
	if got[0].Inodes != 1 {
		t.Errorf("Inodes = %d, want 1", got[0].Inodes)
	}
}

// A namespace containing a cycle must terminate rather than spin. It should not
// be reachable, but an accounting pass is exactly the kind of background walk
// that would hang a daemon if it ever were.
func TestComputeTerminatesOnACycle(t *testing.T) {
	tree := build([]entry{
		{10, "down", 11},
		{11, "back", 10},
	})
	inodes := map[uint64]*metadata.InodeRecord{10: dir(10), 11: dir(11)}

	done := make(chan []Usage, 1)
	go func() { done <- Compute(tree, inodes, map[uint64]Limits{10: {}}) }()
	select {
	case got := <-done:
		if got[0].Inodes != 1 {
			t.Errorf("Inodes = %d, want 1", got[0].Inodes)
		}
	case <-timeout():
		t.Fatal("Compute did not terminate on a cyclic namespace")
	}
}

// An over-limit subtree has to be reported as such, and a zero limit has to
// mean unlimited rather than "everything is over".
func TestOverLimitReporting(t *testing.T) {
	cases := []struct {
		u          Usage
		wantBytes  bool
		wantInodes bool
	}{
		{Usage{Limits: Limits{Bytes: 100}, Bytes: 101}, true, false},
		{Usage{Limits: Limits{Bytes: 100}, Bytes: 100}, false, false},
		{Usage{Limits: Limits{}, Bytes: 1 << 40}, false, false},
		{Usage{Limits: Limits{Inodes: 2}, Inodes: 3}, false, true},
	}
	for i, c := range cases {
		if c.u.OverBytes() != c.wantBytes || c.u.OverInodes() != c.wantInodes {
			t.Errorf("case %d: OverBytes=%v OverInodes=%v, want %v/%v",
				i, c.u.OverBytes(), c.u.OverInodes(), c.wantBytes, c.wantInodes)
		}
	}
}

func TestUsageStringMarksUnlimited(t *testing.T) {
	u := Usage{Root: 7, Limits: Limits{Bytes: 500}, Bytes: 100, Inodes: 2}
	want := "root=7 bytes=100/500 inodes=2/unlimited"
	if got := u.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// timeout bounds the cycle test, so a non-terminating walk fails the run
// instead of hanging it.
func timeout() <-chan time.Time { return time.After(5 * time.Second) }
