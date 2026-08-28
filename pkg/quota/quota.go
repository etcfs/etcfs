// Package quota accounts space and inode usage per directory subtree.
//
// A quota root is a directory carrying a quota:<ino> key with its limits. Usage
// beneath it is computed by walking the namespace, because an inode records no
// parent — the only way to know which subtree a file is in is to build the
// parent index from the directory entries, the same walk Store.isDescendant
// does for a directory rename.
//
// That walk is why accounting here is periodic rather than transactional.
// Charging a write to its subtree at write time would need the enclosing quota
// root on the write path, which means either a parent pointer on every inode
// (a second source of truth to keep consistent) or a counter update in the
// transaction that publishes the write. The write path is already bound by the
// number of Raft round trips it makes — that is the finding the performance
// work landed on — so adding one more to every write is the wrong trade for a
// limit that is a policy rather than a correctness invariant.
//
// The consequence is stated rather than hidden: these are soft quotas. Usage
// is as of the last pass, so a subtree can exceed its limit between passes and
// is reported once the next one runs. Nothing here rejects a write.
package quota

import (
	"fmt"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// Limits is what a quota root allows. A zero field means unlimited, so a root
// can cap bytes without capping file count or the other way round.
type Limits struct {
	Bytes  uint64 `json:"bytes"`
	Inodes uint64 `json:"inodes"`
}

// Usage is one quota root's limits and what is currently charged against them.
type Usage struct {
	Root   uint64 `json:"root"`
	Limits Limits `json:"limits"`
	Bytes  uint64 `json:"bytes"`
	Inodes uint64 `json:"inodes"`
}

// OverBytes reports whether the byte limit is set and exceeded.
func (u Usage) OverBytes() bool { return u.Limits.Bytes > 0 && u.Bytes > u.Limits.Bytes }

// OverInodes reports whether the inode limit is set and exceeded.
func (u Usage) OverInodes() bool { return u.Limits.Inodes > 0 && u.Inodes > u.Limits.Inodes }

// Tree is the parent/child structure of the namespace, built once and walked
// once per quota root.
type Tree struct {
	children map[uint64][]uint64
}

// BuildTree assembles the namespace from directory-entry keys.
//
// Keys are the raw dirent keys and values the inode numbers they resolve to,
// exactly as a prefix scan over dirent: returns them.
func BuildTree(direntKeys []string, targets []uint64) *Tree {
	t := &Tree{children: make(map[uint64][]uint64, len(direntKeys))}
	for i, key := range direntKeys {
		if i >= len(targets) {
			break
		}
		if parent, _, ok := metadata.ParseDirentKey(key); ok {
			t.children[parent] = append(t.children[parent], targets[i])
		}
	}
	return t
}

// Compute charges every inode beneath each quota root against that root.
//
// A file with hard links in two different quota roots is charged to both. That
// is the same choice every filesystem with subtree quotas makes, and the
// alternative — charging the first root a walk happens to reach — would make
// the answer depend on map iteration order.
//
// The walk is bounded by the number of inodes it has already visited, so a
// namespace that somehow contains a cycle terminates rather than spinning.
func Compute(tree *Tree, inodes map[uint64]*metadata.InodeRecord, limits map[uint64]Limits) []Usage {
	out := make([]Usage, 0, len(limits))
	for root, lim := range limits {
		u := Usage{Root: root, Limits: lim}
		seen := make(map[uint64]bool)
		stack := []uint64{root}
		for len(stack) > 0 {
			ino := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if seen[ino] {
				continue
			}
			seen[ino] = true

			// The root directory itself is the container, not something stored
			// in it, so it is not charged against its own limits.
			if ino != root {
				if rec := inodes[ino]; rec != nil {
					u.Inodes++
					if rec.Mode&metadata.S_IFMT != metadata.ModeDir {
						u.Bytes += rec.Size
					}
				}
			}
			stack = append(stack, tree.children[ino]...)
		}
		out = append(out, u)
	}
	return out
}

// String renders a usage line for the CLI.
func (u Usage) String() string {
	return fmt.Sprintf("root=%d bytes=%d/%s inodes=%d/%s",
		u.Root, u.Bytes, limitStr(u.Limits.Bytes), u.Inodes, limitStr(u.Limits.Inodes))
}

func limitStr(v uint64) string {
	if v == 0 {
		return "unlimited"
	}
	return fmt.Sprintf("%d", v)
}
