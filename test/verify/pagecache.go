package verify

import (
	"fmt"
	"sort"

	"github.com/etcfs/etcfs/internal/history"
)

// The page-cache property: no node keeps the kernel's data pages for an inode
// once it has given that inode's lock key up.
//
// This one is not a linearizability question and is not checked with Porcupine.
// It is an ordering obligation on a single node — between two events of its own,
// with no concurrency to search through — and phrasing it as a model would hide
// a simple scan behind a solver. What makes it checkable at all is that the
// invalidation is recorded: the release itself says nothing about the pages, and
// a stale page is invisible in a read history because it looks exactly like a
// read that was served correctly a moment earlier.
//
// The obligation is discharged by internal/ipc.releaseKeyLocked, which
// invalidates before it deletes the key and refuses to release if that fails.
// What is checked here is that the recorded events agree: every yielded key was
// preceded by an invalidation of its inode, on the same node, that had already
// returned. Two outcomes discharge the obligation without any page being
// dropped, and both are recorded rather than left implicit: a FUSE session that
// has gone away, which took its page cache with it, and an inode nothing could
// have cached in the first place (caching off, or no open ever told the kernel
// it could cache). A history that simply omitted those would be
// indistinguishable from one where the invalidation was owed and skipped.

const pageInvalOpcode = 1003

const (
	pageInvalDone      = 0
	pageInvalNoClient  = 1
	pageInvalFailed    = 2
	pageInvalNotCached = 3
)

// PageInvalOp is one kernel page-cache invalidation.
type PageInvalOp struct {
	Node    string
	Ino     uint64
	Outcome byte
	Call    int64
	Ret     int64
}

// DecodePageInvals turns a recorded history into its page-invalidation events.
func DecodePageInvals(entries []history.Entry) ([]PageInvalOp, error) {
	ops := make([]PageInvalOp, 0, len(entries))
	for _, e := range entries {
		if e.Opcode != pageInvalOpcode {
			continue
		}
		req, resp, err := e.Payloads()
		if err != nil {
			return nil, err
		}
		r := newReader(req)
		ino := r.u64()
		if !r.ok || len(resp) < 1 {
			return nil, fmt.Errorf("page invalidation at %d: truncated payload", e.CallNs)
		}
		ops = append(ops, PageInvalOp{
			Node: e.Node, Ino: ino, Outcome: resp[0], Call: e.CallNs, Ret: e.ReturnNs,
		})
	}
	return ops, nil
}

// PageCacheViolation is one lock key yielded with the kernel's pages for its
// inode possibly still cached.
type PageCacheViolation struct {
	Node string
	Ino  uint64
	// ReleasedAt is when the key release began, in Unix nanoseconds.
	ReleasedAt int64
	Reason     string
}

func (v PageCacheViolation) String() string {
	return fmt.Sprintf("node %s yielded ino %d at %d: %s", v.Node, v.Ino, v.ReleasedAt, v.Reason)
}

// CheckPageCache reports every lock key that was yielded without the inode's
// kernel pages having been invalidated first.
//
// A release with no invalidation at all before it is only a violation once the
// node has invalidated something at some point: a daemon running with page
// caching switched off never invalidates anything and never caches anything
// either, and reporting every one of its releases would be reporting the
// configuration rather than a defect.
func CheckPageCache(keys []LockOp, invals []PageInvalOp) []PageCacheViolation {
	type held struct {
		node string
		ino  uint64
	}

	caches := map[string]bool{}
	invalsFor := map[held][]PageInvalOp{}
	for _, iv := range invals {
		caches[iv.Node] = true
		k := held{iv.Node, iv.Ino}
		invalsFor[k] = append(invalsFor[k], iv)
	}
	for _, list := range invalsFor {
		sort.Slice(list, func(i, j int) bool { return list[i].Ret < list[j].Ret })
	}

	releasesFor := map[held][]LockOp{}
	for _, key := range keys {
		if key.Kind != lockReleaseExclusive && key.Kind != lockReleaseShared {
			continue
		}
		if !caches[key.Node] {
			continue
		}
		k := held{key.Node, key.Ino}
		releasesFor[k] = append(releasesFor[k], key)
	}

	var out []PageCacheViolation
	for k, releases := range releasesFor {
		sort.Slice(releases, func(i, j int) bool { return releases[i].actualCall() < releases[j].actualCall() })
		// Each release needs an invalidation of its own, after the previous
		// release and finished before this one began: pages cached under the
		// hold that is ending are not covered by the invalidation that ended the
		// hold before it, and the acknowledgement is what makes "finished
		// before" the right comparison — the peer taking the inode must not run
		// while the pages are still there.
		next := 0
		list := invalsFor[k]
		for i, rel := range releases {
			var floor int64
			if i > 0 {
				floor = releases[i-1].Ret
			}
			matched := PageInvalOp{}
			found := false
			for next < len(list) && list[next].Ret <= rel.actualCall() {
				if list[next].Ret >= floor {
					matched, found = list[next], true
				}
				next++
			}
			switch {
			case !found:
				out = append(out, PageCacheViolation{k.node, k.ino, rel.Call,
					"the key was given up with no invalidation of this inode under the hold that ended"})
			case matched.Outcome == pageInvalFailed:
				out = append(out, PageCacheViolation{k.node, k.ino, rel.Call,
					"the last invalidation before the release failed and the key was given up anyway"})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ReleasedAt < out[j].ReleasedAt })
	return out
}
