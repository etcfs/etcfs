package verify

import (
	"sort"

	"github.com/etcfs/etcfs/internal/history"
)

// Daemon incarnations: which run of a node an event belongs to.
//
// A node id is not an identity that survives a crash. The chaos suite kills a
// daemon and starts it again under the same id, appending to the same history
// file, and two kinds of state the checkers reason about do not survive with
// it: the etcd lock keys it held, which the lease drops a TTL later, and the
// blocks its allocator had reserved, which live only in the dead process's
// memory. Read as one continuous node, both look like state held indefinitely,
// and whoever legitimately takes the inode or the block next reads as a
// conflicting second owner.
//
// The daemon records a marker when it starts (history.OpStart) so this is read
// off the history rather than guessed at from gaps in it. A history with no
// markers — one recorded before the daemon emitted them — has a single
// incarnation per node, which is how these checks behaved before.

// startOpcode matches history.OpStart, copied for the same reason the wire
// payloads are decoded again here: a checker that shares the daemon's
// constants cannot notice the daemon changing them.
const startOpcode = 1005

// StartOp is one daemon incarnation coming up.
type StartOp struct {
	Node string
	At   int64
}

// DecodeStarts turns a recorded history into its daemon-start markers.
func DecodeStarts(entries []history.Entry) []StartOp {
	var out []StartOp
	for _, e := range entries {
		if e.Opcode == startOpcode {
			out = append(out, StartOp{Node: e.Node, At: e.CallNs})
		}
	}
	return out
}

// incarnations indexes start markers so events can be attributed to the run of
// the node that produced them.
type incarnations map[string][]int64

func newIncarnations(starts []StartOp) incarnations {
	in := incarnations{}
	for _, s := range starts {
		in[s.Node] = append(in[s.Node], s.At)
	}
	for _, b := range in {
		sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
	}
	return in
}

// at reports which incarnation of node was running at t: the last one that had
// started by then. Events preceding every marker belong to incarnation 0, so a
// history that begins mid-run is attributed rather than discarded.
func (in incarnations) at(node string, t int64) int {
	b := in[node]
	return sort.Search(len(b), func(i int) bool { return b[i] > t })
}

// endOf reports when incarnation gen of node stopped: the instant its
// successor started. Returns 0 for the incarnation still running at the end of
// the history, which has not ended and whose state is nobody else's yet.
func (in incarnations) endOf(node string, gen int) int64 {
	b := in[node]
	if gen < len(b) {
		return b[gen]
	}
	return 0
}
