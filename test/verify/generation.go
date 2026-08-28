package verify

import (
	"fmt"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/etcfs/etcfs/internal/history"
)

// The generation model: once a guarded commit has been rejected for a fence,
// does any later commit from that node ever succeed?
//
// This is the single most safety-critical property in the codebase — it is
// what stands between a fenced node and writing to an arena a healthy node now
// owns — and it is the one place a Porcupine-style search earns its keep over
// a simple ordered scan: several FUSE worker threads on one node call
// commitGuarded concurrently, so their attempts genuinely overlap in wall
// time, and the model has to consider every order the overlap allows rather
// than trust whichever order they happened to return in.
//
// The etcd side of this (the guard's compare-and-commit) is trusted, per
// docs/verification/index.md — this checks that the daemon's own code
// correctly turns "the guard rejected me" into "I stop mutating", which is
// exactly the kind of regression a future refactor of commitGuarded could
// introduce without etcd noticing at all.

// GuardOp is one guarded-commit attempt.
type GuardOp struct {
	Node      string
	Gen       uint64
	Committed bool
	Fenced    bool
	Call, Ret int64
}

const guardedCommitOpcode = 1001

// DecodeGuardedCommits turns a recorded history into guarded-commit attempts,
// in call order. Entries other than historyOpGuardedCommit are skipped.
func DecodeGuardedCommits(entries []history.Entry) ([]GuardOp, error) {
	ops := make([]GuardOp, 0, len(entries))
	for _, e := range entries {
		if e.Opcode != guardedCommitOpcode {
			continue
		}
		req, _, err := e.Payloads()
		if err != nil {
			return nil, err
		}
		r := newReader(req)
		gen := r.u64()
		if !r.ok || r.pos >= len(req) {
			return nil, fmt.Errorf("guarded commit at %d: truncated payload", e.CallNs)
		}
		flag := req[r.pos]
		ops = append(ops, GuardOp{
			Node: e.Node, Gen: gen,
			Committed: flag&1 != 0, Fenced: flag&2 != 0,
			Call: e.CallNs, Ret: e.ReturnNs,
		})
	}
	return ops, nil
}

func guardOperations(ops []GuardOp) []porcupine.Operation {
	clients := map[string]int{}
	out := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		id, seen := clients[op.Node]
		if !seen {
			id = len(clients)
			clients[op.Node] = id
		}
		out = append(out, porcupine.Operation{
			ClientId: id, Input: op, Output: op.Committed, Call: op.Call, Return: op.Ret,
		})
	}
	return out
}

// guardState is the highest generation this node is known to have been
// fenced at, and whether it has been fenced at all.
//
// Tracking the generation rather than a bare "fenced" flag is what keeps a
// legitimate restart from reading as a violation. A fenced node that comes
// back adopts the cluster's new generation and resumes writing, which is the
// design working -- the fence is an epoch boundary, not a permanent ban -- and
// the recorder appends to the same file under the same node ID across that
// restart, so the resumed commits arrive in the same partition as the fenced
// ones. A flag alone cannot tell them apart and reports the restart as a
// write from the fenced incarnation.
//
// The generation can, because it is monotone: once generation G has been
// fenced, no later incarnation ever carries G again. So a commit is a
// violation exactly when it carries a generation that has already been
// fenced -- which is also strictly stronger than the flag, since it rejects
// any commit at or below a fenced generation rather than only those after a
// fence was observed.
type guardState struct {
	fenced   bool
	fencedAt uint64
}

func (s guardState) step(op GuardOp) (bool, guardState) {
	switch {
	case op.Committed && s.fenced && op.Gen <= s.fencedAt:
		// A commit carrying a generation already known to be dead.
		return false, s
	case op.Fenced && (!s.fenced || op.Gen > s.fencedAt):
		return true, guardState{fenced: true, fencedAt: op.Gen}
	default:
		return true, s
	}
}

// GenerationModel checks that no node commits metadata under a generation it
// has already been fenced at, partitioned per node since fencing is a per-node
// fact.
var GenerationModel = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		byNode := map[int][]porcupine.Operation{}
		for _, o := range h {
			byNode[o.ClientId] = append(byNode[o.ClientId], o)
		}
		out := make([][]porcupine.Operation, 0, len(byNode))
		for _, ops := range byNode {
			out = append(out, ops)
		}
		return out
	},
	Init: func() interface{} { return guardState{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		ok, next := state.(guardState).step(input.(GuardOp))
		return ok, next
	},
	Equal: func(a, b interface{}) bool { return a.(guardState) == b.(guardState) },
	DescribeOperation: func(input, output interface{}) string {
		op := input.(GuardOp)
		return fmt.Sprintf("gen=%d committed=%v fenced=%v", op.Gen, op.Committed, op.Fenced)
	},
}

// CheckGenerations checks a decoded guarded-commit history for a write that
// slipped through after its node was fenced.
func CheckGenerations(ops []GuardOp, timeout time.Duration) porcupine.CheckResult {
	return porcupine.CheckOperationsTimeout(GenerationModel, guardOperations(ops), timeout)
}
