package verify

import (
	"fmt"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/etcfs/etcfs/internal/history"
)

// Wire constants for the lock_hold synthetic history entries, matching
// historyOpLockHold and the event bytes in internal/ipc/retry.go. Kept as a
// separate copy rather than a shared import, on purpose: see decode.go.
const (
	lockHoldOpcode   = 1000
	lockKeyOpcode    = 1002
	lockEventAcquire = 0
	lockEventRelease = 1
)

// The lock model: does the exclusive/shared inode lock ever admit two holders
// that should have excluded each other?
//
// This is not phrased as "is this history linearizable" over some register —
// a lock has no value for a read to disagree about, so an ordinary
// linearizability check would accept any interleaving. What actually matters
// is mutual exclusion, and it is checked as exactly that: a state machine over
// acquire/release events, each recorded at the precise instant it happened
// (the etcd transaction that granted or dropped it), not over an interval. A
// Each event is checked over the interval its own etcd transaction spanned,
// not as a point: the transaction commits at a single revision, but nothing
// in the daemon observes that revision's wall-clock instant — only the call
// and return around it. Recording the interval is the honest statement of
// what is known, and it is what keeps ordinary clock offset between two
// hosts from reading as two holders of one lock.

// lockOpKind distinguishes the two events a lock's lifetime is made of.
type lockOpKind int

const (
	lockAcquireExclusive lockOpKind = iota
	lockReleaseExclusive
	lockAcquireShared
	lockReleaseShared
)

// LockOp is one endpoint of a lock's hold interval, over the interval the
// operation that moved it actually spanned.
type LockOp struct {
	Node string
	Ino  uint64
	Kind lockOpKind
	// Call and Ret bracket the etcd transaction that granted or dropped the
	// lock. The instant itself is somewhere inside; nothing observes it
	// directly, and pretending otherwise is what made clock offset between
	// two hosts look like two holders of one lock.
	Call int64
	Ret  int64
	// ActualCall is when the event really started, which differs from Call only
	// for a release of a key that had already expired: Call is widened back to
	// the acquisition so a peer's legitimate acquire can be ordered inside it,
	// and that widening would misplace anything asking what the node did just
	// before letting go. Falls back to Call for histories recorded without it.
	ActualCall int64
}

func (k lockOpKind) String() string {
	switch k {
	case lockAcquireExclusive:
		return "acquire-exclusive"
	case lockReleaseExclusive:
		return "release-exclusive"
	case lockAcquireShared:
		return "acquire-shared"
	case lockReleaseShared:
		return "release-shared"
	}
	return "?"
}

// DecodeLocks turns a recorded history into its per-operation lock
// acquire/release events, in call order.
func DecodeLocks(entries []history.Entry) ([]LockOp, error) {
	return decodeLockStream(entries, lockHoldOpcode)
}

// DecodeLockKeys turns a recorded history into the acquire/release events of
// the *cached* etcd lock key, which is what actually excludes peers.
//
// The per-operation events DecodeLocks returns span a subset of this interval:
// the key is taken before the operation that needed it and kept afterwards,
// against the next operation on the same inode. A subset is the safe direction
// to be wrong in for a mutual-exclusion checker — it can only report overlaps
// that really happened — but it is also blind to the whole failure the cached
// lock introduced, which is a key held past the point the cluster believes it
// was given up. That is why both streams are recorded and both are checked; the
// same model runs over each.
func DecodeLockKeys(entries []history.Entry) ([]LockOp, error) {
	return decodeLockStream(entries, lockKeyOpcode)
}

func decodeLockStream(entries []history.Entry, opcode uint16) ([]LockOp, error) {
	ops := make([]LockOp, 0, len(entries))
	for _, e := range entries {
		if e.Opcode != opcode {
			continue
		}
		req, _, err := e.Payloads()
		if err != nil {
			return nil, err
		}
		if len(req) < 10 {
			return nil, fmt.Errorf("lock event at %d: payload too short", e.CallNs)
		}
		event, mode := req[0], req[1]
		r := newReader(req[2:])
		ino := r.u64()
		if !r.ok {
			return nil, fmt.Errorf("lock event at %d: truncated inode", e.CallNs)
		}

		var kind lockOpKind
		switch {
		case event == lockEventAcquire && mode == 1:
			kind = lockAcquireExclusive
		case event == lockEventRelease && mode == 1:
			kind = lockReleaseExclusive
		case event == lockEventAcquire && mode == 0:
			kind = lockAcquireShared
		case event == lockEventRelease && mode == 0:
			kind = lockReleaseShared
		default:
			return nil, fmt.Errorf("lock event at %d: unknown event/mode %d/%d", e.CallNs, event, mode)
		}
		actual := e.CallNs
		if len(req) >= 18 {
			ar := newReader(req[10:])
			if v := ar.u64(); ar.ok {
				actual = int64(v)
			}
		}
		ops = append(ops, LockOp{
			Node: e.Node, Ino: ino, Kind: kind,
			Call: e.CallNs, Ret: e.ReturnNs, ActualCall: actual,
		})
	}
	return ops, nil
}

// lockOperations turns decoded LockOps into Porcupine operations, each over
// the interval its etcd transaction spanned.
func lockOperations(ops []LockOp) []porcupine.Operation {
	clients := map[string]int{}
	out := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		id, seen := clients[op.Node]
		if !seen {
			id = len(clients)
			clients[op.Node] = id
		}
		out = append(out, porcupine.Operation{
			ClientId: id, Input: op, Output: nil, Call: op.Call, Return: op.Ret,
		})
	}
	return out
}

// lockState is 0 (free), -1 (exclusive) or the number of shared holders.
type lockState int

func (s lockState) step(op LockOp) (bool, lockState) {
	switch op.Kind {
	case lockAcquireExclusive:
		if s != 0 {
			return false, s
		}
		return true, -1
	case lockReleaseExclusive:
		if s != -1 {
			return false, s
		}
		return true, 0
	case lockAcquireShared:
		if s == -1 {
			return false, s
		}
		return true, s + 1
	case lockReleaseShared:
		if s <= 0 {
			return false, s
		}
		return true, s - 1
	}
	return false, s
}

// LockModel is the mutual-exclusion state machine, partitioned per inode.
var LockModel = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		byIno := map[uint64][]porcupine.Operation{}
		for _, o := range h {
			ino := o.Input.(LockOp).Ino
			byIno[ino] = append(byIno[ino], o)
		}
		out := make([][]porcupine.Operation, 0, len(byIno))
		for _, ops := range byIno {
			out = append(out, ops)
		}
		return out
	},
	Init: func() interface{} { return lockState(0) },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		ok, next := state.(lockState).step(input.(LockOp))
		return ok, next
	},
	Equal: func(a, b interface{}) bool { return a.(lockState) == b.(lockState) },
	DescribeOperation: func(input, output interface{}) string {
		op := input.(LockOp)
		return fmt.Sprintf("ino=%d %s", op.Ino, op.Kind)
	},
}

// DefaultLockLeaseTTL is the session TTL the lock keys are written under, and
// so the window inside which etcd drops the locks of a node that stopped
// renewing. It matches the daemon's default --lease-ttl.
const DefaultLockLeaseTTL = 5 * time.Second

// withLeaseExpiryReleases appends a release for every lock a node still holds
// at the end of the history.
//
// A node killed mid-hold never records one: its locks are written under a
// session lease, and it is etcd that drops them when the lease stops being
// renewed, inside a process that no longer exists to log anything. Without
// this the lock is modelled as held forever and the next holder -- which
// acquired it perfectly legitimately -- reads as a mutual-exclusion
// violation. The chaos suite SIGKILLs daemons, so this is the common case,
// not an exotic one.
//
// The synthetic release spans [last event from that incarnation, + leaseTTL],
// which is the honest bound: the lease cannot have expired before the node's
// last observed activity, and cannot survive a TTL past it. That interval also
// keeps a genuinely leaked lock a violation -- a node that stayed alive and
// simply failed to release goes on emitting events, so its synthetic release
// lands after them, well after any conflicting acquire.
//
// "Incarnation", not "node", is what the bound is taken over, and the
// distinction is the whole point of the starts argument. The chaos suite kills
// a daemon and restarts it under the same node id, appending to the same
// history: treating that as one continuous identity puts the synthetic release
// for a lock held at the kill at the end of the *run* rather than the end of
// the incarnation that held it, and every legitimate acquisition by whoever
// took the inode next then reads as a second holder. Locks die with the
// session that wrote them, so each incarnation is closed out on its own.
//
// A history with no start markers -- one recorded before the daemon emitted
// them -- has exactly one incarnation per node, which is the behaviour this
// had before.
func withLeaseExpiryReleases(ops []LockOp, starts []StartOp, leaseTTL time.Duration) []LockOp {
	in := newIncarnations(starts)

	type incarnation struct {
		node string
		gen  int
	}
	type holding struct {
		node string
		gen  int
		ino  uint64
		excl bool
	}

	lastSeen := map[incarnation]int64{}
	held := map[holding]int{}
	for _, op := range ops {
		gen := in.at(op.Node, op.Call)
		if ik := (incarnation{op.Node, gen}); op.Ret > lastSeen[ik] {
			lastSeen[ik] = op.Ret
		}
		key := holding{node: op.Node, gen: gen, ino: op.Ino,
			excl: op.Kind == lockAcquireExclusive || op.Kind == lockReleaseExclusive}
		switch op.Kind {
		case lockAcquireExclusive, lockAcquireShared:
			held[key]++
		case lockReleaseExclusive, lockReleaseShared:
			held[key]--
		}
	}

	out := ops
	for key, n := range held {
		for i := 0; i < n; i++ {
			kind := lockReleaseShared
			if key.excl {
				kind = lockReleaseExclusive
			}
			last := lastSeen[incarnation{key.node, key.gen}]
			out = append(out, LockOp{
				Node: key.node, Ino: key.ino, Kind: kind,
				Call: last,
				Ret:  last + int64(leaseTTL),
			})
		}
	}
	return out
}

// CheckLocks checks a decoded lock history for mutual-exclusion violations.
//
// leaseTTL is the session TTL the locks were written under; pass
// DefaultLockLeaseTTL unless the cluster was configured otherwise. starts marks
// where each daemon incarnation began, so that locks a killed one held are
// closed out when its session died rather than at the end of the run; pass the
// result of DecodeStarts, or nil for a history with no restarts in it.
func CheckLocks(ops []LockOp, starts []StartOp, leaseTTL, timeout time.Duration) porcupine.CheckResult {
	return porcupine.CheckOperationsTimeout(
		LockModel, lockOperations(withLeaseExpiryReleases(ops, starts, leaseTTL)), timeout)
}

// actualCall is when the event really began: the widened Call for a release of
// an already-expired key says when the hold *may* have ended, which is the
// right thing for mutual exclusion and the wrong thing for ordering a release
// against the invalidation that had to precede it.
func (o LockOp) actualCall() int64 {
	if o.ActualCall != 0 {
		return o.ActualCall
	}
	return o.Call
}
