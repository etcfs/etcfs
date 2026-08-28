package verify

import (
	"encoding/binary"
	"fmt"
	"maps"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/etcfs/etcfs/internal/history"
)

// The extent model: does a read ever return bytes that disagree with every
// write that could have produced them?
//
// WRITE and READ are decoded from the daemon's own IPC history — the same
// entries the namespace model reads, no separate recording needed, since both
// operations already cross the socket and are already logged.
//
// Deferred writes changed what a write means here. A write is acknowledged from
// this node's RAM: its bytes are on no device and in no etcd record until a
// flush publishes them, and a node that dies before that flush takes them with
// it. What the model checks is therefore not "every acknowledged write is
// durable" but the guarantee the filesystem actually offers:
//
//   - a read never contradicts a write that was fsynced, from any node;
//   - a read never contradicts a write from its own node, flushed or not,
//     because that node serves its own buffer;
//   - a write that was never fsynced may vanish, but only for a node the caller
//     names as crashed, and only for readers other than the node that wrote it.
//
// That last relaxation is the only one, and it is deliberately not automatic: a
// checker that let any unflushed write vanish would accept a history where the
// data simply disappeared under a healthy cluster, which is the failure this
// model exists to catch. The chaos suite knows which daemons it killed and says
// so; a run that killed nothing checks the strict property.
//
// Scope, stated plainly: this model only constrains a byte position once some
// operation in the history has shown a value for it, exactly the way the
// namespace model's dirState.known tracks names it has not yet seen evidence
// about. A read of a position the history never touched is accepted
// unconditionally — it might be a legitimate hole (reads as zero) or bytes
// written before the recorded window started, and this model has no way to
// tell those apart, so it does not guess. What it does catch: a write's bytes
// disappearing, a read returning a value that contradicts every write and
// every prior read of that position, or a torn read mixing bytes from two
// different writes at one offset. Truncate, fallocate and SETATTR-driven size
// changes are out of scope — they do not cross WRITE/READ, and this file adds
// nothing for them.
const (
	extentWriteOpcode   = 23
	extentReadOpcode    = 22
	extentSetattrOpcode = 12
	extentFallocOpcode  = 35
	extentFsyncOpcode   = 24
	extentFlushOpcode   = 26
	fattrSize           = 1 << 3 // FUSE_SET_ATTR_SIZE, from internal/ipc/handlers.go
)

// ExtentKind is what a decoded data-path operation does to a file's bytes.
type ExtentKind int

const (
	// ExtentWrite puts bytes at an offset; ExtentRead observes them.
	ExtentWrite ExtentKind = iota
	ExtentRead
	// ExtentTruncate sets the file's size: everything at or past Off stops
	// existing, and reads there return zeroes rather than what was written.
	ExtentTruncate
	// ExtentInvalidate marks a range as no longer described by this model --
	// a hole punched by fallocate, whose exact result is not worth modelling
	// precisely when forgetting the range is already sound.
	ExtentInvalidate
	// ExtentFsync is the durability barrier: every write this node has
	// acknowledged for the inode is published by the time it returns, and so
	// cannot be lost by a later crash.
	ExtentFsync
)

// ExtentOp is one decoded data-path operation.
type ExtentOp struct {
	Node string
	Ino  uint64
	Kind ExtentKind
	// Off is the byte offset for a write or read, the new size for a
	// truncate, and the start of the range for an invalidate.
	Off uint64
	// Len is the length of an invalidated range; unused otherwise.
	Len uint64
	// Data is the bytes written, or the bytes a read returned.
	Data  []byte
	Errno int32
	Call  int64
	Ret   int64
}

// DecodeExtents turns a recorded history into WRITE/READ operations, in call
// order. Entries of other opcodes are skipped. A short write or a short read
// (fewer bytes acted on than requested) is decoded using the length the
// response actually reports, since that is what happened.
func DecodeExtents(entries []history.Entry) ([]ExtentOp, error) {
	ops := make([]ExtentOp, 0, len(entries))
	for _, e := range entries {
		switch e.Opcode {
		case extentWriteOpcode:
			op, err := decodeWrite(e)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)
		case extentReadOpcode:
			op, err := decodeRead(e)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)
		case extentSetattrOpcode:
			op, ok, err := decodeTruncate(e)
			if err != nil {
				return nil, err
			}
			if ok {
				ops = append(ops, op)
			}
		case extentFallocOpcode:
			op, err := decodeFallocate(e)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)
		case extentFsyncOpcode, extentFlushOpcode:
			op, err := decodeFsync(e)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)
		}
	}
	return ops, nil
}

func decodeWrite(e history.Entry) (ExtentOp, error) {
	req, resp, err := e.Payloads()
	if err != nil {
		return ExtentOp{}, err
	}
	if len(req) < 20 {
		return ExtentOp{}, fmt.Errorf("write at %d: request too short", e.CallNs)
	}
	ino := binary.BigEndian.Uint64(req)
	off := binary.BigEndian.Uint64(req[8:])
	dataLen := binary.BigEndian.Uint32(req[16:])
	if len(req) < 20+int(dataLen) {
		return ExtentOp{}, fmt.Errorf("write at %d: request truncated", e.CallNs)
	}
	data := req[20 : 20+dataLen]

	errno, written := int32(0), uint32(0)
	if len(resp) >= 4 {
		errno = int32(binary.BigEndian.Uint32(resp))
	}
	if errno == 0 && len(resp) >= 8 {
		written = binary.BigEndian.Uint32(resp[4:])
	}
	if written < uint32(len(data)) {
		data = data[:written]
	}
	return ExtentOp{Node: e.Node, Ino: ino, Kind: ExtentWrite, Off: off, Data: data,
		Errno: errno, Call: e.CallNs, Ret: e.ReturnNs}, nil
}

// decodeTruncate picks out the setattr calls that change a file's size, which
// are the ones that change what a read returns. The kernel's valid mask says
// which fields it actually means; the rest carry whatever the caller's struct
// stat happened to hold.
func decodeTruncate(e history.Entry) (ExtentOp, bool, error) {
	req, resp, err := e.Payloads()
	if err != nil {
		return ExtentOp{}, false, err
	}
	if len(req) < 28 {
		return ExtentOp{}, false, fmt.Errorf("setattr at %d: request too short", e.CallNs)
	}
	valid := binary.BigEndian.Uint32(req[16:])
	if valid&fattrSize == 0 {
		return ExtentOp{}, false, nil
	}
	errno := int32(0)
	if len(resp) >= 4 {
		errno = int32(binary.BigEndian.Uint32(resp))
	}
	return ExtentOp{
		Node: e.Node, Ino: binary.BigEndian.Uint64(req), Kind: ExtentTruncate,
		Off: binary.BigEndian.Uint64(req[20:]), Errno: errno,
		Call: e.CallNs, Ret: e.ReturnNs,
	}, true, nil
}

// decodeFallocate treats every fallocate as invalidating its range. Punching a
// hole zeroes those bytes and the other modes only allocate, but forgetting
// the range covers all of them without having to be right about which.
func decodeFallocate(e history.Entry) (ExtentOp, error) {
	req, resp, err := e.Payloads()
	if err != nil {
		return ExtentOp{}, err
	}
	if len(req) < 28 {
		return ExtentOp{}, fmt.Errorf("fallocate at %d: request too short", e.CallNs)
	}
	errno := int32(0)
	if len(resp) >= 4 {
		errno = int32(binary.BigEndian.Uint32(resp))
	}
	return ExtentOp{
		Node: e.Node, Ino: binary.BigEndian.Uint64(req), Kind: ExtentInvalidate,
		Off: binary.BigEndian.Uint64(req[12:]), Len: binary.BigEndian.Uint64(req[20:]),
		Errno: errno, Call: e.CallNs, Ret: e.ReturnNs,
	}, nil
}

// decodeFsync reads an FSYNC or FLUSH: [u64:ino]. Both flush this inode's
// buffer before replying, so both are the same barrier. A failed one is not:
// its errno is carried through, and the model ignores it, which is what makes
// an fsync that returned EIO stop being a durability promise.
func decodeFsync(e history.Entry) (ExtentOp, error) {
	req, resp, err := e.Payloads()
	if err != nil {
		return ExtentOp{}, err
	}
	if len(req) < 8 {
		return ExtentOp{}, fmt.Errorf("fsync at %d: request too short", e.CallNs)
	}
	errno := int32(0)
	if len(resp) >= 4 {
		errno = int32(binary.BigEndian.Uint32(resp))
	}
	if errno < 0 {
		errno = -errno
	}
	return ExtentOp{
		Node: e.Node, Ino: binary.BigEndian.Uint64(req), Kind: ExtentFsync,
		Errno: errno, Call: e.CallNs, Ret: e.ReturnNs,
	}, nil
}

func decodeRead(e history.Entry) (ExtentOp, error) {
	req, resp, err := e.Payloads()
	if err != nil {
		return ExtentOp{}, err
	}
	if len(req) < 20 {
		return ExtentOp{}, fmt.Errorf("read at %d: request too short", e.CallNs)
	}
	ino := binary.BigEndian.Uint64(req)
	off := binary.BigEndian.Uint64(req[8:])

	errno := int32(0)
	var data []byte
	if len(resp) >= 4 {
		errno = int32(binary.BigEndian.Uint32(resp))
	}
	if errno == 0 && len(resp) >= 8 {
		n := binary.BigEndian.Uint32(resp[4:])
		if len(resp) >= int(8+n) {
			data = resp[8 : 8+n]
		}
	}
	return ExtentOp{Node: e.Node, Ino: ino, Kind: ExtentRead, Off: off, Data: data,
		Errno: errno, Call: e.CallNs, Ret: e.ReturnNs}, nil
}

func extentOperations(ops []ExtentOp) []porcupine.Operation {
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

// cell is what the history has established about one byte position: its value,
// which node's unpublished buffer it is still only in, and whether anything has
// made it durable — an fsync, or an observation by a reader.
//
// owner is empty and durable true for a value a read established: a byte
// somebody has already seen cannot go back to being lost.
type cell struct {
	b       byte
	owner   string
	durable bool
}

// byteState is the bytes this history has established, per absolute offset —
// deliberately sparse rather than a flat buffer, since only positions the
// history has touched carry any constraint at all.
type byteState map[uint64]cell

func (s byteState) step(op ExtentOp, crashed map[string]bool) (bool, byteState) {
	if op.Errno != 0 {
		// An operation the store rejected changed nothing; there is nothing
		// here to check or to learn from.  A failed fsync in particular
		// promises nothing: its writes stay losable.
		return true, s
	}
	next := maps.Clone(s)
	if next == nil {
		next = byteState{}
	}

	switch op.Kind {
	case ExtentFsync:
		// Everything this node had buffered for the inode is published by the
		// time the call returns, so none of it can be lost from here on.
		for pos, c := range next {
			if c.owner == op.Node && !c.durable {
				c.durable = true
				next[pos] = c
			}
		}
		return true, next
	case ExtentTruncate:
		// Everything at or past the new size stops existing. Reads there
		// return zeroes, which is not what was written, and holding on to
		// the old bytes would report that correct read as a contradiction.
		for pos := range next {
			if pos >= op.Off {
				delete(next, pos)
			}
		}
		return true, next
	case ExtentInvalidate:
		for pos := op.Off; pos < op.Off+op.Len; pos++ {
			delete(next, pos)
		}
		return true, next
	}

	for i, b := range op.Data {
		pos := op.Off + uint64(i)
		if op.Kind == ExtentWrite {
			next[pos] = cell{b: b, owner: op.Node}
			continue
		}

		known, seen := next[pos]
		switch {
		case !seen, known.b == b:
			// Nothing established here yet, or the read agrees with what was.
			// Either way the reader has now seen this value, and no crash can
			// take a byte back from a node that has already returned it.
			next[pos] = cell{b: b, durable: true}
		case !known.durable && known.owner != op.Node && crashed[known.owner]:
			// The only excused disagreement: a write that was acknowledged out
			// of a buffer, never fsynced, and lost with the node that held it.
			next[pos] = cell{b: b, durable: true}
		default:
			// A read disagreeing with an established byte is the violation
			// this model exists to catch.
			return false, s
		}
	}
	return true, next
}

// ExtentModel is the per-inode byte-position register described above, for a
// run in which the named nodes died without a clean shutdown. Pass none for the
// strict model, under which no acknowledged write may ever vanish.
func ExtentModel(crashed ...string) porcupine.Model {
	lost := make(map[string]bool, len(crashed))
	for _, n := range crashed {
		lost[n] = true
	}
	return porcupine.Model{
		Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
			byIno := map[uint64][]porcupine.Operation{}
			for _, o := range h {
				ino := o.Input.(ExtentOp).Ino
				byIno[ino] = append(byIno[ino], o)
			}
			out := make([][]porcupine.Operation, 0, len(byIno))
			for _, ops := range byIno {
				out = append(out, ops)
			}
			return out
		},
		Init: func() interface{} { return byteState{} },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			ok, next := state.(byteState).step(input.(ExtentOp), lost)
			return ok, next
		},
		Equal: func(a, b interface{}) bool {
			return maps.Equal(a.(byteState), b.(byteState))
		},
		DescribeOperation: func(input, output interface{}) string {
			op := input.(ExtentOp)
			names := map[ExtentKind]string{
				ExtentWrite: "write", ExtentRead: "read", ExtentTruncate: "truncate",
				ExtentInvalidate: "invalidate", ExtentFsync: "fsync",
			}
			return fmt.Sprintf("%s(ino=%d off=%d len=%d)", names[op.Kind], op.Ino, op.Off, len(op.Data))
		},
	}
}

// CheckExtents checks a decoded data-path history against the byte-register
// model. WRITE and READ are both linearizable as observed over the socket —
// see docs/verification/porcupine.md for why the write path's internal
// serializable pre-read does not need its own classifier here.
//
// crashed names the nodes that were killed rather than shut down, whose
// unflushed writes are allowed to have been lost with them.
func CheckExtents(ops []ExtentOp, crashed []string, timeout time.Duration) porcupine.CheckResult {
	return porcupine.CheckOperationsTimeout(ExtentModel(crashed...), extentOperations(ops), timeout)
}
