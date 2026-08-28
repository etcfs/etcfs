package verify

import (
	"encoding/base64"
	"encoding/binary"
	"testing"

	"github.com/etcfs/etcfs/internal/history"
)

// The new decoders are checked against the frames the daemon actually writes
// (internal/ipc/histevents.go), byte for byte, since nothing else would catch a
// payload whose layout changed on one side only.

func rawEntry(node, op string, code uint16, call, ret int64, req, resp []byte) history.Entry {
	return history.Entry{
		Node: node, Op: op, Opcode: code, CallNs: call, ReturnNs: ret,
		Request:  base64.StdEncoding.EncodeToString(req),
		Response: base64.StdEncoding.EncodeToString(resp),
	}
}

func TestDecodeLockKeysReadsTheDaemonsFrames(t *testing.T) {
	req := make([]byte, 10)
	req[0], req[1] = lockEventAcquire, 1 // exclusive
	binary.BigEndian.PutUint64(req[2:], 7)

	ops, err := DecodeLockKeys([]history.Entry{
		rawEntry("n1", "lock_key", lockKeyOpcode, 10, 20, req, nil),
		// A per-operation hold event must not be picked up by the key decoder.
		rawEntry("n1", "lock_hold", lockHoldOpcode, 30, 40, req, nil),
	})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("decoded %d events, want 1", len(ops))
	}
	if ops[0].Ino != 7 || ops[0].Kind != lockAcquireExclusive || ops[0].Call != 10 {
		t.Fatalf("decoded %+v", ops[0])
	}
}

func TestDecodeBlocksReadsTheDaemonsFrames(t *testing.T) {
	req := make([]byte, 17)
	req[0] = blockEventFree
	binary.BigEndian.PutUint64(req[1:], 4096)
	binary.BigEndian.PutUint64(req[9:], 8192)

	ops, err := DecodeBlocks([]history.Entry{rawEntry("n1", "block", blockOpcode, 10, 10, req, nil)})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(ops) != 1 || ops[0].Reserve || ops[0].DiskOff != 4096 || ops[0].Length != 8192 {
		t.Fatalf("decoded %+v", ops)
	}
}

func TestDecodePageInvalsReadsTheDaemonsFrames(t *testing.T) {
	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, 42)

	ops, err := DecodePageInvals([]history.Entry{
		rawEntry("n1", "page_inval", pageInvalOpcode, 10, 20, req, []byte{pageInvalNoClient}),
	})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(ops) != 1 || ops[0].Ino != 42 || ops[0].Outcome != pageInvalNoClient {
		t.Fatalf("decoded %+v", ops)
	}
}

func TestDecodeExtentsReadsFsyncAsABarrier(t *testing.T) {
	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, 42)

	ops, err := DecodeExtents([]history.Entry{
		rawEntry("n1", "fsync", extentFsyncOpcode, 10, 20, req, errnoResp(0)),
		rawEntry("n1", "flush", extentFlushOpcode, 30, 40, req, errnoResp(-5)),
	})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("decoded %d operations, want 2", len(ops))
	}
	if ops[0].Kind != ExtentFsync || ops[0].Ino != 42 || ops[0].Errno != 0 {
		t.Fatalf("decoded %+v", ops[0])
	}
	if ops[1].Kind != ExtentFsync || ops[1].Errno != 5 {
		t.Fatalf("a failed flush decoded as %+v", ops[1])
	}
}
