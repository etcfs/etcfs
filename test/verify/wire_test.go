package verify

import (
	"encoding/base64"
	"encoding/binary"

	"github.com/etcfs/etcfs/internal/history"
)

// recordedNamespaceHistory builds entries in exactly the frame format the
// daemon records, so the decoder is exercised against the wire rather than
// against a convenient in-memory shape.
func recordedNamespaceHistory() []history.Entry {
	entry := func(op string, code uint16, call, ret int64, req, resp []byte) history.Entry {
		return history.Entry{
			Node: "n1", Op: op, Opcode: code, CallNs: call, ReturnNs: ret,
			Request:  base64.StdEncoding.EncodeToString(req),
			Response: base64.StdEncoding.EncodeToString(resp),
		}
	}
	return []history.Entry{
		entry("lookup", 1, 10, 20, nameReq(1, "f"), negativeEntryResp()),
		entry("create", 5, 30, 40, nameReq(1, "f"), inoResp(42)),
		entry("lookup", 1, 50, 60, nameReq(1, "f"), inoResp(42)),
		entry("unlink", 7, 70, 80, nameReq(1, "f"), errnoResp(0)),
		entry("lookup", 1, 90, 100, nameReq(1, "f"), negativeEntryResp()),
	}
}

// nameReq is the [u64:parent][u32:len][name] prefix every namespace request
// starts with. The trailing fields (mode, umask, credentials) are not decoded
// by the namespace model, so they are left off.
func nameReq(parent uint64, name string) []byte {
	b := make([]byte, 12+len(name))
	binary.BigEndian.PutUint64(b, parent)
	binary.BigEndian.PutUint32(b[8:], uint32(len(name)))
	copy(b[12:], name)
	return b
}

func errnoResp(code int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(code))
	return b
}

func inoResp(ino uint64) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint32(b, 0)
	binary.BigEndian.PutUint64(b[4:], ino)
	return b
}

// negativeEntryResp is how the daemon reports a name that is not there: a
// successful reply carrying inode 0, which is the only form the kernel can
// cache. Read literally it says the name is taken by inode 0, so the decoder
// has to recognise it — hence a test that feeds it the real frame.
func negativeEntryResp() []byte {
	return inoResp(0)
}
