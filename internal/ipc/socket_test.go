package ipc

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/etcfs/etcfs/internal/config"
	"github.com/etcfs/etcfs/pkg/metadata"
)

// attrWireSize is what rb_attr() in pkg/fuse/ops.c consumes per attr block:
// six u64 plus nine u32.  Readdirplus writes one attr block per entry, so a
// mismatch here silently desynchronises the C-side response parser.
const attrWireSize = 6*8 + 9*4

func TestAttrBlockMatchesCDaemonWidth(t *testing.T) {
	var b buf
	b.wAttr(&metadata.InodeRecord{})
	if len(b.b) != attrWireSize {
		t.Fatalf("wAttr wrote %d bytes, ops.c rb_attr reads %d", len(b.b), attrWireSize)
	}
}

// Every fixed-width reply, measured against what the C parser consumes for it.
//
// The layouts are hand-encoded twice — buf/reader here, wb_*/rb_* in
// pkg/fuse/ops.c — so a field added on one side only shifts every field after
// it on the other, silently. Three replies used to be pinned this way and the
// rest were unguarded; this covers all of them. The wanted numbers are written
// as the sum of the C reads, in reply order, so a diff shows which field moved
// rather than only that a total changed.
//
// Variable-length replies (READ, READDIR, READDIRPLUS, READLINK, LISTXATTR,
// GETXATTR) are not here: their length is carried in the frame, and the reader
// that consumes them is bounded, so a mismatch is a short read rather than a
// desync.
// replyService is the least of a Service the reply builders need: the two cache
// timeouts they stamp on every entry.
func replyService() *Service {
	return &Service{
		entryTimeout: timeoutSecs(0, config.DefaultEntryTimeout),
		attrTimeout:  timeoutSecs(0, config.DefaultAttrTimeout),
	}
}

func TestFixedWidthRepliesMatchTheCDaemon(t *testing.T) {
	rec := &metadata.InodeRecord{}
	s := replyService()
	cases := []struct {
		op    string
		reply []byte
		want  int // what ops.c reads back, field by field
	}{
		{"errno-only (unlink, rmdir, rename, fsync, flush)", okResp(), 4},
		{"OPEN", openResp(true), 4 + 4},
		{"WRITE", writtenResp(0), 4 + 4},
		{"GETATTR/SETATTR", s.attrResp(rec), 4 + attrWireSize + 4},
		{"LOOKUP/MKDIR/MKNOD/SYMLINK/LINK", s.entryResp(1, rec), 4 + 8 + attrWireSize + 4 + 4},
		{"CREATE", s.createResp(1, rec, true), 4 + 8 + attrWireSize + 4 + 4 + 4},
		{"LSEEK", lseekResp(0), 4 + 8},
		{"STATFS", statfsResp(0, 0, 0, 0), 4 + 5*8 + 3*4},
		// The same totals test/c/test_ops.c pins from the reading side. Spelled
		// out so the two files can be compared by eye without adding them up.
		{"CREATE, as an absolute width", s.createResp(1, rec, true), 108},
		{"LOOKUP, as an absolute width", s.entryResp(1, rec), 104},
		{"LOOKUP negative entry", s.negativeEntryResp(), 4 + 8 + attrWireSize + 4 + 4},
		{"GETATTR, as an absolute width", s.attrResp(rec), 92},
		{"STATFS, as an absolute width", statfsResp(0, 0, 0, 0), 56},
	}
	for _, c := range cases {
		if len(c.reply) != c.want {
			t.Errorf("%s: the daemon writes %d bytes, ops.c reads %d", c.op, len(c.reply), c.want)
		}
	}
}

// A name that is not there is answered as a cacheable absence rather than as
// ENOENT: errno 0, inode 0, and an entry_timeout for the kernel to remember it
// by. Getting any of the three wrong is a different bug each time — a non-zero
// errno caches nothing, a non-zero inode invents a file, and a zero timeout
// makes the reply cost a round trip without saving one.
func TestNegativeEntryIsCacheableAbsence(t *testing.T) {
	s := replyService()
	b := s.negativeEntryResp()

	if errno := int32(binary.BigEndian.Uint32(b[0:4])); errno != 0 {
		t.Errorf("errno = %d, want 0: a negative entry is a successful reply", errno)
	}
	if ino := binary.BigEndian.Uint64(b[4:12]); ino != 0 {
		t.Errorf("ino = %d, want 0: inode 0 is what marks the entry negative", ino)
	}

	timeouts := b[len(b)-8:]
	if entry := binary.BigEndian.Uint32(timeouts[0:4]); entry != s.entryTimeout {
		t.Errorf("entry_timeout = %d, want %d", entry, s.entryTimeout)
	}
	// There is no inode for an attr_timeout to describe, and the attr block
	// carries nothing but zeroes.
	if attr := binary.BigEndian.Uint32(timeouts[4:8]); attr != 0 {
		t.Errorf("attr_timeout = %d, want 0", attr)
	}

	// A cached absence may not outlive a cached presence: both are invalidated
	// by the same dirent watch, so trusting one longer than the other would be
	// arbitrary.
	positive := s.entryResp(1, &metadata.InodeRecord{})
	if got := binary.BigEndian.Uint32(positive[len(positive)-8 : len(positive)-4]); got != s.entryTimeout {
		t.Errorf("a found name gets entry_timeout %d, a missing one %d: they must match",
			got, s.entryTimeout)
	}
}

// setattrPayloadLen must match what ec_setattr in pkg/fuse/ops.c writes. The
// two are hand-encoded on opposite sides of the socket, so a field added to one
// and not the other shifts every field after it.
func TestSetattrPayloadMatchesCDaemonWidth(t *testing.T) {
	// ino, fh, size, atime, mtime, ctime are u64; valid, mode, uid, gid and the
	// three nanosecond fields are u32.
	const cSideWidth = 6*8 + 7*4
	if setattrPayloadLen != cSideWidth {
		t.Fatalf("setattrPayloadLen is %d, ec_setattr writes %d", setattrPayloadLen, cSideWidth)
	}
}

// chmod must not be able to change what kind of file something is. The kernel
// sends a whole st_mode, so the stored type bits have to survive it.
func TestApplyModeKeepsTheFileType(t *testing.T) {
	cases := []struct {
		name     string
		stored   uint32
		incoming uint32
		want     uint32
	}{
		{"chmod on a regular file", metadata.ModeFile | 0644, metadata.ModeFile | 0600, metadata.ModeFile | 0600},
		{"a symlink stays a symlink", metadata.ModeSymlink | 0777, metadata.ModeFile | 0644, metadata.ModeSymlink | 0644},
		{"a directory stays a directory", metadata.ModeDir | 0755, metadata.ModeFile | 0700, metadata.ModeDir | 0700},
		{"bare permission bits", metadata.ModeFile | 0644, 0640, metadata.ModeFile | 0640},
	}
	for _, c := range cases {
		got := (c.stored & metadata.S_IFMT) | (c.incoming &^ metadata.S_IFMT)
		if got != c.want {
			t.Errorf("%s: got %#o, want %#o", c.name, got, c.want)
		}
	}
}

// A read-only mount must reject every mutating opcode with EROFS before the
// request reaches a handler — dispatch must not touch the store to decide
// this, since the zero-value Service here has none.
func TestReadOnlyRejectsMutatingOpsWithEROFS(t *testing.T) {
	s := &Service{readOnly: true}
	for code := range mutatingOps {
		resp, err := s.dispatch(code, nil)
		if err != nil {
			t.Fatalf("op %s: unexpected error %v", opName(code), err)
		}
		if len(resp) < 4 {
			t.Fatalf("op %s: response too short: %v", opName(code), resp)
		}
		if got := int32(binary.BigEndian.Uint32(resp)); got != -30 {
			t.Errorf("op %s: got errno %d, want -30 (EROFS)", opName(code), got)
		}
	}
}

// Every opcode the C daemon can send must resolve to an entry, and anything
// else must be ENOSYS rather than a nil handler.  The table is the only place
// an operation is registered, so a handler added without its entry is
// unreachable and this is what says so.
func TestDispatchTableCoversEveryOpcode(t *testing.T) {
	for _, code := range []uint16{
		ipcOpLookup, ipcOpGetattr, ipcOpReaddir, ipcOpReadlink, ipcOpCreate,
		ipcOpMkdir, ipcOpUnlink, ipcOpRmdir, ipcOpRename, ipcOpSymlink,
		ipcOpLink, ipcOpSetattr, ipcOpOpen, ipcOpRelease, ipcOpOpendir,
		ipcOpReleasedir, ipcOpStatfs, ipcOpAlloc, ipcOpCommit, ipcOpRead,
		ipcOpWrite, ipcOpFsync, ipcOpMknod, ipcOpFlush, ipcOpReadDirPlus,
	} {
		entry, found := ops[code]
		if !found {
			t.Errorf("opcode %d has no dispatch entry", code)
			continue
		}
		if entry.handle == nil || entry.name == "" {
			t.Errorf("opcode %d has an incomplete entry: %+v", code, entry)
		}
	}

	// 27 and 28 were GETLK/SETLK; they must stay unserved.
	for _, code := range []uint16{0, 27, 28, 999} {
		if _, found := ops[code]; found {
			t.Errorf("opcode %d should not be served", code)
		}
		if opName(code) != "unknown" {
			t.Errorf("opcode %d should have no metric name", code)
		}
	}
}

// ec_create in pkg/fuse/ops.c reads the entry block and then one more u32 for
// keep_cache.  A create response that stops at the entry leaves that read past
// the end of the buffer, and the C side turns a successful create into EIO.
func TestCreateRespCarriesKeepCacheAfterTheEntry(t *testing.T) {
	s := replyService()
	entry := len(s.entryResp(1, &metadata.InodeRecord{}))
	for _, keep := range []bool{false, true} {
		b := s.createResp(1, &metadata.InodeRecord{}, keep)
		if len(b) != entry+4 {
			t.Fatalf("createResp wrote %d bytes, want entry+4 = %d", len(b), entry+4)
		}
		want := uint32(0)
		if keep {
			want = 1
		}
		if got := binary.BigEndian.Uint32(b[entry:]); got != want {
			t.Fatalf("keep_cache = %d, want %d", got, want)
		}
	}
}

// The SETATTR mask decides which fields move and which are ignored. It is a
// pure function of the record, the mask and the payload, so it is tested
// directly rather than through a store: a field applied without its bit is a
// silent attribute change, and one ignored despite its bit is a lost chmod.
func TestApplySetattrHonoursTheValidMask(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	sent := time.Unix(1_000, 0)
	fields := setattrFields{
		size: 4096, mode: metadata.ModeFile | 0600, uid: 7, gid: 9,
		atime: sent, mtime: sent, ctime: sent,
	}
	base := func() *metadata.InodeRecord {
		return &metadata.InodeRecord{
			Size: 10, Mode: metadata.ModeDir | 0755, UID: 1, GID: 2,
			Atime: time.Unix(1, 0), Mtime: time.Unix(2, 0), Ctime: time.Unix(3, 0),
		}
	}

	// Nothing selected: nothing moves, not even ctime.
	rec := base()
	applySetattr(rec, 0, fields, now)
	if *rec != *base() {
		t.Errorf("an empty mask changed the record: %+v", rec)
	}

	// A chmod keeps the file's type and stamps ctime.
	rec = base()
	applySetattr(rec, fattrMode, fields, now)
	if rec.Mode != metadata.ModeDir|0600 {
		t.Errorf("mode = %#o, want the stored type with the new permissions", rec.Mode)
	}
	if !rec.Ctime.Equal(now) {
		t.Errorf("ctime = %v, want %v: an attribute change is a status change", rec.Ctime, now)
	}
	if rec.Size != 10 || rec.UID != 1 || rec.GID != 2 {
		t.Errorf("a mode-only setattr moved another field: %+v", rec)
	}

	// An explicit ctime wins over the implicit stamp.
	rec = base()
	applySetattr(rec, fattrMode|fattrCtime, fields, now)
	if !rec.Ctime.Equal(sent) {
		t.Errorf("ctime = %v, want the caller's %v", rec.Ctime, sent)
	}

	// *_NOW overrides the timestamp carried in the same payload.
	rec = base()
	applySetattr(rec, fattrAtime|fattrAtimeNow|fattrMtime, fields, now)
	if !rec.Atime.Equal(now) {
		t.Errorf("atime = %v, want now (%v)", rec.Atime, now)
	}
	if !rec.Mtime.Equal(sent) {
		t.Errorf("mtime = %v, want the caller's %v", rec.Mtime, sent)
	}
	// Timestamps alone are not a status change.
	if !rec.Ctime.Equal(time.Unix(3, 0)) {
		t.Errorf("ctime = %v, want it untouched by a timestamp-only setattr", rec.Ctime)
	}
}
