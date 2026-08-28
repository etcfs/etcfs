package ipc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/etcfs/etcfs/internal/config"
	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/metrics"
)

// opcodes matching pkg/fuse/ops.c
const (
	ipcOpLookup     = 1
	ipcOpGetattr    = 2
	ipcOpReaddir    = 3
	ipcOpReadlink   = 4
	ipcOpCreate     = 5
	ipcOpMkdir      = 6
	ipcOpUnlink     = 7
	ipcOpRmdir      = 8
	ipcOpRename     = 9
	ipcOpSymlink    = 10
	ipcOpLink       = 11
	ipcOpSetattr    = 12
	ipcOpOpen       = 13
	ipcOpRelease    = 14
	ipcOpOpendir    = 15
	ipcOpReleasedir = 16
	ipcOpStatfs     = 17
	ipcOpAlloc      = 18
	ipcOpCommit     = 19
	// 27 and 28 were GETLK/SETLK, removed so the kernel handles fcntl() locks
	// locally; not reused, so an old C daemon's lock request fails loudly.
	ipcOpRead        = 22
	ipcOpWrite       = 23
	ipcOpFsync       = 24
	ipcOpMknod       = 25
	ipcOpFlush       = 26
	ipcOpReadDirPlus = 29

	ipcOpSetxattr    = 30
	ipcOpGetxattr    = 31
	ipcOpListxattr   = 32
	ipcOpRemovexattr = 33
	ipcOpLseek       = 34
	ipcOpFallocate   = 35
)

// op describes one IPC operation: the name its metrics and logs carry, and the
// handler that serves it.
//
// A table rather than a switch because adding an operation used to mean editing
// the dispatch switch, its metrics labels, and the opcode list separately, on
// code that was already tested.  One entry now covers all three, and an opcode
// with no entry is ENOSYS by construction rather than by a default branch
// someone has to remember to keep correct.
//
// Payload bounds are not declared here: every handler decodes through the
// reader in frame.go, which refuses to read past the payload it was given and
// leaves the handler to reply EINVAL.  Declaring an arity per entry as well
// would be a second copy of that, one that could disagree with the decoder.
type op struct {
	name   string
	handle func(*Service, context.Context, []byte) ([]byte, error)
}

// ok answers an operation that has no distributed state to keep: opening and
// closing a directory take no locks and defer nothing.
func ok(*Service, context.Context, []byte) ([]byte, error) { return okResp(), nil }

// unsupported answers an opcode the backend deliberately does not serve.
func unsupported(*Service, context.Context, []byte) ([]byte, error) {
	return int32Resp(errNoSys), nil
}

var ops = map[uint16]op{
	ipcOpLookup:      {"lookup", (*Service).handleLookup},
	ipcOpGetattr:     {"getattr", (*Service).handleGetattr},
	ipcOpReaddir:     {"readdir", (*Service).handleReaddir},
	ipcOpReadDirPlus: {"readdirplus", (*Service).handleReaddirPlus},
	ipcOpReadlink:    {"readlink", (*Service).handleReadlink},
	ipcOpStatfs:      {"statfs", (*Service).handleStatfs},
	ipcOpRead:        {"read", (*Service).handleRead},
	ipcOpCreate:      {"create", (*Service).handleCreate},
	ipcOpMkdir:       {"mkdir", (*Service).handleMkdir},
	ipcOpUnlink:      {"unlink", (*Service).handleUnlink},
	ipcOpRmdir:       {"rmdir", (*Service).handleRmdir},
	ipcOpRename:      {"rename", (*Service).handleRename},
	ipcOpWrite:       {"write", (*Service).handleWrite},
	ipcOpSetattr:     {"setattr", (*Service).handleSetattr},
	ipcOpSymlink:     {"symlink", (*Service).handleSymlink},
	ipcOpLink:        {"link", (*Service).handleLink},
	ipcOpMknod:       {"mknod", (*Service).handleMknod},
	ipcOpSetxattr:    {"setxattr", (*Service).handleSetxattr},
	ipcOpGetxattr:    {"getxattr", (*Service).handleGetxattr},
	ipcOpListxattr:   {"listxattr", (*Service).handleListxattr},
	ipcOpRemovexattr: {"removexattr", (*Service).handleRemovexattr},
	ipcOpLseek:       {"lseek", (*Service).handleLseek},
	ipcOpFallocate:   {"fallocate", (*Service).handleFallocate},
	ipcOpOpen:        {"open", (*Service).handleOpen},
	ipcOpOpendir:     {"opendir", ok},
	ipcOpRelease:     {"release", (*Service).handleRelease},
	ipcOpReleasedir:  {"releasedir", ok},
	// Both publish this inode's deferred writes and block until they are in
	// etcd.  flush arrives on every close(), fsync on the call of that name;
	// neither can be answered locally once a write may be sitting in RAM.
	ipcOpFlush: {"flush", (*Service).handleFlush},
	ipcOpFsync: {"fsync", (*Service).handleFsync},
	// Block allocation happens inside the WRITE handler, not as a separate
	// request; these opcodes are unused.
	ipcOpAlloc:  {"alloc", unsupported},
	ipcOpCommit: {"commit", unsupported},
}

// mutatingOps are the opcodes a read-only mount rejects with EROFS instead of
// dispatching. Everything else (lookups, reads, release/flush, statfs) is safe
// to serve: it changes no metadata and touches the device only to read it.
// fsync and flush do commit metadata, but only metadata a write left buffered,
// and a read-only mount rejects every write — so they have nothing to publish
// and refusing them would only break close().
// Open is here because the daemon sends it only for O_TRUNC, which empties the
// file.
var mutatingOps = map[uint16]bool{
	ipcOpCreate:      true,
	ipcOpMkdir:       true,
	ipcOpUnlink:      true,
	ipcOpRmdir:       true,
	ipcOpRename:      true,
	ipcOpSymlink:     true,
	ipcOpLink:        true,
	ipcOpMknod:       true,
	ipcOpSetattr:     true,
	ipcOpWrite:       true,
	ipcOpSetxattr:    true,
	ipcOpRemovexattr: true,
	ipcOpFallocate:   true,
	ipcOpOpen:        true,
}

func opName(code uint16) string {
	if o, found := ops[code]; found {
		return o.name
	}
	return "unknown"
}

// RunSocket starts a raw binary IPC server on the given listener.
// Each connection is handled in its own goroutine.
func (s *Service) RunSocket(listener net.Listener) error {
	s.serving.Store(true)
	defer s.serving.Store(false)
	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("accept: %w", err)
		}
		go s.handleConn(conn)
	}
}

func (s *Service) handleConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	s.log.Info("ipc connection accepted", "remote", conn.RemoteAddr())

	for {
		code, payload, err := recvReq(conn)
		if err != nil {
			if err != io.EOF {
				s.log.Warn("ipc recv", "error", err)
			}
			return
		}

		resp, err := s.observedDispatch(code, payload)
		if err != nil {
			s.log.Warn("ipc dispatch", "op", code, "error", err)
			return
		}

		if resp != nil {
			if err := sendResp(conn, resp); err != nil {
				s.log.Warn("ipc send", "error", err)
				return
			}
		}
	}
}

// observedDispatch records the operation, its latency, and whether it returned
// an errno.  Wrapped around the whole dispatch rather than added to each
// handler: every FUSE operation this daemon serves passes through here, so one
// wrapper covers all of them and no new handler can forget to.
func (s *Service) observedDispatch(code uint16, payload []byte) ([]byte, error) {
	name := opName(code)
	start := time.Now()
	resp, err := s.safeDispatch(code, payload)
	// The history is recorded here for the same reason the metrics are: every
	// operation this daemon serves passes through this one wrapper, so no
	// handler can be added that forgets to appear in it.
	s.history.Record(name, code, start, time.Now(), payload, resp)
	metrics.FuseOps.WithLabelValues(name).Inc()
	metrics.FuseOpDuration.WithLabelValues(name).Observe(time.Since(start).Seconds())
	// Every response frame starts with an int32 errno, negative on failure.
	// Big-endian, like every other integer on this wire: read little-endian
	// this happened to give the right sign for errnos up to 128, because the
	// 0xFF bytes land where the sign bit is read from, and the wrong one for
	// everything above it.
	if err != nil || (len(resp) >= 4 && int32(binary.BigEndian.Uint32(resp)) < 0) {
		metrics.FuseErrors.WithLabelValues(name).Inc()
	}
	return resp, err
}

// safeDispatch runs a handler and turns a panic into one failed request.
//
// Connections are served one goroutine each with no recovery of their own, so
// an unrecovered panic ends the whole process — every mount this daemon serves,
// not just the request that tripped it.  The payload readers make that
// unreachable through a malformed frame; this is the backstop for everything
// else, and it logs loudly because reaching it is a bug either way.
func (s *Service) safeDispatch(code uint16, payload []byte) (resp []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			s.log.Error("ipc handler panicked", "op", opName(code), "payload_len", len(payload),
				"panic", p, "stack", string(debug.Stack()))
			resp, err = int32Resp(errIO), nil // EIO for this request only
		}
	}()
	return s.dispatch(code, payload)
}

// ListenPrivate binds a Unix socket only its owner can use.
//
// The permissions are set by the umask around the bind rather than by a chmod
// after it: between bind and chmod the socket carries the process umask, and
// anything able to connect in that window is already past the only access
// control there is.  The parent directory is created 0700 for the same reason —
// a socket is only as private as the directory holding it.
func ListenPrivate(sockPath string) (net.Listener, error) {
	if dir := filepath.Dir(sockPath); dir != "." && dir != "/" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, fmt.Errorf("create socket directory %s: %w", dir, err)
		}
	}
	_ = os.Remove(sockPath)

	old := syscall.Umask(0177)
	listener, err := net.Listen("unix", sockPath)
	syscall.Umask(old)
	if err != nil {
		return nil, fmt.Errorf("listen unix %s: %w", sockPath, err)
	}
	return listener, nil
}

// ---- wire format ----
//
// Request:  [u16:be opcode][u32:be payload_len][payload]
// Response: [u32:be payload_len][payload]

// maxFrameLen caps what one request may claim to carry.  The length is read
// straight off the wire and used as an allocation size, so an unbounded field
// lets a desynchronised peer ask for 4 GiB.  The largest legitimate frame is a
// write payload plus its fixed header, and the C daemon caps its own reads at
// the same number.  The +28 is that header (ino + offset + size + uid + flags,
// see ec_write's payload in pkg/fuse/ops.c) — without it, a write of exactly
// the negotiated max_write size (1 MiB) is rejected as over the limit.
const maxFrameLen = 1<<20 + 28 // 1 MiB write payload + its fixed header

func recvReq(r io.Reader) (uint16, []byte, error) {
	var hdr [6]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	op := binary.BigEndian.Uint16(hdr[0:2])
	plen := binary.BigEndian.Uint32(hdr[2:6])
	if plen > maxFrameLen {
		// The stream is desynchronised: whatever follows is not a frame
		// boundary, so the connection cannot be recovered by skipping ahead.
		return 0, nil, fmt.Errorf("ipc frame of %d bytes exceeds the %d byte limit", plen, maxFrameLen)
	}

	payload := make([]byte, plen)
	if plen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, err
		}
	}
	return op, payload, nil
}

func sendResp(w io.Writer, data []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return nil
}

// ---- payload readers ----

// reader walks a request payload without ever slicing past its end.
//
// The peer is the local C daemon over a 0600 socket, so a malformed frame is a
// protocol desync rather than an attack — but a desync of exactly this kind has
// happened before, in the readdirplus parser, and every handler used to slice
// with a length field it had not checked.  One out-of-range length panicked the
// connection goroutine, and an unrecovered panic takes down the daemon serving
// every mount on the node.
//
// Once a read has run past the end, ok stays false and every later read returns
// a zero value, so a handler only has to test ok once, before it acts.
type reader struct {
	b  []byte
	ok bool
}

func newReader(payload []byte) *reader {
	return &reader{b: payload, ok: true}
}

func (r *reader) u32() uint32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

func (r *reader) u64() uint64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// str reads a u32 length followed by that many bytes — the encoding every name
// and symlink target uses.
func (r *reader) str() string {
	n := r.u32()
	b := r.take(int(n))
	if b == nil {
		return ""
	}
	return string(b)
}

// blob is str's counterpart for payload data, returning the bytes themselves
// rather than a copy of them as a string.
func (r *reader) blob() []byte {
	n := r.u32()
	b := r.take(int(n))
	if b == nil {
		return nil
	}
	return b
}

func (r *reader) take(n int) []byte {
	if !r.ok || n < 0 || n > len(r.b) {
		r.ok = false
		return nil
	}
	b := r.b[:n]
	r.b = r.b[n:]
	return b
}

// ---- payload writers ----

type buf struct {
	b []byte
}

func (b *buf) w32(v uint32) {
	b.b = append(b.b,
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (b *buf) w64(v uint64) {
	b.b = append(b.b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (b *buf) wAttr(rec *metadata.InodeRecord) {
	b.w64(rec.Ino)
	b.w64(rec.Size)
	b.w64(rec.Blocks)
	b.w64(uint64(rec.Atime.Unix()))
	b.w64(uint64(rec.Mtime.Unix()))
	b.w64(uint64(rec.Ctime.Unix()))
	b.w32(uint32(rec.Atime.Nanosecond()))
	b.w32(uint32(rec.Mtime.Nanosecond()))
	b.w32(uint32(rec.Ctime.Nanosecond()))
	b.w32(rec.Mode)
	b.w32(rec.Nlink)
	b.w32(rec.UID)
	b.w32(rec.GID)
	b.w32(rec.Rdev)
	b.w32(rec.Blksize)
}

// ---- response helpers ----

func int32Resp(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

func okResp() []byte {
	return int32Resp(0)
}

// openResp answers an OPEN: [i32:error][u32:keep_cache]
//
// The flag decides whether the kernel may hold this file's data pages across
// reads.  The daemon decides it rather than the C side, because only the daemon
// knows whether it can take them back again.
func openResp(keepCache bool) []byte {
	var b buf
	b.w32(0)
	if keepCache {
		b.w32(1)
	} else {
		b.w32(0)
	}
	return b.b
}

// writtenResp answers a WRITE: [i32:error][u32:written]
func writtenResp(written uint32) []byte {
	var b buf
	b.w32(0)
	b.w32(written)
	return b.b
}

// dataResp answers a READ: [i32:error][u32:len][bytes]
func dataResp(data []byte) []byte {
	var b buf
	b.w32(0)
	b.w32(uint32(len(data)))
	b.b = append(b.b, data...)
	return b.b
}

// createResp answers a CREATE: an entry with the OPEN keep_cache flag appended,
// because a create hands back an open descriptor and that descriptor needs the
// same answer an open would have got.
//
//	[i32:error][u64:ino][attr][u32:entry_timeout][u32:attr_timeout][u32:keep_cache]
func (s *Service) createResp(ino uint64, rec *metadata.InodeRecord, keepCache bool) []byte {
	b := s.entryResp(ino, rec)
	if keepCache {
		return append(b, 0, 0, 0, 1)
	}
	return append(b, 0, 0, 0, 0)
}

// lseekResp answers an LSEEK: [i32:error][u64:offset]
func lseekResp(offset uint64) []byte {
	var b buf
	b.w32(0)
	b.w64(offset)
	return b.b
}

// statfsResp answers a STATFS:
//
//	[i32:error][u64:blocks][u64:bfree][u64:bavail][u64:files][u64:ffree]
//	[u32:bsize][u32:namelen][u32:frsize]
//
// bavail repeats bfree: EtcFS reserves nothing for root, so the space an
// unprivileged writer may use is the space that is free.
func statfsResp(blocks, bfree, files, ffree uint64) []byte {
	var b buf
	b.w32(0)
	b.w64(blocks)
	b.w64(bfree)
	b.w64(bfree)
	b.w64(files)
	b.w64(ffree)
	b.w32(4096) // bsize
	b.w32(255)  // namelen
	b.w32(4096) // frsize
	return b.b
}

// How long the kernel may answer from its own caches before asking this daemon
// again, in seconds, as the reply carries it.
//
// Both are backed by a cluster-wide watch, which is what makes a long value
// defensible: a name changed anywhere reaches every node as an INVAL_ENTRY, and
// an inode record changed anywhere reaches it as an INVAL_ATTR.  The watches
// resume from where they stopped when etcd ends them (see watch.go), so a
// timeout is the fail-safe for a watch that could not be resumed at all rather
// than the mechanism coherence rests on.
//
// They used to be one second each, and the attribute one had to be: nothing
// watched an inode's attributes, so that timeout was the whole guarantee.  One
// second is also shorter than any walk of a real tree, which is why a warm
// `find` or `du` over eighty thousand files cost exactly what a cold one did —
// every name and every attribute had expired before the walk came back to it.
//
// The defaults are in internal/config, next to the flags that carry them.

// entryResp answers every op that returns a directory entry — LOOKUP, MKDIR,
// MKNOD, SYMLINK, LINK, and CREATE through createResp:
//
//	[i32:error][u64:ino][attr][u32:entry_timeout][u32:attr_timeout]
//
// The attr block is fixed-width, so a missing inode record is sent as a zeroed
// one rather than omitted; a short response would desynchronise the C parser.
func (s *Service) entryResp(ino uint64, rec *metadata.InodeRecord) []byte {
	if rec == nil {
		rec = &metadata.InodeRecord{Ino: ino}
	}
	var b buf
	b.w32(0) // success
	b.w64(ino)
	b.wAttr(rec)
	b.w32(s.entryTimeout)
	b.w32(s.attrTimeout)
	return b.b
}

// negativeEntryResp answers a LOOKUP for a name that is not there, in the form
// that lets the kernel remember the absence.
//
// FUSE reserves inode 0 in an entry reply for exactly this: the reply means
// "no such name", and its entry_timeout says for how long further lookups of
// that name may be answered without asking again.  Returning ENOENT instead —
// which is what this did — leaves the kernel nothing to cache, so a program
// that repeatedly probes for files that do not exist (a compiler walking an
// include path, a package manager looking for an optional config) pays an IPC
// round trip and an etcd read on every probe.
//
// The timeout is the same one a *found* name gets, and deliberately so: both
// are invalidated by the same dirent watch, so nothing justifies trusting one
// longer than the other.  The window either way is one second of a name's
// existence being stale on this node, which is the guarantee positive entries
// have always had.
//
// attr_timeout is zero because there is no inode for it to describe; the
// zeroed attr block is there only to keep the reply the width the C parser
// expects.
func (s *Service) negativeEntryResp() []byte {
	var b buf
	b.w32(0) // the lookup was answered — the name simply is not there
	b.w64(0) // ino 0: negative entry
	b.wAttr(&metadata.InodeRecord{})
	b.w32(s.entryTimeout)
	b.w32(0)
	return b.b
}

func (s *Service) attrResp(rec *metadata.InodeRecord) []byte {
	var b buf
	b.w32(0) // error = success
	b.wAttr(rec)
	b.w32(s.attrTimeout)
	return b.b
}

// ---- dispatch ----

func (s *Service) dispatch(code uint16, payload []byte) ([]byte, error) {
	o, found := ops[code]
	if !found {
		return int32Resp(errNoSys), nil
	}
	if s.readOnly && mutatingOps[code] {
		return int32Resp(errROFS), nil
	}

	// Bounded here rather than at each of the ~35 individual store calls the
	// handlers make: a context without a deadline blocks for as long as the
	// etcd client will keep retrying, which under a partition is indefinitely,
	// and every handler inherits this one.  Calls that manage their own budget
	// (commitGuarded, a flush) build their contexts from Background and are
	// deliberately not truncated by this deadline — a commit already in flight
	// should finish or fail on its own terms rather than be cut mid-transaction.
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	return o.handle(s, ctx, payload)
}

// StartSocketServer is the public entry point: listens on the Unix socket path
// and blocks until ctx is cancelled or the listener errors.
//
// RunSocket's Accept loop has no ctx of its own — closing the listener from a
// side goroutine when ctx is cancelled is what turns "the process received
// SIGTERM" into "Accept returns an error and RunSocket returns", which is what
// lets main's post-serve shutdown steps (releasing this node's arena) run at
// all. Without this, main blocked in RunSocket forever and never reached them
// on anything short of SIGKILL.
func StartSocketServer(ctx context.Context, svc *Service, sockPath string, log *config.Logger) error {
	listener, err := ListenPrivate(sockPath)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	log.Info("binary IPC server listening", "path", sockPath)
	if err := svc.RunSocket(listener); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}
