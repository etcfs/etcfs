package ipc

import (
	"time"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// Synthetic history events for the three things caching made unobservable from
// the IPC socket alone: how long the etcd lock key is really held, when the
// kernel's data pages for an inode are dropped, and when a block is reserved or
// given back.
//
// None of them cross the socket, so each carries a made-up opcode far above the
// real ones, in the same range as historyOpLockHold.  The decoders live in
// test/verify and read these bytes independently — see decode.go for why that
// duplication is deliberate.
//
// Recording is free when no history is being written, which is every run except
// a verification one: Recorder.Record on a nil recorder returns immediately.
const (
	historyOpLockKey   = 1002
	historyOpPageInval = 1003
	historyOpBlock     = 1004
)

const (
	blockEventReserve = 0
	blockEventFree    = 1
)

// Page-invalidation outcomes, as the response byte of a page_inval event.
const (
	pageInvalDone      = 0 // the kernel dropped the inode's pages
	pageInvalNoClient  = 1 // no FUSE session is connected, so no pages exist
	pageInvalFailed    = 2 // the client is there and reported a failure
	pageInvalNotCached = 3 // caching is off, or no open ever let the kernel cache
)

// recordKeyEvent appends one endpoint of the *cached* hold of an inode's etcd
// lock key, which outlives the operation that acquired it.
//
// The per-operation events (recordLockEvent) span a subset of this interval, so
// a checker fed only those can report mutual exclusion but never notice a key
// held past the operation that took it.  Recording both streams is what makes
// the cached hold checkable as the thing that actually excludes peers.
// call may be widened back to the acquisition when the key turns out to have
// expired under the node, so that a peer's legitimate acquisition can be
// ordered inside it. That widening is right for mutual exclusion and wrong for
// anything asking what this node did just before letting go, so the instant the
// release actually started is carried alongside it as actualCall.
func (s *Service) recordKeyEvent(ino uint64, mode metadata.LockMode, event byte, call, ret time.Time, actualCall time.Time) {
	if s.history == nil {
		return
	}
	var b buf
	b.b = append(b.b, event, lockModeByte(mode))
	b.w64(ino)
	b.w64(uint64(actualCall.UnixNano()))
	s.history.Record("lock_key", historyOpLockKey, call, ret, b.b, nil)
}

// recordPageInval appends a kernel page-cache invalidation, so that "no node
// serves a cached page for an inode after yielding its lock" can be checked at
// all.  Without the event there is nothing in the history between the last read
// and the release to say the pages went.
func (s *Service) recordPageInval(ino uint64, outcome byte, call, ret time.Time) {
	if s.history == nil {
		return
	}
	var b buf
	b.w64(ino)
	s.history.Record("page_inval", historyOpPageInval, call, ret, b.b, []byte{outcome})
}

// recordBlockEvent appends a reservation or a release of a device range, which
// together are a block's whole lifetime as far as this node is concerned.
func (s *Service) recordBlockEvent(event byte, diskOff, length uint64) {
	if s.history == nil || length == 0 {
		return
	}
	now := time.Now()
	var b buf
	b.b = append(b.b, event)
	b.w64(diskOff)
	b.w64(length)
	s.history.Record("block", historyOpBlock, now, now, b.b, nil)
}

// freeBlocks returns a device range to the arena.  Every release goes through
// here so that the history sees all of them: a free recorded in one caller and
// missed in another would read as a block freed twice.
func (s *Service) freeBlocks(diskOff, length uint64) {
	s.recordBlockEvent(blockEventFree, diskOff, length)
	s.alloc.Free(diskOff, length)
}
