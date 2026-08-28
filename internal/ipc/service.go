// Package ipc implements the binary IPC server that the C FUSE daemon calls.
//
// The C daemon (etcfuse) opens a Unix domain socket to this service and
// sends binary-framed requests for each FUSE operation.  This service
// translates those requests into etcd operations and returns binary responses.
package ipc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/internal/config"
	"github.com/etcfs/etcfs/internal/history"
	"github.com/etcfs/etcfs/pkg/arena"
	"github.com/etcfs/etcfs/pkg/blockio"
	"github.com/etcfs/etcfs/pkg/fencing"
	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/metrics"
)

// Service handles FUSE operation requests from the C daemon.
type Service struct {
	store        *metadata.Store
	membership   *metadata.Membership
	watchdog     *fencing.Watchdog
	alloc        *arena.Allocator
	log          *config.Logger
	dev          *blockio.Device
	notifyServer *notifyServer

	// writeBarriers adds a device flush, a range sync and a readback to every
	// write, and a device flush to every read.  See writeRun.
	writeBarriers bool

	// flushInterval is how long an inode's extents may stay buffered in RAM
	// before they are published to etcd.  Zero disables deferral entirely, so
	// every write commits before it is acknowledged.  See delegate.go.
	flushInterval time.Duration
	// flushMaxBytes caps the write payload one buffer may stand for, so a hot
	// inode cannot turn an unbounded amount of acknowledged data into data that
	// a crash would lose.  With dataCache on it is also the bound on how much
	// RAM one inode's buffered payload may occupy, and the backpressure: a write
	// past the cap waits for the flush that makes room for it.
	flushMaxBytes uint64

	// bufferMaxBytes caps the buffered payload across every inode at once, and
	// bufferedOps/bufferedBytes are the running totals it is compared against.
	// The per-inode cap bounds one hot file; without this one, a workload with
	// many hot files holds an unbounded amount of acknowledged-but-unpublished
	// data in RAM.
	bufferMaxBytes int64
	bufferedOps    atomic.Int64
	bufferedBytes  atomic.Int64

	// dataCache buffers a deferred write's payload in RAM alongside its extents
	// instead of putting it on the device as the write is served, so a write
	// costs no device I/O either.  The flush writes the bytes before it
	// publishes the extents naming them, which is the ordering that keeps a lost
	// write a lost write rather than a read of garbage.
	dataCache bool

	// entryTimeout and attrTimeout are how many seconds the kernel may answer a
	// name's existence and an inode's attributes from its own caches before
	// asking again.  Both are backed by a cluster-wide watch that invalidates
	// them, so they bound how long a watch that could not be resumed leaves this
	// node answering from a cache nothing has corrected.  See socket.go.
	entryTimeout uint32
	attrTimeout  uint32

	// pageCache lets the kernel keep an inode's data pages across reads while
	// this node holds its lock, so a re-read costs nothing at all.  What makes
	// it sound is that the pages are invalidated before the lock is yielded;
	// what makes it possible to switch off is that it buys nothing for a
	// workload whose reads are O_DIRECT and does not defend itself.
	pageCache bool
	// pagesCached records that at least one open was told the kernel may cache
	// this filesystem's data, so a later release knows it cannot skip the
	// invalidation just because no client happens to be connected now.
	pagesCached atomic.Bool
	// noPageCacheLogged keeps the "opens are not cacheable" warning to one per
	// outage rather than one per open.  Cleared again when an open is answered
	// as cacheable, so a second outage is reported like the first.
	noPageCacheLogged atomic.Bool

	// serving records that the IPC socket has been accepted on, which is the
	// point from which a mount can actually be answered.  Reported by the
	// readiness endpoint; an orchestrator that routes work here before it is
	// true gets EIO from a daemon that is merely still starting.
	serving atomic.Bool

	// readOnly rejects every mutating opcode with EROFS before it reaches a
	// handler. Checked in dispatch rather than per-handler so a new mutating
	// operation is safe by default: it must be added to mutatingOps to be
	// servable at all, at which point read-only coverage is a one-line review
	// rather than a call site to remember.
	readOnly bool

	// history records every served operation for offline consistency checking.
	// Nil unless --history-log was given, and a nil recorder records nothing.
	history *history.Recorder

	// open counts this node's open descriptors per inode, so an unlink of the
	// last name can keep the record alive until the last one closes.
	open *openFiles

	// inodes hands out inode numbers from a block reserved in etcd, so a create
	// does not wait on Raft for its number. See inodealloc.go.
	inodes *inodeBlocks

	// locks caches this node's inode locks past the operations that took them,
	// so a repeat acquisition costs no etcd round trip. See lockcache.go for
	// what a cached lock obliges this node to do before giving one up.
	locks *lockMap

	// recalls names the inodes with a recall already in flight. See
	// StartLockRevocation.
	recalls *recallSet

	// dirents remembers directory name sets, so a lookup of a name that is not
	// there is answered without reaching etcd. See direntcache.go.
	dirents *direntCache

	// dirCursors remembers where each directory's last listing stopped, so a
	// sequential scan reads forward instead of counting from the start. See
	// readdircursor.go.
	dirCursors *dirCursors

	// Fencing generation this node started with.  Every data-path commit is
	// guarded against it, so once the fencing controller bumps gen:<node_id>
	// this node's commits stop being accepted by etcd.
	genMu    sync.Mutex
	genInit  bool
	startGen uint64
}

// DefaultFlushInterval is the flush interval a daemon runs with unless it is
// configured otherwise.  Stated here because it is a property of write
// delegation rather than of the flag that carries it.
const DefaultFlushInterval = defaultFlushInterval

// Options is everything about a Service that is decided once, before it serves
// anything, and never again.
//
// These used to be six setters called on a constructed Service. Nothing made
// them run before the socket started accepting, and each stayed writable for
// the process's lifetime — so "is the page cache on?" was a question with no
// answer that held. Passing them here makes the configuration a property of the
// Service rather than a sequence of calls someone has to get right.
//
// Every field means exactly what it says: a zero FlushInterval commits each
// write before acknowledging it rather than selecting a default.
type Options struct {
	// FlushInterval bounds how long an acknowledged write may stay unpublished.
	// Zero commits every write before acknowledging it, which loses nothing to a
	// crash and pays a Raft commit per write.
	FlushInterval time.Duration

	// DataCache buffers a deferred write's payload in RAM alongside its extents.
	// Off puts the bytes on the device as the write is served.
	DataCache bool

	// PageCache lets the kernel hold an inode's data pages while this node holds
	// its lock. Off sends every read through to the daemon.
	PageCache bool

	// EntryTimeout and AttrTimeout bound how long the kernel may answer a name's
	// existence and an inode's attributes without asking. Zero selects
	// config.DefaultEntryTimeout and config.DefaultAttrTimeout; sub-second
	// values are rounded down to whole seconds, which is what FUSE carries.
	EntryTimeout time.Duration
	AttrTimeout  time.Duration

	// ReadOnly rejects every mutating opcode with EROFS, for mounting a
	// filesystem for backup or inspection while another node writes.
	ReadOnly bool

	// Device is the shared block device. Nil is metadata-only mode, where a
	// write updates the inode's size and nothing else.
	Device *blockio.Device

	// WriteBarriers adds a flush, a range sync and a readback to every write. It
	// is forced on for a device opened without O_DIRECT, where the bytes really
	// do sit in this node's page cache until something pushes them out.
	WriteBarriers bool

	// History records every served operation for offline consistency checking.
	History *history.Recorder
}

// NewService creates a Service that will serve under opts.
func NewService(store *metadata.Store, membership *metadata.Membership,
	watchdog *fencing.Watchdog, log *config.Logger, opts Options) *Service {
	s := &Service{
		store:          store,
		membership:     membership,
		watchdog:       watchdog,
		alloc:          arena.NewAllocator(membership.NodeID(), store),
		log:            log,
		open:           newOpenFiles(),
		inodes:         newInodeBlocks(store),
		recalls:        newRecallSet(),
		dirCursors:     newDirCursors(),
		dirents:        newDirentCache(entryTimeout(opts)),
		notifyServer:   &notifyServer{},
		flushInterval:  opts.FlushInterval,
		flushMaxBytes:  defaultFlushMaxBytes,
		bufferMaxBytes: defaultBufferMaxBytes,
		dataCache:      opts.DataCache,
		entryTimeout:   timeoutSecs(opts.EntryTimeout, config.DefaultEntryTimeout),
		attrTimeout:    timeoutSecs(opts.AttrTimeout, config.DefaultAttrTimeout),
		pageCache:      opts.PageCache,
		readOnly:       opts.ReadOnly,
		history:        opts.History,
	}
	s.locks = newLockMap(s.dropCachedLocks)
	if opts.Device != nil {
		s.setBlockDevice(opts.Device, opts.WriteBarriers)
	}
	return s
}

// entryTimeout is how long a name's existence may be answered without asking
// etcd, as a duration rather than the whole seconds the wire carries.
//
// The dirent cache is bounded by it for the same reason the kernel's negative
// dentry is: both answer an absence, both are invalidated by the same watch,
// and trusting one longer than the other would be arbitrary.
func entryTimeout(opts Options) time.Duration {
	if opts.EntryTimeout == 0 {
		return config.DefaultEntryTimeout
	}
	return opts.EntryTimeout
}

// timeoutSecs converts a cache timeout to the whole seconds the FUSE reply
// carries, substituting the default for an unset one.  A value below a second
// but above zero becomes zero, which is "do not cache" — the caller asked for
// less than the wire can express, and rounding *up* would hand out a longer
// timeout than was asked for.
func timeoutSecs(d, fallback time.Duration) uint32 {
	if d == 0 {
		d = fallback
	}
	if d < 0 {
		return 0
	}
	return uint32(d / time.Second)
}

// pagesCacheable reports whether an invalidation could have anything to do.
func (s *Service) pagesCacheable() bool {
	return s.pageCache && s.pagesCached.Load()
}

// setBlockDevice attaches the block device for data I/O.
//
// barriers is forced on for a device opened without O_DIRECT: there the bytes
// really do sit in this node's page cache until something pushes them out.
func (s *Service) setBlockDevice(dev *blockio.Device, barriers bool) {
	s.dev = dev
	s.writeBarriers = barriers || !dev.IsDirect()
	// The allocator hands out arenas by multiplying an ID by the arena size,
	// with nothing else to stop it running past the end of the device.
	s.alloc.SetDeviceSize(uint64(dev.TotalSize()))
}

// refreshDeviceSize re-reads the device size and tells the allocator about it,
// reporting whether the device turned out to be larger than what the allocator
// was working with.
//
// A shared volume can be grown while the cluster stays mounted, and the size
// read at construction would otherwise hold until every daemon restarted.
// Running this only when an allocation has already failed for space keeps the
// ioctl off the write path: a filesystem that is not full never pays for it.
func (s *Service) refreshDeviceSize() bool {
	if s.dev == nil {
		return false
	}
	size, err := s.dev.RefreshSize()
	if err != nil {
		s.log.Warn("cannot re-read the device size", "error", err)
		return false
	}
	if uint64(size) <= s.alloc.DeviceSize() {
		return false
	}
	s.log.Info("the device has grown", "bytes", size)
	s.alloc.SetDeviceSize(uint64(size))
	return true
}

// ReclaimOrphans deletes the inodes a previous incarnation of this node was
// keeping alive for open descriptors that died with it. Nothing names them and
// no descriptor survives a restart, so they are pure leak from here on.
func (s *Service) ReclaimOrphans(ctx context.Context) {
	inos, err := s.store.ListOrphans(ctx, s.membership.NodeID())
	if err != nil {
		s.log.Warn("cannot list orphaned inodes", "error", err)
		return
	}
	for _, ino := range inos {
		if err := s.store.DeleteOrphan(ctx, ino); err != nil {
			s.log.Warn("cannot delete orphaned inode", "ino", ino, "error", err)
			continue
		}
		s.log.Info("reclaimed an inode left open by a previous run", "ino", ino)
	}
}

// ReconstructArenas rebuilds the arena free-list from existing extents in etcd.
func (s *Service) ReconstructArenas(ctx context.Context) error {
	return s.alloc.Reconstruct(ctx)
}

// Allocator returns this node's block allocator, for background passes that
// need to ask it about the device or sweep it.  Anything that *returns* space
// wants Reclaimer instead, so the release lands in the history.
func (s *Service) Allocator() *arena.Allocator {
	return s.alloc
}

func (s *Service) FreeBlock(diskOff, length uint64) {
	s.freeBlocks(diskOff, length)
}

// Reclaimer returns the allocator wrapped so that a background pass returning
// disk ranges is recorded in the operation history like every other release.
//
// Handing out the bare allocator instead is what made the scrubber's
// reclamations invisible: freeBlocks is the single point every release is
// supposed to pass through, and a range the scrubber freed silently, then
// handed out again, read as one block reserved twice with nothing between —
// a corruption report for a history that was merely incomplete.
func (s *Service) Reclaimer() interface {
	Free(diskOff, size uint64)
	Owns(diskOff uint64) bool
} {
	return recordedReclaimer{s}
}

// recordedReclaimer satisfies scrub.Reclaimer structurally, so the scrubber
// needs no knowledge of this package and this package needs no import of it.
type recordedReclaimer struct{ s *Service }

func (r recordedReclaimer) Free(diskOff, size uint64) { r.s.freeBlocks(diskOff, size) }
func (r recordedReclaimer) Owns(diskOff uint64) bool  { return r.s.alloc.Owns(diskOff) }

// Holds reports whether this node has anything cached for an inode's lock, and
// so whether an operation here may still be resolving that inode's extents.
//
// The background scrubber asks before it hands an extent's blocks back to the
// allocator: it takes no inode lock itself, so without this it can free blocks
// under a read on this node that resolved them and has not yet issued its
// device read.  See scrub.InodeLocks for the whole of that argument.
//
// The answer is deliberately an over-approximation. An entry outlives the
// operation that made it — a released or recalled lock leaves it in place, and
// it goes only on eviction — so an inode this node has merely touched recently
// reads as held. That costs a reclaim deferred to a later pass, which is
// nothing; the opposite error costs a read of another file's data.
func (s *Service) Holds(ino uint64) bool {
	return s.locks.lookup(ino) != nil
}

// Store returns the underlying metadata store.
func (s *Service) Store() *metadata.Store {
	return s.store
}

// IsFenced returns true if self-fencing has triggered.
func (s *Service) IsFenced() bool {
	return s.watchdog != nil && s.watchdog.IsFenced()
}

// InitGeneration ensures this node's gen:<node_id> key exists and caches the
// generation the node starts with.  Idempotent, and safe to retry after a
// transient etcd failure.
func (s *Service) InitGeneration(ctx context.Context) error {
	s.genMu.Lock()
	defer s.genMu.Unlock()
	return s.initGenerationLocked(ctx)
}

// InstallStoreGuard makes every store transaction carry this node's fencing
// generation, so namespace mutations are rejected once the node is fenced —
// not just extent writes.
//
// The guard reports unavailable rather than falling back to generation 0 when
// initialisation has not happened yet: a wrong guard value is worse than no
// transaction, since generation 0 would compare successfully on a node that
// has never been fenced and mask the missing initialisation.
func (s *Service) InstallStoreGuard() {
	s.store.SetGuard(func() (clientv3.Cmp, uint64, bool) {
		s.genMu.Lock()
		defer s.genMu.Unlock()
		if !s.genInit {
			return clientv3.Cmp{}, 0, false
		}
		return metadata.WithGenerationGuard(s.membership.NodeID(), s.startGen), s.startGen, true
	})
}

func (s *Service) initGenerationLocked(ctx context.Context) error {
	if s.genInit {
		return nil
	}
	gen, err := s.store.EnsureGenerationKey(ctx, s.membership.NodeID())
	if err != nil {
		return err
	}
	s.startGen = gen
	s.genInit = true
	metrics.FencingGeneration.Set(float64(gen))
	return nil
}

// guardGeneration returns the generation that data-path transactions must be
// guarded against, initialising it on first use if startup initialisation was
// skipped or failed.
func (s *Service) guardGeneration(ctx context.Context) (uint64, error) {
	s.genMu.Lock()
	defer s.genMu.Unlock()
	if err := s.initGenerationLocked(ctx); err != nil {
		return 0, err
	}
	return s.startGen, nil
}

// Serving reports whether the IPC socket is accepting connections, which is
// the point from which this daemon can answer a mount.
func (s *Service) Serving() bool { return s.serving.Load() }
