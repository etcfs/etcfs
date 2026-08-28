package ipc

import (
	"context"
	"errors"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/pkg/arena"
	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/metrics"
)

// Metadata operation handlers: everything answerable from etcd alone.
// Operations that touch the block device live in datapath.go.

// errnoFor maps a store error to the errno the FUSE client should see.
//
// A fencing rejection must surface as EIO and never as the operation's usual
// failure code: reporting a fenced create as EEXIST, or a fenced unlink as
// ENOENT, makes a fencing bug indistinguishable from ordinary contention in a
// fuzz log, and misleads anyone reading the mount's errors during an incident.
// Anything else keeps the caller's ordinary errno.
func errnoFor(err error, fallback int32) int32 {
	switch {
	case errors.Is(err, metadata.ErrFenced), errors.Is(err, metadata.ErrGuardUnavailable):
		return errIO
	case errors.Is(err, metadata.ErrExists):
		return errExist
	case errors.Is(err, metadata.ErrNotFound):
		return errNoEnt
	case errors.Is(err, metadata.ErrNotDir):
		return errNotDir
	case errors.Is(err, metadata.ErrIsDir):
		return errIsDir
	case errors.Is(err, metadata.ErrInvalid):
		return errInval
	case errors.Is(err, metadata.ErrNotEmpty):
		return errNotEmpty
	case errors.Is(err, metadata.ErrPerm):
		return errPerm
	case errors.Is(err, metadata.ErrNoData):
		return errNoData
	case errors.Is(err, metadata.ErrTooBig):
		return err2Big
	}
	return fallback
}

// LOOKUP payload: [u64:parent][u32:name_len][name_bytes]
// Response: [i32:error][u64:ino][u64×9+u32×6:attr][u32:entry_timeout][u32:attr_timeout]
//
// A name that is not there is answered as a negative entry — errno 0 with
// ino 0 — rather than as ENOENT, so the kernel can cache the absence; see
// negativeEntryResp.  A lookup that could not be *decided* still fails, since
// caching "not there" is only sound when the store said so.
func (s *Service) handleLookup(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name := r.u64(), r.str()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	// A name this node's cached set of the directory says is not there is
	// answered without a round trip.  Only an absence is answered that way; a
	// name the set knows about still goes to etcd, so nothing here can invent a
	// file or hand back the wrong inode.  See direntcache.go.
	if s.direntsAbsent(parent, name) {
		return s.negativeEntryResp(), nil
	}

	ino, err := s.store.LookupDirent(ctx, parent, name)
	if err != nil {
		s.log.Warn("lookup dirent", "parent", parent, "name", name, "error", err)
		return int32Resp(errIO), nil
	}
	if ino == 0 {
		// The first miss in a directory is what pays for the rest of them: one
		// range read now answers every later probe of a name that is not there.
		s.prefetchDirents(ctx, parent)
		return s.negativeEntryResp(), nil
	}
	// etcd has just said the name is there, which is at least as strong as the
	// watch saying so.
	s.dirents.observed(parent, name, true)

	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		s.log.Warn("lookup getinode", "ino", ino, "error", err)
		return int32Resp(errIO), nil
	}

	return s.entryResp(ino, s.withPending(rec)), nil
}

// withPending replaces the parts of a record that this node has changed and not
// yet published: a file's size, and a directory's timestamps.
//
// etcd is behind by up to the flush interval for an inode this node is writing,
// and by up to the same interval for a directory this node has added an entry
// to, so without this a write or a create followed by a stat on the same node
// reports the state from before it.  A peer's stat still reads etcd and still
// lags: that lag is what deferring publication costs, and it is bounded by the
// same interval.  Copying rather than mutating keeps the cached record
// immutable, which every other reader of it relies on.
func (s *Service) withPending(rec *metadata.InodeRecord) *metadata.InodeRecord {
	updated := *rec
	changed := false

	if size, found := s.pendingSize(rec.Ino); found && size != rec.Size {
		updated.Size = size
		changed = true
	}
	if withTimes, moved := s.store.PendingInodeTimes(&updated); moved {
		updated = withTimes
		changed = true
	}

	if !changed {
		return rec
	}
	return &updated
}

// GETATTR payload: [u64:ino]
// Response: [i32:error][u64×9+u32×6:attr][u32:attr_timeout]
func (s *Service) handleGetattr(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino := r.u64()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	// Read from etcd, not from the snapshot cached under this node's lock, even
	// when it holds one.  That snapshot is authoritative for what the lock
	// protects — the extent list and the size — and not for the rest of the
	// record: setattr changes mode, ownership and times under a compare-and-set
	// and takes no lock at all, so a peer may rewrite those fields of an inode
	// this node holds exclusively.  Answering a stat from the snapshot would
	// serve that peer's permission change as the old one until the lock was
	// given up, which is unbounded rather than merely late.
	//
	// Making this cacheable means first bringing mode and ownership under the
	// inode lock, which is a change to how chmod behaves cluster-wide, not an
	// optimisation of this handler.
	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(errNoEnt), nil
	}

	return s.attrResp(s.withPending(rec)), nil
}

// READDIR payload: [u64:ino][u64:offset][u32:size]
// Response: [i32:error][u32:count][entries...]
// Each entry: [u64:ino][u32:name_len][name_bytes][u32:type][u64:off]
func (s *Service) handleReaddir(ctx context.Context, payload []byte) ([]byte, error) {
	return s.readdirResp(ctx, payload, false)
}

// READDIRPLUS is READDIR with an attr block and two timeouts appended to
// every entry.
func (s *Service) handleReaddirPlus(ctx context.Context, payload []byte) ([]byte, error) {
	return s.readdirResp(ctx, payload, true)
}

func (s *Service) readdirResp(ctx context.Context, payload []byte, plus bool) ([]byte, error) {
	r := newReader(payload)
	ino, offset, size := r.u64(), r.u64(), r.u32()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	entries, err := s.direntPage(ctx, ino, offset, size, plus)
	if err != nil {
		return int32Resp(errIO), nil
	}

	// One round trip for every inode record rather than one per entry: readdir
	// on a directory of a thousand files used to be a thousand sequential etcd
	// gets, repeated on every listing.
	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		keys = append(keys, metadata.InodeKey(e.Ino))
	}
	records, err := s.store.GetMany(ctx, keys)
	if err != nil {
		return int32Resp(errIO), nil
	}

	var b buf
	b.w32(0) // error = success
	b.w32(uint32(len(entries)))

	for i, e := range entries {
		rec := metadata.DecodeInode(records[metadata.InodeKey(e.Ino)])

		b.w64(e.Ino)
		b.w32(uint32(len(e.Name)))
		b.b = append(b.b, []byte(e.Name)...)
		b.w32(direntType(rec))
		b.w64(offset + uint64(i) + 1) // directory offset cookie

		if !plus {
			continue
		}
		// The attr block is fixed-width, so an entry whose inode record has
		// vanished still has to write a full-size placeholder — a short write
		// here desynchronises the C parser and turns every following entry
		// into garbage.
		if rec == nil {
			rec = &metadata.InodeRecord{Ino: e.Ino}
		}
		b.wAttr(s.withPending(rec))
		b.w32(s.entryTimeout)
		b.w32(s.attrTimeout)
	}

	return b.b, nil
}

// direntPage returns the entries one READDIR should answer with.
//
// The fast path reads forward from the name the previous reply ended on, so a
// full scan of a directory costs one pass over it rather than one pass per
// page.  It applies when the request continues exactly where the last one
// stopped, which a sequential scan always does; anything else reads the
// directory and slices by position, which is what every request did before this
// existed and is what makes a missed cursor slow rather than wrong.
//
// Both paths end in truncateToBuffer, so what fits in the kernel's buffer is
// decided in one place from the names actually read.
func (s *Service) direntPage(ctx context.Context, ino, offset uint64, size uint32, plus bool) ([]metadata.DirentEntry, error) {
	var entries []metadata.DirentEntry

	if after, resuming := s.dirCursors.resumeAt(ino, offset); resuming {
		metrics.ReaddirPage.WithLabelValues("resumed").Inc()
		page, err := s.store.ListDirentsAfter(ctx, ino, after, pageLimit(size, plus))
		if err != nil {
			return nil, err
		}
		entries = page
	} else {
		metrics.ReaddirPage.WithLabelValues("rescanned").Inc()
		all, err := s.store.ListDirents(ctx, ino)
		if err != nil {
			return nil, err
		}
		// The cookie of an entry is its 1-based position, so the kernel's
		// offset is the count already returned.
		if offset >= uint64(len(all)) {
			all = nil
		} else {
			all = all[offset:]
		}
		entries = all
	}

	entries = truncateToBuffer(entries, size, plus)
	if n := len(entries); n > 0 {
		s.dirCursors.record(ino, offset+uint64(n), entries[n-1].Name)
	}
	return entries, nil
}

// pageLimit is how many entries to ask etcd for to fill a reply buffer of
// size bytes.
//
// Derived from the *smallest* an entry's framing can be, so the answer is an
// upper bound: reading a few more than fit costs one comparison each in
// truncateToBuffer, while reading too few would silently shorten the listing
// into an extra round trip. A size of zero means the caller set no bound.
func pageLimit(size uint32, plus bool) int64 {
	if size == 0 {
		return 0
	}
	const minDirentCost = 24 + 8 // fuse_dirent, plus the shortest padded name
	cost := minDirentCost
	if plus {
		cost += 128 // fuse_entry_out
	}
	if limit := int64(size) / int64(cost); limit > 0 {
		return limit
	}
	// A reply always carries at least one entry, because an empty one is how a
	// listing ends; truncateToBuffer makes the same allowance.
	return 1
}

// truncateToBuffer drops the entries that would not fit in the kernel's reply
// buffer anyway.
//
// The estimate is the kernel's own dirent framing — a fixed header plus the
// name, padded to eight bytes — with the entry_out block added for
// readdirplus.  It is deliberately generous: under-filling costs one more
// readdir call, while over-filling costs nothing at all, since the C daemon
// stops adding entries once its buffer is full.  At least one entry is always
// returned, because an empty reply is how a listing ends.
func truncateToBuffer(entries []metadata.DirentEntry, size uint32, plus bool) []metadata.DirentEntry {
	if size == 0 {
		return entries
	}
	const direntHeader = 24   // fuse_dirent, before the name
	const entryOutBytes = 128 // fuse_entry_out, readdirplus only

	budget := int(size)
	for i, e := range entries {
		cost := direntHeader + (len(e.Name)+7)/8*8
		if plus {
			cost += entryOutBytes
		}
		budget -= cost
		if budget < 0 {
			if i == 0 {
				return entries[:1]
			}
			return entries[:i]
		}
	}
	return entries
}

// direntType maps an inode record to its DT_* directory entry type.
// A missing record is reported as a regular file.
func direntType(rec *metadata.InodeRecord) uint32 {
	if rec == nil {
		return metadata.DirentTypeFile
	}
	switch rec.Mode & metadata.S_IFMT {
	case metadata.ModeDir:
		return metadata.DirentTypeDir
	case metadata.ModeSymlink:
		return metadata.DirentTypeSymlink
	default:
		return metadata.DirentTypeFile
	}
}

// READLINK payload: [u64:ino]
// Response: [i32:error][u32:target_len][target_bytes]
func (s *Service) handleReadlink(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino := r.u64()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	target, err := s.store.Get(ctx, metadata.InodeSymlinkKey(ino))
	if err != nil || target == nil {
		return int32Resp(errNoEnt), nil
	}

	var b buf
	b.w32(0) // error = success
	b.w32(uint32(len(target)))
	b.b = append(b.b, target...)
	return b.b, nil
}

// freeBytes estimates the space left on the shared device, for statfs.
//
// The device is shared but each arena belongs to one node, so the two halves of
// the answer come from different places.  Space no node has claimed is a
// cluster-wide fact, read from the ownership records in etcd.  Space still free
// *inside* an arena is known only to that arena's owner, so only this node's
// own slack can be added to it.
//
// The result therefore under-reports: every other node's unused space inside
// its own arenas is counted as used.  That is a deliberate choice of which way
// to be wrong.  Deriving the whole number from this node's arena occupancy —
// what this used to do — was wrong in both directions at once, reporting a
// nearly empty device as full whenever this node's own arenas happened to be
// full, and a nearly full one as empty whenever they happened to be free.
func (s *Service) freeBytes(ctx context.Context, deviceSize uint64) uint64 {
	owned, err := s.store.CountOwnedArenas(ctx)
	if err != nil {
		s.log.Warn("statfs: cannot count the cluster's arenas", "error", err)
		return 0
	}

	claimed := uint64(owned) * arena.ArenaSizeBytes
	unclaimed := uint64(0)
	if claimed < deviceSize {
		unclaimed = deviceSize - claimed
	}

	mine := uint64(s.alloc.ArenaCount()) * arena.ArenaSizeBytes
	localFree := uint64(float64(mine) * (1 - s.alloc.LiveRatio()))

	return unclaimed + localFree
}

// STATFS payload: empty
// Response: [i32:error][u64:blocks][u64:bfree][u64:bavail][u64:files][u64:ffree][u32:bsize][u32:namelen][u32:frsize]
func (s *Service) handleStatfs(ctx context.Context, _ []byte) ([]byte, error) {
	blocks := uint64(1 << 30)
	bfree := uint64(1 << 29)
	if s.dev != nil {
		size := uint64(s.dev.TotalSize())
		blocks = size / 512
		bfree = s.freeBytes(ctx, size) / 512
	}
	// The inode allocation counter, not a scan of the inode space: every `df`
	// used to be a full range read over the whole namespace to use nothing but
	// its length.  The counter counts numbers handed out rather than inodes
	// alive, so it over-reports after deletions — an upper bound is the right
	// error to make for a number no caller can act on.
	files := uint64(0)
	if v, err := s.store.Get(ctx, metadata.KeyInodeAllocCounter); err == nil && v != nil {
		if n := metadata.DecodeUint64(v); n > metadata.FirstUsableIno {
			files = n - metadata.FirstUsableIno
		}
	}
	// Inode numbers are 64-bit, so nothing but space limits how many files can
	// exist, and every file needs at least one block.  The hardcoded 1,000,000
	// ceiling this replaces was not a limit the filesystem enforced anywhere.
	ffree := bfree

	return statfsResp(blocks, bfree, files, ffree), nil
}

// ---- write operation handlers ----

// applyUmask clears the permission bits the caller's umask masks out, leaving
// the file type alone.  The kernel does not apply it for us on a filesystem
// that declares FUSE_DONT_MASK behaviour, and every create path needs the same
// treatment, so it lives here rather than at four call sites.
func applyUmask(mode, umask uint32) uint32 {
	return mode &^ (umask & 0777)
}

// CREATE payload:  [u64:parent][u32:name_len][name][u32:mode][u32:flags][u32:umask][u32:uid][u32:gid]
// Response: [i32:error][u64:ino][u64×9+u32×6:attr][u32:entry_timeout][u32:attr_timeout][u32:keep_cache]
func (s *Service) handleCreate(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name := r.u64(), r.str()
	mode := r.u32()
	flags := r.u32()
	umask, uid, gid := r.u32(), r.u32(), r.u32()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	ino, err := s.allocInode(ctx)
	if err != nil {
		// ENOSPC unless the node has been fenced, which has to surface as EIO:
		// a fenced create reported as "disk full" is indistinguishable from a
		// genuinely full device in a log.
		return int32Resp(errnoFor(err, errNoSpace)), nil
	}

	// The exclusive lock on the file rides the transaction that creates it, so
	// the write that follows the create needs no acquisition of its own.  A
	// failure to mint it is not a reason to fail the create: the create still
	// commits, and the first write acquires the lock the way it always did.
	holder, lockCmp, lockOp, lerr := s.store.PrepareLock(ino, metadata.LockExclusive, inodeLockTTL)
	extra := metadata.CreateExtra{}
	if lerr != nil {
		s.log.Warn("cannot take a new file's lock with its create; the first write will acquire it",
			"ino", ino, "error", lerr)
		holder = ""
	} else {
		extra = metadata.CreateExtra{Cmps: []clientv3.Cmp{lockCmp}, Ops: []clientv3.Op{lockOp}}
	}

	call := time.Now()
	rec, err := s.store.AtomicCreateFile(metadata.WithTxnOrigin(ctx, "create"), parent, name, ino,
		applyUmask(mode, umask), uid, gid, extra)
	if err != nil {
		if holder != "" {
			s.discardCreatedLock(ino, holder)
		}
		return int32Resp(errnoFor(err, errExist)), nil // EEXIST unless fenced
	}
	if holder != "" && !s.seedCreatedLock(ino, holder, rec, call, time.Now()) {
		// The key is in etcd and nothing names it, so it has to go; the first
		// write acquires the lock the way it did before.
		s.discardCreatedLock(ino, holder)
	}
	s.dirents.created(parent, name)
	s.parentTimesChanged(parent)

	// A create hands back an open descriptor, and the release that closes it
	// arrives like any other, so it has to be counted like any other.
	s.open.retain(rec.Ino)

	// The descriptor a create hands back is answered like any open: a page can
	// only be filled by a read that reached the daemon under this inode's lock,
	// and that lock's release invalidates it.  Nothing about a fresh inode
	// changes either half of that.
	return s.createResp(rec.Ino, rec, s.cacheableOpen(flags)), nil
}

// oTrunc is O_TRUNC as the kernel passes it in fuse_file_info.flags.
const oTrunc = 0x200

// OPEN payload: [u64:ino][u32:flags]
// Response: [i32:error]
//
// Two things happen here that cannot be answered on the C side: O_TRUNC empties
// the file, and the descriptor is counted so that unlinking the file's last
// name keeps the record alive until the last close.
func (s *Service) handleOpen(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino, flags := r.u64(), r.u32()
	if !r.ok {
		return int32Resp(errInval), nil
	}
	if flags&oTrunc != 0 {
		// O_TRUNC deletes every extent of the file and publishes the new size,
		// which is the same class of mutation a write makes and needs the same
		// exclusive lock: without it a concurrent write commits an extent this
		// truncate has already read past, and the file keeps data it was told
		// to drop.
		lk, lerr := s.lockInode(ctx, ino, metadata.LockExclusive)
		if lerr != nil {
			s.log.Warn("open: cannot lock inode for truncate", "ino", ino, "error", lerr)
			return int32Resp(errAgain), nil
		}
		defer lk.Release()

		if ferr := lk.flush(ctx); ferr != nil {
			s.log.Warn("open: cannot publish deferred writes before truncating", "ino", ino, "error", ferr)
			return int32Resp(errIO), nil
		}
		if err := s.truncateToZero(ctx, ino); err != nil {
			s.log.Warn("open: truncate failed", "ino", ino, "error", err)
			return int32Resp(errnoFor(err, errIO)), nil
		}
	}
	s.open.retain(ino)
	return openResp(s.cacheableOpen(flags)), nil
}

// cacheableOpen reports whether the kernel may keep this open's data pages
// rather than going through to the daemon for every read.
//
// It is safe only because something can take the pages back: the invalidation
// that runs before an inode's lock key is yielded.  So it needs a client
// connected to carry that invalidation — without one the pages would outlive
// the lock with nothing able to drop them.
//
// A synchronous open keeps the direct-IO path it has always had.  The
// guarantee that O_SYNC and O_DSYNC disable deferral rests on the flags
// arriving with every write, which was measured on that path; a buffered open
// reaches the kernel's write path through different code, and narrowing the
// open is cheaper than widening what has been checked.
func (s *Service) cacheableOpen(flags uint32) bool {
	// Logged rather than inferred.  Whether the kernel may cache an open's pages
	// is decided from three inputs and reported to the C side as a single bit,
	// and a mount that turns out not to be caching gives no way to tell which
	// input said no -- or whether the bit was what the C side acted on.
	defer func() {
		s.log.Debug("OPEN cacheability", "flags", flags,
			"page_cache", s.pageCache,
			"dsync", flags&oDSync != 0,
			"notify_client", s.notifyServer.connected())
	}()

	if !s.pageCache || flags&oDSync != 0 {
		return false
	}
	if !s.notifyServer.connected() {
		// The one condition here that is a fault rather than a setting, and the
		// one that used to be invisible: a mount whose notify client never
		// connected serves every read from the daemon and looks, from any
		// measurement, exactly like a slow coordination layer.
		if !s.noPageCacheLogged.Swap(true) {
			s.log.Warn("no cache-invalidation client is connected, so the kernel is not " +
				"allowed to cache this mount's file data; every read will reach the daemon")
		}
		return false
	}
	s.noPageCacheLogged.Store(false)
	s.pagesCached.Store(true)
	return true
}

// RELEASE payload: [u64:ino]
// Response: [i32:error]
func (s *Service) handleRelease(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino := r.u64()
	if !r.ok {
		return int32Resp(errInval), nil
	}
	if s.open.release(ino) {
		// The last descriptor on a file whose names are all gone: nothing can
		// reach it any more, so the record and its attributes go now. Its
		// extents are left to the scrubber, exactly as an ordinary unlink
		// leaves them.
		if err := s.store.DeleteOrphan(ctx, ino); err != nil {
			s.log.Warn("release: cannot delete orphaned inode", "ino", ino, "error", err)
		}
		// And the inode's cached lock goes with it, so the scrubber stops
		// skipping the extents this inode has just orphaned. Only when nothing
		// is buffered under it — see yieldQuietCachedLock for why a drop that
		// publishes would put the deleted record back.
		if err := s.yieldQuietCachedLock(ino, "unlinked"); err != nil {
			s.log.Warn("released orphan's cached lock not given up; its blocks wait for the cache to evict it",
				"ino", ino, "error", err)
		}
	}
	return okResp(), nil
}

// MKDIR payload:  [u64:parent][u32:name_len][name][u32:mode][u32:umask][u32:uid][u32:gid]
// Response: same as CREATE
func (s *Service) handleMkdir(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name := r.u64(), r.str()
	mode, umask, uid, gid := r.u32(), r.u32(), r.u32(), r.u32()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(errnoFor(err, errNoSpace)), nil
	}

	rec, err := s.store.AtomicCreateDir(ctx, parent, name, ino, applyUmask(mode, umask), uid, gid)
	if err != nil {
		return int32Resp(errnoFor(err, errExist)), nil
	}
	s.dirents.created(parent, name)
	s.parentTimesChanged(parent)

	return s.entryResp(rec.Ino, rec), nil
}

// UNLINK payload: [u64:parent][u32:name_len][name]
// Response: [i32:error]
func (s *Service) handleUnlink(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name := r.u64(), r.str()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	removed, err := s.store.AtomicUnlinkKeepingOpen(ctx, parent, name, s.open.heldOpen)
	if err != nil {
		return int32Resp(errnoFor(err, errNoEnt)), nil // ENOENT unless fenced
	}
	s.dirents.deleted(parent, name)
	s.parentTimesChanged(parent)
	// The file is gone and its extents are now orphans, which only the node
	// owning their arena may reclaim — this one.  The scrubber refuses to
	// reclaim an orphan while anything is cached for its inode's lock, and this
	// node's cache keeps that entry until it is evicted, so an inode nobody can
	// reach again would otherwise pin its blocks for as long as the entry
	// lives.  Under churn that deletes as fast as it writes, nothing comes back
	// at all and the filesystem reaches ENOSPC with almost nothing live in it.
	if removed != 0 {
		if yerr := s.yieldQuietCachedLock(removed, "unlinked"); yerr != nil {
			// The blocks are not lost, only late: the entry stays, and the
			// eviction that eventually takes it lets the next scrub pass
			// reclaim them.
			s.log.Warn("unlinked inode's cached lock not given up; its blocks wait for the cache to evict it",
				"ino", removed, "error", yerr)
		}
	}
	return okResp(), nil
}

// RMDIR payload: [u64:parent][u32:name_len][name]
// Response: [i32:error]
func (s *Service) handleRmdir(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name := r.u64(), r.str()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	if err := s.store.AtomicRmdir(ctx, parent, name); err != nil {
		return int32Resp(errnoFor(err, errNoEnt)), nil
	}
	s.dirents.deleted(parent, name)
	s.parentTimesChanged(parent)
	return okResp(), nil
}

// RENAME payload: [u64:old_parent][u32:old_name_len][old_name][u64:new_parent][u32:new_name_len][new_name][u32:flags]
// Response: [i32:error]
func (s *Service) handleRename(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	oldParent, oldName := r.u64(), r.str()
	newParent, newName := r.u64(), r.str()
	flags := r.u32()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	// Resolve old inode
	ino, err := s.store.LookupDirent(ctx, oldParent, oldName)
	if err != nil || ino == 0 {
		return int32Resp(errNoEnt), nil
	}

	// write(); close(); rename() is how a program publishes a file atomically,
	// and it only works if the data is there by the time the name is.  Renaming
	// while this node still had the writes buffered would name a file whose
	// content is a flush interval behind — the ext4 delayed-allocation trap.
	if ferr := s.flushInode(ctx, ino); ferr != nil {
		s.log.Warn("rename: cannot publish deferred writes", "ino", ino, "error", ferr)
		return int32Resp(errIO), nil
	}

	err = s.store.AtomicRename(ctx, oldParent, oldName, newParent, newName, ino, flags)
	if err != nil {
		return int32Resp(errnoFor(err, errExist)), nil // EEXIST or other, unless fenced
	}
	s.dirents.deleted(oldParent, oldName)
	s.dirents.created(newParent, newName)
	s.parentTimesChanged(oldParent)
	if newParent != oldParent {
		s.parentTimesChanged(newParent)
	}
	return okResp(), nil
}

// SETATTR payload:
//
//	[u64:ino][u64:fh][u32:valid][u64:size][u32:mode][u32:uid][u32:gid]
//	[u64:atime][u64:mtime][u64:ctime][u32:atime_nsec][u32:mtime_nsec][u32:ctime_nsec]
//
// Response: [i32:error][attr:84][u32:attr_timeout]
//
// valid is the kernel's FUSE_SET_ATTR_* mask saying which of those fields it
// actually means; the rest carry whatever the caller's struct stat happened to
// hold and must be ignored.
const (
	fattrMode     = 1 << 0
	fattrUID      = 1 << 1
	fattrGID      = 1 << 2
	fattrSize     = 1 << 3
	fattrAtime    = 1 << 4
	fattrMtime    = 1 << 5
	fattrAtimeNow = 1 << 7
	fattrMtimeNow = 1 << 8
	fattrCtime    = 1 << 10
)

const setattrPayloadLen = 8 + 8 + 4 + 8 + 4 + 4 + 4 + 8 + 8 + 8 + 4 + 4 + 4

// setattrFields is a SETATTR request's payload after decoding: every field the
// kernel may carry, whether or not the valid mask selects it.
type setattrFields struct {
	size                uint64
	mode, uid, gid      uint32
	atime, mtime, ctime time.Time
}

// applySetattr writes the selected fields onto the record.
//
// A pure function of (record, mask, fields, now) — the truncate, the lock and
// the compare-and-set stay in the handler. What is worth isolating is the
// decision table itself: which bit selects which field, and the two rules that
// are not a plain assignment.
func applySetattr(rec *metadata.InodeRecord, valid uint32, f setattrFields, now time.Time) {
	if valid&fattrSize != 0 {
		rec.Size = f.size
	}
	if valid&fattrMode != 0 {
		// The kernel sends a whole st_mode, but chmod may not change what kind
		// of file this is.  Keeping the stored type bits is what stops a chmod
		// on a symlink or a device node quietly turning it into something else.
		rec.Mode = (rec.Mode & metadata.S_IFMT) | (f.mode &^ metadata.S_IFMT)
	}
	if valid&fattrUID != 0 {
		rec.UID = f.uid
	}
	if valid&fattrGID != 0 {
		rec.GID = f.gid
	}
	if valid&fattrAtime != 0 {
		rec.Atime = f.atime
	}
	if valid&fattrMtime != 0 {
		rec.Mtime = f.mtime
	}
	if valid&fattrCtime != 0 {
		rec.Ctime = f.ctime
	}
	if valid&fattrAtimeNow != 0 {
		rec.Atime = now
	}
	if valid&fattrMtimeNow != 0 {
		rec.Mtime = now
	}
	// Any attribute change is a status change, unless the caller set ctime
	// itself.
	if valid&(fattrMode|fattrUID|fattrGID|fattrSize) != 0 && valid&fattrCtime == 0 {
		rec.Ctime = now
	}
}

// setattrFieldLabel names what a setattr request asked for, for the metric that
// counts how much of a workload's setattr traffic is deferrable.  Bounded in
// cardinality by construction: four groups, always in the same order.
func setattrFieldLabel(valid uint32) string {
	label := ""
	for _, group := range []struct {
		mask uint32
		name string
	}{
		{fattrMode, "mode"},
		{fattrUID | fattrGID, "owner"},
		{fattrSize, "size"},
		{fattrAtime | fattrMtime | fattrCtime | fattrAtimeNow | fattrMtimeNow, "times"},
	} {
		if valid&group.mask != 0 {
			if label != "" {
				label += "+"
			}
			label += group.name
		}
	}
	if label == "" {
		return "none"
	}
	return label
}

// setattrEnforces reports whether a setattr changes anything a peer's access
// check depends on, given the record as it stands.
//
// The distinction is what may be deferred.  A timestamp has no enforcement
// meaning, so a peer seeing it late costs nothing; mode and ownership decide
// who may open the file, and a peer checks them against what etcd holds — so a
// change that takes permission away has to be there before the call returns,
// or that peer goes on granting it.  A setattr that names those fields without
// moving them is not such a change: `tar` restores the mode a file was created
// with on every file it extracts, and the record already says so.
func setattrEnforces(rec *metadata.InodeRecord, valid uint32, f setattrFields) bool {
	if valid&fattrMode != 0 &&
		(rec.Mode&metadata.S_IFMT)|(f.mode&^metadata.S_IFMT) != rec.Mode {
		return true
	}
	if valid&fattrUID != 0 && f.uid != rec.UID {
		return true
	}
	if valid&fattrGID != 0 && f.gid != rec.GID {
		return true
	}
	return valid&fattrSize != 0
}

func (s *Service) handleSetattr(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino := r.u64()
	r.u64() // fh
	valid := r.u32()
	var f setattrFields
	f.size = r.u64()
	f.mode, f.uid, f.gid = r.u32(), r.u32(), r.u32()
	atime, mtime, ctime := r.u64(), r.u64(), r.u64()
	atimeNsec, mtimeNsec, ctimeNsec := r.u32(), r.u32(), r.u32()
	f.atime = time.Unix(int64(atime), int64(atimeNsec))
	f.mtime = time.Unix(int64(mtime), int64(mtimeNsec))
	f.ctime = time.Unix(int64(ctime), int64(ctimeNsec))
	if !r.ok {
		return int32Resp(errInval), nil
	}

	// A size change rewrites extents and republishes the size, so it is held
	// against concurrent writers by the same exclusive lock the write path
	// takes.  The other attributes are settled by the record's own
	// compare-and-set below and need no lock.
	if valid&fattrSize != 0 {
		lk, lerr := s.lockInode(ctx, ino, metadata.LockExclusive)
		if lerr != nil {
			s.log.Warn("setattr: cannot lock inode for size change", "ino", ino, "error", lerr)
			return int32Resp(errAgain), nil
		}
		defer lk.Release()

		// The record and extents below are read from etcd, so what this node
		// has buffered has to be there first — otherwise a shrink plans against
		// a file missing its most recent writes, and the size published here
		// overwrites theirs.
		if ferr := lk.flush(ctx); ferr != nil {
			s.log.Warn("setattr: cannot publish deferred writes before a size change",
				"ino", ino, "error", ferr)
			return int32Resp(errIO), nil
		}
	}

	// Read without a revision: this copy only decides which way the call goes,
	// and the committing path re-reads once anything queued is out of the way.
	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(errNoEnt), nil
	}

	// A setattr that moves nothing a peer enforces against is timestamps only —
	// the ones it names, plus the status change POSIX owes any attribute call —
	// and those are queued rather than committed.  `tar` sets a file's mode and
	// then its times after writing it, so this is most of what an unpacking
	// archive asks of setattr, and none of it needs consensus before the call
	// returns.
	fields := setattrFieldLabel(valid)
	if !setattrEnforces(rec, valid, f) {
		if resp, deferred := s.deferSetattrTimes(rec, valid, f); deferred {
			metrics.Setattr.WithLabelValues(fields, "deferred").Inc()
			return resp, nil
		}
	}
	metrics.Setattr.WithLabelValues(fields, "committed").Inc()

	// Committing synchronously: anything queued for this inode is older than
	// what is about to be written, so it is folded into the same transaction
	// rather than published ahead of it.  Publishing it first would be a commit
	// of its own, and an archive reaches here twice per file — for the mode and
	// for the owner — which was enough to hand back every commit the deferral
	// had removed.
	//
	// Retried rather than refused, because this node's own timestamp sweep is
	// now one of the writers this comparison can lose to.  A caller that asked
	// for a mode it is entitled to should not be handed EAGAIN because a
	// queued ctime happened to publish in between; only a genuine race with
	// another operation is worth reporting, and that one still ends in EAGAIN
	// once the attempts are spent.
	var rev int64
	for attempt := 0; ; attempt++ {
		queued, hadQueued := s.store.TakeInodeTimes(ino)
		rec, rev, err = s.store.GetInodeRev(ctx, ino)
		if err != nil || rec == nil {
			if hadQueued {
				s.store.RequeueInodeTimes(queued)
			}
			return int32Resp(errNoEnt), nil
		}
		// The queued times go on first: they happened before this call, and
		// applySetattr overwrites whichever of them this call names itself.
		if hadQueued {
			queued.ApplyTo(rec)
		}

		resp, done := s.commitSetattr(ctx, ino, rec, rev, valid, f)
		if done {
			return resp, nil
		}
		// The transaction did not commit, so those timestamps are still owed.
		if hadQueued {
			s.store.RequeueInodeTimes(queued)
		}
		if attempt == setattrRetries {
			return int32Resp(errAgain), nil
		}
	}
}

// setattrRetries is how many times a synchronous setattr rebuilds its change
// against a record that moved underneath it before giving up.
const setattrRetries = 3

// commitSetattr applies one attempt of a synchronous setattr, reporting whether
// it settled the call.  Not settled means the record moved between the read and
// the transaction, and the caller should build the change again from what
// replaced it.
func (s *Service) commitSetattr(ctx context.Context, ino uint64, rec *metadata.InodeRecord,
	rev int64, valid uint32, f setattrFields) ([]byte, bool) {

	// Shrinking releases the extents past the new end, and that runs before the
	// size is published: metadata-then-data, so no reader can still resolve a
	// range whose blocks have already gone back to the arena.
	if valid&fattrSize != 0 && f.size < rec.Size {
		if terr := s.truncate(ctx, ino, f.size); terr != nil {
			// Reporting success here would tell the caller the file had shrunk
			// while every extent past the new end was still readable — which is
			// what a fenced node did, since the guard rejects its writes.
			s.log.Error("truncate failed, size not changed", "ino", ino, "error", terr)
			return int32Resp(errnoFor(terr, errIO)), true
		}
	}

	applySetattr(rec, valid, f, time.Now())

	// Pinned to the revision the record was read at, so a concurrent update to
	// a different field is not silently overwritten by this one.
	ok, err := s.store.Txn(metadata.WithTxnOrigin(ctx, "setattr"),
		[]clientv3.Cmp{metadata.InodeUnchanged(ino, rev)},
		[]clientv3.Op{clientv3.OpPut(metadata.InodeKey(ino), string(metadata.EncodeInode(rec)))}, nil)
	if err != nil {
		return int32Resp(errnoFor(err, errIO)), true
	}
	if !ok {
		return nil, false
	}

	return s.attrResp(rec), true
}

// deferSetattrTimes queues the timestamps a setattr assigns and answers it from
// the record they would produce, reporting whether it did.
//
// False when write-behind is off, which leaves the caller to commit as before.
func (s *Service) deferSetattrTimes(rec *metadata.InodeRecord, valid uint32,
	f setattrFields) ([]byte, bool) {

	// Built on what this node believes rather than on what etcd holds: a
	// previous setattr's times may still be queued, and this call may leave
	// them alone — utimensat with UTIME_OMIT does exactly that.  Answering from
	// the stored record would hand the kernel a reply in which the earlier
	// change had never happened, and the kernel would cache it.
	base, _ := s.store.PendingInodeTimes(rec)
	now := time.Now()
	updated := base
	applySetattr(&updated, valid, f, now)

	// Only the fields this call moved are queued.  One it left alone is either
	// already queued or equal to what etcd holds, and in both cases saying so
	// again would change nothing.
	if !s.store.QueueInodeTimes(updated.Ino, updated.Atime, updated.Mtime, updated.Ctime,
		!updated.Atime.Equal(base.Atime), !updated.Mtime.Equal(base.Mtime),
		!updated.Ctime.Equal(base.Ctime)) {
		return nil, false
	}
	return s.attrResp(&updated), true
}

// SYMLINK payload: [u64:parent][u32:name_len][name][u32:target_len][target][u32:uid][u32:gid]
// Response: [i32:error][u64:ino][attr:84][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleSymlink(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name, target := r.u64(), r.str(), r.str()
	uid, gid := r.u32(), r.u32()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(errnoFor(err, errNoSpace)), nil
	}

	rec, err := s.store.AtomicCreateSymlink(ctx, parent, name, ino, target, uid, gid)
	if err != nil {
		return int32Resp(errnoFor(err, errExist)), nil
	}
	s.dirents.created(parent, name)
	s.parentTimesChanged(parent)

	return s.entryResp(ino, rec), nil
}

// LINK payload: [u64:ino][u64:new_parent][u32:new_name_len][new_name]
// Response: [i32:error][u64:ino][attr:84][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleLink(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino, newParent, name := r.u64(), r.u64(), r.str()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	// A second name for the inode, for the same reason a rename gets one: the
	// name must not become reachable before the data it points at.
	if ferr := s.flushInode(ctx, ino); ferr != nil {
		s.log.Warn("link: cannot publish deferred writes", "ino", ino, "error", ferr)
		return int32Resp(errIO), nil
	}

	rec, err := s.store.AtomicLink(ctx, ino, newParent, name)
	if err != nil {
		return int32Resp(errnoFor(err, errExist)), nil
	}
	s.dirents.created(newParent, name)
	s.parentTimesChanged(newParent)
	return s.entryResp(ino, rec), nil
}

// MKNOD payload: [u64:parent][u32:name_len][name][u32:mode][u32:rdev][u32:umask][u32:uid][u32:gid]
// Response: [i32:error][u64:ino][attr:84][u32:entry_timeout][u32:attr_timeout]
func (s *Service) handleMknod(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	parent, name := r.u64(), r.str()
	mode, rdev, umask := r.u32(), r.u32(), r.u32()
	uid, gid := r.u32(), r.u32()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	ino, err := s.allocInode(ctx)
	if err != nil {
		return int32Resp(errnoFor(err, errNoSpace)), nil
	}

	rec, err := s.store.AtomicCreateNode(ctx, parent, name, ino, applyUmask(mode, umask), rdev, uid, gid)
	if err != nil {
		return int32Resp(errnoFor(err, errExist)), nil
	}
	s.dirents.created(parent, name)
	s.parentTimesChanged(parent)

	return s.entryResp(ino, rec), nil
}

// ---- lock handlers ----
//
// EtcFS does not track byte-range locks in etcd.  The lock:<ino> keys used by
// the read and write paths are whole-inode leases held for the duration of a
// single operation, which is a different thing from a process-owned POSIX
// record lock and cannot answer GETLK/SETLK on its own.
//
// So both handlers report "no conflict", which leaves the kernel's own local
// lock bookkeeping in charge: correct within one node, not enforced across
// nodes.  Reporting a conflict instead would be worse than useless — SETLK cannot grant a lock it
// does not track, so every caller would spin on EAGAIN forever.

// GETLK and SETLK are deliberately not handled here, and the C daemon does not
// implement the matching FUSE operations.  libfuse's contract is that "if the
// locking methods are not implemented, the kernel will still allow file locking
// to work locally" — implementing them takes that job away from the kernel, so
// answering "always free / always granted" left fcntl() locks excluding nothing
// at all, not even between two processes on one node.  Leaving them unwired
// gives fcntl() the same node-local-correct behavior flock() already has.

// allocInode reserves an inode number, from the block this node is holding
// when it still has one and from etcd when it does not.  See inodealloc.go.
//
// Inodes 0 and 1 are both reserved: 0 is not a valid inode, and 1 is
// FUSE_ROOT_ID — the root directory, which the C daemon answers for locally
// and which seed-etcd writes to inode:1.  Handing 1 out to a regular file
// overwrites the root inode record and makes the whole mount return EIO, so
// the first allocation must be 2.
func (s *Service) allocInode(ctx context.Context) (uint64, error) {
	return s.inodes.reserve(ctx)
}
