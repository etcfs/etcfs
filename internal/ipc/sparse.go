package ipc

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// Sparse-file operations: lseek(SEEK_HOLE/SEEK_DATA) and fallocate.
//
// Both are queries or edits over the extent map, which already records exactly
// which logical ranges of a file are backed — a file here is sparse by
// construction, since a range nothing ever wrote simply has no extent.

// whence values, matching the kernel's SEEK_DATA and SEEK_HOLE.
const (
	seekData = 3
	seekHole = 4
)

// fallocate mode bits, matching the kernel's.
const (
	fallocKeepSize  = 0x01
	fallocPunchHole = 0x02
)

// LSEEK payload: [u64:ino][u64:offset][u32:whence]
// Response: [i32:error][u64:offset]
func (s *Service) handleLseek(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino, offset, whence := r.u64(), r.u64(), r.u32()
	if !r.ok {
		return int32Resp(errInval), nil
	}
	if whence != seekData && whence != seekHole {
		return int32Resp(errInval), nil // EINVAL: the kernel handles SET/CUR/END
	}

	// Both answers come from the extent map in etcd, which is behind by
	// whatever this node has buffered — a SEEK_DATA over a range only this node
	// has written would otherwise report a hole.
	if ferr := s.flushInode(ctx, ino); ferr != nil {
		s.log.Warn("lseek: cannot publish deferred writes", "ino", ino, "error", ferr)
		return int32Resp(errIO), nil
	}

	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(errNoEnt), nil
	}

	extents, err := s.store.GetExtents(ctx, ino)
	if err != nil {
		return int32Resp(errnoFor(err, errIO)), nil
	}

	var found uint64
	var ok bool
	if whence == seekData {
		found, ok = metadata.SeekData(extents, rec.Size, offset)
	} else {
		found, ok = metadata.SeekHole(extents, rec.Size, offset)
	}
	if !ok {
		return int32Resp(errNXIO), nil // ENXIO: no such offset in this file
	}

	return lseekResp(found), nil
}

// FALLOCATE payload: [u64:ino][u32:mode][u64:offset][u64:length]
// Response: [i32:error]
//
// Two modes are served. FALLOC_FL_PUNCH_HOLE deallocates a range, which is the
// truncate path restricted to a bounded range rather than everything past a
// new end. Plain allocation (mode 0) publishes the new size when the range
// extends the file and does nothing else: blocks here are claimed from the
// arena at write time, so there is no reservation to make that a later write
// would consult. That makes it an honest ENOSPC-deferral rather than the
// guarantee fallocate nominally offers, and the ponytail note below says so.
//
// Every other mode is refused rather than approximated. FALLOC_FL_ZERO_RANGE
// and FALLOC_FL_COLLAPSE_RANGE have precise semantics a caller relies on, and
// silently doing something adjacent is how a database corrupts a file.
func (s *Service) handleFallocate(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino, mode, offset, length := r.u64(), r.u32(), r.u64(), r.u64()
	if !r.ok {
		return int32Resp(errInval), nil
	}
	if length == 0 {
		return int32Resp(errInval), nil
	}
	// Overflow would wrap the end offset back below the start and turn a
	// punch into a reclaim of the whole file.
	if offset+length < offset {
		return int32Resp(errInval), nil
	}

	// Allocation with KEEP_SIZE changes nothing here — blocks are claimed at
	// write time — so it is answered before anything is locked.
	if mode == fallocKeepSize {
		return okResp(), nil
	}

	// Both served modes mutate the file — a punch rewrites extents, plain
	// allocation republishes the size — so both take the exclusive lock the
	// write path takes, and for the same reason: a concurrent write must not
	// commit an extent into a range this operation has already read past.
	lk, lerr := s.lockInode(ctx, ino, metadata.LockExclusive)
	if lerr != nil {
		s.log.Warn("fallocate: cannot lock inode", "ino", ino, "error", lerr)
		return int32Resp(errAgain), nil
	}
	defer lk.Release()

	// Both modes plan against what etcd holds, so this node's own buffered
	// writes have to be published before the plan is built.
	if ferr := lk.flush(ctx); ferr != nil {
		s.log.Warn("fallocate: cannot publish deferred writes", "ino", ino, "error", ferr)
		return int32Resp(errIO), nil
	}

	switch {
	case mode&fallocPunchHole != 0:
		// The kernel requires KEEP_SIZE alongside PUNCH_HOLE, and a punch that
		// changed the size would be a different operation entirely.
		if mode&fallocKeepSize == 0 {
			return int32Resp(errInval), nil
		}
		if err := s.punchHole(ctx, ino, offset, offset+length); err != nil {
			s.log.Error("fallocate: punch hole failed", "ino", ino, "error", err)
			return int32Resp(errnoFor(err, errIO)), nil
		}
		return okResp(), nil

	case mode == 0:
		if err := s.growTo(ctx, ino, offset+length); err != nil {
			s.log.Error("fallocate: grow failed", "ino", ino, "error", err)
			return int32Resp(errnoFor(err, errIO)), nil
		}
		return okResp(), nil
	}

	return int32Resp(errNotSupp), nil
}

// punchHole reclaims every extent range inside [start, end), leaving the file's
// size unchanged so the punched range reads back as zeroes.
//
// It is truncate's loop over a bounded range: reclaimCovered already rewrites
// an extent into the parts a write over [start, end) leaves readable, which is
// exactly what a punch needs, and it already declines ranges this node does not
// own so a peer's arena bitmap is never rewritten from here.
func (s *Service) punchHole(ctx context.Context, ino, start, end uint64) error {
	return retry(ctx, etcdAttempts, func() error {
		extents, err := s.store.GetExtents(ctx, ino)
		if err != nil {
			return fmt.Errorf("punch hole ino %d: read extents: %w", ino, err)
		}
		chunk := uint64(0)
		for _, e := range extents {
			if e.Chunk >= chunk {
				chunk = e.Chunk + 1
			}
		}
		for _, ext := range extents {
			if ext.End() <= start || ext.LogOff >= end {
				continue
			}
			if rerr := s.reclaimCovered(ctx, ext, start, end, &chunk); rerr != nil {
				return fmt.Errorf("punch hole ino %d: %w", ino, rerr)
			}
		}
		return nil
	})
}

// growTo publishes a larger size for an inode, leaving the new range as a hole.
//
// ponytail: this is a size change, not a reservation — blocks are claimed from
// the arena when a write lands, so a later write into the range can still fail
// with ENOSPC. Making it a real guarantee means allocating the blocks here and
// recording extents for them, which the arena allocator can already do; it is
// deferred because it would write out the zeroes a sparse file exists to avoid.
func (s *Service) growTo(ctx context.Context, ino, newSize uint64) error {
	rec, rev, err := s.store.GetInodeRev(ctx, ino)
	if err != nil {
		return fmt.Errorf("fallocate ino %d: %w", ino, err)
	}
	if rec == nil {
		return metadata.ErrNotFound
	}
	if newSize <= rec.Size {
		return nil
	}

	rec.Size = newSize
	rec.Mtime = time.Now()
	rec.Ctime = rec.Mtime
	ok, err := s.store.Txn(ctx,
		[]clientv3.Cmp{metadata.InodeUnchanged(ino, rev)},
		[]clientv3.Op{clientv3.OpPut(metadata.InodeKey(ino), string(metadata.EncodeInode(rec)))}, nil)
	if err != nil {
		return fmt.Errorf("fallocate ino %d: %w", ino, err)
	}
	if !ok {
		return fmt.Errorf("fallocate ino %d: %w", ino, metadata.ErrConflict)
	}
	return nil
}
