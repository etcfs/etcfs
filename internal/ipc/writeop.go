package ipc

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/pkg/arena"
	"github.com/etcfs/etcfs/pkg/metadata"
)

// writeOp is one WRITE in progress: the blocks it reserved, the metadata it
// planned against, and the proposal it built from the two.
//
// The stages used to be closures inside one function, threading this state
// between them by capture. They are the same stages — reserve and pad, put the
// bytes on the device, build the transaction — but each is now nameable,
// separately readable, and can be re-run on a retry without the reader having
// to track which captured variable the previous attempt left behind.
type writeOp struct {
	s      *Service
	ino    uint64
	offset uint64
	data   []byte
	uid    uint32

	// dataLen is the payload; padded rounds it up to whole blocks. Every run
	// starts and ends on a block boundary, so one padded buffer can be sliced
	// per run and still satisfy O_DIRECT. The padding past dataLen is written
	// but never referenced by an extent.
	dataLen int
	padded  int

	// gen is the fencing generation stamped on every extent this write
	// publishes.
	gen uint64

	// runs are the device ranges reserved for the payload; written records that
	// the bytes have already gone down, so the device write stays idempotent
	// across the paths that may both reach it.
	runs    []arena.Run
	written bool

	// rec and existing are the inode as this write planned against it. Both are
	// replaced on a retry, and every derived counter is recomputed from them.
	rec      *metadata.InodeRecord
	existing []metadata.Extent

	// chunk and seq run one past the highest in use — not off the extent count,
	// because a truncate deletes records from the middle and counting would hand
	// back a number that is still live.
	chunk uint64
	seq   uint64

	// end is the logical end of the file after this write, plans the reclaims
	// folded into the transaction, deferredReclaim the ones that did not fit,
	// and nextChunk the first chunk number left unused by either.
	end             uint64
	plans           []*reclaimPlan
	deferredReclaim []metadata.Extent
	nextChunk       uint64

	// modeChanged records that this write costs the file its set-user-ID bits,
	// which is the one part of a write that must not be deferred: deferring the
	// bytes trades durability, deferring this trades privilege.
	modeChanged bool
}

func newWriteOp(s *Service, ino, offset uint64, data []byte, uid uint32, gen uint64,
	m *inodeMeta) *writeOp {

	w := &writeOp{
		s: s, ino: ino, offset: offset, data: data, uid: uid, gen: gen,
		dataLen:  len(data),
		padded:   (len(data) + arena.BlockSize - 1) / arena.BlockSize * arena.BlockSize,
		rec:      m.rec,
		existing: m.extents,
	}
	w.countFrom()
	return w
}

// allocate reserves the device space for the payload.
func (w *writeOp) allocate(ctx context.Context) error {
	runs, err := w.s.allocateBlocks(ctx, uint64(w.dataLen))
	if err != nil {
		return err
	}
	w.runs = runs
	return nil
}

// freeRuns gives the reserved blocks back, for every path that gives up before
// anything in etcd refers to them.
func (w *writeOp) freeRuns() {
	for _, r := range w.runs {
		w.s.freeBlocks(r.DiskOff, r.Length)
	}
}

// writeThrough puts the payload on the device now, which is what every write
// that is not going to be buffered has to do before its extents are published.
// Idempotent, and a no-op once it has run.
func (w *writeOp) writeThrough() error {
	if w.written {
		return nil
	}
	writeData := w.data
	if w.padded != w.dataLen || !w.s.directSafe(w.data) {
		aligned, free := w.s.ioBuffer(w.padded)
		defer free()
		if !w.s.directSafe(aligned) {
			return errUnalignedBuffer
		}
		copy(aligned, w.data)
		writeData = aligned[:w.padded]
	}
	pos := uint64(0)
	for _, r := range w.runs {
		if werr := w.s.writeRun(writeData[pos:pos+r.Length], r.DiskOff); werr != nil {
			return werr
		}
		pos += r.Length
	}
	w.written = true
	return nil
}

// bufferedPayload copies the payload into per-run slices, for a write whose
// bytes are held in RAM alongside its extents rather than put on the device
// now. The flush writes them before it publishes anything naming them.
func (w *writeOp) bufferedPayload() []bufferedRun {
	payload := make([]byte, w.padded)
	copy(payload, w.data)
	bufData := make([]bufferedRun, 0, len(w.runs))
	pos := uint64(0)
	for _, r := range w.runs {
		bufData = append(bufData, bufferedRun{diskOff: r.DiskOff, buf: payload[pos : pos+r.Length]})
		pos += r.Length
	}
	return bufData
}

// countFrom recomputes the chunk and sequence numbers this write may use from
// the extent list it is planning against.
func (w *writeOp) countFrom() {
	w.chunk, w.seq = 0, 0
	for _, e := range w.existing {
		if e.Chunk >= w.chunk {
			w.chunk = e.Chunk + 1
		}
		if e.Seq >= w.seq {
			w.seq = e.Seq + 1
		}
	}
}

// readExtents re-reads the inode's extents and recounts from them.
//
// The list the lock cached is used as-is, and so is a serializable re-read:
// both only build a proposal, and the commit refuses to overwrite a chunk that
// already exists, so a stale answer costs a retry rather than a buried extent.
// That retry re-reads linearizably, which is the only way to be sure the second
// attempt is not working from the same stale view.
func (w *writeOp) readExtents(ctx context.Context, opts ...clientv3.OpOption) error {
	extents, err := w.s.store.GetExtents(ctx, w.ino, opts...)
	if err != nil {
		return err
	}
	w.existing = extents
	w.countFrom()
	return nil
}

// proposal builds the transaction this write commits — or buffers.
//
// One extent per run, in order, so the logical range stays contiguous even
// though the device ranges behind it are not. Rebuilt on a retry, since both
// counters move.
//
// Everything this write does to metadata goes into this one transaction: the
// new extents, the size change, and the rewrite of every extent the write
// buries. Each of those used to be its own round trip after the commit, and
// each was a Raft commit on the critical path of every write. Folding them also
// makes the write atomic in a way it was not: a buried extent stops being
// referenced at the same revision the extent burying it appears.
func (w *writeOp) proposal() ([]clientv3.Cmp, []clientv3.Op) {
	cmps := make([]clientv3.Cmp, 0, len(w.runs))
	ops := make([]clientv3.Op, 0, len(w.runs)+2)
	w.plans = w.plans[:0]
	w.deferredReclaim = w.deferredReclaim[:0]
	w.end = w.offset
	pos := uint64(0)
	next := w.chunk
	for _, r := range w.runs {
		// The final run is padded; its extent covers only the real bytes.
		extLen := min(r.Length, uint64(w.dataLen)-pos)
		// Every run of one write shares a sequence: they are one write, and they
		// cover disjoint logical ranges, so nothing needs to order them against
		// each other.
		ext := metadata.Extent{
			LogOff: w.offset + pos, DiskOff: r.DiskOff, Length: extLen,
			Gen: w.gen, Seq: w.seq, Node: w.s.store.NodeID(),
		}
		key := metadata.ExtentKey(w.ino, next)
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(key), "=", 0))
		ops = append(ops, clientv3.OpPut(key, ext.Encode()))
		next++
		w.end = ext.End()
		pos += r.Length
	}

	// Data is already durable on the device; if the guard rejects the commit the
	// bytes stay unreferenced and the blocks go back to the arena.
	// The inode is rewritten when the write grows the file, and also when it
	// costs the file its set-user-ID bits — both ride the transaction that
	// publishes the write rather than a round trip of their own.
	mode := metadata.ClearSetIDOnWrite(w.rec.Mode, w.uid)
	w.modeChanged = mode != w.rec.Mode
	if w.end > w.rec.Size || w.modeChanged {
		updated := *w.rec
		updated.Size = max(w.rec.Size, w.end)
		updated.Mode = mode
		ops = append(ops, clientv3.OpPut(metadata.InodeKey(w.ino), string(metadata.EncodeInode(&updated))))
	}

	// The write is about to bury these, so they stop being readable through the
	// extents that held them. Reclaiming here rather than leaving it all to the
	// scrubber keeps an overwrite-heavy workload from holding a scrub interval's
	// worth of buried blocks alive at all times.
	for _, old := range w.existing {
		if old.LogOff >= w.end || w.offset >= old.End() {
			continue
		}
		// etcd caps a transaction's size, and one write can bury many extents.
		// What does not fit is reclaimed afterwards, in the round trips of its
		// own this folding exists to avoid — correct either way, just not free.
		if len(ops)+maxReclaimOps+1 > maxWriteTxnOps || len(cmps)+1 > maxWriteTxnOps {
			w.deferredReclaim = append(w.deferredReclaim, old)
			continue
		}
		if p := w.s.planReclaim(old, w.offset, w.end, &next); p != nil {
			cmps = append(cmps, p.cmp)
			ops = append(ops, p.ops...)
			w.plans = append(w.plans, p)
		}
	}

	w.nextChunk = next
	return cmps, ops
}

// replan takes the inode as some other publication left it and rebuilds the
// proposal against it.
func (w *writeOp) replan(m *inodeMeta) ([]clientv3.Cmp, []clientv3.Op) {
	w.rec, w.existing = m.rec, m.extents
	w.countFrom()
	return w.proposal()
}

// reclaimDeferred returns what did not fit in the transaction, in a round trip
// each. The write itself is published and correct by this point; failing here
// only leaks blocks the scrubber will find.
func (w *writeOp) reclaimDeferred(ctx context.Context) {
	for _, old := range w.deferredReclaim {
		if rerr := w.s.reclaimCovered(ctx, old, w.offset, w.end, &w.nextChunk); rerr != nil {
			w.s.log.Warn("buried extent not reclaimed, the scrubber will pick it up",
				"ino", w.ino, "error", rerr)
		}
	}
}
