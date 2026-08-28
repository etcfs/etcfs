package ipc

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/pkg/arena"
	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/metrics"
)

// Write delegation: publishing an inode's extents is deferred while this node
// holds its exclusive lock.
//
// A write used to cost one device I/O plus one Raft commit, and the commit is
// the larger of the two by roughly a factor of two.  It does not have to be
// there: while this node holds an inode's exclusive lock no peer can take even
// a shared one, so no peer can observe the gap between the bytes landing on the
// device and the extent naming them appearing in etcd.  The extent record is
// buffered in the lock entry instead and published in batches.
//
// The trade is durability, and it is the same one NFS makes by default: a write
// that was acknowledged but not yet flushed is lost if this node crashes.  That
// is POSIX-legal — write() promises visibility, never durability — and it is
// what fsync, O_SYNC and a flush interval exist to bound.  The loss is clean:
// the bytes are on the volume and only the reference to them is missing, so the
// blocks are reclaimed on restart and the file reads back as it was at the last
// flush.  Publication is one transaction, so there is no partial state.
//
// What makes the flush safe rather than merely late is its comparison set: the
// keys are asserted to be where this node last saw them, and this node's own
// lock key is asserted to still exist by exact holder token.  A flush that
// arrives after a lost lease or a recall is rejected by that comparison and its
// blocks go back to the arena, so nothing a stale holder buffered can ever be
// published.

const (
	// defaultFlushInterval bounds how long an acknowledged write can sit
	// unpublished, and with it both the crash window and how far a peer's stat
	// of this inode can lag.
	defaultFlushInterval = 100 * time.Millisecond

	// defaultFlushMaxBytes caps how much acknowledged data one inode's buffer
	// may stand for.  The interval bounds the crash window in time; this bounds
	// it in bytes, for a writer fast enough to make the interval irrelevant.
	defaultFlushMaxBytes = 16 << 20

	// defaultBufferMaxBytes caps the acknowledged-but-unpublished payload held
	// across every inode at once.  The per-inode cap bounds one hot file; on its
	// own it bounds nothing at all, because a workload with a thousand hot files
	// multiplies it by a thousand and holds all of it in RAM.
	//
	// 256 MiB is sixteen inodes at the per-inode cap: generous enough that an
	// ordinary workload never reaches it, small enough to be a bound.
	defaultBufferMaxBytes = 256 << 20

	// flushIOConcurrency is how many of a flush's device writes may be in
	// flight at once.  A flush that issued them one at a time would spend the
	// whole batch at the device's latency, serialized, with an inode's lock
	// held — which is what a queue depth of one costs and what the device's
	// own queue exists to avoid.
	//
	// It buys time, not rate.  A provisioned volume meters operations per
	// second and charges the same for a batch however it is queued, so this
	// helps a batch of large writes, where latency is the binding constraint,
	// and does nothing for scattered small ones — see minBufferedWriteBytes,
	// which is where that distinction is actually made.
	//
	// ponytail: a constant rather than a knob, sized to keep a batch of the
	// size the transaction op cap allows (~46 writes) inside a couple of device
	// round trips.  Make it configurable when a device shows up that wants a
	// different queue depth.
	flushIOConcurrency = 32

	// minBufferedWriteBytes is the write size above which the payload is worth
	// buffering even when it continues nothing.
	//
	// The two regimes a shared volume presents want opposite things.  Small
	// scattered writes are rate-limited — a provisioned volume meters I/O
	// operations per second — so batching them changes nothing about the cost and
	// only turns steady latency into a burst; only merging adjacent ones into
	// fewer operations helps, which is what streakContinues detects.  A large
	// write is already a whole operation or more, so its workload is bound by the
	// device's latency at queue depth one instead, and there batching is pure
	// gain: the flush issues the batch against the device's queue rather than one
	// write at a time.
	//
	// 64 KiB sits above the point a write stops being cheap to meter and starts
	// being worth pipelining.
	minBufferedWriteBytes = 64 << 10

	// flushBatchInodes is how many inodes one pass of the interval sweep takes
	// in hand at a time.
	//
	// The sweep holds an inode's local lock from the moment it claims it until
	// the transaction it rides has committed, so an operation on a claimed inode
	// waits for that commit.  Chunking bounds that wait at one commit however
	// many inodes the sweep finds, and the transaction op cap already splits a
	// chunk further when its inodes are carrying large buffers.
	flushBatchInodes = 64

	// oDSync is O_DSYNC as the kernel passes it in a write request's flags.
	// O_SYNC carries this bit too, so one test covers both.
	oDSync = 0o10000
)

// pending is the metadata one or more writes have produced but not yet
// published: the transaction that would publish them, coalesced by key.
//
// Coalescing by key is what keeps the comparison set honest across several
// buffered writes.  A key's comparison is taken the first time the buffer
// touches it — when it still describes etcd's real state — and never replaced,
// because the revisions a later write sees for that key come from this buffer
// rather than from etcd.  The operations themselves are last-write-wins, which
// is exactly the order the writes were served in.
type pending struct {
	cmps map[string]clientv3.Cmp
	ops  map[string]clientv3.Op
	// order preserves the order keys were first touched in, so a flush builds
	// the same transaction on every attempt.
	order []string

	// plans hold the blocks buried writes gave back.  They stay reserved in
	// this node's arena until the flush commits: freeing them earlier would let
	// the allocator hand out blocks an extent still published in etcd refers to.
	plans []*reclaimPlan

	// data is the payload of those writes, when it is buffered rather than put
	// on the device as the write is served.  Each entry is one run: the device
	// range was reserved at write time, so the extents above already name their
	// final offsets and only the bytes are outstanding.
	data []bufferedRun

	// runs are the blocks those buffered writes reserved.  A buffer that can
	// never be published leaves its extents unwritten, so its blocks are given
	// back with it — without this they would stay marked allocated in this
	// node's bitmap until the process restarted and rebuilt it from etcd.
	runs []arena.Run

	bytes uint64    // buffered write payload, for the size cap
	since time.Time // when the oldest unflushed write landed

	// err marks a flush that failed for a transient reason.  The buffer is
	// kept — discarding dirty state after a failed writeback is the Postgres
	// fsyncgate failure, where the retry succeeds with the data gone — and
	// every fsync fails until a flush commits.
	err error
}

func (p *pending) empty() bool { return p == nil || len(p.order) == 0 }

// bufferedRun is one device range whose bytes are still in RAM.
type bufferedRun struct {
	diskOff uint64
	buf     []byte
}

func (r bufferedRun) end() uint64 { return r.diskOff + uint64(len(r.buf)) }

// readAt serves a device range from the buffer, reporting false when the range
// is not wholly buffered.
//
// ponytail: a linear scan of the runs, which is bounded by the buffer cap and
// sits behind a read that would otherwise be a device round trip.  A sorted
// index is the upgrade if a profile ever shows it.
func (p *pending) readAt(dst []byte, diskOff uint64) bool {
	if p == nil {
		return false
	}
	end := diskOff + uint64(len(dst))
	for _, r := range p.data {
		if r.diskOff <= diskOff && end <= r.end() {
			copy(dst, r.buf[diskOff-r.diskOff:])
			return true
		}
	}
	return false
}

// add folds one write's proposal into the buffer.
func (p *pending) add(cmps []clientv3.Cmp, ops []clientv3.Op, plans []*reclaimPlan, bytes uint64) {
	if p.cmps == nil {
		p.cmps, p.ops, p.since = make(map[string]clientv3.Cmp), make(map[string]clientv3.Op), time.Now()
	}
	// Comparisons first, and only for keys the buffer does not already own: a
	// key this buffer has written carries a revision that exists nowhere but
	// here, so a comparison against it could never pass.  The comparison
	// already recorded for that key still describes etcd's real state and is
	// the one the flush must carry.
	for _, c := range cmps {
		key := string(c.Key)
		if _, owned := p.ops[key]; owned {
			continue
		}
		p.cmps[key] = c
	}
	for _, op := range ops {
		key := string(op.KeyBytes())
		if _, seen := p.ops[key]; !seen {
			p.order = append(p.order, key)
		}
		p.ops[key] = op
	}
	p.plans = append(p.plans, plans...)
	p.bytes += bytes
}

// txn returns the buffered transaction, in the order the keys were first
// touched.
func (p *pending) txn() ([]clientv3.Cmp, []clientv3.Op) {
	cmps := make([]clientv3.Cmp, 0, len(p.order))
	ops := make([]clientv3.Op, 0, len(p.order))
	for _, key := range p.order {
		if c, found := p.cmps[key]; found {
			cmps = append(cmps, c)
		}
		ops = append(ops, p.ops[key])
	}
	return cmps, ops
}

// reset empties the buffer, returning the plans whose blocks a commit frees and
// the runs a discard frees.  Which of the two the caller gives back is the
// difference between a flush that published and one that never can: a published
// extent owns its run, and only what it buried becomes free.
func (p *pending) reset() ([]*reclaimPlan, []arena.Run) {
	plans, runs := p.plans, p.runs
	p.cmps, p.ops, p.order, p.plans = nil, nil, nil, nil
	p.data, p.runs, p.bytes, p.err = nil, nil, 0, nil
	return plans, runs
}

// deferWrites reports whether extent publication may be deferred at all.
// Zero is today's behaviour: one commit per write.
func (s *Service) deferWrites() bool { return s.flushInterval > 0 }

// errBufferPublished reports that the buffer had to be published to make room
// for the write being folded in, and that the write was therefore not folded.
//
// The caller must re-plan before trying again.  A flush moves every key it
// wrote to a new revision, so a proposal built before it — including the
// snapshot the caller derived for the cache — compares against revisions etcd
// has already left behind, and could never commit.
var errBufferPublished = errors.New("the buffer was published to make room")

// bufferWrite folds a committed-to-device write into the entry's buffer and
// updates the cached metadata to match, so this node's own reads and its next
// write see the write immediately.
//
// The caller holds the entry's exclusive local lock, so no other operation on
// this inode is in flight.
// data and runs are non-empty only when the payload is buffered too: the bytes
// this write would otherwise have put on the device, and the blocks reserved to
// hold them.  Both are empty when the write has already been written through.
func (s *Service) bufferWrite(ctx context.Context, e *lockEntry, replay *txnReplay,
	cmps []clientv3.Cmp, ops []clientv3.Op, plans []*reclaimPlan, bytes uint64,
	data []bufferedRun, runs []arena.Run) error {

	e.keyMu.Lock()
	defer e.keyMu.Unlock()

	if e.pending == nil {
		e.pending = &pending{}
	}
	// etcd caps a transaction's size, so a buffer that cannot absorb this write
	// is published before the write joins it.  The write does not join it here:
	// its plan predates the flush, and the caller has to build a new one.  The
	// retry cannot land here again, because the buffer it found full is empty
	// now and one write's proposal is capped at what a transaction can carry.
	if !e.pending.empty() &&
		(len(e.pending.order)+len(ops) > maxWriteTxnOps || e.pending.bytes+bytes > s.flushMaxBytes) {
		if err := s.flushLocked(ctx, e, "buffer_full"); err != nil {
			return err
		}
		return errBufferPublished
	}

	// The per-inode cap above says nothing about how many inodes are buffering
	// at once, so the process-wide total is drained here before this write joins
	// it.  Other inodes' buffers are published, never this one's: a flush of
	// this entry would invalidate the proposal the caller is holding, which is
	// what errBufferPublished exists to signal, and the caller has already been
	// told its own buffer had room.
	if s.bufferedBytes.Load()+int64(bytes) > s.bufferMaxBytes {
		s.drainBuffers(ctx, e)
	}

	before := len(e.pending.order)
	e.pending.add(cmps, ops, plans, bytes)
	s.bufferAccounted(len(e.pending.order)-before, int64(bytes))
	e.pending.data = append(e.pending.data, data...)
	e.pending.runs = append(e.pending.runs, runs...)
	// The snapshot is updated under the same mutex the buffer is, so the two can
	// never disagree about what this node has written — and only here, past
	// every return above, so a write that did not join the buffer leaves the
	// snapshot exactly as it found it.
	if e.holder != "" && e.meta != nil {
		// The snapshot is edited in place, which is safe only while no reader
		// can be looking at it.
		e.mustHoldExclusive(s, "bufferWrite")
		e.meta, e.metaFor = replay.apply(e.meta, 0), e.holder
	}

	return nil
}

// bufferAccounted moves the process-wide buffered totals by one entry's change
// and republishes them as the pending gauges.
//
// Process-wide rather than per-entry because that is the number that matters in
// both uses: it is what a crash would lose right now, and it is what bounds the
// RAM these buffers may occupy.  The gauges used to be set from whichever inode
// was last written, so with more than one inode buffering they described no
// real quantity at all.
func (s *Service) bufferAccounted(ops int, bytes int64) {
	totalOps := s.bufferedOps.Add(int64(ops))
	totalBytes := s.bufferedBytes.Add(bytes)
	metrics.PendingExtents.Set(float64(totalOps))
	metrics.PendingBytes.Set(float64(totalBytes))
}

// drainBuffers publishes other inodes' buffers until the process-wide total is
// back under the cap.
//
// Entries with an operation in flight are skipped rather than waited for: a
// flush cannot run alongside an operation whose comparisons it would
// invalidate, and blocking here would hold one writer behind another's slow
// request.  If every buffer is in flight the total stays over the cap until
// they finish, which overshoots by at most one write per inode in flight.
func (s *Service) drainBuffers(ctx context.Context, skip *lockEntry) {
	var entries []*lockEntry
	for _, e := range s.locks.all() {
		if e != skip {
			entries = append(entries, e)
		}
	}

	for _, e := range entries {
		if s.bufferedBytes.Load() <= s.bufferMaxBytes {
			return
		}
		// TryLock, never Lock: the caller reached here holding another entry's
		// keyMu, and flushEntry below takes this one's.  Waiting for the lock
		// would let this and an eviction sweep — the other path that holds
		// several entries at once — each end up waiting for what the other
		// holds.  Skipping a busy entry costs a buffer that stays until the
		// next sweep.
		if !e.tryLockExclusive() {
			continue
		}
		if err := s.flushEntry(ctx, e, "memory_pressure"); err != nil {
			s.log.Warn("deferred writes not published under memory pressure",
				"ino", e.ino, "error", err)
		}
		e.unlockExclusive()
	}
}

// flushEntry publishes an entry's buffer.  The caller must hold the entry's
// exclusive local lock: the flush rewrites the very keys an in-flight
// operation's comparisons were built against.
func (s *Service) flushEntry(ctx context.Context, e *lockEntry, trigger string) error {
	e.keyMu.Lock()
	defer e.keyMu.Unlock()
	return s.flushLocked(ctx, e, trigger)
}

// proposeFlushLocked puts a buffer's data on the device and returns the
// transaction that would publish it, with keyMu already held.
//
// Nil comparisons and nil operations mean there is nothing left to publish: the
// buffer was empty, or its lock key is gone and the buffer with it.  An error
// means the device write failed, which keeps the buffer for a later attempt.
//
// Split out from flushLocked because it is the half a batch shares.  One
// inode's proposal asserts only things about that inode — the keys it is
// rewriting are where this node last saw them, and its own lock key still
// exists — so several inodes' proposals concatenate into one transaction that
// means exactly what the separate ones meant.
func (s *Service) proposeFlushLocked(e *lockEntry) ([]clientv3.Cmp, []clientv3.Op, error) {
	// The key is gone, so nothing buffered under it may be published.  The
	// blocks were never referenced by anything in etcd, so giving them back is
	// safe and is the only way they come back at all.
	if e.holder == "" {
		s.discardPending(e, "the lock key backing them is gone")
		return nil, nil, nil
	}

	// Data before metadata, always.  Publishing an extent whose bytes are not
	// yet on the volume is the one inversion that turns a lost write into a read
	// of garbage: a crash between the two leaves blocks nothing references, which
	// is unreachable and reclaimed, while the reverse leaves a file pointing at
	// whatever those blocks held before.
	if err := s.flushData(e.pending); err != nil {
		metrics.FlushFailures.WithLabelValues("device").Inc()
		e.pending.err = err
		s.log.Error("buffered write data not put on the device; it is kept and will be retried",
			"ino", e.ino, "error", err)
		return nil, nil, err
	}

	cmps, ops := e.pending.txn()
	// By exact holder token rather than over the lock prefix: a prefix check is
	// satisfied by the key of the peer that took the inode from us, which is
	// precisely the case the comparison exists to reject.
	cmps = append(cmps, clientv3.Compare(
		clientv3.CreateRevision(metadata.LockKey(e.ino, e.mode, e.holder)), "!=", 0))
	return cmps, ops, nil
}

// flushLocked publishes the buffer with keyMu already held.
//
// Everything the buffered writes would have committed goes in one transaction,
// with this node's own lock key added to the comparisons.  See
// proposeFlushLocked, and flushEntries for the same transaction shared by
// several inodes.
func (s *Service) flushLocked(ctx context.Context, e *lockEntry, trigger string) error {
	if e.pending.empty() {
		return nil
	}
	ctx = metadata.WithTxnOrigin(ctx, "extent_flush")

	cmps, ops, err := s.proposeFlushLocked(e)
	if err != nil {
		return err
	}
	if ops == nil {
		return nil
	}

	committed, fenced, rev, err := s.commitGuarded(ctx, cmps, ops)
	metrics.Flushes.WithLabelValues(trigger).Inc()

	switch {
	case fenced:
		// A fenced node must never publish, and never will: the guard rejects
		// every later attempt too, so keeping the buffer only holds its blocks
		// hostage until the process exits.
		metrics.FlushFailures.WithLabelValues("fenced").Inc()
		s.discardPending(e, "this node has been fenced")
		return metadata.ErrFenced

	case err != nil:
		// Transient. The buffer survives and fsync keeps failing until a flush
		// commits — discarding here is what makes a retried fsync succeed with
		// the data gone.
		metrics.FlushFailures.WithLabelValues("error").Inc()
		e.pending.err = err
		s.log.Error("deferred writes not published; they are kept and will be retried",
			"ino", e.ino, "error", err)
		return err

	case !committed:
		// A rejection is also what a committed transaction whose reply was lost
		// looks like, so that is ruled out before anything is discarded or
		// given up on.
		if revs, landed := s.flushAlreadyLanded(ctx, e); landed {
			s.log.Warn("adopted a flush whose reply was lost; its writes are published",
				"ino", e.ino)
			s.flushCommitted(e, revs)
			return nil
		}

		metrics.FlushFailures.WithLabelValues("rejected").Inc()
		if held, herr := s.store.LockHeldBy(ctx, e.ino, e.mode, e.holder); herr == nil && !held {
			// The lease went, etcd deleted the key, and a peer may already own
			// the inode.  Nothing buffered was ever published, so nothing can
			// reference the blocks.
			s.keyLostLocked(e, "the lock key backing them was lost")
			return metadata.ErrConflict
		}
		// The lock key is still ours and a comparison still failed, so an inode
		// this node holds was modified by something that did not take its lock.
		// The buffer cannot be published from here and must not be dropped.
		e.pending.err = metadata.ErrConflict
		s.log.Error("deferred writes rejected while the lock is still held; the inode was modified without it",
			"ino", e.ino)
		return metadata.ErrConflict
	}

	revs := make(map[string]int64, len(ops))
	for _, op := range ops {
		if op.IsPut() {
			revs[string(op.KeyBytes())] = rev
		}
	}
	s.flushCommitted(e, revs)
	return nil
}

// flushData puts the buffered payload on the device: adjacent runs coalesced
// into one I/O, and the resulting I/Os issued concurrently.
//
// The two do different jobs, against different limits, and conflating them is
// what made the first version of this slower than no buffering at all.
//
// Coalescing reduces the number of operations the device is charged for, which
// is the only thing that helps against a provisioned volume: it meters I/O
// operations per second, so a batch of scattered writes costs exactly what the
// same writes cost issued one at a time. Merging needs adjacency, and only a
// caller that has one gets this far — streakContinues turns the rest away
// precisely because batching them would buy nothing.
//
// Issuing concurrently reduces the time the batch takes, which helps against
// the device's latency at queue depth one. That is the large-write case, where
// a single write is already a whole operation or more and the rate is not the
// binding constraint. It buys no rate: a metered device charges the same 46
// operations whether they are outstanding together or in sequence — measured,
// and it is why parallel issue alone moved sequential throughput from 12 to
// 44 MiB/s while leaving random-write p99 at 64ms.
//
// Every write targets a block reserved by this node and named by no published
// extent, so they are disjoint and may be issued in any order. What may not
// move is the boundary after them: all of it is on the device before the
// transaction naming any of it is committed.
//
// The runs stay in the buffer, since the extents naming them are still
// unpublished; only the payload is dropped, because from here the device is
// where those bytes live and a retried flush must not write them twice.
func (s *Service) flushData(p *pending) error {
	if len(p.data) == 0 {
		return nil
	}
	sort.Slice(p.data, func(i, j int) bool { return p.data[i].diskOff < p.data[j].diskOff })

	// Coalesce first, serially: it is memcpy against a device round trip, and
	// the groups have to exist before they can be handed out.
	type group struct {
		diskOff uint64
		buf     []byte
		free    func()
		runs    int
	}
	var groups []group
	defer func() {
		for _, g := range groups {
			g.free()
		}
	}()
	for i := 0; i < len(p.data); {
		j, length := i+1, uint64(len(p.data[i].buf))
		for j < len(p.data) && p.data[j].diskOff == p.data[i].diskOff+length {
			length += uint64(len(p.data[j].buf))
			j++
		}
		// Copied even for a single run: the payload is an ordinary Go slice, and
		// O_DIRECT needs a sector-aligned address the allocator cannot promise.
		buf, free := s.ioBuffer(int(length))
		pos := 0
		for _, r := range p.data[i:j] {
			pos += copy(buf[pos:], r.buf)
		}
		groups = append(groups, group{diskOff: p.data[i].diskOff, buf: buf[:length], free: free, runs: j - i})
		i = j
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, flushIOConcurrency)
	for _, g := range groups {
		wg.Add(1)
		sem <- struct{}{}
		go func(g group) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.writeRun(g.buf, g.diskOff); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	p.data = nil
	return nil
}

// flushCommitted finishes a flush that reached etcd.
//
// The blocks its reclaims freed go back to the arena — only now, because before
// the commit a rejected transaction would have freed blocks the file still
// refers to — and the cached extents take the revisions their keys now carry,
// which is what a later comparison on those extents has to match.
func (s *Service) flushCommitted(e *lockEntry, revs map[string]int64) {
	ops, bytes := len(e.pending.order), e.pending.bytes
	plans, _ := e.pending.reset()
	for _, p := range plans {
		s.freeReclaimed(p)
	}
	s.bufferAccounted(-ops, -int64(bytes))

	if e.meta == nil {
		return
	}
	extents := make([]metadata.Extent, len(e.meta.extents))
	copy(extents, e.meta.extents)
	for i := range extents {
		if rev, found := revs[extents[i].Key]; found {
			extents[i].ModRevision = rev
		}
	}
	e.meta = &inodeMeta{rec: e.meta.rec, extents: extents}
}

// flushEntries publishes several inodes' buffers in one transaction.
//
// One inode's proposal asserts only things about that inode, so their
// comparison sets concatenate and the batch means precisely what the separate
// flushes meant — for one Raft commit rather than N.  That is what an unpacking
// archive needs: every file it closes leaves a buffer, and the sweep that
// publishes them was paying a commit per file.
//
// The caller holds each entry's exclusive local lock, for the same reason a
// single flush needs it: the transaction rewrites the very keys an in-flight
// operation's comparisons were built against.
//
// A batch is all-or-nothing, which is the wrong shape for a rejection: one
// inode whose lock key was lost would hold every other inode's writes
// unpublished forever.  So a rejected batch is not diagnosed here.  Each member
// is retried on its own through flushLocked, which has the per-inode handling a
// rejection needs — a commit whose reply was lost, a key the lease dropped, a
// key still held while a comparison failed anyway.  The batch is the fast path;
// the single flush remains the one that decides what a failure meant.
func (s *Service) flushEntries(ctx context.Context, entries []*lockEntry, trigger string) {
	var batch flushBatch
	for _, e := range entries {
		e.keyMu.Lock()
		if e.pending.empty() {
			e.keyMu.Unlock()
			continue
		}
		cmps, ops, err := s.proposeFlushLocked(e)
		if err != nil || ops == nil {
			// Either the device write failed and the buffer is kept for the next
			// sweep, or there was nothing left to publish.  Both are already
			// accounted for by proposeFlushLocked.
			e.keyMu.Unlock()
			continue
		}
		// etcd caps how many operations and comparisons one transaction may
		// carry, and a single inode's proposal can approach that cap on its own.
		if !batch.fits(cmps, ops) {
			s.commitFlushBatch(ctx, &batch, trigger)
		}
		batch.add(e, cmps, ops)
	}
	s.commitFlushBatch(ctx, &batch, trigger)
}

// flushBatch is the transaction several inodes' buffers share, and the entries
// riding it.  Each member's keyMu is held from the moment it joins until the
// transaction has been accounted for.
type flushBatch struct {
	entries []*lockEntry
	cmps    []clientv3.Cmp
	ops     []clientv3.Op
}

// fits reports whether one more proposal stays inside etcd's transaction cap.
// A batch of one always fits: a single write's proposal is capped at what a
// transaction can carry (see bufferWrite), so an entry that does not fit an
// empty batch cannot be made to fit any batch.
func (b *flushBatch) fits(cmps []clientv3.Cmp, ops []clientv3.Op) bool {
	return len(b.entries) == 0 ||
		(len(b.ops)+len(ops) <= maxWriteTxnOps && len(b.cmps)+len(cmps) <= maxWriteTxnOps)
}

func (b *flushBatch) add(e *lockEntry, cmps []clientv3.Cmp, ops []clientv3.Op) {
	b.entries = append(b.entries, e)
	b.cmps = append(b.cmps, cmps...)
	b.ops = append(b.ops, ops...)
}

// release drops every member's keyMu and empties the batch.
func (b *flushBatch) release() {
	for _, e := range b.entries {
		e.keyMu.Unlock()
	}
	b.entries, b.cmps, b.ops = nil, nil, nil
}

// commitFlushBatch publishes a batch and accounts for what happened to each of
// its members, then releases them.
func (s *Service) commitFlushBatch(ctx context.Context, b *flushBatch, trigger string) {
	if len(b.entries) == 0 {
		return
	}
	defer b.release()

	committed, fenced, rev, err := s.commitGuarded(ctx, b.cmps, b.ops)
	metrics.FlushBatches.Inc()
	metrics.FlushBatchInodes.Add(float64(len(b.entries)))
	for range b.entries {
		metrics.Flushes.WithLabelValues(trigger).Inc()
	}

	switch {
	case fenced:
		// A fenced node must never publish, and never will: the guard rejects
		// every later attempt too, so keeping the buffers only holds their
		// blocks hostage until the process exits.
		for _, e := range b.entries {
			metrics.FlushFailures.WithLabelValues("fenced").Inc()
			s.discardPending(e, "this node has been fenced")
		}

	case err != nil:
		// Transient, and already retried to exhaustion by commitGuarded.  The
		// buffers survive and every fsync on them keeps failing until a flush
		// commits — discarding here is what makes a retried fsync succeed with
		// the data gone.
		for _, e := range b.entries {
			metrics.FlushFailures.WithLabelValues("error").Inc()
			e.pending.err = err
		}
		s.log.Error("deferred writes not published; they are kept and will be retried",
			"inodes", len(b.entries), "error", err)

	case !committed:
		// Which inode's comparison failed is not knowable from here, so each is
		// retried alone and told apart by the handling flushLocked already has.
		for _, e := range b.entries {
			if ferr := s.flushLocked(ctx, e, trigger); ferr != nil {
				s.log.Warn("deferred writes not published after the batch they rode was rejected",
					"ino", e.ino, "error", ferr)
			}
		}

	default:
		revs := make(map[string]int64, len(b.ops))
		for _, op := range b.ops {
			if op.IsPut() {
				revs[string(op.KeyBytes())] = rev
			}
		}
		for _, e := range b.entries {
			s.flushCommitted(e, revs)
		}
	}
}

// flushAlreadyLanded reports whether the transaction a flush just had rejected
// is in fact already in etcd, and the revisions its keys carry if so.
//
// commitGuarded retries a transient failure, and the retry re-proposes the same
// comparisons — including `CreateRevision == 0` on keys the first attempt has by
// then created. So a committed transaction whose reply was lost comes back as a
// rejection. Taking that at face value would strand a buffer that was in fact
// published: its fsync would return EIO for good, its reclaimed blocks would
// stay reserved, and every later write to the inode would be doomed to the same
// rejection.
//
// Only this node can have written those keys — it holds the inode's exclusive
// lock, and the values carry its own extents — so a key present with exactly
// the value this flush proposed is proof the flush landed. This is the same
// problem, and the same answer, as an acquisition whose reply was lost.
func (s *Service) flushAlreadyLanded(ctx context.Context, e *lockEntry) (map[string]int64, bool) {
	_, published, err := s.store.GetInodeAndExtents(ctx, e.ino)
	if err != nil {
		return nil, false
	}

	revs := make(map[string]int64, len(published))
	values := make(map[string]string, len(published))
	for _, ext := range published {
		revs[ext.Key] = ext.ModRevision
		values[ext.Key] = ext.Encode()
	}

	// The inode record rides the same transaction but is not an extent, and a
	// buffer carrying nothing but that record proves nothing either way.
	prefix := metadata.ExtentPrefix(e.ino)
	checked := 0
	for _, key := range e.pending.order {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		op := e.pending.ops[key]
		value, found := values[key]
		switch {
		case op.IsDelete() && found:
			return nil, false
		case op.IsPut() && (!found || value != string(op.ValueBytes())):
			return nil, false
		}
		checked++
	}
	return revs, checked > 0
}

// discardPending drops a buffer that can never be published and returns its
// blocks to the arena.
func (s *Service) discardPending(e *lockEntry, why string) {
	ops, bytes := len(e.pending.order), e.pending.bytes
	plans, runs := e.pending.reset()
	for _, p := range plans {
		s.freeReclaimed(p)
	}
	// The extents naming these runs will never be published, so nothing on the
	// device or in etcd refers to them and they are free to hand out again.
	for _, r := range runs {
		s.freeBlocks(r.DiskOff, r.Length)
	}
	// The cached snapshot describes writes that no longer exist anywhere.
	e.meta, e.metaFor = nil, ""
	s.bufferAccounted(-ops, -int64(bytes))
	s.log.Error("deferred writes discarded and their blocks released", "ino", e.ino,
		"operations", ops, "reason", why)
}

// flushInode publishes an inode's buffer from a caller that holds no lock on
// it.  A no-op when nothing is deferred.
func (s *Service) flushInode(ctx context.Context, ino uint64) error {
	if !s.deferWrites() || !s.hasPending(ino) {
		// Every close() sends a flush, most of them for descriptors that never
		// wrote.  Answering those without taking the exclusive lock is what
		// keeps a read-only open from paying a commit to upgrade a cached
		// shared lock it never needed.
		return nil
	}
	lk, err := s.lockInode(ctx, ino, metadata.LockExclusive)
	if err != nil {
		return err
	}
	defer lk.Release()
	// The flush republishes the snapshot's revisions itself, so the fail-safe
	// drop an unpublishing exclusive hold would otherwise do is not needed.
	lk.metaPublished = true
	return s.flushEntry(ctx, lk.e, "operation")
}

// FlushAll publishes every buffer this node is holding.  Called before the
// cached locks are given up on shutdown, so a peer that takes an inode next
// sees the writes this node acknowledged.
//
// It waits for each inode rather than skipping a busy one: nothing here is on a
// request's critical path, and a buffer left behind is acknowledged data lost.
// It takes them in the same chunks the interval sweep does, so it never holds
// the whole cache's local locks across a commit.
func (s *Service) FlushAll(ctx context.Context, trigger string) {
	held := make([]*lockEntry, 0, flushBatchInodes)
	for _, e := range s.locks.all() {
		e.lockExclusive()
		held = append(held, e)
		if len(held) == flushBatchInodes {
			s.flushHeld(ctx, held, trigger)
			held = held[:0]
		}
	}
	s.flushHeld(ctx, held, trigger)
}

// flushHeld publishes a chunk of entries whose local locks the caller holds,
// and gives them back.
func (s *Service) flushHeld(ctx context.Context, held []*lockEntry, trigger string) {
	if len(held) == 0 {
		return
	}
	s.flushEntries(ctx, held, trigger)
	for _, e := range held {
		e.unlockExclusive()
	}
}

// StartFlusher publishes buffers that have been sitting longer than the flush
// interval, which is what bounds the crash window and a peer's stat lag for an
// inode nobody is contending for.
//
// ponytail: a sweep of the lock cache per tick, which is bounded by
// lockCacheMax and skips any entry with an operation in flight.  A queue of
// dirty inodes is the upgrade if the sweep ever shows up in a profile.
func (s *Service) StartFlusher(ctx context.Context) {
	if !s.deferWrites() {
		return
	}
	go func() {
		ticker := time.NewTicker(s.flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.flushExpired(ctx)
			}
		}
	}()
}

func (s *Service) flushExpired(ctx context.Context) {
	deadline := time.Now().Add(-s.flushInterval)

	due := make([]*lockEntry, 0, flushBatchInodes)
	for _, e := range s.locks.all() {
		e.keyMu.Lock()
		expired := !e.pending.empty() && e.pending.since.Before(deadline)
		e.keyMu.Unlock()
		if !expired {
			continue
		}
		// TryLock rather than Lock: an inode with an operation in flight is
		// skipped and picked up on the next tick.  Waiting would hold the
		// sweeper behind one slow request, and the flush must not run alongside
		// an operation whose comparisons it would invalidate.
		if !e.tryLockExclusive() {
			continue
		}
		due = append(due, e)
		// A chunk at a time, rather than the whole sweep at once: the entries in
		// hand have their local locks held across the commit, and an operation on
		// one of them waits that long.  Publishing in transaction-sized chunks
		// bounds that wait at one commit however many inodes the sweep finds.
		if len(due) == flushBatchInodes {
			s.flushHeld(ctx, due, "interval")
			due = due[:0]
		}
	}
	s.flushHeld(ctx, due, "interval")
}

// FSYNC/FLUSH/FSYNCDIR payload: [u64:ino]
// Response: [i32:error]
//
// All three publish what this node is holding for the inode and block until it
// commits, which is what makes the acknowledgement mean what fsync promises.
// A flush that failed for a transient reason keeps its buffer and keeps
// returning EIO here until one commits.
//
// For a directory the deferred state is its timestamp rather than any extent,
// and the flush of it is what makes fsyncdir mean something: a create is
// published by its own transaction before it is acknowledged, but the parent's
// clock is queued (see the directory write-behind in pkg/metadata) and a caller
// that asked for the directory to be durable is asking for that too.
func (s *Service) handleFsync(ctx context.Context, payload []byte) ([]byte, error) {
	return s.fsyncInode(ctx, payload, true)
}

// FLUSH is close(), which promises nothing about durability — POSIX asks it to
// surface an error the descriptor already suffered, and nothing more.  It still
// publishes the extents, because a peer that opens the file next has to see the
// data (see close-to-open in the design decisions), but not the timestamps: an
// archive sets a file's times on the descriptor and then closes it, so
// publishing them here is a commit per file for a change nobody asked to be
// durable, which is exactly the commit deferring them was meant to remove.
// They are late by at most one sweep, which is the bound already documented.
func (s *Service) handleFlush(ctx context.Context, payload []byte) ([]byte, error) {
	return s.fsyncInode(ctx, payload, false)
}

func (s *Service) fsyncInode(ctx context.Context, payload []byte, durable bool) ([]byte, error) {
	r := newReader(payload)
	ino := r.u64()
	if !r.ok {
		return int32Resp(errInval), nil
	}

	if durable {
		s.store.FlushInodeTimes(ctx, ino)
	}

	if err := s.flushInode(ctx, ino); err != nil {
		s.log.Warn("fsync: cannot publish deferred writes", "ino", ino, "error", err)
		return int32Resp(errIO), nil
	}

	// Without O_DIRECT the bytes are in this node's page cache rather than on
	// the volume, so the extent being published says nothing about the data
	// being there.  With it, the pwrite was the publication and this is free to
	// skip.
	if s.dev != nil && !s.dev.IsDirect() {
		if err := s.dev.FlushDevice(); err != nil {
			s.log.Warn("fsync: cannot flush the device", "ino", ino, "error", err)
			return int32Resp(errIO), nil
		}
	}
	return okResp(), nil
}
