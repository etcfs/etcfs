// Package arena implements the arena-based block allocator.
//
// The raw block device is divided into arenas (~1 GiB contiguous ranges),
// each leased exclusively to one node via an etcd transaction on
// arena:<node_id>/<arena_id>.  A node allocates blocks from its arenas using a
// local free-list, only touching etcd when acquiring or releasing arenas.
//
// This converts the classic distributed-allocator hot-key problem into
// an infrequent etcd operation (~one per GB of writes).
package arena

import (
	"context"
	"fmt"
	"math/bits"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/metrics"
)

// ArenaSizeBytes is the default arena size (1 GiB).
const ArenaSizeBytes = 1 << 30

// BlockSize is the allocation granularity within an arena (4 KiB).  Defined by
// the metadata layer, which needs it to snap a split extent's surviving parts
// to boundaries this allocator can hand back out.
const BlockSize = metadata.BlockSize

// BlocksPerArena is the number of allocatable blocks per arena.
const BlocksPerArena = ArenaSizeBytes / BlockSize

// Allocator manages block allocation within this node's arenas.
type Allocator struct {
	mu     sync.Mutex
	nodeID string
	store  *metadata.Store
	arenas []*Arena

	// doubleFrees counts blocks freed while already free — see freeLocked.
	doubleFrees uint64

	// deviceSize bounds the arenas that may be handed out.  Zero means unknown
	// — a metadata-only service with no device attached — and no arena is
	// refused for size in that case.
	deviceSize uint64
}

// ErrNoSpace reports that the device has no room for another arena.  It is a
// distinct error because the caller has to answer ENOSPC rather than EIO: a
// write past the end of the device fails at the pwrite with a short write or
// EINVAL, which surfaces as a disk error rather than a full filesystem.
var ErrNoSpace = fmt.Errorf("no space left on device")

// Arena represents a single contiguous range on the block device.
type Arena struct {
	ID        uint64 // unique arena identifier
	DiskStart uint64 // byte offset on the block device
	DiskEnd   uint64 // byte offset (exclusive)

	// bitmap tracks allocated blocks.  bit=1 means allocated, bit=0 means free.
	bitmap []uint64

	// hint is where the next search starts, left where the last one finished.
	// Without it every allocation rescanned the arena from block 0, so a
	// nearly-full arena cost a full sweep per call and a fragmented one cost
	// several.  It is only a hint: the search wraps, so nothing is missed if it
	// points past free space that has since been returned.
	hint uint64
}

// SetDeviceSize tells the allocator how large the shared device is, so it can
// refuse an arena that would not fit on it.
func (a *Allocator) SetDeviceSize(bytes uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deviceSize = bytes
}

// DeviceSize returns the size the allocator was given, or 0 if none was.
func (a *Allocator) DeviceSize() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.deviceSize
}

// NewAllocator creates an Allocator for the given node.
func NewAllocator(nodeID string, store *metadata.Store) *Allocator {
	return &Allocator{
		nodeID: nodeID,
		store:  store,
	}
}

// AcquireArena reserves a new arena from the global pool via etcd.
// The arena is leased exclusively to this node.
func (a *Allocator) AcquireArena(ctx context.Context) (*Arena, error) {
	// Checked before an ID is drawn as well as after, because a device too
	// small for a single arena would otherwise burn a counter value on every
	// attempt.
	if size := a.DeviceSize(); size > 0 && size < ArenaSizeBytes {
		return nil, fmt.Errorf("acquire arena: the device holds %d bytes, less than one arena (%w)",
			size, ErrNoSpace)
	}

	arenaID, reused, err := a.allocateArenaID(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire arena: %w", err)
	}

	diskStart := arenaID * ArenaSizeBytes
	diskEnd := diskStart + ArenaSizeBytes

	// The ID is not handed back on refusal.  A device with no room left is
	// stuck for the whole cluster until it grows, and fsck reports the unowned
	// ID as an orphaned arena — a cheaper outcome than a return path that has
	// to be correct under a concurrent claim of the same ID.
	if size := a.DeviceSize(); size > 0 && diskEnd > size {
		return nil, fmt.Errorf("acquire arena %d: ends at %d, past the %d byte device (%w)",
			arenaID, diskEnd, size, ErrNoSpace)
	}

	// Record ownership before using the arena.  Without this the node has no
	// durable claim on the range it is about to write into, and a restart
	// cannot tell which arenas were its own.
	//
	// Conditioned on the key not already existing: a record that is already
	// there means the arena ID came back from the counter or the free pool
	// while someone still holds it, and writing into it anyway is the silent
	// two-writers-one-range corruption the arena scheme exists to prevent.
	// Failing the write that triggered this is the safe outcome.
	if err := a.recordOwnership(ctx, arenaID); err != nil {
		return nil, err
	}

	free := &Arena{
		ID:        arenaID,
		DiskStart: diskStart,
		DiskEnd:   diskEnd,
		bitmap:    make([]uint64, BlocksPerArena/64),
	}

	// A recycled arena is not an empty one.  A node that departs or is fenced
	// returns its arena with its files' extents still live in it, so a fresh
	// all-zero bitmap here would let Allocate hand out blocks that another
	// inode's data already occupies — the same silent overwrite the arena
	// scheme exists to prevent, just deferred to the next owner.  Only a
	// never-issued arena is safe to assume empty.
	if reused {
		extents, err := a.store.AllExtents(ctx)
		if err != nil {
			return nil, fmt.Errorf("acquire arena %d: read live extents: %w", arenaID, err)
		}
		markLiveExtents(free, extents)
	}

	a.mu.Lock()
	a.arenas = append(a.arenas, free)
	a.mu.Unlock()

	return free, nil
}

// recordOwnership durably claims arenaID for this node, refusing to overwrite
// an existing record for the same arena.
func (a *Allocator) recordOwnership(ctx context.Context, arenaID uint64) error {
	key := metadata.ArenaOwnerKey(a.nodeID, arenaID)
	ok, err := a.store.Txn(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "=", 0)},
		[]clientv3.Op{clientv3.OpPut(key, string(metadata.EncodeUint64(arenaID)))}, nil)
	if err != nil {
		return fmt.Errorf("acquire arena %d: record ownership: %w", arenaID, err)
	}
	if !ok {
		return fmt.Errorf("acquire arena %d: already owned", arenaID)
	}
	return nil
}

// allocateArenaID obtains an arena ID, preferring one already returned to the
// global free pool over extending the device with a brand-new arena, and
// reports which of the two it was.
//
// Preferring the pool is what makes reclamation mean anything: the arena
// counter only ever grows, so without a claim here a freed arena's space is
// never handed out again and the free_arena: keys accumulate as a record of
// space nobody uses.  A failed claim is not fatal — falling through to a new
// arena costs device space but always works, whereas failing the write that
// triggered this would surface an etcd hiccup as an I/O error.
func (a *Allocator) allocateArenaID(ctx context.Context) (id uint64, reused bool, err error) {
	if id, ok, cerr := a.store.ClaimFreeArena(ctx); cerr == nil && ok {
		return id, true, nil
	}
	id, err = a.store.NextCounter(ctx, metadata.PrefixArenaLog, 0)
	return id, false, err
}

// Run is one contiguous span of device bytes handed out by the allocator.
// Length is always a whole number of blocks; the caller trims the final run's
// extent to the length of the data it actually holds.
type Run struct {
	DiskOff uint64
	Length  uint64
}

// Allocate reserves size bytes and returns the runs backing them, in order.
//
// A file is a list of extents, so its data does not have to be contiguous on
// the device.  Allocation therefore fills from as many runs as it takes rather
// than failing when no single run is large enough — free space that is merely
// fragmented is still usable space, and refusing it would push the write onto a
// fresh arena and grow the device for room that was already there.
//
// It fails only when the arenas this node holds genuinely cannot cover the
// request, which is the caller's signal to acquire another arena.
func (a *Allocator) Allocate(size uint64) ([]Run, error) {
	want := (size + BlockSize - 1) / BlockSize
	if want == 0 {
		return nil, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	var runs []Run
	got := uint64(0)
	for _, ar := range a.arenas {
		for got < want {
			start, length := ar.findRun(want - got)
			if length == 0 {
				break
			}
			for i := uint64(0); i < length; i++ {
				ar.markAllocated(start + i)
			}
			runs = append(runs, Run{
				DiskOff: ar.DiskStart + start*BlockSize,
				Length:  length * BlockSize,
			})
			got += length
		}
		if got == want {
			return runs, nil
		}
	}

	// Undo the partial reservation: leaving it marked would leak every block
	// taken on the way to discovering the request could not be met.
	for _, r := range runs {
		a.freeLocked(r.DiskOff, r.Length)
	}
	return nil, fmt.Errorf("no arena has %d free blocks", want)
}

// Free marks a range of blocks as free.  A range outside every arena this node
// owns is ignored: the free list is per-process, so only the owner can return
// its own space.
func (a *Allocator) Free(diskOff uint64, size uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.freeLocked(diskOff, size)
}

func (a *Allocator) freeLocked(diskOff uint64, size uint64) {
	blocks := (size + BlockSize - 1) / BlockSize
	for _, ar := range a.arenas {
		if diskOff >= ar.DiskStart && diskOff < ar.DiskEnd {
			start := (diskOff - ar.DiskStart) / BlockSize
			for i := uint64(0); i < blocks && start+i < BlocksPerArena; i++ {
				// Freeing a block that is already free means two callers
				// believe they own it, and the next allocation will hand a live
				// range to a second writer.  The write path, the scrubber and
				// the failed-allocation undo all call this, so the double free
				// is worth naming where it happens rather than at the
				// corruption it causes later.
				if ar.isFree(start + i) {
					a.doubleFrees++
					continue
				}
				ar.markFree(start + i)
			}
			// Reuse what was just returned before sweeping forward again.
			if start < ar.hint {
				ar.hint = start
			}
			return
		}
	}
}

// DoubleFrees returns how many already-free blocks this allocator has been
// asked to free.  Anything above zero is a bug in a caller.
func (a *Allocator) DoubleFrees() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.doubleFrees
}

// Owns reports whether diskOff falls inside an arena this node currently holds.
//
// The scrubber asks before reclaiming an orphaned extent: deleting the extent
// record of a range owned by a *live peer* would strip the only reference that
// peer's in-memory bitmap is rebuilt from, so those blocks would stay marked
// allocated there until it restarts.
func (a *Allocator) Owns(diskOff uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, ar := range a.arenas {
		if diskOff >= ar.DiskStart && diskOff < ar.DiskEnd {
			return true
		}
	}
	return false
}

// LiveRatio returns the fraction of allocated blocks across all arenas.
func (a *Allocator) LiveRatio() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	total := uint64(0)
	used := uint64(0)
	for _, free := range a.arenas {
		total += BlocksPerArena
		used += free.countAllocated()
	}
	if total == 0 {
		// Nothing held is nothing used.  Reporting 1.0 made statfs answer that
		// the filesystem was full before the first write took an arena.
		return 0.0
	}
	return float64(used) / float64(total)
}

// reportUtilization publishes this node's arena occupancy.  Sampled from the
// reaper's tick rather than updated at every allocation: both values are
// derived by walking the arena bitmaps under the allocator lock, which is on
// the write path, and a gauge one tick stale is worth more than contention.
func (a *Allocator) reportUtilization() {
	metrics.ArenaUtilization.Set(a.LiveRatio())
	metrics.ArenasOwned.Set(float64(a.ArenaCount()))
}

// NodeID returns the owning node.
func (a *Allocator) NodeID() string { return a.nodeID }

// Reconstruct rebuilds the arena free-list from existing extents in etcd.
// Called at startup after reconnecting to etcd.
func (a *Allocator) Reconstruct(ctx context.Context) error {
	arenaIDs, err := a.existingArenaIDs(ctx)
	if err != nil {
		return err
	}

	a.mu.Lock()
	a.arenas = nil
	a.mu.Unlock()

	for _, id := range arenaIDs {
		free := &Arena{
			ID:        id,
			DiskStart: id * ArenaSizeBytes,
			DiskEnd:   (id + 1) * ArenaSizeBytes,
			bitmap:    make([]uint64, BlocksPerArena/64),
		}
		a.mu.Lock()
		a.arenas = append(a.arenas, free)
		a.mu.Unlock()
	}

	extents, err := a.store.AllExtents(ctx)
	if err != nil {
		return err
	}
	for _, free := range a.arenas {
		markLiveExtents(free, extents)
	}
	return nil
}

// markLiveExtents marks every block covered by an extent inside ar as
// allocated, ignoring extents that live in another arena.
func markLiveExtents(ar *Arena, extents []metadata.Extent) {
	for _, ext := range extents {
		if !ext.WithinDisk(ar.DiskStart, ar.DiskEnd) {
			continue
		}
		start := (ext.DiskOff - ar.DiskStart) / BlockSize
		blocks := (ext.Length + BlockSize - 1) / BlockSize
		for i := uint64(0); i < blocks && start+i < BlocksPerArena; i++ {
			ar.markAllocated(start + i)
		}
	}
}

// existingArenaIDs returns every arena this node owns, read from its own
// arena:<node_id>/ ownership records.
//
// Only this node's records are read, never the whole arena: prefix.  Scanning
// the prefix would pull every *other* live node's arenas into this node's
// free-list, and Allocate would then hand out disk offsets inside a range
// another node is actively writing.  Both nodes would write different data to
// the same block and both extent commits would succeed, because neither node
// is fenced and the generation guard has nothing to object to — silent
// corruption, detectable only after the fact by the scrubber's
// CheckExtentCollisions.
func (a *Allocator) existingArenaIDs(ctx context.Context) ([]uint64, error) {
	kvs, err := a.store.GetPrefix(ctx, metadata.ArenaNodePrefix(a.nodeID))
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(kvs))
	for _, kv := range kvs {
		// A record is exactly the 8-byte big-endian arena ID.  Anything else is
		// malformed, and adopting it would mean allocating into an arena this
		// node may not own — skip it instead, which only costs a fresh arena.
		// Note arena ID 0 is a valid ID, so a present record must be
		// distinguished by length, not by a zero test.
		if len(kv.Value) != 8 {
			continue
		}
		ids = append(ids, metadata.DecodeUint64(kv.Value))
	}
	return ids, nil
}

// ArenaCount returns the number of arenas managed by this allocator.
func (a *Allocator) ArenaCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.arenas)
}

// ReapEmptyArenas periodically returns emptied arenas to the global free pool.
// Blocks until ctx is cancelled.
func (a *Allocator) ReapEmptyArenas(ctx context.Context, interval time.Duration) {
	a.reportUtilization()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = a.ReleaseEmptyArenas(ctx)
			a.reportUtilization()
		}
	}
}

// ReleaseEmptyArenas returns every arena this node has emptied to the global
// free pool, reporting the arenas actually released.
//
// Deletes and truncates free blocks inside an arena, but the arena itself stays
// this node's until it is handed back, and only departure and fencing ever did
// that.  A long-lived node therefore accumulated arenas it no longer used, and
// that space was reserved to it until the process exited — reclaimable in
// principle, unusable by any peer in practice.
//
// The arena is detached from the local free list *before* the etcd release, so
// no allocation can land in a range that is on its way to another node, and
// reattached if the release does not go through.  The reverse order would open
// exactly the two-writers-one-range window the arena scheme exists to close.
func (a *Allocator) ReleaseEmptyArenas(ctx context.Context) ([]uint64, error) {
	a.mu.Lock()
	var empty []*Arena
	kept := a.arenas[:0]
	for _, ar := range a.arenas {
		if ar.isEmpty() {
			empty = append(empty, ar)
			continue
		}
		kept = append(kept, ar)
	}
	a.arenas = kept
	a.mu.Unlock()

	released := make([]uint64, 0, len(empty))
	for _, ar := range empty {
		ok, err := a.store.ReleaseArenaID(ctx, a.nodeID, ar.ID)
		if err != nil || !ok {
			a.mu.Lock()
			a.arenas = append(a.arenas, ar)
			a.mu.Unlock()
			if err != nil {
				return released, fmt.Errorf("release empty arena %d: %w", ar.ID, err)
			}
			continue
		}
		released = append(released, ar.ID)
	}
	return released, nil
}

// ---- bitmap operations ----

// findRun returns the first free run of blocks, at most max long.  A length of
// 0 means the arena has no free block left.
//
// ponytail: a linear bit scan, resumed from the previous allocation's end and
// wrapping once, so a nearly-full arena costs at most one sweep per call.
// Skipping fully-allocated words keeps that tolerable; a free-list is the
// upgrade if allocation ever shows up in a profile.
func (ar *Arena) findRun(max uint64) (start, length uint64) {
	// Two passes: from the hint to the end, then from the start back to it, so
	// a wrap costs no more than the single sweep this replaced.
	if start, length = ar.findRunIn(ar.hint, BlocksPerArena, max); length > 0 {
		ar.hint = start + length
		return start, length
	}
	if start, length = ar.findRunIn(0, ar.hint, max); length > 0 {
		ar.hint = start + length
		return start, length
	}
	return 0, 0
}

// findRunIn searches [from, to) for a free run of at most max blocks.
func (ar *Arena) findRunIn(from, to, max uint64) (start, length uint64) {
	for i := from; i < to; {
		if ar.bitmap[i/64] == ^uint64(0) {
			i = (i/64 + 1) * 64 // whole word allocated
			continue
		}
		if !ar.isFree(i) {
			i++
			continue
		}
		start = i
		for length < max && i < to && ar.isFree(i) {
			length++
			i++
		}
		return start, length
	}
	return 0, 0
}

// isEmpty reports whether the arena holds no allocated block.
func (ar *Arena) isEmpty() bool {
	for _, word := range ar.bitmap {
		if word != 0 {
			return false
		}
	}
	return true
}

func (ar *Arena) isFree(block uint64) bool {
	idx := block / 64
	bit := block % 64
	return (ar.bitmap[idx] & (1 << bit)) == 0
}

func (ar *Arena) markAllocated(block uint64) {
	idx := block / 64
	bit := block % 64
	ar.bitmap[idx] |= (1 << bit)
}

func (ar *Arena) markFree(block uint64) {
	idx := block / 64
	bit := block % 64
	ar.bitmap[idx] &^= (1 << bit)
}

func (ar *Arena) countAllocated() uint64 {
	var count uint64
	for _, word := range ar.bitmap {
		count += uint64(bits.OnesCount64(word))
	}
	return count
}
