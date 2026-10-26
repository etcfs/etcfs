package metadata

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Extent maps a logical byte range of an inode onto a range of the shared
// block device.
//
// Extents live under one key per extent, `extent:<ino>/<chunk>`, with the
// value encoded as
// `<logical_off>,<disk_off>,<length>,<generation>,<sequence>,<node>`.  The
// generation is the writer's fencing generation at commit time and node is the
// writer itself; the scrubber needs both, since a generation is per-node and
// means nothing without knowing whose it is.
//
// This file is the single owner of that encoding.  Nothing outside it should
// format or parse an extent key or value by hand.
type Extent struct {
	// Key is the etcd key the extent was read from.  Empty for an extent
	// that has not been stored yet; never part of the encoded value.
	Key string

	// Chunk is the extent's chunk number, taken from Key.  It makes the key
	// unique within an inode and nothing more; it does not order writes.
	Chunk uint64

	// ModRevision is the etcd revision the record was last written at, or 0
	// for an extent that has not been stored yet.  A caller that rewrites an
	// extent it read earlier compares against it, so that a record which
	// changed in between fails the transaction instead of being overwritten
	// from a stale view — see ipc.Service.planReclaim.
	ModRevision int64

	// Seq orders writes to the same logical bytes: of two extents covering a
	// range, the one with the higher Seq is the later write, and the one a read
	// must resolve to.
	//
	// It is stored in the value rather than derived from the key because
	// splitting an extent has to produce two records that are both *as old as
	// their parent*.  A key-derived order cannot express that — a second key
	// would have to claim the piece is newer than the extent it was cut from,
	// and it would then win over a genuinely newer extent overlapping it.
	//
	Seq uint64

	// Node is the node ID that wrote the extent.  Without it a stamped
	// generation cannot be compared against anything: generations are per-node,
	// so the scrubber has no way to tell an extent written at generation 3 by a
	// node now at 3 from one written by a node now at 7.
	Node string

	LogOff  uint64
	DiskOff uint64
	Length  uint64
	Gen     uint64
}

// ExtentKey returns the etcd key for one extent of an inode.
func ExtentKey(ino, chunk uint64) string {
	return fmt.Sprintf("%s%d/%d", PrefixExtent, ino, chunk)
}

// ExtentPrefix returns the etcd key prefix covering all extents of an inode.
func ExtentPrefix(ino uint64) string {
	return fmt.Sprintf("%s%d/", PrefixExtent, ino)
}

// ParseExtentKey splits an extent key into its inode and chunk numbers.
// Returns ok=false for a key that is not a well-formed extent key.
func ParseExtentKey(key string) (ino, chunk uint64, ok bool) {
	rest, found := strings.CutPrefix(key, PrefixExtent)
	if !found {
		return 0, 0, false
	}
	inoStr, chunkStr, found := strings.Cut(rest, "/")
	if !found {
		return 0, 0, false
	}
	ino, err := strconv.ParseUint(inoStr, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	chunk, err = strconv.ParseUint(chunkStr, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return ino, chunk, true
}

// InoFromExtentKey returns just the inode number of an extent key, or 0 if
// the key is malformed.  Convenience for callers that don't need the chunk.
func InoFromExtentKey(key string) uint64 {
	ino, _, _ := ParseExtentKey(key)
	return ino
}

// Encode renders the extent in its stored form.  Key is not part of the value.
func (e Extent) Encode() string {
	return fmt.Sprintf("%d,%d,%d,%d,%d,%s", e.LogOff, e.DiskOff, e.Length, e.Gen, e.Seq, e.Node)
}

// Ino returns the inode the extent belongs to, derived from its key.
func (e Extent) Ino() uint64 { return InoFromExtentKey(e.Key) }

// End returns the exclusive logical end offset of the extent.
func (e Extent) End() uint64 { return e.LogOff + e.Length }

// WithinDisk reports whether the extent lies entirely inside the device byte
// range [start, end).  Used to attribute an extent to an arena.
func (e Extent) WithinDisk(start, end uint64) bool {
	return e.DiskOff >= start && e.DiskOff+e.Length <= end
}

// DecodeExtent parses a stored extent key/value pair.  Returns ok=false unless
// the value is five integers followed by the writer's node ID, so a corrupt
// record is skipped rather than silently read as a zero-length extent at disk
// offset 0.
//
// The node ID is taken as the whole remainder, so one containing a comma still
// round-trips.
func DecodeExtent(key string, value []byte) (Extent, bool) {
	parts := strings.SplitN(string(value), ",", 6)
	if len(parts) != 6 {
		return Extent{}, false
	}
	var fields [5]uint64
	for i, p := range parts[:5] {
		v, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			return Extent{}, false
		}
		fields[i] = v
	}
	_, chunk, _ := ParseExtentKey(key)
	return Extent{
		Key: key, Chunk: chunk, Seq: fields[4], LogOff: fields[0], DiskOff: fields[1],
		Length: fields[2], Gen: fields[3], Node: parts[5],
	}, true
}

// DecodeExtents decodes a batch of extent key/values, skipping malformed ones,
// and returns them ordered by logical offset, most recent write first within
// one offset.
//
// The offset ordering matters because etcd returns keys in lexicographic order,
// so an inode with more than ten extents comes back as chunk 0, 1, 10, 11, 2, …
// — reading that back in key order reassembles the file wrong.
//
// The sequence ordering matters because a write is not an in-place update: it
// allocates fresh blocks and appends an extent, so overwriting a range leaves
// two extents covering it.  A reader takes the first extent that covers the
// offset it wants, so the descending sequence order is what makes that the newer
// one.  Sorting on offset alone left the choice to sort.Slice, which is not
// stable — the same file could read back differently from one call to the next.
//
// Chunk breaks the remaining tie.  Two extents can share a sequence — the two
// halves of a split both keep their parent's — but only ever over disjoint
// logical ranges, so the tiebreak is there for determinism, not for meaning.
func DecodeExtents(kvs []*mvccpb.KeyValue) []Extent {
	extents := make([]Extent, 0, len(kvs))
	for _, kv := range kvs {
		if e, ok := DecodeExtent(string(kv.Key), kv.Value); ok {
			e.ModRevision = kv.ModRevision
			extents = append(extents, e)
		}
	}
	SortExtents(extents)
	return extents
}

// SortExtents puts an extent list in resolution order: by logical offset, and
// where two cover the same one, newest first.  Walking it forward and taking
// the first extent that reaches a cursor therefore resolves an overwrite to the
// later write.
//
// Exported so that a list maintained incrementally — from a transaction's own
// operations rather than from a fresh read — is ordered by the same rule as one
// that came out of DecodeExtents. Two orderings that could drift apart would be
// two different answers to what a byte of the file contains.
func SortExtents(extents []Extent) {
	slices.SortFunc(extents, extentOrder)
}

// InsertExtent puts one extent into an already-ordered list at its place, and
// returns the list.  For a list maintained incrementally this is what replaces
// appending and sorting again: a write appends an extent whose logical offset
// falls in the middle of the file, so the append alone leaves the list unsorted
// every single time.
func InsertExtent(extents []Extent, e Extent) []Extent {
	i := sort.Search(len(extents), func(i int) bool { return extentOrder(extents[i], e) >= 0 })
	extents = append(extents, Extent{})
	copy(extents[i+1:], extents[i:])
	extents[i] = e
	return extents
}

func extentOrder(a, b Extent) int {
	if a.LogOff != b.LogOff {
		return cmp.Compare(a.LogOff, b.LogOff)
	}
	if a.Seq != b.Seq {
		return cmp.Compare(b.Seq, a.Seq)
	}
	return cmp.Compare(b.Chunk, a.Chunk)
}

// Supersedes reports whether e covers every logical byte of other and was
// written later, which makes other's disk range dead.
func (e Extent) Supersedes(other Extent) bool {
	return e.Seq > other.Seq &&
		e.LogOff <= other.LogOff && e.End() >= other.End()
}

// BlockSize is the granularity at which device space is allocated and freed.
// Extents describe byte ranges, but the space behind them is only ever handed
// out and returned in whole blocks, so any range an extent gives up has to be
// snapped to these boundaries before it can be reused.
const BlockSize = 4096

// SplitAround returns the parts of old that a write covering [start, end)
// leaves readable.  A nil head and a nil tail mean the write buries old
// entirely.
//
// Both survivors keep a block-aligned device offset, because a read has to be
// able to reach them under O_DIRECT, where the offset must be sector-aligned.
// For the head that is free — it starts where old did.  The tail is what
// constrains the split: its device offset advances by however many bytes the
// write covered, so that distance is rounded *down* to a block boundary.
//
// Rounding down means the tail keeps a few bytes the write also covers.  That
// is the safe direction and costs at most one block: the overlap resolves to
// the newer write, because the tail keeps the original extent's chunk number
// and the write's is higher.  Rounding up would be a data loss — the bytes
// between the write's end and the tail's start would be described by no extent
// at all and would read back as zeroes.
func SplitAround(old Extent, start, end uint64) (head, tail *Extent) {
	if start > old.LogOff {
		head = &Extent{
			LogOff:  old.LogOff,
			DiskOff: old.DiskOff,
			Length:  start - old.LogOff,
			Gen:     old.Gen,
			Seq:     old.Seq,
			Node:    old.Node,
		}
	}
	if end < old.End() {
		skip := (end - old.LogOff) / BlockSize * BlockSize
		tail = &Extent{
			LogOff:  old.LogOff + skip,
			DiskOff: old.DiskOff + skip,
			Length:  old.Length - skip,
			Gen:     old.Gen,
			Seq:     old.Seq,
			Node:    old.Node,
		}
	}
	return head, tail
}

// CoveredBlocks returns the device range old gives up once it has been split
// into head and tail, as a whole number of blocks.  A zero length means the
// split freed nothing.
//
// The head's own last block is kept even when it is only partly used: the live
// bytes of the head share it, and blocks are the unit of reuse.  The tail's
// first block is likewise the boundary on the other side.
func CoveredBlocks(old Extent, head, tail *Extent) (diskOff, length uint64) {
	first := uint64(0)
	if head != nil {
		first = blocksFor(head.Length)
	}
	last := blocksFor(old.Length)
	if tail != nil {
		last = (tail.DiskOff - old.DiskOff) / BlockSize
	}
	if last <= first {
		return 0, 0
	}
	return old.DiskOff + first*BlockSize, (last - first) * BlockSize
}

// blocksFor returns the number of whole blocks needed to hold n bytes.
func blocksFor(n uint64) uint64 { return (n + BlockSize - 1) / BlockSize }

// GetExtents returns all extents of an inode, ordered by logical offset.
//
// Read options are passed through: the write path asks for a serializable read
// (see handleWriteBlock), which answers from whichever member the client is
// already connected to instead of costing a leader round trip.
func (s *Store) GetExtents(ctx context.Context, ino uint64, opts ...clientv3.OpOption) ([]Extent, error) {
	kvs, err := s.getPrefix(ctx, ExtentPrefix(ino), opts...)
	if err != nil {
		return nil, fmt.Errorf("get extents ino %d: %w", ino, err)
	}
	return DecodeExtents(kvs), nil
}

// GetInodeAndExtents reads an inode record together with its extent list, as
// one serializable transaction.  A nil record means the inode is gone.
//
// The read path needs both before it can answer: the record to clamp the
// request to the file's size, the extents to resolve the range.  Read
// separately they cost two round trips and can straddle a revision; read this
// way they cost one and are a consistent snapshot.
//
// Serializable because the shared lock, not the read's placement in etcd's
// linear order, is what keeps a concurrent writer off the range — and a read
// that could not take the lock is already best effort.
//
// The lock orders the commits, though, not what the answering member has
// applied: a follower outside the quorum, or one that has not yet applied the
// acquisition, still answers, with an extent list from before the previous
// holder's writes. So a snapshot older than any revision this store has seen
// committed — the lock acquisition included — is read again linearizably.
func (s *Store) GetInodeAndExtents(ctx context.Context, ino uint64) (*InodeRecord, []Extent, error) {
	resp, err := s.readTxn(ctx,
		clientv3.OpGet(InodeKey(ino), clientv3.WithSerializable()),
		clientv3.OpGet(ExtentPrefix(ino), clientv3.WithPrefix(), clientv3.WithSerializable()),
	)
	if err == nil && resp.Header.Revision < s.seenRev.Load() {
		resp, err = s.client.Txn(ctx).Then(
			clientv3.OpGet(InodeKey(ino)),
			clientv3.OpGet(ExtentPrefix(ino), clientv3.WithPrefix()),
		).Commit()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("get inode and extents ino %d: %w", ino, err)
	}
	if len(resp.Responses) != 2 {
		return nil, nil, fmt.Errorf("get inode and extents ino %d: %d responses", ino, len(resp.Responses))
	}
	inodeKvs := resp.Responses[0].GetResponseRange().Kvs
	if len(inodeKvs) == 0 {
		return nil, nil, nil
	}
	return DecodeInode(inodeKvs[0].Value), DecodeExtents(resp.Responses[1].GetResponseRange().Kvs), nil
}

// AllExtents returns every extent in the filesystem, ordered by logical
// offset within no particular inode.  Used by the whole-filesystem scanners
// (fsck, scrubber, arena reconstruction).
func (s *Store) AllExtents(ctx context.Context) ([]Extent, error) {
	kvs, err := s.GetPrefix(ctx, PrefixExtent)
	if err != nil {
		return nil, fmt.Errorf("get all extents: %w", err)
	}
	return DecodeExtents(kvs), nil
}

// NextExtentChunk returns the chunk number a new extent for this inode should
// be written under.
//
// It is one past the highest chunk currently stored, not the number of stored
// extents: truncate deletes extents from the middle and the end, and deriving
// the next chunk from a count would hand back a chunk number that is still
// live and silently overwrite it.
func (s *Store) NextExtentChunk(ctx context.Context, ino uint64) (uint64, error) {
	kvs, err := s.GetPrefix(ctx, ExtentPrefix(ino))
	if err != nil {
		return 0, fmt.Errorf("next extent chunk ino %d: %w", ino, err)
	}
	next := uint64(0)
	for _, kv := range kvs {
		if _, chunk, ok := ParseExtentKey(string(kv.Key)); ok && chunk >= next {
			next = chunk + 1
		}
	}
	return next, nil
}

// AppendExtent stores a new extent for an inode under the next free chunk.
func (s *Store) AppendExtent(ctx context.Context, ino, logicalOff, diskOff, length, generation uint64) error {
	chunk, err := s.NextExtentChunk(ctx, ino)
	if err != nil {
		return err
	}
	ext := Extent{LogOff: logicalOff, DiskOff: diskOff, Length: length, Gen: generation, Seq: chunk, Node: s.nodeID}
	if _, err := s.Put(ctx, ExtentKey(ino, chunk), []byte(ext.Encode())); err != nil {
		return fmt.Errorf("append extent ino %d: %w", ino, err)
	}
	return nil
}

// DataRanges resolves an inode's extents into the logical byte ranges that
// hold data, merged, clamped to size, and in ascending order. The gaps between
// them — and everything from the last range to size — are holes.
//
// It walks the extent list exactly as a read does, and deliberately so: the
// list is sorted by logical offset with the newest extent first where two share
// one, so taking the first extent that reaches the cursor resolves an overwrite
// to the later write. Any other traversal here would let SEEK_HOLE and SEEK_DATA
// disagree with what a read of the same offset actually returns, which is a
// worse failure than not supporting them at all — a backup tool would skip a
// range that has data in it.
//
// Extents must be ordered as GetExtents returns them.
func DataRanges(extents []Extent, size uint64) [][2]uint64 {
	var ranges [][2]uint64
	pos := uint64(0)
	for _, ext := range extents {
		if pos >= size {
			break
		}
		if ext.End() <= pos {
			continue // entirely behind the cursor, or buried by a newer extent
		}
		start := max(ext.LogOff, pos)
		end := min(ext.End(), size)
		if start >= end {
			continue
		}
		// Merge with the previous range when they meet, so a file written in
		// many small appends reports one run rather than one range per write.
		if n := len(ranges); n > 0 && ranges[n-1][1] >= start {
			ranges[n-1][1] = max(ranges[n-1][1], end)
		} else {
			ranges = append(ranges, [2]uint64{start, end})
		}
		pos = end
	}
	return ranges
}

// SeekData returns the offset of the first byte of data at or after offset.
// ok is false when there is none, which lseek(2) reports as ENXIO.
func SeekData(extents []Extent, size, offset uint64) (uint64, bool) {
	if offset >= size {
		return 0, false
	}
	for _, r := range DataRanges(extents, size) {
		if r[1] > offset {
			return max(r[0], offset), true
		}
	}
	return 0, false
}

// SeekHole returns the offset of the first byte of a hole at or after offset.
// ok is false only when offset is at or past the end of the file.
//
// A file always ends in a hole as far as lseek(2) is concerned, so an offset
// inside the file always has an answer: at worst, size itself.
func SeekHole(extents []Extent, size, offset uint64) (uint64, bool) {
	if offset >= size {
		return 0, false
	}
	for _, r := range DataRanges(extents, size) {
		if offset < r[0] {
			return offset, true // already in the hole before this range
		}
		if offset < r[1] {
			offset = r[1] // inside data: the hole starts where this run ends
		}
	}
	return offset, true
}
