package ipc

import (
	"fmt"
	"testing"

	"github.com/etcfs/etcfs/pkg/arena"
	"github.com/etcfs/etcfs/pkg/metadata"
)

// What one 4 KiB write costs as a function of how many extents the inode
// already has.
//
// A file under random overwrite gains an extent per write, so a benchmark that
// only ever sees a short list measures the wrong thing entirely: the interval
// flush publishes in batches, the lock's cache answers the metadata lookup, and
// what is left on the write path is proportional to the list rather than to the
// payload.  The counts below bracket what a 30 s fio run produces.
var benchExtentCounts = []int{1_000, 10_000, 50_000}

// benchExtents builds a contiguous extent list of n 4 KiB extents, which is the
// shape sequential writes leave behind and random ones then overwrite.
func benchExtents(n int) []metadata.Extent {
	out := make([]metadata.Extent, n)
	for i := range out {
		off := uint64(i) * arena.BlockSize
		out[i] = metadata.Extent{
			Key:     metadata.ExtentKey(1, uint64(i)),
			Chunk:   uint64(i),
			LogOff:  off,
			DiskOff: off,
			Length:  arena.BlockSize,
			Seq:     uint64(i),
		}
	}
	return out
}

func benchWriteOp(existing []metadata.Extent) *writeOp {
	// A middle-of-the-file 4 KiB overwrite: it buries exactly one extent, so
	// anything the benchmark shows beyond a constant is the cost of finding it.
	off := uint64(len(existing)/2) * arena.BlockSize
	w := &writeOp{
		s:        &Service{store: &metadata.Store{}},
		ino:      1,
		offset:   off,
		dataLen:  arena.BlockSize,
		padded:   arena.BlockSize,
		rec:      &metadata.InodeRecord{Ino: 1, Mode: 0100644, Nlink: 1, Size: uint64(len(existing)) * arena.BlockSize},
		existing: existing,
		runs:     []arena.Run{{DiskOff: 1 << 40, Length: arena.BlockSize}},
	}
	w.countFrom()
	return w
}

func BenchmarkWriteProposal(b *testing.B) {
	for _, n := range benchExtentCounts {
		b.Run(fmt.Sprintf("extents=%d", n), func(b *testing.B) {
			w := benchWriteOp(benchExtents(n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w.proposal()
			}
		})
	}
}

// The cost of folding one write's transaction back into the cached snapshot,
// which is what lets the next operation on the file skip etcd.
func BenchmarkReplayApply(b *testing.B) {
	for _, n := range benchExtentCounts {
		b.Run(fmt.Sprintf("extents=%d", n), func(b *testing.B) {
			w := benchWriteOp(benchExtents(n))
			_, ops := w.proposal()
			m := &inodeMeta{rec: w.rec, extents: w.existing}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				replay := replayTxn(1, w.rec, ops)
				if replay == nil {
					b.Fatal("the transaction was not replayable into the cache")
				}
				m = replay.apply(m, 0)
			}
		})
	}
}
