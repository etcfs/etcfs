package ipc

import (
	"context"
	"sync"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// Inode number allocation.
//
// A number used to cost a read and a Raft commit of its own, taken before the
// transaction that created the file — so every create waited on Raft twice for
// what is, in the end, a counter.  The counter is now reserved a block at a
// time and handed out from memory, which leaves one commit on the create path:
// the one that publishes the file.
//
// The cost is that a node which stops holding a partly used block strands the
// rest of it.  Inode numbers are 64-bit and nothing reuses them anyway, so a
// block per daemon start is not a resource anyone can run out of; the counter
// simply counts numbers handed out rather than files alive, which is exactly
// what statfs already reports it as.

// inodeBlockSize is how many numbers one reservation covers.
//
// Large enough that an unpacking tar pays the commit once per thousand files
// rather than once per file, small enough that the numbers a restart strands
// stay a rounding error against the 64-bit space.
const inodeBlockSize = 1024

// inodeBlocks hands out inode numbers from a reserved block, taking a new
// block from etcd when the current one runs out.
type inodeBlocks struct {
	store *metadata.Store

	mu   sync.Mutex
	next uint64 // the next number to hand out
	end  uint64 // one past the last number this block covers
}

func newInodeBlocks(store *metadata.Store) *inodeBlocks {
	return &inodeBlocks{store: store}
}

// next reserves one inode number.
//
// The etcd reservation runs with the mutex held, so a burst of concurrent
// creates that all find the block empty takes one block between them rather
// than one each.  It is a single transaction against a counter no operation
// waits on, so serialising it costs less than the blocks it would otherwise
// strand.
func (a *inodeBlocks) reserve(ctx context.Context) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.next >= a.end {
		first, err := a.store.ReserveCounter(ctx, metadata.KeyInodeAllocCounter,
			metadata.FirstUsableIno, inodeBlockSize)
		if err != nil {
			return 0, err
		}
		a.next, a.end = first, first+inodeBlockSize
	}

	ino := a.next
	a.next++
	return ino, nil
}
