//go:build integration
// +build integration

package ipc

import (
	"context"
	"sync"
	"testing"

	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/test/etcdtest"
)

// Inode numbers are handed out from a block reserved in etcd, so the property
// that matters is the one the counter used to give for free: no number is ever
// handed out twice, on one node or across two.

func TestIntegration_InodeBlockNumbersAreUnique(t *testing.T) {
	store := metadata.NewStore(etcdtest.Client(t), "node-a")
	blocks := newInodeBlocks(store)
	ctx := context.Background()

	// More than one block's worth, so the refill path is exercised rather than
	// only the first reservation.
	const n = inodeBlockSize + 17
	seen := make(map[uint64]bool, n)
	for i := 0; i < n; i++ {
		ino, err := blocks.reserve(ctx)
		if err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
		if ino < metadata.FirstUsableIno {
			t.Fatalf("handed out reserved inode %d", ino)
		}
		if seen[ino] {
			t.Fatalf("handed out inode %d twice", ino)
		}
		seen[ino] = true
	}
}

func TestIntegration_InodeBlocksDoNotOverlapBetweenNodes(t *testing.T) {
	cli := etcdtest.Client(t)
	a := newInodeBlocks(metadata.NewStore(cli, "node-a"))
	b := newInodeBlocks(metadata.NewStore(cli, "node-b"))
	ctx := context.Background()

	// Interleaved, and past the first block on each side: two nodes drawing
	// from one counter must never land on the same number even once both are
	// refilling.
	seen := make(map[uint64]bool)
	for i := 0; i < inodeBlockSize+5; i++ {
		for _, blocks := range []*inodeBlocks{a, b} {
			ino, err := blocks.reserve(ctx)
			if err != nil {
				t.Fatalf("reserve: %v", err)
			}
			if seen[ino] {
				t.Fatalf("two nodes were handed inode %d", ino)
			}
			seen[ino] = true
		}
	}
}

func TestIntegration_InodeBlocksAreConcurrencySafe(t *testing.T) {
	store := metadata.NewStore(etcdtest.Client(t), "node-a")
	blocks := newInodeBlocks(store)
	ctx := context.Background()

	const goroutines, each = 8, 300
	var mu sync.Mutex
	seen := make(map[uint64]bool, goroutines*each)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				ino, err := blocks.reserve(ctx)
				if err != nil {
					t.Errorf("reserve: %v", err)
					return
				}
				mu.Lock()
				if seen[ino] {
					t.Errorf("handed out inode %d twice", ino)
				}
				seen[ino] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
}
