//go:build integration
// +build integration

// Integration tests for statfs. See datapath_integration_test.go for what a run
// needs.
package ipc

import (
	"context"
	"testing"

	"github.com/etcfs/etcfs/pkg/arena"
)

// statfsFree runs the STATFS handler and returns the free byte count it
// reported, converted back from 512-byte units.
func statfsFree(t *testing.T, svc *Service) uint64 {
	t.Helper()
	resp, err := svc.handleStatfs(context.Background(), nil)
	if err != nil {
		t.Fatalf("statfs: %v", err)
	}
	r := newReader(resp[4:]) // past the error word
	_ = r.u64()              // blocks
	return r.u64() * 512
}

// Free space is a property of the shared device, not of this node's arenas.
// Deriving it from local occupancy reported a nearly empty device as nearly
// full as soon as this node's own arena filled up.
func TestStatfsReportsClusterWideFreeSpace(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()

	before := statfsFree(t, svc)
	if before < deviceBytes-arena.ArenaSizeBytes {
		t.Fatalf("an untouched device reported only %d of %d bytes free", before, uint64(deviceBytes))
	}

	// One write puts this node in possession of exactly one arena, and fills a
	// vanishing fraction of it.
	seedFile(t, store, 4242, 0o100644)
	if _, err := svc.handleWrite(ctx, writePayload(4242, 0, []byte("hello"), 0)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := svc.alloc.ArenaCount(); got != 1 {
		t.Fatalf("expected the write to take one arena, got %d", got)
	}

	after := statfsFree(t, svc)
	// The claimed arena is counted as consumed, and its unused remainder is
	// added back because this node owns it: the total may not move by more
	// than a rounding error against the arena's occupancy.
	if before-after > arena.ArenaSizeBytes/2 {
		t.Fatalf("one small write cost %d bytes of reported free space", before-after)
	}

	// Fill most of this node's arena, reserving the blocks rather than writing
	// them so the test does not move a gigabyte. The rest of the device is
	// untouched and still free, and that is what statfs has to say: deriving
	// the answer from local occupancy reported this as a nearly full device.
	if _, err := svc.alloc.Allocate(arena.ArenaSizeBytes - 64<<20); err != nil {
		t.Fatalf("reserve most of the arena: %v", err)
	}
	full := statfsFree(t, svc)
	if full < arena.ArenaSizeBytes {
		t.Fatalf("a full local arena reported %d bytes free on a %d byte device with %d unclaimed",
			full, uint64(deviceBytes), uint64(deviceBytes)-arena.ArenaSizeBytes)
	}
}
