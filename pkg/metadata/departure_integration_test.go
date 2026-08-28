//go:build integration
// +build integration

package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/etcfs/etcfs/test/etcdtest"
)

// A departure marker describes the departure it was written for, not the node
// for ever. A node that leaves cleanly, comes back, and then dies badly must be
// fenced like any other — so registering again drops the marker.
func TestIntegration_DepartureMarkerClearedOnRejoin(t *testing.T) {
	cli := etcdtest.Client(t)
	store := NewStore(cli, "rejoin-node")
	mem := NewMembership(cli, "rejoin-node", "test-cluster", 10*time.Second)
	ctx := context.Background()

	leaseID, err := mem.grantAndRegister(ctx)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _, _ = cli.Revoke(context.Background(), leaseID) })

	marked, err := store.MarkDeparted(ctx, "rejoin-node")
	if err != nil {
		t.Fatalf("mark departed: %v", err)
	}
	if !marked {
		t.Fatal("a live member could not announce its departure")
	}

	if _, err := mem.grantAndRegister(ctx); err != nil {
		t.Fatalf("re-register: %v", err)
	}

	departed, err := store.HasDeparted(ctx, "rejoin-node")
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	if departed {
		t.Error("the marker survived re-registration, so a later crash would not be fenced")
	}
}

// Leave announces the departure only when every arena really did come back. A
// release that failed part way leaves this node recorded as owning a range, and
// the only safe way for peers to reclaim it is to fence — so the node must not
// claim otherwise.
func TestIntegration_LeaveAnnouncesDepartureAfterReleasingArenas(t *testing.T) {
	cli := etcdtest.Client(t)
	store := NewStore(cli, "leaving-node")
	mem := NewMembership(cli, "leaving-node", "test-cluster", 10*time.Second)
	ctx := context.Background()

	if _, err := mem.grantAndRegister(ctx); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := store.Put(ctx, ArenaOwnerKey("leaving-node", 3), []byte("1")); err != nil {
		t.Fatalf("seed arena ownership: %v", err)
	}

	released, err := mem.Leave(ctx, store)
	if err != nil {
		t.Fatalf("leave: %v", err)
	}
	if len(released) != 1 {
		t.Fatalf("released %v, want the one arena the node owned", released)
	}

	owns, err := store.OwnsArenas(ctx, "leaving-node")
	if err != nil {
		t.Fatalf("read arena ownership: %v", err)
	}
	if owns {
		t.Error("the node still owns an arena after leaving")
	}

	departed, err := store.HasDeparted(ctx, "leaving-node")
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	if !departed {
		t.Error("a node that gave everything back did not announce its departure")
	}

	alive, err := store.Get(ctx, MembershipKey("leaving-node"))
	if err != nil {
		t.Fatalf("read membership: %v", err)
	}
	if alive != nil {
		t.Error("the departure did not take the node out of membership")
	}
}
