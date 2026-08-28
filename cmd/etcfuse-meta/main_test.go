package main

import (
	"context"
	"testing"
	"time"

	"github.com/etcfs/etcfs/internal/config"
	"github.com/etcfs/etcfs/internal/ipc"
	"github.com/etcfs/etcfs/pkg/fencing"
	"github.com/etcfs/etcfs/pkg/metadata"
)

// A self-fence must cancel the daemon's context — so the shutdown path that
// releases this node's arenas runs — and must be distinguishable from a signal,
// because the two produce different exit statuses.
func TestStopOnSignalOrFence_SelfFenceCancelsAndReports(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// A membership that never went alive: the watchdog fences on its first
	// tick, with no etcd involved.
	membership := metadata.NewMembership(nil, "node-a", "test", time.Millisecond)
	watchdog := fencing.NewWatchdog(membership, time.Millisecond)
	go watchdog.Run(ctx)

	svc := ipc.NewService(nil, membership, watchdog, config.NewLogger(0), ipc.Options{})
	fenced := stopOnSignalOrFence(ctx, cancel, svc, watchdog, config.NewLogger(0))

	select {
	case <-fenced:
	case <-time.After(2 * time.Second):
		t.Fatal("a self-fence should be reported to the caller")
	}
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a self-fence should cancel the daemon context")
	}
}

// An ordinary shutdown must not look like a self-fence, or every clean stop
// would exit 77 and read as a fencing incident.
func TestStopOnSignalOrFence_OrdinaryShutdownIsNotAFence(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	membership := metadata.NewMembership(nil, "node-a", "test", time.Hour)
	watchdog := fencing.NewWatchdog(membership, time.Hour)

	svc := ipc.NewService(nil, membership, watchdog, config.NewLogger(0), ipc.Options{})
	fenced := stopOnSignalOrFence(ctx, cancel, svc, watchdog, config.NewLogger(0))
	cancel()

	select {
	case <-fenced:
		t.Fatal("a cancelled context is not a self-fence")
	case <-time.After(100 * time.Millisecond):
	}
}
