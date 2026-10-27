// Package fencing implements the EtcFS self-fencing watchdog and
// external fencing controller interfaces.
//
// Self-fencing is the first line of defence against split-brain: each EtcFS
// daemon monitors its own etcd lease health.  Once the lease has been dead
// longer than 2× the TTL, the watchdog declares the node fenced and exits the
// process with code 77.
//
// The watchdog itself only signals: it sets the fenced flag and closes the
// Fenced() channel.  main waits on that channel and runs the same shutdown a
// SIGTERM does — cached locks back first, then the flush, the lock session and
// the arena release — before exiting 77.  Every step of that is an etcd call
// made by a node that has just concluded it cannot reach etcd, so each is best
// effort; what is not best effort is process exit, which lets the kernel
// release the block device and the FUSE mount, and open handles get EIO.
//
// The check runs on a ticker of a third of the TTL, so the fence lands between
// 2× and 2⅓× TTL after the last successful keepalive depending on tick phase.
//
// This narrows, but does not close, the most dangerous window — a node
// partitioned from etcd but still connected to the shared block device.
// Writes already issued to the kernel are not cancelled by the exit; they are
// made unreachable instead, because the generation guard rejects the metadata
// commit that would have published them.
package fencing

import (
	"context"
	"sync"
	"time"

	"github.com/etcfs/etcfs/internal/config"
	"github.com/etcfs/etcfs/pkg/metadata"
)

// Watchdog monitors etcd lease health and triggers self-fencing
// when the lease is lost and cannot be re-established.
type Watchdog struct {
	membership leaseState
	leaseTTL   time.Duration
	log        *config.Logger
	fenced     chan struct{} // closed when self-fence triggers

	mu       sync.Mutex
	isFenced bool
}

// leaseState is the part of metadata.Membership the watchdog reads, narrowed so
// tests can drive lease loss without etcd.
type leaseState interface {
	IsAlive() bool
	LastAlive() time.Time
	NodeID() string
}

// pollDivisor sets the poll interval to TTL/pollDivisor.  Polling once per TTL
// let a fence land up to a whole TTL past the 2× window; a third bounds the
// overshoot at TTL/3 for a negligible cost, since each poll is a mutex read.
const pollDivisor = 3

// NewWatchdog creates a self-fencing watchdog.
func NewWatchdog(membership *metadata.Membership, leaseTTL time.Duration) *Watchdog {
	return &Watchdog{
		membership: membership,
		leaseTTL:   leaseTTL,
		log:        config.NewLogger(1),
		fenced:     make(chan struct{}),
	}
}

// Run starts the watchdog.  It polls the membership every third of the lease
// TTL and triggers self-fence if the lease has been dead longer than
// config.SelfFenceWindow, which is 2× the TTL.
//
// Blocks until ctx is cancelled or self-fence triggers.
func (w *Watchdog) Run(ctx context.Context) {
	ticker := time.NewTicker(w.leaseTTL / pollDivisor)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.membership.IsAlive() {
				continue
			}

			// Membership was never established at all (the heartbeat loop
			// failed before its first successful keepalive).  A node with no
			// lease must not serve, so fence immediately rather than
			// computing an elapsed time against a zero timestamp.
			if w.membership.LastAlive().IsZero() {
				w.trigger()
				return
			}

			// Lease is dead.  Check how long it's been dead.
			deadSince := time.Since(w.membership.LastAlive())
			if deadSince > config.SelfFenceWindow(w.leaseTTL) {
				w.trigger()
				return
			}
		}
	}
}

// trigger executes the self-fence sequence.
func (w *Watchdog) trigger() {
	w.mu.Lock()
	if w.isFenced {
		w.mu.Unlock()
		return
	}
	w.isFenced = true
	close(w.fenced)
	w.mu.Unlock()

	w.log.Error("SELF-FENCED: lease expired beyond grace period, shutting down",
		"node_id", w.membership.NodeID(),
		"last_alive", w.membership.LastAlive(),
		"dead_for", time.Since(w.membership.LastAlive()))

	// Closing Fenced() above is the whole signal.  main waits on it and runs
	// the same shutdown a SIGTERM does before exiting 77 — exiting from here
	// skipped that, so a self-fenced node never released its arenas and leaked
	// them permanently in single-signal mode, where the fencing controller does
	// not reclaim them either.
}

// SelfFenceExitCode is the process exit status after a self-fence, distinct so
// systemd and the chaos harness can tell it from an ordinary failure.
const SelfFenceExitCode = 77

// IsFenced returns true if self-fencing has triggered.
func (w *Watchdog) IsFenced() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.isFenced
}

// Fenced returns a channel that is closed when self-fence triggers.
func (w *Watchdog) Fenced() <-chan struct{} {
	return w.fenced
}
