package fencing

import (
	"context"
	"testing"
	"time"
)

type deadLease struct{ since time.Time }

func (d deadLease) IsAlive() bool        { return false }
func (d deadLease) LastAlive() time.Time { return d.since }
func (d deadLease) NodeID() string       { return "node-a" }

func newTestWatchdog(ttl time.Duration, lease leaseState) *Watchdog {
	w := NewWatchdog(nil, ttl)
	w.membership = lease
	return w
}

// The fence must land close to the 2×TTL window, not up to a whole TTL past
// it: polling at TTL/3 bounds the overshoot at a third of the TTL.
func TestWatchdog_FencesCloseToTwiceTTL(t *testing.T) {
	const ttl = 300 * time.Millisecond
	lost := time.Now()
	w := newTestWatchdog(ttl, deadLease{since: lost})

	go w.Run(t.Context())
	select {
	case <-w.Fenced():
	case <-time.After(5 * ttl):
		t.Fatal("watchdog did not fence a dead lease")
	}
	elapsed := time.Since(lost)
	if elapsed < 2*ttl {
		t.Fatalf("fenced after %s, before the %s window", elapsed, 2*ttl)
	}
	// TTL/3 of tick phase plus scheduling slack.
	if limit := 2*ttl + ttl/3 + 100*time.Millisecond; elapsed > limit {
		t.Fatalf("fenced after %s, want at most %s", elapsed, limit)
	}
}

// A live lease never fences, however long the watchdog runs.
func TestWatchdog_LiveLeaseNeverFences(t *testing.T) {
	w := newTestWatchdog(30*time.Millisecond, liveLease{})
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	w.Run(ctx)
	if w.IsFenced() {
		t.Fatal("a live lease must not fence")
	}
}

type liveLease struct{}

func (liveLease) IsAlive() bool        { return true }
func (liveLease) LastAlive() time.Time { return time.Now() }
func (liveLease) NodeID() string       { return "node-a" }
