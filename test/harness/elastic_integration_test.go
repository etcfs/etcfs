//go:build integration

package harness

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/pkg/arena"
	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/test/etcdtest"
)

// Elastic join and leave, driven through the path the daemon runs:
// metadata.Membership for the lease-bound membership key, arena.Allocator for
// the arena claim (lazy, on first write), Membership.Leave for departure.

// leaseTTL is short so a lease-expiry test finishes quickly, and long enough
// that keepalive jitter cannot expire a live node.
const leaseTTL = 3 * time.Second

type node struct {
	id     string
	cli    *clientv3.Client
	mem    *metadata.Membership
	store  *metadata.Store
	alloc  *arena.Allocator
	cancel context.CancelFunc
	done   chan struct{}
}

// join starts a node's membership heartbeat on its own etcd client and waits
// until the key is registered. Each node gets its own client so that closing
// one models that node's process dying without touching its peers.
func join(t *testing.T, id string) *node {
	t.Helper()
	cli := etcdtest.Client(t)
	ctx, cancel := context.WithCancel(context.Background())
	n := &node{
		id:     id,
		cli:    cli,
		mem:    metadata.NewMembership(cli, id, "test-cluster", leaseTTL),
		store:  metadata.NewStore(cli, id),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	n.alloc = arena.NewAllocator(id, n.store)
	go func() { defer close(n.done); n.mem.Run(ctx) }()
	t.Cleanup(n.stop)
	require.Eventually(t, n.mem.IsAlive, 5*time.Second, 20*time.Millisecond, "%s never registered", id)
	return n
}

func (n *node) stop() {
	n.cancel()
	<-n.done
}

func isMember(t *testing.T, s *metadata.Store, id string) bool {
	t.Helper()
	v, err := s.Get(context.Background(), metadata.MembershipKey(id))
	require.NoError(t, err)
	return v != nil
}

func ownsArenas(t *testing.T, s *metadata.Store, id string) bool {
	t.Helper()
	owns, err := s.OwnsArenas(context.Background(), id)
	require.NoError(t, err)
	return owns
}

func TestIntegration_Elastic_JoinRegistersLeaseBoundKey(t *testing.T) {
	n := join(t, "node-3")
	assert.True(t, isMember(t, n.store, "node-3"))
	// Arena acquisition is lazy: joining alone claims nothing.
	assert.False(t, ownsArenas(t, n.store, "node-3"))

	_, err := n.alloc.AcquireArena(context.Background())
	require.NoError(t, err)
	assert.True(t, ownsArenas(t, n.store, "node-3"))
}

func TestIntegration_Elastic_GracefulLeaveReleasesArenas(t *testing.T) {
	ctx := context.Background()
	n := join(t, "node-exit")
	a, err := n.alloc.AcquireArena(ctx)
	require.NoError(t, err)

	released, err := n.mem.Leave(ctx, n.store)
	require.NoError(t, err)
	assert.Equal(t, []uint64{a.ID}, released)
	assert.False(t, isMember(t, n.store, "node-exit"))
	assert.False(t, ownsArenas(t, n.store, "node-exit"))

	departed, err := n.store.HasDeparted(ctx, "node-exit")
	require.NoError(t, err)
	assert.True(t, departed, "a clean leave must be announced so peers skip fencing")

	// The released arena is reissued to the next node rather than lost.
	m := join(t, "node-next")
	b, err := m.alloc.AcquireArena(ctx)
	require.NoError(t, err)
	assert.Equal(t, a.ID, b.ID)
}

// The point of the lease: a node that dies without leaving must not stay a
// member. Its arenas stay owned — only the fencing controller may release
// them, after the device has cut the dead node off.
func TestIntegration_Elastic_DeadNodeMembershipExpires(t *testing.T) {
	ctx := context.Background()
	observer := join(t, "observer")
	victim := join(t, "victim")
	_, err := victim.alloc.AcquireArena(ctx)
	require.NoError(t, err)

	// Stop keepalives without revoking, as a SIGKILL would: close the
	// heartbeat's client first, so the revoke on shutdown cannot reach etcd.
	_ = victim.cli.Close()
	victim.stop()

	require.Eventually(t, func() bool { return !isMember(t, observer.store, "victim") },
		4*leaseTTL+5*time.Second, 100*time.Millisecond, "membership key outlived its lease")
	assert.True(t, ownsArenas(t, observer.store, "victim"),
		"a dead node's arenas must wait for a fence, not vanish with its lease")
	departed, err := observer.store.HasDeparted(ctx, "victim")
	require.NoError(t, err)
	assert.False(t, departed, "a crash must not look like a clean departure")
}

func TestIntegration_Elastic_ConcurrentJoinsGetDisjointArenas(t *testing.T) {
	const nodes = 5
	ns := make([]*node, nodes)
	for i := range ns {
		ns[i] = join(t, fmt.Sprintf("concurrent-%d", i))
	}

	ids := make([]uint64, nodes)
	errs := make([]error, nodes)
	var wg sync.WaitGroup
	for i, n := range ns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a, err := n.alloc.AcquireArena(context.Background())
			errs[i] = err
			if err == nil {
				ids[i] = a.ID
			}
		}()
	}
	wg.Wait()

	seen := map[uint64]string{}
	for i, id := range ids {
		require.NoError(t, errs[i])
		prev, dup := seen[id]
		assert.False(t, dup, "arena %d handed to %s and %s", id, prev, ns[i].id)
		seen[id] = ns[i].id
	}
}

func TestIntegration_Elastic_RepeatedJoinLeaveRecyclesArenas(t *testing.T) {
	ctx := context.Background()
	var first uint64
	for cycle := range 5 {
		n := join(t, fmt.Sprintf("cycle-%d", cycle))
		a, err := n.alloc.AcquireArena(ctx)
		require.NoError(t, err)
		if cycle == 0 {
			first = a.ID
		}
		assert.Equal(t, first, a.ID, "cycle %d grew the device instead of reusing the freed arena", cycle)
		_, err = n.mem.Leave(ctx, n.store)
		require.NoError(t, err)
		n.stop()
		assert.False(t, isMember(t, n.store, n.id))
	}
}
