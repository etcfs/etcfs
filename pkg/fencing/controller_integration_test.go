//go:build integration
// +build integration

// Fencing controller integration tests.
//
// These need real etcd because the property under test is the *ordering* of
// two effects — the volume detach and the generation bump — and the second is
// only observable as committed etcd state.
//
// Run with:
//
//	ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration -count=1 -v ./pkg/fencing/
package fencing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/internal/config"
	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/test/etcdtest"
)

func testController(t *testing.T, nodeID string) (*Controller, *metadata.Store, context.Context) {
	t.Helper()

	cli := etcdtest.Client(t)

	store := metadata.NewStore(cli, nodeID)
	mem := metadata.NewMembership(cli, nodeID, "test-cluster", 10*time.Second)
	return NewController(store, mem, config.NewLogger(0)), store, context.Background()
}

// stubFencer records what it was asked to do and fails on demand.
type stubFencer struct {
	called     int
	nodeID     string
	instanceID string
	err        error
}

func (s *stubFencer) Fence(_ context.Context, nodeID, instanceID string) error {
	s.called++
	s.nodeID = nodeID
	s.instanceID = instanceID
	return s.err
}

// The safety property: a confirmed detach is a precondition of the bump, not
// a parallel action. Bumping first would tell the cluster it may reclaim the
// node's arenas while the node might still be writing to them.
func TestController_BumpsOnlyAfterConfirmedDetach(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	stub := &stubFencer{}
	c.SetFencer(stub)

	c.fenceNode(ctx, "dead-node", "i-0123456789", false)

	assert.Equal(t, 1, stub.called, "the fence must be attempted")
	assert.Equal(t, "dead-node", stub.nodeID)
	assert.Equal(t, "i-0123456789", stub.instanceID)

	gen, err := store.GetGeneration(ctx, "dead-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen, "generation must be bumped once the detach is confirmed")
}

// The failure direction that matters. An unconfirmed detach means the node may
// still be writing; advertising it as fenced is worse than admitting the fence
// did not happen, because the fenced flag is what authorises peers to reclaim
// its arenas and locks.
func TestController_DoesNotBumpWhenDetachFails(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	stub := &stubFencer{err: errors.New("still attached after 60s")}
	c.SetFencer(stub)

	c.fenceNode(ctx, "wedged-node", "i-0123456789", false)

	assert.Equal(t, 1, stub.called)

	gen, err := store.GetGeneration(ctx, "wedged-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), gen,
		"generation must stay put when the volume was not confirmed detached")
}

// A node whose membership key predates instance-ID recording (rolling
// upgrade) cannot be detached, so it must not be reported as fenced either.
// The instance ID is the EBS path's requirement, not the controller's — an
// NVMeFencer needs no instance at all — so the refusal now comes from the
// detacher, and this test drives the real one to prove the controller honours
// it.
func TestController_DoesNotBumpWithoutInstanceID(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	c.SetFencer(&EBSDetacher{api: &fakeEC2{}, volumeID: "vol-test"})

	c.fenceNode(ctx, "legacy-node", "", false)

	gen, err := store.GetGeneration(ctx, "legacy-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), gen)
}

// Without a fencer configured (Docker, bare metal) the controller keeps its
// previous single-signal behaviour rather than refusing to fence at all.
func TestController_SingleSignalWhenNoFencer(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")

	c.fenceNode(ctx, "plain-node", "", false)

	gen, err := store.GetGeneration(ctx, "plain-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen,
		"no detacher means fence on lease expiry alone, as before")
}

func TestController_InstanceIDRoundTripsThroughMembershipValue(t *testing.T) {
	// The controller reads the instance ID out of the deleted key's previous
	// value, so the encoding written by Membership must be readable by the
	// extractor. A silent mismatch here would disable detaching entirely
	// while every test that stubs the value directly still passed.
	cli := etcdtest.Client(t)

	ctx := context.Background()
	mem := metadata.NewMembership(cli, "rt-node", "test-cluster", 10*time.Second)
	mem.SetInstanceID("i-roundtrip")

	runCtx, cancel := context.WithCancel(ctx)
	go mem.Run(runCtx)
	defer cancel()

	var raw []byte
	for i := 0; i < 50; i++ {
		resp, gerr := cli.Get(ctx, metadata.MembershipKey("rt-node"))
		require.NoError(t, gerr)
		if len(resp.Kvs) > 0 {
			raw = resp.Kvs[0].Value
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NotEmpty(t, raw, "membership key was never written")

	assert.Equal(t, "i-roundtrip", metadata.InstanceIDFromMembership(raw))
}

func TestInstanceIDFromMembership_MissingFieldIsEmpty(t *testing.T) {
	// An older node's value has no instance_id at all; that must read as
	// empty rather than panicking or returning garbage.
	legacy := []byte(`{"node_id":"n1","cluster":"c","joined_at":"2026-01-01T00:00:00Z"}`)
	assert.Equal(t, "", metadata.InstanceIDFromMembership(legacy))
}

// The retry gap this mechanism closes: the membership watch is edge-triggered,
// so a fence that fails has no event left to re-trigger it. The intent record
// is what survives the failed attempt, and the sweep is what consumes it.
func TestController_SweepRetriesFailedFence(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	stub := &stubFencer{err: errors.New("preempt timed out")}
	c.SetFencer(stub)

	require.NoError(t, store.RecordFenceIntent(ctx, "wedged-node", "i-0123456789"))
	c.fenceNode(ctx, "wedged-node", "i-0123456789", true)

	gen, err := store.GetGeneration(ctx, "wedged-node")
	require.NoError(t, err)
	require.Equal(t, uint64(0), gen, "precondition: the first attempt did not fence")

	// The device comes back; the sweep must pick the owed fence up unprompted.
	stub.err = nil
	c.reconcile(ctx)

	assert.Equal(t, 2, stub.called, "the sweep must re-attempt the failed fence")
	gen, err = store.GetGeneration(ctx, "wedged-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen, "the retry must complete the fence")
}

// The intent is the record of a fence that is *owed*; leaving it after a
// successful fence would make the sweep re-fence the node forever.
func TestController_SuccessfulFenceClearsIntent(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	c.SetFencer(&stubFencer{})

	require.NoError(t, store.RecordFenceIntent(ctx, "dead-node", "i-0123456789"))
	c.fenceNode(ctx, "dead-node", "i-0123456789", true)

	intents, err := store.ListFenceIntents(ctx)
	require.NoError(t, err)
	assert.NotContains(t, intents, "dead-node", "a completed fence owes nothing")
}

// A node that re-registered holds a live lease again, so it recovered from the
// expiry that triggered the fence. Severing its device access at that point
// would take down a healthy node.
func TestController_SweepDropsIntentWhenNodeRejoins(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	stub := &stubFencer{}
	c.SetFencer(stub)

	require.NoError(t, store.RecordFenceIntent(ctx, "rejoined-node", "i-0123456789"))
	_, err := store.Put(ctx, metadata.MembershipKey("rejoined-node"),
		[]byte(`{"node_id":"rejoined-node","instance_id":"i-0123456789"}`))
	require.NoError(t, err)

	c.reconcile(ctx)

	assert.Equal(t, 0, stub.called, "a re-registered node must not be fenced")
	gen, err := store.GetGeneration(ctx, "rejoined-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), gen)
	intents, err := store.ListFenceIntents(ctx)
	require.NoError(t, err)
	assert.NotContains(t, intents, "rejoined-node")
}

// The race half of the gap: every survivor sees the same DELETE event, so
// dedup has to be cluster-wide, not the per-process map it used to be.
func TestController_ConcurrentControllersFenceOnce(t *testing.T) {
	c1, store, ctx := testController(t, "survivor-1")
	c2, _, _ := testController(t, "survivor-2")
	stub1, stub2 := &stubFencer{}, &stubFencer{}
	c1.SetFencer(stub1)
	c2.SetFencer(stub2)

	require.NoError(t, store.RecordFenceIntent(ctx, "dead-node", "i-0123456789"))

	// c1 holds the claim for the whole of c2's attempt.
	leaseID, won, err := store.ClaimFence(ctx, "dead-node", c1.claimTTL)
	require.NoError(t, err)
	require.True(t, won)

	c2.fenceNode(ctx, "dead-node", "i-0123456789", true)
	assert.Equal(t, 0, stub2.called, "the loser of the claim must not fence")

	require.NoError(t, store.ReleaseFenceClaim(ctx, leaseID))
	c1.fenceNode(ctx, "dead-node", "i-0123456789", true)
	assert.Equal(t, 1, stub1.called)

	gen, err := store.GetGeneration(ctx, "dead-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen, "exactly one fence must have landed")
}

// The TOCTOU the claim alone does not close: a sweep decides what to fence
// from a ListFenceIntents snapshot, and that snapshot can go stale while the
// call waits on the claim. Without the post-claim re-check, a straggler
// replays a fence another controller already finished — a second real preempt
// or detach, and a second generation bump.
func TestController_SweepSkipsFenceCompletedWhileWaitingForClaim(t *testing.T) {
	c, store, ctx := testController(t, "straggler")
	stub := &stubFencer{}
	c.SetFencer(stub)

	// Exactly the state a straggler wakes up to: it listed the intent, another
	// controller then completed the fence (bumped, cleared, released), and the
	// claim is free again by the time this call reaches it.
	require.NoError(t, store.PutGeneration(ctx, "dead-node", 1))

	c.fenceNode(ctx, "dead-node", "i-0123456789", true)

	assert.Equal(t, 0, stub.called,
		"a fence already completed must not be re-issued against the device")
	gen, err := store.GetGeneration(ctx, "dead-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen, "the generation must not be bumped a second time")
}

// The watch path must stay unguarded by that re-check: it acts on a DELETE
// event it observed itself, so there is no stale snapshot, and an intent that
// failed to record must not silently disable fencing.
func TestController_WatchPathFencesWithoutARecordedIntent(t *testing.T) {
	c, store, ctx := testController(t, "watcher")
	stub := &stubFencer{}
	c.SetFencer(stub)

	c.fenceNode(ctx, "dead-node", "i-0123456789", false)

	assert.Equal(t, 1, stub.called, "the watch path must fence regardless of intent state")
	gen, err := store.GetGeneration(ctx, "dead-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen)
}

// A confirmed device fence must reclaim the fenced node's arena, and must do
// so as part of fenceNode itself — not eventually, via the sweep or anything
// else. Reclaim is gated on Fencer confirmation because that confirmation is
// the proof (device already rejects the node's writes) that makes immediate
// reissue safe.
func TestController_ReclaimsArenaAfterConfirmedFence(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	t.Cleanup(func() {
		store.Client().Delete(ctx, metadata.PrefixArena, clientv3.WithPrefix())
		store.Client().Delete(ctx, metadata.PrefixFreeArena, clientv3.WithPrefix())
	})
	stub := &stubFencer{}
	c.SetFencer(stub)

	_, err := store.Put(ctx, metadata.ArenaOwnerKey("dead-node", 7), metadata.EncodeUint64(7))
	require.NoError(t, err)

	start := time.Now()
	c.fenceNode(ctx, "dead-node", "i-0123456789", false)
	elapsed := time.Since(start)

	v, err := store.Get(ctx, metadata.ArenaOwnerKey("dead-node", 7))
	require.NoError(t, err)
	assert.Nil(t, v, "arena:dead-node/7 must be gone once fenceNode returns, took %s", elapsed)

	free, err := store.Get(ctx, metadata.FreeArenaKey(7))
	require.NoError(t, err)
	assert.NotNil(t, free, "arena 7 must be in the free pool once fenceNode returns")
}

// Single-signal mode (no Fencer) has no proof the fenced node's kernel
// stopped writing, so its arena must be left alone.
func TestController_DoesNotReclaimArenaWithoutFencer(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	t.Cleanup(func() {
		store.Client().Delete(ctx, metadata.PrefixArena, clientv3.WithPrefix())
		store.Client().Delete(ctx, metadata.PrefixFreeArena, clientv3.WithPrefix())
	})

	_, err := store.Put(ctx, metadata.ArenaOwnerKey("dead-node", 7), metadata.EncodeUint64(7))
	require.NoError(t, err)

	c.fenceNode(ctx, "dead-node", "i-0123456789", false)

	v, err := store.Get(ctx, metadata.ArenaOwnerKey("dead-node", 7))
	require.NoError(t, err)
	assert.NotNil(t, v, "arena:dead-node/7 must survive a single-signal fence — no severance proof exists")
}

// The gap the sweep exists to close: a membership DELETE that lands while the
// watch is being re-established reaches no controller, so nothing ever records
// an intent for it. Retrying only recorded intents left that node unfenced
// forever; deciding from current state fences it on the next pass.
func TestController_SweepFencesANodeNoEventWasSeenFor(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	stub := &stubFencer{}
	c.SetFencer(stub)

	// The node started once — that is all the cluster knows of it — and its
	// membership key is gone with no intent behind it.
	_, err := store.EnsureGenerationKey(ctx, "silently-gone")
	require.NoError(t, err)

	c.reconcile(ctx)

	assert.Equal(t, 1, stub.called, "a departed node must be fenced without an event")
	gen, err := store.GetGeneration(ctx, "silently-gone")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen)

	// And it must not be fenced again on every later pass.
	c.reconcile(ctx)
	assert.Equal(t, 1, stub.called, "a node already fenced must not be re-fenced")

	// Until it comes back and leaves again.
	_, err = store.Put(ctx, metadata.MembershipKey("silently-gone"),
		[]byte(`{"node_id":"silently-gone"}`))
	require.NoError(t, err)
	c.reconcile(ctx)
	require.NoError(t, store.Delete(ctx, metadata.MembershipKey("silently-gone")))
	c.reconcile(ctx)
	assert.Equal(t, 2, stub.called, "a second departure is a second fence")
}

// restartingFencer simulates the node coming back while the fence is in
// flight: the restart lands between the controller's gate and the steps that
// cannot be taken back.  Membership.grantAndRegister drops the node's fence
// intent as it re-registers, so that is what this does too.
type restartingFencer struct {
	called int
	store  *metadata.Store
	cli    *clientv3.Client
	nodeID string
	// leaveAgain re-creates the intent afterwards, modelling the node that
	// departs, restarts, and departs once more -- absent at both ends, and a
	// different incarnation in between.
	leaveAgain bool
	t          *testing.T
}

func (f *restartingFencer) Fence(ctx context.Context, _, _ string) error {
	f.called++
	_, err := f.cli.Put(ctx, metadata.MembershipKey(f.nodeID), `{"node_id":"`+f.nodeID+`"}`)
	require.NoError(f.t, err)
	require.NoError(f.t, f.store.ClearFenceIntent(ctx, f.nodeID))
	if f.leaveAgain {
		_, err = f.cli.Delete(ctx, metadata.MembershipKey(f.nodeID))
		require.NoError(f.t, err)
		require.NoError(f.t, f.store.RecordFenceIntent(ctx, f.nodeID, "i-second-life"))
	}
	return nil
}

// A node that restarts mid-fence must not be fenced for the departure it has
// already recovered from.  Before the incarnation check, the fence ran to
// completion against it: the node came back healthy, was cut off from the
// device, and was left with a cached startGen one behind the cluster's, so
// every write it made failed EIO for the life of the process while nothing
// reported it as unhealthy.  Found by TLC, not by fault injection -- see
// docs/verification/tla-plus.md.
func TestController_AbandonsFenceWhenNodeRestartsMidFence(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	cli := etcdtest.Client(t)
	const victim = "flapping-node"

	require.NoError(t, store.RecordFenceIntent(ctx, victim, "i-0123456789"))
	c.SetFencer(&restartingFencer{store: store, cli: cli, nodeID: victim, t: t})

	c.fenceNode(ctx, victim, "i-0123456789", false)

	gen, err := store.GetGeneration(ctx, victim)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), gen,
		"a node that came back must not have its generation bumped out from under it")
}

// The same, for the case a liveness check cannot catch: the node departs,
// restarts, and departs again.  It is equally absent at both ends, so only
// the incarnation the fence started against distinguishes them.  TLC rejected
// the liveness-only version of this fix on exactly this trace.
func TestController_AbandonsFenceWhenNodeLeftAndReturned(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	cli := etcdtest.Client(t)
	const victim = "flapping-node"

	require.NoError(t, store.RecordFenceIntent(ctx, victim, "i-0123456789"))
	c.SetFencer(&restartingFencer{store: store, cli: cli, nodeID: victim, leaveAgain: true, t: t})

	// The arena this incarnation legitimately re-claimed after restarting.
	// Releasing it is what would put a peer into a range the node is writing.
	_, err := cli.Put(ctx, metadata.ArenaOwnerKey(victim, 7), string(metadata.EncodeUint64(7)))
	require.NoError(t, err)

	c.fenceNode(ctx, victim, "i-0123456789", false)

	gen, gerr := store.GetGeneration(ctx, victim)
	require.NoError(t, gerr)
	assert.Equal(t, uint64(0), gen,
		"the fence must not bump a generation it no longer has the right incarnation for")

	owned, oerr := cli.Get(ctx, metadata.ArenaOwnerKey(victim, 7))
	require.NoError(t, oerr)
	assert.Len(t, owned.Kvs, 1,
		"the arena the restarted node re-claimed must not be released to a peer")
}

// The departure protocol.
//
// A node that shuts down on purpose used to be indistinguishable from one that
// crashed — etcd reports an explicit lease revoke and a lease that timed out as
// the same delete — so the cluster severed its device access on the way out and
// it could not simply be restarted. These pin the three things that make
// skipping that fence safe.

// A node that released everything and announced its departure is left alone.
func TestController_DoesNotFenceANodeThatLeftOnPurpose(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	stub := &stubFencer{}
	c.SetFencer(stub)

	// A gen: key is what makes the sweep consider a node at all; without one
	// this test would pass whether or not the departure marker did anything.
	_, err := store.EnsureGenerationKey(ctx, "departing-node")
	require.NoError(t, err)
	_, err = store.Put(ctx, metadata.MembershipKey("departing-node"),
		[]byte(`{"node_id":"departing-node","instance_id":"i-0123456789"}`))
	require.NoError(t, err)

	marked, err := store.MarkDeparted(ctx, "departing-node")
	require.NoError(t, err)
	require.True(t, marked, "precondition: a live member can announce its departure")

	c.reconcile(ctx)

	assert.Equal(t, 0, stub.called, "a node that left on purpose must not be severed")
	gen, err := store.GetGeneration(ctx, "departing-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), gen, "a clean departure is not an epoch boundary")
	intents, err := store.ListFenceIntents(ctx)
	require.NoError(t, err)
	assert.NotContains(t, intents, "departing-node")
}

// The claim is checked against the cluster's own records, not believed. A node
// still recorded as owning an arena has not given up what it says it gave up,
// and its arena can only be reclaimed by fencing it.
func TestController_FencesADepartingNodeThatStillOwnsArenas(t *testing.T) {
	c, store, ctx := testController(t, "controller-node")
	stub := &stubFencer{}
	c.SetFencer(stub)

	_, err := store.EnsureGenerationKey(ctx, "liar-node")
	require.NoError(t, err)
	_, err = store.Put(ctx, metadata.MembershipKey("liar-node"),
		[]byte(`{"node_id":"liar-node","instance_id":"i-0123456789"}`))
	require.NoError(t, err)
	_, err = store.Put(ctx, metadata.ArenaOwnerKey("liar-node", 7), []byte("1"))
	require.NoError(t, err)

	marked, err := store.MarkDeparted(ctx, "liar-node")
	require.NoError(t, err)
	require.True(t, marked)

	c.reconcile(ctx)

	assert.Equal(t, 1, stub.called,
		"a departure contradicted by the arena records must still be fenced")
	gen, err := store.GetGeneration(ctx, "liar-node")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen)
}

// The transaction is conditioned on the node still being a member, so a node
// whose lease has already timed out cannot go back and call its departure
// intentional. This is what stops a partitioned node writing itself an
// exemption from the protocol built to contain it.
func TestMarkDeparted_RefusedOnceTheLeaseIsGone(t *testing.T) {
	_, store, ctx := testController(t, "controller-node")

	marked, err := store.MarkDeparted(ctx, "expired-node")
	require.NoError(t, err)
	assert.False(t, marked, "a node with no membership key cannot announce a departure")

	departed, err := store.HasDeparted(ctx, "expired-node")
	require.NoError(t, err)
	assert.False(t, departed, "the refused transaction must not have written a marker")
}

// The marker and the membership delete are one transaction, so a controller can
// never see the departure without already being able to see the marker. A
// marker that landed after the delete would leave exactly the window in which a
// clean departure is fenced anyway.
func TestMarkDeparted_IsAtomicWithLeavingMembership(t *testing.T) {
	_, store, ctx := testController(t, "controller-node")

	_, err := store.Put(ctx, metadata.MembershipKey("atomic-node"), []byte(`{"node_id":"atomic-node"}`))
	require.NoError(t, err)

	marked, err := store.MarkDeparted(ctx, "atomic-node")
	require.NoError(t, err)
	require.True(t, marked)

	alive, err := store.Get(ctx, metadata.MembershipKey("atomic-node"))
	require.NoError(t, err)
	assert.Nil(t, alive, "the same transaction must have removed the node from membership")

	departed, err := store.HasDeparted(ctx, "atomic-node")
	require.NoError(t, err)
	assert.True(t, departed)
}
