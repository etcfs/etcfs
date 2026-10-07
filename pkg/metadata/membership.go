package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Membership manages this node's presence in the EtcFS cluster.
//
// On start, it creates a lease-backed membership key in etcd:
//
//	membership:<node_id> = {node_id, cluster_name, joined_at, instance_id}
//
// instance_id is the cloud instance backing this node, recorded so the
// fencing controller can detach the shared volume from a node that has
// already expired and can no longer be asked.  Empty when not running on a
// cloud instance (Docker, bare metal); external fencing then degrades to a
// generation bump without a detach.
//
// The node's liveness is tied to the etcd lease — if the node stops
// sending keepalives, the key is auto-deleted and other nodes (including
// the fencing controller) detect the expiry.
type Membership struct {
	client     *clientv3.Client
	nodeID     string
	cluster    string
	leaseTTL   time.Duration
	instanceID string

	// log reports the heartbeat's failures.  Run returns on any of them, and
	// the watchdog eventually notices the lease is dead — but "eventually, with
	// no reason given" is the wrong thing to leave in an incident log.
	log Logger

	mu        sync.Mutex
	leaseID   clientv3.LeaseID
	alive     bool
	lastAlive time.Time
}

// Logger is the reporting interface the membership heartbeat needs.
type Logger interface {
	Error(msg string, args ...any)
}

// SetLogger attaches a logger.  Without one the heartbeat's failures are
// silent, which is correct only in tests.
func (m *Membership) SetLogger(l Logger) { m.log = l }

func (m *Membership) reportf(msg string, args ...any) {
	if m.log != nil {
		m.log.Error(msg, args...)
	}
}

// NewMembership creates a Membership for the given node.
func NewMembership(client *clientv3.Client, nodeID, cluster string, leaseTTL time.Duration) *Membership {
	return &Membership{
		client:   client,
		nodeID:   nodeID,
		cluster:  cluster,
		leaseTTL: leaseTTL,
	}
}

// SetInstanceID records the cloud instance backing this node.  It must be set
// before Run, because the value is written into the membership key and cannot
// be added afterwards — by the time the fencing controller needs it, the key
// has already been deleted by lease expiry.
func (m *Membership) SetInstanceID(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.instanceID = id
}

// Run starts the membership heartbeat loop.  Blocks until ctx is cancelled.
func (m *Membership) Run(ctx context.Context) {
	// Grant a lease and register
	leaseID, err := m.grantAndRegister(ctx)
	if err != nil {
		// The watchdog will detect this as a failure, after the self-fencing
		// window; saying so now is the difference between a diagnosable
		// startup and a node that simply stops.
		m.reportf("membership registration failed, this node will self-fence",
			"node", m.nodeID, "error", err)
		return
	}

	keepCh, err := m.client.KeepAlive(ctx, leaseID)
	if err != nil {
		m.reportf("membership keepalive could not be established, this node will self-fence",
			"node", m.nodeID, "error", err)
		return
	}

	m.updateAlive()

	for {
		select {
		case <-ctx.Done():
			m.revokeLease(context.Background())
			return
		case resp, ok := <-keepCh:
			if !ok {
				// KeepAlive channel closed — lease may be expired.
				// Try to re-establish.
				newID, err := m.grantAndRegister(ctx)
				if err != nil {
					m.setAlive(false)
					m.reportf("membership re-registration failed after the keepalive stream closed",
						"node", m.nodeID, "error", err)
					return
				}
				keepCh, err = m.client.KeepAlive(ctx, newID)
				if err != nil {
					m.setAlive(false)
					m.reportf("membership keepalive could not be re-established",
						"node", m.nodeID, "error", err)
					return
				}
				continue
			}
			if resp != nil {
				m.updateAlive()
			}
		}
	}
}

func (m *Membership) grantAndRegister(ctx context.Context) (clientv3.LeaseID, error) {
	resp, err := m.client.Grant(ctx, int64(m.leaseTTL.Seconds()))
	if err != nil {
		return 0, fmt.Errorf("membership: grant lease: %w", err)
	}

	m.mu.Lock()
	instanceID := m.instanceID
	m.mu.Unlock()
	value, err := json.Marshal(MembershipRecord{
		NodeID:     m.nodeID,
		Cluster:    m.cluster,
		JoinedAt:   time.Now().UTC(),
		InstanceID: instanceID,
	})
	if err != nil {
		_, _ = m.client.Revoke(ctx, resp.ID)
		return 0, fmt.Errorf("membership: encode record: %w", err)
	}

	_, err = m.client.Put(ctx, MembershipKey(m.nodeID), string(value), clientv3.WithLease(resp.ID))
	if err != nil {
		_, _ = m.client.Revoke(ctx, resp.ID)
		return 0, fmt.Errorf("membership: put key: %w", err)
	}

	// This node is alive again, so any fence owed for an earlier departure is
	// moot -- and, more importantly, any fence still in flight for it is now
	// acting on an incarnation that no longer exists.  Dropping the intent
	// here is what makes that visible: a fence re-checks the intent's
	// create-revision before every irreversible step, so clearing it aborts
	// the stale attempt instead of letting it sever, bump and reclaim a node
	// that has already come back.  The reconciliation sweep does the same
	// thing, but only every 30 s, and the window this closes is the one a
	// fast restart lands in.
	//
	// Best effort: a failure here costs one stale fence that the sweep will
	// still correct, and must not stop the node registering.
	if _, derr := m.client.Delete(ctx, FencePendingKey(m.nodeID)); derr != nil {
		m.reportf("membership: could not drop stale fence intent: %v", derr)
	}
	if _, derr := m.client.Delete(ctx, FenceDoneKey(m.nodeID)); derr != nil {
		m.reportf("membership: could not drop stale fence mark: %v", derr)
	}
	// A departure marker describes the departure it was written for, not this
	// node forever.  Dropping it as the node comes back is what makes the next
	// departure judged on its own merits rather than inheriting a clean bill
	// from the last one.
	if _, derr := m.client.Delete(ctx, DepartedKey(m.nodeID)); derr != nil {
		m.reportf("membership: could not drop stale departure marker: %v", derr)
	}

	m.mu.Lock()
	m.leaseID = resp.ID
	m.alive = true
	m.lastAlive = time.Now()
	m.mu.Unlock()

	return resp.ID, nil
}

// Leave takes this node out of the cluster: it returns every arena the node
// owns to the global free pool, then revokes the membership lease.  It reports
// the arenas actually released, which is empty when the node held none or when
// a fencing controller already released them.
//
// This is deliberately *not* called from Run's ctx.Done path.  Run is cancelled
// at the start of shutdown, while the arena may only be released once the node
// is provably serving nothing.  Only the owner
// of the serving path knows when that is true, so it must call Leave itself,
// after its IPC server has stopped.  Releasing on cancellation instead would
// hand the arena to another node while writes were still draining out of this
// one.
//
// The order matters in the other direction too: the arenas are released before
// the lease is revoked, so the ownership records are already gone by the time
// the membership key's deletion can wake a fencing controller.  The
// controller's own release is CAS-guarded and simply reports "nothing
// released" rather than racing this one.
func (m *Membership) Leave(ctx context.Context, store *Store) ([]uint64, error) {
	released, err := store.ReleaseArena(ctx, m.nodeID)

	// Announce the departure as intentional, so peers skip fencing this node
	// and it can be restarted without its device being detached first.
	//
	// Only when every arena really did come back.  The marker says "I have
	// given up everything I held", and a release that failed part way means
	// this node is still recorded as owning a range — which its peers must be
	// free to reclaim the only way they safely can, by fencing it.  Claiming a
	// clean departure here would be the one case where the claim is a lie the
	// node itself could have known about.
	if err == nil {
		marked, merr := store.MarkDeparted(ctx, m.nodeID)
		switch {
		case merr != nil:
			m.reportf("membership: could not announce departure, peers will fence this node: %v", merr)
		case !marked:
			m.reportf("membership: lease had already expired, so this departure cannot be announced as intentional")
		}
	}

	m.revokeLease(ctx)
	return released, err
}

func (m *Membership) revokeLease(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.leaseID != 0 {
		_, _ = m.client.Revoke(ctx, m.leaseID)
		m.leaseID = 0
	}
	m.alive = false
}

func (m *Membership) updateAlive() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alive = true
	m.lastAlive = time.Now()
}

func (m *Membership) setAlive(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alive = v
}

// IsAlive reports whether this node's membership lease is currently active.
//
// Being "alive" requires two things: the keepalive loop has not observed a
// terminal failure, *and* the last successful keepalive is recent enough that
// etcd cannot already have expired the lease server-side.
//
// The second condition is not redundant.  Under a total network partition the
// etcd client's KeepAlive channel is never closed — the client retries
// indefinitely and reports nothing — so the loop in Run never reaches the
// reconnect path that clears the flag.  Without a deadline check a partitioned
// node believes itself alive forever, the self-fencing watchdog never fires,
// and the node keeps serving while etcd has already handed its lease's
// expiry to the fencing controller.  Verified: 8+ minutes partitioned with no
// self-fence before this check existed.
//
// The lease TTL is the correct threshold because it is exactly the point at
// which etcd expires the lease after the last renewal.  The client library
// renews at roughly TTL/3, so a healthy node has ~3x margin and this cannot
// produce a false positive from ordinary jitter.
func (m *Membership) IsAlive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.alive || m.lastAlive.IsZero() {
		return false
	}
	return time.Since(m.lastAlive) <= m.leaseTTL
}

// LastAlive returns the time of the last successful keepalive.
func (m *Membership) LastAlive() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastAlive
}

// NodeID returns this node's identifier.
func (m *Membership) NodeID() string {
	return m.nodeID
}

// LeaseTTL returns the configured lease TTL.
func (m *Membership) LeaseTTL() time.Duration {
	return m.leaseTTL
}

// InstanceIDFromMembership extracts instance_id from a membership key's value.
//
// A record that is unreadable, or that has no instance_id at all, yields "" —
// the caller treats that as "cannot detach" rather than as an error, because
// the node whose value this is has already expired and cannot be asked again.
func InstanceIDFromMembership(value []byte) string {
	var rec MembershipRecord
	if err := json.Unmarshal(value, &rec); err != nil {
		return ""
	}
	return rec.InstanceID
}
