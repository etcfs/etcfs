package metadata

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/etcfs/etcfs/pkg/metrics"
)

// GuardFunc supplies the fencing-generation comparison that every structural
// mutation must carry.  It returns the comparison, the generation that
// comparison encodes, and true once the node's generation is known; false
// means the guard is not yet available (generation not initialised) and the
// transaction must not proceed.
//
// It is a function rather than a stored Cmp because the generation is resolved
// lazily, on first use, by the owning service.  The generation is returned
// alongside the Cmp so a failed transaction can be classified without
// unpacking the comparison's protobuf representation.
type GuardFunc func() (cmp clientv3.Cmp, gen uint64, ok bool)

// Store is the primary metadata facade.  It wraps the etcd client and
// provides schema-aware operations (inode CRUD, dirent mutation, locking,
// fencing generation).  All structural mutations go through this type.
type Store struct {
	client *clientv3.Client
	nodeID string

	// localClient, when set, is a client dialed only at the etcd member
	// colocated with this node, and every read is attempted through it first.
	// See SetLocalClient.
	localClient *clientv3.Client

	// session is the lease every lock this node holds is written under, granted
	// once and kept alive for the store's lifetime instead of per acquisition.
	// lockSeq makes holder tokens unique within it.  See AcquireLock.
	sessionMu sync.Mutex
	session   *concurrency.Session
	lockSeq   atomic.Uint64

	// guard, when set, is prepended to the comparisons of every Txn.  A nil
	// guard leaves transactions unguarded — correct only for control-plane
	// stores (see SetGuard).
	guard GuardFunc

	// dirTouch, when set, coalesces the directory timestamp updates that follow
	// namespace mutations instead of committing one per mutation.  Nil writes
	// each one through.  See dirtouch.go.
	//
	// An atomic rather than a mutex because every stat consults it — a
	// directory's queued timestamp is what this node answers its own stat of
	// that directory with — and a shared mutex on that path would have every
	// FUSE thread queue behind the others for a pointer that is written once.
	dirTouch atomic.Pointer[dirTouch]
}

// NewStore creates a Store backed by the given etcd client.
//
// The returned Store is unguarded.  Callers serving filesystem requests must
// call SetGuard before serving, or a fenced node's namespace mutations will be
// accepted.
func NewStore(client *clientv3.Client, nodeID string) *Store {
	return &Store{
		client: client,
		nodeID: nodeID,
	}
}

// SetGuard installs the fencing-generation guard applied to every Txn.
//
// The guard is deliberately opt-out rather than opt-in: the failure mode being
// designed against is a new mutation path forgetting to guard itself, which is
// exactly how namespace operations went unguarded while the helper existed.
// Paths that must bypass it call txnRaw explicitly — there are only three
// (see EnsureGenerationKey, BumpGeneration, and bootstrap membership
// registration), and each is unguarded for a reason documented at the call.
func (s *Store) SetGuard(g GuardFunc) {
	s.guard = g
}

// SetLocalClient installs a client dialed only at the etcd member colocated
// with this node, through which reads are then issued.
//
// The round-robin client spreads requests over every endpoint, so a
// serializable read — which exists to avoid a leader round trip — can still
// leave the machine.  Pinning reads to the local member is what makes it
// actually local.  Linearizable reads are unaffected in meaning: the local
// member still confirms its read index with the leader.
//
// Writes keep using the cluster-wide client, and a read the local member
// cannot answer is retried there, so losing the colocated member costs
// latency rather than availability.
func (s *Store) SetLocalClient(c *clientv3.Client) {
	s.localClient = c
}

// read issues a range request, preferring the colocated member when one is
// configured.
func (s *Store) read(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if s.localClient != nil {
		if resp, err := s.localClient.Get(ctx, key, opts...); err == nil {
			return resp, nil
		}
	}
	return s.client.Get(ctx, key, opts...)
}

// readTxn issues read-only operations as one unconditional transaction.
//
// A transaction needs no comparisons to be useful here: etcd applies its
// operations against a single revision, so the batch costs one round trip and
// is a consistent snapshot rather than a sequence of independent reads.
func (s *Store) readTxn(ctx context.Context, ops ...clientv3.Op) (*clientv3.TxnResponse, error) {
	if s.localClient != nil {
		if resp, err := s.localClient.Txn(ctx).Then(ops...).Commit(); err == nil {
			return resp, nil
		}
	}
	return s.client.Txn(ctx).Then(ops...).Commit()
}

// Client returns the underlying etcd client (for direct use by watch
// multiplexer, fencing watchdog, etc.).
func (s *Store) Client() *clientv3.Client {
	return s.client
}

// NodeID returns this node's identifier.
func (s *Store) NodeID() string {
	return s.nodeID
}

// Txn executes a transaction guarded by this node's fencing generation.
// The caller provides comparison ops, success ops, and failure ops.  Returns
// true if the transaction succeeded (all comparisons matched and success ops
// were applied).
//
// When the transaction fails, the guard is re-checked to tell a fence apart
// from an ordinary CAS miss: a guard failure returns ErrFenced, because the
// two demand opposite responses.  A CAS miss is retryable contention; a fence
// is permanent and the caller must stop mutating metadata.
func (s *Store) Txn(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, error) {
	ok, _, err := s.TxnRev(ctx, ifs, thens, elses)
	return ok, err
}

// TxnRev is Txn, also returning the revision the transaction committed at —
// which is the ModRevision etcd stamped on every key it wrote.
//
// A caller that keeps its own copy of what it just wrote needs that number: it
// is what a later compare-and-set on those keys must compare against.
func (s *Store) TxnRev(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, int64, error) {
	resp, err := s.TxnResponse(ctx, ifs, thens, elses)
	if err != nil {
		return false, 0, err
	}
	return resp.Succeeded, resp.Header.Revision, nil
}

// TxnResponse is TxnRev with etcd's whole reply, for the callers that have to
// know what an individual operation did rather than only whether the
// transaction committed.
//
// A batch of deletes is the case that needs it: "the transaction committed"
// says nothing about which of its keys were still there to delete, and that
// distinction is what tells a lock this node dropped from one its lease had
// already dropped for it.
func (s *Store) TxnResponse(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (*clientv3.TxnResponse, error) {
	guarded := ifs
	if s.guard != nil {
		cmp, _, ok := s.guard()
		if !ok {
			return nil, fmt.Errorf("txn: %w", ErrGuardUnavailable)
		}
		// Prepend so the guard is evaluated with the caller's comparisons in
		// one atomic evaluation, not as a separate round trip that could race
		// a fence landing in between.
		guarded = append([]clientv3.Cmp{cmp}, ifs...)
	}

	resp, err := s.txnRawResponse(ctx, guarded, thens, elses)
	if err != nil {
		return nil, err
	}
	if !resp.Succeeded && s.guard != nil {
		if fenced, ferr := s.guardFailed(ctx); ferr == nil && fenced {
			return nil, fmt.Errorf("txn: %w", ErrFenced)
		}
	}
	return resp, nil
}

// Transaction origins.  A commit is the unit of cost in this filesystem, so
// which operation asked for one is worth carrying: a context tag rather than a
// parameter, because the callers that matter are several layers above the
// transaction and threading it by hand would touch every one of them.
type txnOriginKey struct{}

// WithTxnOrigin labels every transaction committed under ctx.
func WithTxnOrigin(ctx context.Context, origin string) context.Context {
	return context.WithValue(ctx, txnOriginKey{}, origin)
}

// TxnOrigin returns the label ctx carries, or "other" for a path that has not
// been tagged.  A large "other" is a sign this instrumentation has fallen
// behind the code, not that the commits came from nowhere.
func TxnOrigin(ctx context.Context) string {
	if origin, ok := ctx.Value(txnOriginKey{}).(string); ok {
		return origin
	}
	return "other"
}

// txnRaw executes a transaction without the fencing guard.
//
// Only for control-plane paths that cannot be guarded: creating the generation
// key the guard compares against, bumping a node's generation (which must not
// be guarded by the generation it is changing), and bootstrap membership
// registration that runs before the generation is known.  Everything else must
// use Txn.
func (s *Store) txnRaw(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, int64, error) {
	resp, err := s.txnRawResponse(ctx, ifs, thens, elses)
	if err != nil {
		return false, 0, err
	}
	return resp.Succeeded, resp.Header.Revision, nil
}

// txnRawResponse is txnRaw with etcd's whole reply.  Same rules: unguarded, so
// control-plane paths only.
func (s *Store) txnRawResponse(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (*clientv3.TxnResponse, error) {
	resp, err := s.client.Txn(ctx).If(ifs...).Then(thens...).Else(elses...).Commit()
	if err != nil {
		metrics.EtcdTxns.WithLabelValues("error").Inc()
		return nil, fmt.Errorf("txn: %w", err)
	}
	if resp.Succeeded {
		metrics.EtcdTxns.WithLabelValues("committed").Inc()
		metrics.EtcdTxnOrigin.WithLabelValues(TxnOrigin(ctx)).Inc()
	} else {
		metrics.EtcdTxns.WithLabelValues("rejected").Inc()
	}
	return resp, nil
}

// guardFailed reports whether the installed guard no longer matches the stored
// generation — that is, whether this node has been fenced since it started.
func (s *Store) guardFailed(ctx context.Context) (bool, error) {
	_, expected, ok := s.guard()
	if !ok {
		return false, nil
	}
	current, err := s.GetGeneration(ctx, s.nodeID)
	if err != nil {
		return false, err
	}
	return current != expected, nil
}

// Get reads a single key's value.  Returns nil if the key doesn't exist.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.read(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

// GetMany reads several keys in one round trip, returning only those present.
//
// Batched because the alternative is a request per key.  A readdir of a
// thousand-entry directory made a thousand sequential gets, on every listing.
func (s *Store) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))

	// etcd rejects a transaction with more than maxTxnOps operations, so a
	// large directory is read in several.
	const maxTxnOps = 128
	for start := 0; start < len(keys); start += maxTxnOps {
		end := min(start+maxTxnOps, len(keys))
		ops := make([]clientv3.Op, 0, end-start)
		for _, k := range keys[start:end] {
			ops = append(ops, clientv3.OpGet(k))
		}
		resp, err := s.readTxn(ctx, ops...)
		if err != nil {
			return nil, fmt.Errorf("get %d keys: %w", end-start, err)
		}
		for _, r := range resp.Responses {
			for _, kv := range r.GetResponseRange().Kvs {
				out[string(kv.Key)] = kv.Value
			}
		}
	}
	return out, nil
}

// GetPrefix reads all keys with the given prefix.
func (s *Store) GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	return s.getPrefix(ctx, prefix)
}

// getPrefix is GetPrefix with read options, kept off MetadataStore so that
// asking for a serializable read stays a decision of the few call sites that
// have established they do not need a leader round trip.
func (s *Store) getPrefix(ctx context.Context, prefix string, opts ...clientv3.OpOption) ([]*mvccpb.KeyValue, error) {
	resp, err := s.read(ctx, prefix, append([]clientv3.OpOption{clientv3.WithPrefix()}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("get prefix %s: %w", prefix, err)
	}
	return resp.Kvs, nil
}

// GetRevision reads a key at a specific etcd revision (point-in-time snapshot).
// Useful for consistent paginated directory listings.
func (s *Store) GetRevision(ctx context.Context, key string, opts ...clientv3.OpOption) ([]*mvccpb.KeyValue, int64, error) {
	resp, err := s.read(ctx, key, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("get rev %s: %w", key, err)
	}
	return resp.Kvs, resp.Header.Revision, nil
}

// Put writes a key-value pair.  Returns the new revision.
//
// When a guard is installed the write is issued as a guarded transaction, not
// a bare Put: a fenced node must not be able to mutate metadata through a call
// that happens to skip Txn.  Several namespace handlers (setattr, symlink,
// mknod) and the truncate path write inode records this way.
func (s *Store) Put(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error) {
	if s.guard != nil {
		return s.guardedWrite(ctx, clientv3.OpPut(key, string(value), opts...), "put", key)
	}
	return s.putRaw(ctx, key, value, opts...)
}

// putRaw writes without the fencing guard.  Control-plane use only — see
// txnRaw for the rules.
func (s *Store) putRaw(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error) {
	resp, err := s.client.Put(ctx, key, string(value), opts...)
	if err != nil {
		return 0, fmt.Errorf("put %s: %w", key, err)
	}
	return resp.Header.Revision, nil
}

// guardedWrite applies a single mutation inside a generation-guarded
// transaction, translating a guard rejection into ErrFenced.
func (s *Store) guardedWrite(ctx context.Context, op clientv3.Op, verb, key string) (int64, error) {
	cmp, _, ok := s.guard()
	if !ok {
		return 0, fmt.Errorf("%s %s: %w", verb, key, ErrGuardUnavailable)
	}
	resp, err := s.client.Txn(ctx).If(cmp).Then(op).Commit()
	if err != nil {
		return 0, fmt.Errorf("%s %s: %w", verb, key, err)
	}
	if !resp.Succeeded {
		return 0, fmt.Errorf("%s %s: %w", verb, key, ErrFenced)
	}
	return resp.Header.Revision, nil
}

// Delete removes a key.  Guarded when a guard is installed — see Put.
func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.DeleteCounting(ctx, key)
	return err
}

// DeleteCounting removes a key and reports how many were actually there.
//
// The count is what distinguishes "this key was mine and I dropped it" from
// "it was already gone", which an unconditional delete cannot tell apart — and
// the two say very different things about when the caller stopped holding
// whatever the key stood for.
func (s *Store) DeleteCounting(ctx context.Context, key string) (int64, error) {
	if s.guard != nil {
		cmp, _, ok := s.guard()
		if !ok {
			return 0, fmt.Errorf("delete %s: %w", key, ErrGuardUnavailable)
		}
		resp, err := s.client.Txn(ctx).If(cmp).Then(clientv3.OpDelete(key)).Commit()
		if err != nil {
			return 0, fmt.Errorf("delete %s: %w", key, err)
		}
		if !resp.Succeeded {
			return 0, fmt.Errorf("delete %s: %w", key, ErrFenced)
		}
		return resp.Responses[0].GetResponseDeleteRange().Deleted, nil
	}
	resp, err := s.client.Delete(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("delete %s: %w", key, err)
	}
	return resp.Deleted, nil
}

// DeletePrefix removes all keys with the given prefix.  Guarded when a guard
// is installed — see Put.
func (s *Store) DeletePrefix(ctx context.Context, prefix string) (int64, error) {
	if s.guard != nil {
		cmp, _, ok := s.guard()
		if !ok {
			return 0, fmt.Errorf("delete prefix %s: %w", prefix, ErrGuardUnavailable)
		}
		resp, err := s.client.Txn(ctx).
			If(cmp).
			Then(clientv3.OpDelete(prefix, clientv3.WithPrefix())).
			Commit()
		if err != nil {
			return 0, fmt.Errorf("delete prefix %s: %w", prefix, err)
		}
		if !resp.Succeeded {
			return 0, fmt.Errorf("delete prefix %s: %w", prefix, ErrFenced)
		}
		return resp.Responses[0].GetResponseDeleteRange().Deleted, nil
	}
	resp, err := s.client.Delete(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("delete prefix %s: %w", prefix, err)
	}
	return resp.Deleted, nil
}

// GrantLease creates a new lease with the given TTL.
func (s *Store) GrantLease(ctx context.Context, ttl time.Duration) (clientv3.LeaseID, error) {
	resp, err := s.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return 0, fmt.Errorf("grant lease: %w", err)
	}
	return resp.ID, nil
}

// KeepAlive starts a keepalive stream for the given lease.
// The caller must receive from the returned channel to keep the lease alive.
func (s *Store) KeepAlive(ctx context.Context, leaseID clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return s.client.KeepAlive(ctx, leaseID)
}

// RevokeLease immediately terminates a lease.
func (s *Store) RevokeLease(ctx context.Context, leaseID clientv3.LeaseID) error {
	_, err := s.client.Revoke(ctx, leaseID)
	return err
}

// Watch creates a watch on the given key or prefix.
// The returned channel delivers WatchResponses until ctx is cancelled.
func (s *Store) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return s.client.Watch(ctx, key, opts...)
}

// MemberList returns the list of etcd cluster members.
func (s *Store) MemberList(ctx context.Context) (*clientv3.MemberListResponse, error) {
	return s.client.MemberList(ctx)
}

// Status returns the etcd endpoint status.
func (s *Store) Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
	return s.client.Status(ctx, endpoint)
}
