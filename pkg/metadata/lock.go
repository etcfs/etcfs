package metadata

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Lock operations: acquire shared/exclusive file locks backed by etcd leases.
//
// A lock is acquired by CAS-inserting a lock:<ino> key with a lease binding.
// If the key already exists with a conflicting mode, the acquire fails.
// Lease expiry (crash, network partition) auto-deletes the lock key.

// LockMode represents the lock type.
type LockMode string

const (
	LockShared    LockMode = "shared"
	LockExclusive LockMode = "exclusive"
)

// fallbackSessionTTL backs a want key written before any lock has been taken.
// In practice a want key only follows a failed acquisition, which has already
// created the session, and lockSession ignores the TTL of every call after the
// first — so this is the value of last resort, not the operative one.
const fallbackSessionTTL = 2 * time.Second

// ErrConflict is returned when a lock cannot be acquired due to a conflict.
var ErrConflict = fmt.Errorf("lock conflict")

// ErrExists is returned when a resource already exists.
var ErrExists = fmt.Errorf("already exists")

// ErrFenced is returned when a transaction was rejected because this node's
// fencing generation was bumped — the node has been fenced and must not mutate
// shared state again.  Callers must surface this as EIO, never as a
// contention or not-found error: a fence is permanent, and misreporting it
// makes a fencing bug look like ordinary contention.
var ErrFenced = fmt.Errorf("node is fenced")

// ErrGuardUnavailable is returned when a guarded transaction is attempted
// before the fencing generation is known.  Failing closed is deliberate — an
// unguarded mutation is exactly what the guard exists to prevent.
var ErrGuardUnavailable = fmt.Errorf("fencing guard unavailable")

// Namespace errors, each mapping to the errno POSIX names for the situation.
var (
	// ErrNotEmpty: a directory that still has entries cannot be replaced.
	ErrNotEmpty = fmt.Errorf("directory not empty")
	// ErrNotDir: a directory can only be renamed over another directory.
	ErrNotDir = fmt.Errorf("not a directory")
	// ErrIsDir: a non-directory cannot be renamed over a directory.
	ErrIsDir = fmt.Errorf("is a directory")
	// ErrInvalid: the operation is rejected on its own terms — an unsupported
	// flag, or a directory rename that would detach a subtree into a cycle.
	ErrInvalid = fmt.Errorf("invalid argument")
	// ErrNotFound: the name or inode the operation names does not exist.
	ErrNotFound = fmt.Errorf("not found")
	// ErrPerm: the operation is not permitted on this kind of inode, such as a
	// hard link to a directory.
	ErrPerm = fmt.Errorf("operation not permitted")
	// ErrNoData: the named extended attribute does not exist.  Distinct from
	// ErrNotFound, which says the inode itself is missing — getxattr(2)
	// separates the two as ENODATA and ENOENT.
	ErrNoData = fmt.Errorf("no such attribute")
	// ErrTooBig: the value exceeds the size the store accepts for it.
	ErrTooBig = fmt.Errorf("value too large")
)

// lockSession returns the lease every lock on this node is written under,
// granting it on first use.
//
// A session that has ended — its lease expired while the node was partitioned,
// or it was revoked — is replaced rather than reused.  Expiry has already
// deleted every lock key written under it, which is the guarantee that matters:
// a node that stopped renewing holds nothing, exactly as when each lock carried
// its own lease.
//
// ttl fixes the session's TTL on the acquisition that creates it and is ignored
// afterwards, since one lease now backs every lock the store takes.
func (s *Store) lockSession(ttl time.Duration) (*concurrency.Session, error) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	if s.session != nil {
		select {
		case <-s.session.Done():
			s.session = nil
		default:
			return s.session, nil
		}
	}

	// Rounded up: etcd's lease TTL is whole seconds, and truncating a
	// sub-second TTL to zero would be rejected outright.
	seconds := int((ttl + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	// The session's keepalive deliberately runs on the client's own context
	// rather than any caller's: a keepalive bound to a request context would
	// stop renewing when that request ended, and the lock would silently expire
	// at its TTL while the holder still believed it held it — precisely the
	// stale-holder situation locking exists to prevent.
	sess, err := concurrency.NewSession(s.client, concurrency.WithTTL(seconds))
	if err != nil {
		return nil, err
	}
	s.session = sess
	return sess, nil
}

// LockSessionLease returns the lease every lock this node currently holds is
// written under, and whether there is a live session at all.
//
// It answers identity, not liveness, and the difference is the whole point.
// A dead session is replaced lazily by the next acquisition, so "is the
// current session alive" is true again the moment any other inode takes a
// lock — while a key written under the *previous* session is already gone,
// deleted with its lease. A caller holding state behind a lock has to compare
// the lease its key was written under against this value, and drop the state
// when they differ, not merely when there is no session.
//
// It never grants a session: a node that has taken no lock holds none.
func (s *Store) LockSessionLease() (clientv3.LeaseID, bool) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	if s.session == nil {
		return 0, false
	}
	select {
	case <-s.session.Done():
		return 0, false
	default:
		return s.session.Lease(), true
	}
}

// LockSessionWatch returns the current session's lease and a channel closed
// when that session ends, so a caller holding state behind this node's locks
// can drop it as soon as the lease is gone rather than at its next operation.
//
// The lease is returned with the channel deliberately: by the time the channel
// closes a later acquisition may already have granted a new session, and only
// the lease says which session the caller was watching.  State written under a
// different lease belongs to that new session and must not be dropped with the
// old one.
//
// It never grants a session, for the same reason LockSessionLease does not: a
// node that has taken no lock has nothing to watch.
func (s *Store) LockSessionWatch() (clientv3.LeaseID, <-chan struct{}, bool) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	if s.session == nil {
		return 0, nil, false
	}
	return s.session.Lease(), s.session.Done(), true
}

// LockHolderLease returns the lease a holder token was minted under.
//
// It lives next to the code that builds the token (see AcquireLock) so the
// format has exactly one definition and one reader.
func LockHolderLease(holder string) (clientv3.LeaseID, bool) {
	lease, _, found := strings.Cut(holder, "-")
	if !found {
		return 0, false
	}
	id, err := strconv.ParseInt(lease, 10, 64)
	if err != nil {
		return 0, false
	}
	return clientv3.LeaseID(id), true
}

// LockHeldBy reports whether one specific holder's key exists.
//
// Used to settle an acquisition whose response was lost: the transaction may
// have committed, and the key's own name says whether it did.  A linearizable
// point read, because the answer decides whether this node holds a lock.
func (s *Store) LockHeldBy(ctx context.Context, ino uint64, mode LockMode, holder string) (bool, error) {
	value, err := s.Get(ctx, LockKey(ino, mode, holder))
	if err != nil {
		return false, fmt.Errorf("lock held by ino %d: %w", ino, err)
	}
	return value != nil, nil
}

// AcquireLock takes a lock on an inode and returns the holder token that
// releases it.
//
// An exclusive lock is blocked by any holder.  A shared lock is blocked only by
// an exclusive one, so any number of readers hold it at once — each under its
// own key, which is what lets one of them release without dropping the lock for
// the rest.
//
// All of a node's locks share one session lease, so an acquisition is a single
// transaction: a per-lock GrantLease and RevokeLease around every write would
// be two Raft commits on the critical path, and the lease TTL is
// what releases a dead holder's lock whether that lease was granted once or
// once per write.  This is the shape of an NFSv4 write delegation, with the
// delegation scoped to the lock rather than to the open — a lock still spans a
// single operation, so no waiter can be made to wait on another node's close.
func (s *Store) AcquireLock(ctx context.Context, ino uint64, mode LockMode, ttl time.Duration) (string, error) {
	if mode != LockShared && mode != LockExclusive {
		return "", fmt.Errorf("acquire lock ino %d: unknown mode %q (%w)", ino, mode, ErrInvalid)
	}

	sess, err := s.lockSession(ttl)
	if err != nil {
		return "", fmt.Errorf("acquire lock ino %d: session: %w", ino, err)
	}

	// The lease is shared, so it cannot identify a holder on its own: two
	// concurrent readers on this node would write the same key and one
	// would release the other's lock.
	holder := fmt.Sprintf("%d-%d", sess.Lease(), s.lockSeq.Add(1))

	// The range that must be empty for this acquisition to be allowed.  Etcd
	// evaluates a comparison over a range as "true for every key in it", and an
	// empty range is vacuously true — so this reads as "no blocking holder
	// exists", decided atomically with the write below rather than in a
	// separate round trip that a competing acquire could slip through.
	blocked := LockPrefix(ino)
	if mode == LockShared {
		blocked = LockModePrefix(ino, LockExclusive)
	}
	cmp := clientv3.Compare(clientv3.CreateRevision(blocked), "=", 0).WithPrefix()

	op := clientv3.OpPut(LockKey(ino, mode, holder), s.nodeID, clientv3.WithLease(sess.Lease()))

	ok, err := s.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{op}, nil)
	if err != nil {
		// The holder is returned alongside the error on purpose: this call may
		// have committed and lost its response, and the token names the key
		// that would prove it.  See LockHeldBy.
		return holder, fmt.Errorf("acquire lock ino %d: %w", ino, err)
	}
	if !ok {
		return holder, fmt.Errorf("acquire lock ino %d: %w", ino, ErrConflict)
	}

	return holder, nil
}

// PrepareLock mints a holder token and the write that takes an inode's lock,
// for a caller that will commit it inside a transaction of its own rather than
// on its own round trip.
//
// The comparison is the same one AcquireLock evaluates — the blocking range
// must be empty — so a lock taken this way is taken under exactly the rule
// every other lock is, not under an assumption about the caller's inode.  What
// the caller gains is only that the assertion and its write ride a transaction
// it was going to commit anyway.
//
// The caller owns what it mints: a transaction that does not commit leaves no
// key, and one whose reply is lost leaves a key only this holder token names.
// See internal/ipc.handleCreate for how that second case is settled.
//
// One coupling to know about before changing inode allocation: the comparison
// fails if *any* lock key already names this inode, so a create folding it in
// fails when one does.  That is unreachable only because inode numbers are
// never reused (see internal/ipc/inodealloc.go); a scheme that recycled them
// could fail a create over a lock key left by the number's previous life.
func (s *Store) PrepareLock(ino uint64, mode LockMode, ttl time.Duration) (string, clientv3.Cmp, clientv3.Op, error) {
	if mode != LockShared && mode != LockExclusive {
		return "", clientv3.Cmp{}, clientv3.Op{},
			fmt.Errorf("prepare lock ino %d: unknown mode %q (%w)", ino, mode, ErrInvalid)
	}
	sess, err := s.lockSession(ttl)
	if err != nil {
		return "", clientv3.Cmp{}, clientv3.Op{}, fmt.Errorf("prepare lock ino %d: session: %w", ino, err)
	}

	holder := fmt.Sprintf("%d-%d", sess.Lease(), s.lockSeq.Add(1))
	blocked := LockPrefix(ino)
	if mode == LockShared {
		blocked = LockModePrefix(ino, LockExclusive)
	}
	return holder,
		clientv3.Compare(clientv3.CreateRevision(blocked), "=", 0).WithPrefix(),
		clientv3.OpPut(LockKey(ino, mode, holder), s.nodeID, clientv3.WithLease(sess.Lease())),
		nil
}

// LockRelease names one holder's lock key, for the batched release below.
type LockRelease struct {
	Ino    uint64
	Mode   LockMode
	Holder string
}

// ReleaseLocks drops several holders' lock keys in one transaction, reporting
// for each whether its key was still there to drop.
//
// One commit for the batch rather than one per key.  A cache eviction sweep is
// what makes that worth having: the cache gives up many inodes at once, and
// with a Raft commit per release a workload touching far more inodes than the
// cache holds would pay a commit per evicted inode.
//
// The per-key answers are what ReleaseLock's own return value is, and they
// matter for the same reason: a key the lease had already dropped means this
// node stopped holding the inode at an instant it never observed.  Nothing here
// compares on the keys, exactly as the single release does not — a key this
// node holds is one no peer can delete, and a key its lease dropped is one the
// delete should find gone rather than refuse.
func (s *Store) ReleaseLocks(ctx context.Context, batch []LockRelease) ([]bool, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	ops := make([]clientv3.Op, len(batch))
	for i, r := range batch {
		ops[i] = clientv3.OpDelete(LockKey(r.Ino, r.Mode, r.Holder))
	}

	resp, err := s.TxnResponse(ctx, nil, ops, nil)
	if err != nil {
		return nil, fmt.Errorf("release %d locks: %w", len(batch), err)
	}
	// Nothing but the fencing guard can reject this transaction, and a rejected
	// guard is reported as ErrFenced above.
	if !resp.Succeeded {
		return nil, fmt.Errorf("release %d locks: rejected", len(batch))
	}

	held := make([]bool, len(batch))
	for i := range batch {
		held[i] = resp.Responses[i].GetResponseDeleteRange().Deleted > 0
	}
	return held, nil
}

// ReleaseLock drops one holder's lock, reporting whether the key was still
// there to drop.
//
// Deleting that holder's key and only that key leaves a shared lock standing
// with its remaining holders — and, since the lease is shared by every lock the
// node holds, a delete is the only release that does not drop all of them.
//
// A false return means the lease had already expired the key: the caller
// stopped holding the lock at some instant it never observed, rather than at
// the moment it got round to releasing.  Callers that record when locks change
// hands have to tell the two apart, or a node whose session died reads as
// having held the inode right up until it noticed — across the whole window in
// which a peer legitimately owned it.
func (s *Store) ReleaseLock(ctx context.Context, ino uint64, mode LockMode, holder string) (bool, error) {
	n, err := s.DeleteCounting(ctx, LockKey(ino, mode, holder))
	if err != nil {
		return false, fmt.Errorf("release lock ino %d: %w", ino, err)
	}
	return n > 0, nil
}

// CloseLockSession ends the node's lock session, revoking the lease and with it
// any lock key still outstanding.  Idempotent.
func (s *Store) CloseLockSession() error {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	if s.session == nil {
		return nil
	}
	err := s.session.Close()
	s.session = nil
	return err
}

// AnnounceLockWant records that this node is waiting for an inode's lock, so
// that a peer holding a cached one drops it instead of keeping it until its
// session ends.  The key is written under the same session lease as the locks
// themselves, so a waiter that dies stops asking.
//
// Written unguarded: it mutates no filesystem state, and a fenced node that
// cannot ask for a lock would spin against a holder that has no reason to
// yield.  The acquisition the want key leads to is guarded as it always was.
func (s *Store) AnnounceLockWant(ctx context.Context, ino uint64) error {
	sess, err := s.lockSession(fallbackSessionTTL)
	if err != nil {
		return fmt.Errorf("announce lock want ino %d: session: %w", ino, err)
	}
	if _, err := s.putRaw(ctx, LockWantKey(ino, s.nodeID), []byte(s.nodeID),
		clientv3.WithLease(sess.Lease())); err != nil {
		return fmt.Errorf("announce lock want ino %d: %w", ino, err)
	}
	return nil
}

// ClearLockWant withdraws this node's request for an inode's lock.
func (s *Store) ClearLockWant(ctx context.Context, ino uint64) error {
	if _, err := s.client.Delete(ctx, LockWantKey(ino, s.nodeID)); err != nil {
		return fmt.Errorf("clear lock want ino %d: %w", ino, err)
	}
	return nil
}

// GetLockInfo returns the current lock state for an inode, or nil if it is
// unlocked.  A single exclusive holder makes the whole lock exclusive.
//
// Observation only — for tooling and tests.  No lock decision may be made from
// this or from IsLocked: whether a lock can be taken is decided inside
// AcquireLock's transaction, atomically with taking it, and a preceding read
// reopens exactly the window that transaction closes.
func (s *Store) GetLockInfo(ctx context.Context, ino uint64) (*LockRecord, error) {
	kvs, err := s.GetPrefix(ctx, LockPrefix(ino))
	if err != nil {
		return nil, fmt.Errorf("get lock ino %d: %w", ino, err)
	}
	if len(kvs) == 0 {
		return nil, nil
	}

	rec := &LockRecord{Mode: string(LockShared), Holders: make([]string, 0, len(kvs))}
	for _, kv := range kvs {
		if mode, ok := ParseLockKey(string(kv.Key), ino); ok && mode == LockExclusive {
			rec.Mode = string(LockExclusive)
		}
		rec.Holders = append(rec.Holders, string(kv.Value))
	}
	return rec, nil
}

// IsLocked returns true if any lock is held on the inode.  Observation only,
// for the same reason as GetLockInfo.
func (s *Store) IsLocked(ctx context.Context, ino uint64) (bool, error) {
	kvs, err := s.GetPrefix(ctx, LockPrefix(ino))
	if err != nil {
		return false, fmt.Errorf("is locked ino %d: %w", ino, err)
	}
	return len(kvs) > 0, nil
}

// WatchLock watches every holder key for an inode.  The returned channel
// delivers on any change to the lock state: acquired, released, or expired.
func (s *Store) WatchLock(ctx context.Context, ino uint64) clientv3.WatchChan {
	return s.Watch(ctx, LockPrefix(ino), clientv3.WithPrefix())
}
