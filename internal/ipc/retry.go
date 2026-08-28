package ipc

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/internal/config"
	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/metrics"
)

// Synthetic history opcodes for events that never cross the IPC socket, kept
// far above the real wire opcodes (which top out at 35) so a decoder can never
// confuse one for the other. See test/verify/lock.go and generation.go for the
// matching decoders — deliberately separate code reading the same bytes, for
// the same reason the IPC history is decoded independently of the daemon that
// wrote it.
const (
	historyOpLockHold      = 1000
	historyOpGuardedCommit = 1001
)

const (
	lockEventAcquire = 0
	lockEventRelease = 1
)

// etcd operations on the data path are retried a few times before being
// reported to the kernel as an error, so that a leader election or a dropped
// connection during failover shows up as a brief stall rather than an EIO.
const (
	// etcdAttempts spans an etcd leader election rather than merely a dropped
	// connection.  Three attempts covered ~100ms, and an election that outlasts
	// that surfaced as EIO on every node at once — the failure the retries exist
	// to hide.  Five spans ~450ms of backoff, and the real ceiling stays the
	// request deadline, which retry aborts on.
	etcdAttempts   = 5
	etcdOpTimeout  = 2 * time.Second
	inodeLockTTL   = 2 * time.Second
	retryBaseDelay = 10 * time.Millisecond
	retryStep      = 40 * time.Millisecond

	// requestTimeout bounds the etcd work behind a single FUSE request.  It is
	// defined in the config package because it constrains what lease TTLs are
	// acceptable, and Parse rejects one that inverts the relationship.
	//
	// One value for every operation class, deliberately.  Metadata reads could
	// justify a tighter bound than lock acquisition, but splitting them is
	// speculative tuning until there is evidence a single ceiling is the wrong
	// shape; the property that matters here is that a bound exists at all.
	requestTimeout = config.RequestTimeout
)

// retryDelay is the pause before the attempt following the given one.
//
// Jittered by up to a quarter of the delay in either direction.  Every node
// retries against the same etcd cluster and a leader election stalls all of
// them at once, so an exact schedule has the whole cluster come back in
// lockstep — each wave arriving together, contending, and failing together.
// The mean is unchanged, which is what keeps the attempt budgets sized against
// this ramp (see contendedAttempts) still valid.
func retryDelay(attempt int) time.Duration {
	base := retryBaseDelay + time.Duration(attempt)*retryStep
	spread := int64(base / 2)
	return base - time.Duration(spread/2) + time.Duration(rand.Int64N(spread+1))
}

// retry runs fn until it succeeds or the attempt budget is spent, returning
// the last error.
//
// The pause between attempts honours ctx: a request whose deadline has already
// passed used to sit out the full backoff before noticing, holding the FUSE
// request open for a reply it was never going to give.
func retry(ctx context.Context, attempts int, fn func() error) error {
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if permanent(err) {
			return err
		}
		if werr := wait(ctx, retryDelay(attempt)); werr != nil {
			return err
		}
	}
	return err
}

// wait pauses for d, or returns early if ctx is done.
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// permanent reports whether an error will still hold on a retry.  A fence is
// permanent by definition — retrying it only delays the EIO the caller is
// going to get anyway, while holding the FUSE request open.
func permanent(err error) bool {
	return errors.Is(err, metadata.ErrFenced) ||
		errors.Is(err, metadata.ErrGuardUnavailable)
}

// retryEtcd runs fn against a fresh bounded context on every attempt.  Each
// attempt gets its own context so that one timed-out call does not poison the
// retries that follow it.
//
// The attempt context keeps the caller's values and drops only its
// cancellation, which is what the detachment was ever for: built from
// Background instead, it also threw away the transaction origin the caller had
// labelled the work with, and every commit taking this path — the extent
// publication behind close(), among others — was counted as "other".
func retryEtcd(ctx context.Context, fn func(context.Context) error) error {
	return retry(ctx, etcdAttempts, func() error {
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), etcdOpTimeout)
		defer cancel()
		return fn(actx)
	})
}

// heldLock is an acquired inode lock.
//
// The lock it holds is node-local: the etcd key behind it is cached by the
// lock cache and outlives this hold, so Release is a mutex unlock and costs no
// round trip at all.  See lockcache.go for why the key stays.
type heldLock struct {
	s        *Service
	e        *lockEntry
	mode     metadata.LockMode
	released bool

	// metaPublished records that the holder has already told the entry what
	// the inode's metadata is now, after changing it.  Without it, an exclusive
	// hold drops the cache on release — see Release.
	metaPublished bool
}

// meta returns the inode's record and extent list, from the cache when this
// node has held the lock key continuously since they were read, and from etcd
// otherwise.  A nil record means the inode does not exist.
//
// The returned value is shared with other holders and must not be mutated.
func (l *heldLock) meta(ctx context.Context) (*inodeMeta, error) {
	if m := l.e.cachedMeta(); m != nil {
		metrics.MetaCache.WithLabelValues("hit").Inc()
		return m, nil
	}
	metrics.MetaCache.WithLabelValues("miss").Inc()

	// The snapshot and the buffer are two halves of one statement about the
	// inode, and every path that invalidates one invalidates the other.  Losing
	// the snapshot while writes are still buffered would send this read to etcd,
	// which is behind by exactly those writes — so it fails instead of quietly
	// answering from a view that is missing data this node has acknowledged.
	l.e.keyMu.Lock()
	orphaned := !l.e.pending.empty()
	l.e.keyMu.Unlock()
	if orphaned {
		return nil, fmt.Errorf("ino %d: metadata snapshot lost with writes still buffered", l.e.ino)
	}

	var m inodeMeta
	if err := retryEtcd(ctx, func(ictx context.Context) error {
		var gerr error
		m.rec, m.extents, gerr = l.s.store.GetInodeAndExtents(ictx, l.e.ino)
		return gerr
	}); err != nil {
		return nil, err
	}
	l.e.setMeta(&m)
	return &m, nil
}

// publishMeta replaces what the entry has cached with the state this operation
// has just committed, so the next operation on the inode still needs no read.
//
// Only a caller that knows the whole post-commit state may use it: an
// operation that changes the inode and does not publish gets the cache dropped
// on release instead, which costs a read and cannot be wrong.
func (l *heldLock) publishMeta(m *inodeMeta) {
	l.e.setMeta(m)
	l.metaPublished = true
}

// flush publishes this inode's deferred writes.
//
// Every path that reads or rewrites an inode's extents in etcd rather than
// through the cached snapshot has to call it first: the buffer is the
// difference between the two, and a plan built from etcd alone would rewrite a
// file as if the writes this node has already acknowledged had not happened.
func (l *heldLock) flush(ctx context.Context) error {
	return l.s.flushEntry(ctx, l.e, "operation")
}

// lockModeByte encodes a LockMode as the single byte the lock history payload
// carries.
func lockModeByte(mode metadata.LockMode) byte {
	if mode == metadata.LockExclusive {
		return 1
	}
	return 0
}

// recordLockEvent appends one endpoint of a lock's hold interval to the
// history, as the interval the operation actually spanned.
//
// The instant a lock changes hands is the revision of one etcd transaction,
// and nothing here observes that instant directly — only the call and the
// return around it.  Recording that surrounding interval says exactly as much
// as is known, and lets the checker place the linearization point anywhere
// inside it.
//
// An earlier version recorded a single point taken after the call returned,
// which claimed a precision this code does not have: it placed the event
// strictly later than it happened, and left no room at all for clock offset
// between hosts, so ordinary skew between two nodes read as two holders of
// one lock.
//
// The interval recorded is the operation's, not the cached etcd key's, which
// is longer at both ends.  A subset of the true hold interval is the safe
// direction to be wrong in for a mutual-exclusion checker: it can only ever
// report overlaps that really happened.  It is also blind to a key kept past
// the operation that took it, which is what caching the lock introduced, so
// the key's own hold is recorded separately — see recordKeyEvent.
func (l *heldLock) recordLockEvent(kind byte, call, ret time.Time) {
	if l.s == nil || l.s.history == nil {
		return
	}
	var b buf
	b.b = append(b.b, kind, lockModeByte(l.mode))
	b.w64(l.e.ino)
	l.s.history.Record("lock_hold", historyOpLockHold, call, ret, b.b, nil)
}

// Release drops the node-local hold.  Idempotent, so a deferred Release is
// safe next to an explicit one.
//
// An exclusive hold that did not publish what it changed drops the entry's
// cached metadata on the way out.  That is the fail-safe direction: every
// mutation runs under this lock, so a path that has never heard of the cache —
// or one that gave up halfway — still cannot leave a stale snapshot behind.
// The cost of being wrong here is one read; the cost of the opposite default
// is serving a file's old extent list after it was rewritten.
func (l *heldLock) Release() {
	if l.released {
		return
	}
	l.released = true
	if l.mode == metadata.LockExclusive && !l.metaPublished {
		l.e.dropMeta()
	}
	call := time.Now()
	if l.mode == metadata.LockExclusive {
		l.e.unlockExclusive()
	} else {
		l.e.rw.RUnlock()
	}
	l.recordLockEvent(lockEventRelease, call, time.Now())
}

// lockInode takes a lock on an inode: the node-local exclusion first, then the
// etcd key that excludes the other nodes — which is usually already there,
// cached from an earlier operation on the same inode.
//
// The local wait is bounded rather than blocking. Two threads on one node
// contending for the same inode used to collide in etcd and get EAGAIN after
// the retry budget; keeping that shape means a pathologically slow holder
// still cannot pin a FUSE request open indefinitely.
func (s *Service) lockInode(ctx context.Context, ino uint64, mode metadata.LockMode) (*heldLock, error) {
	call := time.Now()

	for attempt := 0; attempt < entryRetries; attempt++ {
		e := s.locks.entryFor(ino)
		if err := lockLocal(ctx, e, mode); err != nil {
			return nil, err
		}
		// The entry can be evicted between the lookup and the local lock, and an
		// evicted entry excludes nothing: the next caller builds a fresh one and
		// takes a different mutex.  Start over on the entry that replaced it.
		if !s.locks.isCurrent(e) {
			unlockLocal(e, mode)
			continue
		}

		if err := s.ensureLockKey(ctx, e, mode); err != nil {
			unlockLocal(e, mode)
			return nil, err
		}

		held := &heldLock{s: s, e: e, mode: mode}
		held.recordLockEvent(lockEventAcquire, call, time.Now())
		return held, nil
	}
	return nil, metadata.ErrConflict
}

// lockLocal takes the entry's node-local lock, giving up rather than waiting
// forever.  sync.RWMutex has no timed acquire, so the budget is spent as
// TryLock attempts on the same backoff every other contended operation uses.
//
// The budget is the same one a peer's key gets: this node's own hold on an
// inode lasts as long as the recall it is waiting out, so sizing the local
// wait for local contention alone handed callers an EAGAIN on a plain write
// while the request deadline still had seconds left.  The deadline remains the
// bound — retry gives up on it.
func lockLocal(ctx context.Context, e *lockEntry, mode metadata.LockMode) error {
	return retry(ctx, contendedAttempts, func() error {
		if mode == metadata.LockExclusive {
			if e.tryLockExclusive() {
				e.exclusive.Store(true)
				return nil
			}
		} else if e.rw.TryRLock() {
			return nil
		}
		return metadata.ErrConflict
	})
}

func unlockLocal(e *lockEntry, mode metadata.LockMode) {
	if mode == metadata.LockExclusive {
		e.exclusive.Store(false)
		e.unlockExclusive()
	} else {
		e.rw.RUnlock()
	}
}

// ensureLockKey makes sure etcd carries a lock key for this inode that covers
// the requested mode, acquiring one only when the cache cannot answer.
func (s *Service) ensureLockKey(ctx context.Context, e *lockEntry, mode metadata.LockMode) error {
	e.keyMu.Lock()
	defer e.keyMu.Unlock()

	// A cached key is only as good as the lease it was written under.  If that
	// lease is gone — expired during a partition, or revoked — etcd deleted the
	// key with it and a peer may already hold the inode, so both the key and
	// anything cached under it are dropped here rather than trusted.  This is
	// what bounds how long a partitioned node can serve from its caches: the
	// lock session's TTL, not the self-fencing watchdog's much longer window.
	//
	// The comparison is against the lease this entry's key was written under,
	// not merely against there being a live session.  A dead session is
	// replaced lazily by the next acquisition on any inode, so "a session is
	// alive" goes true again while this entry's key — written under the
	// previous lease, and deleted with it — is already gone.  Liveness would
	// let exactly the stale holder this check exists to catch through.
	//
	// It costs a mutex and a channel poll, no round trip, so every operation
	// pays it.
	if e.holder != "" {
		if lease, ok := s.store.LockSessionLease(); !ok || lease != e.lease {
			s.log.Warn("lock session lost, dropping this node's cached lock",
				"ino", e.ino, "key_lease", e.lease, "session_lease", lease)
			// Anything buffered under that key can never be published: the
			// flush's own comparison on it would reject the transaction, and a
			// peer may already own the inode.  Nothing in etcd references the
			// blocks, so they go back to the arena.
			s.keyLostLocked(e, "the lock session backing them was lost")
		}
	}

	if e.holder != "" && covers(e.mode, mode) {
		return nil
	}
	// A cached shared key blocks this node's own exclusive acquisition — the
	// comparison behind it rejects any holder, including us — so it goes first.
	// The upgrade is not downgraded afterwards, so a read-modify-write sequence
	// pays this once rather than on every alternation.
	if e.holder != "" {
		// The kernel's pages go with the key here as well: the delete and the
		// re-acquisition are not one transaction, and a peer that slips into the
		// gap and writes would otherwise stay invisible behind them.
		if err := s.releaseKeyLocked(e); err != nil {
			return err
		}
	}

	call := time.Now()
	holder, err := s.acquireLockKey(ctx, e.ino, mode)
	if err != nil {
		return err
	}
	lease, _ := metadata.LockHolderLease(holder)
	e.holder, e.lease, e.mode, e.acquiredAt = holder, lease, mode, time.Now()
	s.recordKeyEvent(e.ino, mode, lockEventAcquire, call, e.acquiredAt, call)
	return nil
}

// acquireLockKey takes the etcd lock key, asking the current holder to yield if
// one is in the way.  The want key is written once per acquisition, not once
// per attempt: a peer needs to be told once, and each repeat is a Raft commit
// against a node already working on the recall.
func (s *Service) acquireLockKey(ctx context.Context, ino uint64, mode metadata.LockMode) (string, error) {
	var holder string
	announced := false
	var attempted []string

	err := retry(ctx, contendedAttempts, func() error {
		actx, cancel := context.WithTimeout(ctx, etcdOpTimeout)
		defer cancel()

		// An earlier attempt may have committed and lost its response, in which
		// case this node already holds the lock under that attempt's token —
		// and, for an exclusive lock, is now blocked by its own key forever,
		// since the token is fresh per attempt and the key lives as long as the
		// session.  Each token names exactly one key, so a point read settles
		// it.  Only reachable from the second attempt on, so an uncontended
		// acquisition never pays for this.
		if adopted, ok := s.adoptOwnLock(actx, ino, mode, attempted); ok {
			holder = adopted
			return nil
		}

		var aerr error
		holder, aerr = s.store.AcquireLock(metadata.WithTxnOrigin(actx, "lock_acquire"),
			ino, mode, inodeLockTTL)
		if holder != "" {
			attempted = append(attempted, holder)
		}
		if aerr != nil {
			holder = ""
		}
		// Announced on every conflicting attempt, not just the first.  A recall
		// fires off the *event* a want key writes, so a want announced against
		// one holder is never seen by the next: with three nodes on one inode,
		// A yields to B, B acquires without ever learning C is still waiting,
		// and C — whose key is sitting in etcd unheeded — spins until its
		// budget runs out and the write takes an EIO.  Re-announcing turns that
		// lost wakeup into one extra commit per attempt, and only while
		// actually blocked.
		if errors.Is(aerr, metadata.ErrConflict) {
			announced = true
			if werr := s.store.AnnounceLockWant(actx, ino); werr != nil {
				s.log.Warn("cannot announce a lock request; the holder will not be asked to yield",
					"ino", ino, "error", werr)
			}
		}
		return aerr
	})

	if announced {
		// Off the critical path: the lock is already ours or already lost, and
		// leaving the want key behind would have every peer yield this inode
		// for nothing from here on.
		go func() {
			cctx, cancel := context.WithTimeout(context.Background(), etcdOpTimeout)
			defer cancel()
			if cerr := s.store.ClearLockWant(cctx, ino); cerr != nil {
				s.log.Warn("lock request not withdrawn; peers will keep yielding this inode",
					"ino", ino, "error", cerr)
			}
		}()
	}
	return holder, err
}

// adoptOwnLock settles an acquisition whose response was lost.
//
// AcquireLock mints a fresh holder token per attempt, so a transaction that
// committed but never reported back leaves a key nothing will ever release —
// and, for an exclusive lock, one that blocks this node's own retries, since
// the acquisition comparison rejects any holder including us.  Each token
// names exactly one key, so checking the tokens this call has already tried
// tells us whether we hold the lock without guessing.
//
// Only this call's own tokens are ever adopted.  A key belonging to another
// operation on this node is left alone: two shared holders on one node are
// legitimate and separately owned, and adopting one would let this operation
// release a lock it never took.
func (s *Service) adoptOwnLock(ctx context.Context, ino uint64,
	mode metadata.LockMode, attempted []string) (string, bool) {

	for _, holder := range attempted {
		held, err := s.store.LockHeldBy(ctx, ino, mode, holder)
		if err != nil {
			return "", false
		}
		if held {
			s.log.Warn("adopted a lock key from an acquisition whose reply was lost",
				"ino", ino, "mode", mode)
			return holder, true
		}
	}
	return "", false
}

// commitGuarded applies ops in one transaction carrying the caller's
// comparisons plus this node's fencing generation.  It reports whether the
// transaction committed, the revision it committed at — the ModRevision every
// key it wrote now carries — and, separately, whether a fence was the reason it
// did not — a fenced node must not mutate metadata again.  Transient etcd errors
// are retried; a failed guard is not, because a fence is permanent.
//
// The guard itself is applied by the store (see metadata.Store.SetGuard), which
// covers every mutation path rather than only the ones that remember to ask.
// This wrapper keeps the retry policy and the "was it a fence" contract its
// callers are written against.
//
// A caller with no comparisons of its own can treat the two the same way — the
// guard is then the only thing that can reject the transaction — but a caller
// that supplies them must not: a comparison miss is contention it can rebuild
// its proposal from, and a fence is permanent.
func (s *Service) commitGuarded(ctx context.Context, cmps []clientv3.Cmp, ops []clientv3.Op) (committed, fenced bool, rev int64, err error) {
	call := time.Now()
	// Ensure the generation is resolved before committing, so a first write
	// fails with a real etcd error rather than ErrGuardUnavailable.
	gctx, gcancel := context.WithTimeout(context.Background(), etcdOpTimeout)
	gen, err := s.guardGeneration(gctx)
	gcancel()
	if err != nil {
		return false, false, 0, err
	}

	err = retryEtcd(ctx, func(tctx context.Context) error {
		var terr error
		committed, rev, terr = s.store.TxnRev(tctx, cmps, ops, nil)
		return terr
	})
	fenced = errors.Is(err, metadata.ErrFenced)
	s.recordGuardedCommit(gen, committed, fenced, call, time.Now())
	if fenced {
		return false, true, 0, nil
	}
	if err != nil {
		return false, false, 0, err
	}
	return committed, false, rev, nil
}

// recordGuardedCommit appends one guarded-commit attempt to the history: the
// generation this node believed it held, and whether the transaction
// committed. See test/verify/generation.go — the property it checks is that
// no commit succeeds once one has been rejected for a fence, which is the
// single most safety-critical invariant this codebase has.
func (s *Service) recordGuardedCommit(gen uint64, committed, fenced bool, call, ret time.Time) {
	if s.history == nil {
		return
	}
	var b buf
	b.w64(gen)
	flag := byte(0)
	if committed {
		flag |= 1
	}
	if fenced {
		flag |= 2
	}
	b.b = append(b.b, flag)
	s.history.Record("guarded_commit", historyOpGuardedCommit, call, ret, b.b, nil)
}
