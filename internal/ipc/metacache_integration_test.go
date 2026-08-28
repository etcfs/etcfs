//go:build integration
// +build integration

package ipc

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/test/etcdtest"
)

// The metadata a node caches under a held inode lock is what its next read is
// answered from, so any drift from etcd's own state is a wrong answer served
// without a round trip to catch it.  These tests compare the two after every
// shape of mutation the write path can commit.

// cachedMetaFor returns what the service has cached for an inode, or nil.
func cachedMetaFor(svc *Service, ino uint64) *inodeMeta {
	e := svc.locks.lookup(ino)
	if e == nil {
		return nil
	}
	return e.cachedMeta()
}

// assertCacheMatchesEtcd fails if the cached view differs from a fresh read.
//
// A write's extents are buffered rather than committed, so the two agree only
// once the buffer is published — the comparison is against what the write
// eventually stored, not against what etcd held mid-flight.  The fsync is
// therefore part of the assertion rather than setup for it: it is the point the
// replay's answer becomes checkable.
func assertCacheMatchesEtcd(t *testing.T, svc *Service, store *metadata.Store, ino uint64, stage string) {
	t.Helper()
	ctx := context.Background()

	if e := fsyncInode(t, svc, ino); e != 0 {
		t.Fatalf("%s: fsync returned errno %d", stage, e)
	}

	m := cachedMetaFor(svc, ino)
	if m == nil {
		t.Fatalf("%s: nothing cached, so the next operation pays a read it should not", stage)
	}

	rec, extents, err := store.GetInodeAndExtents(ctx, ino)
	if err != nil {
		t.Fatalf("%s: read metadata: %v", stage, err)
	}
	if rec.Size != m.rec.Size || rec.Mode != m.rec.Mode {
		t.Errorf("%s: cached record (size %d mode %o) is not etcd's (size %d mode %o)",
			stage, m.rec.Size, m.rec.Mode, rec.Size, rec.Mode)
	}
	if len(extents) != len(m.extents) {
		t.Fatalf("%s: cached %d extents, etcd has %d", stage, len(m.extents), len(extents))
	}
	for i := range extents {
		if extents[i] != m.extents[i] {
			t.Errorf("%s: extent %d cached as %+v, etcd has %+v", stage, i, m.extents[i], extents[i])
		}
	}
}

// A write publishes its own outcome into the cache instead of re-reading it.
// Appends, overwrites that bury an earlier extent, and a write that splits one
// in two all go through the same replay, and all three have to land on exactly
// what etcd stored.
func TestIntegration_CachedMetadataMatchesEtcdAfterWrites(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9101
	seedFile(t, store, ino, 0o100644)

	block := make([]byte, 4096)
	for i := range block {
		block[i] = byte(i)
	}

	// Append, then extend: two extents, no burial.
	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, block, 0)); err != nil {
		t.Fatalf("write: %v", err)
	}
	assertCacheMatchesEtcd(t, svc, store, ino, "first write")

	if _, err := svc.handleWrite(ctx, writePayload(ino, 4096, block, 0)); err != nil {
		t.Fatalf("append: %v", err)
	}
	assertCacheMatchesEtcd(t, svc, store, ino, "append")

	// Full overwrite of the first extent: it is buried and reclaimed inside
	// the same transaction that publishes the new one.
	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, block, 0)); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	assertCacheMatchesEtcd(t, svc, store, ino, "overwrite")

	// A write landing strictly inside an extent splits it, so the replay has
	// to account for a delete and two puts at once.
	if _, err := svc.handleWrite(ctx, writePayload(ino, 5120, block[:1024], 0)); err != nil {
		t.Fatalf("split write: %v", err)
	}
	assertCacheMatchesEtcd(t, svc, store, ino, "split write")
}

// Every mutation that does not publish its outcome has to drop the cache
// instead, or the next read answers from a list the mutation invalidated.
func TestIntegration_MutationsThatDoNotPublishDropTheCache(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9102
	seedFile(t, store, ino, 0o100644)

	block := make([]byte, 8192)
	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, block, 0)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if cachedMetaFor(svc, ino) == nil {
		t.Fatal("write left nothing cached")
	}

	// Shrink through setattr, which rewrites extents without telling the cache.
	resp, err := svc.handleSetattr(ctx, setattrPayload(ino, fattrSize, 4096, 0, 0, 0, 0, 0, 0))
	if err != nil {
		t.Fatalf("setattr: %v", err)
	}
	if code := respCode(resp); code != 0 {
		t.Fatalf("setattr returned %d", code)
	}
	if m := cachedMetaFor(svc, ino); m != nil {
		t.Fatalf("truncate left a stale snapshot cached: %+v", m.extents)
	}

	// The next read refills it, and what it refills with has to be etcd's.
	if _, err := svc.handleRead(ctx, readRequest(ino, 0, 8192)); err != nil {
		t.Fatalf("read: %v", err)
	}
	assertCacheMatchesEtcd(t, svc, store, ino, "read after truncate")
}

// respCode reads the errno a handler's response opens with.
func respCode(resp []byte) int32 {
	return int32(binary.BigEndian.Uint32(resp[:4]))
}

// readRequest builds a READ payload.
func readRequest(ino, offset uint64, size uint32) []byte {
	var b buf
	b.w64(ino)
	b.w64(offset)
	b.w32(size)
	return b.b
}

// A cached lock key is only valid while the lease it was written under is
// still this node's.  The session is replaced lazily by the next acquisition
// on any inode, so "a session is alive" comes back true while a key written
// under the previous lease is already gone — and a node that trusted liveness
// would go on serving reads for an inode a peer has since taken.
func TestIntegration_CachedLockIsDroppedWhenItsSessionIsReplaced(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino, other = 9201, 9202
	seedFile(t, store, ino, 0o100644)

	block := make([]byte, 4096)
	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, block, 0)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if cachedMetaFor(svc, ino) == nil {
		t.Fatal("write left nothing cached")
	}

	// End the session the lock key was written under.  etcd deletes the key
	// with the lease, exactly as an expiry during a partition would.
	if err := store.CloseLockSession(); err != nil {
		t.Fatalf("close lock session: %v", err)
	}

	// Any acquisition on any inode now mints a fresh session, which is what
	// makes a liveness check answer "yes" again.
	if _, err := store.AtomicCreateFile(ctx, metadata.RootIno, t.Name()+"-other", other, 0o100644, 1000, 1000, metadata.CreateExtra{}); err != nil {
		t.Fatalf("seed second inode: %v", err)
	}
	if _, err := svc.handleWrite(ctx, writePayload(other, 0, block, 0)); err != nil {
		t.Fatalf("write to second inode: %v", err)
	}

	// A peer takes the first inode.  This can only succeed if our key really
	// is gone, so it doubles as the check that the setup did what it claims.
	peer := metadata.NewStore(etcdtest.Client(t), "peer-node")
	if _, err := peer.AcquireLock(ctx, ino, metadata.LockExclusive, 10*time.Second); err != nil {
		t.Fatalf("peer could not take the inode, so this node's key outlived its lease: %v", err)
	}

	// The read must now go and fail to acquire, not answer from a snapshot
	// whose lock belongs to someone else.
	resp, err := svc.handleRead(ctx, readRequest(ino, 0, 4096))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if code := respCode(resp); code != -11 {
		t.Fatalf("read returned %d, want -11 (EAGAIN): it was served from a lock this node no longer holds", code)
	}
}

// An acquisition whose reply is lost leaves a key nothing will release, and
// for an exclusive lock the retry is then blocked by this node's own key.
// The tokens the call already tried are what settle it.
func TestIntegration_AcquisitionAdoptsItsOwnOrphanedKey(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9203

	// Stands in for the attempt that committed and lost its response.
	holder, err := store.AcquireLock(ctx, ino, metadata.LockExclusive, 10*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	got, ok := svc.adoptOwnLock(ctx, ino, metadata.LockExclusive, []string{holder})
	if !ok || got != holder {
		t.Fatalf("own key not adopted: got %q, ok=%v", got, ok)
	}

	// A token this call never wrote must never be adopted, or an operation
	// would release a lock it does not own.
	if _, ok := svc.adoptOwnLock(ctx, ino, metadata.LockExclusive, []string{"999-999"}); ok {
		t.Fatal("adopted a key this acquisition never wrote")
	}
}
