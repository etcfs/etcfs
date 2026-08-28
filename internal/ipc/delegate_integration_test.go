//go:build integration
// +build integration

package ipc

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// Write delegation buffers an inode's extents while this node holds its lock
// and publishes them in one transaction.  The comparisons that transaction
// carries are what make it safe, and they are built from the cached snapshot —
// so the snapshot's revisions have to describe etcd exactly, at every point a
// write could read them.
//
// The hard case is a buffer that fills mid-workload: the flush that empties it
// moves every key it wrote to a new revision, and anything the next write plans
// from a snapshot predating that flush compares against a revision etcd has
// already left behind.  That transaction can never commit, so the buffer is
// stuck with it, fsync fails from then on, and the inode is wedged.

// fsyncInode drives the fsync handler and returns its errno.
func fsyncInode(t *testing.T, svc *Service, ino uint64) int32 {
	t.Helper()
	var b buf
	b.w64(ino)
	resp, err := svc.handleFsync(context.Background(), b.b)
	if err != nil {
		t.Fatalf("fsync ino %d: %v", ino, err)
	}
	return int32(binary.BigEndian.Uint32(resp))
}

// writeAt writes one block and fails the test on an errno.
func writeAt(t *testing.T, svc *Service, ino, offset uint64, data []byte) {
	t.Helper()
	resp, err := svc.handleWrite(context.Background(), writePayload(ino, offset, data, 0))
	if err != nil {
		t.Fatalf("write ino %d at %d: %v", ino, offset, err)
	}
	if e := int32(binary.BigEndian.Uint32(resp)); e != 0 {
		t.Fatalf("write ino %d at %d: errno %d", ino, offset, e)
	}
}

// A random-overwrite workload fills the buffer faster than the flush interval
// does: every overwrite contributes both a new extent and the rewrite of the
// one it buries, so the transaction's op ceiling is reached first.  The flush
// that ceiling forces must leave the node able to keep writing.
func TestIntegration_WritesSurviveABufferFilledByOverwrites(t *testing.T) {
	svc, store := newTestService(t)
	const ino = 9301
	seedFile(t, store, ino, 0o100644)

	block := make([]byte, 4096)
	for i := range block {
		block[i] = byte(i)
	}

	// Lay the file out first, then overwrite it in place.  The layout writes
	// bury nothing and contribute one op each; the overwrites contribute two,
	// so the buffer's op ceiling falls inside the second loop.
	const blocks = 2 * maxWriteTxnOps
	for i := uint64(0); i < blocks; i++ {
		writeAt(t, svc, ino, i*4096, block)
	}
	for i := uint64(0); i < blocks; i++ {
		writeAt(t, svc, ino, i*4096, block)
	}

	if e := fsyncInode(t, svc, ino); e != 0 {
		t.Fatalf("fsync returned errno %d: the buffer could not be published, "+
			"so every write acknowledged since the last flush is stranded", e)
	}

	// Published is not the same as correct: the file has to read back as the
	// last write left it, at the size the writes imply.
	rec, extents, err := store.GetInodeAndExtents(context.Background(), ino)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if want := uint64(blocks) * 4096; rec.Size != want {
		t.Errorf("size = %d after %d blocks, want %d", rec.Size, blocks, want)
	}
	if len(extents) == 0 {
		t.Fatal("no extents published for a file that was written twice over")
	}
}

// The snapshot a write plans from must agree with etcd about every revision it
// compares against.  Checking it directly says which of the two drifted, where
// the test above only reports that a flush stopped being possible.
func TestIntegration_CachedRevisionsSurviveAFlush(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9302
	seedFile(t, store, ino, 0o100644)

	block := make([]byte, 4096)

	// Enough writes to force at least one flush from the buffer's op ceiling.
	for i := uint64(0); i <= maxWriteTxnOps; i++ {
		writeAt(t, svc, ino, i*4096, block)
	}
	if e := fsyncInode(t, svc, ino); e != 0 {
		t.Fatalf("fsync returned errno %d", e)
	}

	m := cachedMetaFor(svc, ino)
	if m == nil {
		t.Fatal("nothing cached after a flush, so the next write pays a read it should not")
	}

	_, published, err := store.GetInodeAndExtents(ctx, ino)
	if err != nil {
		t.Fatalf("read extents: %v", err)
	}
	revs := make(map[string]int64, len(published))
	for _, e := range published {
		revs[e.Key] = e.ModRevision
	}
	for _, e := range m.extents {
		rev, found := revs[e.Key]
		if !found {
			t.Errorf("extent %s is cached but not published", e.Key)
			continue
		}
		if e.ModRevision != rev {
			t.Errorf("extent %s cached at revision %d, etcd holds it at %d: "+
				"a write planning against it would build a comparison that can never pass",
				e.Key, e.ModRevision, rev)
		}
	}
}

// A flush whose reply is lost commits in etcd and comes back as a rejection,
// because commitGuarded's retry re-proposes comparisons the first attempt has
// already invalidated — `CreateRevision == 0` on keys it just created.
//
// Replaying the same buffer against a flush that already landed reproduces that
// exactly, with no fault injection: the transaction is identical and etcd
// rejects it for the same reason. Treating the rejection at face value would
// wedge the inode on EIO for good and hold its blocks forever.
func TestIntegration_FlushWhoseReplyWasLostIsAdoptedNotRejected(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9304
	seedFile(t, store, ino, 0o100644)

	block := make([]byte, 4096)
	for i := uint64(0); i < 4; i++ {
		writeAt(t, svc, ino, i*4096, block)
	}

	e := svc.locks.lookup(ino)
	if e == nil {
		t.Fatal("no lock entry for an inode this node just wrote")
	}

	// The buffer as it stood before the flush consumed it, which is what a
	// retry after a lost reply would carry.
	e.keyMu.Lock()
	replay := *e.pending
	e.keyMu.Unlock()

	if err := svc.flushEntry(ctx, e, "test"); err != nil {
		t.Fatalf("first flush: %v", err)
	}

	_, before, err := store.GetInodeAndExtents(ctx, ino)
	if err != nil {
		t.Fatalf("read extents: %v", err)
	}

	e.keyMu.Lock()
	e.pending = &replay
	e.keyMu.Unlock()

	if err := svc.flushEntry(ctx, e, "test"); err != nil {
		t.Fatalf("a flush that had already landed was reported as failed: %v", err)
	}

	e.keyMu.Lock()
	stillPending := !e.pending.empty()
	e.keyMu.Unlock()
	if stillPending {
		t.Error("the buffer was kept after its transaction was found already published")
	}

	// Adoption must not have written anything a second time.
	_, after, err := store.GetInodeAndExtents(ctx, ino)
	if err != nil {
		t.Fatalf("re-read extents: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("extent count changed from %d to %d across an adopted flush",
			len(before), len(after))
	}
	for i := range after {
		if after[i] != before[i] {
			t.Errorf("extent %d changed across an adopted flush: %+v -> %+v",
				i, before[i], after[i])
		}
	}

	// And the inode must still be writable, which is what the wedge cost.
	writeAt(t, svc, ino, 0, block)
	if e := fsyncInode(t, svc, ino); e != 0 {
		t.Fatalf("fsync after an adopted flush returned errno %d", e)
	}
}

// A write that costs the file its set-user-ID bits must not leave them readable
// to anyone while the extent that dropped them waits in the buffer.  Deferring
// the bytes is a durability trade; deferring the mode change is a privilege one,
// and a peer that executes the file in the meantime gets the old mode.
func TestIntegration_SetIDBitsAreClearedWithoutWaitingForAFlush(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9303
	seedFile(t, store, ino, 0o104777) // setuid + setgid

	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, []byte("x"), 65534)); err != nil {
		t.Fatalf("write: %v", err)
	}

	rec, err := store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		t.Fatalf("read inode: %v", err)
	}
	if rec.Mode&(metadata.S_ISUID|metadata.S_ISGID) != 0 {
		t.Errorf("mode = %o in etcd right after an unprivileged write, want the "+
			"set-user-ID and set-group-ID bits gone", rec.Mode)
	}
}

// The per-inode cap bounds one hot file and says nothing about how many files
// are hot at once.  Without a process-wide cap, a workload spread across many
// inodes holds an unbounded amount of acknowledged-but-unpublished payload in
// RAM — every byte of which a crash would lose.
func TestIntegration_BufferedPayloadIsCappedAcrossInodes(t *testing.T) {
	svc, store := newTestService(t)

	// Long enough that the interval never fires during the run: the cap has to
	// be what publishes these buffers, not the timer.
	svc.flushInterval = time.Minute
	svc.dataCache = true

	const write = 256 << 10
	const cap = 4 * write
	svc.bufferMaxBytes = cap

	block := make([]byte, write)
	for i := range block {
		block[i] = byte(i)
	}

	const inodes = 24
	for i := 0; i < inodes; i++ {
		ino := uint64(9400 + i)
		// Not seedFile: it names every inode after the test, and this one needs
		// a directory entry per inode.
		if _, err := store.AtomicCreateFile(context.Background(), metadata.RootIno,
			fmt.Sprintf("%s-%d", t.Name(), i), ino, 0o100644, 1000, 1000, metadata.CreateExtra{}); err != nil {
			t.Fatalf("seed inode %d: %v", ino, err)
		}
		writeAt(t, svc, ino, 0, block)

		// One write may cross the cap before the next drain, and an entry with
		// an operation in flight is skipped — but nothing is in flight here, so
		// the overshoot is bounded by exactly one write.
		if held := svc.bufferedBytes.Load(); held > cap+write {
			t.Fatalf("after %d inodes, %d bytes buffered with a %d byte cap", i+1, held, cap)
		}
	}

	// Draining must publish, not discard: every file still has to read back.
	for i := 0; i < inodes; i++ {
		ino := uint64(9400 + i)
		if e := fsyncInode(t, svc, ino); e != 0 {
			t.Fatalf("fsync ino %d: errno %d", ino, e)
		}
		rec, extents, err := store.GetInodeAndExtents(context.Background(), ino)
		if err != nil {
			t.Fatalf("read metadata for ino %d: %v", ino, err)
		}
		if rec.Size != write {
			t.Errorf("ino %d: size = %d, want %d", ino, rec.Size, write)
		}
		if len(extents) == 0 {
			t.Errorf("ino %d: no extents published", ino)
		}
	}

	if held := svc.bufferedBytes.Load(); held != 0 {
		t.Errorf("%d bytes still counted as buffered after every inode was fsynced", held)
	}
	if ops := svc.bufferedOps.Load(); ops != 0 {
		t.Errorf("%d operations still counted as buffered after every inode was fsynced", ops)
	}
}

// The interval sweep is where a write that nothing fsynced is published, and it
// must put several inodes in one transaction — that is the whole saving.  Every
// one of them has to come out published and correct.
func TestIntegration_TheIntervalSweepPublishesManyInodesAtOnce(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()

	const files = 24
	block := make([]byte, 4096)
	for i := range block {
		block[i] = byte(i)
	}
	inos := make([]uint64, 0, files)
	for i := uint64(0); i < files; i++ {
		ino := 9320 + i
		if _, err := store.AtomicCreateFile(ctx, metadata.RootIno,
			fmt.Sprintf("%s-%d", t.Name(), i), ino, 0o100644, 1000, 1000,
			metadata.CreateExtra{}); err != nil {
			t.Fatalf("seed inode %d: %v", ino, err)
		}
		writeAt(t, svc, ino, 0, block)
		inos = append(inos, ino)
	}

	// Everything written above is older than the interval by the time this runs.
	time.Sleep(2 * DefaultFlushInterval)
	svc.flushExpired(ctx)

	for _, ino := range inos {
		rec, published, err := store.GetInodeAndExtents(ctx, ino)
		if err != nil {
			t.Fatalf("read ino %d: %v", ino, err)
		}
		if len(published) == 0 {
			t.Fatalf("ino %d has no extents after the sweep", ino)
		}
		if rec.Size != 4096 {
			t.Errorf("ino %d size = %d, want 4096", ino, rec.Size)
		}
	}
}
