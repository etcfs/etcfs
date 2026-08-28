//go:build integration
// +build integration

package ipc

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// Zero-copy handoff.
//
// A lock key outlives the operation that took it, so a producer that has
// finished writing still holds the inode. Without an explicit publish the
// consumer on another node pays for that: a want key, the producer's revocation
// watch noticing it, the minimum hold time, and only then an acquire — three
// etcd round trips on the consumer's critical path. Publishing moves all of it
// to the producer, so what the consumer arrives to is a free lock and extents
// already committed, and it reads the producer's own blocks off the device.

// setxattrOn drives the setxattr handler and returns its errno.
func setxattrOn(t *testing.T, svc *Service, ino uint64, name string) int32 {
	t.Helper()
	var b buf
	b.w64(ino)
	b.w32(uint32(len(name)))
	b.b = append(b.b, name...)
	b.w32(0)    // value length
	b.w32(0)    // flags
	b.w32(1000) // uid
	b.w32(1000) // gid
	resp, err := svc.handleSetxattr(context.Background(), b.b)
	if err != nil {
		t.Fatalf("setxattr %q on ino %d: %v", name, ino, err)
	}
	return int32(binary.BigEndian.Uint32(resp))
}

// lockHeld reports whether any lock key exists in etcd for an inode.
func lockHeld(t *testing.T, store *metadata.Store, ino uint64) bool {
	t.Helper()
	kvs, err := store.GetPrefix(context.Background(), metadata.LockPrefix(ino))
	if err != nil {
		t.Fatalf("read lock keys for ino %d: %v", ino, err)
	}
	return len(kvs) > 0
}

// The property the whole feature is for: after publishing, the inode's lock key
// is gone from etcd, so a peer acquires it without asking for it back.
func TestIntegration_PublishYieldsTheLockKey(t *testing.T) {
	svc, store := newTestService(t)
	const ino = 9601
	seedFile(t, store, ino, 0o100644)

	block := make([]byte, 4096)
	for i := range block {
		block[i] = byte(i)
	}
	writeAt(t, svc, ino, 0, block)

	if !lockHeld(t, store, ino) {
		t.Fatal("precondition: the writer should still hold the inode's lock key")
	}

	if e := setxattrOn(t, svc, ino, xattrPublish); e != 0 {
		t.Fatalf("publish returned errno %d", e)
	}

	if lockHeld(t, store, ino) {
		t.Error("the lock key survived the publish, so a peer still has to recall it")
	}
}

// Publishing is a handoff, so the bytes and the extents naming them have to be
// there before the key is. A consumer that acquired the freed lock and found no
// extents would read a file the producer had already written.
func TestIntegration_PublishCommitsWritesBeforeYielding(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9602
	seedFile(t, store, ino, 0o100644)

	block := make([]byte, 4096)
	for i := range block {
		block[i] = byte(i % 251)
	}
	writeAt(t, svc, ino, 0, block)

	if e := setxattrOn(t, svc, ino, xattrPublish); e != 0 {
		t.Fatalf("publish returned errno %d", e)
	}

	_, extents, err := store.GetInodeAndExtents(ctx, ino)
	if err != nil {
		t.Fatalf("read extents: %v", err)
	}
	if len(extents) == 0 {
		t.Fatal("the publish yielded the lock without committing the write it was holding")
	}
	if lockHeld(t, store, ino) {
		t.Error("the lock key was not yielded")
	}
}

// The publish is an action, not an attribute. Storing it would leave the file
// permanently carrying the record of a handoff that happened once, and would
// show up in every listing of its attributes.
func TestIntegration_PublishIsNotStoredAsAnAttribute(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9603
	seedFile(t, store, ino, 0o100644)

	if e := setxattrOn(t, svc, ino, xattrPublish); e != 0 {
		t.Fatalf("publish returned errno %d", e)
	}

	names, err := store.ListXattrs(ctx, ino)
	if err != nil {
		t.Fatalf("list xattrs: %v", err)
	}
	for _, n := range names {
		if n == xattrPublish {
			t.Errorf("%q was stored as an attribute", xattrPublish)
		}
	}
}

// Publishing an inode this node holds nothing for is a no-op that succeeds:
// there is no handoff to make, and failing would make the call unusable as an
// unconditional "I am done with this file" from an application that does not
// track whether it wrote anything.
func TestIntegration_PublishWithNoCachedLockSucceeds(t *testing.T) {
	svc, store := newTestService(t)
	const ino = 9604
	seedFile(t, store, ino, 0o100644)

	if e := setxattrOn(t, svc, ino, xattrPublish); e != 0 {
		t.Fatalf("publish on an unlocked inode returned errno %d", e)
	}
}

// An ordinary attribute must still be stored: the interception is one exact
// name, not the whole user namespace.
func TestIntegration_OrdinaryXattrIsUnaffectedByThePublishHook(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 9605
	seedFile(t, store, ino, 0o100644)

	if e := setxattrOn(t, svc, ino, "user.etcfs.publisher"); e != 0 {
		t.Fatalf("setxattr returned errno %d", e)
	}

	names, err := store.ListXattrs(ctx, ino)
	if err != nil {
		t.Fatalf("list xattrs: %v", err)
	}
	var found bool
	for _, n := range names {
		if n == "user.etcfs.publisher" {
			found = true
		}
	}
	if !found {
		t.Error("a name merely sharing the publish prefix was swallowed instead of stored")
	}
}
