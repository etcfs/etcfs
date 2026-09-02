package ipc

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/internal/config"
	"github.com/etcfs/etcfs/pkg/metadata"
)

// A client that keeps its socket open and never answers costs a full ack
// timeout per lock release, on a path that is one socket and one thread. After
// enough of them in a row it is declared unresponsive and acknowledged
// messages fail immediately instead.
func TestNotifyBreakerTripsOnRepeatedAckTimeouts(t *testing.T) {
	n := &notifyServer{ackTimeout: 20 * time.Millisecond}

	// A fresh pipe per attempt: a timeout drops the connection, and the wedged
	// client this models reconnects afterwards. The breaker has to count across
	// those reconnections or it never reaches its limit.
	wedge := func() {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = client.Close() })
		go func() { _, _ = client.Read(make([]byte, 12)) }() // read, never answer
		n.set(server)
	}

	for i := 1; i < notifyBreakerTrips; i++ {
		wedge()
		if err := n.send([]byte("msg"), true); !errors.Is(err, errNoNotifyClient) {
			t.Fatalf("attempt %d: got %v, want a lost client", i, err)
		}
	}

	wedge()
	err := n.send([]byte("msg"), true)
	if !errors.Is(err, errNotifyClientUnresponsive) {
		t.Fatalf("the breaker did not trip after %d timeouts: %v", notifyBreakerTrips, err)
	}

	// While tripped, an acknowledged message must fail without waiting: a
	// release that still paid the timeout would be no better off.
	wedge()
	start := time.Now()
	err = n.send([]byte("msg"), true)
	if !errors.Is(err, errNotifyClientUnresponsive) {
		t.Fatalf("after tripping, got %v, want the client reported unresponsive", err)
	}
	if waited := time.Since(start); waited > n.ackTimeout {
		t.Fatalf("waited %v for a send that should have failed immediately", waited)
	}

	// An unacknowledged message is unaffected: nothing waits on it, so there is
	// nothing for the breaker to protect.
	if err := n.send([]byte("msg"), false); err != nil {
		t.Fatalf("unacknowledged send failed while the breaker was tripped: %v", err)
	}
}

// Two notifications written back to back share one read on a stream socket.
// The name used to carry no length, so the reader could only take "the rest of
// what arrived" as the name — which swallowed the message behind it and left
// every later header being read from the middle of one. Nothing recovered from
// that: acknowledged messages stopped being recognised, the release waiting on
// one timed out, and the connection was dropped for good, which switched the
// kernel's page cache off for the life of the mount.
//
// The reader below is the C one in pkg/fuse/fuse.c, in the only terms that
// matter: header, declared length, exactly that many bytes.
func TestNotifyMessagesAreSelfDelimiting(t *testing.T) {
	type msg struct {
		typ  uint32
		ino  uint64
		name string
	}
	want := []msg{
		{notifyInvalEntry, 42, "first-name"},
		{notifyInvalEntry, 42, "a-much-longer-second-name"},
		{notifyInvalInode, 7, ""},
		{notifyInvalEntry, 1, strings.Repeat("x", notifyMaxName)},
	}

	var stream []byte
	for _, m := range want {
		stream = append(stream, notifyMsg(m.typ, m.ino, m.name)...)
	}

	var got []msg
	for len(stream) > 0 {
		if len(stream) < notifyHeaderLen {
			t.Fatalf("%d bytes left over, less than one header", len(stream))
		}
		nlen := binary.BigEndian.Uint32(stream[12:16])
		if nlen > notifyMaxName {
			t.Fatalf("name length %d exceeds the reader's buffer, so the stream is out of step", nlen)
		}
		if uint32(len(stream)) < notifyHeaderLen+nlen {
			t.Fatalf("message declares %d name bytes but only %d remain", nlen, len(stream)-notifyHeaderLen)
		}
		got = append(got, msg{
			typ:  binary.BigEndian.Uint32(stream[0:4]),
			ino:  binary.BigEndian.Uint64(stream[4:12]),
			name: string(stream[notifyHeaderLen : notifyHeaderLen+nlen]),
		})
		stream = stream[notifyHeaderLen+nlen:]
	}

	if len(got) != len(want) {
		t.Fatalf("recovered %d messages from the stream, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("message %d: got {%d %d %q}, want {%d %d %q}",
				i, got[i].typ, got[i].ino, got[i].name, want[i].typ, want[i].ino, want[i].name)
		}
	}
}

// The dirent watch is the only thing that invalidates a cached name, a cached
// absence, or a cached directory listing on this node. etcd ends a watch for
// reasons that have nothing to do with this process stopping — a compaction
// past the watched revision, most often — so a drain that stops at the first
// closed channel leaves the daemon serving from caches nothing can ever
// invalidate again. It must re-open instead.
func TestDirentWatchIsReopenedWhenItEnds(t *testing.T) {
	s := &Service{log: config.NewLogger(0)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opens := make(chan int64, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runWatch(ctx, watcher{
			what:   "dirent",
			prefix: metadata.PrefixDirent,
			event:  func(*clientv3.Event) {},
			open: func(resume int64) clientv3.WatchChan {
				opens <- resume
				ch := make(chan clientv3.WatchResponse, 1)
				// One response, so the loop has a revision to resume from, then
				// a watch that has ended.
				ch <- clientv3.WatchResponse{Header: &pb.ResponseHeader{Revision: 41}}
				close(ch)
				return ch
			},
		})
	}()

	// Two opens is the whole property: the first is the initial watch, the
	// second only happens because the loop noticed the first had ended — and it
	// resumes from after the last revision it saw rather than from current, so
	// the changes in the gap are replayed instead of lost.
	wantResume := []int64{0, 42}
	for i, want := range wantResume {
		select {
		case got := <-opens:
			if got != want {
				t.Errorf("open %d resumed from revision %d, want %d", i, got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("the watch was opened %d times; it stopped after the channel closed", i)
		}
	}

	// And it stops when the daemon does, rather than looping past shutdown.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the watch loop outlived its context")
	}
}

// A watch etcd compacted past cannot be resumed: the changes it missed are gone
// from the history, so the caches it keeps fresh are stale and the loop has to
// say so rather than quietly carry on from current.
func TestCompactedWatchReportsTheGapAndRestartsFromCurrent(t *testing.T) {
	s := &Service{log: config.NewLogger(0)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opens := make(chan int64, 4)
	gaps := make(chan struct{}, 4)
	go s.runWatch(ctx, watcher{
		what:   "dirent",
		prefix: metadata.PrefixDirent,
		event:  func(*clientv3.Event) {},
		gap:    func() { gaps <- struct{}{} },
		open: func(resume int64) clientv3.WatchChan {
			opens <- resume
			ch := make(chan clientv3.WatchResponse, 2)
			ch <- clientv3.WatchResponse{Header: &pb.ResponseHeader{Revision: 41}}
			ch <- clientv3.WatchResponse{Canceled: true, CompactRevision: 100}
			close(ch)
			return ch
		},
	})

	if got := <-opens; got != 0 {
		t.Errorf("the first open resumed from %d, want 0 (from current)", got)
	}
	select {
	case <-gaps:
	case <-time.After(2 * time.Second):
		t.Fatal("a compacted watch was not reported as a gap, so stale caches go uncorrected")
	}
	// From current, not from the revision that was compacted away: resuming
	// there again would be cancelled again, forever.
	if got := <-opens; got != 0 {
		t.Errorf("the reopened watch resumed from %d, want 0: that revision is compacted away", got)
	}
}

// An inode this node holds the lock key for cannot have been written by a peer,
// so the change is this node's own and the kernel already has it from the reply
// that made it. Invalidating those would undo the attribute cache on every
// create, which is the workload the longer timeout exists for.
func TestAttrInvalidationSkipsInodesThisNodeHolds(t *testing.T) {
	s := &Service{log: config.NewLogger(0), notifyServer: &notifyServer{}}
	s.locks = newLockMap(func(es []*lockEntry, _ string) []*lockEntry { return es })

	client, daemon := net.Pipe()
	defer func() { _ = client.Close() }()
	s.notifyServer.set(daemon)

	const held, peers = uint64(7), uint64(8)
	e := s.locks.entryFor(held)
	e.holder, e.mode = "lease-1", metadata.LockExclusive

	// Held here: nothing is sent, so nothing is read on the other end.
	go s.inodeChanged(&clientv3.Event{Kv: &mvccpb.KeyValue{Key: []byte(metadata.InodeKey(held))}})
	if err := client.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	var b [notifyHeaderLen]byte
	if n, err := io.ReadFull(client, b[:]); err == nil {
		t.Fatalf("an inode this node holds was invalidated (%d bytes)", n)
	}

	// Not held here: the change can only be a peer's, and the kernel's copy has
	// to go.
	go s.inodeChanged(&clientv3.Event{Kv: &mvccpb.KeyValue{Key: []byte(metadata.InodeKey(peers))}})
	if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := io.ReadFull(client, b[:]); err != nil {
		t.Fatalf("a peer's inode change was not invalidated: %v", err)
	}
	if typ := binary.BigEndian.Uint32(b[0:4]); typ != notifyInvalAttr {
		t.Errorf("message type %d, want INVAL_ATTR (%d)", typ, notifyInvalAttr)
	}
	if ino := binary.BigEndian.Uint64(b[4:12]); ino != peers {
		t.Errorf("invalidated inode %d, want %d", ino, peers)
	}
}

// A timeout the wire cannot express must round down, never up: FUSE carries
// whole seconds, and rounding up would hand the kernel a longer licence to
// answer from its cache than the operator asked for.
func TestCacheTimeoutRoundsDown(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want uint32
	}{
		{"unset takes the default", 0, uint32(config.DefaultEntryTimeout / time.Second)},
		{"whole seconds pass through", 5 * time.Second, 5},
		{"sub-second becomes no caching", 900 * time.Millisecond, 0},
		{"a fraction is truncated", 2500 * time.Millisecond, 2},
		{"negative is no caching", -time.Second, 0},
	}
	for _, c := range cases {
		if got := timeoutSecs(c.in, config.DefaultEntryTimeout); got != c.want {
			t.Errorf("%s: timeoutSecs(%s) = %d, want %d", c.name, c.in, got, c.want)
		}
	}
}
