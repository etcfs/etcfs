//go:build integration
// +build integration

package ipc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/etcfs/etcfs/internal/config"
	"github.com/etcfs/etcfs/internal/history"
	"github.com/etcfs/etcfs/pkg/blockio"
	"github.com/etcfs/etcfs/pkg/fencing"
	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/test/etcdtest"
	"github.com/etcfs/etcfs/test/verify"
)

// historyDeviceBytes is one arena plus room to grow, sparse on disk — the same
// size the datapath integration tests use.
const historyDeviceBytes = 2 << 30

// Two nodes contending over one directory, recorded through the daemon's own
// dispatch path and checked for linearizability.
//
// The unit tests in test/verify check the checker; this one checks the
// filesystem. It lives here rather than beside them because driving a Service
// means going through dispatch, and exporting an entry point into production
// code so a test can reach it would be a worse trade than this import.
func TestIntegration_RecordedNamespaceHistoryIsLinearizable(t *testing.T) {
	cli := etcdtest.Client(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "history.jsonl")

	store := metadata.NewStore(cli, "n1")
	if _, err := store.CreateInode(ctx, metadata.RootIno, metadata.ModeDir|0755, 0, 0); err != nil {
		t.Fatalf("seed root: %v", err)
	}

	services := make([]*Service, 0, 2)
	for _, node := range []string{"n1", "n2"} {
		st := metadata.NewStore(cli, node)
		membership := metadata.NewMembership(cli, node, "verify", 10*time.Second)
		svc := NewService(st, membership, fencing.NewWatchdog(membership, 10*time.Second),
			config.NewLogger(0), Options{FlushInterval: DefaultFlushInterval})
		if err := svc.InitGeneration(ctx); err != nil {
			t.Fatalf("init generation for %s: %v", node, err)
		}
		svc.InstallStoreGuard()

		rec, err := history.NewRecorder(path, node)
		if err != nil {
			t.Fatalf("open recorder for %s: %v", node, err)
		}
		t.Cleanup(func() { _ = rec.Close() })
		svc.history = rec
		services = append(services, svc)
	}

	// A handful of names, contended by both nodes: the interleavings a
	// single-node test cannot produce are the ones linearizability is about.
	const rounds = 10
	var wg sync.WaitGroup
	for i, svc := range services {
		wg.Add(1)
		go func(i int, svc *Service) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				name := fmt.Sprintf("contended-%d", round%3)
				ino := uint64(3000 + i*1000 + round)
				_, _ = svc.observedDispatch(ipcOpCreate, createPayload(metadata.RootIno, name, ino))
				_, _ = svc.observedDispatch(ipcOpLookup, lookupPayload(metadata.RootIno, name))
				_, _ = svc.observedDispatch(ipcOpUnlink, lookupPayload(metadata.RootIno, name))
			}
		}(i, svc)
	}
	wg.Wait()

	entries, err := history.Load(path)
	if err != nil {
		t.Fatalf("load history: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no operations were recorded")
	}
	ops, err := verify.DecodeNamespace(entries)
	if err != nil {
		t.Fatalf("decode history: %v", err)
	}
	t.Logf("checking %d recorded namespace operations from %d entries", len(ops), len(entries))

	res := verify.Check(verify.NamespaceModel, verify.Operations(ops), verify.AllLinearizable, 0, 60*time.Second)
	switch res {
	case porcupine.Ok:
	case porcupine.Unknown:
		t.Fatalf("the checker did not finish in time on %d operations", len(ops))
	default:
		dump, _ := os.ReadFile(path)
		t.Fatalf("the recorded history is not linearizable:\n%s", dump)
	}
}

// A checker that passes on every history it is given proves nothing, so the
// recorded history is also perturbed in the one way a real lost update would
// show up — a name reported created twice with no unlink between — and the
// check has to reject it.
func TestIntegration_TamperedHistoryIsRejected(t *testing.T) {
	cli := etcdtest.Client(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "history.jsonl")

	st := metadata.NewStore(cli, "n1")
	if _, err := st.CreateInode(ctx, metadata.RootIno, metadata.ModeDir|0755, 0, 0); err != nil {
		t.Fatalf("seed root: %v", err)
	}
	membership := metadata.NewMembership(cli, "n1", "verify", 10*time.Second)
	svc := NewService(st, membership, fencing.NewWatchdog(membership, 10*time.Second), config.NewLogger(0),
		Options{FlushInterval: DefaultFlushInterval})
	if err := svc.InitGeneration(ctx); err != nil {
		t.Fatalf("init generation: %v", err)
	}
	svc.InstallStoreGuard()
	rec, err := history.NewRecorder(path, "n1")
	if err != nil {
		t.Fatalf("open recorder: %v", err)
	}
	defer func() { _ = rec.Close() }()
	svc.history = rec

	_, _ = svc.observedDispatch(ipcOpCreate, createPayload(metadata.RootIno, "dup", 0))
	_, _ = svc.observedDispatch(ipcOpLookup, lookupPayload(metadata.RootIno, "dup"))

	entries, err := history.Load(path)
	if err != nil {
		t.Fatalf("load history: %v", err)
	}
	ops, err := verify.DecodeNamespace(entries)
	if err != nil {
		t.Fatalf("decode history: %v", err)
	}
	if len(ops) < 2 {
		t.Fatalf("recorded %d operations, want at least 2", len(ops))
	}

	// The second create is the sabotage: the same name created again, after
	// the first succeeded, with nothing removing it in between.
	dup := ops[0]
	dup.Ino++
	dup.Call = ops[len(ops)-1].Ret + 10
	dup.Ret = dup.Call + 10
	tampered := append(append([]verify.Op{}, ops...), dup)

	res := verify.Check(verify.NamespaceModel, verify.Operations(tampered), verify.AllLinearizable, 0, 60*time.Second)
	if res == porcupine.Ok {
		t.Fatal("a history with a duplicated create was accepted; the check has no teeth")
	}
}

// createPayload is a CREATE request: the inode number is allocated by the
// handler, so the one a caller has in mind is not part of it.
func createPayload(parent uint64, name string, _ uint64) []byte {
	var b buf
	b.w64(parent)
	b.w32(uint32(len(name)))
	b.b = append(b.b, name...)
	b.w32(0644) // mode
	b.w32(0)    // flags
	b.w32(0)    // umask
	b.w32(0)    // uid
	b.w32(0)    // gid
	return b.b
}

func lookupPayload(parent uint64, name string) []byte {
	var b buf
	b.w64(parent)
	b.w32(uint32(len(name)))
	b.b = append(b.b, name...)
	return b.b
}

// Two nodes, one shared etcd, one shared block device, contending on writes
// and reads to the same inodes and taking real locks against each other.
// The recorded history is checked against all three models this session
// added: does a read ever disagree with the writes that could have produced
// it, does the exclusive/shared lock ever admit two holders that should have
// excluded each other, and does any commit succeed after this node's own
// fencing generation says it should not.
func TestIntegration_RecordedDataPathHistoryIsConsistent(t *testing.T) {
	cli := etcdtest.Client(t)
	ctx := context.Background()
	historyPath := filepath.Join(t.TempDir(), "history.jsonl")
	devPath := filepath.Join(t.TempDir(), "device.img")

	f, err := os.Create(devPath)
	if err != nil {
		t.Fatalf("create device file: %v", err)
	}
	if err := f.Truncate(historyDeviceBytes); err != nil {
		t.Fatalf("size device file: %v", err)
	}
	_ = f.Close()

	if _, err := metadata.NewStore(cli, "n1").CreateInode(ctx, metadata.RootIno, metadata.ModeDir|0755, 0, 0); err != nil {
		t.Fatalf("seed root: %v", err)
	}

	const inos = 3
	names := make([]string, inos)
	for i := range names {
		names[i] = fmt.Sprintf("shared-%d", i)
		if _, err := metadata.NewStore(cli, "n1").AtomicCreateFile(ctx, metadata.RootIno, names[i],
			uint64(900+i), metadata.ModeFile|0644, 0, 0, metadata.CreateExtra{}); err != nil {
			t.Fatalf("seed inode %d: %v", i, err)
		}
	}

	services := make([]*Service, 0, 2)
	for _, node := range []string{"n1", "n2"} {
		dev, err := blockio.OpenBuffered(devPath)
		if err != nil {
			t.Fatalf("open device for %s: %v", node, err)
		}
		t.Cleanup(func() { _ = dev.Close() })

		st := metadata.NewStore(cli, node)
		membership := metadata.NewMembership(cli, node, "verify", 10*time.Second)
		svc := NewService(st, membership, fencing.NewWatchdog(membership, 10*time.Second), config.NewLogger(0),
			Options{FlushInterval: DefaultFlushInterval})
		if err := svc.InitGeneration(ctx); err != nil {
			t.Fatalf("init generation for %s: %v", node, err)
		}
		svc.InstallStoreGuard()
		svc.setBlockDevice(dev, false)

		rec, err := history.NewRecorder(historyPath, node)
		if err != nil {
			t.Fatalf("open recorder for %s: %v", node, err)
		}
		t.Cleanup(func() { _ = rec.Close() })
		svc.history = rec
		services = append(services, svc)
	}

	const rounds = 15
	var wg sync.WaitGroup
	for i, svc := range services {
		wg.Add(1)
		go func(i int, svc *Service) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				ino := uint64(900 + round%inos)
				data := []byte(fmt.Sprintf("node-%d-round-%d", i, round))
				_, _ = svc.observedDispatch(ipcOpWrite, writePayloadFor(ino, 0, data, 0))
				_, _ = svc.observedDispatch(ipcOpRead, readPayloadFor(ino, 0, 4096))
			}
		}(i, svc)
	}
	wg.Wait()

	// A write buffers its extents rather than committing them, so the guarded
	// commit the generation history is checked against belongs to the flush.
	// Publishing here rather than after every write keeps the rounds above
	// exercising the buffered path, which is what the extent history checks.
	for _, svc := range services {
		for round := 0; round < inos; round++ {
			var b buf
			b.w64(uint64(900 + round))
			_, _ = svc.observedDispatch(ipcOpFsync, b.b)
		}
	}

	entries, err := history.Load(historyPath)
	if err != nil {
		t.Fatalf("load history: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no operations were recorded")
	}

	extents, err := verify.DecodeExtents(entries)
	if err != nil {
		t.Fatalf("decode extents: %v", err)
	}
	t.Logf("checking %d extent operations", len(extents))
	// No crashed nodes: this test kills nothing, so a write that went missing
	// is a defect rather than a buffer a SIGKILL legitimately took with it.
	if res := verify.CheckExtents(extents, nil, 60*time.Second); res != porcupine.Ok {
		t.Fatalf("recorded extent history is not consistent (%v)", res)
	}

	locks, err := verify.DecodeLocks(entries)
	if err != nil {
		t.Fatalf("decode locks: %v", err)
	}
	t.Logf("checking %d lock events", len(locks))
	if len(locks) == 0 {
		t.Fatal("no lock events were recorded")
	}
	if res := verify.CheckLocks(locks, verify.DecodeStarts(entries), verify.DefaultLockLeaseTTL, 60*time.Second); res != porcupine.Ok {
		t.Fatalf("recorded lock history admits a mutual-exclusion violation (%v)", res)
	}

	guards, err := verify.DecodeGuardedCommits(entries)
	if err != nil {
		t.Fatalf("decode guarded commits: %v", err)
	}
	t.Logf("checking %d guarded commits", len(guards))
	if len(guards) == 0 {
		t.Fatal("no guarded commits were recorded")
	}
	if res := verify.CheckGenerations(guards, 60*time.Second); res != porcupine.Ok {
		t.Fatalf("recorded generation history admits a write after a fence (%v)", res)
	}
}

func writePayloadFor(ino, offset uint64, data []byte, uid uint32) []byte {
	var b buf
	b.w64(ino)
	b.w64(offset)
	b.w32(uint32(len(data)))
	b.b = append(b.b, data...)
	b.w32(uid)
	b.w32(0) // open flags: neither O_SYNC nor O_DSYNC
	return b.b
}

func readPayloadFor(ino, offset uint64, size uint32) []byte {
	var b buf
	b.w64(ino)
	b.w64(offset)
	b.w32(size)
	return b.b
}

func readdirPayloadFor(ino, offset uint64, size uint32) []byte {
	var b buf
	b.w64(ino)
	b.w64(offset)
	b.w32(size)
	return b.b
}

// The readdir decoder in test/verify is written against the wire format, not
// shared with the encoder that produces it, so the only thing that proves the
// two agree is decoding a response the real handler actually wrote. Both
// framings are exercised: READDIRPLUS appends a fixed attr block to every
// entry, and getting its width wrong turns every entry after the first into
// garbage without failing loudly.
func TestIntegration_ReaddirDecodesWhatTheHandlerEncoded(t *testing.T) {
	cli := etcdtest.Client(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "history.jsonl")

	store := metadata.NewStore(cli, "n1")
	if _, err := store.CreateInode(ctx, metadata.RootIno, metadata.ModeDir|0755, 0, 0); err != nil {
		t.Fatalf("seed root: %v", err)
	}

	membership := metadata.NewMembership(cli, "n1", "verify", 10*time.Second)
	svc := NewService(store, membership, fencing.NewWatchdog(membership, 10*time.Second), config.NewLogger(0),
		Options{FlushInterval: DefaultFlushInterval})
	if err := svc.InitGeneration(ctx); err != nil {
		t.Fatalf("init generation: %v", err)
	}
	svc.InstallStoreGuard()
	rec, err := history.NewRecorder(path, "n1")
	if err != nil {
		t.Fatalf("recorder: %v", err)
	}
	defer func() { _ = rec.Close() }()
	svc.history = rec

	want := map[string]bool{"alpha": true, "beta": true, "gamma": true}
	for name := range want {
		if _, _ = svc.observedDispatch(ipcOpCreate, createPayload(metadata.RootIno, name, 0)); false {
			t.Fatal("unreachable")
		}
	}

	// A full listing, then the same through READDIRPLUS.
	_, _ = svc.observedDispatch(ipcOpReaddir, readdirPayloadFor(metadata.RootIno, 0, 4096))
	_, _ = svc.observedDispatch(ipcOpReadDirPlus, readdirPayloadFor(metadata.RootIno, 0, 4096))

	entries, err := history.Load(path)
	if err != nil {
		t.Fatalf("load history: %v", err)
	}
	ops, err := verify.DecodeNamespace(entries)
	if err != nil {
		t.Fatalf("decode history: %v", err)
	}

	listings := 0
	for _, op := range ops {
		if op.Kind != verify.KindReaddir {
			continue
		}
		listings++
		got := map[string]bool{}
		for _, e := range op.Entries {
			got[e.Name] = true
			if e.Ino == 0 {
				t.Errorf("entry %q decoded with inode 0, so the entry framing is misaligned", e.Name)
			}
		}
		for name := range want {
			if !got[name] {
				t.Errorf("listing is missing %q; decoded %v", name, got)
			}
		}
		if len(got) != len(want) {
			t.Errorf("decoded %d entries, want %d: %v", len(got), len(want), got)
		}
	}
	if listings != 2 {
		t.Fatalf("decoded %d readdir operations, want 2 (readdir and readdirplus)", listings)
	}

	if res := verify.Check(verify.NamespaceModel, verify.Operations(ops),
		verify.AllLinearizable, 0, 60*time.Second); res != porcupine.Ok {
		t.Fatalf("a history containing real listings was rejected (%v)", res)
	}
}

// The two events caching a lock made unobservable — when the etcd key is taken
// and when it is given up — are what the mutual-exclusion and page-cache
// checkers actually run over, so both new ways of moving that key have to show
// up in them: a key taken by the transaction that creates a file, and a batch
// of keys given up by one eviction sweep.
//
// A create that recorded no acquisition would leave the eviction's release
// dangling, which reads as a lock released twice; a batched release timed from
// before its invalidations would read as a key yielded with the kernel's pages
// still cached. Neither is visible without decoding the recorded history, which
// is what this does.
func TestIntegration_CreatedAndBatchReleasedLockKeysAreCheckable(t *testing.T) {
	cli := etcdtest.Client(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "history.jsonl")

	store := metadata.NewStore(cli, "n1")
	if _, err := store.CreateInode(ctx, metadata.RootIno, metadata.ModeDir|0755, 0, 0); err != nil {
		t.Fatalf("seed root: %v", err)
	}

	membership := metadata.NewMembership(cli, "n1", "verify", 10*time.Second)
	svc := NewService(store, membership, fencing.NewWatchdog(membership, 10*time.Second),
		config.NewLogger(0), Options{FlushInterval: DefaultFlushInterval})
	if err := svc.InitGeneration(ctx); err != nil {
		t.Fatalf("init generation: %v", err)
	}
	svc.InstallStoreGuard()
	rec, err := history.NewRecorder(path, "n1")
	if err != nil {
		t.Fatalf("recorder: %v", err)
	}
	defer func() { _ = rec.Close() }()
	svc.history = rec

	const files = 40
	for i := 0; i < files; i++ {
		_, _ = svc.observedDispatch(ipcOpCreate,
			createPayload(metadata.RootIno, fmt.Sprintf("created-%d", i), 0))
	}

	entries := svc.locks.all()
	if len(entries) < files {
		t.Fatalf("%d lock entries cached after %d creates; the create did not take the lock",
			len(entries), files)
	}
	held := 0
	for _, e := range entries {
		if e.holder != "" {
			held++
		}
	}
	if held < files {
		t.Fatalf("%d of %d created inodes hold a lock key", held, files)
	}

	// The eviction sweep's own path: every victim's key given up by one
	// transaction, with the release recorded per inode.
	for _, e := range entries {
		e.rw.Lock()
	}
	released := svc.dropCachedLocks(entries, "eviction")
	for _, e := range entries {
		e.rw.Unlock()
	}
	if len(released) != len(entries) {
		t.Fatalf("%d of %d locks yielded by the batch", len(released), len(entries))
	}

	recorded, err := history.Load(path)
	if err != nil {
		t.Fatalf("load history: %v", err)
	}
	keys, err := verify.DecodeLockKeys(recorded)
	if err != nil {
		t.Fatalf("decode lock keys: %v", err)
	}
	if len(keys) < 2*files {
		t.Fatalf("%d lock-key events for %d creates and their releases, want at least %d",
			len(keys), files, 2*files)
	}
	if res := verify.CheckLocks(keys, verify.DecodeStarts(recorded),
		verify.DefaultLockLeaseTTL, 60*time.Second); res != porcupine.Ok {
		t.Fatalf("the cached lock keys admit a mutual-exclusion violation (%v)", res)
	}

	invals, err := verify.DecodePageInvals(recorded)
	if err != nil {
		t.Fatalf("decode page invalidations: %v", err)
	}
	if violations := verify.CheckPageCache(keys, invals); len(violations) > 0 {
		t.Fatalf("a lock key was yielded without its inode's pages going first: %v", violations[0])
	}
}
