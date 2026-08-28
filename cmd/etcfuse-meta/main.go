/*
Package main is the EtcFS metadata backend binary.

It serves a binary protocol over a Unix domain socket, receiving FUSE
operation requests from the C daemon (etcfuse) and translating them into etcd
transactions, leases, and watches.

The binary also runs:
  - The membership lease heartbeat (keepalive to etcd)
  - The self-fencing watchdog (lease health → stop if expired)
  - The etcd watch that drives cache invalidation on the C side

Usage:

	etcfuse-meta \
	  --listen=unix:///tmp/etcfuse.sock \
	  --etcd-endpoints=https://10.0.0.1:2379,https://10.0.0.2:2379,https://10.0.0.3:2379 \
	  --etcd-cert=/etc/etcfuse/client.crt \
	  --etcd-key=/etc/etcfuse/client.key \
	  --etcd-ca=/etc/etcfuse/ca.crt \
	  --node-id=etcfuse-node-1 \
	  --lease-ttl=10s
*/
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/internal/config"
	"github.com/etcfs/etcfs/internal/history"
	"github.com/etcfs/etcfs/internal/ipc"
	"github.com/etcfs/etcfs/pkg/blockio"
	"github.com/etcfs/etcfs/pkg/fencing"
	"github.com/etcfs/etcfs/pkg/fsck"
	"github.com/etcfs/etcfs/pkg/fsinfo"
	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/metrics"
	"github.com/etcfs/etcfs/pkg/scrub"
)

// errSelfFenced reports that the daemon shut down because its own watchdog
// fenced it, rather than because it was asked to stop.  It reaches the exit
// status and nothing else: an operator and a supervisor both need to tell the
// two apart, and the log line alone does not do that.
var errSelfFenced = errors.New("self-fenced")

func main() {
	cfg := config.Parse()

	if cfg.ShowVersion {
		fmt.Println("etcfuse-meta", config.Version)
		return
	}

	log := config.NewLogger(cfg.LogLevel)

	var err error
	switch {
	case cfg.RunFsck:
		err = runFsck(context.Background(), cfg)
	case cfg.RunInfo:
		err = runInfo(context.Background(), cfg)
	default:
		err = run(context.Background(), cfg, log)
	}

	switch {
	case errors.Is(err, errSelfFenced):
		os.Exit(fencing.SelfFenceExitCode)
	case err != nil:
		log.Error("etcfuse-meta failed", "error", err)
		os.Exit(1)
	}
}

// connect dials etcd with the failover settings the daemon and the one-shot
// modes both need.
func connect(cfg *config.Config) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:            cfg.EtcdEndpoints,
		DialTimeout:          3 * time.Second,
		DialKeepAliveTime:    1 * time.Second,
		DialKeepAliveTimeout: 1 * time.Second,
		PermitWithoutStream:  true,
		AutoSyncInterval:     30 * time.Second,
		TLS:                  cfg.EtcdTLSConfig(),
	})
}

// connectLocal dials only the etcd member colocated with this node, for the
// reads the store serves through it.
//
// Endpoint auto-sync is deliberately off: it would discover the whole cluster
// and hand the balancer the other members back, which is exactly the pinning
// this client exists to keep.
func connectLocal(cfg *config.Config) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:            []string{cfg.EtcdLocalEndpoint},
		DialTimeout:          3 * time.Second,
		DialKeepAliveTime:    1 * time.Second,
		DialKeepAliveTimeout: 1 * time.Second,
		PermitWithoutStream:  true,
		TLS:                  cfg.EtcdTLSConfig(),
	})
}

func runFsck(ctx context.Context, cfg *config.Config) error {
	cli, err := connect(cfg)
	if err != nil {
		return fmt.Errorf("connect to etcd: %w", err)
	}
	defer func() { _ = cli.Close() }()

	chk := fsck.New(metadata.NewStore(cli, cfg.NodeID))
	findings := chk.Run(ctx)
	fmt.Printf("fsck: %d errors, %d warnings\n", chk.ErrorCount(), chk.WarningCount())
	for _, f := range findings {
		fmt.Printf("  [%s] %s\n", f.Level, f.Message)
	}
	return nil
}

func runInfo(ctx context.Context, cfg *config.Config) error {
	cli, err := connect(cfg)
	if err != nil {
		return fmt.Errorf("connect to etcd: %w", err)
	}
	defer func() { _ = cli.Close() }()

	info, err := fsinfo.Collect(ctx, metadata.NewStore(cli, cfg.NodeID))
	if err != nil {
		return fmt.Errorf("collect filesystem info: %w", err)
	}
	fmt.Println(info.String())
	return nil
}

// run starts the daemon's subsystems and serves until the process is signalled
// or self-fences, then shuts down in the order the correctness argument needs.
//
// It returns rather than exiting so that every deferred close runs, and so the
// shutdown ordering below is reachable from a test.  A startup failure is
// returned for the same reason: os.Exit from inside here would skip the block
// device close and the etcd client close above it.
func run(ctx context.Context, cfg *config.Config, log *config.Logger) error {
	log.Info("etcfuse-meta starting", "version", config.Version)
	log.Info("listening", "socket", cfg.ListenAddr)
	log.Info("etcd", "endpoints", cfg.EtcdEndpoints)
	log.Info("node", "id", cfg.NodeID)
	log.Info("lease", "ttl", cfg.LeaseTTL)

	// Resolve before anything opens the device — both the data path and the
	// NVMe fencer must agree on which device this volume is right now, and a
	// stale path from a previous attachment would point at the wrong disk.
	// Fatal rather than falling back to --block-device: the fallback is what
	// silently opens someone else's volume.
	if cfg.VolumeID != "" {
		path, err := blockio.ResolvePath(cfg.VolumeID)
		if err != nil {
			return fmt.Errorf("resolve volume %s to a block device: %w", cfg.VolumeID, err)
		}
		log.Info("volume resolved", "volume_id", cfg.VolumeID, "path", path)
		cfg.BlockDevice = path
	}

	etcdCli, err := connect(cfg)
	if err != nil {
		return fmt.Errorf("connect to etcd: %w", err)
	}
	defer func() { _ = etcdCli.Close() }()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Membership: register this node with a lease-backed key
	membership := metadata.NewMembership(etcdCli, cfg.NodeID, cfg.ClusterName, cfg.LeaseTTL)
	// Recorded in the membership key so a peer can detach the shared volume
	// from this node after its lease expires — at which point the key is gone
	// and this node can no longer be asked for it.
	membership.SetInstanceID(cfg.EC2InstanceID)
	membership.SetLogger(log)

	// Metadata store: wraps etcd client with schema-aware helpers
	store := metadata.NewStore(etcdCli, cfg.NodeID)

	if cfg.EtcdLocalEndpoint != "" {
		localCli, lerr := connectLocal(cfg)
		if lerr != nil {
			return fmt.Errorf("connect to local etcd member %s: %w", cfg.EtcdLocalEndpoint, lerr)
		}
		defer func() { _ = localCli.Close() }()
		store.SetLocalClient(localCli)
		log.Info("etcd reads pinned to local member", "endpoint", cfg.EtcdLocalEndpoint)
	}

	// Self-fencing watchdog
	watchdog := fencing.NewWatchdog(membership, cfg.LeaseTTL)

	var historyRecorder *history.Recorder
	if cfg.HistoryLog != "" {
		rec, herr := history.NewRecorder(cfg.HistoryLog, cfg.NodeID)
		if herr != nil {
			return fmt.Errorf("open history log: %w", herr)
		}
		defer func() { _ = rec.Close() }()
		historyRecorder = rec
		log.Info("recording an operation history", "path", cfg.HistoryLog)
	}

	// IPC service: handles FUSE op requests from the C daemon
	// The block device is opened before the service, because a Service is
	// configured once, at construction, rather than by a sequence of setters
	// whose ordering nothing enforces.
	var dev *blockio.Device
	if cfg.BlockDevice != "" {
		openDevice := blockio.Open
		if cfg.AllowBufferedIO {
			openDevice = blockio.OpenBuffered
		}
		opened, err := openDevice(cfg.BlockDevice)
		if err != nil {
			return fmt.Errorf("open block device %s: %w", cfg.BlockDevice, err)
		}
		if !opened.IsDirect() {
			log.Warn("block device opened WITHOUT O_DIRECT: writes are served from this node's "+
				"page cache and are not proven visible to other attachers; safe only on an "+
				"unshared device", "path", cfg.BlockDevice)
		}
		defer func() { _ = opened.Close() }()
		dev = opened
	}

	svc := ipc.NewService(store, membership, watchdog, log, ipc.Options{
		FlushInterval: cfg.MetadataFlushInterval,
		DataCache:     cfg.WriteDataCache && cfg.MetadataFlushInterval > 0,
		PageCache:     cfg.PageCache,
		EntryTimeout:  cfg.EntryTimeout,
		AttrTimeout:   cfg.AttrTimeout,
		ReadOnly:      cfg.ReadOnly,
		Device:        dev,
		WriteBarriers: cfg.WriteBarriers,
		History:       historyRecorder,
	})

	// Establish this node's fencing generation before serving any request, then
	// install it as the store-wide guard so namespace mutations are covered too,
	// not only extent writes.  A later bump by the fencing controller stops this
	// node from mutating anything.
	//
	// Fatal on failure: without the generation, every guarded transaction fails
	// closed, so the daemon could not serve writes anyway — exiting reports the
	// real cause instead of an unexplained EIO on every mutation.
	if err := svc.InitGeneration(ctx); err != nil {
		return fmt.Errorf("initialise fencing generation: %w", err)
	}
	svc.InstallStoreGuard()

	// A previous run may have died holding an unlinked file open, leaving a
	// record nothing names and no descriptor can reach.
	svc.ReclaimOrphans(ctx)

	if cfg.ReadOnly {
		log.Info("mounted read-only: every mutating FUSE operation will be rejected with EROFS")
	}

	if dev != nil {
		_ = svc.ReconstructArenas(ctx)
		log.Info("block device opened", "path", cfg.BlockDevice,
			"sector_size", dev.SectorSize(), "total_size", dev.TotalSize(),
			"direct_io", dev.IsDirect())
	}

	go membership.Run(ctx)
	go watchdog.Run(ctx)

	controller, err := newFencingController(ctx, cfg, store, membership, log)
	if err != nil {
		return err
	}
	go controller.Run(ctx)

	// POSIX fcntl/flock locks are node-local: neither daemon implements
	// GETLK/SETLK, so the kernel enforces them within this node and nothing
	// enforces them between nodes.  Warned at startup because a workload
	// relying on cross-node locking gets no other signal — the calls succeed,
	// they simply exclude nothing on other nodes.
	log.Warn("POSIX file locks (fcntl/flock) are enforced within this node only, NOT across the " +
		"cluster; see https://github.com/etcfs/etcfs-docs/blob/main/docs/architecture/metadata/posix-lock-operations.md")

	// Start background scrubber (checks every 30s)
	scrubber := scrub.New(store, cfg.NodeID, 30*time.Second, log)
	// Without this the scrubber deletes an unlinked file's dangling extent
	// records but never returns their blocks, so disk space leaks on every
	// deletion.
	scrubber.SetReclaimer(svc.Reclaimer())
	// A scrub pass takes no inode lock, so without this it can free blocks
	// under a read on this node that has already resolved the extent naming
	// them.  The check is a lookup in this node's own lock cache.
	scrubber.SetInodeLocks(svc)
	// The range check compares against the real device rather than a hardcoded
	// ceiling; without a device attached it is skipped.
	scrubber.SetDeviceSize(svc.Allocator().DeviceSize())
	go scrubber.Run(ctx)

	// Return arenas this node has emptied to the global free pool.  Without
	// this a node keeps every arena it ever acquired, so space freed by deletes
	// and truncates stays reserved to this node and no peer can ever use it.
	go svc.Allocator().ReapEmptyArenas(ctx, time.Minute)

	if cfg.MetricsAddr != "" {
		go func() { _ = metrics.StartServer(cfg.MetricsAddr, readiness(svc, membership), cfg.Pprof) }()
		log.Info("metrics server listening", "addr", cfg.MetricsAddr)
	}

	selfFenced := stopOnSignalOrFence(ctx, cancel, svc, watchdog, log)

	log.Info("binary IPC server starting")
	svc.StartNotificationServer(ctx)
	// The other half of what the kernel caches.  Names are invalidated by the
	// dirent watch above; attributes had nothing at all watching them, which is
	// why their timeout could not be raised past a second.
	svc.StartAttrInvalidation(ctx)
	svc.StartLockRevocation(ctx)
	svc.StartSessionWatch(ctx)
	svc.StartFlusher(ctx)
	// A directory's timestamp is owed a commit by every entry added to or
	// removed from it, and that commit is not foldable into the namespace
	// transaction itself.  Coalescing them under the same interval that governs
	// extent publication is what stops an unpacking tar paying two Raft commits
	// per file; zero keeps every timestamp written through, as it keeps every
	// extent written through.
	store.StartDirTouchBatching(ctx, cfg.MetadataFlushInterval, log)
	if cfg.MetadataFlushInterval > 0 {
		log.Info("deferring extent publication; a crash loses writes not yet flushed or fsynced",
			"flush_interval", cfg.MetadataFlushInterval, "write_data_cache", cfg.WriteDataCache)
	}
	go func() {
		if err := ipc.StartNotifyServer(svc, cfg.NotifyAddr); err != nil {
			// Not fatal: the mount works without it, but every node's caches
			// then rely on their timeouts alone, so it must not be silent.
			log.Error("cache-invalidation notify server stopped; peers will not be "+
				"invalidated until it is restarted", "path", cfg.NotifyAddr, "error", err)
		}
	}()
	serveErr := ipc.StartSocketServer(ctx, svc, cfg.ListenAddr, log)

	leaveCluster(cfg, svc, store, membership, log)

	log.Info("etcfuse-meta stopped")
	if serveErr != nil {
		return fmt.Errorf("IPC server failed: %w", serveErr)
	}
	select {
	case <-selfFenced:
		return errSelfFenced
	default:
		return nil
	}
}

// readiness reports whether this node can serve I/O, for /readyz.
//
// Three conditions, each of which makes the daemon useless to route work to
// while leaving the process perfectly alive: the IPC socket is not being
// served yet, the membership lease has lapsed (so peers may already be fencing
// this node), or self-fencing has triggered (so every write will be rejected).
func readiness(svc *ipc.Service, membership *metadata.Membership) func() error {
	return func() error {
		switch {
		case !svc.Serving():
			return errors.New("the IPC socket is not serving yet")
		case !membership.IsAlive():
			return errors.New("the membership lease is not live")
		case svc.IsFenced():
			return errors.New("this node has fenced itself")
		}
		return nil
	}
}

// newFencingController selects the external fencing backend.
//
// A failure is returned rather than degraded in either branch: an operator who
// asked for device-enforced or dual-confirmed fencing and quietly got the
// weaker single-signal guarantee has a gap that only shows up as corruption
// during an incident.
func newFencingController(ctx context.Context, cfg *config.Config, store *metadata.Store,
	membership *metadata.Membership, log *config.Logger) (*fencing.Controller, error) {
	controller := fencing.NewController(store, membership, log)

	switch {
	case cfg.NVMeReservations:
		fencer, err := fencing.NewNVMeFencer(cfg.BlockDevice, cfg.NodeID)
		if err != nil {
			return nil, fmt.Errorf("initialise NVMe reservation fencing on %s: %w",
				cfg.BlockDevice, err)
		}
		controller.SetFencer(fencer)
		log.Info("external fencing: device-enforced (NVMe reservation preempt)",
			"device", cfg.BlockDevice)
	case cfg.EBSVolumeID != "":
		detacher, err := fencing.NewEBSDetacher(ctx, cfg.EBSVolumeID)
		if err != nil {
			return nil, fmt.Errorf("initialise EBS fencing on %s: %w", cfg.EBSVolumeID, err)
		}
		controller.SetFencer(detacher)
		log.Info("external fencing: dual-confirmed (EBS detach + poll)", "volume", cfg.EBSVolumeID)
	default:
		log.Warn("external fencing: single-signal (generation bump on lease expiry only); " +
			"pass --ebs-volume-id to detach the shared volume before bumping")
	}
	return controller, nil
}

// stopOnSignalOrFence cancels ctx on SIGINT/SIGTERM or on a self-fence, and
// returns a channel that is closed only in the self-fence case.
//
// A self-fence shuts the node down through the same path a signal does, so the
// arena release in leaveCluster still runs.  The watchdog used to call os.Exit
// itself, which skipped it: a self-fenced node's arenas leaked, permanently in
// single-signal mode, where no fencing controller reclaims them either.
func stopOnSignalOrFence(ctx context.Context, cancel context.CancelFunc,
	svc *ipc.Service, watchdog *fencing.Watchdog, log *config.Logger) <-chan struct{} {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fenced := make(chan struct{})
	go func() {
		defer cancel()
		select {
		case <-ctx.Done():
		case sig := <-sigCh:
			log.Info("received signal, shutting down", "signal", sig)
		case <-watchdog.Fenced():
			log.Error("self-fenced, shutting down")
			// Ahead of the orderly shutdown below, because a fenced node's
			// cached lock keys are the one piece of shared state its own
			// generation guard does not neutralise: its commits are already
			// being rejected, but a lock it is still holding blocks a healthy
			// peer for as long as it takes this process to exit.
			svc.ReleaseCachedLocks()
			close(fenced)
		}
	}()
	return fenced
}

// leaveCluster gives back everything this node holds, once it is serving
// nothing, and announces that it went on purpose.
//
// A departing node is its own proof of quiescence — the IPC server has stopped,
// so no further write can be issued from here — which is what a fenced node
// needs an external Fencer to establish.  Skipping this is what made arena
// space leak on every departure, graceful or not.
//
// The order is the announcement's meaning.  Locks, then arenas, then the
// departure marker: by the time peers can see the marker this node holds
// nothing, which is the claim the marker makes and the claim they check it
// against.  Announcing first would publish it while a release was still in
// flight, and a peer that believed it would skip the fence that is the only
// thing able to reclaim what had not come back.
func leaveCluster(cfg *config.Config, svc *ipc.Service, store *metadata.Store,
	membership *metadata.Membership, log *config.Logger) {
	// A context of its own: the daemon's is already cancelled by now.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Directory timestamps this node queued rather than committed.  They go
	// before the locks, because after the locks are gone a peer may already be
	// changing the same directories and the queue would be writing over them.
	store.FlushInodeTimes(ctx, 0)

	// Cached inode locks first: their keys outlive the operations that took
	// them, so nothing else has dropped them, and a peer blocked on one should
	// not have to wait for the lease revocation below to notice.
	svc.ReleaseCachedLocks()

	// Ends the lease backing this node's file locks.  This clears whatever a
	// failed release left behind — which would otherwise stand until the lease
	// expires, and the lease is renewed for as long as the process lives.
	if err := store.CloseLockSession(); err != nil {
		log.Warn("lock session not closed, stale lock keys expire with its lease",
			"node", cfg.NodeID, "error", err)
	}

	released, err := membership.Leave(ctx, store)
	switch {
	case err != nil:
		log.Warn("arenas not all released on shutdown, that space stays leaked; "+
			"peers will fence this node rather than take its word for a clean departure",
			"node", cfg.NodeID, "released", released, "error", err)
	case len(released) > 0:
		log.Info("released arenas on shutdown", "node", cfg.NodeID, "arenas", released)
	}
}
