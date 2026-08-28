// Package config defines CLI flags and configuration for etcfuse-meta.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// Version is stamped at build time.
var Version = "0.1.0"

// Config holds all parsed configuration.
type Config struct {
	ListenAddr    string
	EtcdEndpoints []string

	// EtcdLocalEndpoint is the endpoint of the etcd member colocated with this
	// node, if there is one.  Reads are issued through it instead of through
	// the round-robin client, so a serializable read on the data path is
	// answered locally rather than over the network; a read the local member
	// cannot answer falls back to the cluster-wide client.  Unset leaves every
	// read on the round-robin client.
	EtcdLocalEndpoint string

	EtcdCertFile string
	EtcdKeyFile  string
	EtcdCAFile   string
	NodeID       string
	ClusterName  string
	LeaseTTL     time.Duration
	LogLevel     int
	ShowVersion  bool
	BlockDevice  string

	// VolumeID identifies the shared data volume by its cloud volume ID.  When
	// set it takes precedence over BlockDevice, whose literal path does not
	// survive a detach/reattach cycle: the path is re-derived from the volume's
	// serial on every start.
	VolumeID    string
	MetricsAddr string
	// Pprof adds Go's profiling endpoints to the metrics listener.  Off by
	// default: that listener is reachable by anything that can route to the
	// node, and a profile is both expensive to produce and a description of
	// what the process is doing.
	Pprof   bool
	RunFsck bool
	RunInfo bool

	// External fencing (AWS). When EBSVolumeID is set the fencing controller
	// detaches the shared volume from an expired node and waits for the
	// detachment to be confirmed before bumping its generation.  Unset (the
	// default, and the only option off EC2) leaves fencing single-signal.
	EBSVolumeID   string
	EC2InstanceID string

	// NVMeReservations selects device-enforced fencing: peers preempt an
	// expired node's NVMe reservation key on BlockDevice, and the device
	// itself rejects that node's writes.  Takes precedence over EBSVolumeID,
	// which is the weaker control-plane fallback.
	NVMeReservations bool

	// AllowBufferedIO permits the data device to be opened without O_DIRECT.
	// On a shared device that is a correctness change, not a fallback — a
	// write served back out of this node's page cache never proves it reached
	// the other attachers — so it is off by default and meant for single-node
	// mounts and file-backed test devices.
	AllowBufferedIO bool

	// WriteBarriers restores the device flush, range sync and readback after
	// every write, and the device flush before every read.  Off by default:
	// with O_DIRECT on a volume that acknowledges a write only once it is
	// durable, they are three device round trips per write that publish
	// nothing the write itself has not already published.  A device with a
	// volatile write cache needs them; buffered mode turns them on regardless.
	WriteBarriers bool

	// MetadataFlushInterval bounds how long a write acknowledged to the kernel
	// may sit with its extent buffered in RAM instead of published to etcd.
	// While this node holds an inode's exclusive lock no peer can read it, so
	// deferring the commit costs nothing cross-node except a peer's stat
	// freshness — and a crash loses whatever has not been flushed, which is
	// what fsync and O_SYNC exist to bound.  Zero commits every write before
	// acknowledging it, which loses nothing and pays a Raft commit per write.
	MetadataFlushInterval time.Duration

	// WriteDataCache buffers a deferred write's payload in RAM beside its
	// extents, so the write costs no device I/O at all and the flush puts the
	// bytes down — coalesced into larger, more sequential I/Os — before it
	// publishes the extents naming them.  It raises the crash exposure that
	// deferring the commit already creates in size, not in kind: an unflushed
	// write was unreachable either way.  Ignored when the flush interval is
	// zero, since nothing is deferred then.
	WriteDataCache bool

	// PageCache lets the kernel hold data pages for an inode this node has a
	// lock on, so a re-read of recently read data costs nothing at all.  The
	// lock is what makes it sound — no peer can write an inode this node holds —
	// and the daemon drops the pages before it yields the lock.  It has no
	// effect on a reader using O_DIRECT, which bypasses the page cache by
	// definition.
	PageCache bool

	// EntryTimeout and AttrTimeout bound how long the kernel may answer a name's
	// existence or absence, and an inode's attributes, from its own caches
	// before asking this daemon again.
	//
	// Both are backed by a cluster-wide etcd watch that pushes an invalidation
	// to every node — the dirent prefix for names, the inode prefix for
	// attributes — and both watches resume from where they stopped when etcd
	// ends them. So these bound the one case the watches cannot cover: a
	// compaction past the resume point, which discards the changes that would
	// have been replayed. Zero selects the defaults; a value below one second
	// disables the cache, since FUSE carries whole seconds.
	EntryTimeout time.Duration
	AttrTimeout  time.Duration

	// NotifyAddr is the socket the C daemon connects to for cache-invalidation
	// notifications.  Configurable for the same reason ListenAddr is: two
	// daemons on one host need two paths.
	NotifyAddr string

	// HistoryLog records every served operation to a file, as the input the
	// consistency checkers take. Off unless set: it writes one line per
	// operation, which is a measurable cost on a busy mount and is meant for
	// test runs rather than production.
	HistoryLog string

	// ReadOnly rejects every mutating FUSE operation with EROFS. Lets a node
	// mount the shared filesystem for backup or inspection while another node
	// writes, and gives fsck a safe way to run against a live volume.
	ReadOnly bool
}

// Default socket paths.
//
// Under /run rather than /tmp: both sockets are removed and re-bound at
// startup, and anything able to write the directory can win that race or
// occupy the path first.  /run is root-writable only, which /tmp is not.
const (
	DefaultSocket       = "/run/etcfuse/etcfuse.sock"
	DefaultNotifySocket = "/run/etcfuse/etcfuse-notify.sock"
)

// RequestTimeout bounds the etcd work behind a single FUSE request.
//
// It lives here rather than in the IPC package because it is one half of a
// constraint on configuration: it must sit below the self-fencing window, which
// is derived from the membership lease TTL.  A TTL that inverts the two makes
// the daemon exit before the request deadline can ever fire — the situation the
// deadline exists to avoid — so Parse rejects it, and it needs the number to do
// so.
//
// Above it, the value has to clear a routine leader election (~1-2 s) plus the
// several sequential store calls a handler makes, or an otherwise healthy
// cluster returns EIO during ordinary failover.
const RequestTimeout = 10 * time.Second

// DefaultEntryTimeout and DefaultAttrTimeout are how long the kernel may answer
// a name's existence and an inode's attributes from its own caches before
// asking the daemon again.
//
// They live here, next to the flags that carry them, for the same reason
// RequestTimeout does: internal/ipc imports this package, so one definition can
// be both the flag's printed default and the value the service runs with.
//
// Both are backed by a cluster-wide watch that pushes an invalidation on every
// change, and both watches resume from where they stopped when etcd ends them —
// so these bound the single case a watch cannot cover, a compaction past the
// resume point. A minute is long enough that a walk of a large tree finds its
// own names and attributes still cached on the way back, which one second never
// was.
const (
	DefaultEntryTimeout = 60 * time.Second
	DefaultAttrTimeout  = 60 * time.Second
)

// SelfFenceWindow is how long a node keeps serving after its membership lease
// stops being renewed, before the watchdog declares it fenced and exits.  It
// mirrors the watchdog's own 2x rule.
func SelfFenceWindow(leaseTTL time.Duration) time.Duration {
	return 2 * leaseTTL
}

// Parse reads CLI flags and returns a Config.
func Parse() *Config {
	cfg := &Config{}

	var etcdEndpoints string
	var leaseTTL string
	var flushInterval string
	var entryTimeout, attrTimeout string

	flag.StringVar(&cfg.NotifyAddr, "notify-socket", DefaultNotifySocket,
		"Unix socket the C daemon connects to for cache-invalidation notifications")
	flag.StringVar(&cfg.ListenAddr, "listen", DefaultSocket,
		"Unix domain socket path for C daemon IPC")
	flag.StringVar(&etcdEndpoints, "etcd-endpoints", "http://localhost:2379",
		"Comma-separated etcd client endpoints")
	flag.StringVar(&cfg.EtcdLocalEndpoint, "etcd-local-endpoint", "",
		"Endpoint of the etcd member colocated with this node; reads are served through it, with the round-robin client as fallback")
	flag.StringVar(&cfg.EtcdCertFile, "etcd-cert", "",
		"Path to etcd client certificate")
	flag.StringVar(&cfg.EtcdKeyFile, "etcd-key", "",
		"Path to etcd client key")
	flag.StringVar(&cfg.EtcdCAFile, "etcd-ca", "",
		"Path to etcd CA certificate")
	flag.StringVar(&cfg.NodeID, "node-id", "",
		"Node identifier (default: hostname)")
	flag.StringVar(&cfg.ClusterName, "cluster-name", "etcfuse",
		"EtcFS cluster name")
	flag.StringVar(&leaseTTL, "lease-ttl", "10s",
		"Membership lease TTL (e.g., 10s, 30s, 1m)")
	flag.IntVar(&cfg.LogLevel, "log-level", 1,
		"Log level: 0=error, 1=info, 2=debug")
	flag.BoolVar(&cfg.ShowVersion, "version", false, "Show version and exit")
	flag.StringVar(&cfg.BlockDevice, "block-device", "",
		"Block device path for data I/O (e.g., /dev/nvme1n1); prefer --volume-id, which survives a detach/reattach")
	flag.StringVar(&cfg.VolumeID, "volume-id", "",
		"Cloud volume ID of the shared data volume (e.g., vol-0abcdef1234567890); its device path is resolved at every start and overrides --block-device")
	flag.StringVar(&cfg.MetricsAddr, "metrics-addr", "",
		"Prometheus metrics HTTP listen address (e.g., :9090)")
	flag.BoolVar(&cfg.Pprof, "pprof", false,
		"Serve Go profiling endpoints under /debug/pprof on the metrics listener")
	flag.BoolVar(&cfg.RunFsck, "fsck", false,
		"Run offline filesystem check and exit")
	flag.BoolVar(&cfg.RunInfo, "info", false,
		"Print filesystem statistics and exit")
	flag.StringVar(&cfg.EBSVolumeID, "ebs-volume-id", "",
		"Shared EBS volume ID; enables dual-confirmed external fencing (detach + poll) when set")
	flag.BoolVar(&cfg.NVMeReservations, "nvme-reservations", false,
		"Fence peers by preempting their NVMe reservation key on --block-device (requires a device supporting NVMe reservations, e.g. an EBS io2 Multi-Attach volume)")
	flag.BoolVar(&cfg.AllowBufferedIO, "allow-buffered-io", false,
		"Permit opening the data device without O_DIRECT; unsafe on a device attached to more than one node")
	flag.BoolVar(&cfg.WriteBarriers, "write-barriers", false,
		"Flush the device cache, sync the range and read it back after every write; needed only on a device with a volatile write cache that does not publish an acknowledged O_DIRECT write to its other attachers (always on without O_DIRECT)")
	flag.StringVar(&flushInterval, "metadata-flush-interval", "100ms",
		"How long a write's extent may stay buffered in memory before it is published to etcd; 0 commits every write before acknowledging it (durable, one Raft commit per write)")
	flag.StringVar(&entryTimeout, "entry-timeout", DefaultEntryTimeout.String(),
		"How long the kernel may answer a name's existence or absence without asking; peers push an invalidation on every change, so this bounds only what an unresumable etcd watch would miss")
	flag.StringVar(&attrTimeout, "attr-timeout", DefaultAttrTimeout.String(),
		"How long the kernel may answer an inode's attributes without asking; peers push an invalidation on every inode change, so this bounds only what an unresumable etcd watch would miss")
	flag.BoolVar(&cfg.PageCache, "page-cache", true,
		"Let the kernel cache data pages for files this node holds a lock on; they are invalidated before the lock is yielded")
	flag.BoolVar(&cfg.WriteDataCache, "write-data-cache", true,
		"Buffer a deferred write's data in memory as well as its extents, and put it on the device at flush time; off writes the data through on every write")
	flag.StringVar(&cfg.EC2InstanceID, "ec2-instance-id", "",
		"This node's EC2 instance ID, recorded in its membership key so peers can detach the volume when it expires")
	flag.StringVar(&cfg.HistoryLog, "history-log", "",
		"Append every served operation to this file, for offline consistency checking (see https://github.com/etcfs/etcfs-docs/blob/main/docs/verification/porcupine.md)")
	flag.BoolVar(&cfg.ReadOnly, "read-only", false,
		"Reject every mutating FUSE operation with EROFS; for backup/inspection mounts and running fsck against a live volume")

	flag.Parse()

	cfg.EtcdEndpoints = strings.Split(etcdEndpoints, ",")

	if cfg.NodeID == "" {
		hostname, _ := os.Hostname()
		cfg.NodeID = hostname
	}

	if cfg.NVMeReservations && cfg.BlockDevice == "" && cfg.VolumeID == "" {
		fmt.Fprintln(os.Stderr, "-nvme-reservations requires -volume-id or -block-device")
		os.Exit(1)
	}

	fi, err := time.ParseDuration(flushInterval)
	if err != nil || fi < 0 {
		fmt.Fprintf(os.Stderr, "invalid metadata-flush-interval %q\n", flushInterval)
		os.Exit(1)
	}
	// A buffer is published before its inode's lock is given up, and a request
	// that has to wait for that flush is bounded by the request deadline.  An
	// interval at or above it would let a peer's recall time out against a
	// holder that was still within its own flush interval.
	if fi >= RequestTimeout {
		fmt.Fprintf(os.Stderr,
			"metadata-flush-interval %s is at or above the %s request timeout: "+
				"a peer's recall would time out waiting for the flush\n", fi, RequestTimeout)
		os.Exit(1)
	}
	cfg.MetadataFlushInterval = fi

	cfg.EntryTimeout = parseCacheTimeout("entry-timeout", entryTimeout)
	cfg.AttrTimeout = parseCacheTimeout("attr-timeout", attrTimeout)

	d, err := time.ParseDuration(leaseTTL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid lease-ttl %q: %v\n", leaseTTL, err)
		os.Exit(1)
	}
	// The self-fencing watchdog declares the node fenced once its lease has
	// been dead for 2x the TTL, and exits.  If that window closes before the
	// request deadline can fire, the daemon dies with requests still waiting
	// for the deadline that would have failed them cleanly — which is the
	// situation the deadline exists to avoid.
	if SelfFenceWindow(d) <= RequestTimeout {
		fmt.Fprintf(os.Stderr,
			"lease-ttl %s gives a %s self-fencing window, at or below the %s request timeout: "+
				"the daemon would exit before a stalled request could fail\n",
			d, SelfFenceWindow(d), RequestTimeout)
		os.Exit(1)
	}
	cfg.LeaseTTL = d

	return cfg
}

// parseCacheTimeout reads a cache timeout flag, rejecting a negative value.
//
// Zero is accepted and means "do not cache", which is the value to reach for
// when debugging a coherence complaint: it takes the kernel's caches out of the
// picture entirely and sends every lookup and stat to the daemon.
func parseCacheTimeout(name, value string) time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil || d < 0 {
		fmt.Fprintf(os.Stderr, "invalid %s %q: want a non-negative duration\n", name, value)
		os.Exit(1)
	}
	return d
}

// EtcdTLSConfig returns a tls.Config from the configured cert files.
// Returns nil if no cert files are specified (plaintext connection).
func (c *Config) EtcdTLSConfig() *tls.Config {
	if c.EtcdCertFile == "" && c.EtcdCAFile == "" {
		return nil
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if c.EtcdCertFile != "" && c.EtcdKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.EtcdCertFile, c.EtcdKeyFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load etcd client cert: %v\n", err)
			os.Exit(1)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	if c.EtcdCAFile != "" {
		caCert, err := os.ReadFile(c.EtcdCAFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to read etcd CA cert: %v\n", err)
			os.Exit(1)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsCfg.RootCAs = caCertPool
	}

	return tlsCfg
}
