// Package metrics defines the Prometheus metrics EtcFS exposes on
// --metrics-addr, and the server that serves them alongside the health and
// readiness endpoints.
//
// The metrics are package-level variables registered with the default
// Prometheus registry rather than values threaded through every constructor:
// a subsystem instruments itself by referring to the metric it owns, and
// nothing has to be wired for a metric to appear. Metric names are an API —
// dashboards and alert rules are written against them — so they are declared
// here, in one place, instead of being spelled out at each call site.
package metrics

import (
	"io"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// FuseOps counts FUSE operations served, by operation name.
	FuseOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_fuse_ops_total",
		Help: "FUSE operations served, by operation.",
	}, []string{"op"})

	// FuseErrors counts FUSE operations that returned an errno, by operation.
	FuseErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_fuse_errors_total",
		Help: "FUSE operations that failed, by operation.",
	}, []string{"op"})

	// FuseOpDuration observes end-to-end handler latency, by operation. The
	// stage timings that once sat beside it are gone: they existed to attribute
	// a request to etcd, the device or the handler while the data path was
	// being tuned. This one stays because it is the question an operator
	// actually asks — is the filesystem slow, and at what — and no counter can
	// answer it.
	FuseOpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "etcfuse_fuse_op_duration_seconds",
		Help:    "FUSE operation handler latency.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 3, 10),
	}, []string{"op"})

	// Setattr counts SETATTR calls by what the request named and whether it
	// reached etcd.  A timestamp is deferrable and a permission change is not,
	// so the split says how much of a workload's setattr traffic can ever be
	// taken off the request path — and, when a deferral is expected and does
	// not happen, which field is the reason.
	Setattr = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_setattr_total",
		Help: "SETATTR calls, by the fields named and whether they were deferred.",
	}, []string{"fields", "outcome"})

	// EtcdTxnOrigin counts committed transactions by the operation that asked
	// for them.  The total alone says how much consensus a workload costs; this
	// says which part of the filesystem is spending it, which is the difference
	// between knowing a number and being able to act on it.
	EtcdTxnOrigin = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_etcd_txn_origin_total",
		Help: "Committed etcd transactions, by the operation that issued them.",
	}, []string{"origin"})

	// LockHandoverHold observes how long a cached inode lock is held before a
	// peer's recall is honoured, in seconds.
	//
	// It is the whole of the hysteresis trade, in one place: the value grows
	// under sustained contention so a node amortises the handover over several
	// operations, and what it spends is exactly this much of the waiting peer's
	// latency.  A distribution pinned at the ceiling means an inode is being
	// fought over continuously and the waiters are paying for it; one pinned at
	// the floor means recalls are arriving after the holder was finished anyway.
	LockHandoverHold = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "etcfuse_lock_handover_hold_seconds",
		Help:    "How long a cached inode lock is held before a peer's recall is honoured.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 8),
	})

	// EtcdTxns counts etcd transactions, by outcome (committed, rejected, error).
	EtcdTxns = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_etcd_txn_total",
		Help: "etcd transactions attempted, by outcome.",
	}, []string{"outcome"})

	// MetaCache counts data-path metadata lookups served from the snapshot
	// cached under a held inode lock ("hit") against those that had to read
	// etcd ("miss").  A workload whose working set fits the lock cache should
	// sit near all hits; a collapse to misses means locks are being recalled or
	// evicted, which is the first thing to look at when latency rises.
	MetaCache = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_metadata_cache_total",
		Help: "Data-path metadata lookups, by whether the lock-held cache answered them.",
	}, []string{"result"})

	// DirentCache counts lookups of a name that is not there, by whether this
	// node's cached set of the directory's names answered it ("hit") or it had
	// to reach etcd ("miss").  A workload that probes for absent files — a
	// compiler walking an include path is the canonical one — should sit near
	// all hits after the first miss in each directory; sustained misses mean the
	// directories are past the per-directory cap, or the dirent watch is not
	// delivering and the cache is disarmed.
	DirentCache = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_dirent_cache_total",
		Help: "Name lookups, by whether the cached directory name set answered them.",
	}, []string{"result"})

	// ReaddirPage counts READDIR calls that resumed from where the previous
	// reply stopped ("resumed") against those that had to read the directory
	// and count from the start ("rescanned").  A sequential scan resumes for
	// every page after the first, so a listing-heavy workload sitting at
	// rescanned means something is defeating the cursor — concurrent scans of
	// one directory, or seekdir — and paying a full read per page for it.
	ReaddirPage = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_readdir_page_total",
		Help: "READDIR pages served, by whether the listing resumed or was re-read from the start.",
	}, []string{"result"})

	// PendingExtents holds how many metadata keys are buffered but not yet
	// published.  Together with PendingBytes it is the size of what a crash
	// would lose right now, which is the number an operator weighing the flush
	// interval actually wants.
	PendingExtents = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etcfuse_pending_extents",
		Help: "Metadata keys written by acknowledged writes and not yet published to etcd.",
	})

	// PendingBytes holds how much acknowledged write payload those keys stand
	// for.
	PendingBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etcfuse_pending_bytes",
		Help: "Acknowledged write payload whose extents are not yet published to etcd.",
	})

	// Flushes counts publications of a buffer, by what triggered them
	// (interval, buffer_full, sync_write, operation, recall, eviction,
	// shutdown).  A run dominated by "recall" means the flush interval is
	// losing to cross-node contention rather than to the timer.
	Flushes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_metadata_flush_total",
		Help: "Publications of deferred metadata, by trigger.",
	}, []string{"trigger"})

	// FlushBatches counts transactions that published several inodes' buffers
	// at once, and FlushBatchInodes the inodes they carried.  The ratio of the
	// two is the only measurement of what batching the interval sweep is worth:
	// one means the batch was a single flush wearing a different name.
	FlushBatches = promauto.NewCounter(prometheus.CounterOpts{
		Name: "etcfuse_metadata_flush_batch_total",
		Help: "Transactions that published one or more inodes' deferred metadata together.",
	})

	FlushBatchInodes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "etcfuse_metadata_flush_batch_inodes_total",
		Help: "Inodes published by batched metadata flush transactions.",
	})

	// FlushFailures counts flushes that did not publish, by why (error,
	// rejected, fenced).  Anything but zero is worth an alert: "error" means
	// acknowledged writes are sitting unpublished, and the other two mean they
	// were discarded.
	FlushFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_metadata_flush_failures_total",
		Help: "Flushes of deferred metadata that did not publish, by reason.",
	}, []string{"reason"})

	// BlockIO counts block-device operations, by direction (read, write).
	BlockIO = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_block_io_total",
		Help: "Block device operations, by direction.",
	}, []string{"op"})

	// BlockIOBytes counts bytes transferred to and from the block device.
	BlockIOBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_block_io_bytes_total",
		Help: "Bytes transferred to and from the block device, by direction.",
	}, []string{"op"})

	// BlockIODuration times individual device reads and writes.
	//
	// It exists to answer what the counters above cannot: how much of a FUSE
	// operation's latency is the device rather than the daemon. A read served
	// from an already-held lock and a valid snapshot does no etcd work at all,
	// so the remainder is this and the IPC hop around it — and without a
	// measurement here the two are indistinguishable. Same buckets as
	// FuseOpDuration, so the two can be read against each other directly.
	BlockIODuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "etcfuse_block_io_duration_seconds",
		Help:    "Block device operation latency, by direction.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 3, 10),
	}, []string{"op"})

	// ScrubAnomalies counts anomalies found by the scrubber, by type
	// (collision, orphan, dead, range, generation, nlink).
	ScrubAnomalies = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_scrub_anomalies_total",
		Help: "Anomalies detected by the scrubber, by type.",
	}, []string{"type"})

	// ScrubPasses counts completed scrub passes.
	ScrubPasses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "etcfuse_scrub_passes_total",
		Help: "Completed scrub passes.",
	})

	// ScrubLastRun holds the Unix timestamp of the last completed scrub pass.
	// A scrubber that has stopped is invisible in the counters above, which
	// simply stop rising; this makes staleness directly alertable.
	ScrubLastRun = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etcfuse_scrub_last_run_seconds",
		Help: "Unix timestamp of the last completed scrub pass.",
	})

	// ArenaUtilization is the fraction of blocks in use across the arenas this
	// node owns, between 0 and 1.
	ArenaUtilization = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etcfuse_arena_utilization",
		Help: "Fraction of blocks in use across arenas owned by this node.",
	})

	// ArenasOwned is the number of arenas this node currently owns.
	ArenasOwned = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etcfuse_arenas_owned",
		Help: "Arenas currently owned by this node.",
	})

	// MembershipCount is the number of live members in the cluster as last
	// observed by this node.
	MembershipCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etcfuse_membership_count",
		Help: "Live cluster members as last observed by this node.",
	})

	// FencingGeneration is this node's current fencing generation. A step in
	// this series is a fence, which is the single most important event an
	// operator can alert on.
	FencingGeneration = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etcfuse_fencing_generation",
		Help: "This node's current fencing generation.",
	})

	// FencedNodes counts the departures this node's fencing controller acted
	// on, by outcome: "fenced", "failed", and "departed" for a node that
	// announced an intentional leave and was deliberately not fenced.  The
	// last is the one worth graphing next to the others — a cluster whose
	// scale-ins show up as "fenced" rather than "departed" is detaching
	// volumes it did not need to.
	FencedNodes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etcfuse_fenced_nodes_total",
		Help: "Departures acted on by this node's fencing controller, by outcome.",
	}, []string{"outcome"})
)

// StartServer serves /metrics, /healthz and /readyz on addr.  It blocks until
// the server stops.
//
// ready reports whether this node can serve I/O right now; a nil ready makes
// /readyz answer for liveness alone.  The distinction is the one an
// orchestrator acts on: /healthz says the process is alive and should not be
// restarted, /readyz says work sent here will be served rather than answered
// with EIO.  A node whose lease has lapsed or that has fenced itself is
// healthy — it is running, and killing it fixes nothing — but not ready.
func StartServer(addr string, ready func() error, pprof bool) error {
	// Timeouts, because this listener is reachable by anything that can route
	// to the node: without them a client that opens a connection and never
	// finishes its request holds a goroutine and an fd for as long as it likes.
	srv := &http.Server{
		Addr:              addr,
		Handler:           Handler(ready, pprof),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return srv.ListenAndServe()
}

// profileWriteTimeout is how long a profiling response may take to produce.
// The server's own write timeout is meant for clients that stall; a CPU profile
// legitimately writes nothing until its sampling window closes, and the caller
// chooses that window.  Generous enough for the minute-long profiles a
// benchmark takes, and applied to these routes alone.
const profileWriteTimeout = 10 * time.Minute

// registerPprof adds Go's profiling endpoints, each with a write deadline of
// its own so a long profile is not cut off mid-response.
func registerPprof(mux *http.ServeMux) {
	routes := map[string]http.HandlerFunc{
		"/debug/pprof/":        pprof.Index,
		"/debug/pprof/cmdline": pprof.Cmdline,
		"/debug/pprof/profile": pprof.Profile,
		"/debug/pprof/symbol":  pprof.Symbol,
		"/debug/pprof/trace":   pprof.Trace,
	}
	for path, h := range routes {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			// Best effort: a ResponseWriter that cannot take a deadline still
			// serves the shorter profiles, which is better than refusing.
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(profileWriteTimeout))
			h(w, r)
		})
	}
}

// Handler builds the served routes.  Separate from StartServer so the
// endpoints can be exercised without binding a port.
func Handler(ready func() error, enablePprof bool) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if enablePprof {
		registerPprof(mux)
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready != nil {
			if err := ready(); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = io.WriteString(w, "ready\n")
	})
	return mux
}
