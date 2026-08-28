package harness

import (
	"regexp"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/etcfs/etcfs/pkg/metrics"
	"github.com/etcfs/etcfs/pkg/scrub"
)

// The metric names below are an API: dashboards and alert rules are written
// against them, so a rename is a breaking change and this list is what makes
// one visible.  Registration is checked against the default registry the
// subsystems instrument, not a private one — a metric declared but never
// gathered would pass the second kind of test and fail the operator.
func TestMetrics_DeclaredNamesAreRegistered(t *testing.T) {
	registered := registeredNames(t)

	for _, name := range []string{
		"etcfuse_fuse_ops_total",
		"etcfuse_fuse_errors_total",
		"etcfuse_fuse_op_duration_seconds",
		"etcfuse_etcd_txn_total",
		"etcfuse_block_io_total",
		"etcfuse_block_io_bytes_total",
		"etcfuse_scrub_anomalies_total",
		"etcfuse_scrub_passes_total",
		"etcfuse_scrub_last_run_seconds",
		"etcfuse_arena_utilization",
		"etcfuse_arenas_owned",
		"etcfuse_membership_count",
		"etcfuse_fencing_generation",
		"etcfuse_fenced_nodes_total",
	} {
		assert.True(t, registered[name], "metric %q should be registered", name)
	}
}

// registeredNames returns every metric name in the default registry.
//
// Read from the collectors' descriptors rather than from a Gather: a labelled
// metric with no series yet — which is every counter on a freshly started node —
// is registered but gathers nothing, so Gather would report the whole surface
// missing until traffic arrives.
func registeredNames(t *testing.T) map[string]bool {
	t.Helper()

	descs := make(chan *prometheus.Desc)
	go func() {
		defer close(descs)
		prometheus.DefaultGatherer.(*prometheus.Registry).Describe(descs)
	}()

	fqName := regexp.MustCompile(`fqName: "([^"]+)"`)
	names := make(map[string]bool)
	for d := range descs {
		if m := fqName.FindStringSubmatch(d.String()); m != nil {
			names[m[1]] = true
		}
	}
	require.NotEmpty(t, names, "the default registry describes no metrics at all")
	return names
}

// Wiring, not the metric type: a scrub pass must move the scrubber's counters
// without anyone passing a registry in.  This is the check that fails if the
// instrumentation is removed from RunScrubPass, which the metric-name test
// above would not notice.
func TestMetrics_ScrubPassIsInstrumented(t *testing.T) {
	cluster := NewCluster(1)
	sc := scrub.New(cluster.Store, "metrics-node", time.Hour, nullLogger{})

	before := testutil.ToFloat64(metrics.ScrubPasses)
	sc.RunScrubPass(t.Context())
	after := testutil.ToFloat64(metrics.ScrubPasses)

	assert.Equal(t, before+1, after, "a scrub pass should increment etcfuse_scrub_passes_total")
	assert.Positive(t, testutil.ToFloat64(metrics.ScrubLastRun),
		"a scrub pass should stamp etcfuse_scrub_last_run_seconds")
}
