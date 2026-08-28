package harness

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/scrub"
)

type testLogger struct{ t *testing.T }

func (l testLogger) Warn(msg string, args ...any)  { l.t.Logf("WARN: "+msg, args...) }
func (l testLogger) Info(msg string, args ...any)  { l.t.Logf("INFO: "+msg, args...) }
func (l testLogger) Error(msg string, args ...any) { l.t.Logf("ERROR: "+msg, args...) }

func newTestScrubber(t *testing.T) (*scrub.Scrubber, *MockStore) {
	store := NewMockStore()
	s := scrub.New(store, "scrub-node", 10*time.Millisecond, testLogger{t})
	return s, store
}

// ---- C8.1: Scrubber detects extent collision ----

// snap gathers the one snapshot a pass shares between its checks.
func snap(t *testing.T, s *scrub.Scrubber, ctx context.Context) *scrub.Snapshot {
	t.Helper()
	got, err := s.Scan(ctx)
	require.NoError(t, err)
	return got
}

func TestScrub_Collision(t *testing.T) {
	scrubber, store := newTestScrubber(t)
	ctx := t.Context()

	// Two extents claiming the same disk_off
	store.kv["extent:100/0"] = []byte("0,4096,4096,1,0,node-A")
	store.kv["extent:200/0"] = []byte("0,4096,4096,1,0,node-A") // same disk_off!

	results := scrubber.CheckExtentCollisions(snap(t, scrubber, ctx))
	require.Len(t, results, 1, "should detect one collision")
	assert.Equal(t, "collision", results[0].Type)
	assert.Contains(t, results[0].Detail, "4096")
}

// ---- C8.2: Scrubber detects out-of-range extent ----

func TestScrub_RangeViolation(t *testing.T) {
	scrubber, store := newTestScrubber(t)
	ctx := t.Context()
	scrubber.SetDeviceSize(4 << 30) // a 4 GiB device

	store.kv["extent:300/0"] = []byte("0,1099511627776,4096,1,0,node-A") // 1 TiB in
	store.kv["extent:301/0"] = []byte("0,4096,4096,1,0,node-A")          // comfortably inside

	results := scrubber.CheckRangeValidity(snap(t, scrubber, ctx))
	assert.Len(t, results, 1, "should detect exactly the out-of-range extent")
}

// Without a device size there is nothing to compare against, and the check
// used to invent a 1 TiB ceiling that matched neither the device nor fsck.
func TestScrub_RangeCheckSkippedWithoutADeviceSize(t *testing.T) {
	scrubber, store := newTestScrubber(t)
	ctx := t.Context()

	store.kv["extent:300/0"] = []byte("0,1099511627776,4096,1,0,node-A")

	assert.Empty(t, scrubber.CheckRangeValidity(snap(t, scrubber, ctx)))
}

// ---- C8.3: Scrubber detects orphan extents ----

func TestScrub_Orphan(t *testing.T) {
	scrubber, store := newTestScrubber(t)
	ctx := t.Context()

	// Extent exists but inode doesn't
	store.kv["extent:999/0"] = []byte("0,8192,4096,1,0,node-A")

	results := scrubber.CheckOrphanExtents(snap(t, scrubber, ctx))
	require.Len(t, results, 1, "should detect one orphan")
	assert.Equal(t, "orphan", results[0].Type)
	assert.True(t, results[0].AutoFix, "orphans should be auto-fixable")
}

// ---- C8.4: Scrubber detects generation mismatch ----

// An extent may not carry a generation its writer has never reached: every
// commit is guarded by that generation, so one above it means the guard let
// through a write it should have rejected.
func TestScrub_GenerationFromTheFutureIsAnomalous(t *testing.T) {
	scrubber, store := newTestScrubber(t)
	ctx := t.Context()

	store.kv["gen:node-A"] = []byte("3")
	store.kv["extent:400/0"] = []byte("0,16384,4096,9,0,node-A")

	results := scrubber.CheckGenerationConsistency(snap(t, scrubber, ctx))
	assert.GreaterOrEqual(t, len(results), 1, "should detect generation mismatch")
}

// The false positive the check used to produce on every healthy node: an
// extent written before its node was fenced carries the older generation, and
// so does every extent written by every node that was never fenced at all.
func TestScrub_ExtentsOlderThanTheirNodesFenceAreNotAnomalies(t *testing.T) {
	scrubber, store := newTestScrubber(t)
	ctx := t.Context()

	store.kv["gen:node-A"] = []byte("7") // fenced repeatedly
	store.kv["gen:node-B"] = []byte("0") // never fenced
	store.kv["extent:400/0"] = []byte("0,16384,4096,3,0,node-A")
	store.kv["extent:401/0"] = []byte("0,20480,4096,0,0,node-B")

	assert.Empty(t, scrubber.CheckGenerationConsistency(snap(t, scrubber, ctx)),
		"extents predating a fence are ordinary data")
}

// ---- C8.5: Scrubber detects nlink mismatch ----

func TestScrub_NlinkMismatch(t *testing.T) {
	scrubber, store := newTestScrubber(t)
	ctx := t.Context()

	// Create inode with nlink=1 but 2 dirents pointing to it
	rec := &metadata.InodeRecord{Ino: 500, Nlink: 1, Mode: 0100644, Size: 4096}
	store.kv["inode:500"] = metadata.EncodeInode(rec)
	store.kv["dirent:1/link1"] = encodeUint64BE(500)
	store.kv["dirent:1/link2"] = encodeUint64BE(500)

	results := scrubber.CheckNlinkConsistency(snap(t, scrubber, ctx))
	assert.GreaterOrEqual(t, len(results), 1, "should detect nlink mismatch")
}

// ---- C8.6: Scrubbing rate-limiting ----

func TestScrub_RateLimit(t *testing.T) {
	scrubber, store := newTestScrubber(t)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	// Add many extents to simulate large scrub
	for i := 0; i < 100; i++ {
		store.kv[fmt.Sprintf("extent:%d/0", 600+i)] = []byte(fmt.Sprintf("%d,%d,4096,1,0", i*4096, i*4096))
	}

	// Run a scrub pass — should complete within timeout
	scrubber.RunScrubPass(ctx)

	passes, _ := scrubber.Stats()
	assert.Equal(t, int64(1), passes, "should have completed one pass")
}

// ---- C8.7: Scrubber survives restart ----

func TestScrub_SurviveRestart(t *testing.T) {
	store := NewMockStore()

	// First scrubber instance
	scrubber1 := scrub.New(store, "node-a", 10*time.Millisecond, testLogger{t})
	ctx := t.Context()

	store.kv["extent:700/0"] = []byte("0,28672,4096,1,0,node-A")
	results1 := scrubber1.CheckOrphanExtents(snap(t, scrubber1, ctx))
	assert.Len(t, results1, 1)

	// "Restart" — create a new scrubber with the same store
	scrubber2 := scrub.New(store, "node-a", 10*time.Millisecond, testLogger{t})
	results2 := scrubber2.CheckOrphanExtents(snap(t, scrubber2, ctx))
	assert.Len(t, results2, 1, "should detect same anomaly after restart")
}

// ---- C8.9: Scrubber throughput ----

func TestScrub_Throughput(t *testing.T) {
	scrubber, store := newTestScrubber(t)
	ctx := t.Context()

	// Add many extents
	count := 500
	for i := 0; i < count; i++ {
		store.kv[fmt.Sprintf("extent:%d/0", 800+i)] = []byte(fmt.Sprintf("%d,%d,4096,1,0", i*4096, i*4096))
	}

	start := time.Now()
	results := scrubber.CheckExtentCollisions(snap(t, scrubber, ctx))
	elapsed := time.Since(start)

	assert.Empty(t, results, "no collisions expected")
	t.Logf("scanned %d extents in %v (%.0f extents/sec)", count, elapsed,
		float64(count)/elapsed.Seconds())
}

// ---- C8.8: All invariants hold under normal operations ----

func TestScrub_AllInvariantsNormalOps(t *testing.T) {
	s := NewSimulator(8001)
	ctx := t.Context()

	// Create normal filesystem state
	for i := 0; i < 10; i++ {
		ino := uint64(9000 + i)
		_, _ = s.createFile(ctx, 1, fmt.Sprintf("n%d", i), ino, 0100644)
		s.writeInode(ctx, ino, 4096)
	}

	// Check invariants via the scrubber's equivalent
	v := s.checkInvariants()
	assert.Zero(t, v, "normal operations should not violate invariants")
}

// ---- C8.10: Alerting ----

func TestScrub_Alerting(t *testing.T) {
	scrubber, store := newTestScrubber(t)
	ctx := t.Context()

	// Initially clean
	rec := &metadata.InodeRecord{Ino: 6000, Nlink: 1, Mode: 0100644, Size: 4096}
	store.kv["inode:6000"] = metadata.EncodeInode(rec)
	store.kv["dirent:1/clean.txt"] = encodeUint64BE(6000)

	results := scrubber.CheckNlinkConsistency(snap(t, scrubber, ctx))
	assert.Empty(t, results, "clean state should have no alerts")

	// Inject anomaly
	store.kv["dirent:1/extra.txt"] = encodeUint64BE(6000) // now 2 dirents, nlink=1
	results = scrubber.CheckNlinkConsistency(snap(t, scrubber, ctx))
	assert.NotEmpty(t, results, "anomaly should trigger alert")
}

// ---- Helpers ----

func encodeUint64BE(v uint64) []byte {
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
		byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}
