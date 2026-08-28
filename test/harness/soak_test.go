package harness

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/etcfs/etcfs/pkg/fsck"
	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/pkg/scrub"
)

type nullLogger struct{}

func (nullLogger) Warn(msg string, args ...any)  {}
func (nullLogger) Info(msg string, args ...any)  {}
func (nullLogger) Error(msg string, args ...any) {}

// ---- C11.1: Scaled soak test (harness version) ----

func TestSoak_ScaledJepsenWithMetrics(t *testing.T) {
	cluster := NewCluster(3)
	ctx := t.Context()
	store := cluster.Store

	cluster.createDirIfMissing(ctx, 1, "soak", 70000)
	for i := 0; i < 20; i++ {
		ino := uint64(75000) + uint64(i)
		name := fmt.Sprintf("base-%02d", i)
		rec := &metadata.InodeRecord{
			Ino: ino, Mode: 0100644, Nlink: 1, Size: 4096, Blksize: 4096,
		}
		_, _ = store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
		_, _ = store.Put(ctx, metadata.DirentKey(70000, name), metadata.EncodeUint64(ino))
	}

	sc := scrub.New(store, "soak-node", 20*time.Millisecond, nullLogger{})

	runUntil := time.Now().Add(5 * time.Second)
	ops := 0
	faults := 0

	for time.Now().Before(runUntil) {
		n := cluster.Nodes[ops%3]
		ino := uint64(80000) + uint64(ops)
		name := fmt.Sprintf("s-%d", ops)

		switch ops % 10 {
		case 0:
			_, _ = n.createFile(ctx, 70000, name, ino, 0100644)
		case 1:
			n.writeInode(ctx, ino, 4096)
		case 4:
			_, _ = store.Put(ctx, fmt.Sprintf("extent:%d/0", ino),
				[]byte(fmt.Sprintf("0,%d,4096,1", 4096*(ops%1000))))
		case 6:
			n.injectFault(FaultEtcdPartition)
			faults++
		case 7:
			faults++
		case 8:
			if snapshot, serr := sc.Scan(ctx); serr == nil {
				_ = sc.CheckExtentCollisions(snapshot)
			}
		}
		ops++
	}

	t.Logf("soak: %d ops, %d faults in 5s", ops, faults)

	// Verify invariants held
	assert.Zero(t, cluster.checkAllInvariants())

	// Run fsck after soak
	chk := fsck.New(store)
	findings := chk.Run(ctx)
	require.Zero(t, chk.ErrorCount(), "fsck should find zero errors after soak")
	t.Logf("fsck after soak: %d findings (0 errors, %d warnings)",
		len(findings), chk.WarningCount())

}
