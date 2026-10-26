//go:build integration
// +build integration

package metadata

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/etcfs/etcfs/test/etcdtest"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// A reader that has taken the lock must see every extent published before the
// writer released it, even when its colocated member is behind the leader: the
// lock orders the commits, not what a lagging member has applied, and a
// serializable read there still succeeds with the older list.
//
// Needs a member partitioned from its peers but reachable by clients, named by
// ETCD_LAGGING_ENDPOINT; ETCD_ENDPOINTS must name the healthy members only.
func TestIntegration_LockedReadIsNotStaleOnALaggingMember(t *testing.T) {
	lagging := os.Getenv("ETCD_LAGGING_ENDPOINT")
	if lagging == "" {
		t.Skip("ETCD_LAGGING_ENDPOINT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	writer := NewStore(etcdtest.Client(t), "node-W")
	reader := NewStore(etcdtest.Client(t), "node-R")
	local := etcdtest.ClientAt(t, lagging)
	reader.SetLocalClient(local)

	const ino = 9950
	_, err := writer.AtomicCreateFile(ctx, RootIno, "f", ino, ModeFile|0644, 1000, 1000, CreateExtra{})
	require.NoError(t, err)
	holder, err := writer.AcquireLock(ctx, ino, LockExclusive, 10*time.Second)
	require.NoError(t, err)
	ext := Extent{LogOff: 0, DiskOff: 4096, Length: 4096, Node: "node-W"}
	_, err = writer.Put(ctx, ExtentKey(ino, 0), []byte(ext.Encode()))
	require.NoError(t, err)
	_, err = writer.ReleaseLock(ctx, ino, LockExclusive, holder)
	require.NoError(t, err)

	probe, err := local.Get(ctx, ExtentKey(ino, 0), clientv3.WithSerializable())
	require.NoError(t, err)
	if len(probe.Kvs) != 0 {
		t.Skip("the member at ETCD_LAGGING_ENDPOINT is not behind")
	}

	_, err = reader.AcquireLock(ctx, ino, LockShared, 10*time.Second)
	require.NoError(t, err)
	_, extents, err := reader.GetInodeAndExtents(ctx, ino)
	require.NoError(t, err)
	require.Len(t, extents, 1, "the read under the lock missed an extent published before it was taken")
}
