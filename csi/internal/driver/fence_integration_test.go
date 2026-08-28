//go:build integration

// The one piece of driver logic that is not boilerplate: ControllerUnpublishVolume
// must record a fence intent for a node that no longer holds a membership
// lease, and must leave a live node alone. What happens to that intent
// afterwards — claiming it, dual-confirmed detach, generation bump — is
// pkg/fencing.Controller's reconciliation sweep, already covered by its own
// integration suite (pkg/fencing/controller_integration_test.go); this test
// stops at the boundary the driver actually owns: the intent record itself.
//
// Run with etcd already up:
//
//	ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration -count=1 -v ./internal/driver/...
package driver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/etcfs/etcfs/pkg/metadata"
	"github.com/etcfs/etcfs/test/etcdtest"
)

func unpublishReq(volumeID, nodeID string) *csi.ControllerUnpublishVolumeRequest {
	return &csi.ControllerUnpublishVolumeRequest{VolumeId: volumeID, NodeId: nodeID}
}

func TestControllerUnpublishVolume_LiveNodeIsLeftAlone(t *testing.T) {
	cli := etcdtest.Client(t)
	store := metadata.NewStore(cli, "test-controller")

	require.NoError(t, registerMembership(t, store, "live-node"))

	srv := &controllerServer{
		cfg:   Config{MountPath: t.TempDir()},
		store: store,
	}
	_, err := srv.ControllerUnpublishVolume(context.Background(), unpublishReq("vol-1", "live-node"))
	require.NoError(t, err)

	intents, err := store.ListFenceIntents(context.Background())
	require.NoError(t, err)
	assert.Empty(t, intents,
		"a node that still holds a membership lease released a volume during ordinary "+
			"rescheduling; fencing it would take out a working host")
}

func TestControllerUnpublishVolume_DepartedNodeRecordsIntent(t *testing.T) {
	cli := etcdtest.Client(t)
	store := metadata.NewStore(cli, "test-controller")

	// No membership key registered for "dead-node": it has already departed.
	srv := &controllerServer{
		cfg:   Config{MountPath: t.TempDir()},
		store: store,
	}
	_, err := srv.ControllerUnpublishVolume(context.Background(), unpublishReq("vol-1", "dead-node"))
	require.NoError(t, err)

	intents, err := store.ListFenceIntents(context.Background())
	require.NoError(t, err)
	assert.Contains(t, intents, "dead-node")
}

// A repeated release must not double the intent record — CSI's own contract
// requires ControllerUnpublishVolume to tolerate being retried after an
// ambiguous RPC failure.
func TestControllerUnpublishVolume_IsIdempotent(t *testing.T) {
	cli := etcdtest.Client(t)
	store := metadata.NewStore(cli, "test-controller")

	srv := &controllerServer{
		cfg:   Config{MountPath: t.TempDir()},
		store: store,
	}
	for i := 0; i < 2; i++ {
		_, err := srv.ControllerUnpublishVolume(context.Background(), unpublishReq("vol-1", "dead-node"))
		require.NoError(t, err)
	}

	intents, err := store.ListFenceIntents(context.Background())
	require.NoError(t, err)
	assert.Len(t, intents, 1)
}

func registerMembership(t *testing.T, store *metadata.Store, nodeID string) error {
	t.Helper()
	_, err := store.Put(context.Background(), metadata.MembershipKey(nodeID), []byte(`{"joined":0}`))
	return err
}
