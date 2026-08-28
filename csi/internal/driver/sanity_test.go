//go:build sanity

package driver

// csi-sanity drives the full CSI RPC surface (idempotency, error codes,
// argument validation, the create/publish/unpublish/delete lifecycle) against
// a live driver instance. It needs a real etcd for the controller service and
// a real filesystem it can bind-mount into, so it is gated behind a build tag
// rather than run in the default unit-test pass — the same trade the
// integration tests elsewhere in the repo make.
//
// Run with etcd already up (e.g. `make dev` from the repo root, or the
// single-node etcd the root CI job starts):
//
//	ETCD_ENDPOINTS=http://127.0.0.1:2379 go test -tags=sanity -run TestCSISanity -v ./internal/driver/...

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sanity "github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/etcfs/etcfs/pkg/metadata"
)

func TestCSISanity(t *testing.T) {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		t.Skip("ETCD_ENDPOINTS not set; see file comment for how to run this test")
	}

	// The mount path stands in for the EtcFS daemon's mount: sanity only cares
	// that publish targets a real, bind-mountable filesystem tree, not that it
	// is coordinated by EtcFS. isMountPoint is satisfied by making it a real
	// mount (tmpfs), because the driver's own precondition check would
	// otherwise reject every publish on a plain subdirectory of /tmp.
	root := t.TempDir()
	mountRoot(t, root)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("connect to etcd: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	store := metadata.NewStore(cli, "csi-sanity")

	// Fixed rather than sanity's random per-call node IDs: sanity's own
	// lifecycle tests learn the node ID by calling NodeGetInfo, which returns
	// whatever this process is configured with, so the membership key seeded
	// below has to match this value rather than an ID generator's output.
	const sanityNodeID = "sanity-fake-node"

	// The controller refuses to publish onto a node that holds no membership
	// lease — deliberately, see controller.go — so ControllerPublishVolume
	// needs a fake member seeded ahead of time. A real EtcFS daemon does this
	// via membership.Manager; the driver never registers it itself.
	if _, err := store.Put(context.Background(), metadata.MembershipKey(sanityNodeID),
		[]byte(`{"joined":0}`)); err != nil {
		t.Fatalf("seed fake membership: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Delete(context.Background(), metadata.MembershipKey(sanityNodeID))
	})

	cfg := Config{
		Name:      "csi-sanity.etcfs.io",
		Version:   "test",
		Mode:      ModeAll,
		NodeID:    sanityNodeID,
		MountPath: root,
	}
	drv, err := New(cfg, store)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// csi-sanity dials the plugin by address, the same way a CSI sidecar
	// would, so this serves the driver on a real unix socket rather than an
	// in-process connection.
	sockPath := filepath.Join(t.TempDir(), "csi.sock")
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	drv.register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	// csi-sanity mkdir's these itself (not MkdirAll), so they must not exist
	// yet — t.TempDir() already creates its directory, which is why these are
	// unique subpaths of one instead.
	work := t.TempDir()
	sanity.Test(t, sanity.TestConfig{
		Address:     "unix://" + sockPath,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		TargetPath:  filepath.Join(work, "target"),
		StagingPath: filepath.Join(work, "staging"),
		TestVolumeParameters: map[string]string{
			"csi.storage.k8s.io/pvc/name": "sanity",
		},
	})
}

// mountRoot bind mounts root onto itself, so isMountPoint(root) is true the
// same way it would be for the EtcFS daemon's real mount. Requires
// CAP_SYS_ADMIN (root, or a user namespace with mount permission); the test
// skips rather than fails when it does not have it, since sanity's value is
// in CI/dev environments that do.
func mountRoot(t *testing.T, root string) {
	t.Helper()
	if err := unix.Mount(root, root, "", unix.MS_BIND, ""); err != nil {
		t.Skipf("bind mount %s onto itself: %v (needs CAP_SYS_ADMIN)", root, err)
	}
	t.Cleanup(func() { _ = unix.Unmount(root, unix.MNT_DETACH) })
}
