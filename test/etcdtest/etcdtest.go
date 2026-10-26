// Package etcdtest connects integration tests to a real etcd, giving each test
// a key space of its own.
//
// The three integration suites (pkg/arena, pkg/metadata, pkg/scrub, plus the
// IPC datapath) all run against one etcd, and `go test ./...` runs their
// binaries in parallel.  Sharing the real key space meant they deleted and
// recreated each other's inodes mid-run: a guard test failing on a missing
// inode, a scrub test finding its extents already reclaimed as orphans.  Both
// read as product bugs and are not.
//
// Every client here is wrapped in an etcd namespace derived from the test's
// name, so no two tests — in one package or across parallel binaries — can see
// each other's keys, and the cleanup is a single prefix delete.
package etcdtest

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
)

// KeyPrefix is where every test's namespace lives, so a stray key from an
// interrupted run is obvious and removable in one delete.
const KeyPrefix = "etcfs-test/"

// Client returns an etcd client scoped to this test's own key space.  Calling
// it twice inside one test returns clients that see the same keys, which is
// what tests simulating two nodes need.
func Client(t *testing.T) *clientv3.Client {
	t.Helper()

	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "http://localhost:2379"
	}
	return ClientAt(t, endpoints)
}

// ClientAt is Client dialed at the given comma-separated endpoints, for a test
// that needs one particular member of the cluster rather than any of them.
func ClientAt(t *testing.T, endpoints string) *clientv3.Client {
	t.Helper()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("cannot connect to etcd at %s: %v — is one running?", endpoints, err)
	}

	prefix := KeyPrefix + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()) + "/"
	cli.KV = namespace.NewKV(cli.KV, prefix)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, prefix)
	cli.Lease = namespace.NewLease(cli.Lease, prefix)

	t.Cleanup(func() {
		// The namespace wrapper turns this into a delete of the test's whole
		// key space, whatever it wrote.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = cli.Delete(ctx, "", clientv3.WithPrefix())
		_ = cli.Close()
	})

	return cli
}
