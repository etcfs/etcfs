//go:build etcd

// End-to-end test: etcd → Go backend → C FUSE daemon → read ops.
//
// Requires a running etcd cluster. Run with:
//
//	ETCD_ENDPOINTS=http://localhost:2379 go test -tags=etcd -count=1 -v ./test/e2e/ -run Phase2
package e2e

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/internal/config"
	"github.com/etcfs/etcfs/internal/ipc"
	"github.com/etcfs/etcfs/pkg/fencing"
	"github.com/etcfs/etcfs/pkg/metadata"
)

func TestPhase2EndToEnd(t *testing.T) {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "http://localhost:2379"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err, "cannot connect to etcd at %s", endpoints)
	defer func() { _ = cli.Close() }()

	ctx := context.Background()
	store := metadata.NewStore(cli, "e2e-test")

	// 1. Seed test data
	seedTestData(t, ctx, store)

	// 2. Build and start Go backend
	sockPath := filepath.Join(t.TempDir(), "etcfuse.sock")

	log := config.NewLogger(1)
	membership := metadata.NewMembership(cli, "e2e-node", "e2e-cluster", 30*time.Second)
	watchdog := fencing.NewWatchdog(membership, 30*time.Second)
	svc := ipc.NewService(store, membership, watchdog, log, ipc.Options{})

	errc := make(chan error, 1)
	listener, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	go func() { errc <- svc.RunSocket(listener) }()
	time.Sleep(500 * time.Millisecond)

	// 3. Build and mount C FUSE daemon
	mntPath := filepath.Join(t.TempDir(), "mnt")
	require.NoError(t, os.MkdirAll(mntPath, 0755))

	binPath := findBinary(t, "etcfuse")

	cmd := exec.Command(binPath,
		"--socket", sockPath,
		"--log-level", "3",
		mntPath,
	)
	cmd.Stderr = io.Discard
	cmd.Stdout = io.Discard
	require.NoError(t, cmd.Start())
	defer func() { _ = cmd.Process.Kill() }()

	time.Sleep(1 * time.Second)

	// 4. Test read operations
	t.Run("ls root", func(t *testing.T) {
		entries, err := os.ReadDir(mntPath)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(entries), 3) // hello.txt, notes.md, subdir, etc.
	})

	t.Run("stat file", func(t *testing.T) {
		fi, err := os.Stat(filepath.Join(mntPath, "hello.txt"))
		require.NoError(t, err)
		assert.False(t, fi.IsDir())
	})

	t.Run("read symlink", func(t *testing.T) {
		target, err := os.Readlink(filepath.Join(mntPath, "link-to-hello"))
		require.NoError(t, err)
		assert.Equal(t, "hello.txt", target)
	})

	t.Run("readdir subdirectory", func(t *testing.T) {
		entries, err := os.ReadDir(filepath.Join(mntPath, "subdir"))
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(entries), 1) // nested.txt
	})

	_ = cmd.Process.Kill()
	_ = listener.Close()
}

func seedTestData(t *testing.T, ctx context.Context, store *metadata.Store) {
	t.Helper()

	const rootIno = 1
	rootRec := metadata.InodeRecord{
		Ino:     rootIno,
		Size:    4096,
		Mode:    metadata.ModeDir | 0755,
		Nlink:   2,
		UID:     0,
		GID:     0,
		Blksize: 4096,
		Atime:   time.Now(),
		Mtime:   time.Now(),
		Ctime:   time.Now(),
	}
	_, err := store.Put(ctx, metadata.InodeKey(rootIno), metadata.EncodeInode(&rootRec))
	require.NoError(t, err)

	_, err = store.AtomicCreateFile(ctx, rootIno, "hello.txt", 10, 0644, 1000, 1000, metadata.CreateExtra{})
	require.NoError(t, err)

	_, err = store.AtomicCreateFile(ctx, rootIno, "empty", 11, 0644, 1000, 1000, metadata.CreateExtra{})
	require.NoError(t, err)

	_, err = store.AtomicCreateDir(ctx, rootIno, "subdir", 20, metadata.ModeDir|0755, 1000, 1000)
	require.NoError(t, err)

	_, err = store.AtomicCreateFile(ctx, 20, "nested.txt", 21, 0644, 1000, 1000, metadata.CreateExtra{})
	require.NoError(t, err)
}

func findBinary(t *testing.T, name string) string {
	t.Helper()

	candidates := []string{
		filepath.Join("bin", name),
		filepath.Join("..", "..", "bin", name),
	}
	for _, p := range candidates {
		abs, _ := filepath.Abs(p)
		if _, err := os.Stat(abs); err == nil {
			return abs
		}
	}
	t.Skipf("binary %q not found — build with: make -C cmd/%s", name, name)
	return ""
}

func init() { _ = fmt.Sprintf("") }
