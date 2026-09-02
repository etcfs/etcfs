// seed-etcd populates an etcd cluster with a known directory tree for
// testing the FUSE read path.  Run before starting the EtcFS daemon.
//
// Usage:
//
//	ETCD_ENDPOINTS=http://localhost:2379 go run cmd/seed-etcd/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/pkg/metadata"
)

func main() {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "http://localhost:2379"
	}
	fmt.Fprintf(os.Stderr, "seed-etcd: using endpoints=%s\n", endpoints)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot connect to etcd: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = cli.Close() }()

	// Create root inode first (ino=1, no parent)
	ctx := context.Background()
	store := metadata.NewStore(cli, "seed-tool")

	rootRec := metadata.InodeRecord{
		Ino:     1,
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
	// Put root inode directly (bypass AtomicCreateDir since no dirent needed)
	rootVal := metadata.EncodeInode(&rootRec)
	_, err = store.Put(ctx, metadata.InodeKey(1), rootVal)
	if err != nil {
		fmt.Fprintf(os.Stderr, "root inode put error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("  created inode:1 (root)")

	type entry struct {
		parent uint64
		name   string
		ino    uint64
		mode   uint32
		uid    uint32
		gid    uint32
		target string // for symlinks
	}

	tree := []entry{
		// Root directory — always ino 1 in FUSE
		{parent: 1, name: ".", ino: 1, mode: metadata.ModeDir | 0755, uid: 0, gid: 0},

		// Top-level files
		{parent: 1, name: "hello.txt", ino: 10, mode: 0100644, uid: 1000, gid: 1000},
		{parent: 1, name: "notes.md", ino: 11, mode: 0100644, uid: 1000, gid: 1000},
		{parent: 1, name: "empty", ino: 12, mode: 0100644, uid: 1000, gid: 1000},

		// Sub-directory
		{parent: 1, name: "subdir", ino: 20, mode: metadata.ModeDir | 0755, uid: 1000, gid: 1000},
		{parent: 20, name: ".", ino: 20, mode: metadata.ModeDir | 0755, uid: 1000, gid: 1000},
		{parent: 20, name: "nested.txt", ino: 21, mode: 0100644, uid: 1000, gid: 1000},
		{parent: 20, name: "deep", ino: 22, mode: metadata.ModeDir | 0755, uid: 1000, gid: 1000},
		{parent: 22, name: ".", ino: 22, mode: metadata.ModeDir | 0755, uid: 1000, gid: 1000},
		{parent: 22, name: "bottom.txt", ino: 23, mode: 0100644, uid: 1000, gid: 1000},

		// Symlink
		{parent: 1, name: "link-to-hello", ino: 30, mode: metadata.ModeSymlink | 0777, uid: 1000, gid: 1000, target: "hello.txt"},
	}

	for _, e := range tree {
		// Skip self-referential directory entries
		if e.name == "." {
			continue
		}

		// Check if dirent already exists
		existing, err := store.LookupDirent(ctx, e.parent, e.name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "lookup error: %v\n", err)
			os.Exit(1)
		}
		if existing != 0 {
			fmt.Printf("  skip %s (already exists, ino=%d)\n", metadata.DirentKey(e.parent, e.name), existing)
			continue
		}

		switch e.mode & metadata.S_IFMT {
		case metadata.ModeDir:
			_, err = store.AtomicCreateDir(ctx, e.parent, e.name, e.ino, e.mode, e.uid, e.gid)
		case metadata.ModeSymlink:
			_, err = store.CreateInode(ctx, e.ino, e.mode, e.uid, e.gid)
			if err == nil {
				// Store symlink target in inode record
				_, err = store.Put(ctx, metadata.InodeSymlinkKey(e.ino), []byte(e.target))
			}
		default:
			_, err = store.AtomicCreateFile(ctx, e.parent, e.name, e.ino, e.mode, e.uid, e.gid, metadata.CreateExtra{})
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating %d/%s (ino %d): %v\n", e.parent, e.name, e.ino, err)
			// Continue — might be a duplicate
			continue
		}
		fmt.Printf("  created %s/%s → ino %d\n", metadata.DirentKey(e.parent, ""), e.name, e.ino)
	}

	fmt.Println("Seed complete.")
}
