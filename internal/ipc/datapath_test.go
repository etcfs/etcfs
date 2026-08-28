package ipc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/etcfs/etcfs/pkg/arena"
	"github.com/etcfs/etcfs/pkg/blockio"
)

// A device without O_DIRECT holds written bytes in this node's page cache, so
// the barriers have to come back on whatever the configuration asked for.
func TestBufferedDeviceForcesWriteBarriers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dev.img")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create device file: %v", err)
	}
	if err := f.Truncate(1 << 20); err != nil {
		t.Fatalf("size device file: %v", err)
	}
	_ = f.Close()

	dev, err := blockio.OpenBuffered(path)
	if err != nil {
		t.Fatalf("open device: %v", err)
	}
	t.Cleanup(func() { _ = dev.Close() })

	s := &Service{alloc: arena.NewAllocator("node-test", nil)}
	s.setBlockDevice(dev, false)

	if dev.IsDirect() {
		t.Skip("this filesystem supports O_DIRECT, nothing to force barriers for")
	}
	if !s.writeBarriers {
		t.Fatal("buffered device left write barriers off")
	}
}
