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

// The barrier's device flush has to come after every run of a flush is written
// and before flushData returns, because its caller commits the extents next.
func TestFlushDataSyncsOnceAfterAllRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dev.img")
	if err := os.WriteFile(path, make([]byte, 1<<20), 0o600); err != nil {
		t.Fatalf("create device file: %v", err)
	}
	dev, err := blockio.OpenBuffered(path)
	if err != nil {
		t.Fatalf("open device: %v", err)
	}
	t.Cleanup(func() { _ = dev.Close() })

	s := &Service{alloc: arena.NewAllocator("node-test", nil)}
	s.setBlockDevice(dev, true)
	ss := dev.SectorSize()

	syncs, written := 0, 0
	s.syncDevice = func() error {
		syncs++
		buf := make([]byte, 3*ss)
		if _, err := dev.ReadAt(buf, 0); err != nil {
			return err
		}
		for _, b := range buf[:ss] {
			if b == 1 {
				written++
				break
			}
		}
		for _, b := range buf[2*ss:] {
			if b == 2 {
				written++
				break
			}
		}
		return nil
	}

	run := func(v byte) []byte {
		b := make([]byte, ss)
		for i := range b {
			b[i] = v
		}
		return b
	}
	p := &pending{data: []bufferedRun{
		{diskOff: uint64(2 * ss), buf: run(2)},
		{diskOff: 0, buf: run(1)},
	}}
	if err := s.flushData(p); err != nil {
		t.Fatalf("flushData: %v", err)
	}
	if syncs != 1 {
		t.Fatalf("device synced %d times, want once", syncs)
	}
	if written != 2 {
		t.Fatalf("device synced before both runs were written")
	}

	// A failed flush fails the write, so its extents are never committed.
	s.syncDevice = func() error { return os.ErrInvalid }
	p = &pending{data: []bufferedRun{{diskOff: 0, buf: run(3)}}}
	if err := s.flushData(p); err == nil {
		t.Fatal("flushData succeeded despite a failed device flush")
	}
}
