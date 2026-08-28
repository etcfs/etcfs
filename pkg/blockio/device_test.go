// nolint: errcheck
package blockio

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	f, err := os.CreateTemp("", "blockio-test-*")
	require.NoError(t, err)
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	require.NoError(t, os.Truncate(name, 4096))

	dev, err := OpenBuffered(name)
	require.NoError(t, err)
	defer dev.Close()

	assert.Greater(t, dev.SectorSize(), 0)
	assert.Greater(t, dev.TotalSize(), int64(0))
}

func TestReadWriteAligned(t *testing.T) {
	f, err := os.CreateTemp("", "blockio-test-*")
	require.NoError(t, err)
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	require.NoError(t, os.Truncate(name, 4096*10))

	dev, err := OpenBuffered(name)
	require.NoError(t, err)
	defer dev.Close()

	ss := dev.SectorSize()
	require.Greater(t, ss, 0)

	buf, err := AlignedBuffer(ss, ss)
	require.NoError(t, err)
	defer FreeBuffer(buf)

	pattern := buf[:ss]
	for i := range pattern {
		pattern[i] = byte(i%256) ^ 0xAA
	}

	n, err := dev.WriteAt(pattern, int64(ss))
	require.NoError(t, err)
	assert.Equal(t, ss, n)

	readBuf, err := AlignedBuffer(ss, ss)
	require.NoError(t, err)
	defer FreeBuffer(readBuf)

	n, err = dev.ReadAt(readBuf[:ss], int64(ss))
	require.NoError(t, err)
	assert.Equal(t, ss, n)
	assert.Equal(t, pattern, readBuf[:ss])
}

func TestSyncRange(t *testing.T) {
	f, err := os.CreateTemp("", "blockio-test-*")
	require.NoError(t, err)
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	require.NoError(t, os.Truncate(name, 4096*10))

	dev, err := OpenBuffered(name)
	require.NoError(t, err)
	defer dev.Close()

	ss := dev.SectorSize()
	buf, err := AlignedBuffer(ss, ss)
	require.NoError(t, err)
	defer FreeBuffer(buf)

	_, err = dev.WriteAt(buf[:ss], 0)
	require.NoError(t, err)
	assert.NoError(t, dev.SyncRange(0, int64(ss)))
}

func TestAlignedBuffer(t *testing.T) {
	ss := 512
	buf, err := AlignedBuffer(ss, ss)
	require.NoError(t, err)
	defer FreeBuffer(buf)
	assert.GreaterOrEqual(t, len(buf), ss)
	assert.Equal(t, 0, len(buf)%ss)
}

func TestUnmapFreesBuffer(t *testing.T) {
	buf, err := AlignedBuffer(4096, 4096)
	require.NoError(t, err)
	assert.NoError(t, FreeBuffer(buf))
}

func TestSectorSizeNonZero(t *testing.T) {
	dev, err := OpenBuffered("/dev/zero")
	if err != nil {
		t.Skip("cannot open /dev/zero")
	}
	defer dev.Close()
	assert.Greater(t, dev.SectorSize(), 0)
}

// Buffered I/O on a shared device is a correctness change, not a fallback: the
// readback that is supposed to make a write visible to the other attachers is
// served from the same page cache the write landed in. Open must therefore
// fail rather than degrade.
func TestOpenRefusesToFallBackToBufferedIO(t *testing.T) {
	// tmpfs does not support O_DIRECT, which is exactly the fallback case.
	name := "/dev/shm/etcfs-odirect-test"
	f, err := os.Create(name)
	if err != nil {
		t.Skipf("no tmpfs available: %v", err)
	}
	_ = f.Close()
	defer func() { _ = os.Remove(name) }()

	if dev, oerr := Open(name); oerr == nil {
		_ = dev.Close()
		t.Skip("this filesystem supports O_DIRECT, nothing to fall back from")
	}
	dev, err := OpenBuffered(name)
	if err != nil {
		t.Fatalf("OpenBuffered: %v", err)
	}
	defer func() { _ = dev.Close() }()
	if dev.IsDirect() {
		t.Error("IsDirect reported true after a buffered fallback")
	}
}

func TestRefreshSizePicksUpGrowth(t *testing.T) {
	f, err := os.CreateTemp("", "blockio-test-*")
	require.NoError(t, err)
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	require.NoError(t, os.Truncate(name, 4096))
	dev, err := OpenBuffered(name)
	require.NoError(t, err)
	defer dev.Close()
	require.Equal(t, int64(4096), dev.TotalSize())

	// A volume grown underneath a mounted filesystem: the size read at open is
	// stale until something re-reads it.
	require.NoError(t, os.Truncate(name, 8192))
	size, err := dev.RefreshSize()
	require.NoError(t, err)
	assert.Equal(t, int64(8192), size)
	assert.Equal(t, int64(8192), dev.TotalSize())

	// Shrinking is not a supported operation and must not be acted on: arenas
	// already handed out from the tail would be stranded.
	require.NoError(t, os.Truncate(name, 4096))
	size, err = dev.RefreshSize()
	require.NoError(t, err)
	assert.Equal(t, int64(8192), size)
}

// A failing device operation must come back as an error, not as a panic. The
// byte counter is a Prometheus counter, which panics rather than go backwards,
// and a failed pread/pwrite returns -1 — so a device that starts erroring
// (a volume detached under a fenced node, say) used to take the IPC handler
// down with it instead of returning EIO.
func TestFailedIODoesNotPanicTheByteCounter(t *testing.T) {
	f, err := os.CreateTemp("", "blockio-test-*")
	require.NoError(t, err)
	name := f.Name()
	f.Close()
	defer os.Remove(name)
	require.NoError(t, os.Truncate(name, 4096*10))

	dev, err := OpenBuffered(name)
	require.NoError(t, err)

	buf, err := AlignedBuffer(dev.SectorSize(), dev.SectorSize())
	require.NoError(t, err)
	defer func() { _ = FreeBuffer(buf) }()

	// Closing the device makes every subsequent syscall fail on a bad fd,
	// which is the -1 the counter used to be handed.
	require.NoError(t, dev.Close())

	require.NotPanics(t, func() {
		n, err := dev.WriteAt(buf, 0)
		require.Error(t, err)
		require.LessOrEqual(t, n, 0)

		n, err = dev.ReadAt(buf, 0)
		require.Error(t, err)
		require.LessOrEqual(t, n, 0)
	})
}
