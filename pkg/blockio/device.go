package blockio

import (
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/etcfs/etcfs/pkg/metrics"
)

const (
	blkSSZGet    = 0x1268
	blkGetSize64 = 0x80081272
	blkFlushBuf  = 0x1261
)

type Device struct {
	fd         int
	path       string
	sectorSize int
	direct     bool

	// totalSize is re-read whenever the device may have grown underneath the
	// running process (see RefreshSize), so it is read by the I/O paths while
	// another goroutine is writing it.
	totalSize atomic.Int64
}

// Open opens the device for data I/O, requiring O_DIRECT.
//
// On a device attached to more than one node, buffered I/O is not a
// degradation but a correctness change: a write lands in this node's page
// cache, and the readback that is supposed to push it out to the other
// attachers is served from that same cache — so the round trip proves nothing,
// and two nodes believe they share bytes only one of them has written.
//
// Use OpenBuffered only where the device is not shared.
func Open(path string) (*Device, error) {
	return open(path, false)
}

// OpenBuffered opens the device, accepting a fall back to buffered I/O when
// O_DIRECT is unavailable.  Correct only for a single-node mount or a
// file-backed test device; see Open.
func OpenBuffered(path string) (*Device, error) {
	return open(path, true)
}

func open(path string, allowBuffered bool) (*Device, error) {
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_DIRECT, 0)
	direct := (err == nil)
	if err != nil {
		if !allowBuffered {
			return nil, fmt.Errorf("open %s with O_DIRECT: %w", path, err)
		}
		fd, err = syscall.Open(path, syscall.O_RDWR, 0)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", path, err)
		}
	}

	d := &Device{fd: fd, path: path, direct: direct}
	if err := d.queryGeometry(); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}

	return d, nil
}

func (d *Device) FlushDevice() error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(d.fd), blkFlushBuf, 0)
	if errno != 0 && errno != syscall.ENOTTY {
		return errno
	}
	return nil
}

func (d *Device) IsDirect() bool { return d.direct }

func (d *Device) queryGeometry() error {
	d.sectorSize = 512

	var sec uint32
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(d.fd), blkSSZGet,
		uintptr(unsafe.Pointer(&sec)))
	if errno == 0 && sec > 0 {
		d.sectorSize = int(sec)
	}

	size, err := d.readSize()
	if err != nil {
		return err
	}
	d.totalSize.Store(size)

	return nil
}

// readSize asks the kernel how large the device is right now.
func (d *Device) readSize() (int64, error) {
	var bs uint64
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(d.fd), blkGetSize64,
		uintptr(unsafe.Pointer(&bs)))
	if errno == 0 {
		return int64(bs), nil
	}
	var stat unix.Stat_t
	if err := unix.Fstat(d.fd, &stat); err != nil {
		return 0, fmt.Errorf("stat: %w", err)
	}
	return stat.Size, nil
}

// RefreshSize re-reads the device size and returns it.  A shared volume can be
// grown while every node that uses it stays mounted — an EBS volume is the
// case that matters — and the new space is then invisible to a size read once
// at open: no arena can be handed out from it and statfs under-reports.
//
// A size that came back smaller is ignored.  Shrinking a volume under a live
// filesystem is not a supported operation, and acting on the smaller number
// would strand arenas that are already in use.
func (d *Device) RefreshSize() (int64, error) {
	size, err := d.readSize()
	if err != nil {
		return d.TotalSize(), err
	}
	for {
		known := d.totalSize.Load()
		if size <= known {
			return known, nil
		}
		if d.totalSize.CompareAndSwap(known, size) {
			return size, nil
		}
	}
}

func (d *Device) SectorSize() int  { return d.sectorSize }
func (d *Device) TotalSize() int64 { return d.totalSize.Load() }
func (d *Device) Path() string     { return d.path }

func (d *Device) ReadAt(buf []byte, offset int64) (int, error) {
	if d.direct {
		if offset%int64(d.sectorSize) != 0 || len(buf)%d.sectorSize != 0 {
			return 0, fmt.Errorf("misaligned O_DIRECT read: off=%d len=%d sector=%d",
				offset, len(buf), d.sectorSize)
		}
	}
	start := time.Now()
	n, err := unix.Pread(d.fd, buf, offset)
	metrics.BlockIODuration.WithLabelValues("read").Observe(time.Since(start).Seconds())
	metrics.BlockIO.WithLabelValues("read").Inc()
	// A failed pread returns -1, and a Prometheus counter panics rather than
	// go backwards — so counting it unconditionally turns every device error
	// into a panic in the handler that was going to return EIO cleanly.
	if n > 0 {
		metrics.BlockIOBytes.WithLabelValues("read").Add(float64(n))
	}
	return n, err
}

func (d *Device) WriteAt(buf []byte, offset int64) (int, error) {
	if d.direct {
		if offset%int64(d.sectorSize) != 0 || len(buf)%d.sectorSize != 0 {
			return 0, fmt.Errorf("misaligned O_DIRECT write: off=%d len=%d sector=%d",
				offset, len(buf), d.sectorSize)
		}
	}
	start := time.Now()
	n, err := unix.Pwrite(d.fd, buf, offset)
	metrics.BlockIODuration.WithLabelValues("write").Observe(time.Since(start).Seconds())
	metrics.BlockIO.WithLabelValues("write").Inc()
	// See ReadAt: -1 from a failed pwrite would panic the counter.
	if n > 0 {
		metrics.BlockIOBytes.WithLabelValues("write").Add(float64(n))
	}
	return n, err
}

func (d *Device) SyncRange(offset int64, length int64) error {
	align := int64(d.sectorSize)
	offAligned := offset & ^(align - 1)
	lenAligned := length + (offset - offAligned)
	_, _, errno := syscall.Syscall6(syscall.SYS_SYNC_FILE_RANGE, uintptr(d.fd),
		uintptr(offAligned), uintptr(lenAligned),
		syncFileRangeWrite|syncFileRangeWaitAfter, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

const (
	syncFileRangeWrite     uintptr = 2
	syncFileRangeWaitAfter uintptr = 4
)

func (d *Device) Close() error {
	return syscall.Close(d.fd)
}

func AlignedBuffer(size int, align int) ([]byte, error) {
	alloc := (size + align - 1) / align * align
	if alloc < os.Getpagesize() {
		alloc = os.Getpagesize()
	}
	b, err := unix.Mmap(-1, 0, alloc, unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANONYMOUS|unix.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func FreeBuffer(buf []byte) error {
	return unix.Munmap(buf)
}
