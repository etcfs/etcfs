// Package nvmeresv issues NVMe reservation commands (Register, Acquire,
// Preempt, Release, Report) to a raw NVMe namespace via the kernel's
// passthrough ioctl.
//
// EtcFS uses reservations as a device-enforced fencing token on EBS io2
// Multi-Attach volumes, which have supported the full reservation command set
// since 2023-09-18.  The reservation type used is Write Exclusive – All
// Registrants: every registered host may write concurrently, and any
// registrant may eject another by preempting its reservation key.  A
// preempted host's next write fails synchronously at write(2) with EBADE,
// with zero bytes reaching the device — a real fencing token enforced at the
// resource rather than an advisory agreement between peers.
//
// The commands are NVMe I/O commands, not an AWS API: no SDK covers them, and
// shelling out to nvme-cli is not acceptable on a fencing path.  This package
// therefore builds the passthrough structure by hand, in the same style as
// pkg/blockio/device.go's raw BLKSSZGET/BLKGETSIZE64 ioctls.
package nvmeresv

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"syscall"
	"unsafe"
)

// NVMe reservation opcodes (NVM Command Set, NVMe 1.4 § 6).
const (
	opReservationRegister = 0x0D
	opReservationReport   = 0x0E
	opReservationAcquire  = 0x11
	opReservationRelease  = 0x15
)

// Reservation types (NVMe 1.4 Figure 322).  Only WEAR is used by EtcFS:
// it is the one type that permits concurrent writers while still allowing an
// individual host to be ejected.
const (
	// TypeWriteExclusiveAllRegistrants permits every registrant to write.
	TypeWriteExclusiveAllRegistrants = 5
)

// Register actions (RREGA, cdw10 bits 2:0).
const (
	registerActionRegister   = 0
	registerActionUnregister = 1
)

// Acquire actions (RACQA, cdw10 bits 2:0).
const (
	acquireActionAcquire      = 0
	acquireActionPreemptAbort = 2 // 1 is plain Preempt, never sent
)

// Release actions (RRELA, cdw10 bits 2:0).
const releaseActionRelease = 0

// ioctl numbers.  NVME_IOCTL_ID is _IO('N', 0x40); NVME_IOCTL_IO_CMD is
// _IOWR('N', 0x43, struct nvme_passthru_cmd), whose size is 72 bytes.
const (
	ioctlNVMeID    = 0x4E40
	ioctlNVMeIOCmd = 0xC0484E43
)

// passthruCmd mirrors struct nvme_passthru_cmd from
// include/uapi/linux/nvme_ioctl.h exactly; the kernel copies 72 bytes from
// this address, so field order and padding are load-bearing.
type passthruCmd struct {
	opcode      uint8
	flags       uint8
	rsvd1       uint16
	nsid        uint32
	cdw2        uint32
	cdw3        uint32
	metadata    uint64
	addr        uint64
	metadataLen uint32
	dataLen     uint32
	cdw10       uint32
	cdw11       uint32
	cdw12       uint32
	cdw13       uint32
	cdw14       uint32
	cdw15       uint32
	timeoutMS   uint32
	result      uint32
}

// Device is an open NVMe namespace that reservation commands are issued
// against.
type Device struct {
	fd   int
	path string
	nsid uint32

	// submit is the seam tests replace.  It receives the fully built command
	// and any data buffer the command transfers, so a fake can both assert on
	// the encoding and fill in a Report response.
	submit func(cmd *passthruCmd, data []byte) error
}

// Open opens an NVMe namespace (e.g. /dev/nvme1n1) for reservation commands.
//
// The device is opened read-write because reservation commands are I/O
// commands and the kernel rejects them on a read-only handle.  This is a
// separate descriptor from the one pkg/blockio holds: fencing must keep
// working regardless of what the data path is doing with its own fd.
func Open(path string) (*Device, error) {
	fd, err := syscall.Open(path, syscall.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("nvmeresv: open %s: %w", path, err)
	}
	d := &Device{fd: fd, path: path}
	d.submit = d.submitIoctl

	nsid, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), ioctlNVMeID, 0)
	if errno != 0 {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("nvmeresv: %s is not an NVMe namespace: %w", path, errno)
	}
	d.nsid = uint32(nsid)
	return d, nil
}

// Close releases the namespace descriptor.  It does not unregister: a
// registration is meant to outlive an individual file descriptor, and dropping
// it on close would hand away the node's fencing token every time the process
// restarts cleanly.
func (d *Device) Close() error { return syscall.Close(d.fd) }

// Path returns the namespace path the device was opened from.
func (d *Device) Path() string { return d.path }

func (d *Device) submitIoctl(cmd *passthruCmd, _ []byte) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(d.fd), ioctlNVMeIOCmd,
		uintptr(unsafe.Pointer(cmd)))
	if errno != 0 {
		return errno
	}
	// A non-zero result is an NVMe status code: the command reached the
	// device and the device refused it (most often 0x83, Reservation
	// Conflict).  That is a failure, not a partial success.
	if cmd.result != 0 {
		return fmt.Errorf("nvme status 0x%x", cmd.result)
	}
	return nil
}

func (d *Device) exec(opcode uint8, cdw10, cdw11 uint32, data []byte) error {
	cmd := passthruCmd{
		opcode:    opcode,
		nsid:      d.nsid,
		cdw10:     cdw10,
		cdw11:     cdw11,
		timeoutMS: 15000,
	}
	if len(data) > 0 {
		cmd.addr = uint64(uintptr(unsafe.Pointer(&data[0])))
		cmd.dataLen = uint32(len(data))
	}
	if err := d.submit(&cmd, data); err != nil {
		return err
	}
	return nil
}

// keyPair encodes the (current key, new/preempt key) payload shared by
// Register, Acquire and Preempt.
func keyPair(current, other uint64) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], current)
	binary.LittleEndian.PutUint64(buf[8:16], other)
	return buf
}

// Register registers key as this host's reservation key on the namespace.
//
// Registration is idempotent from the caller's point of view only in the
// sense that re-registering the same key after a preempt is how a node
// rejoins; registering while already holding a different key is a conflict
// and is reported as one.
func (d *Device) Register(key uint64) error {
	return d.exec(opReservationRegister, registerActionRegister, 0, keyPair(0, key))
}

// Unregister removes this host's registration.  Used on graceful departure so
// a leaving node stops appearing as a registrant.
func (d *Device) Unregister(key uint64) error {
	return d.exec(opReservationRegister, registerActionUnregister, 0, keyPair(key, 0))
}

// Acquire takes a Write Exclusive – All Registrants reservation using key.
//
// Under WEAR the reservation is shared: the first registrant to acquire holds
// it and later registrants may write without acquiring again, so an acquire
// that reports a reservation conflict because a peer already holds the
// reservation is not an error condition for the caller — see Fencer's use.
func (d *Device) Acquire(key uint64) error {
	cdw10 := uint32(acquireActionAcquire) | uint32(TypeWriteExclusiveAllRegistrants)<<8
	return d.exec(opReservationAcquire, cdw10, 0, keyPair(key, 0))
}

// Preempt ejects the registrant holding victimKey, using this host's own
// registered key as the current key.
//
// This is the fencing primitive, sent as Preempt and Abort rather than plain
// Preempt.  Both remove the victim's registration, so its later writes fail at
// the device with EBADE.  Only Preempt and Abort also covers the writes it had
// already submitted: the NVMe Base Specification (2.0a, section 8.19) holds
// the command's completion until every command the victim had in progress on
// the namespace has completed, aborted or not.  A plain Preempt returns with
// those writes possibly still executing, and the fence that follows hands the
// victim's arenas to a peer, which an in-flight write would then overwrite.
func (d *Device) Preempt(selfKey, victimKey uint64) error {
	cdw10 := uint32(acquireActionPreemptAbort) | uint32(TypeWriteExclusiveAllRegistrants)<<8
	return d.exec(opReservationAcquire, cdw10, 0, keyPair(selfKey, victimKey))
}

// Release releases the reservation held with key.  Registration survives; only
// the reservation is dropped.
func (d *Device) Release(key uint64) error {
	cdw10 := uint32(releaseActionRelease) | uint32(TypeWriteExclusiveAllRegistrants)<<8
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, key)
	return d.exec(opReservationRelease, cdw10, 0, buf)
}

// Registrant is one registered controller in a reservation report.
type Registrant struct {
	ControllerID uint16
	Key          uint64
	// HoldsReservation reflects RCSTS bit 0 — whether this controller holds
	// the reservation, as opposed to merely being registered.
	HoldsReservation bool
}

// Report is the parsed Reservation Status data structure.
type Report struct {
	Generation  uint32
	Type        uint8
	Registrants []Registrant
}

// Holds reports whether key is currently registered on the namespace.  This
// is how a preempt is confirmed: the victim's key must be gone.
func (r Report) Holds(key uint64) bool {
	for _, reg := range r.Registrants {
		if reg.Key == key {
			return true
		}
	}
	return false
}

const (
	reportHeaderLen = 24
	registrantLen   = 24
	// reportBufLen holds the header plus 64 registrants — far beyond the
	// 16-attachment ceiling EBS Multi-Attach imposes, so a single fixed-size
	// request always suffices and no two-phase size negotiation is needed.
	reportBufLen = reportHeaderLen + 64*registrantLen
)

// Report reads the namespace's current reservation status.
func (d *Device) Report() (Report, error) {
	buf := make([]byte, reportBufLen)
	// NUMD is the transfer size in dwords, zero-based.
	numd := uint32(len(buf)/4 - 1)
	if err := d.exec(opReservationReport, numd, 0, buf); err != nil {
		return Report{}, fmt.Errorf("nvmeresv: report: %w", err)
	}
	return parseReport(buf)
}

// parseReport decodes the Reservation Status data structure (NVMe 1.4
// Figure 316): a 24-byte header followed by REGCTL 24-byte registered
// controller entries.
func parseReport(buf []byte) (Report, error) {
	if len(buf) < reportHeaderLen {
		return Report{}, fmt.Errorf("nvmeresv: report truncated: %d bytes", len(buf))
	}
	r := Report{
		Generation: binary.LittleEndian.Uint32(buf[0:4]),
		Type:       buf[4],
	}
	count := int(binary.LittleEndian.Uint16(buf[5:7]))
	if need := reportHeaderLen + count*registrantLen; len(buf) < need {
		return Report{}, fmt.Errorf("nvmeresv: report claims %d registrants, buffer holds %d bytes",
			count, len(buf))
	}
	r.Registrants = make([]Registrant, 0, count)
	for i := 0; i < count; i++ {
		e := buf[reportHeaderLen+i*registrantLen:]
		r.Registrants = append(r.Registrants, Registrant{
			ControllerID:     binary.LittleEndian.Uint16(e[0:2]),
			HoldsReservation: e[2]&1 != 0,
			Key:              binary.LittleEndian.Uint64(e[14:22]),
		})
	}
	return r, nil
}

// KeyForNode derives a node's reservation key from its node ID.
//
// Deriving rather than assigning means any node can compute the key of the
// node it must fence without a registry, and a node that rejoins after being
// preempted re-registers under the same key.  Reuse is safe because the
// generation guard, not the key, is what distinguishes epochs: a preempted
// node cannot mutate metadata again until it restarts and re-reads its
// generation, and its writes stay blocked at the device until it re-registers
// deliberately.
//
// Key 0 is reserved by the spec (it means "no key"), so a hash collapsing to
// zero is nudged to 1.
func KeyForNode(nodeID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(nodeID))
	if k := h.Sum64(); k != 0 {
		return k
	}
	return 1
}
