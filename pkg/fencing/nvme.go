package fencing

import (
	"context"
	"fmt"

	"github.com/etcfs/etcfs/pkg/nvmeresv"
)

// reservationDevice is the slice of pkg/nvmeresv this package uses, extracted
// so tests can substitute a fake without an NVMe namespace.
type reservationDevice interface {
	Register(key uint64) error
	Acquire(key uint64) error
	Preempt(selfKey, victimKey uint64) error
	Report() (nvmeresv.Report, error)
}

// NVMeFencer fences a node by preempting its NVMe reservation key on the
// shared device.
//
// This is the strongest of the fencing mechanisms and the only one that is
// enforced by the resource itself.  Where EBSDetacher *requests* a detach and
// then polls for evidence that it happened, a preempt is synchronous: when it
// returns, the preempted host's writes already fail at write(2) with EBADE,
// zero bytes reaching the device.  There is no residual-I/O window to reason
// about, which is what makes reclaiming a fenced node's arenas safe
// immediately rather than after a grace period nobody can size correctly.
//
// The reservation type is Write Exclusive – All Registrants, the only type
// that allows every node to write concurrently while still permitting one to
// be ejected individually.
type NVMeFencer struct {
	dev     reservationDevice
	selfKey uint64
}

// NewNVMeFencer opens the shared namespace, registers this node's key, and
// acquires the shared reservation.
//
// Acquire is attempted unconditionally rather than only by the first node.
// Under Write Exclusive – All Registrants the reservation is shared, so a
// later node's acquire is redundant when a peer already holds it; the device
// reports that as a reservation conflict, which is not a failure to fence and
// is therefore tolerated.  Registration, by contrast, must succeed: an
// unregistered node can neither be preempted nor preempt anyone.
func NewNVMeFencer(devicePath, nodeID string) (*NVMeFencer, error) {
	dev, err := nvmeresv.Open(devicePath)
	if err != nil {
		return nil, err
	}
	return newNVMeFencer(dev, nodeID)
}

func newNVMeFencer(dev reservationDevice, nodeID string) (*NVMeFencer, error) {
	key := nvmeresv.KeyForNode(nodeID)
	if err := dev.Register(key); err != nil {
		return nil, fmt.Errorf("nvme fencing: register key for %s: %w", nodeID, err)
	}
	_ = dev.Acquire(key)
	return &NVMeFencer{dev: dev, selfKey: key}, nil
}

// Fence implements Fencer.  The EC2 instance ID is unused: reservations are
// addressed by the key derived from the node ID, so fencing needs nothing
// from the cloud control plane and works identically on any NVMe namespace
// that supports reservations.
func (f *NVMeFencer) Fence(_ context.Context, nodeID, _ string) error {
	victim := nvmeresv.KeyForNode(nodeID)
	if victim == f.selfKey {
		return fmt.Errorf("nvme fencing: refusing to preempt own key for %s", nodeID)
	}
	if err := f.dev.Preempt(f.selfKey, victim); err != nil {
		return fmt.Errorf("nvme fencing: preempt %s: %w", nodeID, err)
	}

	// Confirm rather than trust.  A successful preempt should already mean
	// the registration is gone, but the generation bump that follows this
	// call is what licenses peers to reclaim the fenced node's space — the
	// same reason EBSDetacher re-reads the volume instead of trusting
	// DetachVolume's return.
	report, err := f.dev.Report()
	if err != nil {
		return fmt.Errorf("nvme fencing: confirm preempt of %s: %w", nodeID, err)
	}
	if report.Holds(victim) {
		return fmt.Errorf("nvme fencing: %s still registered after preempt", nodeID)
	}
	return nil
}
