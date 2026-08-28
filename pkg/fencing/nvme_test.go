package fencing

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/etcfs/etcfs/pkg/nvmeresv"
)

// fakeNamespace models the reservation state that matters: a set of
// registered keys, where a preempt removes one and a report shows what
// remains.  Same pattern as fakeEC2 in detach_test.go.
type fakeNamespace struct {
	registered  map[uint64]bool
	preempts    int
	registerErr error
	preemptErr  error
	reportErr   error
	// ignorePreempt models the failure this fencer exists to catch: the
	// command reports success but the registration survives.
	ignorePreempt bool
}

func newFakeNamespace(keys ...uint64) *fakeNamespace {
	f := &fakeNamespace{registered: map[uint64]bool{}}
	for _, k := range keys {
		f.registered[k] = true
	}
	return f
}

func (f *fakeNamespace) Register(key uint64) error {
	if f.registerErr != nil {
		return f.registerErr
	}
	f.registered[key] = true
	return nil
}

func (f *fakeNamespace) Acquire(uint64) error { return nil }

func (f *fakeNamespace) Preempt(_, victim uint64) error {
	f.preempts++
	if f.preemptErr != nil {
		return f.preemptErr
	}
	if !f.ignorePreempt {
		delete(f.registered, victim)
	}
	return nil
}

func (f *fakeNamespace) Report() (nvmeresv.Report, error) {
	if f.reportErr != nil {
		return nvmeresv.Report{}, f.reportErr
	}
	r := nvmeresv.Report{Type: nvmeresv.TypeWriteExclusiveAllRegistrants}
	for k := range f.registered {
		r.Registrants = append(r.Registrants, nvmeresv.Registrant{Key: k})
	}
	return r, nil
}

func TestNVMeFencer_RegistersOwnKeyOnStart(t *testing.T) {
	ns := newFakeNamespace()
	_, err := newNVMeFencer(ns, "n1")
	require.NoError(t, err)
	assert.True(t, ns.registered[nvmeresv.KeyForNode("n1")],
		"an unregistered node can neither preempt nor be preempted")
}

func TestNVMeFencer_RegisterFailureIsFatal(t *testing.T) {
	ns := newFakeNamespace()
	ns.registerErr = errors.New("nvme status 0x83")
	_, err := newNVMeFencer(ns, "n1")
	require.Error(t, err, "starting without a registration would leave the node unfenceable")
}

func TestNVMeFencer_PreemptsVictimKey(t *testing.T) {
	ns := newFakeNamespace(nvmeresv.KeyForNode("n2"))
	f, err := newNVMeFencer(ns, "n1")
	require.NoError(t, err)

	require.NoError(t, f.Fence(context.Background(), "n2", ""))

	assert.Equal(t, 1, ns.preempts)
	assert.False(t, ns.registered[nvmeresv.KeyForNode("n2")])
	assert.True(t, ns.registered[nvmeresv.KeyForNode("n1")], "fencing a peer must not eject ourselves")
}

// The failure direction that matters: reporting a fence that did not happen
// authorises peers to reclaim a node's arenas while it can still write.
func TestNVMeFencer_SurvivingRegistrationIsNotAFence(t *testing.T) {
	ns := newFakeNamespace(nvmeresv.KeyForNode("n2"))
	ns.ignorePreempt = true
	f, err := newNVMeFencer(ns, "n1")
	require.NoError(t, err)

	err = f.Fence(context.Background(), "n2", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "still registered")
}

func TestNVMeFencer_UnreadableReportIsNotProof(t *testing.T) {
	ns := newFakeNamespace(nvmeresv.KeyForNode("n2"))
	f, err := newNVMeFencer(ns, "n1")
	require.NoError(t, err)
	ns.reportErr = errors.New("nvme status 0x2")

	require.Error(t, f.Fence(context.Background(), "n2", ""))
}

func TestNVMeFencer_PreemptErrorPropagates(t *testing.T) {
	ns := newFakeNamespace(nvmeresv.KeyForNode("n2"))
	f, err := newNVMeFencer(ns, "n1")
	require.NoError(t, err)
	ns.preemptErr = errors.New("nvme status 0x83")

	require.Error(t, f.Fence(context.Background(), "n2", ""))
}

// A node ID equal to our own means the controller saw our own membership key
// expire; preempting ourselves would cut the surviving node off its own disk.
func TestNVMeFencer_RefusesToPreemptItself(t *testing.T) {
	ns := newFakeNamespace()
	f, err := newNVMeFencer(ns, "n1")
	require.NoError(t, err)

	err = f.Fence(context.Background(), "n1", "")
	require.Error(t, err)
	assert.Zero(t, ns.preempts)
}
