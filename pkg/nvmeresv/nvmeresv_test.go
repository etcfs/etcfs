package nvmeresv

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeNVMe captures the command encoding instead of reaching a device.  The
// encoding is the whole of this package's behaviour — a wrong cdw10 bit is
// the difference between ejecting a node and silently doing nothing — so it
// is what the tests assert on.
type fakeNVMe struct {
	cmds  []passthruCmd
	data  [][]byte
	err   error
	reply []byte // copied into the command's data buffer, for Report
}

func (f *fakeNVMe) submit(cmd *passthruCmd, data []byte) error {
	f.cmds = append(f.cmds, *cmd)
	f.data = append(f.data, append([]byte(nil), data...))
	if f.reply != nil {
		copy(data, f.reply)
	}
	return f.err
}

func testDevice(f *fakeNVMe) *Device {
	d := &Device{fd: -1, nsid: 1}
	d.submit = f.submit
	return d
}

func TestRegisterEncodesNewKey(t *testing.T) {
	f := &fakeNVMe{}
	require.NoError(t, testDevice(f).Register(0xAABB))

	require.Len(t, f.cmds, 1)
	assert.Equal(t, uint8(opReservationRegister), f.cmds[0].opcode)
	assert.Equal(t, uint32(registerActionRegister), f.cmds[0].cdw10)
	assert.Equal(t, uint64(0), binary.LittleEndian.Uint64(f.data[0][0:8]), "current key must be 0 when registering")
	assert.Equal(t, uint64(0xAABB), binary.LittleEndian.Uint64(f.data[0][8:16]))
}

func TestAcquireUsesWriteExclusiveAllRegistrants(t *testing.T) {
	f := &fakeNVMe{}
	require.NoError(t, testDevice(f).Acquire(0x11))

	assert.Equal(t, uint8(opReservationAcquire), f.cmds[0].opcode)
	assert.Equal(t, uint32(acquireActionAcquire), f.cmds[0].cdw10&0x7, "action bits")
	assert.Equal(t, uint32(TypeWriteExclusiveAllRegistrants), (f.cmds[0].cdw10>>8)&0xFF, "reservation type")
}

// The fencing primitive: the victim's key goes in the second slot, this
// node's own key in the first.  Swapping them would preempt nobody.  The
// action must be Preempt and Abort: plain Preempt leaves the victim's
// in-flight writes running after the fence has returned.
func TestPreemptEncodesVictimKey(t *testing.T) {
	f := &fakeNVMe{}
	require.NoError(t, testDevice(f).Preempt(0x1111, 0x2222))

	assert.Equal(t, uint32(acquireActionPreemptAbort), f.cmds[0].cdw10&0x7, "preempt and abort")
	assert.Equal(t, uint32(TypeWriteExclusiveAllRegistrants), (f.cmds[0].cdw10>>8)&0xFF, "reservation type")
	assert.Equal(t, uint64(0x1111), binary.LittleEndian.Uint64(f.data[0][0:8]), "current key is our own")
	assert.Equal(t, uint64(0x2222), binary.LittleEndian.Uint64(f.data[0][8:16]), "preempt key is the victim's")
}

func TestCommandErrorPropagates(t *testing.T) {
	f := &fakeNVMe{err: errors.New("nvme status 0x83")}
	err := testDevice(f).Preempt(1, 2)
	require.Error(t, err, "a reservation conflict must not read as a successful fence")
}

func TestReportParsesRegistrants(t *testing.T) {
	buf := make([]byte, reportBufLen)
	binary.LittleEndian.PutUint32(buf[0:4], 7) // generation
	buf[4] = TypeWriteExclusiveAllRegistrants
	binary.LittleEndian.PutUint16(buf[5:7], 2) // two registrants

	e0 := buf[reportHeaderLen:]
	binary.LittleEndian.PutUint16(e0[0:2], 1)
	e0[2] = 1 // holds the reservation
	binary.LittleEndian.PutUint64(e0[14:22], 0xDEAD)

	e1 := buf[reportHeaderLen+registrantLen:]
	binary.LittleEndian.PutUint16(e1[0:2], 2)
	binary.LittleEndian.PutUint64(e1[14:22], 0xBEEF)

	f := &fakeNVMe{reply: buf}
	r, err := testDevice(f).Report()
	require.NoError(t, err)

	assert.Equal(t, uint32(7), r.Generation)
	require.Len(t, r.Registrants, 2)
	assert.Equal(t, uint64(0xDEAD), r.Registrants[0].Key)
	assert.True(t, r.Registrants[0].HoldsReservation)
	assert.False(t, r.Registrants[1].HoldsReservation)
	assert.True(t, r.Holds(0xBEEF))
	assert.False(t, r.Holds(0x1234), "an unregistered key must not read as present")
}

func TestReportRejectsImpossibleRegistrantCount(t *testing.T) {
	buf := make([]byte, reportHeaderLen)
	binary.LittleEndian.PutUint16(buf[5:7], 100)
	_, err := parseReport(buf)
	require.Error(t, err, "a count the buffer cannot hold must not be parsed as zero registrants")
}

func TestKeyForNodeIsStableAndNonZero(t *testing.T) {
	assert.Equal(t, KeyForNode("n1"), KeyForNode("n1"), "keys must be derivable by peers, not assigned")
	assert.NotEqual(t, KeyForNode("n1"), KeyForNode("n2"))
	assert.NotZero(t, KeyForNode("n1"), "0 means 'no key' to the device")
}
