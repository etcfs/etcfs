package ipc

// The errnos the IPC layer answers with, negated as the wire carries them: a
// response's first word is an int32, negative for a failure.
//
// Named rather than written as bare literals at each call site, because `-11`
// against `-22` is a typo the compiler cannot see — it turns an
// "inode is busy, retry" into "you passed nonsense", which the kernel reports
// to the application as EINVAL and nothing in the daemon ever contradicts.
//
// Linux values, matching the C side's own errno.h rather than Go's syscall
// package: the number crosses the socket and is handed to fuse_reply_err by a
// C daemon on the same host.
const (
	errPerm     = -1  // EPERM
	errNoEnt    = -2  // ENOENT
	errIO       = -5  // EIO
	errNXIO     = -6  // ENXIO
	err2Big     = -7  // E2BIG
	errNoMem    = -12 // ENOMEM
	errAgain    = -11 // EAGAIN
	errExist    = -17 // EEXIST
	errNotDir   = -20 // ENOTDIR
	errIsDir    = -21 // EISDIR
	errInval    = -22 // EINVAL
	errNoSpace  = -28 // ENOSPC
	errROFS     = -30 // EROFS
	errNoSys    = -38 // ENOSYS
	errNotEmpty = -39 // ENOTEMPTY
	errNoData   = -61 // ENODATA
	errNotSupp  = -95 // EOPNOTSUPP
)
