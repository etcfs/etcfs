package ipc

import (
	"sync"
	"time"
)

// Where a directory's last listing stopped.
//
// A READDIR carries an offset, and an offset is a *position* — "skip the first
// N names".  etcd cannot skip: a range starts at a key, so serving position N
// by reading means reading the N keys before it and throwing them away.  Doing
// that once per page makes one scan of a directory read the whole directory
// once per page, which is quadratic in its size: measured at a second per
// thousand entries.
//
// The way out is that a scan is sequential: the offset a READDIR asks for is
// almost always the one the previous reply ended on.  So the daemon remembers
// the last name it handed out for a directory, and a request resuming from
// exactly there is answered by reading forward from that name instead of from
// the beginning.  Anything else — a seekdir, a second process scanning the same
// directory, a cursor that has expired — misses and falls back to reading the
// directory and slicing by position.
//
// Nothing here is a cache of directory *contents*: a cursor is a name used as
// the start of a fresh linearizable range read, so a page served from it cannot
// return a deleted name or hide an existing one.  A miss is weaker.  Its offset
// is only a position, so a name removed before it since the previous page makes
// one unrelated name be skipped, and one added makes a name repeat.  libfuse
// asks for offsets that never do that; POSIX only leaves unspecified whether the
// added or removed name itself appears.
type dirCursor struct {
	offset uint64 // the position the next request must ask for
	name   string // the last name handed out, to resume after
	used   time.Time
}

const (
	// dirCursorMax bounds how many directories are tracked at once.  One entry
	// per directory being scanned, not per page, so this is far more than a
	// walk of a large tree needs concurrently.
	dirCursorMax = 1024

	// dirCursorTTL drops a cursor whose scan was abandoned.  A scan that pauses
	// longer than this pays one full read to resume, the cost every page would
	// carry without a cursor.
	dirCursorTTL = 60 * time.Second
)

type dirCursors struct {
	mu sync.Mutex
	m  map[uint64]dirCursor
}

func newDirCursors() *dirCursors {
	return &dirCursors{m: make(map[uint64]dirCursor)}
}

// resumeAt returns the name to read forward from for a request at this offset,
// and whether there was one.  An offset of zero never resumes: it is the start
// of a listing by definition.
func (c *dirCursors) resumeAt(ino, offset uint64) (string, bool) {
	if offset == 0 {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	cur, found := c.m[ino]
	if !found || cur.offset != offset || time.Since(cur.used) > dirCursorTTL {
		return "", false
	}
	cur.used = time.Now()
	c.m[ino] = cur
	return cur.name, true
}

// record notes where a reply stopped, so the request that continues from it can
// resume rather than count.  An empty name means the listing ended and there is
// nothing to continue from.
func (c *dirCursors) record(ino, offset uint64, name string) {
	if name == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.m) >= dirCursorMax {
		if _, replacing := c.m[ino]; !replacing {
			c.evictOldestLocked()
		}
	}
	c.m[ino] = dirCursor{offset: offset, name: name, used: time.Now()}
}

// forget drops a directory's cursor.
//
// Only the readdir benchmark calls this, to start a page cold and take the
// fallback path deliberately. That test is behind the `integration` build tag,
// which the lint run does not set, so `unused` sees no caller at all.
//
//nolint:unused // called by readdir_bench_integration_test.go (build tag: integration)
func (c *dirCursors) forget(ino uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, ino)
}

// ponytail: a linear sweep for the oldest entry, the same shape the lock cache
// evicts with and for the same reason — it runs only at the cap, over at most
// dirCursorMax entries, on a path that has just done an etcd round trip.  A
// heap would be the upgrade if a profile ever puts this anywhere.
func (c *dirCursors) evictOldestLocked() {
	var oldestIno uint64
	var oldest time.Time
	for ino, cur := range c.m {
		if oldest.IsZero() || cur.used.Before(oldest) {
			oldestIno, oldest = ino, cur.used
		}
	}
	delete(c.m, oldestIno)
}
