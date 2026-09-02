package metadata

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Dirent operations: lookup, create, unlink, rename.
//
// Every namespace mutation is a single atomic etcd Txn.  No directory-level
// locking is used — concurrent creates in the same directory are independent
// transactions that each CAS on their specific dirent key.

// LookupDirent resolves a name in a directory to an inode number.
// Returns 0 if the entry does not exist.
func (s *Store) LookupDirent(ctx context.Context, parent uint64, name string) (uint64, error) {
	value, err := s.Get(ctx, DirentKey(parent, name))
	if err != nil {
		return 0, fmt.Errorf("lookup %d/%q: %w", parent, name, err)
	}
	if value == nil {
		return 0, nil
	}
	return DecodeUint64(value), nil
}

// CreateDirent atomically creates a directory entry pointing to an inode.
// The transaction fails if the entry already exists (CREATE exclusivity).
func (s *Store) CreateDirent(ctx context.Context, parent uint64, name string, ino uint64) error {
	cmp := clientv3.Compare(clientv3.CreateRevision(DirentKey(parent, name)), "=", 0)
	op := PutDirent(parent, name, ino)

	ok, err := s.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{op}, nil)
	if err != nil {
		return fmt.Errorf("create dirent %d/%q: %w", parent, name, err)
	}
	if !ok {
		return fmt.Errorf("create dirent %d/%q: already exists (%w)", parent, name, ErrExists)
	}
	return nil
}

// RemoveDirent atomically removes a directory entry.
// The transaction fails if the entry does not exist.
func (s *Store) RemoveDirent(ctx context.Context, parent uint64, name string) error {
	cmp := clientv3.Compare(clientv3.CreateRevision(DirentKey(parent, name)), ">", 0)
	del := DeleteDirent(parent, name)

	ok, err := s.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{del}, nil)
	if err != nil {
		return fmt.Errorf("remove dirent %d/%q: %w", parent, name, err)
	}
	if !ok {
		return fmt.Errorf("remove dirent %d/%q: not found", parent, name)
	}
	return nil
}

// ListDirents returns all entries in a directory as (name, ino) pairs.
func (s *Store) ListDirents(ctx context.Context, parent uint64) ([]DirentEntry, error) {
	kvs, err := s.GetPrefix(ctx, DirentPrefix(parent))
	if err != nil {
		return nil, fmt.Errorf("list dirents %d: %w", parent, err)
	}

	entries := make([]DirentEntry, 0, len(kvs))
	for _, kv := range kvs {
		entries = append(entries, DirentEntry{
			Name: extractNameFromKey(string(kv.Key), parent),
			Ino:  DecodeUint64(kv.Value),
		})
	}
	return entries, nil
}

// ListDirentsAfter returns at most limit entries of a directory, beginning at
// the first name that sorts strictly after `after` — or at the first name in
// the directory when `after` is empty.
//
// This is the read a paginated listing wants: etcd has no notion of "skip the
// first N keys", so serving a page by position means reading every key before
// it, and a listing that does that once per page reads the whole directory once
// per page.  Resuming from the last name returned instead makes one full scan
// cost one pass over the directory however many pages it takes.
//
// A limit of zero means no limit.  The read is linearizable, exactly as the
// whole-directory listing is: paging changes how much is read, not how fresh it
// is.
func (s *Store) ListDirentsAfter(ctx context.Context, parent uint64, after string, limit int64) ([]DirentEntry, error) {
	prefix := DirentPrefix(parent)
	start := prefix
	if after != "" {
		// A key's successor: etcd's range start is inclusive, and the caller
		// has already been given `after`.
		start = DirentKey(parent, after) + "\x00"
	}

	opts := []clientv3.OpOption{clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix))}
	if limit > 0 {
		opts = append(opts, clientv3.WithLimit(limit))
	}
	resp, err := s.read(ctx, start, opts...)
	if err != nil {
		return nil, fmt.Errorf("list dirents %d after %q: %w", parent, after, err)
	}

	entries := make([]DirentEntry, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		entries = append(entries, DirentEntry{
			Name: extractNameFromKey(string(kv.Key), parent),
			Ino:  DecodeUint64(kv.Value),
		})
	}
	return entries, nil
}

// DirentEntry is a directory listing entry.
type DirentEntry struct {
	Name string
	Ino  uint64
}

// extractNameFromKey strips the dirent prefix from an etcd key to get the filename.
// Example: "dirent:42/foo" → "foo"
func extractNameFromKey(key string, parent uint64) string {
	prefix := fmt.Sprintf("%s%d/", PrefixDirent, parent)
	if len(key) > len(prefix) {
		return key[len(prefix):]
	}
	return key
}

// CreateExtra is whatever a create folds into its own transaction beyond the
// dirent and the inode record: a symlink's target, or the exclusive lock the
// creating node takes on the inode it is about to write.
//
// It exists so that a caller with something to assert can assert it *with* the
// create rather than after it.  A create is already one transaction; anything
// that can ride it costs no second Raft commit, and anything that cannot ride
// it costs one per file.
type CreateExtra struct {
	Cmps []clientv3.Cmp
	Ops  []clientv3.Op
}

// atomicCreate publishes a new inode and the name that refers to it in one
// transaction, together with whatever else the file type needs (a symlink's
// target, say).  Every creating operation goes through here: an inode written
// without its dirent is unreachable and invisible to the orphan checks, which
// look for extents without inodes, not inodes without names.
//
// The transaction asserts both that the name is free and that the inode number
// is unused, so a create that loses either race writes nothing at all.
func (s *Store) atomicCreate(ctx context.Context, parent uint64, name string, rec *InodeRecord, extra CreateExtra) error {
	fail := func(err error) error {
		return fmt.Errorf("atomic create %d/%q: %w", parent, name, err)
	}
	build := func() ([]clientv3.Cmp, []clientv3.Op, error) {
		cmps := append([]clientv3.Cmp{
			clientv3.Compare(clientv3.CreateRevision(DirentKey(parent, name)), "=", 0),
			clientv3.Compare(clientv3.CreateRevision(InodeKey(rec.Ino)), "=", 0),
		}, extra.Cmps...)
		ops := append([]clientv3.Op{
			PutDirent(parent, name, rec.Ino),
			clientv3.OpPut(InodeKey(rec.Ino), string(EncodeInode(rec))),
		}, extra.Ops...)
		if rec.Mode&S_IFMT != ModeDir {
			return cmps, ops, nil
		}
		// A new directory's ".." is a reference to its parent, so the parent's
		// link count has to rise with it, in the same transaction: a count that
		// is a commit late is simply a wrong count.
		bump, err := s.adjustDirNlink(ctx, parent, +1)
		if err != nil {
			return nil, nil, err
		}
		return append(cmps, bump.cmps...), append(ops, bump.ops...), nil
	}

	// Only a directory needs the parent pinned, so an ordinary file create
	// still commits without one and two nodes making unrelated files in one
	// directory still never abort each other. Concurrent mkdirs in the *same*
	// directory do contend, and that is what the retry is for.
	err := retryCAS(ctx, fmt.Sprintf("atomic create %d/%q", parent, name), func() (bool, error) {
		cmps, ops, berr := build()
		if berr != nil {
			return false, berr
		}
		ok, terr := s.Txn(ctx, cmps, ops, nil)
		if terr != nil || ok {
			return ok, terr
		}
		// The name being taken is the caller's answer; anything else is the
		// parent having moved under us, which is contention worth retrying.
		if existing, lerr := s.LookupDirent(ctx, parent, name); lerr == nil && existing != 0 {
			return false, ErrExists
		}
		if rec.Mode&S_IFMT != ModeDir {
			return false, ErrExists
		}
		return false, nil
	})
	if err != nil {
		return fail(err)
	}
	if rec.Mode&S_IFMT != ModeDir {
		s.touchDir(ctx, parent)
	}
	return nil
}

// nlinkAdjust is the comparison and write that move a directory's link count,
// ready to be folded into the transaction that makes the move true.
type nlinkAdjust struct {
	cmps []clientv3.Cmp
	ops  []clientv3.Op
}

// adjustDirNlink prepares a change to a directory's link count, which counts
// its own "." and the ".." of every subdirectory it holds — so it moves only
// when a subdirectory arrives or leaves.
//
// A missing record yields nothing to do rather than an error: a create under a
// parent that no longer exists is already failing on its own comparisons, and
// the root of a filesystem seeded by an older version may predate the record.
func (s *Store) adjustDirNlink(ctx context.Context, ino uint64, delta int) (nlinkAdjust, error) {
	rec, rev, err := s.GetInodeRev(ctx, ino)
	if err != nil {
		return nlinkAdjust{}, err
	}
	if rec == nil {
		return nlinkAdjust{}, nil
	}
	switch {
	case delta > 0:
		rec.Nlink += uint32(delta)
	case delta < 0 && rec.Nlink >= uint32(-delta):
		rec.Nlink -= uint32(-delta)
	default:
		// A count that cannot absorb the decrement is already wrong; fsck
		// reports it, and taking it below zero here would make it worse.
		return nlinkAdjust{}, nil
	}
	// The same write carries the timestamps touchDir would have set, so a
	// directory operation does not pay for a second commit to record them.
	rec.Mtime = time.Now()
	rec.Ctime = rec.Mtime
	return nlinkAdjust{
		cmps: []clientv3.Cmp{InodeUnchanged(ino, rev)},
		ops:  []clientv3.Op{clientv3.OpPut(InodeKey(ino), string(EncodeInode(rec)))},
	}, nil
}

// touchDir marks a directory changed, as POSIX requires of every operation
// that adds or removes an entry in it.
//
// It runs after the transaction that changed the namespace rather than inside
// it: folding it in would pin the parent's record in every create and unlink,
// so two nodes making unrelated entries in one directory would abort each
// other. A timestamp one commit late is a better trade than a namespace
// operation that fails under concurrency, which is also why a failure here is
// reported to the caller's log and not to the caller.
//
// With batching started (see inodetimes.go) the commit is queued rather than made
// here, so a stream of entries into one directory costs one timestamp commit
// per interval instead of one per entry.
func (s *Store) touchDir(ctx context.Context, ino uint64) {
	now := time.Now()
	if t := s.dirTouches(); t != nil {
		t.queue(ino, now)
		return
	}
	_ = s.commitInodeTimes(ctx, ino, timeUpdate{bump: now})
}

// AtomicCreateFile creates a regular file and its directory entry in a single
// etcd transaction, together with whatever the caller folds in through extra —
// which for a node that is about to write the file is its exclusive lock on it.
func (s *Store) AtomicCreateFile(ctx context.Context, parent uint64, name string, ino uint64, mode uint32, uid, gid uint32, extra CreateExtra) (*InodeRecord, error) {
	rec := NewInodeRecord(ino, mode, uid, gid)
	return rec, s.atomicCreate(ctx, parent, name, rec, extra)
}

// AtomicCreateDir creates a directory (mkdir) in a single etcd transaction.
// Same pattern as AtomicCreateFile, but with nlink=2 (its own "." and its entry
// in its parent), S_IFDIR mode, and the parent's own count raised for the ".."
// the new directory brings with it.
func (s *Store) AtomicCreateDir(ctx context.Context, parent uint64, name string, ino uint64, mode uint32, uid, gid uint32) (*InodeRecord, error) {
	rec := NewInodeRecord(ino, mode|ModeDir, uid, gid)
	rec.Size = 4096
	return rec, s.atomicCreate(ctx, parent, name, rec, CreateExtra{})
}

// AtomicCreateSymlink creates a symlink inode, its target record and its
// directory entry in a single etcd transaction.
func (s *Store) AtomicCreateSymlink(ctx context.Context, parent uint64, name string, ino uint64, target string, uid, gid uint32) (*InodeRecord, error) {
	// A symlink's own permission bits are not meaningful — the target's are what
	// an access check consults — so the mode is fixed rather than umask-masked.
	rec := NewInodeRecord(ino, ModeSymlink|0777, uid, gid)
	rec.Size = uint64(len(target))
	return rec, s.atomicCreate(ctx, parent, name, rec,
		CreateExtra{Ops: []clientv3.Op{clientv3.OpPut(InodeSymlinkKey(ino), target)}})
}

// AtomicCreateNode creates a device node, FIFO or socket (mknod) and its
// directory entry in a single etcd transaction.
func (s *Store) AtomicCreateNode(ctx context.Context, parent uint64, name string, ino uint64, mode, rdev uint32, uid, gid uint32) (*InodeRecord, error) {
	rec := NewInodeRecord(ino, mode, uid, gid)
	rec.Rdev = rdev
	return rec, s.atomicCreate(ctx, parent, name, rec, CreateExtra{})
}

// AtomicLink adds a second name for an existing inode: the new dirent and the
// raised link count commit together, so a name that loses the race to another
// creator does not leave the count permanently inflated.
//
// The inode is pinned to the revision its link count was read at, for the same
// reason every other read-modify-write of an inode record is.
func (s *Store) AtomicLink(ctx context.Context, ino, parent uint64, name string) (*InodeRecord, error) {
	var linked *InodeRecord
	err := retryCAS(ctx, fmt.Sprintf("atomic link %d/%q", parent, name), func() (bool, error) {
		rec, rev, err := s.GetInodeRev(ctx, ino)
		if err != nil {
			return false, fmt.Errorf("atomic link %d/%q: %w", parent, name, err)
		}
		if rec == nil {
			return false, fmt.Errorf("atomic link %d/%q: inode %d %w", parent, name, ino, ErrNotFound)
		}
		// Hard links to a directory would let the namespace form a cycle that no
		// unlink can break, and POSIX reserves the right to refuse them.
		if rec.Mode&S_IFMT == ModeDir {
			return false, fmt.Errorf("atomic link %d/%q: %w", parent, name, ErrPerm)
		}
		rec.Nlink++
		// The link count is part of the inode's status, so adding a name to it
		// is a status change of the file itself, not only of the directory.
		rec.Ctime = time.Now()

		direntKey := DirentKey(parent, name)
		cmps := []clientv3.Cmp{
			clientv3.Compare(clientv3.CreateRevision(direntKey), "=", 0),
			InodeUnchanged(ino, rev),
		}
		ops := []clientv3.Op{
			PutDirent(parent, name, ino),
			clientv3.OpPut(InodeKey(ino), string(EncodeInode(rec))),
		}
		ok, err := s.Txn(ctx, cmps, ops, nil)
		if err != nil {
			return false, fmt.Errorf("atomic link %d/%q: %w", parent, name, err)
		}
		if ok {
			linked = rec
			return true, nil
		}
		// The inode moving is contention worth retrying; the name already
		// existing is the caller's answer.
		if existing, lerr := s.LookupDirent(ctx, parent, name); lerr == nil && existing != 0 {
			return false, fmt.Errorf("atomic link %d/%q: %w", parent, name, ErrExists)
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	s.touchDir(ctx, parent)
	return linked, nil
}

// AtomicUnlink removes a directory entry and drops one reference to the inode
// it named, deleting the inode once nothing points at it.
//
// The transaction is pinned to both records as they were read: the dirent still
// naming this inode, and the inode still holding the link count the new one was
// computed from. Without the second, two concurrent unlinks of two names for
// one inode each read nlink=2, each write nlink=1, and the inode is never
// freed. Losing either comparison is contention, so the work is redone against
// fresh state rather than reported as a failure.
func (s *Store) AtomicUnlink(ctx context.Context, parent uint64, name string) error {
	_, err := s.AtomicUnlinkKeepingOpen(ctx, parent, name, nil)
	return err
}

// AtomicUnlinkKeepingOpen is AtomicUnlink with a say in what happens to an
// inode that loses its last name: when heldOpen reports the inode still has a
// descriptor on this node, the record survives with a link count of zero and an
// orphan:<node>/<ino> key marking who must finish the job.
//
// POSIX requires the file to stay readable through an open descriptor after its
// last name is gone. Only this node's descriptors are known here — a peer
// unlinking a file this node holds open still takes it away, which is the
// limitation an open-count in etcd would close at the cost of a round trip on
// every open.
//
// The inode number is reported back when the unlink removed the record
// outright, and zero when it did not — a link that remains, or a file this node
// still holds open.  The caller needs to know: an inode that is gone leaves its
// extents behind as orphans for the scrubber, and the scrubber will not touch
// them while this node still has anything cached for that inode's lock.
func (s *Store) AtomicUnlinkKeepingOpen(ctx context.Context, parent uint64, name string,
	heldOpen func(ino uint64) bool) (uint64, error) {

	var removed uint64
	err := retryCAS(ctx, fmt.Sprintf("atomic unlink %d/%q", parent, name), func() (bool, error) {
		fail := func(format string, args ...any) (bool, error) {
			prefix := fmt.Sprintf("atomic unlink %d/%q: ", parent, name)
			return false, fmt.Errorf(prefix+format, args...)
		}

		direntKey := DirentKey(parent, name)
		direntKvs, _, err := s.GetRevision(ctx, direntKey)
		if err != nil {
			return fail("%w", err)
		}
		if len(direntKvs) == 0 {
			return fail("%w", ErrNotFound)
		}
		ino := DecodeUint64(direntKvs[0].Value)

		rec, rev, err := s.GetInodeRev(ctx, ino)
		if err != nil {
			return fail("%w", err)
		}
		if rec == nil {
			return fail("inode %d not found (%w)", ino, ErrNotFound)
		}

		cmps := []clientv3.Cmp{
			clientv3.Compare(clientv3.ModRevision(direntKey), "=", direntKvs[0].ModRevision),
			InodeUnchanged(ino, rev),
		}
		var tail []clientv3.Op
		removed = 0
		if rec.Nlink <= 1 && rec.Mode&S_IFMT != ModeDir && heldOpen != nil && heldOpen(ino) {
			orphaned := *rec
			orphaned.Nlink = 0
			orphaned.Ctime = time.Now()
			tail = []clientv3.Op{
				clientv3.OpPut(InodeKey(ino), string(EncodeInode(&orphaned))),
				clientv3.OpPut(OrphanKey(s.NodeID(), ino), ""),
			}
		} else {
			tail = s.unlinkInodeOps(rec)
			if rec.Nlink <= 1 {
				removed = ino
			}
		}
		ops := append([]clientv3.Op{DeleteDirent(parent, name)}, tail...)
		ok, err := s.Txn(ctx, cmps, ops, nil)
		if ok && err == nil {
			s.touchDir(ctx, parent)
		}
		if !ok || err != nil {
			removed = 0
		}
		return ok, err
	})
	return removed, err
}

// AtomicRmdir removes an empty directory and its entry in one transaction.
//
// Emptiness is asserted inside the transaction rather than read beforehand.
// etcd cannot express "no keys under this prefix" as a value comparison, but it
// can compare a whole range: "every key under dirent:<ino>/ has creation
// revision 0" is true only when the range is empty, since a key that exists has
// a non-zero one.  A separate listing followed by a delete would let another
// node create an entry in between and strand the subtree — the parent's name
// gone, the children reachable by nothing.
func (s *Store) AtomicRmdir(ctx context.Context, parent uint64, name string) error {
	return retryCAS(ctx, fmt.Sprintf("atomic rmdir %d/%q", parent, name), func() (bool, error) {
		fail := func(format string, args ...any) (bool, error) {
			prefix := fmt.Sprintf("atomic rmdir %d/%q: ", parent, name)
			return false, fmt.Errorf(prefix+format, args...)
		}

		direntKey := DirentKey(parent, name)
		direntKvs, _, err := s.GetRevision(ctx, direntKey)
		if err != nil {
			return fail("%w", err)
		}
		if len(direntKvs) == 0 {
			return fail("%w", ErrNotFound)
		}
		ino := DecodeUint64(direntKvs[0].Value)

		rec, rev, err := s.GetInodeRev(ctx, ino)
		if err != nil {
			return fail("%w", err)
		}
		if rec == nil {
			return fail("inode %d not found (%w)", ino, ErrNotFound)
		}
		if rec.Mode&S_IFMT != ModeDir {
			return fail("%w", ErrNotDir)
		}

		// The ".." this directory pointed at its parent goes with it. Read
		// before the two slices are built so both can be sized for what they
		// will actually hold.
		drop, err := s.adjustDirNlink(ctx, parent, -1)
		if err != nil {
			return fail("%w", err)
		}

		cmps := make([]clientv3.Cmp, 0, 3+len(drop.cmps))
		cmps = append(cmps,
			clientv3.Compare(clientv3.ModRevision(direntKey), "=", direntKvs[0].ModRevision),
			InodeUnchanged(ino, rev),
			DirEmpty(ino),
		)
		cmps = append(cmps, drop.cmps...)

		ops := make([]clientv3.Op, 0, 2+len(drop.ops))
		ops = append(ops,
			DeleteDirent(parent, name),
			clientv3.OpDelete(InodeKey(ino)),
		)
		ops = append(ops, drop.ops...)

		ok, err := s.Txn(ctx, cmps, ops, nil)
		if err != nil {
			return fail("%w", err)
		}
		if ok {
			return true, nil
		}
		// Losing a revision comparison is contention worth retrying; an entry
		// having appeared in the directory is the caller's answer.
		entries, lerr := s.ListDirents(ctx, ino)
		if lerr == nil && len(entries) > 0 {
			return fail("%w", ErrNotEmpty)
		}
		return false, nil
	})
}

// DirEmpty holds only when a directory has no entries.  A range comparison is
// the only way to ask etcd this: every key under the prefix must have creation
// revision 0, which no existing key does, so it is true exactly when the range
// is empty.
func DirEmpty(ino uint64) clientv3.Cmp {
	return clientv3.Compare(clientv3.CreateRevision(DirentPrefix(ino)), "=", 0).WithPrefix()
}

// AtomicRename moves ino from oldParent/oldName to newParent/newName in a
// single etcd transaction.
//
// POSIX requires the rename to replace an existing target, and replacing it is
// an unlink: the target's nlink drops, and its inode record goes with it at
// zero.  Leaving that out is what orphaned the replaced file — its inode and
// every extent behind it stayed in etcd, reachable by nothing.
//
// flags carries RENAME_NOREPLACE and RENAME_EXCHANGE.  Exchange is rejected
// rather than approximated: an ordinary rename deletes the source and
// overwrites the target, which is data loss for a caller that asked for the two
// names to swap.
func (s *Store) AtomicRename(ctx context.Context, oldParent uint64, oldName string, newParent uint64, newName string, ino uint64, flags uint32) error {
	fail := func(format string, args ...any) error {
		prefix := fmt.Sprintf("atomic rename %d/%q → %d/%q: ", oldParent, oldName, newParent, newName)
		return fmt.Errorf(prefix+format, args...)
	}

	if flags&RenameExchange != 0 {
		return fail("RENAME_EXCHANGE is not implemented (%w)", ErrInvalid)
	}

	// A dirent whose inode is missing is already corrupt, and fsck reports it.
	// Renaming it is still allowed: moving a broken pointer breaks nothing
	// further, and refusing would take away the obvious way to clear it.  Its
	// type is unknown, so the directory checks below simply do not apply.
	src, err := s.GetInode(ctx, ino)
	if err != nil {
		return fail("%w", err)
	}
	srcIsDir := src != nil && src.Mode&S_IFMT == ModeDir

	// A directory moved beneath itself detaches its whole subtree: the entries
	// still exist, but no path from the root reaches them, and no later
	// operation can tell that from ordinary data.
	if srcIsDir {
		under, aerr := s.isDescendant(ctx, newParent, ino)
		if aerr != nil {
			return fail("%w", aerr)
		}
		if newParent == ino || under {
			return fail("destination is inside the directory being moved (%w)", ErrInvalid)
		}
	}

	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(DirentKey(oldParent, oldName)), ">", 0),
	}
	ops := []clientv3.Op{
		DeleteDirent(oldParent, oldName),
		PutDirent(newParent, newName, ino),
	}

	targetKey := DirentKey(newParent, newName)
	targetKvs, _, err := s.GetRevision(ctx, targetKey)
	if err != nil {
		return fail("read target: %w", err)
	}

	if len(targetKvs) == 0 {
		// Nothing there now, and nothing may appear before the commit lands.
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(targetKey), "=", 0))
		if err := s.applyRenameNlink(ctx, oldParent, newParent, srcIsDir, false, &cmps, &ops); err != nil {
			return fail("%w", err)
		}
		if err := s.commitRename(ctx, cmps, ops); err != nil {
			return fail("%w", err)
		}
		s.touchRenameDirs(ctx, oldParent, newParent)
		return nil
	}

	if flags&RenameNoReplace != 0 {
		return fail("target exists (%w)", ErrExists)
	}

	// Pin the target to the revision the checks below are about to read, so a
	// concurrent write to that name aborts this rename instead of being
	// silently replaced by it.
	cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(targetKey), "=", targetKvs[0].ModRevision))

	victimIsDirectory := false
	victimIno := DecodeUint64(targetKvs[0].Value)
	victim, victimRev, err := s.GetInodeRev(ctx, victimIno)
	if err != nil {
		return fail("read target inode: %w", err)
	}
	if victim != nil {
		victimIsDir := victim.Mode&S_IFMT == ModeDir
		switch {
		case srcIsDir && !victimIsDir:
			return fail("cannot replace a file with a directory (%w)", ErrNotDir)
		case !srcIsDir && victimIsDir:
			return fail("cannot replace a directory with a file (%w)", ErrIsDir)
		}
		if victimIsDir {
			entries, lerr := s.ListDirents(ctx, victimIno)
			if lerr != nil {
				return fail("list target: %w", lerr)
			}
			if len(entries) > 0 {
				return fail("target directory is not empty (%w)", ErrNotEmpty)
			}
			// The listing above is what produces ENOTEMPTY; this is what makes
			// it safe, by refusing the rename if an entry appears in between.
			cmps = append(cmps, DirEmpty(victimIno))
		}
		// Pinned like the dirent above: the link count written below is
		// computed from this record, so a concurrent change to it has to abort
		// the rename rather than be overwritten by it.
		cmps = append(cmps, InodeUnchanged(victimIno, victimRev))
		ops = append(ops, s.unlinkInodeOps(victim)...)
		victimIsDirectory = victimIsDir
	}

	if err := s.applyRenameNlink(ctx, oldParent, newParent, srcIsDir, victimIsDirectory, &cmps, &ops); err != nil {
		return fail("%w", err)
	}
	if err := s.commitRename(ctx, cmps, ops); err != nil {
		return fail("%w", err)
	}
	s.touchRenameDirs(ctx, oldParent, newParent)
	return nil
}

// applyRenameNlink folds into a rename the link-count moves its directories
// cause: a directory arriving raises its new parent's count and lowers its old
// one, and a directory it replaces lowers the new parent's again. Both land in
// the rename's own transaction, so no interleaving can leave a count that
// describes neither the state before nor the state after.
//
// The two parents are read here, one revision at a time, and pinned: a
// concurrent mkdir in either of them aborts this rename rather than losing its
// increment to it.
func (s *Store) applyRenameNlink(ctx context.Context, oldParent, newParent uint64,
	srcIsDir, victimIsDir bool, cmps *[]clientv3.Cmp, ops *[]clientv3.Op) error {

	deltas := map[uint64]int{}
	if srcIsDir && oldParent != newParent {
		deltas[oldParent]--
		deltas[newParent]++
	}
	if victimIsDir {
		deltas[newParent]--
	}
	for ino, delta := range deltas {
		if delta == 0 {
			continue
		}
		adj, err := s.adjustDirNlink(ctx, ino, delta)
		if err != nil {
			return err
		}
		*cmps = append(*cmps, adj.cmps...)
		*ops = append(*ops, adj.ops...)
	}
	return nil
}

// touchRenameDirs marks both ends of a rename changed, once when they are the
// same directory.
func (s *Store) touchRenameDirs(ctx context.Context, oldParent, newParent uint64) {
	s.touchDir(ctx, oldParent)
	if newParent != oldParent {
		s.touchDir(ctx, newParent)
	}
}

// unlinkInodeOp is the write that drops one reference to rec: a smaller nlink,
// or the inode's removal once nothing points at it any more.
//
// The extents of a removed inode are deliberately left behind.  They become
// orphans, which the scrubber reclaims on the node that owns their arena — the
// only node that may, since its in-memory free list is rebuilt from exactly
// those records.
func (s *Store) unlinkInodeOps(rec *InodeRecord) []clientv3.Op {
	// A directory's count is its own "." plus the ".." of each subdirectory it
	// holds, so it says nothing about how many names refer to it — a directory
	// has exactly one, and losing it removes the directory outright.
	// Decrementing instead would leave the record behind with a count no path
	// can ever bring back to zero.
	if rec.Mode&S_IFMT == ModeDir {
		return []clientv3.Op{
			clientv3.OpDelete(InodeKey(rec.Ino)),
			clientv3.OpDelete(XattrPrefix(rec.Ino), clientv3.WithPrefix()),
		}
	}
	// A surviving hard link keeps the inode, and the attributes belong to the
	// inode rather than to the name being removed, so they stay.
	if rec.Nlink > 1 {
		reduced := *rec
		reduced.Nlink--
		reduced.Ctime = time.Now()
		return []clientv3.Op{clientv3.OpPut(InodeKey(rec.Ino), string(EncodeInode(&reduced)))}
	}
	ops := []clientv3.Op{
		clientv3.OpDelete(InodeKey(rec.Ino)),
		// Extended attributes are owned outright by the inode, so they go with
		// it.  Left behind they would leak a key per attribute, and — worse —
		// a later inode reusing this number would inherit them.
		clientv3.OpDelete(XattrPrefix(rec.Ino), clientv3.WithPrefix()),
	}
	// A symlink's target lives in its own key, which nothing else references —
	// leaving it behind leaks a key per deleted symlink, and no check looks for
	// one.
	if rec.Mode&S_IFMT == ModeSymlink {
		ops = append(ops, clientv3.OpDelete(InodeSymlinkKey(rec.Ino)))
	}
	return ops
}

// DeleteOrphan removes an inode this node was keeping alive for an open
// descriptor, once the last one closes. The extents are left to the scrubber,
// exactly as an ordinary unlink leaves them.
func (s *Store) DeleteOrphan(ctx context.Context, ino uint64) error {
	rec, err := s.GetInode(ctx, ino)
	if err != nil {
		return fmt.Errorf("delete orphan %d: %w", ino, err)
	}
	ops := []clientv3.Op{clientv3.OpDelete(OrphanKey(s.NodeID(), ino))}
	if rec != nil && rec.Nlink == 0 {
		ops = append(ops, s.unlinkInodeOps(rec)...)
	}
	if _, err := s.Txn(ctx, nil, ops, nil); err != nil {
		return fmt.Errorf("delete orphan %d: %w", ino, err)
	}
	return nil
}

// ListOrphans returns the inodes a node left behind, which is non-empty only
// after that node died holding an unlinked file open.
func (s *Store) ListOrphans(ctx context.Context, node string) ([]uint64, error) {
	kvs, err := s.GetPrefix(ctx, OrphanPrefix(node))
	if err != nil {
		return nil, fmt.Errorf("list orphans of %q: %w", node, err)
	}
	inos := make([]uint64, 0, len(kvs))
	for _, kv := range kvs {
		ino, perr := strconv.ParseUint(strings.TrimPrefix(string(kv.Key), OrphanPrefix(node)), 10, 64)
		if perr != nil {
			continue
		}
		inos = append(inos, ino)
	}
	return inos, nil
}

func (s *Store) commitRename(ctx context.Context, cmps []clientv3.Cmp, ops []clientv3.Op) error {
	ok, err := s.Txn(ctx, cmps, ops, nil)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("source or target changed under the rename (%w)", ErrConflict)
	}
	return nil
}

// isDescendant reports whether ino is anywhere beneath ancestor in the
// namespace.
//
// Inodes record no parent, so the only way up the tree is a reverse index built
// from the dirent values.  One prefix scan is enough, and this runs only when a
// *directory* is being renamed, which is rare — a per-inode parent pointer
// would be a second source of truth to keep consistent for no benefit at this
// scale.
//
// The walk is bounded by the number of directories it has seen: a namespace
// that already contains a cycle would otherwise spin here forever.
func (s *Store) isDescendant(ctx context.Context, ino, ancestor uint64) (bool, error) {
	if ino == ancestor {
		return true, nil
	}

	kvs, err := s.GetPrefix(ctx, PrefixDirent)
	if err != nil {
		return false, fmt.Errorf("build parent index: %w", err)
	}
	parent := make(map[uint64]uint64, len(kvs))
	for _, kv := range kvs {
		p, _, ok := ParseDirentKey(string(kv.Key))
		if !ok {
			continue
		}
		parent[DecodeUint64(kv.Value)] = p
	}

	for step := 0; step <= len(parent); step++ {
		next, ok := parent[ino]
		if !ok {
			return false, nil // reached the root, or an unparented inode
		}
		if next == ancestor {
			return true, nil
		}
		ino = next
	}
	return false, fmt.Errorf("parent chain from %d does not terminate", ino)
}

// ---- rename constants ----
const (
	RenameNoReplace = 1 << 0
	RenameExchange  = 1 << 1
)

// ---- helpers ----

// timeNow is a variable so a test can pin the clock.
var timeNow = time.Now
