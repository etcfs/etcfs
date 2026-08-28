package harness

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// FaultType enumerates injectable faults.
type FaultType int

const (
	FaultNone FaultType = iota
	FaultEtcdPartition
	FaultLeaderElection
	FaultMajorityLoss
	FaultLeaseExpiry
	FaultNodeCrash
)

// Simulator runs deterministic metadata operations with fault injection.
type Simulator struct {
	store      *MockStore
	rng        *rand.Rand
	seed       int64
	tick       int64
	ops        int
	faults     int
	inoCounter uint64

	inodes  map[uint64]*metadata.InodeRecord
	dirents map[string]uint64
	locks   map[uint64]*metadata.LockRecord

	faultSchedule map[int64]FaultType
	crashPoints   map[int64]bool
}

func NewSimulator(seed int64) *Simulator {
	return NewSimulatorWithStore(seed, NewMockStore())
}

func NewSimulatorWithStore(seed int64, store *MockStore) *Simulator {
	return &Simulator{
		store:         store,
		rng:           rand.New(rand.NewPCG(uint64(seed), 0)),
		seed:          seed,
		inodes:        make(map[uint64]*metadata.InodeRecord),
		dirents:       make(map[string]uint64),
		locks:         make(map[uint64]*metadata.LockRecord),
		faultSchedule: make(map[int64]FaultType),
		crashPoints:   make(map[int64]bool),
	}
}

func (s *Simulator) AddFault(tick int64, ft FaultType) {
	s.faultSchedule[tick] = ft
}

func (s *Simulator) AddCrash(tick int64) {
	s.crashPoints[tick] = true
}

func (s *Simulator) Run(ops int, ticks int64) int {
	violations := 0

	for i := 0; i < ops; i++ {
		for t := int64(0); t < ticks; t++ {
			if ft, ok := s.faultSchedule[s.tick]; ok {
				s.injectFault(ft)
				s.faults++
			}
			if s.crashPoints[s.tick] {
				s.simulateCrash()
			}
			s.store.Tick()
			s.tick++
		}

		s.executeRandomOp()
		if v := s.checkInvariants(); v > 0 {
			violations += v
		}
		s.ops++
	}

	return violations
}

func (s *Simulator) executeRandomOp() {
	op := s.rng.IntN(10)
	ctx := context.Background()
	parent := uint64(1)
	ino := s.nextIno()
	name := fmt.Sprintf("file-%d", s.rng.IntN(10000))

	switch op {
	case 0:
		_, _ = s.createFile(ctx, parent, name, ino, 0100644)
	case 1:
		_, _ = s.createDir(ctx, parent, name, ino)
	case 2:
		s.unlinkFile(ctx, parent, name)
	case 3:
		newName := fmt.Sprintf("renamed-%d", s.rng.IntN(10000))
		s.renameFile(ctx, parent, name, parent, newName, ino)
	case 4:
		s.writeInode(ctx, ino, uint64(s.rng.IntN(4096)))
	case 5:
		_ = s.getattr(ino)
	case 6:
		_ = s.lookup(parent, name)
	case 7:
		s.listDir(parent)
	case 8:
		s.truncate(ctx, ino, 0)
	case 9:
		s.acquireLock(ctx, ino)
	}
}

func (s *Simulator) createFile(ctx context.Context, parent uint64, name string, ino uint64, mode uint32) (*metadata.InodeRecord, error) {
	key := metadata.DirentKey(parent, name)
	if _, ok := s.dirents[key]; ok {
		return nil, fmt.Errorf("already exists")
	}
	rec := &metadata.InodeRecord{
		Ino: ino, Mode: mode, Nlink: 1, Blksize: 4096,
		Atime: time.Now(), Mtime: time.Now(), Ctime: time.Now(),
	}
	s.inodes[ino] = rec
	s.dirents[key] = ino

	_, _ = s.store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
	_, _ = s.store.Put(ctx, key, metadata.EncodeUint64(ino))
	return rec, nil
}

func (s *Simulator) createDir(ctx context.Context, parent uint64, name string, ino uint64) (*metadata.InodeRecord, error) {
	rec := &metadata.InodeRecord{
		Ino: ino, Mode: metadata.ModeDir | 0755, Nlink: 1, Blksize: 4096,
		Atime: time.Now(), Mtime: time.Now(), Ctime: time.Now(),
	}
	s.inodes[ino] = rec
	key := metadata.DirentKey(parent, name)
	s.dirents[key] = ino

	_, _ = s.store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
	_, _ = s.store.Put(ctx, key, metadata.EncodeUint64(ino))
	return rec, nil
}

func (s *Simulator) unlinkFile(ctx context.Context, parent uint64, name string) {
	key := metadata.DirentKey(parent, name)
	ino, ok := s.dirents[key]
	if !ok {
		return
	}
	delete(s.dirents, key)
	rec, ok := s.inodes[ino]
	if !ok {
		return
	}
	rec.Nlink--
	if rec.Nlink == 0 {
		delete(s.inodes, ino)
		_ = s.store.Delete(ctx, metadata.InodeKey(ino))
	}
	_ = s.store.Delete(ctx, key)
}

func (s *Simulator) renameFile(ctx context.Context, oldParent uint64, oldName string,
	newParent uint64, newName string, ino uint64) {
	oldKey := metadata.DirentKey(oldParent, oldName)
	newKey := metadata.DirentKey(newParent, newName)
	if _, ok := s.dirents[oldKey]; !ok {
		return
	}
	delete(s.dirents, oldKey)
	s.dirents[newKey] = ino
	_ = s.store.Delete(ctx, oldKey)
	_, _ = s.store.Put(ctx, newKey, metadata.EncodeUint64(ino))
}

func (s *Simulator) writeInode(ctx context.Context, ino uint64, size uint64) {
	rec, ok := s.inodes[ino]
	if !ok {
		return
	}
	rec.Size = size
	_, _ = s.store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
}

func (s *Simulator) getattr(ino uint64) *metadata.InodeRecord {
	return s.inodes[ino]
}

func (s *Simulator) lookup(parent uint64, name string) uint64 {
	key := metadata.DirentKey(parent, name)
	return s.dirents[key]
}

func (s *Simulator) listDir(parent uint64) {
	prefix := metadata.DirentPrefix(parent)
	for k := range s.dirents {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			_ = k
		}
	}
}

func (s *Simulator) truncate(ctx context.Context, ino uint64, newSize uint64) {
	rec, ok := s.inodes[ino]
	if !ok {
		return
	}
	rec.Size = newSize
	_, _ = s.store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
}

func (s *Simulator) acquireLock(ctx context.Context, ino uint64) {
	s.locks[ino] = &metadata.LockRecord{Mode: "exclusive"}
	_, _ = s.store.Put(ctx, metadata.LockKey(ino, metadata.LockExclusive, "harness"), []byte("sim-node"))
}

func (s *Simulator) injectFault(ft FaultType) {
	s.store.log = append(s.store.log, fmt.Sprintf("fault: %d", ft))
	if ft == FaultLeaseExpiry {
		for lid := range s.store.leases {
			delete(s.store.leases, lid)
		}
	}
}

func (s *Simulator) simulateCrash() {
	s.inodes = make(map[uint64]*metadata.InodeRecord)
	s.dirents = make(map[string]uint64)
	s.locks = make(map[uint64]*metadata.LockRecord)

	// Both prefixes from one snapshot.  A peer creating files while this node
	// restarts publishes the inode before the dirent that names it, so one
	// snapshot can never hold the second without the first — but reading the
	// two ranges separately can, and that torn view fails the invariant check
	// for a state no restart could actually observe.
	ctx := context.Background()
	snap, _ := s.store.GetPrefixes(ctx, metadata.PrefixInode, metadata.PrefixDirent)
	for _, kv := range snap[0] {
		rec := decodeInode(kv.Value)
		if rec != nil {
			s.inodes[rec.Ino] = rec
		}
	}
	for _, kv := range snap[1] {
		ino := metadata.DecodeUint64(kv.Value)
		s.dirents[string(kv.Key)] = ino
	}
	s.store.log = append(s.store.log, fmt.Sprintf("crash: restart at tick %d", s.tick))
}

func (s *Simulator) checkInvariants() int {
	v := 0
	v += s.checkNlinkConsistency()
	v += s.checkInodeExistence()
	return v
}

func (s *Simulator) checkNlinkConsistency() int {
	violations := 0
	refCount := make(map[uint64]uint32)
	for _, ino := range s.dirents {
		refCount[ino]++
	}
	for ino, rec := range s.inodes {
		if rec.Nlink != refCount[ino] {
			s.store.log = append(s.store.log,
				fmt.Sprintf("INVARIANT: nlink mismatch ino=%d nlink=%d dirents=%d",
					ino, rec.Nlink, refCount[ino]))
			violations++
		}
	}
	return violations
}

func (s *Simulator) checkInodeExistence() int {
	violations := 0
	for key, ino := range s.dirents {
		if _, ok := s.inodes[ino]; !ok {
			s.store.log = append(s.store.log,
				fmt.Sprintf("INVARIANT: dirent %s points to missing inode %d", key, ino))
			violations++
		}
	}
	return violations
}

func (s *Simulator) Stats() (ops, faults, violations int) {
	return s.ops, s.faults, s.checkInvariants()
}

func (s *Simulator) Seed() int64 { return s.seed }

// decodeInode wraps metadata.DecodeInode for the simulator.
func decodeInode(data []byte) *metadata.InodeRecord {
	return metadata.DecodeInode(data)
}

func (s *Simulator) nextIno() uint64 {
	s.inoCounter++
	return s.inoCounter
}
