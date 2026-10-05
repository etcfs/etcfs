package metadata

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Shared monotonic counters.
//
// Inode numbers and arena IDs are both handed out from a single etcd key
// incremented under CAS.

// NextCounter atomically reserves the next value of a monotonically increasing
// counter key, returning the reserved value.
func (s *Store) NextCounter(ctx context.Context, key string, floor uint64) (uint64, error) {
	return s.ReserveCounter(ctx, key, floor, 1)
}

// ReserveCounter atomically reserves a run of count consecutive values of a
// monotonically increasing counter key, returning the first of them.
//
// floor is the lowest value that may ever be handed out; a counter that is
// missing or still below it starts there.  The key stores the *next* value to
// hand out, so a reader can always take the stored value as-is.
//
// Reserving a run is what takes the counter off the critical path of a create.
// One commit per number would make every file creation wait on Raft twice — once for
// the number and once for the entry naming it — and the numbers are 64-bit, so
// the run a node dies holding is stranded at no cost anyone can measure.  The
// key therefore counts numbers handed *out*, not files alive, which is what
// statfs already reports it as: an upper bound.
//
// The CAS is retried on contention with the same jittered backoff every
// read-modify-write here uses (casBackoff) — without jitter, callers that lose
// a race tend to have started their retry in lockstep and collide again on the
// same tick; measured without it, 16 concurrent callers in
// TestIntegration_CounterIsUniqueUnderConcurrency exhaust an 8-attempt budget
// with only 9 successes.  A missing key is compared
// on CreateRevision rather than value, because a value comparison against a
// key that does not exist never matches.
func (s *Store) ReserveCounter(ctx context.Context, key string, floor, count uint64) (uint64, error) {
	if count == 0 {
		return 0, fmt.Errorf("counter %s: a reservation of zero values is not a reservation", key)
	}
	for attempt := 0; attempt < casAttempts; attempt++ {
		v, err := s.Get(ctx, key)
		if err != nil {
			return 0, err
		}

		var guard clientv3.Cmp
		stored := uint64(0)
		if v == nil {
			guard = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		} else {
			stored = DecodeUint64(v)
			guard = clientv3.Compare(clientv3.Value(key), "=", string(v))
		}

		reserved := stored
		if reserved < floor {
			reserved = floor
		}

		ok, err := s.Txn(ctx, []clientv3.Cmp{guard},
			[]clientv3.Op{clientv3.OpPut(key, string(EncodeUint64(reserved+count)))}, nil)
		if err != nil {
			return 0, err
		}
		if ok {
			return reserved, nil
		}
		if err := casBackoff(ctx, attempt); err != nil {
			return 0, fmt.Errorf("counter %s: %w", key, err)
		}
	}
	return 0, fmt.Errorf("counter %s: contended beyond retry limit", key)
}
