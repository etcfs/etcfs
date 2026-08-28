package harness

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// MockStore is a deterministic in-memory implementation of MetadataStore.
type MockStore struct {
	mu       sync.Mutex
	kv       map[string][]byte
	leases   map[clientv3.LeaseID]*mockLease
	watchers map[string][]*mockWatcher

	clock   int64
	rev     int64
	nextLID clientv3.LeaseID

	log []string
}

type mockLease struct {
	id   clientv3.LeaseID
	ttl  int64
	keys []string
}

type mockWatcher struct {
	ch  chan clientv3.WatchResponse
	ctx context.Context
}

func NewMockStore() *MockStore {
	return &MockStore{
		kv:       make(map[string][]byte),
		leases:   make(map[clientv3.LeaseID]*mockLease),
		watchers: make(map[string][]*mockWatcher),
		nextLID:  1,
	}
}

func (s *MockStore) Tick() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clock++

	for lid, l := range s.leases {
		l.ttl--
		if l.ttl <= 0 {
			for _, key := range l.keys {
				delete(s.kv, key)
				s.rev++
				s.deliverWatchEvent(key, mvccpb.DELETE)
			}
			delete(s.leases, lid)
		}
	}
}

func (s *MockStore) Log() []string { return s.log }

// ---- MetadataStore implementation ----

func (s *MockStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kv[key], nil
}

func (s *MockStore) Put(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[key] = value
	s.rev++
	s.deliverWatchEvent(key, mvccpb.PUT)
	return s.rev, nil
}

func (s *MockStore) CASPut(ctx context.Context, key string, value []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.kv[key]; exists {
		return false
	}
	s.kv[key] = value
	s.rev++
	return true
}

func (s *MockStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.kv, key)
	s.rev++
	s.deliverWatchEvent(key, mvccpb.DELETE)
	return nil
}

func (s *MockStore) GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prefixLocked(prefix), nil
}

// GetPrefixes answers several prefixes from one snapshot, which is what a
// caller rebuilding a consistent view of the tree needs: two GetPrefix calls
// see two different states, and a write landing between them is visible through
// one prefix and not the other.  Real etcd gets this from reading every range
// at one revision; here it is the one lock held across all of them.
func (s *MockStore) GetPrefixes(ctx context.Context, prefixes ...string) ([][]*mvccpb.KeyValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([][]*mvccpb.KeyValue, 0, len(prefixes))
	for _, prefix := range prefixes {
		out = append(out, s.prefixLocked(prefix))
	}
	return out, nil
}

func (s *MockStore) prefixLocked(prefix string) []*mvccpb.KeyValue {
	var kvs []*mvccpb.KeyValue
	for k, v := range s.kv {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			kvs = append(kvs, &mvccpb.KeyValue{Key: []byte(k), Value: v})
		}
	}
	sort.Slice(kvs, func(i, j int) bool {
		return string(kvs[i].Key) < string(kvs[j].Key)
	})
	return kvs
}

func (s *MockStore) Txn(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.evalCmps(ifs) {
		s.applyOps(thens)
		return true, nil
	}
	s.applyOps(elses)
	return false, nil
}

// evalCmps evaluates comparisons against the in-memory key-value store.
// Supports simple value-equality comparisons by comparing the serialized bytes.
func (s *MockStore) evalCmps(cmps []clientv3.Cmp) bool {
	for _, cmp := range cmps {
		key := string(cmp.KeyBytes())
		val := s.kv[key]

		// Simple equality check based on etcd Compare API
		if !cmpMatches(cmp, key, val) {
			return false
		}
	}
	return true
}

// cmpMatches checks if the given comparison holds for a key-value pair.
func cmpMatches(cmp clientv3.Cmp, key string, val []byte) bool {
	target := int32(cmp.Target)
	// 1 = Compare_CREATE (CreateRevision).  "= 0" means key absent.
	if target == 1 {
		return val == nil
	}
	// 3 = Compare_VALUE.  Compare the serialised bytes.
	if target == 3 {
		if val == nil {
			return false
		}
		return string(val) == string(cmp.ValueBytes())
	}
	return val != nil
}

func (s *MockStore) applyOps(ops []clientv3.Op) {
	for _, op := range ops {
		key := string(op.KeyBytes())
		switch {
		case op.IsPut():
			s.kv[key] = op.ValueBytes()
		case op.IsDelete():
			delete(s.kv, key)
		}
		s.rev++
	}
}

func (s *MockStore) GrantLease(ctx context.Context, ttl time.Duration) (clientv3.LeaseID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.nextLID
	s.nextLID++
	s.leases[id] = &mockLease{
		id:  id,
		ttl: int64(ttl.Seconds()),
	}
	return id, nil
}

func (s *MockStore) KeepAlive(ctx context.Context, leaseID clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ch := make(chan *clientv3.LeaseKeepAliveResponse, 16)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			s.mu.Lock()
			l, ok := s.leases[leaseID]
			if !ok {
				s.mu.Unlock()
				return
			}
			_ = l.ttl // refresh TTL
			s.mu.Unlock()

			select {
			case ch <- &clientv3.LeaseKeepAliveResponse{ID: leaseID, TTL: l.ttl}:
			case <-ctx.Done():
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return ch, nil
}

func (s *MockStore) RevokeLease(ctx context.Context, leaseID clientv3.LeaseID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, ok := s.leases[leaseID]
	if !ok {
		return fmt.Errorf("lease not found")
	}
	for _, key := range l.keys {
		delete(s.kv, key)
	}
	delete(s.leases, leaseID)
	return nil
}

func (s *MockStore) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch := make(chan clientv3.WatchResponse, 100)
	w := &mockWatcher{ch: ch, ctx: ctx}
	s.watchers[key] = append(s.watchers[key], w)

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		for i, w2 := range s.watchers[key] {
			if w2 == w {
				s.watchers[key] = append(s.watchers[key][:i], s.watchers[key][i+1:]...)
				break
			}
		}
		s.mu.Unlock()
		close(ch)
	}()

	return ch
}

func (s *MockStore) deliverWatchEvent(key string, evType mvccpb.Event_EventType) {
	for prefix, watchers := range s.watchers {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			for _, w := range watchers {
				select {
				case w.ch <- clientv3.WatchResponse{}:
				default:
				}
			}
		}
	}
}

// ---- fencing generation helpers ----

// GetGeneration returns a generation value stored at gen:<nodeID>.
func (s *MockStore) GetGeneration(ctx context.Context, nodeID string) (uint64, error) {
	val, err := s.Get(ctx, metadata.GenKey(nodeID))
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}
	return strconv.ParseUint(string(val), 10, 64)
}

// BumpGeneration atomically increments the generation for a node.
func (s *MockStore) BumpGeneration(ctx context.Context, nodeID string, expectedOld uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := metadata.GenKey(nodeID)
	val := s.kv[key]
	current := uint64(0)
	if val != nil {
		current, _ = strconv.ParseUint(string(val), 10, 64)
	}
	if current != expectedOld {
		return 0, fmt.Errorf("CAS failed: expected %d, got %d", expectedOld, current)
	}
	newGen := expectedOld + 1
	s.kv[key] = []byte(strconv.FormatUint(newGen, 10))
	return newGen, nil
}

var _ metadata.MetadataStore = (*MockStore)(nil)
