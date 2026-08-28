package membership

import (
	"context"
	"fmt"
	"strconv"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/etcfs/etcfs/pkg/metadata"
)

// MetadataStore is the slice of the metadata store membership needs.
type MetadataStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)
	Put(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error)
	Delete(ctx context.Context, key string) error
	Txn(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, error)
}

type Manager struct {
	Store  MetadataStore
	NodeID string
}

func New(store MetadataStore, nodeID string) *Manager {
	return &Manager{Store: store, NodeID: nodeID}
}

func (m *Manager) Join(ctx context.Context) error {
	_ = m.registerMembership(ctx)

	arenas, _ := m.Store.GetPrefix(ctx, metadata.PrefixArena)
	seen := make(map[string]bool)
	for _, kv := range arenas {
		node, _, ok := metadata.ParseArenaKey(string(kv.Key))
		if !ok || seen[node] {
			continue
		}
		seen[node] = true
		m.registerRecognition(ctx, node)
	}

	return m.AcquireArena(ctx)
}

func (m *Manager) registerMembership(ctx context.Context) error {
	key := metadata.MembershipKey(m.NodeID)
	val := []byte(fmt.Sprintf(`{"joined":%d}`, time.Now().Unix()))
	_, err := m.Store.Put(ctx, key, val)
	return err
}

func (m *Manager) registerRecognition(ctx context.Context, peerID string) {
	key := fmt.Sprintf("peers:%s:%s", m.NodeID, peerID)
	_, _ = m.Store.Put(ctx, key, []byte("known"))
}

func (m *Manager) AcquireArena(ctx context.Context) error {
	key := metadata.PrefixArenaLog

	for attempt := 0; attempt < 5; attempt++ {
		val, err := m.Store.Get(ctx, key)
		if err != nil {
			return err
		}
		current := uint64(0)
		if val != nil {
			current = metadata.DecodeUint64(val)
		}
		next := current + 1

		var cmps []clientv3.Cmp
		if current == 0 {
			cmps = []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "=", 0)}
		} else {
			cmps = []clientv3.Cmp{clientv3.Compare(clientv3.Value(key), "=", string(metadata.EncodeUint64(current)))}
		}
		op := clientv3.OpPut(key, string(metadata.EncodeUint64(next)))
		ok, txErr := m.Store.Txn(ctx, cmps, []clientv3.Op{op}, nil)
		if txErr != nil {
			return txErr
		}
		if ok {
			arenaKey := metadata.ArenaOwnerKey(m.NodeID, current)
			_, _ = m.Store.Put(ctx, arenaKey, metadata.EncodeUint64(current))
			return nil
		}
	}
	return fmt.Errorf("arena acquisition exhausted retries")
}

func (m *Manager) LeaveGraceful(ctx context.Context) error {
	arenas, _ := m.Store.GetPrefix(ctx, metadata.ArenaNodePrefix(m.NodeID))
	for _, kv := range arenas {
		_, id, ok := metadata.ParseArenaKey(string(kv.Key))
		if !ok {
			continue
		}
		m.releaseArena(ctx, string(kv.Key), id)
	}
	_ = m.Store.Delete(ctx, metadata.MembershipKey(m.NodeID))
	return nil
}

// releaseArena hands one arena back to the global pool.
//
// Both halves in one transaction: a crash between a separate delete and put
// would leave the arena owned by nobody and listed as free by nobody, which is
// space no node can ever reissue.
func (m *Manager) releaseArena(ctx context.Context, ownerKey string, arenaID uint64) {
	_, _ = m.Store.Txn(ctx, nil, []clientv3.Op{
		clientv3.OpDelete(ownerKey),
		clientv3.OpPut(metadata.FreeArenaKey(arenaID), "free"),
	}, nil)
}

func (m *Manager) LeaveUngraceful(ctx context.Context) {
	_ = m.LeaveGraceful(ctx)
}

// RebalanceArena reassigns arenaID from fromNode to toNode.
//
// Restricted to reclaiming a FENCED node's arena — fromNode must already have
// a bumped fencing generation (gen:<fromNode> > 0). This is not an arbitrary
// restriction: it is the one precondition under which reassignment is
// actually safe. Moving an arena away from a live, healthy node is
// unclosable by any guard — both the old and new owner would be unfenced, so
// no CAS check has anything to reject, and the two could both write into the
// same range. A fencing-generation check turns that into a closed case: once
// fromNode is fenced, the generation guard on its own metadata transactions
// (see pkg/metadata's store-wide guard, if this Store is a guarded
// *metadata.Store) already prevents it from writing anywhere, arenaID
// included, so reassigning its arena to a live node cannot collide with it.
//
// This does NOT provide full proof of quiescence — reissuing an arena still
// requires no proof of quiescence beyond the generation bump, which is a
// real signal (the fenced node cannot commit further writes) but not a
// guarantee its kernel has stopped issuing them at the block-device level.
// That gap is unchanged by this guard: arena reclamation still has no
// device-confirmed quiescence check.
func (m *Manager) RebalanceArena(ctx context.Context, fromNode, toNode string, arenaID uint64) error {
	fromKey := metadata.ArenaOwnerKey(fromNode, arenaID)
	fromVal, err := m.Store.Get(ctx, fromKey)
	if err != nil {
		return fmt.Errorf("rebalance arena %d: read %s: %w", arenaID, fromNode, err)
	}
	if fromVal == nil {
		return fmt.Errorf("rebalance arena %d: source node %s does not hold it", arenaID, fromNode)
	}

	genVal, err := m.Store.Get(ctx, metadata.GenKey(fromNode))
	if err != nil {
		return fmt.Errorf("rebalance arena %d: read generation of %s: %w", arenaID, fromNode, err)
	}
	var gen uint64
	if genVal != nil {
		gen, err = strconv.ParseUint(string(genVal), 10, 64)
		if err != nil {
			return fmt.Errorf("rebalance arena %d: malformed generation for %s: %w", arenaID, fromNode, err)
		}
	}
	if gen == 0 {
		return fmt.Errorf("rebalance arena %d: source node %s has not been fenced — "+
			"rebalancing a live node's arena is unsafe, see kleppmann-stale-write-analysis.md",
			arenaID, fromNode)
	}

	// Single atomic transaction rather than a separate Delete then Put: the
	// unguarded two-step version left a window, on a crash between the two
	// calls, where the arena belonged to neither node — worse than either
	// keeping it or losing it cleanly. The CAS re-verifies fromKey still
	// holds arenaID at commit time, guarding against a concurrent rebalance
	// or reacquisition racing this one between the read above and the write.
	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(fromKey), "=", string(metadata.EncodeUint64(arenaID))),
	}
	toKey := metadata.ArenaOwnerKey(toNode, arenaID)
	ops := []clientv3.Op{
		clientv3.OpDelete(fromKey),
		clientv3.OpPut(toKey, string(metadata.EncodeUint64(arenaID))),
	}
	ok, err := m.Store.Txn(ctx, cmps, ops, nil)
	if err != nil {
		return fmt.Errorf("rebalance arena %d: %s -> %s: %w", arenaID, fromNode, toNode, err)
	}
	if !ok {
		return fmt.Errorf("rebalance arena %d: %s -> %s: concurrent modification, retry", arenaID, fromNode, toNode)
	}
	return nil
}

func (m *Manager) IsMember(ctx context.Context, nodeID string) bool {
	val, _ := m.Store.Get(ctx, metadata.MembershipKey(nodeID))
	return val != nil
}

func (m *Manager) ArenaCount(ctx context.Context, nodeID string) int {
	kvs, _ := m.Store.GetPrefix(ctx, metadata.ArenaNodePrefix(nodeID))
	return len(kvs)
}
