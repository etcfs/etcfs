//go:build integration
// +build integration

package ipc

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/etcfs/etcfs/pkg/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// lostResponseKV fails the next `lose` extent-publishing transactions with a
// timeout, applying the first of them, as a client sees when the leader
// committed the entry, the reply was lost, and the retries got no answer.
type lostResponseKV struct {
	clientv3.KV
	lose    *atomic.Int32
	applied *atomic.Bool
}

func (k lostResponseKV) Txn(ctx context.Context) clientv3.Txn {
	return &lostResponseTxn{Txn: k.KV.Txn(ctx), kv: k}
}

type lostResponseTxn struct {
	clientv3.Txn
	kv     lostResponseKV
	extent bool
}

func (t *lostResponseTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	t.Txn = t.Txn.If(cs...)
	return t
}

func (t *lostResponseTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	for _, op := range ops {
		if op.IsPut() && strings.HasPrefix(string(op.KeyBytes()), metadata.PrefixExtent) {
			t.extent = true
		}
	}
	t.Txn = t.Txn.Then(ops...)
	return t
}

func (t *lostResponseTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	t.Txn = t.Txn.Else(ops...)
	return t
}

func (t *lostResponseTxn) Commit() (*clientv3.TxnResponse, error) {
	if !t.extent || t.kv.lose.Add(-1) < 0 {
		return t.Txn.Commit()
	}
	if t.kv.applied.CompareAndSwap(false, true) {
		if _, err := t.Txn.Commit(); err != nil {
			return nil, err
		}
	}
	return nil, context.DeadlineExceeded
}

// An extent commit whose reply is lost may have landed. Its blocks must not go
// back to the free list on that guess: a published extent would then name
// blocks the next write is handed, and that write overwrites the first file.
// Losing one reply makes the retry's comparison fail against the landed
// commit; losing every reply exhausts the retries.
func TestIntegration_AmbiguousExtentCommitKeepsItsBlocks(t *testing.T) {
	for name, lose := range map[string]int32{"retry rejected": 1, "retries exhausted": etcdAttempts} {
		t.Run(name, func(t *testing.T) { ambiguousExtentCommit(t, lose) })
	}
}

func ambiguousExtentCommit(t *testing.T, lose int32) {
	svc, store := newTestService(t)
	svc.flushInterval = 0
	svc.dataCache = false
	ctx := context.Background()
	const first, second = 7300, 7301
	kv := lostResponseKV{KV: store.Client().KV, lose: new(atomic.Int32), applied: new(atomic.Bool)}
	store.Client().KV = kv
	seedFile(t, store, first, metadata.ModeFile|0644)
	_, err := store.AtomicCreateFile(ctx, metadata.RootIno, t.Name()+"-2", second,
		metadata.ModeFile|0644, 1000, 1000, metadata.CreateExtra{})
	if err != nil {
		t.Fatalf("seed second: %v", err)
	}

	kv.lose.Store(lose)
	original := bytes.Repeat([]byte{0x11}, 4096)
	if _, err := svc.handleWrite(ctx, writePayload(first, 0, original, 0)); err != nil {
		t.Fatalf("first write: %v", err)
	}
	kv.lose.Store(0)
	if _, err := svc.handleWrite(ctx, writePayload(second, 0, bytes.Repeat([]byte{0x22}, 4096), 0)); err != nil {
		t.Fatalf("second write: %v", err)
	}

	a, err := store.GetExtents(ctx, first)
	if err != nil {
		t.Fatalf("extents of first: %v", err)
	}
	b, err := store.GetExtents(ctx, second)
	if err != nil {
		t.Fatalf("extents of second: %v", err)
	}
	for _, x := range a {
		for _, y := range b {
			if x.DiskOff < y.DiskOff+y.Length && y.DiskOff < x.DiskOff+x.Length {
				t.Fatalf("published extents share blocks: %+v and %+v", x, y)
			}
		}
	}
	// The first file's published data, if any, is still its own.
	if len(a) > 0 {
		var rq buf
		rq.w64(first)
		rq.w64(0)
		rq.w32(4096)
		resp, rerr := svc.handleRead(ctx, rq.b)
		if rerr != nil {
			t.Fatalf("read: %v", rerr)
		}
		if got := readPayload(t, resp); !bytes.Equal(got, original) {
			t.Fatalf("first file's data was overwritten")
		}
	}
}
