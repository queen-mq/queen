package queen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/feedrt"
)

// FeedBroker is the client as a feed stream's runner uses it: its KV,
// transactions, locks, ephemeral queues and feed reader. *Queen is a
// streams.FeedClient through it, so streams.FromFeed(client, ...) takes the
// client itself. The streams packages cannot import this one (an import
// cycle), so the adapter lives here, as streams_adapter.go does for queues.
func (q *Queen) FeedBroker() feedrt.Broker { return &feedBroker{q: q} }

type feedBroker struct{ q *Queen }

func (b *feedBroker) NewReader(opts feed.Options) (*feed.Reader, error) { return b.q.Feed(opts) }

func decodeValue(raw json.RawMessage) interface{} {
	if len(raw) == 0 {
		return nil
	}
	v, err := jsv.Unmarshal(raw)
	if err != nil {
		return nil
	}
	return v
}

func (b *feedBroker) KVGet(ctx context.Context, ns, key string) (feedrt.KVRow, error) {
	res, err := b.q.KV().Batch(ctx, KVGetOp(ns, key))
	if err != nil {
		return feedrt.KVRow{}, err
	}
	r := res[0]
	row := feedrt.KVRow{Key: key, Found: r.Found != nil && *r.Found, Version: r.Version}
	if row.Found {
		row.Value = decodeValue(r.Value)
	}
	return row, nil
}

func rowsOf(rows []KVRow) []feedrt.KVRow {
	out := make([]feedrt.KVRow, len(rows))
	for i, r := range rows {
		out[i] = feedrt.KVRow{Key: r.Key, Found: true, Value: decodeValue(r.Value), Version: r.Version}
	}
	return out
}

func (b *feedBroker) KVGetMany(ctx context.Context, ns string, keys []string) (feedrt.KVMany, error) {
	m, err := b.q.KV().GetMany(ctx, ns, keys)
	if err != nil {
		return feedrt.KVMany{}, err
	}
	return feedrt.KVMany{Rows: rowsOf(m.Rows), Missing: m.Missing, Truncated: m.Truncated}, nil
}

func (b *feedBroker) KVGetPrefix(ctx context.Context, ns, prefix string, keysOnly bool, limit int, after string) (feedrt.KVPage, error) {
	p, err := b.q.KV().GetPrefix(ctx, ns, prefix, KVPrefixOptions{After: after, Limit: limit, KeysOnly: keysOnly})
	if err != nil {
		return feedrt.KVPage{}, err
	}
	return feedrt.KVPage{Rows: rowsOf(p.Rows), Truncated: p.Truncated, NextAfter: p.NextAfter}, nil
}

func (b *feedBroker) Transaction() feedrt.Tx { return &feedTx{tb: b.q.Transaction()} }

func (b *feedBroker) Lock(name string, ttl time.Duration, retryMin, retryMax time.Duration) feedrt.Lock {
	return &feedLock{l: b.q.Lock(name, ttl, LockOptions{RetryMin: retryMin, RetryMax: retryMax})}
}

func (b *feedBroker) Ephemeral() feedrt.Ephemeral { return &feedEphemeral{e: b.q.Ephemeral()} }

// ---- transactions ------------------------------------------------------------

type feedTx struct {
	tb  *TransactionBuilder
	err error
}

func rawJSON(v interface{}) (json.RawMessage, error) {
	b, err := jsv.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

func (t *feedTx) fail(err error) {
	if t.err == nil {
		t.err = err
	}
}

func (t *feedTx) Guard(lock feedrt.Lock) feedrt.Tx {
	if l, ok := lock.(*feedLock); ok {
		t.tb.Guard(l.l)
	} else {
		t.fail(fmt.Errorf("queen: a feed transaction guards with the client's own lock"))
	}
	return t
}

func (t *feedTx) Push(queue, partition string, items []feedrt.PushItem) feedrt.Tx {
	payloads := make([]interface{}, len(items))
	for i, it := range items {
		raw, err := rawJSON(it.Data)
		if err != nil {
			t.fail(err)
			return t
		}
		payloads[i] = PushItem{Payload: raw, TransactionID: it.TransactionID}
	}
	t.tb.Queue(queue).Partition(partition).Push(payloads)
	return t
}

func (t *feedTx) KVCheck(ns, key string, expect int64, required bool) feedrt.Tx {
	t.tb.KV(KVCheckOp(ns, key, expect, KVWriteOptions{Required: required}))
	return t
}

func expiryOf(forever bool) Expiry {
	if forever {
		return Forever()
	}
	return Expiry{err: errors.New("queen: a feed KV write is forever")}
}

func (t *feedTx) KVPut(ns, key string, value interface{}, expect *int64, required, forever bool) feedrt.Tx {
	raw, err := rawJSON(value)
	if err != nil {
		t.fail(err)
		return t
	}
	t.tb.KV(KVPutOp(ns, key, raw, expiryOf(forever), KVWriteOptions{Expect: expect, Required: required}))
	return t
}

func (t *feedTx) KVPutIfAbsent(ns, key string, value interface{}, required, forever bool) feedrt.Tx {
	raw, err := rawJSON(value)
	if err != nil {
		t.fail(err)
		return t
	}
	t.tb.KV(KVPutIfAbsentOp(ns, key, raw, expiryOf(forever), KVWriteOptions{Required: required}))
	return t
}

func (t *feedTx) KVDelete(ns, key string) feedrt.Tx {
	t.tb.KV(KVDeleteOp(ns, key))
	return t
}

func (t *feedTx) Commit(ctx context.Context) (feedrt.TxResult, error) {
	if t.err != nil {
		return feedrt.TxResult{}, t.err
	}
	resp, err := t.tb.Commit(ctx)
	if err != nil {
		if errors.Is(err, ErrLockNotHeld) {
			return feedrt.TxResult{}, fmt.Errorf("%w: %v", feedrt.ErrLockNotHeld, err)
		}
		if resp != nil && resp.Reason != "" {
			return feedrt.TxResult{}, &feedrt.TxError{Reason: resp.Reason, Message: resp.Error, Err: err}
		}
		return feedrt.TxResult{}, err
	}
	out := feedrt.TxResult{Success: resp.Success, Reason: resp.Reason, Error: resp.Error}
	for _, raw := range resp.Results {
		var r struct {
			Type    string `json:"type"`
			Op      string `json:"op"`
			Key     string `json:"key"`
			Version *int64 `json:"version"`
		}
		if decodeKV(raw, &r) == nil && r.Type == "kv" {
			out.KV = append(out.KV, feedrt.KVResult{Op: r.Op, Key: r.Key, Version: r.Version})
		}
	}
	return out, nil
}

// ---- locks -------------------------------------------------------------------------

type feedLock struct {
	l        *Lock
	mu       sync.Mutex
	handlers []func(string)
	watching <-chan struct{}
	stop     chan struct{}
}

func (f *feedLock) watch() {
	lost := f.l.Lost()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.watching == lost {
		return
	}
	if f.stop != nil {
		close(f.stop)
	}
	stop := make(chan struct{})
	f.watching, f.stop = lost, stop
	go func() {
		select {
		case <-lost:
			f.mu.Lock()
			hs := append([]func(string){}, f.handlers...)
			f.mu.Unlock()
			for _, h := range hs {
				func() {
					defer func() { _ = recover() }()
					h("lost")
				}()
			}
		case <-stop:
		}
	}()
}

func (f *feedLock) unwatch() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stop != nil {
		close(f.stop)
		f.stop = nil
	}
	f.watching = nil
}

func (f *feedLock) TryAcquire(ctx context.Context) (bool, error) {
	ok, err := f.l.TryAcquire(ctx)
	if ok {
		f.watch()
	}
	return ok, err
}

func (f *feedLock) Acquire(ctx context.Context) (bool, error) {
	if err := f.l.Acquire(ctx); err != nil {
		if ctx.Err() != nil {
			return false, nil
		}
		return false, err
	}
	f.watch()
	return true, nil
}

func (f *feedLock) Held() bool { return f.l.Held() }

func (f *feedLock) Release(ctx context.Context) error {
	f.unwatch()
	_, err := f.l.Release(ctx)
	return err
}

func (f *feedLock) OnLost(fn func(reason string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers = append(f.handlers, fn)
}

// ---- ephemeral queues ----------------------------------------------------------------

type feedEphemeral struct{ e *Ephemeral }

func ephErr(err error) error {
	if errors.Is(err, ErrEphemeralUnsupported) {
		return fmt.Errorf("%w: %v", feedrt.ErrEphemeralUnsupported, err)
	}
	return err
}

func (f *feedEphemeral) Push(ctx context.Context, queue string, payloads []interface{}) error {
	items := make([]EphemeralMessage, len(payloads))
	for i, p := range payloads {
		raw, err := rawJSON(p)
		if err != nil {
			return err
		}
		items[i] = EphemeralMessage{Payload: raw}
	}
	_, err := f.e.Push(ctx, queue, items)
	return ephErr(err)
}

func (f *feedEphemeral) Pop(ctx context.Context, queue string, wait bool, timeoutMillis, batch int, commitOnDelivery bool) ([]interface{}, error) {
	got, err := f.e.Pop(ctx, queue, EphemeralPopOptions{Wait: wait, TimeoutMillis: timeoutMillis, Batch: batch, CommitOnDelivery: commitOnDelivery})
	if err != nil {
		return nil, ephErr(err)
	}
	out := make([]interface{}, 0, len(got.Messages))
	for _, m := range got.Messages {
		out = append(out, decodeValue(m.Payload))
	}
	return out, nil
}

func (f *feedEphemeral) Delete(ctx context.Context, queue string) error {
	_, err := f.e.Delete(ctx, queue)
	return ephErr(err)
}
