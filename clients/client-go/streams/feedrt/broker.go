// Package feedrt is the engine of feed streams (streams.FromFeed): the port
// of the JS SDK's FeedRunner and runtime/feed/*. The same steps, the same
// clocks, the same commits, and the same KV formats (the position record, the
// snapshot layers, every engine's state keys and values), so one stream can
// move between the SDKs: a JS runner stops, a Go runner with the same queryId
// goes on where it was.
//
// The package does not import the client (that would be an import cycle):
// the runner reaches the broker through Broker, which the root package
// implements over the client's own KV, transactions, locks, ephemeral queues
// and feed reader (*queen.Queen.FeedBroker()).
package feedrt

import (
	"context"
	"errors"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
)

// Broker is what a feed runner needs of the Queen client.
type Broker interface {
	// NewReader is queen.feed(options): a feed reader with the client's credentials.
	NewReader(opts feed.Options) (*feed.Reader, error)

	KVGet(ctx context.Context, ns, key string) (KVRow, error)
	KVGetMany(ctx context.Context, ns string, keys []string) (KVMany, error)
	// KVGetPrefix reads one page of a prefix (after: exclusive cursor, "" = from the start).
	KVGetPrefix(ctx context.Context, ns, prefix string, keysOnly bool, limit int, after string) (KVPage, error)

	Transaction() Tx

	// Lock is queen.lock(name, { ttl, retryMinMs, retryMaxMs }).
	Lock(name string, ttl time.Duration, retryMin, retryMax time.Duration) Lock

	// Ephemeral is the client's ephemeral queues, or nil when it has none
	// (a runner then serves no live state).
	Ephemeral() Ephemeral
}

// KVRow is one KV row: Value is decoded JSON (numbers float64).
type KVRow struct {
	Key     string
	Found   bool
	Value   interface{}
	Version int64
}

// KVMany is a getMany answer.
type KVMany struct {
	Rows      []KVRow
	Missing   []string
	Truncated bool
}

// KVPage is a getPrefix page.
type KVPage struct {
	Rows      []KVRow
	Truncated bool
	NextAfter string
}

// PushItem is one message a commit pushes: its payload and its transaction id.
type PushItem struct {
	Data          interface{}
	TransactionID string
}

// Tx is one transaction: the pushes, the KV writes and the guard of a commit.
type Tx interface {
	Guard(lock Lock) Tx
	Push(queue, partition string, items []PushItem) Tx
	// KVCheck is a check at expect (0: the key must not exist).
	KVCheck(ns, key string, expect int64, required bool) Tx
	// KVPut writes with expect when not nil.
	KVPut(ns, key string, value interface{}, expect *int64, required, forever bool) Tx
	KVPutIfAbsent(ns, key string, value interface{}, required, forever bool) Tx
	KVDelete(ns, key string) Tx
	// Commit sends it. A lost precondition is returned (Success false, Reason
	// "kv_precondition"); any other refusal is an error, a *TxError when the
	// broker named a reason ("duplicate", ...). ErrLockNotHeld when a guard's
	// lock holds nothing.
	Commit(ctx context.Context) (TxResult, error)
}

// TxResult is a commit's answer.
type TxResult struct {
	Success bool
	Reason  string
	Error   string
	// KV is the kv rider's results, with the version a write answered.
	KV []KVResult
}

// KVResult is one kv result of a commit.
type KVResult struct {
	Op      string
	Key     string
	Version *int64
}

// ReasonKVPrecondition is a lost required precondition.
const ReasonKVPrecondition = "kv_precondition"

// TxError is a transaction the broker refused, with its reason.
type TxError struct {
	Reason  string
	Message string
	Err     error
}

func (e *TxError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "transaction failed (" + e.Reason + ")"
}

func (e *TxError) Unwrap() error { return e.Err }

// ErrLockNotHeld is LOCK_NOT_HELD: a guard was asked of a lock that holds nothing.
var ErrLockNotHeld = errors.New("LOCK_NOT_HELD")

// Lock is one holder's lease on a lock.
type Lock interface {
	// TryAcquire takes the permit once: false when somebody else holds it.
	TryAcquire(ctx context.Context) (bool, error)
	// Acquire waits for the permit until ctx is done: false when the wait ended first.
	Acquire(ctx context.Context) (bool, error)
	Held() bool
	Release(ctx context.Context) error
	// OnLost runs fn(reason) whenever a held permit is lost (renew, guard, expired).
	OnLost(fn func(reason string))
}

// Ephemeral is the ephemeral queues a runner answers live-state questions on.
type Ephemeral interface {
	Push(ctx context.Context, queue string, payloads []interface{}) error
	// Pop answers the payloads, decoded.
	Pop(ctx context.Context, queue string, wait bool, timeoutMillis, batch int, commitOnDelivery bool) ([]interface{}, error)
	Delete(ctx context.Context, queue string) error
}

// ErrEphemeralUnsupported is matched by the error of a broker without ephemeral queues.
var ErrEphemeralUnsupported = errors.New("ephemeral_unsupported")
