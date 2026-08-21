package queen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
)

// The ephemeral surface (EPHEMERAL_QUEUES.md §1, §3.1, §4).
//
// Eight verbs over one route family, `/api/v1/ephemeral/*`: Configure, Reset,
// Delete, Push, Pop, Ack, Queues, Depth. Flat functions, not a builder chain --
// the durable `Queue(name).Partition(p).Push(...)` fluency exists because a
// durable queue has a dozen configured properties that read well as a sentence;
// an ephemeral queue has a ring in a broker's RAM and a handful of bounds, and a
// chain would only hide how few moving parts there are.
//
// WHAT THIS SURFACE IS ABOUT, BEFORE ANY SIGNATURE: contents survive NOTHING
// (§1.2). Not a restart, not a crash, not a deploy, not the ownership move that a
// membership change causes. Treat a failover like a Redis restart. Declared
// CONFIGURATION is durable -- it lives in PG and comes back after a restart, as
// configured and EMPTY. There is no replay, no history, no subscriptionMode and
// no DLQ, because none of those concepts has a referent when there is no history
// to have.
//
// DELIVERY IS NOT "AT MOST ONCE" (§1.3), and the docs must not say it is. The
// class picks what can be LOST; the ack mode picks the guarantee. AutoAck
// advances the cursor at delivery and is at-most-once. The default -- explicit
// ack -- is at-least-once for as long as the owning broker incarnation lives: an
// unacked message redelivers when its lease expires, with Attempts incremented,
// until retryLimit, after which it is DROPPED and counted (no DLQ, §9). Consumers
// still need idempotency, exactly as on durable queues.
//
// CONSUMPTION SEMANTICS COME FROM THE GROUP, EXACTLY AS ON THE DURABLE ENGINE
// (§1.5). There is no queue-level mode to choose:
//
//	Pop(ctx, q, EphemeralPopOptions{Group: "workers"}) // competing: one cursor
//	Pop(ctx, q, EphemeralPopOptions{Group: "tail-a"})  // fan-out: own cursor
//	Pop(ctx, q)                                        // groupless queue mode
//
// Every group has its own cursor over the ONE ring, so fan-out subscribers each
// see everything and competing consumers of one group share the work.
//
// ORDERING is FIFO per (queue, partition) within one ownership incarnation.
// Across an incarnation boundary the question is empty: the contents are gone.
//
// AND THE ONE ERROR YOU WILL MEET FIRST. No SDK in this repo negotiates a
// version. Against a broker or proxy older than 1.1 the whole family answers 404
// -- the broker because the routes do not exist, the proxy because an unknown API
// path is `route_blocked` -- so a 404 on this family is mapped to
// ErrEphemeralUnsupported, with the original *HTTPError still reachable through
// errors.As. Branch on the sentinel, never on the prose.
//
// With exactly one exception, and it is why that mapping reads the CODE and not
// just the status: Depth is the only verb that can 404 for a reason of its own,
// because it is the only one that has to say "no such queue" -- every other verb
// either creates the queue by naming it (Push, Pop) or describes a miss inside a
// 200 (Reset answers dropped:0, Delete answers Deleted:false). That 404 carries
// `code: ephemeral_queue_not_found` and becomes ErrEphemeralQueueNotFound.

// ---------------------------------------------------------------------------
// The old-broker error.
// ---------------------------------------------------------------------------

// ErrEphemeralUnsupported is what every 404 on this route family becomes.
//
// Match it with errors.Is. It means the routes are not there: an old broker 404s
// because it never registered them, an old proxy 404s `route_blocked` because it
// fails closed on unknown API paths. Both are "upgrade", neither is "your queue
// is missing" -- the ephemeral verbs answer an absent queue with a normal body,
// never a 404.
//
// The message is fixed and identical across every SDK (§4), so an operator who
// greps two clients' logs sees one string.
var ErrEphemeralUnsupported = errors.New("broker/proxy does not support ephemeral queues (requires >= 1.1)")

// EphemeralUnsupportedError is the concrete error carrying that verdict. It
// unwraps to BOTH the sentinel above and the *HTTPError it was built from, so
// errors.Is(err, ErrEphemeralUnsupported) and errors.As(err, &httpErr) both work
// on the same value.
type EphemeralUnsupportedError struct {
	// HTTP is the 404 as it arrived, kept so the body and any proxy `code`
	// stay reachable for diagnosis.
	HTTP *HTTPError
}

func (e *EphemeralUnsupportedError) Error() string {
	return "queen: " + ErrEphemeralUnsupported.Error()
}

// Unwrap returns the sentinel and the transport error. The multi-error form
// (Go 1.20+) is what lets one value answer both questions a caller can ask:
// "is this the upgrade case?" and "what exactly came back?".
func (e *EphemeralUnsupportedError) Unwrap() []error {
	if e.HTTP == nil {
		return []error{ErrEphemeralUnsupported}
	}
	return []error{ErrEphemeralUnsupported, e.HTTP}
}

// EphemeralCodeQueueNotFound is the `code` on the one 404 this family answers
// for a reason of its own -- see ErrEphemeralQueueNotFound.
const EphemeralCodeQueueNotFound = "ephemeral_queue_not_found"

// ErrEphemeralQueueNotFound is what Depth answers for a queue that is not there.
//
// It is the ONLY verb that can, and that is worth knowing rather than
// discovering: Push and Pop create a queue by naming it, Reset answers
// dropped:0, and Delete answers Deleted:false. So this is a real "no such
// queue", not the family-wide upgrade case, and the two are told apart by the
// error code rather than by the status they share.
//
// Match it with errors.Is; the *HTTPError stays reachable through errors.As.
var ErrEphemeralQueueNotFound = errors.New("no ephemeral queue by that name exists on this broker")

// ephemeralError is the one place a request from this file turns into a caller's
// error, so the 404 rule has a single home.
//
// Everything that is not a 404 is passed through UNCHANGED, and deliberately not
// through surfaceError: the KV/timer envelope puts the machine-readable code in
// `error`, while this family's envelope is `{error: <prose>, code: <stable id>}`
// -- the proxy's shape, which *HTTPError.Code already parses correctly. Running
// it through surfaceError would file the prose under Code and make the one field
// a client may branch on unusable.
func ephemeralError(err error) error {
	if err == nil {
		return nil
	}
	var he *HTTPError
	if errors.As(err, &he) && he.StatusCode == 404 {
		// The code, not the status. An old broker and an old proxy both answer
		// 404 with no ephemeral code of their own, so anything that DOES carry
		// one is a live broker answering a question about a queue.
		if he.Code == EphemeralCodeQueueNotFound {
			return fmt.Errorf("%w: %w", ErrEphemeralQueueNotFound, he)
		}
		return &EphemeralUnsupportedError{HTTP: he}
	}
	return err
}

// ---------------------------------------------------------------------------
// Configure options.
// ---------------------------------------------------------------------------

// The two bound policies of §1.6. `reject` answers 429 `queue_full` -- the same
// backpressure shape the 1.0.6 buffered push already drains against -- while
// `dropOldest` is feed semantics: the ring keeps accepting and the head falls
// off, counted as `eph_dropped_bounds`.
const (
	EphemeralPolicyReject     = "reject"
	EphemeralPolicyDropOldest = "dropOldest"
)

// EphemeralWindowBuffer lets a WAITING pop fatten its batch: it returns when
// Count messages are ready or Ms have passed since the first one, bounded by the
// pop's own timeout (§1.7). Delivery-side batching only -- it changes nothing
// about what is stored.
type EphemeralWindowBuffer struct {
	Ms    int `json:"ms,omitempty"`
	Count int `json:"count,omitempty"`
}

// EphemeralOptions are the seven knobs of Configure (§3.1).
//
// THIS STRUCT IS THE CLOSED LIST. The JS client has to refuse an unknown option
// key by hand, because an object literal will happily carry a misspelt
// `ttlSecond` that is then dropped on the floor -- and every one of these bounds
// something (bytes, length, age, redelivery), so a silently ignored one is a ring
// that grows until a global budget answers 503. In Go the compiler is that check:
// a field this client does not know does not compile, which is the same refusal
// one stage earlier.
//
// The numeric knobs are pointers so that "not supplied" and "supplied as zero"
// stay different things; build them with Int64. Absent knobs are omitted from the
// body entirely and the broker's own defaults own them.
type EphemeralOptions struct {
	// MaxBytes and MaxLength are the per-queue budget (defaults 16 MiB /
	// 10 000). Policy decides what breaching it does.
	MaxBytes  *int64 `json:"maxBytes,omitempty"`
	MaxLength *int64 `json:"maxLength,omitempty"`

	// Policy is EphemeralPolicyReject (the default) or
	// EphemeralPolicyDropOldest. The value is passed through unvalidated: an
	// unknown one is refused by the broker, which is the only party that knows
	// the current list.
	Policy string `json:"policy,omitempty"`

	// TTLSeconds drops messages older than this, head first.
	//
	// It is NOT the durable `retention`, and the two words are kept apart on
	// purpose: retention cleans consumed history and never touches pending,
	// while this drops UNCONSUMED messages. One word per contract.
	TTLSeconds *int64 `json:"ttlSeconds,omitempty"`

	// LeaseSeconds and RetryLimit shape redelivery (defaults 30 / 5). A
	// message whose attempts reach RetryLimit is dropped and counted; there is
	// no DLQ on this class (§9).
	LeaseSeconds *int64 `json:"leaseSeconds,omitempty"`
	RetryLimit   *int64 `json:"retryLimit,omitempty"`

	WindowBuffer *EphemeralWindowBuffer `json:"windowBuffer,omitempty"`
}

// ---------------------------------------------------------------------------
// Messages, acks, and the shapes that come back.
// ---------------------------------------------------------------------------

// EphemeralMessage is one message on the push wire, and it is `{payload}` and
// nothing else -- no transactionId, because there is no dedup index to hold one,
// and no queue or partition, because the envelope already carries them.
type EphemeralMessage struct {
	Payload interface{} `json:"payload"`
}

// EphemeralPopped is one delivered message: `{id, partition, payload, attempts}`.
//
// ID is OPAQUE. It encodes the owning broker incarnation, which is what lets an
// ack arriving after a restart or an ownership move answer `stale` instead of
// acking somebody else's message. Do not parse it.
//
// Payload is handed back as raw JSON for the same reason a KV value is: it is
// whatever the producer sent, and this client does not pretend to know its shape.
// Decode it with json.Unmarshal into your own type.
type EphemeralPopped struct {
	ID        string          `json:"id"`
	Partition string          `json:"partition"`
	Payload   json.RawMessage `json:"payload"`
	// Attempts counts deliveries of this message, starting at 1. It grows on
	// redelivery after a lease expiry or a failed/retried ack.
	Attempts int `json:"attempts"`
}

// EphemeralBatch is the answer to a Pop. Messages is never nil -- an empty batch
// is an empty slice, so a range over it is always safe.
type EphemeralBatch struct {
	Queue    string
	Messages []EphemeralPopped
}

// The statuses an ack may carry (§1.3). `completed` is the default when none is
// given; `failed` and `retry` both redeliver with Attempts+1 until RetryLimit,
// after which the message is dropped and counted.
const (
	EphemeralStatusCompleted = "completed"
	EphemeralStatusFailed    = "failed"
	EphemeralStatusRetry     = "retry"
)

// The four outcomes of an ack (§3.1). A closed taxonomy: a client that has to
// tell them apart writes a switch, it does not read a sentence.
const (
	// EphemeralOutcomeAcked -- the cursor advanced past this message.
	EphemeralOutcomeAcked = "acked"
	// EphemeralOutcomeRedelivered -- a failed/retry ack put it back.
	EphemeralOutcomeRedelivered = "redelivered"
	// EphemeralOutcomeStale -- the id belongs to a previous incarnation of the
	// ring. NOT an error and never arrives as one: it is how this class fences
	// a restart or an ownership move without a lease protocol.
	EphemeralOutcomeStale = "stale"
	// EphemeralOutcomeUnknown -- the lease is no longer ours to release
	// (already acked, or already expired and redelivered).
	EphemeralOutcomeUnknown = "unknown"
)

// EphemeralAck is one entry of the ack array.
type EphemeralAck struct {
	ID     string `json:"id"`
	Status string `json:"status,omitempty"`
	// Error is accepted by the broker and IGNORED on this class -- there is no
	// DLQ row and no trace store to record it on (§9). It exists so one ack
	// builder can serve both engines.
	Error string `json:"error,omitempty"`
}

// EphemeralAckResult is one `{id, outcome}` of the answer, in request order.
type EphemeralAckResult struct {
	ID      string `json:"id"`
	Outcome string `json:"outcome"`
}

// EphemeralPushResult is what a Push produced.
//
// Buffered is the fork in this struct: a buffered push resolves once the
// messages are IN the buffer, not once they are at the broker, so Pushed is
// meaningless there and Count is the number accepted into the buffer instead.
type EphemeralPushResult struct {
	// Pushed is the broker's count, from `{pushed}`. Zero on a buffered push.
	Pushed int64
	// Buffered reports that these messages went into a client-side buffer.
	Buffered bool
	// Count is how many messages this call accounted for: what the buffer took
	// on a buffered push, what was sent on a direct one.
	Count int
}

// ---------------------------------------------------------------------------
// Per-call options.
// ---------------------------------------------------------------------------

// EphemeralPushOptions are the optional halves of a push.
type EphemeralPushOptions struct {
	// Partition picks the ring -- FIFO is per partition (§1.4). Left empty,
	// the field never reaches the wire and the BROKER picks; this client does
	// not invent a default, because which partition an ephemeral push without
	// one lands on is the broker's rule to make.
	Partition string

	// Buffered turns this push into a client-side batch through the SAME
	// machinery the durable push uses (§4.1): blocking backpressure at MaxSize,
	// a failed batch back at the FRONT and retried, Queen.Close draining it.
	//
	// A buffered message that has not flushed dies with the process. That is
	// already inside this class's contract, which is exactly why buffering is a
	// reasonable default here and a considered decision on a durable queue.
	Buffered *BufferConfig
}

// EphemeralPopOptions are the optional halves of a pop.
type EphemeralPopOptions struct {
	Partition string
	// Batch is the ceiling on messages returned; the broker's default is 1.
	Batch int

	// Wait is a real long poll, parked on a RAM gate with no database behind it
	// and no polling interval anywhere (§3.4) -- the structural reason an
	// ephemeral inbox answers in transport time.
	Wait bool
	// TimeoutMillis is how long the BROKER waits, in milliseconds; it is sent
	// only when Wait is set, and defaults to EphemeralDefaultWaitTimeoutMillis.
	// The HTTP deadline is set past it so the broker's own timeout always fires
	// first.
	TimeoutMillis int

	// Group is the whole of the consumption semantics (§1.5): same group =
	// competing consumers, own group = fan-out, empty = queue mode.
	Group string

	// AutoAck commits at delivery. At-most-once, and no lease bookkeeping at
	// all -- there is nothing to ack afterwards.
	AutoAck bool
}

// EphemeralAckOptions are the optional halves of an ack. A per-entry Status or
// Error on an EphemeralAck wins over these, which is how a mixed batch (some
// completed, one retry) travels in a single request.
type EphemeralAckOptions struct {
	// Group must be the group the pop used -- cursors are per group.
	Group  string
	Status string
	Error  string
}

// EphemeralDefaultWaitTimeoutMillis is the long-poll default, matching the
// durable pop's, when Wait is asked for without a TimeoutMillis.
const EphemeralDefaultWaitTimeoutMillis = 30000

// ephemeralWaitSlackMillis keeps the HTTP deadline past the server's own
// long-poll timeout, so the client never aborts a request the broker was about
// to answer. Same 5s slack the durable pop uses.
const ephemeralWaitSlackMillis = 5000

// ---------------------------------------------------------------------------
// The client surface.
// ---------------------------------------------------------------------------

// Ephemeral is the ephemeral-queue API. Get it from Queen.Ephemeral.
type Ephemeral struct {
	httpClient    *HttpClient
	bufferManager *BufferManager
}

// Ephemeral returns the ephemeral-queue API.
func (q *Queen) Ephemeral() *Ephemeral {
	return &Ephemeral{httpClient: q.httpClient, bufferManager: q.bufferManager}
}

func requireEphemeralQueue(queue string) error {
	if queue == "" {
		return errors.New("queen: ephemeral queue name must not be empty")
	}
	return nil
}

// ------------------------------------------------------------- declaration

// Configure declares a queue and its bounds, and persists the OPTIONS in PG
// (§1.1): the configuration survives a restart, the contents never do, and the
// queue comes back declared and empty.
//
// Optional in every sense -- a push or a pop that names an unknown queue creates
// it implicitly with the tenant defaults. Declare when you want non-default
// bounds, or when you want the queue to exist in the dashboard before its first
// message.
func (e *Ephemeral) Configure(ctx context.Context, queue string, options EphemeralOptions) (map[string]interface{}, error) {
	if err := requireEphemeralQueue(queue); err != nil {
		return nil, err
	}
	body := ephemeralConfigureRequest{Queue: queue, Options: options}
	result, err := e.httpClient.Post(ctx, "/api/v1/ephemeral/configure", body)
	if err != nil {
		return nil, ephemeralError(err)
	}
	logDebug("Ephemeral.Configure", map[string]interface{}{"queue": queue})
	return result, nil
}

// Reset drops every message, voids every lease and rewinds every group cursor.
// It returns how many messages were dropped.
//
// A verb that would be indefensible on a durable queue and is merely honest
// here: it destroys nothing the class ever promised to keep (§1.2). The declared
// configuration stays.
func (e *Ephemeral) Reset(ctx context.Context, queue string) (int64, error) {
	if err := requireEphemeralQueue(queue); err != nil {
		return 0, err
	}
	raw, err := e.httpClient.PostRaw(ctx, "/api/v1/ephemeral/reset", ephemeralQueueRequest{Queue: queue})
	if err != nil {
		return 0, ephemeralError(err)
	}
	var out struct {
		Dropped int64 `json:"dropped"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			return 0, fmt.Errorf("queen: ephemeral reset response: %w", err)
		}
	}
	logDebug("Ephemeral.Reset", map[string]interface{}{"queue": queue, "dropped": out.Dropped})
	return out.Dropped, nil
}

// EphemeralDeleted is the answer to a Delete.
//
// CHECK Deleted, NOT err. A queue that was not there is a 200 with
// Deleted:false, and the in-house scar behind this struct is the durable queue
// delete: it answers deleted:false the same way, and every client that ignored
// the field read a miss as a success. The status code describes the call; the
// field describes the queue.
type EphemeralDeleted struct {
	Queue string `json:"queue"`
	// Deleted is true when anything went: the RAM rings, the declaration row,
	// or both.
	Deleted bool `json:"deleted"`
	// Declared reports whether a PG declaration row was removed too -- i.e.
	// whether this was a declared queue rather than an implicit one. It is the
	// only part of an ephemeral queue that ever survived anything.
	Declared bool `json:"declared"`
}

// Delete removes the queue: contents, cursors, and the declared configuration in
// PG.
func (e *Ephemeral) Delete(ctx context.Context, queue string) (EphemeralDeleted, error) {
	if err := requireEphemeralQueue(queue); err != nil {
		return EphemeralDeleted{}, err
	}
	raw, err := e.httpClient.DeleteRaw(ctx, "/api/v1/ephemeral/queue/"+url.PathEscape(queue), nil)
	if err != nil {
		return EphemeralDeleted{}, ephemeralError(err)
	}
	out := EphemeralDeleted{Queue: queue}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			return EphemeralDeleted{}, fmt.Errorf("queen: ephemeral delete response: %w", err)
		}
	}
	if out.Queue == "" {
		out.Queue = queue
	}
	logDebug("Ephemeral.Delete", map[string]interface{}{
		"queue": queue, "deleted": out.Deleted, "declared": out.Declared,
	})
	return out, nil
}

// ------------------------------------------------------------------- push

// Push sends one message or many. All-or-nothing per request.
//
// messages may be a single payload, an EphemeralMessage, a slice of either, or a
// slice of maps:
//
//	e.Push(ctx, "presence", map[string]interface{}{"user": "a"})
//	e.Push(ctx, "presence", msgs, EphemeralPushOptions{Partition: "room-7"})
//
// Unlike the JS client there is no `{payload: ...}` / `{data: ...}` map sugar: Go
// has a type for that, EphemeralMessage, and reading a key out of an arbitrary
// map would mean a map that happens to carry a "payload" key travels with its
// other keys silently dropped. A map is always the payload.
func (e *Ephemeral) Push(ctx context.Context, queue string, messages interface{}, opts ...EphemeralPushOptions) (EphemeralPushResult, error) {
	if err := requireEphemeralQueue(queue); err != nil {
		return EphemeralPushResult{}, err
	}
	if len(opts) > 1 {
		return EphemeralPushResult{}, errors.New("queen: pass at most one EphemeralPushOptions")
	}
	var opt EphemeralPushOptions
	if len(opts) == 1 {
		opt = opts[0]
	}

	items, err := toEphemeralMessages(messages)
	if err != nil {
		return EphemeralPushResult{}, err
	}
	if len(items) == 0 {
		return EphemeralPushResult{}, nil
	}

	if opt.Buffered != nil {
		return e.pushBuffered(ctx, queue, opt.Partition, items, *opt.Buffered)
	}
	return e.pushDirect(ctx, queue, opt.Partition, items)
}

// pushDirect is the unbuffered POST, and the drain of the buffered one.
func (e *Ephemeral) pushDirect(ctx context.Context, queue, partition string, items []EphemeralMessage) (EphemeralPushResult, error) {
	body := ephemeralPushRequest{Queue: queue, Partition: partition, Messages: items}
	raw, err := e.httpClient.PostRaw(ctx, ephemeralPushPath, body)
	if err != nil {
		return EphemeralPushResult{}, ephemeralError(err)
	}
	var out struct {
		Pushed int64 `json:"pushed"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			return EphemeralPushResult{}, fmt.Errorf("queen: ephemeral push response: %w", err)
		}
	}
	logDebug("Ephemeral.Push", map[string]interface{}{
		"queue": queue, "partition": partition, "count": len(items), "pushed": out.Pushed,
	})
	return EphemeralPushResult{Pushed: out.Pushed, Count: len(items)}, nil
}

// pushBuffered hands the messages to the shared buffer machinery under an
// ephemeral address, with an ephemeral sink.
func (e *Ephemeral) pushBuffered(ctx context.Context, queue, partition string, items []EphemeralMessage, config BufferConfig) (EphemeralPushResult, error) {
	if e.bufferManager == nil {
		return EphemeralPushResult{}, errors.New("queen: buffered ephemeral push needs the client's buffer manager -- use Queen.Ephemeral(), not a hand-built Ephemeral")
	}

	// PushItem is the buffer's element type on both families. The ephemeral
	// sink reads only Payload off it -- Queue and Partition are carried so a
	// re-queued batch stays self-describing, and TransactionID is left empty
	// because this wire has nowhere to put one.
	pushItems := make([]PushItem, len(items))
	for i, m := range items {
		pushItems[i] = PushItem{Queue: queue, Partition: partition, Payload: m.Payload}
	}

	address := ephemeralBufferAddress(queue, partition)
	if err := e.bufferManager.Add(ctx, address, pushItems, config, e.ephemeralSink(queue, partition)); err != nil {
		logError("Ephemeral.Push", map[string]interface{}{
			"queue": queue, "partition": partition, "status": "not-buffered",
			"count": len(items), "error": err.Error(),
		})
		return EphemeralPushResult{}, fmt.Errorf("queen: failed to buffer ephemeral messages: %w", err)
	}

	logDebug("Ephemeral.Push", map[string]interface{}{
		"queue": queue, "partition": partition, "status": "buffered", "count": len(items),
	})
	return EphemeralPushResult{Buffered: true, Count: len(items)}, nil
}

// Flush sends everything buffered for one ephemeral queue/partition, now.
//
// Queen.Close and Queen.FlushAllBuffers already drain ephemeral buffers with
// everything else -- they live in the same manager under a namespaced address.
// This is for the times one queue has to land before the rest.
func (e *Ephemeral) Flush(ctx context.Context, queue string, opts ...EphemeralPushOptions) error {
	if err := requireEphemeralQueue(queue); err != nil {
		return err
	}
	if e.bufferManager == nil {
		return nil
	}
	var partition string
	if len(opts) == 1 {
		partition = opts[0].Partition
	}
	return e.bufferManager.Flush(ctx, ephemeralBufferAddress(queue, partition))
}

// -------------------------------------------------------------------- pop

// Pop takes up to Batch messages. Messages is an EMPTY SLICE when there was
// nothing, never nil.
//
//	batch, err := e.Pop(ctx, "inbox", EphemeralPopOptions{Wait: true})
//	for _, msg := range batch.Messages { ... }
func (e *Ephemeral) Pop(ctx context.Context, queue string, opts ...EphemeralPopOptions) (EphemeralBatch, error) {
	if err := requireEphemeralQueue(queue); err != nil {
		return EphemeralBatch{}, err
	}
	if len(opts) > 1 {
		return EphemeralBatch{}, errors.New("queen: pass at most one EphemeralPopOptions")
	}
	var opt EphemeralPopOptions
	if len(opts) == 1 {
		opt = opts[0]
	}

	timeoutMillis := opt.TimeoutMillis
	if timeoutMillis <= 0 {
		timeoutMillis = EphemeralDefaultWaitTimeoutMillis
	}

	params := url.Values{}
	params.Set("queue", queue)
	if opt.Partition != "" {
		params.Set("partition", opt.Partition)
	}
	if opt.Batch > 0 {
		params.Set("batch", strconv.Itoa(opt.Batch))
	}
	// Sent only when waiting, so a plain pop is the shortest query this route
	// can receive and the broker's own defaults own everything else.
	if opt.Wait {
		params.Set("wait", "true")
		params.Set("timeout", strconv.Itoa(timeoutMillis))
	}
	if opt.Group != "" {
		params.Set("group", opt.Group)
	}
	if opt.AutoAck {
		params.Set("autoAck", "true")
	}

	// Affinity so repeated pops of one queue land on one backend when the
	// client holds several URLs: the broker forwards to the rendezvous owner
	// either way, so this saves a hop, it does not create correctness.
	group := opt.Group
	if group == "" {
		group = QueueModeConsumerGroup
	}
	partition := opt.Partition
	if partition == "" {
		partition = "*"
	}
	affinityKey := fmt.Sprintf("%s:%s:%s", queue, partition, group)

	httpTimeout := 0
	var reqOpts []RequestOption
	if opt.Wait {
		httpTimeout = timeoutMillis + ephemeralWaitSlackMillis
		// A long poll that meets a 429 should back off and keep waiting rather
		// than give up after a handful of tries.
		reqOpts = append(reqOpts, WithLongPollRetry())
	}

	raw, err := e.httpClient.GetRawWith(ctx, "/api/v1/ephemeral/pop?"+params.Encode(), httpTimeout, affinityKey, reqOpts...)
	if err != nil {
		return EphemeralBatch{}, ephemeralError(err)
	}

	out := struct {
		Queue    string            `json:"queue"`
		Messages []EphemeralPopped `json:"messages"`
	}{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			return EphemeralBatch{}, fmt.Errorf("queen: ephemeral pop response: %w", err)
		}
	}
	if out.Queue == "" {
		out.Queue = queue
	}
	if out.Messages == nil {
		out.Messages = []EphemeralPopped{}
	}

	logDebug("Ephemeral.Pop", map[string]interface{}{
		"queue": queue, "group": opt.Group, "wait": opt.Wait, "count": len(out.Messages),
	})
	return EphemeralBatch{Queue: out.Queue, Messages: out.Messages}, nil
}

// -------------------------------------------------------------------- ack

// Ack acknowledges popped messages and returns one `{id, outcome}` per ack, in
// request order.
//
// acks may be an EphemeralPopped, a slice of them, a bare id string, a slice of
// strings, or EphemeralAck values built by hand:
//
//	e.Ack(ctx, "inbox", batch.Messages, EphemeralAckOptions{Group: "workers"})
//	e.Ack(ctx, "inbox", []EphemeralAck{{ID: id, Status: EphemeralStatusRetry}})
//
// EphemeralOutcomeStale is NOT an error and never arrives as one -- see the
// constant. Pass the same Group the pop used: cursors are per group.
func (e *Ephemeral) Ack(ctx context.Context, queue string, acks interface{}, opts ...EphemeralAckOptions) ([]EphemeralAckResult, error) {
	if err := requireEphemeralQueue(queue); err != nil {
		return nil, err
	}
	if len(opts) > 1 {
		return nil, errors.New("queen: pass at most one EphemeralAckOptions")
	}
	var opt EphemeralAckOptions
	if len(opts) == 1 {
		opt = opts[0]
	}

	list, err := toEphemeralAcks(acks, opt)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return []EphemeralAckResult{}, nil
	}

	raw, err := e.httpClient.PostRaw(ctx, "/api/v1/ephemeral/ack", ephemeralAckRequest{
		Queue: queue,
		Group: opt.Group,
		Acks:  list,
	})
	if err != nil {
		return nil, ephemeralError(err)
	}
	var out struct {
		Results []EphemeralAckResult `json:"results"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, fmt.Errorf("queen: ephemeral ack response: %w", err)
		}
	}
	if out.Results == nil {
		out.Results = []EphemeralAckResult{}
	}
	logDebug("Ephemeral.Ack", map[string]interface{}{
		"queue": queue, "group": opt.Group, "count": len(list),
	})
	return out.Results, nil
}

// ----------------------------------------------------------------- status

// Queues lists every ephemeral queue this tenant currently has, declared and
// implicit.
//
// Free to poll: the gauges are read out of the broker's own memory, with no
// database behind them -- unlike the durable meter, whose 1s poll is
// load-bearing on PG.
//
// It hands back the decoded body rather than a struct, exactly as the Admin
// gauge endpoints do: the shape is the broker's, and typing it here would make
// the fields that a future broker adds vanish silently on the way to the caller.
func (e *Ephemeral) Queues(ctx context.Context) (map[string]interface{}, error) {
	result, err := e.httpClient.Get(ctx, "/api/v1/ephemeral/queues", 0, "")
	if err != nil {
		return nil, ephemeralError(err)
	}
	return result, nil
}

// Depth returns the gauges for one queue: ring length, bytes, and the per-group
// cursors. Same untyped-body rule as Queues.
//
// This is the ONE verb of the family that answers a 404 about a queue rather
// than about the routes: a queue that is not there comes back as
// ErrEphemeralQueueNotFound, which on this class also means "empty and idle long
// enough to have been collected" -- an implicit queue is a live ring and nothing
// else.
func (e *Ephemeral) Depth(ctx context.Context, queue string) (map[string]interface{}, error) {
	if err := requireEphemeralQueue(queue); err != nil {
		return nil, err
	}
	result, err := e.httpClient.Get(ctx, "/api/v1/ephemeral/queues/"+url.PathEscape(queue)+"/depth", 0, "")
	if err != nil {
		return nil, ephemeralError(err)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Sinks and addresses (the Go half of buffer/sinks.js).
//
// The buffer machinery -- blocking backpressure at MaxSize, one drain loop per
// address, a failed batch put back at the FRONT and retried until it lands -- is
// about ordering, occupancy and loss. None of that is durable-specific, and none
// of it is worth writing twice. In this SDK the seam already exists and needed no
// refactor: BufferManager.Add takes the FlushFunc that will drain the buffer it
// creates, so a sink is just a different closure. The durable drain
// (PushBuilder.executeBuffered) is untouched, byte for byte, and
// TestDurableBufferedPushBodyIsUnchanged exists for no other reason than to fail
// if that ever stops being true.
//
// The two wires disagree about where the identity lives, which is the whole
// reason a sink is a parameter at all:
//
//   - the DURABLE push repeats {queue, partition} on EVERY item, so the envelope
//     is just {items};
//   - the EPHEMERAL push hoists them to the envelope -- {queue, partition?,
//     messages:[{payload}...]} -- so the batch elements carry nothing but their
//     payload.
// ---------------------------------------------------------------------------

const ephemeralPushPath = "/api/v1/ephemeral/push"

// ephemeralSink is the drain of one ephemeral buffer: it reshapes the batch into
// the ephemeral envelope and POSTs it to the ephemeral route.
func (e *Ephemeral) ephemeralSink(queue, partition string) FlushFunc {
	return func(ctx context.Context, items []PushItem) error {
		msgs := make([]EphemeralMessage, len(items))
		for i, it := range items {
			msgs[i] = EphemeralMessage{Payload: it.Payload}
		}
		_, err := e.pushDirect(ctx, queue, partition, msgs)
		return err
	}
}

// ephemeralBufferAddress is the key an ephemeral buffer is held under:
// `eph:queue/partition`, or `eph:queue` when the caller named no partition
// (which is a DIFFERENT destination from any named one, because the broker picks
// and a buffer must not merge the two).
//
// The `eph:` prefix is the same namespacing the broker applies to its own queue
// keys (§3.2), for the same reason: an ephemeral `orders` and a durable `orders`
// are unrelated objects (§10 Q8), and the durable address is `queue/partition`
// (QueueBuilder.getBufferKey) -- so without the prefix one family's messages
// would drain through the other family's sink.
func ephemeralBufferAddress(queue, partition string) string {
	if partition == "" {
		return "eph:" + queue
	}
	return "eph:" + queue + "/" + partition
}

// ---------------------------------------------------------------------------
// Wire bodies. Their JSON tags ARE the contract (§3.1).
// ---------------------------------------------------------------------------

type ephemeralPushRequest struct {
	Queue string `json:"queue"`
	// Omitted, never defaulted client-side.
	Partition string             `json:"partition,omitempty"`
	Messages  []EphemeralMessage `json:"messages"`
}

type ephemeralQueueRequest struct {
	Queue string `json:"queue"`
}

type ephemeralConfigureRequest struct {
	Queue   string           `json:"queue"`
	Options EphemeralOptions `json:"options"`
}

type ephemeralAckRequest struct {
	Queue string         `json:"queue"`
	Group string         `json:"group,omitempty"`
	Acks  []EphemeralAck `json:"acks"`
}

// ---------------------------------------------------------------------------
// Input normalization.
// ---------------------------------------------------------------------------

// toEphemeralMessages accepts the shapes a caller actually has in hand.
func toEphemeralMessages(messages interface{}) ([]EphemeralMessage, error) {
	switch m := messages.(type) {
	case nil:
		return nil, errors.New("queen: a message may not be nil -- push EphemeralMessage{Payload: nil} to send a null payload")
	case EphemeralMessage:
		return []EphemeralMessage{m}, nil
	case []EphemeralMessage:
		return m, nil
	case []interface{}:
		out := make([]EphemeralMessage, len(m))
		for i, item := range m {
			if em, ok := item.(EphemeralMessage); ok {
				out[i] = em
				continue
			}
			out[i] = EphemeralMessage{Payload: item}
		}
		return out, nil
	case []map[string]interface{}:
		out := make([]EphemeralMessage, len(m))
		for i, item := range m {
			out[i] = EphemeralMessage{Payload: item}
		}
		return out, nil
	default:
		return []EphemeralMessage{{Payload: messages}}, nil
	}
}

// toEphemeralAcks accepts a popped message, a bare id, or a hand-built ack, one
// or many. A per-entry status or error wins over the call-wide default.
func toEphemeralAcks(acks interface{}, opt EphemeralAckOptions) ([]EphemeralAck, error) {
	var out []EphemeralAck

	add := func(a EphemeralAck, index int) error {
		if a.ID == "" {
			return fmt.Errorf("queen: ack at index %d carries no message id -- pass the popped message, or its ID", index)
		}
		if a.Status == "" {
			a.Status = opt.Status
		}
		if a.Error == "" {
			a.Error = opt.Error
		}
		out = append(out, a)
		return nil
	}

	switch a := acks.(type) {
	case nil:
		return nil, errors.New("queen: an ack needs a message id")
	case EphemeralAck:
		return out, add(a, 0)
	case []EphemeralAck:
		for i, item := range a {
			if err := add(item, i); err != nil {
				return nil, err
			}
		}
	case EphemeralPopped:
		return out, add(EphemeralAck{ID: a.ID}, 0)
	case []EphemeralPopped:
		for i, item := range a {
			if err := add(EphemeralAck{ID: item.ID}, i); err != nil {
				return nil, err
			}
		}
	case EphemeralBatch:
		for i, item := range a.Messages {
			if err := add(EphemeralAck{ID: item.ID}, i); err != nil {
				return nil, err
			}
		}
	case string:
		return out, add(EphemeralAck{ID: a}, 0)
	case []string:
		for i, id := range a {
			if err := add(EphemeralAck{ID: id}, i); err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("queen: cannot acknowledge a %T -- pass EphemeralPopped, EphemeralAck, an id string, or a slice of any of them", acks)
	}

	return out, nil
}
