package queen

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// The timer surface (PLAN_KV_TIMERS.md §4, §8.1): a message you schedule now and
// the broker pushes later, into a real queue, with the ordinary delivery
// guarantees from that moment on.
//
// FOUR THINGS THIS API CANNOT HIDE FROM YOU, and every one of them has cost
// somebody a production incident somewhere:
//
//  1. deliverAt is "NO EARLIER THAN", never "exactly at". The floor on this
//     stack is a single hop (p50 ~10 ms, fsync ~4 ms) plus one sweeper cycle.
//
//  2. `absent` on a cancel MAY MEAN ALREADY DELIVERED (§4.4). The row is
//     DELETEd on fire, so there is no tombstone: absent says "no longer
//     pending" and nothing more. The authority is the log -- look for the
//     timer's txn in the destination queue, which is why the cancel answer
//     carries that txn back. Any saga that cancels a compensation timer must
//     have the compensation consumer check the saga's KV state before acting.
//
//  3. `too_late` is a verdict, not a failure (§4.3). The broker holding the
//     claim has already packed that payload and is about to commit it; granting
//     the cancel would leave "did it go out?" with no answer. Bounded by the
//     lease (30 s by default). The remedy is a new key, or waiting for delivery
//     and acting on the message.
//
//  4. There is NO dedup net on the fire (§20.2, §20.7). Delete-plus-push in one
//     transaction is what makes a single fire exactly-once; a fixed txn is the
//     message's IDENTITY, not a net. Rescheduling or republishing a timer that
//     has already fired produces a SECOND message in the log, and no layer stops
//     it.

// Statuses a timer operation can answer. Closed taxonomy (§4.1): a client that
// has to tell them apart writes a switch, it does not read a sentence.
const (
	TimerStatusScheduled   = "scheduled"
	TimerStatusRescheduled = "rescheduled"
	TimerStatusCancelled   = "cancelled"
	TimerStatusAbsent      = "absent"
	TimerStatusTooLate     = "too_late"
)

// ---------------------------------------------------------------------------
// Requests.
// ---------------------------------------------------------------------------

// TimerSchedule describes one timer.
//
// Identity is (queue, timerKey) inside your tenant: scheduling the same key
// twice is an upsert, so a retry after a crash is safe by construction, and a
// rescheduled timer is a NEW timer under an old name (attempts and last error go
// back to zero).
type TimerSchedule struct {
	// Queue is the destination queue the message will be pushed into. Required.
	Queue string
	// TimerKey is the identity of the timer inside that queue. Required.
	TimerKey string
	// Partition is the destination partition ("Default" when empty).
	Partition string

	// Delay is how long from now the message becomes deliverable. A delay in the
	// PAST is legal and fires on the first cycle.
	//
	// Milliseconds on the wire, and that is a rule rather than an accident
	// (§20.6): the durations that can be sub-second are in milliseconds, the
	// ones that cannot are in seconds. A 250 ms retry backoff is one of the best
	// uses of a timer; a sub-second TTL is nobody's use case.
	Delay time.Duration

	// DeliverAt is client-side sugar for Delay: it is converted to a relative
	// delay when the request is sent, because an absolute instant is NOT
	// expressible on this wire -- one clock, Postgres's, so no broker skew can
	// enter (§4.2). Supplying both is an error.
	DeliverAt time.Time

	// Payload is the message body. []byte and json.RawMessage travel verbatim;
	// anything else is JSON-encoded first. It reaches the wire base64-encoded.
	Payload interface{}

	// TransactionID is the txn of the message that will be delivered, minted
	// when empty. It is what makes "was it sent?" answerable: look for it in the
	// destination queue. Every reschedule OVERWRITES it -- a rescheduled timer
	// is a new message (§20.2).
	TransactionID string
}

// TimerCancelOptions are the optional halves of a cancel.
type TimerCancelOptions struct {
	// ExpectedTransactionID is echoed back on `absent`, so the "has it already
	// been delivered?" check needs no second API. It is your own identifier and
	// nothing is revealed by sending it.
	ExpectedTransactionID string
}

// TimerListOptions are the optional halves of a list.
type TimerListOptions struct {
	// After is an EXCLUSIVE keyset cursor over timerKey, not an offset.
	After string
	// Limit defaults to 100 server-side and is CLAMPED, never rejected.
	Limit int
}

// TimerOp is one element of a POST /api/v1/timers batch or of a transaction's
// `timers` rider array. Build it with ScheduleTimerOp, RescheduleTimerOp or
// CancelTimerOp.
//
// The fields the SERVER owns are absent by construction and must stay absent:
// producerSub, messageId, tenant and deliverAt are all rejected rather than
// ignored. Ignoring producerSub would make a timer the one way in this product
// to forge the provenance of a frame (§4.2).
type TimerOp struct {
	Op        string `json:"op"`
	Queue     string `json:"queue"`
	TimerKey  string `json:"timerKey"`
	Partition string `json:"partition,omitempty"`
	DelayMs   *int64 `json:"delayMs,omitempty"`
	Txn       string `json:"txn,omitempty"`
	Payload   string `json:"payload,omitempty"`

	err error
}

// encodeTimerPayload turns a caller's payload into the base64 the wire carries.
func encodeTimerPayload(v interface{}) (string, error) {
	var raw []byte
	switch p := v.(type) {
	case nil:
		raw = []byte("null")
	case []byte:
		raw = p
	case json.RawMessage:
		raw = p
	case string:
		// A string payload is JSON-encoded, not taken as raw bytes: a consumer
		// reads messages as JSON, and passing "hello" through verbatim would
		// deliver a frame that is not valid JSON.
		b, err := json.Marshal(p)
		if err != nil {
			return "", err
		}
		raw = b
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("queen: timer payload cannot be encoded as JSON: %w", err)
		}
		raw = b
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

// scheduleOp builds a schedule or reschedule op out of a TimerSchedule.
func scheduleOp(kind string, s TimerSchedule) TimerOp {
	op := TimerOp{Op: kind, Queue: s.Queue, TimerKey: s.TimerKey, Partition: s.Partition}
	if s.Queue == "" {
		op.err = errors.New("queen: a timer needs a queue")
		return op
	}
	if s.TimerKey == "" {
		op.err = errors.New("queen: a timer needs a timerKey")
		return op
	}
	if s.Delay != 0 && !s.DeliverAt.IsZero() {
		op.err = errors.New("queen: set Delay or DeliverAt, not both")
		return op
	}
	delay := s.Delay
	if !s.DeliverAt.IsZero() {
		delay = time.Until(s.DeliverAt)
	}
	op.DelayMs = Int64(int64(delay / time.Millisecond))

	payload, err := encodeTimerPayload(s.Payload)
	if err != nil {
		op.err = err
		return op
	}
	op.Payload = payload

	op.Txn = s.TransactionID
	if op.Txn == "" {
		op.Txn = GenerateUUID()
	}
	return op
}

// ScheduleTimerOp builds a schedule operation.
func ScheduleTimerOp(s TimerSchedule) TimerOp { return scheduleOp("schedule", s) }

// RescheduleTimerOp builds a reschedule operation. It is the SAME upsert as a
// schedule -- the two names exist because they mean different things to the
// reader, not to the server.
func RescheduleTimerOp(s TimerSchedule) TimerOp { return scheduleOp("reschedule", s) }

// CancelTimerOp builds a cancel operation for a transaction's rider array (the
// saga close bundle: ack the message and cancel its compensation timer in one
// commit).
//
// For a STANDALONE cancel use Timers.Cancel, which goes to
// DELETE /api/v1/timers/:queue/*timerKey -- the one route that is guaranteed
// never to be blocked, whatever the cluster's quota state (§9.6). A cancel sent
// through POST /api/v1/timers inherits that route's class instead, and a mixed
// batch is refused WHOLE on a blocked cluster.
func CancelTimerOp(queue, timerKey string) TimerOp {
	op := TimerOp{Op: "cancel", Queue: queue, TimerKey: timerKey}
	if queue == "" || timerKey == "" {
		op.err = errors.New("queen: a cancel needs a queue and a timerKey")
	}
	return op
}

func validateTimerOps(ops []TimerOp) error {
	for i, op := range ops {
		if op.err != nil {
			return fmt.Errorf("queen: timer operation at index %d: %w", i, op.err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Results.
// ---------------------------------------------------------------------------

// TimerResult is the answer to a schedule, reschedule or cancel.
//
// OK is false on `absent` and on `too_late`, and neither is a Go error: they are
// verdicts with HTTP 200. `absent` carrying OK:false is deliberate -- the
// in-house lesson is queue delete, where deleted:false with a 200 read as
// success to every client that trusted the field.
type TimerResult struct {
	OK       bool   `json:"ok"`
	Status   string `json:"status"`
	Queue    string `json:"queue"`
	TimerKey string `json:"timerKey"`
	// TransactionID is the txn of the message this timer will deliver (or was
	// going to deliver). On `absent` it is the txn to look for in the
	// destination queue.
	TransactionID string `json:"txn"`
	// MessageID is promised at schedule time so a delivered frame can be
	// correlated without a second API.
	MessageID string    `json:"messageId"`
	DeliverAt time.Time `json:"deliverAt"`

	// OpIndex and Type are filled when the result comes back inside a
	// transaction's flat result array.
	OpIndex int    `json:"opIndex"`
	Type    string `json:"type"`
}

// TimerInfo is one pending timer, as returned by Peek (with the payload) or List
// (without it).
type TimerInfo struct {
	Found         bool
	Queue         string
	TimerKey      string
	Partition     string
	DeliverAt     time.Time
	TransactionID string
	MessageID     string
	// Payload is the stored payload, base64-decoded, and EXACTLY as it is
	// stored: peek is an inspection surface and does not quietly decrypt or
	// decompress what the fire will deliver -- Encrypted and PayloadZstd tell
	// you the truth about the bytes. Nil on List, which never carries payloads.
	Payload     []byte
	PayloadZstd bool
	Encrypted   bool
	ProducerSub string
	// Attempts counts only PERMANENT and configuration failures: a transient one
	// (a serialization failure, a connection loss) backs off without consuming
	// the budget. At QUEEN_SWEEPER_MAX_ATTEMPTS the timer goes to the
	// destination queue's DLQ.
	Attempts  int
	LastError string
	// Claimed means a broker holds this row right now. A timer in BACKOFF reads
	// claimed:false, deliberately: it is still cancellable, and that is the
	// whole reason the claim token is cleared on failure.
	Claimed   bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// TimerPage is one page of a list.
type TimerPage struct {
	Rows      []TimerInfo
	Truncated bool
	NextAfter string
}

// timerInfoWire is the shape on the wire. It is separate from TimerInfo for one
// reason: `payload` arrives base64 and TimerInfo hands back bytes, and a public
// struct with a staging field would be a field that is always empty by the time
// anybody reads it.
type timerInfoWire struct {
	Found         bool      `json:"found"`
	Queue         string    `json:"queue"`
	TimerKey      string    `json:"timerKey"`
	Partition     string    `json:"partition"`
	DeliverAt     time.Time `json:"deliverAt"`
	TransactionID string    `json:"txn"`
	MessageID     string    `json:"messageId"`
	Payload       string    `json:"payload"`
	PayloadZstd   bool      `json:"payloadZstd"`
	Encrypted     bool      `json:"encrypted"`
	ProducerSub   string    `json:"producerSub"`
	Attempts      int       `json:"attempts"`
	LastError     string    `json:"lastError"`
	Claimed       bool      `json:"claimed"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

// timerPageWire is the list envelope. `rows` never carries a `found` field, so a
// row is always a real timer.
type timerPageWire struct {
	Rows      []timerInfoWire `json:"rows"`
	Truncated bool            `json:"truncated"`
	NextAfter string          `json:"nextAfter"`
}

func (w timerInfoWire) info(found bool) (TimerInfo, error) {
	info := TimerInfo{
		Found:         found,
		Queue:         w.Queue,
		TimerKey:      w.TimerKey,
		Partition:     w.Partition,
		DeliverAt:     w.DeliverAt,
		TransactionID: w.TransactionID,
		MessageID:     w.MessageID,
		PayloadZstd:   w.PayloadZstd,
		Encrypted:     w.Encrypted,
		ProducerSub:   w.ProducerSub,
		Attempts:      w.Attempts,
		LastError:     w.LastError,
		Claimed:       w.Claimed,
		CreatedAt:     w.CreatedAt,
		UpdatedAt:     w.UpdatedAt,
	}
	if w.Payload != "" {
		raw, err := base64.StdEncoding.DecodeString(w.Payload)
		if err != nil {
			return info, fmt.Errorf("queen: timer payload is not valid base64: %w", err)
		}
		info.Payload = raw
	}
	return info, nil
}

// ---------------------------------------------------------------------------
// The client surface.
// ---------------------------------------------------------------------------

// Timers is the timer API. Get it from Queen.Timers.
type Timers struct {
	httpClient *HttpClient
}

// Timers returns the timer API.
func (q *Queen) Timers() *Timers {
	return &Timers{httpClient: q.httpClient}
}

// timerPath builds a path route. See kvPath for why the segments are escaped and
// why a `/` inside a timerKey survives it.
func timerPath(queue, timerKey string) string {
	return "/api/v1/timers/" + url.PathEscape(queue) + "/" + strings.ReplaceAll(url.PathEscape(timerKey), "%2F", "/")
}

// timerBatchRequest is the batch envelope, the same shape as the KV one.
type timerBatchRequest struct {
	Operations []TimerOp `json:"operations"`
}

func (t *Timers) apply(ctx context.Context, ops []TimerOp) ([]TimerResult, error) {
	if err := validateTimerOps(ops); err != nil {
		return nil, err
	}
	if len(ops) == 0 {
		return nil, errors.New("queen: a timer batch needs at least one operation")
	}
	body, err := t.httpClient.PostRaw(ctx, "/api/v1/timers", timerBatchRequest{Operations: ops})
	if err != nil {
		return nil, surfaceError(err)
	}
	var out struct {
		Results []TimerResult `json:"results"`
	}
	if err := decodeKV(body, &out); err != nil {
		return nil, fmt.Errorf("queen: timers response: %w", err)
	}
	if len(out.Results) != len(ops) {
		return nil, fmt.Errorf("queen: timers returned %d results for %d operations", len(out.Results), len(ops))
	}
	return out.Results, nil
}

// Schedule schedules one timer.
func (t *Timers) Schedule(ctx context.Context, s TimerSchedule) (TimerResult, error) {
	res, err := t.apply(ctx, []TimerOp{ScheduleTimerOp(s)})
	if err != nil {
		return TimerResult{}, err
	}
	return res[0], nil
}

// Reschedule moves an existing timer. Identical to Schedule on the server: the
// same upsert, with attempts and last error reset.
func (t *Timers) Reschedule(ctx context.Context, s TimerSchedule) (TimerResult, error) {
	res, err := t.apply(ctx, []TimerOp{RescheduleTimerOp(s)})
	if err != nil {
		return TimerResult{}, err
	}
	return res[0], nil
}

// ScheduleBatch schedules several timers in one transaction. A (queue,
// timerKey) may appear at most once per call.
func (t *Timers) ScheduleBatch(ctx context.Context, schedules ...TimerSchedule) ([]TimerResult, error) {
	ops := make([]TimerOp, len(schedules))
	for i, s := range schedules {
		ops[i] = ScheduleTimerOp(s)
	}
	return t.apply(ctx, ops)
}

// Apply runs a mixed batch of timer operations in one transaction.
//
// Note the asymmetry this buys, and it is deliberate: on a cluster blocked for
// quota, a batch containing a schedule is refused WHOLE, cancels included. When
// what you need is the cancel, use Cancel (§9.6).
func (t *Timers) Apply(ctx context.Context, ops ...TimerOp) ([]TimerResult, error) {
	return t.apply(ctx, ops)
}

// Cancel removes a pending timer, through the route that is never blocked.
//
// Read the `absent` contract at the top of this file before trusting a false
// return: it does NOT mean the timer never fired.
func (t *Timers) Cancel(ctx context.Context, queue, timerKey string, opts ...TimerCancelOptions) (TimerResult, error) {
	if queue == "" || timerKey == "" {
		return TimerResult{}, errors.New("queen: a cancel needs a queue and a timerKey")
	}
	if len(opts) > 1 {
		return TimerResult{}, errors.New("queen: pass at most one TimerCancelOptions")
	}
	path := timerPath(queue, timerKey)
	if len(opts) == 1 && opts[0].ExpectedTransactionID != "" {
		path += "?txn=" + url.QueryEscape(opts[0].ExpectedTransactionID)
	}
	body, err := t.httpClient.DeleteRaw(ctx, path, nil)
	if err != nil {
		return TimerResult{}, surfaceError(err)
	}
	var r TimerResult
	if err := decodeKV(body, &r); err != nil {
		return TimerResult{}, fmt.Errorf("queen: timer cancel response: %w", err)
	}
	return r, nil
}

// Peek reads one pending timer, payload included. A miss is Found:false with no
// error.
func (t *Timers) Peek(ctx context.Context, queue, timerKey string) (TimerInfo, error) {
	body, err := t.httpClient.GetRaw(ctx, timerPath(queue, timerKey))
	if err != nil {
		return TimerInfo{}, surfaceError(err)
	}
	var w timerInfoWire
	if err := decodeKV(body, &w); err != nil {
		return TimerInfo{}, fmt.Errorf("queen: timer peek response: %w", err)
	}
	return w.info(w.Found)
}

// List pages through the pending timers of ONE queue.
//
// The queue is mandatory and is a path segment rather than a filter: a
// tenant-wide list would be a scan that an end user of your own product could
// trigger (§4.1).
func (t *Timers) List(ctx context.Context, queue string, opts ...TimerListOptions) (TimerPage, error) {
	if queue == "" {
		return TimerPage{}, errors.New("queen: a timer list needs a queue")
	}
	if len(opts) > 1 {
		return TimerPage{}, errors.New("queen: pass at most one TimerListOptions")
	}
	path := "/api/v1/timers/" + url.PathEscape(queue)
	if len(opts) == 1 {
		q := url.Values{}
		if opts[0].After != "" {
			q.Set("after", opts[0].After)
		}
		if opts[0].Limit > 0 {
			q.Set("limit", strconv.Itoa(opts[0].Limit))
		}
		if len(q) > 0 {
			path += "?" + q.Encode()
		}
	}
	body, err := t.httpClient.GetRaw(ctx, path)
	if err != nil {
		return TimerPage{}, surfaceError(err)
	}
	var w timerPageWire
	if err := decodeKV(body, &w); err != nil {
		return TimerPage{}, fmt.Errorf("queen: timer list response: %w", err)
	}
	page := TimerPage{Truncated: w.Truncated, NextAfter: w.NextAfter}
	for _, row := range w.Rows {
		info, err := row.info(true)
		if err != nil {
			return TimerPage{}, err
		}
		page.Rows = append(page.Rows, info)
	}
	return page, nil
}
