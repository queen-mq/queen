package queen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// The key/value surface (PLAN_KV_TIMERS.md §5, §8.1).
//
// Seven names, five code paths: get, getMany, getPrefix, put, putIfAbsent
// (an alias for put with expect:0), delete, incr.
//
// THREE RULES THAT DECIDE HOW THIS FILE READS, all of them the server's and none
// of them negotiable here:
//
//  1. THE STATUS CODE DESCRIBES THE CALL, NEVER THE PREDICATE (§8.1). A missing
//     key, a lost putIfAbsent race, a delete that hit nothing and an incr over
//     its ceiling are all HTTP 200 with an explicit field in the body. So none of
//     them is a Go error: `applied:false` is the single most frequent outcome of
//     this product, and returning it as an error would put it inside every
//     caller's retry logic. Check `Applied`, not `err`.
//
//  2. EXPIRY IS MANDATORY ON EVERY WRITE (§5.1). Exactly one of ttlSeconds and
//     forever, never zero and never both. The zero value of Expiry is therefore
//     NOT a declaration and is refused before the request leaves: a put that
//     silently inherited the previous TTL is the fastest way to make a marker
//     immortal.
//
//  3. AN EXPIRED KEY IS NEVER RETURNED AND NEVER COUNTS AS EXISTING (§5.7), even
//     while the sweeper has not pruned it yet. The truth is the predicate, not
//     the presence of the row.
//
// And one rule that is this file's own: nothing here decodes through
// map[string]interface{}. See parseBody in http_client.go for why.

// ---------------------------------------------------------------------------
// Expiry — the mandatory half of every write.
// ---------------------------------------------------------------------------

// Expiry is the lifetime of a KV key. Exactly one of TTLSeconds/TTL/Until and
// Forever must be supplied on every put, putIfAbsent and incr; the zero value is
// invalid on purpose, so "I forgot to say" cannot be spelled.
//
// The wire carries ttlSeconds, an integer number of SECONDS (§20.1, ratified).
// The declared product rule is: durations that can be sub-second are in
// milliseconds, the ones that cannot are in seconds. A KV TTL cannot.
type Expiry struct {
	seconds int64
	forever bool
	set     bool
	err     error
}

// TTLSeconds sets the lifetime in whole seconds. Must be greater than zero.
func TTLSeconds(seconds int64) Expiry {
	if seconds <= 0 {
		return Expiry{err: fmt.Errorf("queen: ttlSeconds must be an integer greater than zero, got %d", seconds)}
	}
	return Expiry{seconds: seconds, set: true}
}

// TTL sets the lifetime from a Duration, rounded UP to the next whole second.
// Rounding up and not down: a TTL rounded down can expire a marker just before
// the end of the window it existed to cover.
func TTL(d time.Duration) Expiry {
	if d <= 0 {
		return Expiry{err: fmt.Errorf("queen: TTL must be greater than zero, got %s", d)}
	}
	secs := int64(d / time.Second)
	if d%time.Second != 0 {
		secs++
	}
	return TTLSeconds(secs)
}

// Until sets the lifetime from an absolute instant, converted to a relative
// delta AT SEND TIME and rounded up. This is client-side sugar: there is no
// expiresAt field on the wire, because a single clock (Postgres's) is what keeps
// broker skew out of the expiry rule (§5.1).
func Until(t time.Time) Expiry {
	return TTL(time.Until(t))
}

// Forever stores the key with no expiry.
//
// Never use it in a test or in an example that runs in CI: a run that fails
// leaves immortal state behind in a shared database (§10.4).
func Forever() Expiry {
	return Expiry{forever: true, set: true}
}

// ttl is the seconds this Expiry puts on the wire (0 when it is Forever).
func (e Expiry) ttl() int64 { return e.seconds }

func (e Expiry) validate() error {
	if e.err != nil {
		return e.err
	}
	if !e.set {
		return errors.New("queen: an expiry is required on every KV write: pass TTL/TTLSeconds/Until or Forever (exactly one of ttlSeconds and forever, PLAN_KV_TIMERS §5.1)")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Options.
// ---------------------------------------------------------------------------

// KVWriteOptions are the optional halves of put, putIfAbsent and delete.
type KVWriteOptions struct {
	// Expect is an optimistic lock. nil is an unconditional upsert; 0 means
	// "must not exist" (and wins against an expired row that has not been pruned
	// yet); N > 0 means "must still be at version N" and NEVER creates the key.
	//
	// Build it with Expect(n). The version handed back to a LOSER is advisory:
	// it is read in the same statement as the failed write, but it is not a
	// fencing token to reuse blindly (§5.3).
	Expect *int64

	// Required escalates a lost precondition from a verdict into a failed
	// transaction (§6.1 point 5). It is meaningful mostly inside
	// Transaction().KV(...): there, `required:true` is what makes the bundle
	// roll back when the gate is lost, and Commit reports it as
	// ReasonKVPrecondition instead of an error.
	Required bool
}

// KVIncrOptions are the optional halves of incr. There is deliberately no
// Expect: incr is the way OUT of compare-and-swap, and a precondition would
// reintroduce the very loop it exists to remove (§5.4).
type KVIncrOptions struct {
	// Min and Max are refused, not saturated: if applying the delta would cross
	// them, nothing is written and the call answers applied:false with
	// reason "limit" and the CURRENT value. With Max, `Applied` IS the admission
	// decision of a rate limiter -- the request that would breach the ceiling
	// has not consumed any budget.
	Min *int64
	Max *int64

	// Required behaves as in KVWriteOptions.
	Required bool
}

// KVPrefixOptions are the optional halves of getPrefix.
type KVPrefixOptions struct {
	// After is an EXCLUSIVE keyset cursor, not an offset. Every page is its own
	// snapshot: with After it may miss a key inserted behind the cursor. Fine
	// for compacting state, not for an exact count (§5.5).
	After string
	// Limit defaults to 100 server-side and is CLAMPED, never rejected.
	Limit int
	// KeysOnly returns keys and versions without values.
	KeysOnly bool
}

// Int64 returns a pointer to v, for the optional numeric fields above.
func Int64(v int64) *int64 { return &v }

// Expect returns a pointer to a version for KVWriteOptions.Expect. Expect(0)
// means "the key must not exist".
//
// The rule worth putting in your own code review: if you believe the partition
// lane already serialises you, pass Expect anyway. If it never fails it cost
// nothing, and the day it does fail you have just discovered that two consumers
// are serving the same partition -- with a verdict instead of a wrong total.
func Expect(version int64) *int64 { return &version }

// Reasons a write did not apply. Closed taxonomy (§5.3).
const (
	KVReasonExists  = "exists"  // expect:0 lost: the key is there
	KVReasonAbsent  = "absent"  // expect:N>0 on a key that is not there
	KVReasonVersion = "version" // expect:N>0 lost: somebody else wrote first
	KVReasonLimit   = "limit"   // incr would cross min/max, or delta is out of range
	KVReasonType    = "type"    // incr on a live key whose value is not a number
)

// ---------------------------------------------------------------------------
// Operations. These are the wire objects, and their JSON tags ARE the contract.
// ---------------------------------------------------------------------------

// KVOp is one operation of a KV batch, or one element of a transaction's `kv`
// rider array. Build them with the KV*Op constructors below.
//
// A construction error (a missing expiry, a value that will not marshal) is
// carried on the op and surfaces when the batch or the transaction is sent,
// naming the index: a constructor that returned an error would make the
// variadic call sites unreadable, and one that panicked would be worse.
type KVOp struct {
	Op       string          `json:"op"`
	Ns       string          `json:"ns"`
	Key      string          `json:"key,omitempty"`
	Keys     []string        `json:"keys,omitempty"`
	Prefix   string          `json:"prefix,omitempty"`
	After    string          `json:"after,omitempty"`
	Limit    int             `json:"limit,omitempty"`
	KeysOnly bool            `json:"keysOnly,omitempty"`
	Value    json.RawMessage `json:"value,omitempty"`
	Delta    *int64          `json:"delta,omitempty"`
	Min      *int64          `json:"min,omitempty"`
	Max      *int64          `json:"max,omitempty"`

	TTLSeconds int64  `json:"ttlSeconds,omitempty"`
	Forever    bool   `json:"forever,omitempty"`
	Expect     *int64 `json:"expect,omitempty"`
	Required   bool   `json:"required,omitempty"`

	err error
}

func (o KVOp) withExpiry(e Expiry) KVOp {
	if err := e.validate(); err != nil {
		o.err = err
		return o
	}
	if e.forever {
		o.Forever = true
	} else {
		o.TTLSeconds = e.ttl()
	}
	return o
}

func (o KVOp) withWriteOptions(opts []KVWriteOptions) KVOp {
	if len(opts) > 1 {
		o.err = errors.New("queen: pass at most one KVWriteOptions")
		return o
	}
	if len(opts) == 1 {
		o.Expect = opts[0].Expect
		o.Required = opts[0].Required
	}
	return o
}

// marshalValue keeps a caller's JSON byte-identical and turns everything else
// into JSON exactly once.
//
// `"value": null` is a LEGAL value and an ABSENT value is not, so nil is
// marshalled to null rather than dropped -- with `omitempty` on the field, a nil
// RawMessage would vanish and the server would answer a 400 the call site cannot
// explain.
func marshalValue(v interface{}) (json.RawMessage, error) {
	if raw, ok := v.(json.RawMessage); ok {
		return raw, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("queen: kv value cannot be encoded as JSON: %w", err)
	}
	return json.RawMessage(b), nil
}

// KVGetOp reads one key.
func KVGetOp(ns, key string) KVOp {
	return KVOp{Op: "get", Ns: ns, Key: key}
}

// KVGetManyOp reads several keys of one namespace. The answer carries `missing`
// explicitly: absence is a datum, never a hole computed by difference (§5.5).
func KVGetManyOp(ns string, keys []string) KVOp {
	return KVOp{Op: "getMany", Ns: ns, Keys: keys}
}

// KVGetPrefixOp reads a page of a prefix.
//
// It lives ONLY on POST /api/v1/kv: it is forbidden inside a transaction (read
// work whose cost the caller does not bound, holding the outermost lock space)
// and it is forbidden as a query string, because `?prefix=quota:acme:` is
// recorded by the broker's access log, the proxy's, the meter sample, the
// per-request-id tracing and any ingress in front (§5.5).
func KVGetPrefixOp(ns, prefix string, opts ...KVPrefixOptions) KVOp {
	op := KVOp{Op: "getPrefix", Ns: ns, Prefix: prefix}
	if prefix == "" {
		op.err = errors.New("queen: getPrefix needs a non-empty prefix: a namespace is not a table to enumerate")
	}
	if len(opts) > 1 {
		op.err = errors.New("queen: pass at most one KVPrefixOptions")
		return op
	}
	if len(opts) == 1 {
		op.After = opts[0].After
		op.Limit = opts[0].Limit
		op.KeysOnly = opts[0].KeysOnly
	}
	return op
}

// KVPutOp writes a key, replacing value AND expiry.
func KVPutOp(ns, key string, value interface{}, exp Expiry, opts ...KVWriteOptions) KVOp {
	op := KVOp{Op: "put", Ns: ns, Key: key}
	raw, err := marshalValue(value)
	if err != nil {
		op.err = err
		return op
	}
	op.Value = raw
	return op.withExpiry(exp).withWriteOptions(opts)
}

// KVPutIfAbsentOp writes a key only if it is not there.
//
// It travels under its own name and the server desugars it to put with
// expect:0, one code path. It is the name of the thing, and `applied` answers
// the question this API is asked most often: did I win? The loser gets the
// winner's value in the same answer, which is the entire point of an idempotency
// marker (§5.3).
func KVPutIfAbsentOp(ns, key string, value interface{}, exp Expiry, opts ...KVWriteOptions) KVOp {
	op := KVPutOp(ns, key, value, exp, opts...)
	op.Op = "putIfAbsent"
	if op.Expect != nil && *op.Expect != 0 {
		op.err = errors.New("queen: putIfAbsent desugars to put with expect:0; a different expect is a contradiction")
	}
	op.Expect = nil
	return op
}

// KVDeleteOp removes a key. A delete that hit nothing is applied:false, not an
// error, and it never counts against an occupancy quota.
func KVDeleteOp(ns, key string, opts ...KVWriteOptions) KVOp {
	return KVOp{Op: "delete", Ns: ns, Key: key}.withWriteOptions(opts)
}

// KVIncrOp adds delta to a numeric key, creating it when absent.
//
// The TTL is CREATE-ONLY: a live key keeps the expiry it has. If incr extended
// it, a fixed-window limiter on an always-active client would never close its
// window, i.e. it would stop limiting exactly under load. An expired key counts
// as zero and starts a NEW window, which is what makes the limiter a single call
// (§5.4).
func KVIncrOp(ns, key string, delta int64, exp Expiry, opts ...KVIncrOptions) KVOp {
	op := KVOp{Op: "incr", Ns: ns, Key: key, Delta: Int64(delta)}
	if len(opts) > 1 {
		op.err = errors.New("queen: pass at most one KVIncrOptions")
		return op
	}
	if len(opts) == 1 {
		op.Min = opts[0].Min
		op.Max = opts[0].Max
		op.Required = opts[0].Required
	}
	return op.withExpiry(exp)
}

// validateOps returns the first construction error, naming the index.
func validateKVOps(ops []KVOp) error {
	for i, op := range ops {
		if op.err != nil {
			return fmt.Errorf("queen: kv operation at index %d: %w", i, op.err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Results.
// ---------------------------------------------------------------------------

// KVRow is one row of a getMany or getPrefix answer. Multi-key reads return
// ROWS, never a key/value map: the shape of the answer makes the confusion
// between "absent" and "present with a null value" inexpressible (§5.5).
type KVRow struct {
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Version   int64           `json:"version"`
	ExpiresAt time.Time       `json:"expiresAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

// KVEntry is the answer to a single-key read. Found is separate from Value
// because `null` is a legal value: {Found:true, Value:"null"} and {Found:false}
// are different things.
type KVEntry struct {
	Found     bool            `json:"found"`
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Version   int64           `json:"version"`
	ExpiresAt time.Time       `json:"expiresAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

// KVMany is the answer to a getMany.
type KVMany struct {
	Rows    []KVRow
	Missing []string
	// Truncated is set when the server's byte budget cut the answer short. Keys
	// cut that way are in neither Rows nor Missing -- calling them absent would
	// be a lie.
	Truncated bool
}

// KVPage is one page of a getPrefix.
type KVPage struct {
	Rows      []KVRow
	Truncated bool
	NextAfter string
}

// KVWrite is the answer to a put, putIfAbsent or delete. Value and Version are
// the CURRENT ones even when the write did not apply, so the loser of a race
// needs no second round trip.
//
// VERSION IS AN OPAQUE TOKEN, NOT A COUNTER, AND NOT ORDERED. It comes from a
// global sequence rather than a per-key counter, so that a key which expired,
// was pruned and was recreated cannot re-issue a version an old holder is still
// carrying. That sequence is cached per backend connection, so two writes of the
// same key through different pooled connections can come back as 2022 and then
// 21: what is guaranteed is that a version is never REUSED, never that it grows.
//
// Test it with == and !=, pass it back as Expect, and never with < or >.
type KVWrite struct {
	Applied bool
	Reason  string
	Key     string
	Value   json.RawMessage
	Version int64
}

// KVCounter is the answer to an incr.
type KVCounter struct {
	Applied bool
	Reason  string
	Key     string
	Value   int64
	Version int64
}

// KVResult is one element of a batch answer, index-aligned to the operation
// that produced it (§6.4). It is a union of the shapes above because the wire is
// one; use Write, Entry, Many, Page and Counter to project it.
type KVResult struct {
	Index   int             `json:"index"`
	Op      string          `json:"op"`
	Applied *bool           `json:"applied"`
	Found   *bool           `json:"found"`
	Reason  string          `json:"reason"`
	Key     string          `json:"key"`
	Value   json.RawMessage `json:"value"`
	Version int64           `json:"version"`

	ExpiresAt time.Time `json:"expiresAt"`
	UpdatedAt time.Time `json:"updatedAt"`

	Rows      []KVRow  `json:"rows"`
	Missing   []string `json:"missing"`
	Truncated bool     `json:"truncated"`
	NextAfter string   `json:"nextAfter"`

	// OpIndex is the position inside the `kv` rider array, kept when a result
	// comes back from a transaction whose flat result space also holds pushes
	// and acks. Index is the flat one.
	OpIndex int    `json:"opIndex"`
	Type    string `json:"type"`
}

// Write projects a put/putIfAbsent/delete result.
func (r KVResult) Write() KVWrite {
	return KVWrite{
		Applied: r.Applied != nil && *r.Applied,
		Reason:  r.Reason,
		Key:     r.Key,
		Value:   r.Value,
		Version: r.Version,
	}
}

// Entry projects a get result.
func (r KVResult) Entry() KVEntry {
	return KVEntry{
		Found:     r.Found != nil && *r.Found,
		Key:       r.Key,
		Value:     r.Value,
		Version:   r.Version,
		ExpiresAt: r.ExpiresAt,
		UpdatedAt: r.UpdatedAt,
	}
}

// Many projects a getMany result.
func (r KVResult) Many() KVMany {
	return KVMany{Rows: r.Rows, Missing: r.Missing, Truncated: r.Truncated}
}

// Page projects a getPrefix result.
func (r KVResult) Page() KVPage {
	return KVPage{Rows: r.Rows, Truncated: r.Truncated, NextAfter: r.NextAfter}
}

// Counter projects an incr result.
//
// The server value is `numeric` and cannot overflow there; this client exposes
// int64 and FAILS EXPLICITLY when the number does not fit, rather than handing
// back a wrong one (§5.4).
func (r KVResult) Counter() (KVCounter, error) {
	c := KVCounter{
		Applied: r.Applied != nil && *r.Applied,
		Reason:  r.Reason,
		Key:     r.Key,
		Version: r.Version,
	}
	if len(r.Value) == 0 || string(r.Value) == "null" {
		return c, nil
	}
	var n json.Number
	if err := decodeKV(r.Value, &n); err != nil {
		return c, fmt.Errorf("queen: incr returned a non-numeric value %s: %w", string(r.Value), err)
	}
	v, err := n.Int64()
	if err != nil {
		return c, fmt.Errorf("queen: incr counter %s does not fit in an int64: %w", n.String(), err)
	}
	c.Value = v
	return c, nil
}

// ---------------------------------------------------------------------------
// Decoding, and the error envelope shared with the timer surface.
// ---------------------------------------------------------------------------

// decodeKV is the only decoder the kv and timer surfaces use. UseNumber matters
// here and is forbidden in parseBody: see the note there.
func decodeKV(body []byte, v interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	return dec.Decode(v)
}

// SurfaceError is the error envelope of the kv and timer surfaces:
//
//	{"error": "<code>", "reason": "<stable id>", "detail": "<human half>"}
//
// Note that the code is in `error` and not in `code`: this envelope predates the
// proxy's and is not the same one (HTTPError.Code stays empty for these).
// Branch on Code or Reason, never on Detail -- string matching on a message is
// forbidden everywhere in this codebase (§13.5).
//
// THIS TYPE WAS CALLED FeatureError, and it carried an IsDisabled() with a
// package-level IsFeatureDisabled(err) beside it. Both are gone (Alice,
// 2026-08-18). They existed for one answer only -- a 404 meaning "this cell was
// booted without the surface" -- and the broker cannot give that answer any
// more: QUEEN_KV_ENABLED and QUEEN_TIMERS_ENABLED were removed, and kv and
// timers are part of the engine now, the way push and pop are. Nobody
// feature-detects push. So there is no feature left to detect, and no feature
// left in the name: what remains is an envelope, which is all this type ever
// was.
//
// A 404 from these routes is now what a 404 is anywhere else -- a wrong URL, or
// a broker too old to carry the surface at all -- and not a deployment choice to
// probe for before the first real call.
type SurfaceError struct {
	StatusCode int
	Code       string
	Reason     string
	Detail     string
	// HTTP is the transport-level error this was built from, kept so the status
	// code and the raw body stay reachable.
	HTTP *HTTPError
}

func (e *SurfaceError) Error() string {
	parts := []string{fmt.Sprintf("HTTP %d", e.StatusCode)}
	if e.Code != "" {
		parts = append(parts, e.Code)
	}
	if e.Reason != "" && e.Reason != e.Code {
		parts = append(parts, e.Reason)
	}
	if e.Detail != "" {
		parts = append(parts, e.Detail)
	}
	return "queen: " + strings.Join(parts, ": ")
}

func (e *SurfaceError) Unwrap() error { return e.HTTP }

// THE REFUSAL THAT REMAINS
//
// Dropping the boot gate did not drop the operator's RUNTIME KILL SWITCH
// (`kv_enabled`, `timers_schedule_enabled`, `timers_fire_enabled` in
// queen.system_state: read on every call, flipped live during an incident,
// expected to be flipped back). A surface that exists on every cell can still be
// paused on one of them, so this stays worth handling -- as a transient refusal
// like any other, not as a configuration to check before use:
//
//	route (/api/v1/kv, /api/v1/timers)  503, Code and Reason both `kv_disabled`
//	                                    or `timers_disabled`, Retry-After: 1.
//	                                    Temporary: back off, come back.
//	transaction rider (kv/timers)       403, and the reason is on the
//	                                    TransactionResponse, not here (that body
//	                                    puts the code in `reason` and prose in
//	                                    `error`, the other way round from this
//	                                    envelope). PERMANENT ON PURPOSE: a bundle
//	                                    carries messages, and retrying it in a
//	                                    loop against a cell an operator has
//	                                    deliberately paused is a retry storm on
//	                                    the hottest path of the product.
//
// Two further 503s wear the same shape and mean the cell rather than the switch:
// `kv_pool_exhausted`, and `kv_standalone_paused` (standalone writes shed under
// pressure -- the same operations inside a transaction keep working). All of
// them are read from Code or Reason, never from prose.

// surfaceError upgrades an *HTTPError from these routes into a SurfaceError.
// Anything else (transport, timeout, context) is passed through untouched.
func surfaceError(err error) error {
	if err == nil {
		return nil
	}
	var he *HTTPError
	if !errors.As(err, &he) {
		return err
	}
	se := &SurfaceError{StatusCode: he.StatusCode, HTTP: he}
	var env struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
		Detail string `json:"detail"`
		Code   string `json:"code"`
	}
	if jsonErr := json.Unmarshal([]byte(he.Body), &env); jsonErr == nil {
		se.Code = env.Error
		if se.Code == "" {
			se.Code = env.Code
		}
		se.Reason = env.Reason
		se.Detail = env.Detail
	}
	return se
}

// ---------------------------------------------------------------------------
// The client surface.
// ---------------------------------------------------------------------------

// KV is the key/value API. Get it from Queen.KV.
type KV struct {
	httpClient *HttpClient
}

// KV returns the key/value API.
func (q *Queen) KV() *KV {
	return &KV{httpClient: q.httpClient}
}

// kvPath builds a path route. Both segments are percent-escaped: a key is an
// arbitrary string, and an unescaped `?` in one would start a query string --
// which these routes reject outright, so the symptom would be a 400 nobody can
// explain from the call site. A `/` inside a key survives either way: the route
// is a catch-all so `order/9f1/items` writes naturally, and %2F decodes to the
// same key.
func kvPath(ns, key string) string {
	return "/api/v1/kv/" + url.PathEscape(ns) + "/" + strings.ReplaceAll(url.PathEscape(key), "%2F", "/")
}

// KVPreconditionError is what an operation carrying Required:true answers with
// when it lost its precondition on the standalone KV surface (§6.1 point 5).
//
// The call is rolled back whole -- that is what Required asks for -- so it is an
// error here, and it carries everything the loser needs so that nobody has to
// make a second call or read a message string. Inside a transaction the same
// verdict is RETURNED instead, on TransactionResponse: there the bundle's own
// outcome is the answer (see TransactionBuilder.Commit).
//
// It arrives with HTTP 200 deliberately: the transaction really did abort in
// SQL, but a lost race is the expected outcome of a legitimate redelivery and
// must pollute neither retry policies nor error metrics.
type KVPreconditionError struct {
	// FailedIndex is the position of the operation that lost, in the array that
	// was sent.
	FailedIndex int
	// Reason is one of the KVReason* constants.
	Reason string
	// Version and Value are the CURRENT ones -- i.e. the winner's.
	Version int64
	Value   json.RawMessage
}

func (e *KVPreconditionError) Error() string {
	return fmt.Sprintf("queen: kv precondition lost at index %d (%s), current version %d", e.FailedIndex, e.Reason, e.Version)
}

// kvPrecondition reads the verdict envelope out of a 200 body, or returns nil
// when the body is an ordinary answer.
func kvPrecondition(body []byte) *KVPreconditionError {
	var v struct {
		Reason      string          `json:"reason"`
		FailedIndex *int            `json:"failedIndex"`
		KVReason    string          `json:"kvReason"`
		Version     int64           `json:"version"`
		Value       json.RawMessage `json:"value"`
	}
	if err := decodeKV(body, &v); err != nil || v.Reason != ReasonKVPrecondition {
		return nil
	}
	e := &KVPreconditionError{Reason: v.KVReason, Version: v.Version, Value: v.Value, FailedIndex: -1}
	if v.FailedIndex != nil {
		e.FailedIndex = *v.FailedIndex
	}
	return e
}

// batch sends ops to POST /api/v1/kv, the complete surface.
func (kv *KV) batch(ctx context.Context, ops []KVOp) ([]KVResult, error) {
	if err := validateKVOps(ops); err != nil {
		return nil, err
	}
	if len(ops) == 0 {
		return nil, errors.New("queen: a KV batch needs at least one operation")
	}
	body, err := kv.httpClient.PostRaw(ctx, "/api/v1/kv", kvBatchRequest{Operations: ops})
	if err != nil {
		return nil, surfaceError(err)
	}
	// A lost `required` precondition answers with the verdict envelope and NO
	// results array, at HTTP 200. Read it before the result count, or the
	// clearest failure of this surface would surface as "0 results for 1
	// operation".
	if pe := kvPrecondition(body); pe != nil {
		return nil, pe
	}
	var out struct {
		Results []KVResult `json:"results"`
	}
	if err := decodeKV(body, &out); err != nil {
		return nil, fmt.Errorf("queen: kv batch response: %w", err)
	}
	if len(out.Results) != len(ops) {
		// §6.4: the results are index-aligned to the input, and a short array
		// means the answer cannot be attributed. Loud, never guessed.
		return nil, fmt.Errorf("queen: kv batch returned %d results for %d operations", len(out.Results), len(ops))
	}
	return out.Results, nil
}

// kvBatchRequest is the batch envelope. `{"operations":[...]}` rather than a
// bare array: it is the same key the transaction wire uses, so the shape is
// learned once.
type kvBatchRequest struct {
	Operations []KVOp `json:"operations"`
}

// Batch runs several operations in ONE transaction, in one round trip.
//
// The server applies them ordered by (namespace, key) and reports them in INPUT
// order, and it refuses a batch that writes the same key twice -- that rule is
// what makes the lock order total, not hygiene (§6.1).
func (kv *KV) Batch(ctx context.Context, ops ...KVOp) ([]KVResult, error) {
	return kv.batch(ctx, ops)
}

// Get reads one key through the path route, which also returns an ETag.
//
// A miss is Found:false with no error: the status code describes the call.
func (kv *KV) Get(ctx context.Context, ns, key string) (KVEntry, error) {
	body, err := kv.httpClient.GetRaw(ctx, kvPath(ns, key))
	if err != nil {
		return KVEntry{}, surfaceError(err)
	}
	var r KVResult
	if err := decodeKV(body, &r); err != nil {
		return KVEntry{}, fmt.Errorf("queen: kv get response: %w", err)
	}
	return r.Entry(), nil
}

// KVGetAs reads one key and decodes its value into T.
//
// This is the ONE generic function of this package (§10.2). Everything else
// hands back json.RawMessage, because a KV value is whatever the caller stored
// and this client does not pretend to know its shape.
//
// found is false when the key is not there (or has expired but not yet been
// pruned -- an expired key is never returned, §5.7), and value is then the zero
// value of T.
func KVGetAs[T any](ctx context.Context, kv *KV, ns, key string) (T, bool, error) {
	var out T
	entry, err := kv.Get(ctx, ns, key)
	if err != nil || !entry.Found {
		return out, false, err
	}
	if len(entry.Value) == 0 {
		return out, true, nil
	}
	if err := decodeKV(entry.Value, &out); err != nil {
		var zero T
		return zero, true, fmt.Errorf("queen: kv value of %s/%s does not decode into %T: %w", ns, key, out, err)
	}
	return out, true, nil
}

// GetMany reads several keys of one namespace.
func (kv *KV) GetMany(ctx context.Context, ns string, keys []string) (KVMany, error) {
	res, err := kv.batch(ctx, []KVOp{KVGetManyOp(ns, keys)})
	if err != nil {
		return KVMany{}, err
	}
	return res[0].Many(), nil
}

// GetPrefix reads one page of a prefix.
func (kv *KV) GetPrefix(ctx context.Context, ns, prefix string, opts ...KVPrefixOptions) (KVPage, error) {
	res, err := kv.batch(ctx, []KVOp{KVGetPrefixOp(ns, prefix, opts...)})
	if err != nil {
		return KVPage{}, err
	}
	return res[0].Page(), nil
}

// Put writes a key through the path route, replacing value AND expiry.
func (kv *KV) Put(ctx context.Context, ns, key string, value interface{}, exp Expiry, opts ...KVWriteOptions) (KVWrite, error) {
	op := KVPutOp(ns, key, value, exp, opts...)
	if op.err != nil {
		return KVWrite{}, op.err
	}
	// The path route names ns and key in the URL and refuses a body that
	// shadows them, so only the payload half travels.
	body, err := kv.httpClient.PutRaw(ctx, kvPath(ns, key), kvPathWrite{
		Value:      op.Value,
		TTLSeconds: op.TTLSeconds,
		Forever:    op.Forever,
		Expect:     op.Expect,
		Required:   op.Required,
	})
	if err != nil {
		return KVWrite{}, surfaceError(err)
	}
	if pe := kvPrecondition(body); pe != nil {
		return KVWrite{}, pe
	}
	var r KVResult
	if err := decodeKV(body, &r); err != nil {
		return KVWrite{}, fmt.Errorf("queen: kv put response: %w", err)
	}
	return r.Write(), nil
}

// PutIfAbsent writes a key only if it is not there, and hands back the WINNER's
// value when it loses.
func (kv *KV) PutIfAbsent(ctx context.Context, ns, key string, value interface{}, exp Expiry, opts ...KVWriteOptions) (KVWrite, error) {
	res, err := kv.batch(ctx, []KVOp{KVPutIfAbsentOp(ns, key, value, exp, opts...)})
	if err != nil {
		return KVWrite{}, err
	}
	return res[0].Write(), nil
}

// Delete removes a key through the path route.
func (kv *KV) Delete(ctx context.Context, ns, key string, opts ...KVWriteOptions) (KVWrite, error) {
	op := KVDeleteOp(ns, key, opts...)
	if op.err != nil {
		return KVWrite{}, op.err
	}
	// No options means no body at all: the common case must not depend on the
	// server tolerating an empty object.
	var payload interface{}
	if op.Expect != nil || op.Required {
		payload = kvPathWrite{Expect: op.Expect, Required: op.Required}
	}
	body, err := kv.httpClient.DeleteRaw(ctx, kvPath(ns, key), payload)
	if err != nil {
		return KVWrite{}, surfaceError(err)
	}
	if pe := kvPrecondition(body); pe != nil {
		return KVWrite{}, pe
	}
	var r KVResult
	if err := decodeKV(body, &r); err != nil {
		return KVWrite{}, fmt.Errorf("queen: kv delete response: %w", err)
	}
	return r.Write(), nil
}

// Incr adds delta to a numeric key. It goes through POST /api/v1/kv, which is
// the only surface that carries it: no literal segment may be added under
// /api/v1/kv/:ns/ or it would make any key named like it unreachable (§8.1).
func (kv *KV) Incr(ctx context.Context, ns, key string, delta int64, exp Expiry, opts ...KVIncrOptions) (KVCounter, error) {
	res, err := kv.batch(ctx, []KVOp{KVIncrOp(ns, key, delta, exp, opts...)})
	if err != nil {
		return KVCounter{}, err
	}
	return res[0].Counter()
}

// kvPathWrite is the body of a PUT or DELETE on a path route: the fields the URL
// does not already name. `op`, `ns` and `key` are deliberately absent -- the
// server rejects a body that carries them rather than ignoring it.
type kvPathWrite struct {
	Value      json.RawMessage `json:"value,omitempty"`
	TTLSeconds int64           `json:"ttlSeconds,omitempty"`
	Forever    bool            `json:"forever,omitempty"`
	Expect     *int64          `json:"expect,omitempty"`
	Required   bool            `json:"required,omitempty"`
}
