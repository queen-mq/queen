package queen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// TransactionBuilder provides a fluent API for atomic operations.
//
// Beside the push and ack operations, a transaction can carry KV writes and
// timer operations: see KV and Timers below. Those two travel as TOP-LEVEL
// arrays of the request and never inside `operations` -- the reason is in the
// comment on transactionRequest, and it is not a matter of taste.
type TransactionBuilder struct {
	httpClient     *HttpClient
	operations     []Operation
	requiredLeases map[string]bool
	kv             []KVOp
	timers         []TimerOp
}

// NewTransactionBuilder creates a new TransactionBuilder.
func NewTransactionBuilder(httpClient *HttpClient) *TransactionBuilder {
	return &TransactionBuilder{
		httpClient:     httpClient,
		operations:     make([]Operation, 0),
		requiredLeases: make(map[string]bool),
	}
}

// Ack adds an acknowledgment operation to the transaction.
// messages can be *Message, []*Message, or []Message
// status should be "completed" or "failed"
func (tb *TransactionBuilder) Ack(messages interface{}, status string, opts AckOptions) *TransactionBuilder {
	// Normalize messages to slice
	var msgs []*Message
	switch m := messages.(type) {
	case *Message:
		msgs = []*Message{m}
	case []*Message:
		msgs = m
	case []Message:
		for i := range m {
			msgs = append(msgs, &m[i])
		}
	case Message:
		msgs = []*Message{&m}
	}

	// Add ack operations
	for _, msg := range msgs {
		op := Operation{
			Type:          "ack",
			TransactionID: msg.TransactionID,
			PartitionID:   msg.PartitionID,
			Status:        status,
			ConsumerGroup: opts.ConsumerGroup,
		}
		tb.operations = append(tb.operations, op)

		// Track required leases
		if msg.LeaseID != "" {
			tb.requiredLeases[msg.LeaseID] = true
		}
	}

	return tb
}

// Queue returns a TransactionQueueBuilder for adding push operations.
func (tb *TransactionBuilder) Queue(name string) *TransactionQueueBuilder {
	return &TransactionQueueBuilder{
		tb:        tb,
		queueName: name,
		partition: DefaultPartition,
	}
}

// KV adds key/value operations to the transaction (PLAN_KV_TIMERS.md §6.3).
//
// This is the point of the whole feature: the write and the gate commit
// together, so an idempotency marker cannot be set by a bundle that then fails,
// and a bundle cannot commit once somebody else has taken the marker. Use
// KVWriteOptions{Required: true} on the op that IS the gate -- without it, a
// lost precondition is only a verdict in the results and the messages still go
// out.
//
// getPrefix is not available here: read work whose cost the caller does not
// bound, inside the transaction holding the outermost lock space (§5.5). It is
// refused before the request leaves.
func (tb *TransactionBuilder) KV(ops ...KVOp) *TransactionBuilder {
	tb.kv = append(tb.kv, ops...)
	return tb
}

// Timers adds timer operations to the transaction: schedule, reschedule and
// cancel, built with ScheduleTimerOp, RescheduleTimerOp and CancelTimerOp.
//
// The canonical uses are the two ends of a saga: schedule the compensation timer
// in the same commit that starts the work, and cancel it in the same commit that
// acks the completion.
func (tb *TransactionBuilder) Timers(ops ...TimerOp) *TransactionBuilder {
	tb.timers = append(tb.timers, ops...)
	return tb
}

// Commit executes the transaction atomically.
//
// WHAT COMES BACK, AND WHEN (§8.3, and the client-go row of §10.2):
//
//   - A lost KV precondition is RETURNED, not raised: err is nil, Success is
//     false and Reason is ReasonKVPrecondition, with FailedIndex, KVReason,
//     Version and Value filled in. It is the expected outcome of every
//     legitimate redelivery -- the redelivery finds the marker already taken --
//     and a caller must be able to branch on it without parsing an error string.
//
//   - Every OTHER failure is an error. A bundle that did not commit must not
//     read as "fine" to a caller who only checks err.
//
// The failing response is still handed back alongside the error, so the reason
// stays readable without unwrapping.
func (tb *TransactionBuilder) Commit(ctx context.Context) (*TransactionResponse, error) {
	if len(tb.operations) == 0 && len(tb.kv) == 0 && len(tb.timers) == 0 {
		return nil, fmt.Errorf("transaction has no operations")
	}
	if err := validateKVOps(tb.kv); err != nil {
		return nil, err
	}
	for i, op := range tb.kv {
		if op.Op == "getPrefix" {
			return nil, fmt.Errorf("queen: kv operation at index %d: getPrefix is not available inside a transaction; it lives only on POST /api/v1/kv", i)
		}
	}
	if err := validateTimerOps(tb.timers); err != nil {
		return nil, err
	}

	// Build required leases list
	leases := make([]string, 0, len(tb.requiredLeases))
	for lease := range tb.requiredLeases {
		leases = append(leases, lease)
	}

	// Build request
	req := transactionRequest{
		Operations:     tb.operations,
		RequiredLeases: leases,
		KV:             tb.kv,
		Timers:         tb.timers,
	}

	// Make request. The RAW path, not Post: this answer carries a KV `version`,
	// which is a BIGINT, and map[string]interface{} would round it through a
	// float64 (§10.4).
	body, err := tb.httpClient.PostRaw(ctx, "/api/v1/transaction", req)
	if err != nil {
		// A 4xx from this endpoint still carries the transaction failure shape
		// (a rider refused by the operator's runtime kill switch answers 403 with
		// `reason:"kv_disabled"` or `"timers_disabled"`, deliberately permanent
		// where the standalone route would say 503). Hand it back beside the
		// error so the caller can branch on the reason rather than on prose.
		return failedTransactionFrom(err), fmt.Errorf("transaction commit failed: %w", surfaceError(err))
	}

	response, err := parseTransactionResponse(body)
	if err != nil {
		return nil, err
	}

	logInfo("TransactionBuilder.Commit", map[string]interface{}{
		"operations": len(tb.operations),
		"leases":     len(leases),
		"kv":         len(tb.kv),
		"timers":     len(tb.timers),
		"success":    response.Success,
		"reason":     response.Reason,
	})

	if !response.Success {
		if response.IsKVPrecondition() {
			return response, nil
		}
		return response, fmt.Errorf("transaction failed (%s): %s", response.Reason, response.Error)
	}
	return response, nil
}

// failedTransactionFrom reads the transaction failure shape out of an HTTP-level
// error body, or returns nil when the body is not one.
func failedTransactionFrom(err error) *TransactionResponse {
	var he *HTTPError
	if !errors.As(err, &he) || he.Body == "" {
		return nil
	}
	resp, perr := parseTransactionResponse([]byte(he.Body))
	if perr != nil {
		return nil
	}
	resp.Success = false
	return resp
}

// parseTransactionResponse decodes a commit answer, including the rider results
// scattered into the flat result space (§8.2 point 1: `[0, ops_flat)` are the
// operations exactly as before, then the kv array, then the timers array).
func parseTransactionResponse(body []byte) (*TransactionResponse, error) {
	var raw struct {
		TransactionID string            `json:"transactionId"`
		Success       *bool             `json:"success"`
		Error         string            `json:"error"`
		Reason        string            `json:"reason"`
		FailedIndex   *int              `json:"failedIndex"`
		KVReason      string            `json:"kvReason"`
		Version       *int64            `json:"version"`
		Value         json.RawMessage   `json:"value"`
		Results       []json.RawMessage `json:"results"`
	}
	if err := decodeKV(body, &raw); err != nil {
		return nil, fmt.Errorf("transaction response: %w", err)
	}

	resp := &TransactionResponse{
		Success:       raw.Success == nil || *raw.Success,
		Error:         raw.Error,
		Reason:        raw.Reason,
		TransactionID: raw.TransactionID,
		KVReason:      raw.KVReason,
		Value:         raw.Value,
		FailedIndex:   -1,
		Results:       raw.Results,
	}
	if raw.Error != "" {
		resp.Success = false
	}
	if raw.FailedIndex != nil {
		resp.FailedIndex = *raw.FailedIndex
	}
	if raw.Version != nil {
		resp.Version = *raw.Version
	}

	// Rider results carry their own `type`, stamped by the broker when it
	// scatters them back into the flat array.
	for _, item := range raw.Results {
		var kind struct {
			Type string `json:"type"`
		}
		if err := decodeKV(item, &kind); err != nil {
			continue
		}
		switch kind.Type {
		case "kv":
			var r KVResult
			if err := decodeKV(item, &r); err == nil {
				resp.KV = append(resp.KV, r)
			}
		case "timer":
			var r TimerResult
			if err := decodeKV(item, &r); err == nil {
				resp.Timers = append(resp.Timers, r)
			}
		}
	}
	return resp, nil
}

// TransactionQueueBuilder provides a fluent API for adding push operations to a transaction.
type TransactionQueueBuilder struct {
	tb        *TransactionBuilder
	queueName string
	partition string
}

// Partition sets the partition for push operations.
func (tqb *TransactionQueueBuilder) Partition(name string) *TransactionQueueBuilder {
	if name == "" {
		tqb.partition = DefaultPartition
	} else {
		tqb.partition = name
	}
	return tqb
}

// Push adds push operations to the transaction.
// payload can be a single item or a slice.
func (tqb *TransactionQueueBuilder) Push(payload interface{}) *TransactionBuilder {
	// Normalize payload to slice
	var payloads []interface{}
	switch p := payload.(type) {
	case []interface{}:
		payloads = p
	case []map[string]interface{}:
		for _, m := range p {
			payloads = append(payloads, m)
		}
	default:
		payloads = []interface{}{payload}
	}

	// Build push items.
	//
	// A caller who passes a PushItem controls its own TransactionID, which is
	// what makes a retried transaction idempotent inside the dedup window.
	// Anything else is treated as a bare payload and gets a minted id, which
	// is the previous behaviour.
	items := make([]PushItem, len(payloads))
	for i, p := range payloads {
		item := PushItem{
			Queue:         tqb.queueName,
			Partition:     tqb.partition,
			Payload:       p,
			TransactionID: GenerateUUID(),
		}
		switch v := p.(type) {
		case PushItem:
			item.Payload = v.Payload
			if v.TransactionID != "" {
				item.TransactionID = v.TransactionID
			}
			item.TraceID = v.TraceID
			if v.Partition != "" {
				item.Partition = v.Partition
			}
		case *PushItem:
			if v != nil {
				item.Payload = v.Payload
				if v.TransactionID != "" {
					item.TransactionID = v.TransactionID
				}
				item.TraceID = v.TraceID
				if v.Partition != "" {
					item.Partition = v.Partition
				}
			}
		}
		items[i] = item
	}

	// Add push operation
	op := Operation{
		Type:  "push",
		Items: items,
	}
	tqb.tb.operations = append(tqb.tb.operations, op)

	return tqb.tb
}
