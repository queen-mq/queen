package queen

import (
	"encoding/json"
	"context"
	"fmt"
)

// PushBuilder provides a fluent API for push operations.
type PushBuilder struct {
	qb            *QueueBuilder
	payloads      []interface{}
	transactionID string
	traceID       string
	headers       map[string]string
}

// NewPushBuilder creates a new PushBuilder.
func NewPushBuilder(qb *QueueBuilder, payload interface{}) *PushBuilder {
	pb := &PushBuilder{
		qb: qb,
	}

	// Handle different payload types
	switch p := payload.(type) {
	case []interface{}:
		pb.payloads = p
	case []map[string]interface{}:
		for _, m := range p {
			pb.payloads = append(pb.payloads, m)
		}
	case []PushItem:
		// One message per item, each with its own transaction id and headers
		// (see buildItems), not one message whose payload is the list.
		for _, it := range p {
			pb.payloads = append(pb.payloads, it)
		}
	case []*PushItem:
		for _, it := range p {
			pb.payloads = append(pb.payloads, it)
		}
	default:
		pb.payloads = []interface{}{payload}
	}

	return pb
}

// TransactionID sets a custom transaction ID for the message.
func (pb *PushBuilder) TransactionID(id string) *PushBuilder {
	pb.transactionID = id
	return pb
}

// TraceID sets a trace ID for the message.
func (pb *PushBuilder) TraceID(id string) *PushBuilder {
	pb.traceID = id
	return pb
}

// Headers sets headers on every message of this push (a cluster at catalogue
// version 8): a map of strings, names of 1-64 bytes of a-z 0-9 . _ - (names
// starting with "queen-" are the broker's). They are never encrypted and never
// part of deduplication; every read returns them (Message.Headers), and every
// where can test them (headers.<name>) without reading the payload:
//
//	client.Queue("orders").Partition("order-9137").
//		Push(map[string]interface{}{"orderId": 9137, "total": 120}).
//		Headers(map[string]string{"type": "order.paid"}).
//		Execute(ctx)
//
// A message given as a PushItem writes its own Headers over these. The broker
// takes 64 headers and 8 KiB of them per message by default, and refuses the
// push (400 bad_body) beyond. They are stored in clear, even on an encrypted
// queue: keep secrets and personal data out of them.
func (pb *PushBuilder) Headers(h map[string]string) *PushBuilder {
	pb.headers = h
	return pb
}

// Execute pushes the messages to the queue.
func (pb *PushBuilder) Execute(ctx context.Context) ([]PushResponse, error) {
	// Validate queue name
	if pb.qb.queueName == "" {
		return nil, fmt.Errorf("queue name is required")
	}

	// Build items
	items := pb.buildItems()

	// Check if buffering is enabled
	if pb.qb.bufferConfig != nil {
		return pb.executeBuffered(ctx, items)
	}

	return pb.executeDirect(ctx, items)
}

// buildItems builds the push items from payloads.
func (pb *PushBuilder) buildItems() []PushItem {
	items := make([]PushItem, len(pb.payloads))
	
	partition := pb.qb.partition
	if partition == "" {
		partition = DefaultPartition
	}

	for i, payload := range pb.payloads {
		item := PushItem{
			Queue:     pb.qb.queueName,
			Partition: partition,
			Payload:   payload,
			Headers:   pb.headers,
		}

		// Set transaction ID
		if i == 0 && pb.transactionID != "" {
			// Use provided transaction ID for first item only
			item.TransactionID = pb.transactionID
		} else {
			// Generate unique transaction ID for all other items
			item.TransactionID = GenerateUUID()
		}

		// Set trace ID
		if pb.traceID != "" && IsValidUUID(pb.traceID) {
			item.TraceID = pb.traceID
		}

		// A message given as a PushItem brings its own transaction id (what
		// makes a retried push a duplicate instead of a second message), trace
		// id, partition and headers.
		if own, ok := pushItemOf(payload); ok {
			item.Payload = own.Payload
			if own.TransactionID != "" {
				item.TransactionID = own.TransactionID
			}
			if own.TraceID != "" {
				item.TraceID = own.TraceID
			}
			if own.Partition != "" {
				item.Partition = own.Partition
			}
			item.Headers = withHeaders(pb.headers, own.Headers)
		}

		items[i] = item
	}

	return items
}

// pushItemOf reads a payload given as a PushItem (or a non-nil *PushItem); ok
// is false for a bare payload.
func pushItemOf(payload interface{}) (PushItem, bool) {
	switch v := payload.(type) {
	case PushItem:
		return v, true
	case *PushItem:
		if v != nil {
			return *v, true
		}
	}
	return PushItem{}, false
}

// executeBuffered adds items to the buffer.
func (pb *PushBuilder) executeBuffered(ctx context.Context, items []PushItem) ([]PushResponse, error) {
	key := pb.qb.getBufferKey()
	
	flushFn := func(ctx context.Context, items []PushItem) error {
		_, err := pb.executeDirect(ctx, items)
		return err
	}

	err := pb.qb.queen.bufferManager.Add(ctx, key, items, *pb.qb.bufferConfig, flushFn)
	if err != nil {
		return nil, fmt.Errorf("failed to buffer messages: %w", err)
	}

	// Return pending responses (items are buffered, not yet sent)
	responses := make([]PushResponse, len(items))
	for i, item := range items {
		responses[i] = PushResponse{
			Status:        "buffered",
			TransactionID: item.TransactionID,
		}
	}

	logDebug("PushBuilder.executeBuffered", map[string]interface{}{
		"queue":   pb.qb.queueName,
		"count":   len(items),
	})

	return responses, nil
}

// executeDirect pushes items directly to the server.
func (pb *PushBuilder) executeDirect(ctx context.Context, items []PushItem) ([]PushResponse, error) {
	hc := pb.qb.queen.httpClient
	if hc.config.BinaryPush {
		if body, ok := binaryPushBody(items, hc.config.PushCompression); ok {
			raw, err := hc.PostRaw(ctx, "/api/v1/push", rawRequestBody(body))
			if err != nil {
				return nil, fmt.Errorf("push request failed: %w", err)
			}
			var answers []struct {
				Status        string `json:"status"`
				TransactionID string `json:"transaction_id"`
				Error         string `json:"error"`
			}
			if err := json.Unmarshal(raw, &answers); err != nil {
				return nil, fmt.Errorf("push answer: %w", err)
			}
			responses := make([]PushResponse, len(items))
			for i := range responses {
				responses[i] = PushResponse{Status: "queued"}
				if i < len(answers) {
					responses[i] = PushResponse{Status: answers[i].Status, TransactionID: answers[i].TransactionID, Error: answers[i].Error}
				}
			}
			return responses, nil
		}
	}

	// Build request
	reqItems := make([]pushRequestItem, len(items))
	for i, item := range items {
		reqItems[i] = pushRequestItem{
			Queue:         item.Queue,
			Partition:     item.Partition,
			Payload:       item.Payload,
			TransactionID: item.TransactionID,
			TraceID:       item.TraceID,
			Headers:       item.Headers,
		}
	}

	req := pushRequest{Items: reqItems}

	// Make request
	result, err := pb.qb.queen.httpClient.Post(ctx, "/api/v1/push", req)
	if err != nil {
		return nil, fmt.Errorf("push request failed: %w", err)
	}

	// Parse response
	responses := parsePushResponses(result, items)

	logDebug("PushBuilder.executeDirect", map[string]interface{}{
		"queue":   pb.qb.queueName,
		"count":   len(items),
		"success": countSuccessful(responses),
	})

	return responses, nil
}

// parsePushResponses parses the push response.
func parsePushResponses(result map[string]interface{}, items []PushItem) []PushResponse {
	responses := make([]PushResponse, len(items))

	// Initialize with defaults
	for i, item := range items {
		responses[i] = PushResponse{
			TransactionID: item.TransactionID,
			Status:        "queued",
		}
	}

	// Check for array response
	if data, ok := result["data"].([]interface{}); ok {
		for i, itemRaw := range data {
			if i >= len(responses) {
				break
			}
			if itemMap, ok := itemRaw.(map[string]interface{}); ok {
				if status, ok := itemMap["status"].(string); ok {
					responses[i].Status = status
				}
				if errMsg, ok := itemMap["error"].(string); ok {
					responses[i].Error = errMsg
				}
				if txID, ok := itemMap["transactionId"].(string); ok {
					responses[i].TransactionID = txID
				}
			}
		}
	}

	return responses
}

// countSuccessful counts successful responses.
func countSuccessful(responses []PushResponse) int {
	count := 0
	for _, r := range responses {
		if r.Status == "queued" {
			count++
		}
	}
	return count
}
