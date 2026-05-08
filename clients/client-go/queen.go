package queen

import (
	"context"
	"fmt"
	"time"
)

// Queen is the main client for Queen MQ.
type Queen struct {
	httpClient    *HttpClient
	bufferManager *BufferManager
	admin         *Admin
}

// New creates a new Queen client.
// Config can be:
//   - string: single URL
//   - []string: multiple URLs
//   - ClientConfig: full configuration
func New(config interface{}) (*Queen, error) {
	var clientConfig ClientConfig

	switch c := config.(type) {
	case string:
		clientConfig = ClientConfig{URL: c}
	case []string:
		clientConfig = ClientConfig{URLs: c}
	case ClientConfig:
		clientConfig = c
	case *ClientConfig:
		if c != nil {
			clientConfig = *c
		}
	default:
		return nil, fmt.Errorf("invalid config type: expected string, []string, or ClientConfig")
	}

	// Apply defaults
	if clientConfig.EnableFailover == false && len(clientConfig.URLs) == 0 && clientConfig.URL == "" {
		clientConfig.EnableFailover = ClientDefaults.EnableFailover
	}

	httpClient, err := NewHttpClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %w", err)
	}

	return &Queen{
		httpClient:    httpClient,
		bufferManager: NewBufferManager(),
	}, nil
}

// Queue returns a QueueBuilder for the specified queue.
func (q *Queen) Queue(name string) *QueueBuilder {
	return NewQueueBuilder(q, name)
}

// Admin returns the Admin API client.
func (q *Queen) Admin() *Admin {
	if q.admin == nil {
		q.admin = NewAdmin(q.httpClient)
	}
	return q.admin
}

// Transaction returns a new TransactionBuilder.
func (q *Queen) Transaction() *TransactionBuilder {
	return NewTransactionBuilder(q.httpClient)
}

// Ack acknowledges one or more messages.
// messages can be *Message, []*Message, or []Message
// success: true for completed, false for failed
func (q *Queen) Ack(ctx context.Context, messages interface{}, success bool, opts AckOptions) ([]AckResponse, error) {
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
	default:
		return nil, fmt.Errorf("invalid messages type: expected *Message, []*Message, or []Message")
	}

	if len(msgs) == 0 {
		return nil, fmt.Errorf("at least one message is required")
	}

	// Validate messages
	if err := ValidateMessages(msgs); err != nil {
		return nil, err
	}

	// Determine status
	status := AckStatusCompleted
	if !success {
		status = AckStatusFailed
	}

	// Single message ack
	if len(msgs) == 1 {
		msg := msgs[0]
		req := ackRequest{
			TransactionID: msg.TransactionID,
			PartitionID:   msg.PartitionID,
			LeaseID:       msg.LeaseID,
			Status:        status,
			Error:         opts.Error,
			ConsumerGroup: opts.ConsumerGroup,
		}

		result, err := q.httpClient.Post(ctx, "/api/v1/ack", req)
		if err != nil {
			return nil, fmt.Errorf("ack request failed: %w", err)
		}

		// Parse response
		response := AckResponse{Success: true}
		if errMsg, ok := result["error"].(string); ok && errMsg != "" {
			response.Success = false
			response.Error = errMsg
		}

		logDebug("Queen.Ack", map[string]interface{}{
			"transactionId": msg.TransactionID,
			"status":        status,
			"success":       response.Success,
		})

		return []AckResponse{response}, nil
	}

	// Batch ack
	acks := make([]ackRequest, len(msgs))
	for i, msg := range msgs {
		acks[i] = ackRequest{
			TransactionID: msg.TransactionID,
			PartitionID:   msg.PartitionID,
			LeaseID:       msg.LeaseID,
			Status:        status,
			Error:         opts.Error,
		}
	}

	req := batchAckRequest{
		Acknowledgments: acks,
		ConsumerGroup:   opts.ConsumerGroup,
	}

	result, err := q.httpClient.Post(ctx, "/api/v1/ack/batch", req)
	if err != nil {
		return nil, fmt.Errorf("batch ack request failed: %w", err)
	}

	// Parse response
	var responses []AckResponse
	if data, ok := result["data"].([]interface{}); ok {
		for _, item := range data {
			if itemMap, ok := item.(map[string]interface{}); ok {
				resp := AckResponse{Success: true}
				if errMsg, ok := itemMap["error"].(string); ok && errMsg != "" {
					resp.Success = false
					resp.Error = errMsg
				}
				responses = append(responses, resp)
			}
		}
	} else {
		// Single response for batch
		resp := AckResponse{Success: true}
		if errMsg, ok := result["error"].(string); ok && errMsg != "" {
			resp.Success = false
			resp.Error = errMsg
		}
		for range msgs {
			responses = append(responses, resp)
		}
	}

	logDebug("Queen.Ack", map[string]interface{}{
		"count":   len(msgs),
		"status":  status,
		"success": len(responses),
	})

	return responses, nil
}

// Renew renews the lease for one or more messages.
// messages can be *Message, []*Message, []Message, string (leaseId), or []string (leaseIds)
func (q *Queen) Renew(ctx context.Context, messages interface{}) ([]RenewResponse, error) {
	// Normalize to lease IDs
	var leaseIDs []string
	switch m := messages.(type) {
	case string:
		leaseIDs = []string{m}
	case []string:
		leaseIDs = m
	case *Message:
		if m.LeaseID == "" {
			return nil, fmt.Errorf("message has no leaseId")
		}
		leaseIDs = []string{m.LeaseID}
	case []*Message:
		for _, msg := range m {
			if msg.LeaseID != "" {
				leaseIDs = append(leaseIDs, msg.LeaseID)
			}
		}
	case []Message:
		for _, msg := range m {
			if msg.LeaseID != "" {
				leaseIDs = append(leaseIDs, msg.LeaseID)
			}
		}
	case Message:
		if m.LeaseID == "" {
			return nil, fmt.Errorf("message has no leaseId")
		}
		leaseIDs = []string{m.LeaseID}
	default:
		return nil, fmt.Errorf("invalid messages type")
	}

	if len(leaseIDs) == 0 {
		return nil, fmt.Errorf("no lease IDs to renew")
	}

	// Dedupe: with v4 multi-partition pop, all messages in one batch share
	// the same leaseId (one renew_lease_v2 call extends every claimed
	// partition_consumers row). Without this dedupe, callers passing the
	// full message slice would issue N redundant identical HTTP calls.
	{
		seen := make(map[string]struct{}, len(leaseIDs))
		deduped := leaseIDs[:0:0]
		for _, id := range leaseIDs {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			deduped = append(deduped, id)
		}
		leaseIDs = deduped
	}

	// Renew each lease
	var responses []RenewResponse
	for _, leaseID := range leaseIDs {
		path := fmt.Sprintf("/api/v1/lease/%s/extend", leaseID)
		result, err := q.httpClient.Post(ctx, path, map[string]interface{}{})

		resp := RenewResponse{LeaseID: leaseID}
		if err != nil {
			resp.Success = false
			resp.Error = err.Error()
		} else {
			resp.Success = true
			if expiresAt, ok := result["newExpiresAt"].(string); ok {
				if t, err := time.Parse(time.RFC3339, expiresAt); err == nil {
					resp.NewExpiresAt = t
				}
			}
		}
		responses = append(responses, resp)

		logDebug("Queen.Renew", map[string]interface{}{
			"leaseId": leaseID,
			"success": resp.Success,
		})
	}

	return responses, nil
}

// FlushAllBuffers flushes all message buffers.
func (q *Queen) FlushAllBuffers(ctx context.Context) error {
	return q.bufferManager.FlushAll(ctx)
}

// GetBufferStats returns statistics about message buffers.
func (q *Queen) GetBufferStats() BufferStats {
	return q.bufferManager.GetStats()
}

// DeleteConsumerGroup deletes a consumer group.
func (q *Queen) DeleteConsumerGroup(ctx context.Context, consumerGroup string, deleteMetadata bool) error {
	path := fmt.Sprintf("/api/v1/consumer-groups/%s?deleteMetadata=%t", consumerGroup, deleteMetadata)
	_, err := q.httpClient.Delete(ctx, path)
	if err != nil {
		return fmt.Errorf("failed to delete consumer group: %w", err)
	}

	logInfo("Queen.DeleteConsumerGroup", map[string]interface{}{
		"consumerGroup":  consumerGroup,
		"deleteMetadata": deleteMetadata,
	})

	return nil
}

// UpdateConsumerGroupTimestamp updates the subscription timestamp for a consumer group.
func (q *Queen) UpdateConsumerGroupTimestamp(ctx context.Context, consumerGroup string, timestamp time.Time) error {
	path := fmt.Sprintf("/api/v1/consumer-groups/%s/subscription", consumerGroup)
	body := map[string]interface{}{
		"subscriptionTimestamp": timestamp.Format(time.RFC3339),
	}

	_, err := q.httpClient.Post(ctx, path, body)
	if err != nil {
		return fmt.Errorf("failed to update consumer group timestamp: %w", err)
	}

	logInfo("Queen.UpdateConsumerGroupTimestamp", map[string]interface{}{
		"consumerGroup": consumerGroup,
		"timestamp":     timestamp,
	})

	return nil
}

// Close gracefully shuts down the Queen client.
// It flushes all buffers before closing.
func (q *Queen) Close(ctx context.Context) error {
	logInfo("Queen.Close", map[string]interface{}{
		"status": "closing",
	})

	// Flush all buffers
	if err := q.bufferManager.FlushAll(ctx); err != nil {
		logError("Queen.Close", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to flush buffers: %w", err)
	}

	// Close HTTP client
	q.httpClient.Close()

	logInfo("Queen.Close", map[string]interface{}{
		"status": "closed",
	})

	return nil
}

// GetHttpClient returns the HTTP client (for internal use).
func (q *Queen) GetHttpClient() *HttpClient {
	return q.httpClient
}

// GetBufferManager returns the buffer manager (for internal use).
func (q *Queen) GetBufferManager() *BufferManager {
	return q.bufferManager
}
