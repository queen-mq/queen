package queen

import (
	"context"
	"net/url"
	"strconv"
)

// DLQBuilder provides a fluent API for dead letter queue queries.
type DLQBuilder struct {
	httpClient    *HttpClient
	queueName     string
	consumerGroup string
	partition     string
	limitVal      int
	offsetVal     int
	fromTime      string
	toTime        string
}

// NewDLQBuilder creates a new DLQBuilder.
func NewDLQBuilder(httpClient *HttpClient, queueName, consumerGroup, partition string) *DLQBuilder {
	return &DLQBuilder{
		httpClient:    httpClient,
		queueName:     queueName,
		consumerGroup: consumerGroup,
		partition:     partition,
	}
}

// Limit sets the maximum number of messages to return.
func (db *DLQBuilder) Limit(count int) *DLQBuilder {
	db.limitVal = count
	return db
}

// Offset sets the offset for pagination.
func (db *DLQBuilder) Offset(count int) *DLQBuilder {
	db.offsetVal = count
	return db
}

// From sets the start timestamp filter.
func (db *DLQBuilder) From(timestamp string) *DLQBuilder {
	db.fromTime = timestamp
	return db
}

// To sets the end timestamp filter.
func (db *DLQBuilder) To(timestamp string) *DLQBuilder {
	db.toTime = timestamp
	return db
}

// Get executes the DLQ query and returns the results.
func (db *DLQBuilder) Get(ctx context.Context) (*DLQResponse, error) {
	// Build query params
	params := url.Values{}

	if db.queueName != "" {
		params.Set("queue", db.queueName)
	}
	if db.consumerGroup != "" {
		params.Set("consumerGroup", db.consumerGroup)
	}
	if db.partition != "" && db.partition != DefaultPartition {
		params.Set("partition", db.partition)
	}
	if db.limitVal > 0 {
		params.Set("limit", strconv.Itoa(db.limitVal))
	}
	if db.offsetVal > 0 {
		params.Set("offset", strconv.Itoa(db.offsetVal))
	}
	if db.fromTime != "" {
		params.Set("from", db.fromTime)
	}
	if db.toTime != "" {
		params.Set("to", db.toTime)
	}

	// Build path
	path := "/api/v1/dlq"
	if encoded := params.Encode(); encoded != "" {
		path += "?" + encoded
	}

	// Make request
	result, err := db.httpClient.Get(ctx, path, 0, "")
	if err != nil {
		// Return empty response on error (matching Python/JS behavior)
		logWarn("DLQBuilder.Get", map[string]interface{}{
			"error": err.Error(),
		})
		return &DLQResponse{Messages: []Message{}, Total: 0}, nil
	}

	// Parse response
	response := &DLQResponse{
		Messages: []Message{},
		Total:    0,
	}

	if messagesRaw, ok := result["messages"]; ok {
		if msgsArray, ok := messagesRaw.([]interface{}); ok {
			for _, msgRaw := range msgsArray {
				if msgMap, ok := msgRaw.(map[string]interface{}); ok {
					msg := parseMessageMap(msgMap)
					response.Messages = append(response.Messages, msg)
				}
			}
		}
	}

	if total, ok := result["total"].(float64); ok {
		response.Total = int(total)
	}

	logDebug("DLQBuilder.Get", map[string]interface{}{
		"queue":         db.queueName,
		"consumerGroup": db.consumerGroup,
		"count":         len(response.Messages),
		"total":         response.Total,
	})

	return response, nil
}

// parseMessageMap parses a message from a map.
func parseMessageMap(msgMap map[string]interface{}) Message {
	msg := Message{}

	if v, ok := msgMap["transactionId"].(string); ok {
		msg.TransactionID = v
	}
	if v, ok := msgMap["partitionId"].(string); ok {
		msg.PartitionID = v
	}
	if v, ok := msgMap["leaseId"].(string); ok {
		msg.LeaseID = v
	}
	if v, ok := msgMap["queue"].(string); ok {
		msg.Queue = v
	}
	if v, ok := msgMap["partition"].(string); ok {
		msg.Partition = v
	}
	if v, ok := msgMap["data"].(map[string]interface{}); ok {
		msg.Data = v
	}
	if v, ok := msgMap["createdAt"].(string); ok {
		msg.CreatedAt = v
	}
	if v, ok := msgMap["errorMessage"].(string); ok {
		msg.ErrorMessage = v
	}
	if v, ok := msgMap["retryCount"].(float64); ok {
		msg.RetryCount = int(v)
	}
	// A dead letter keeps the headers its message had.
	msg.Headers = headersOf(msgMap["headers"])

	return msg
}
