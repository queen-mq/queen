package queen

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
)

// QueueBuilder provides a fluent API for queue operations.
type QueueBuilder struct {
	queen     *Queen
	queueName string

	// Configuration
	namespace        string
	task             string
	partition        string
	queueConfig      *QueueConfig
	bufferConfig     *BufferConfig
	consumerGroup    string
	concurrency      int
	batch            int
	limit            int
	idleMillis       int
	autoAck          *bool
	wait             *bool
	timeoutMillis    int
	renewLease       bool
	renewLeaseMillis int
	subscriptionMode string
	subscriptionFrom string
	each             bool
	maxPartitions    int
}

// NewQueueBuilder creates a new QueueBuilder.
func NewQueueBuilder(queen *Queen, queueName string) *QueueBuilder {
	return &QueueBuilder{
		queen:     queen,
		queueName: queueName,
		partition: DefaultPartition,
	}
}

// Name returns the queue name. Used by the streaming SDK to wire .From()
// and .To() targets without exposing the unexported queueName field.
func (qb *QueueBuilder) Name() string {
	return qb.queueName
}

// Namespace sets the namespace for the queue.
func (qb *QueueBuilder) Namespace(name string) *QueueBuilder {
	qb.namespace = name
	return qb
}

// Task sets the task name for the queue.
func (qb *QueueBuilder) Task(name string) *QueueBuilder {
	qb.task = name
	return qb
}

// Partition sets the partition for operations.
func (qb *QueueBuilder) Partition(name string) *QueueBuilder {
	if name == "" {
		qb.partition = DefaultPartition
	} else {
		qb.partition = name
	}
	return qb
}

// Config sets the queue configuration for create operations.
func (qb *QueueBuilder) Config(config QueueConfig) *QueueBuilder {
	qb.queueConfig = &config
	return qb
}

// Buffer enables client-side buffering for push operations.
func (qb *QueueBuilder) Buffer(config BufferConfig) *QueueBuilder {
	qb.bufferConfig = &config
	return qb
}

// Group sets the consumer group for consume/pop operations.
func (qb *QueueBuilder) Group(name string) *QueueBuilder {
	qb.consumerGroup = name
	return qb
}

// Concurrency sets the number of concurrent workers for consume operations.
func (qb *QueueBuilder) Concurrency(count int) *QueueBuilder {
	qb.concurrency = count
	return qb
}

// Batch sets the batch size for consume/pop operations.
func (qb *QueueBuilder) Batch(size int) *QueueBuilder {
	qb.batch = size
	return qb
}

// Partitions enables v4 multi-partition pop: claim up to N partitions in a
// single call. With Partitions(N), the global Batch(B) budget is shared
// across all claimed partitions — at most B total messages, drawn from up
// to N partitions, in one network round-trip. All N share a single leaseId,
// so a single Renew call extends them all atomically.
//
// Default 1 = legacy single-partition behavior.
func (qb *QueueBuilder) Partitions(n int) *QueueBuilder {
	if n < 1 {
		n = 1
	}
	qb.maxPartitions = n
	return qb
}

// Limit sets the maximum number of messages to process.
func (qb *QueueBuilder) Limit(count int) *QueueBuilder {
	qb.limit = count
	return qb
}

// IdleMillis sets the idle timeout in milliseconds.
func (qb *QueueBuilder) IdleMillis(millis int) *QueueBuilder {
	qb.idleMillis = millis
	return qb
}

// AutoAck sets whether to automatically acknowledge messages.
func (qb *QueueBuilder) AutoAck(enabled bool) *QueueBuilder {
	qb.autoAck = &enabled
	return qb
}

// Wait sets whether to use long polling.
func (qb *QueueBuilder) Wait(enabled bool) *QueueBuilder {
	qb.wait = &enabled
	return qb
}

// TimeoutMillis sets the timeout for long polling.
func (qb *QueueBuilder) TimeoutMillis(millis int) *QueueBuilder {
	qb.timeoutMillis = millis
	return qb
}

// RenewLease enables automatic lease renewal during processing.
func (qb *QueueBuilder) RenewLease(enabled bool, intervalMillis int) *QueueBuilder {
	qb.renewLease = enabled
	qb.renewLeaseMillis = intervalMillis
	return qb
}

// SubscriptionMode sets the subscription mode (all, new, new-only).
func (qb *QueueBuilder) SubscriptionMode(mode string) *QueueBuilder {
	qb.subscriptionMode = mode
	return qb
}

// SubscriptionFrom sets the subscription start point (now, timestamp).
func (qb *QueueBuilder) SubscriptionFrom(from string) *QueueBuilder {
	qb.subscriptionFrom = from
	return qb
}

// Each sets whether to process messages one at a time.
func (qb *QueueBuilder) Each() *QueueBuilder {
	qb.each = true
	return qb
}

// Create returns an OperationBuilder for creating the queue.
func (qb *QueueBuilder) Create() *OperationBuilder {
	return NewOperationBuilder(qb, "create")
}

// Delete returns an OperationBuilder for deleting the queue.
func (qb *QueueBuilder) Delete() *OperationBuilder {
	return NewOperationBuilder(qb, "delete")
}

// Push returns a PushBuilder for pushing messages.
func (qb *QueueBuilder) Push(payload interface{}) *PushBuilder {
	return NewPushBuilder(qb, payload)
}

// Pop pops messages from the queue.
func (qb *QueueBuilder) Pop(ctx context.Context) ([]*Message, error) {
	// Validate queue name
	if qb.queueName == "" && qb.namespace == "" && qb.task == "" {
		return nil, fmt.Errorf("queue name, namespace, or task is required")
	}

	// Build path
	var path string
	if qb.queueName != "" {
		if qb.partition != "" && qb.partition != DefaultPartition {
			path = fmt.Sprintf("/api/v1/pop/queue/%s/partition/%s", url.PathEscape(qb.queueName), url.PathEscape(qb.partition))
		} else {
			path = fmt.Sprintf("/api/v1/pop/queue/%s", url.PathEscape(qb.queueName))
		}
	} else {
		path = "/api/v1/pop"
	}

	// Build query params
	params := qb.buildPopParams()
	if params != "" {
		path += "?" + params
	}

	// Determine timeout
	timeout := qb.timeoutMillis
	if timeout == 0 {
		timeout = PopDefaults.TimeoutMillis
	}

	// Add buffer for long polling
	wait := qb.wait
	if wait == nil {
		w := PopDefaults.Wait
		wait = &w
	}
	if *wait {
		timeout += 5000 // Add 5s buffer for long polling
	}

	// Make request
	result, err := qb.queen.httpClient.Get(ctx, path, timeout, "")
	if err != nil {
		return nil, fmt.Errorf("pop request failed: %w", err)
	}

	// Parse response
	messages := parseMessages(result)

	logDebug("QueueBuilder.Pop", map[string]interface{}{
		"queue":   qb.queueName,
		"count":   len(messages),
	})

	return messages, nil
}

// buildPopParams builds the query parameters for pop requests.
func (qb *QueueBuilder) buildPopParams() string {
	params := url.Values{}

	// Batch size
	batch := qb.batch
	if batch == 0 {
		batch = PopDefaults.Batch
	}
	params.Set("batch", strconv.Itoa(batch))

	// Wait (long polling)
	wait := qb.wait
	if wait == nil {
		w := PopDefaults.Wait
		wait = &w
	}
	params.Set("wait", strconv.FormatBool(*wait))

	// Timeout
	timeout := qb.timeoutMillis
	if timeout == 0 {
		timeout = PopDefaults.TimeoutMillis
	}
	params.Set("timeout", strconv.Itoa(timeout))

	// Consumer group
	if qb.consumerGroup != "" {
		params.Set("consumerGroup", qb.consumerGroup)
	}

	// Auto ack (for pop, this is server-side)
	if qb.autoAck != nil {
		params.Set("autoAck", strconv.FormatBool(*qb.autoAck))
	}

	// Subscription mode
	if qb.subscriptionMode != "" {
		params.Set("subscriptionMode", qb.subscriptionMode)
	}

	// Subscription from
	if qb.subscriptionFrom != "" {
		params.Set("subscriptionFrom", qb.subscriptionFrom)
	}

	// v4 multi-partition pop: drain up to N sparse partitions per call.
	if qb.maxPartitions > 1 {
		params.Set("partitions", strconv.Itoa(qb.maxPartitions))
	}

	// Namespace and task (for namespace/task mode)
	if qb.namespace != "" {
		params.Set("namespace", qb.namespace)
	}
	if qb.task != "" {
		params.Set("task", qb.task)
	}

	return params.Encode()
}

// Consume starts consuming messages from the queue.
func (qb *QueueBuilder) Consume(ctx context.Context, handler MessageHandler) *ConsumeBuilder {
	return NewConsumeBuilder(qb, handler)
}

// ConsumeBatch starts consuming messages in batches.
func (qb *QueueBuilder) ConsumeBatch(ctx context.Context, handler BatchMessageHandler) *ConsumeBuilder {
	return NewConsumeBatchBuilder(qb, handler)
}

// FlushBuffer flushes the buffer for this queue/partition.
func (qb *QueueBuilder) FlushBuffer(ctx context.Context) error {
	key := qb.getBufferKey()
	return qb.queen.bufferManager.Flush(ctx, key)
}

// DLQ returns a DLQBuilder for querying the dead letter queue.
func (qb *QueueBuilder) DLQ(consumerGroup string) *DLQBuilder {
	return NewDLQBuilder(qb.queen.httpClient, qb.queueName, consumerGroup, qb.partition)
}

// getBufferKey returns the buffer key for this queue/partition.
func (qb *QueueBuilder) getBufferKey() string {
	partition := qb.partition
	if partition == "" {
		partition = DefaultPartition
	}
	return fmt.Sprintf("%s/%s", qb.queueName, partition)
}

// getConsumeOptions returns the consume options from the builder configuration.
func (qb *QueueBuilder) getConsumeOptions() ConsumeOptions {
	opts := ConsumeOptions{
		Queue:            qb.queueName,
		Partition:        qb.partition,
		Namespace:        qb.namespace,
		Task:             qb.task,
		Group:            qb.consumerGroup,
		Concurrency:      qb.concurrency,
		Batch:            qb.batch,
		Limit:            qb.limit,
		IdleMillis:       qb.idleMillis,
		TimeoutMillis:    qb.timeoutMillis,
		RenewLease:       qb.renewLease,
		RenewLeaseIntervalMillis: qb.renewLeaseMillis,
		SubscriptionMode: qb.subscriptionMode,
		SubscriptionFrom: qb.subscriptionFrom,
		Each:             qb.each,
		MaxPartitions:    qb.maxPartitions,
	}

	// Apply defaults
	if opts.Concurrency == 0 {
		opts.Concurrency = ConsumeDefaults.Concurrency
	}
	if opts.Batch == 0 {
		opts.Batch = ConsumeDefaults.Batch
	}
	if opts.TimeoutMillis == 0 {
		opts.TimeoutMillis = ConsumeDefaults.TimeoutMillis
	}
	if opts.MaxPartitions < 1 {
		opts.MaxPartitions = 1
	}

	// Auto ack
	if qb.autoAck != nil {
		opts.AutoAck = *qb.autoAck
	} else {
		opts.AutoAck = ConsumeDefaults.AutoAck
	}

	// Wait
	if qb.wait != nil {
		opts.Wait = *qb.wait
	} else {
		opts.Wait = ConsumeDefaults.Wait
	}

	return opts
}

// parseMessages parses messages from the response.
func parseMessages(result map[string]interface{}) []*Message {
	var messages []*Message

	if result == nil {
		return messages
	}

	// Get messages array
	msgsRaw, ok := result["messages"]
	if !ok {
		return messages
	}

	msgsArray, ok := msgsRaw.([]interface{})
	if !ok {
		return messages
	}

	for _, msgRaw := range msgsArray {
		msgMap, ok := msgRaw.(map[string]interface{})
		if !ok {
			continue
		}

		msg := &Message{}

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
		if v, ok := msgMap["producerSub"].(string); ok {
			msg.ProducerSub = v
		}

		messages = append(messages, msg)
	}

	return messages
}
