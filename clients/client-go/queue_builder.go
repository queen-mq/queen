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
	conflation       bool
	// autopilot is the per-call override for pop autopilot: nil = the client
	// default (on unless EnvPopAutopilot turned it off). batch and
	// maxPartitions stay at 0 when the user never called their setters, and
	// that zero is load-bearing — it is what "let the broker decide this one"
	// looks like all the way down to buildPopParams.
	autopilot *bool
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

// Batch sets the batch size for consume/pop operations, and pins it: an
// explicit size is sent on the wire exactly as before and the broker never
// second-guesses it.
//
// LEAVING IT UNSET IS NOW A CHOICE, not an omission. A builder that never calls
// Batch asks the broker to size the batch (see Autopilot), instead of the
// client-side default of 1 this SDK used to substitute. Batch(0) means the same
// thing as never calling it.
//
// Requires broker >= 1.2 for the unset case; an older broker ignores the
// autopilot parameter and applies its own server-side default (200) instead.
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
// An explicit N is a pin, N=1 included: Partitions(1) holds the claim to a
// single partition for good. LEAVING IT UNSET now hands the sweep width to the
// broker (see Autopilot), where it used to mean the client-side default of 1.
//
// Requires broker >= 1.2 for the unset case; an older broker ignores the
// autopilot parameter and claims 1 partition, which is what unset used to mean.
func (qb *QueueBuilder) Partitions(n int) *QueueBuilder {
	if n < 1 {
		n = 1
	}
	qb.maxPartitions = n
	return qb
}

// Autopilot turns broker-side pop sizing on or off for this builder.
//
// It is ON by default and it only ever touches the knobs you did NOT set:
// with autopilot on, a Batch or Partitions you set explicitly travels on the
// wire unchanged and stays yours, while the one you left alone is omitted and
// the broker picks it from state the client cannot see (ready partitions, the
// age of the oldest ready message, arrival rate).
//
//	client.Queue("events").Group("workers").Partitions(1).  // pinned
//	    Consume(ctx, handler).Execute(ctx)                  // batch: broker's
//
// Autopilot(false) restores this SDK's pre-1.2 behavior byte for byte: the
// client-side defaults come back (batch 1, partitions 1) and no autopilot
// parameter is sent. Use it to pin a workload to the old sizing without
// spelling both knobs out. The QUEEN_SDK_POP_AUTOPILOT=off environment variable
// (see EnvPopAutopilot) does the same for a whole process; an explicit call
// here wins over it in both directions.
//
// Setting BOTH Batch and Partitions leaves autopilot nothing to decide, so no
// autopilot parameter is sent in that case either, whatever this flag says.
func (qb *QueueBuilder) Autopilot(enabled bool) *QueueBuilder {
	qb.autopilot = &enabled
	return qb
}

// autopilotEnabled resolves this builder's autopilot decision: its own
// override if it has one, otherwise the client-wide default settled at New.
func (qb *QueueBuilder) autopilotEnabled() bool {
	if qb.autopilot != nil {
		return *qb.autopilot
	}
	return qb.queen == nil || !qb.queen.autopilotOff
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

// Conflation requests last-value delivery for the consumer group: each pop of a
// partition returns only its NEWEST visible message and commits everything
// below it. Under backlog the consumer runs the handler once per partition
// instead of once per message.
//
// This is a GROUP policy, fixed at the group's first registration and stored by
// the broker — not a per-call flag. A later consumer of the same group that
// declares the opposite is warned once and keeps running on the stored policy;
// to change the policy, delete and recreate the group.
//
//	client.Queue("recompute").Group("workers").Conflation(true).
//	    Consume(ctx, handler).Execute(ctx)
//
// Requires broker >= 1.1.0. Against an older broker the flag is ignored server
// side and the pop returns the full backlog; rather than draining it silently,
// Pop and the consume loop fail with ErrConflationUnsupported on the first
// response.
func (qb *QueueBuilder) Conflation(enabled bool) *QueueBuilder {
	qb.conflation = enabled
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

// PopResult is the full answer to a pop: the messages, plus the additive
// metadata the broker sends alongside them.
//
// Pop returns the messages alone, which is what almost every caller wants. This
// exists for the ones that also want to see what the broker decided.
type PopResult struct {
	// Messages is what Pop returns, unchanged.
	Messages []*Message
	// Autopilot is what the broker chose for this pop, or nil when this pop
	// did not engage autopilot or the broker is older than 1.2.
	Autopilot *AutopilotDecision
}

// Pop pops messages from the queue.
func (qb *QueueBuilder) Pop(ctx context.Context) ([]*Message, error) {
	res, err := qb.PopResult(ctx)
	if err != nil {
		return nil, err
	}
	return res.Messages, nil
}

// PopResult pops messages and reports the broker's autopilot decision with
// them. Identical to Pop on the wire — same request, same error behavior — it
// simply does not throw away the sizing the broker echoed back.
//
//	res, err := client.Queue("events").Group("workers").PopResult(ctx)
//	if res.Autopilot != nil {
//	    log.Printf("broker swept %d partitions for up to %d messages",
//	        res.Autopilot.Partitions, res.Autopilot.Batch)
//	}
func (qb *QueueBuilder) PopResult(ctx context.Context) (PopResult, error) {
	// Validate queue name
	if qb.queueName == "" && qb.namespace == "" && qb.task == "" {
		return PopResult{}, fmt.Errorf("queue name, namespace, or task is required")
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
	// wait=true is a long-poll: on a 429 it should back off and keep waiting
	// rather than give up after the bounded push-like attempt budget.
	var opts []RequestOption
	if *wait {
		timeout += 5000 // Add 5s buffer for long polling
		opts = append(opts, WithLongPollRetry())
	}

	// Make request
	result, err := qb.queen.httpClient.Get(ctx, path, timeout, "", opts...)
	if err != nil {
		return PopResult{}, fmt.Errorf("pop request failed: %w", err)
	}

	// Conflation is verified BEFORE the messages are handed to the caller: a
	// broker that ignored the flag answers with the whole backlog, and returning
	// it "successfully" is exactly the silent degradation this check exists to
	// prevent (PLAN_CONFLATION.md §4).
	if cerr := checkConflationEcho(result, qb.conflation,
		conflationTarget(qb.queueName, qb.namespace, qb.task), qb.consumerGroup); cerr != nil {
		return PopResult{}, cerr
	}

	// Parse response
	messages := parseMessages(result)

	logDebug("QueueBuilder.Pop", map[string]interface{}{
		"queue":   qb.queueName,
		"count":   len(messages),
	})

	return PopResult{Messages: messages, Autopilot: parseAutopilotDecision(result)}, nil
}

// buildPopParams builds the query parameters for pop requests.
//
// THIS BUILDER IS SEPARATE FROM ConsumerManager.buildParams (PLAN_CONFLATION.md
// §4) — any option added here must be added there too. The batch/partitions
// half is the exception, and deliberately so: both builders hand it to
// popSizing.apply, which owns the autopilot emission rule outright so the two
// cannot drift on it.
func (qb *QueueBuilder) buildPopParams() string {
	params := url.Values{}

	// Batch and partitions, and with them the autopilot flag. qb.batch and
	// qb.maxPartitions are the user's own values: 0 means the setter was never
	// called, which is what autopilot acts on.
	popSizing{
		Batch:         qb.batch,
		MaxPartitions: qb.maxPartitions,
		FallbackBatch: PopDefaults.Batch,
		Autopilot:     qb.autopilotEnabled(),
	}.apply(params)

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

	// Conflation: last-value delivery for this group. Emitted ONLY when true,
	// never as conflation=false — a consumer that does not opt in must produce
	// the request it produced before this option existed.
	if qb.conflation {
		params.Set("conflation", "true")
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
	autopilot := qb.autopilotEnabled()

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
		Conflation:       qb.conflation,
		Autopilot:        &autopilot,
	}

	// Apply defaults
	if opts.Concurrency == 0 {
		opts.Concurrency = ConsumeDefaults.Concurrency
	}
	if opts.TimeoutMillis == 0 {
		opts.TimeoutMillis = ConsumeDefaults.TimeoutMillis
	}
	// Batch and MaxPartitions keep their zero when autopilot is on, and the
	// zero has to survive all the way to buildParams: it is the ONLY record
	// that the user said nothing about that dimension. Filling it here — which
	// is what this function did before autopilot — would erase the difference
	// between "never called Batch" and "called Batch(1)" and hand the broker a
	// pin the user never asked for.
	if !autopilot {
		if opts.Batch == 0 {
			opts.Batch = ConsumeDefaults.Batch
		}
		if opts.MaxPartitions < 1 {
			opts.MaxPartitions = 1
		}
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
