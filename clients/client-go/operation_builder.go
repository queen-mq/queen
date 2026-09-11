package queen

import (
	"context"
	"fmt"
	"net/url"
)

// OperationBuilder provides a fluent API for queue create/delete operations.
type OperationBuilder struct {
	qb        *QueueBuilder
	operation string // "create" or "delete"
	// replace asks the broker to REPLACE the whole configuration instead of
	// merging into it. See Replace; read by executeCreate only.
	replace bool
	// options set key by key with Option, overlaid on the QueueConfig bag.
	// Nil unless Option was called. See Option for why it exists.
	rawOptions map[string]interface{}
}

// NewOperationBuilder creates a new OperationBuilder.
func NewOperationBuilder(qb *QueueBuilder, operation string) *OperationBuilder {
	return &OperationBuilder{
		qb:        qb,
		operation: operation,
	}
}

// Replace turns this create/reconfigure into a full replacement of the queue's
// configuration: every option the request does not carry goes back to its
// default, which is what /configure did for every caller before 1.6.0.
//
// WITHOUT IT the call MERGES, and merging is what you want almost always. This
// SDK sends only the options you set on QueueConfig — a zero stays home — so a
//
//	client.Queue("orders").Config(queen.QueueConfig{LeaseTime: 60}).Create()
//
// used to reset the queue's dedup window, its retention and its dead-letter
// policy to the defaults on its way past. Merged, it changes the lease and
// nothing else.
//
// Replace(true) is for a caller whose input IS the whole configuration — a
// declarative manifest, `queenctl apply -f`, a reconciler that reads its desired
// state from a file — where an option the file does not mention genuinely means
// "back to the default".
//
// Requires broker >= 1.6.0. An older broker ignores the flag and replaces in
// both cases, which is what it has always done.
func (ob *OperationBuilder) Replace(enabled bool) *OperationBuilder {
	ob.replace = enabled
	return ob
}

// Option puts one option on the wire literally, whatever its value — including
// the values QueueConfig cannot express.
//
// QueueConfig's fields are plain ints and bools, so buildOptions has to read a
// zero as "not set" and omit it; there is no way to tell `DeadLetterQueue:
// false` from a QueueConfig nobody filled in. Under merge semantics that is the
// difference between "leave the dead-letter policy alone" and "turn it OFF", and
// the second one was unsendable from this SDK: every option could be switched on
// and none could be switched off.
//
//	q.Config(queen.QueueConfig{LeaseTime: 60}).
//		Option("deadLetterQueue", false).      // an explicit false
//		Option("dlqAfterMaxRetries", false).
//		Option("retentionSeconds", 0).         // an explicit zero
//		Option("retentionSinkHold", nil).      // null = back to the default
//		Create().Execute(ctx)
//
// `nil` sends JSON null, which is the broker's own "restore this option's
// default" and is otherwise unreachable from here. Keys set with Option are
// applied AFTER the QueueConfig bag, so they win over it; the spelling is the
// wire's (camelCase, as configure_queue_v1 parses it) and is not validated here.
func (ob *OperationBuilder) Option(key string, value interface{}) *OperationBuilder {
	if ob.rawOptions == nil {
		ob.rawOptions = make(map[string]interface{})
	}
	ob.rawOptions[key] = value
	return ob
}

// Execute executes the operation.
func (ob *OperationBuilder) Execute(ctx context.Context) (map[string]interface{}, error) {
	switch ob.operation {
	case "create":
		return ob.executeCreate(ctx)
	case "delete":
		return ob.executeDelete(ctx)
	default:
		return nil, fmt.Errorf("unknown operation: %s", ob.operation)
	}
}

// executeCreate creates the queue.
func (ob *OperationBuilder) executeCreate(ctx context.Context) (map[string]interface{}, error) {
	// Validate queue name
	if ob.qb.queueName == "" {
		return nil, fmt.Errorf("queue name is required")
	}

	name, err := ValidateQueueName(ob.qb.queueName)
	if err != nil {
		return nil, err
	}

	// Build request
	req := configureRequest{
		Queue:     name,
		Namespace: ob.qb.namespace,
		Task:      ob.qb.task,
	}
	// Only the replacing half is ever spelled out: "merge" is the broker's
	// default, and an absent key keeps this request identical to the one every
	// released version of this SDK sends.
	if ob.replace {
		req.Mode = "replace"
	}

	// Add options if provided. Option() keys are overlaid last, so an explicit
	// false / 0 / null survives buildOptions' omit-the-zero rule.
	if ob.qb.queueConfig != nil || ob.rawOptions != nil {
		req.Options = ob.buildOptions()
	}

	// Make request
	result, err := ob.qb.queen.httpClient.Post(ctx, "/api/v1/configure", req)
	if err != nil {
		return nil, fmt.Errorf("create queue failed: %w", err)
	}

	logInfo("OperationBuilder.executeCreate", map[string]interface{}{
		"queue":     name,
		"namespace": ob.qb.namespace,
		"task":      ob.qb.task,
	})

	// Add configured flag for compatibility with Python/JS clients
	if result == nil {
		result = make(map[string]interface{})
	}
	result["configured"] = true

	return result, nil
}

// buildOptions builds the options map from QueueConfig.
//
// A zero (and a false) is OMITTED, which is why Replace exists: on a merging
// call — the default — an omitted option means "leave this queue's value
// alone", so a config that sets two fields edits two fields. On a replacing
// call it means "put this option back to its default", so the same two-field
// config resets the other nineteen. Neither reading is wrong; they are the
// difference between an edit and a manifest, and Replace is where a caller says
// which one it is holding.
func (ob *OperationBuilder) buildOptions() map[string]interface{} {
	opts := make(map[string]interface{})
	config := ob.qb.queueConfig
	if config == nil {
		config = &QueueConfig{}
	}

	if config.LeaseTime > 0 {
		opts["leaseTime"] = config.LeaseTime
	}
	if config.RetryLimit > 0 {
		opts["retryLimit"] = config.RetryLimit
	}
	if config.Priority != 0 {
		opts["priority"] = config.Priority
	}
	if config.DelayedProcessing > 0 {
		opts["delayedProcessing"] = config.DelayedProcessing
	}
	if config.WindowBuffer > 0 {
		opts["windowBuffer"] = config.WindowBuffer
	}
	if config.MaxSize > 0 {
		opts["maxSize"] = config.MaxSize
	}
	if config.RetentionSeconds > 0 {
		opts["retentionSeconds"] = config.RetentionSeconds
	}
	if config.CompletedRetentionSeconds > 0 {
		opts["completedRetentionSeconds"] = config.CompletedRetentionSeconds
	}
	if config.RetentionEnabled {
		opts["retentionEnabled"] = config.RetentionEnabled
	}
	if config.DeadLetterQueue {
		opts["deadLetterQueue"] = config.DeadLetterQueue
	}
	if config.DlqAfterMaxRetries {
		opts["dlqAfterMaxRetries"] = config.DlqAfterMaxRetries
	}
	if config.EncryptionEnabled {
		opts["encryptionEnabled"] = config.EncryptionEnabled
	}

	// Last, and therefore authoritative: a caller who spelled an option out with
	// Option meant that value, zero and false and null included.
	for k, v := range ob.rawOptions {
		opts[k] = v
	}

	return opts
}

// executeDelete deletes the queue.
func (ob *OperationBuilder) executeDelete(ctx context.Context) (map[string]interface{}, error) {
	// Validate queue name
	if ob.qb.queueName == "" {
		return nil, fmt.Errorf("queue name is required")
	}

	name, err := ValidateQueueName(ob.qb.queueName)
	if err != nil {
		return nil, err
	}

	// Build path
	path := fmt.Sprintf("/api/v1/resources/queues/%s", url.PathEscape(name))

	// Make request
	result, err := ob.qb.queen.httpClient.Delete(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("delete queue failed: %w", err)
	}

	logInfo("OperationBuilder.executeDelete", map[string]interface{}{
		"queue": name,
	})

	return result, nil
}
