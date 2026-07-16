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
}

// NewOperationBuilder creates a new OperationBuilder.
func NewOperationBuilder(qb *QueueBuilder, operation string) *OperationBuilder {
	return &OperationBuilder{
		qb:        qb,
		operation: operation,
	}
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

	// Add options if provided
	if ob.qb.queueConfig != nil {
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
func (ob *OperationBuilder) buildOptions() map[string]interface{} {
	opts := make(map[string]interface{})
	config := ob.qb.queueConfig

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
	if config.EncryptionEnabled {
		opts["encryptionEnabled"] = config.EncryptionEnabled
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
