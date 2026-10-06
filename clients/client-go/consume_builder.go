package queen

import (
	"context"
	"errors"
)

// ErrCommitOnDeliveryConsume is what Execute returns, before any request, for a
// builder with CommitOnDelivery(true): a consumer always leases its messages.
var ErrCommitOnDeliveryConsume = errors.New("CommitOnDelivery is a Pop option; Consume always leases its messages")

// ConsumeBuilder provides a fluent API for starting consumers.
type ConsumeBuilder struct {
	qb           *QueueBuilder
	handler      MessageHandler
	batchHandler BatchMessageHandler
	isBatch      bool
}

// NewConsumeBuilder creates a new ConsumeBuilder for single message handling.
func NewConsumeBuilder(qb *QueueBuilder, handler MessageHandler) *ConsumeBuilder {
	return &ConsumeBuilder{
		qb:      qb,
		handler: handler,
		isBatch: false,
	}
}

// NewConsumeBatchBuilder creates a new ConsumeBuilder for batch message handling.
func NewConsumeBatchBuilder(qb *QueueBuilder, handler BatchMessageHandler) *ConsumeBuilder {
	return &ConsumeBuilder{
		qb:           qb,
		batchHandler: handler,
		isBatch:      true,
	}
}

// Execute starts the consumer and blocks until completion.
func (cb *ConsumeBuilder) Execute(ctx context.Context) error {
	if cb.qb.commitOnDelivery {
		return ErrCommitOnDeliveryConsume
	}
	opts := cb.qb.getConsumeOptions()

	// Create consumer manager
	cm := NewConsumerManager(cb.qb.queen.httpClient, cb.qb.queen)

	// Start consuming
	if cb.isBatch {
		return cm.StartBatch(ctx, cb.batchHandler, opts)
	}
	return cm.Start(ctx, cb.handler, opts)
}

// Start is an alias for Execute (matches Python/JS API).
func (cb *ConsumeBuilder) Start(ctx context.Context) error {
	return cb.Execute(ctx)
}
