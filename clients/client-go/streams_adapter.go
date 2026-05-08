package queen

import (
	"context"

	"github.com/smartpricing/queen/clients/client-go/streams/runtime"
)

// streamSourceAdapter wraps a *QueueBuilder so it satisfies the
// runtime.Source interface used by the streaming engine. The streams runtime
// can't import the root "queen" package directly (it would cause an import
// cycle), so this adapter lives in the root package and is bound to a
// QueueBuilder via .AsStreamSource().
type streamSourceAdapter struct {
	qb *QueueBuilder
}

func (a *streamSourceAdapter) Name() string { return a.qb.Name() }

func (a *streamSourceAdapter) Batch(n int) runtime.Source {
	return &streamSourceAdapter{qb: a.qb.Batch(n)}
}
func (a *streamSourceAdapter) Wait(b bool) runtime.Source {
	return &streamSourceAdapter{qb: a.qb.Wait(b)}
}
func (a *streamSourceAdapter) TimeoutMillis(ms int) runtime.Source {
	return &streamSourceAdapter{qb: a.qb.TimeoutMillis(ms)}
}
func (a *streamSourceAdapter) Group(g string) runtime.Source {
	return &streamSourceAdapter{qb: a.qb.Group(g)}
}
func (a *streamSourceAdapter) Partitions(n int) runtime.Source {
	return &streamSourceAdapter{qb: a.qb.Partitions(n)}
}
func (a *streamSourceAdapter) SubscriptionMode(m string) runtime.Source {
	return &streamSourceAdapter{qb: a.qb.SubscriptionMode(m)}
}
func (a *streamSourceAdapter) SubscriptionFrom(t string) runtime.Source {
	return &streamSourceAdapter{qb: a.qb.SubscriptionFrom(t)}
}

func (a *streamSourceAdapter) Pop(ctx context.Context) ([]runtime.Message, error) {
	msgs, err := a.qb.Pop(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]runtime.Message, len(msgs))
	for i, m := range msgs {
		out[i] = runtime.Message{
			// queen.Message has no separate ID — use TransactionID as the
			// stable identifier (Queen guarantees its uniqueness).
			ID:            m.TransactionID,
			Data:          coerceData(m.Data),
			CreatedAt:     m.CreatedAt,
			TransactionID: m.TransactionID,
			LeaseID:       m.LeaseID,
			Partition:     m.Partition,
			PartitionID:   m.PartitionID,
		}
	}
	return out, nil
}

func coerceData(v interface{}) map[string]interface{} {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}
	// Other shapes (e.g. encrypted bytes) are returned wrapped under a
	// well-known key so downstream operators can still reason about them.
	return map[string]interface{}{"_raw": v}
}

// AsStreamSource returns a runtime.Source backed by this QueueBuilder so it
// can be passed directly to streams.From(...).
func (qb *QueueBuilder) AsStreamSource() runtime.Source {
	return &streamSourceAdapter{qb: qb}
}
