// Gate / rate-limiter scenarios. Cross-language equivalent of the Python /
// JS rate-limiter integration coverage. Validates token-bucket gating with
// FIFO ordering across the deny -> lease-expiry -> redeliver loop.
package streams_integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/smartpricing/queen/clients/client-go/streams"
	"github.com/smartpricing/queen/clients/client-go/streams/helpers"
)

// TestGateTokenBucketBasic validates that a token-bucket gate lets every
// pushed message through (eventually) and preserves per-tenant FIFO order.
func TestGateTokenBucketBasic(t *testing.T) {
	src := mkName("gateTokenBucketBasic", "src")
	sink := mkName("gateTokenBucketBasic", "sink")
	queryID := mkName("gateTokenBucketBasic", "q")

	_, _ = testClient.Queue(src).Config(queen.QueueConfig{
		LeaseTime: 2, RetryLimit: 100, RetentionSeconds: 3600, CompletedRetentionSeconds: 3600,
	}).Create().Execute(context.Background())
	_, _ = testClient.Queue(sink).Config(queen.QueueConfig{
		LeaseTime: 30, RetentionSeconds: 3600, CompletedRetentionSeconds: 3600,
	}).Create().Execute(context.Background())

	const totalPerTenant = 30
	tenants := []string{"tenant-0", "tenant-1"}
	for _, t1 := range tenants {
		for seq := 0; seq < totalPerTenant; seq++ {
			_, _ = testClient.Queue(src).Partition(t1).Push(map[string]interface{}{
				"tenantId": t1, "seq": seq,
			}).Execute(context.Background())
		}
	}

	gateFn := helpers.TokenBucketGate(helpers.TokenBucketGateOptions{
		Capacity: 10, RefillPerSec: 20,
	})
	stream := streams.From(testClient.Queue(src).AsStreamSource()).
		Gate(gateFn).
		To(testClient.Queue(sink))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := stream.Run(ctx, streams.RunOptions{
		QueryID: queryID, URL: queenURL, BatchSize: 10, MaxPartitions: 2,
		MaxWaitMillis: 500, Reset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	expected := totalPerTenant * len(tenants)
	var drained int64
	deadline := time.Now().Add(20 * time.Second)
	cg := "rl-test"
	arrivals := make(map[string][]int)
	for atomic.LoadInt64(&drained) < int64(expected) && time.Now().Before(deadline) {
		batch, err := testClient.Queue(sink).Group(cg).Batch(50).Wait(true).TimeoutMillis(500).Pop(context.Background())
		if err != nil || len(batch) == 0 {
			continue
		}
		for _, m := range batch {
			tid, _ := m.Data["tenantId"].(string)
			seqF, _ := m.Data["seq"].(float64)
			arrivals[tid] = append(arrivals[tid], int(seqF))
		}
		atomic.AddInt64(&drained, int64(len(batch)))
		_, _ = testClient.Ack(context.Background(), batch, true, queen.AckOptions{ConsumerGroup: cg})
	}

	got := atomic.LoadInt64(&drained)
	if got != int64(expected) {
		t.Fatalf("expected %d drained, got %d", expected, got)
	}

	// FIFO per-tenant
	for tid, seqs := range arrivals {
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Fatalf("FIFO broken for %s: %v", tid, seqs)
			}
		}
	}
	m := r.Metrics()
	if m.GateAllowsTotal != int64(expected) {
		t.Fatalf("expected gateAllows=%d, got %d", expected, m.GateAllowsTotal)
	}
}
