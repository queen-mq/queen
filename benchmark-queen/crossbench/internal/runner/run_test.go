package runner

import (
	"context"
	"io"
	"testing"
	"time"

	"crossbench/internal/broker"
	"crossbench/internal/broker/mem"
	"crossbench/internal/workload"
)

// shortRun is a fast but structurally complete run: both flows, all three hops,
// all twelve streams, warm-up, pacer, verifier.
func shortRun(t *testing.T, props, rate, durSec int) *Result {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Topology.Properties = props
	cfg.Topology.RateEvents = rate
	cfg.Topology.PayloadB = 512
	cfg.LogDir = t.TempDir()
	cfg.DurationSec = durSec
	cfg.RampSec = 0
	cfg.DrainSec = 20
	cfg.ReportSec = 1
	cfg.PushShards = 8
	cfg.WarmupConc = 32
	cfg.Out = io.Discard

	b := mem.New()
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	res, err := Run(ctx, b, cfg)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return res
}

// TestHarnessIsCleanAgainstPerfectBroker is the control of the whole campaign.
// The in-memory broker delivers exactly once in strict per-key order, so ANY
// gap, duplicate or ordering violation here is the harness's fault, not a
// system's. Every later result depends on this passing.
func TestHarnessIsCleanAgainstPerfectBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("timed run")
	}
	res := shortRun(t, 20, 200, 4)

	if res.Verify.Gaps != 0 {
		t.Errorf("harness manufactured %d gaps against a lossless broker", res.Verify.Gaps)
	}
	if res.Verify.Dups != 0 {
		t.Errorf("harness manufactured %d duplicates against an exactly-once broker", res.Verify.Dups)
	}
	if res.Verify.Viols != 0 {
		t.Errorf("harness manufactured %d order violations against an ordered broker", res.Verify.Viols)
	}
	if !res.Verify.Pass {
		t.Fatalf("control run must PASS, got FAIL (gaps=%d viols=%d)", res.Verify.Gaps, res.Verify.Viols)
	}
	if len(res.Verify.Stages) != 12 {
		t.Fatalf("expected 12 verified streams, got %d", len(res.Verify.Stages))
	}
	for _, s := range res.Verify.Stages {
		if s.Msgs == 0 {
			t.Errorf("stream %s recorded nothing — the topology is not fully wired", s.File)
		}
	}
}

// TestFanOutIsRealNotAliased checks that the five terminal groups each received
// their OWN copy of every event. If a system (or the harness) aliased the
// groups, per-group counts would diverge or collapse.
func TestFanOutIsRealNotAliased(t *testing.T) {
	if testing.Short() {
		t.Skip("timed run")
	}
	res := shortRun(t, 10, 120, 4)

	byStream := map[string]int64{}
	for _, s := range res.Verify.Stages {
		byStream[s.File] = s.Unique
	}
	var otaCounts []int64
	for name, n := range byStream {
		if len(name) > 8 && name[:12] == "cm-ota-sync_" {
			otaCounts = append(otaCounts, n)
		}
	}
	if len(otaCounts) != workload.FanOut {
		t.Fatalf("expected %d cm-ota-sync groups, got %d", workload.FanOut, len(otaCounts))
	}
	// All five groups consume the same stream, so their unique counts must be
	// within a small tail of each other (the run cuts off mid-flight).
	min, max := otaCounts[0], otaCounts[0]
	for _, c := range otaCounts {
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
	}
	if min == 0 {
		t.Fatalf("a fan-out group received nothing: %v", otaCounts)
	}
	if float64(max-min)/float64(max) > 0.25 {
		t.Errorf("fan-out groups diverged too much: %v", otaCounts)
	}
}

// TestOfferedRateIsHonoured guards the open-loop pacer: the rig must offer what
// it promised, or every throughput number in the campaign is meaningless.
func TestOfferedRateIsHonoured(t *testing.T) {
	if testing.Short() {
		t.Skip("timed run")
	}
	const rate, dur = 400, 5
	res := shortRun(t, 40, rate, dur)

	offered := res.Counters.Offered.Load()
	want := int64(rate * dur)
	lo, hi := int64(float64(want)*0.85), int64(float64(want)*1.15)
	if offered < lo || offered > hi {
		t.Errorf("offered %d events, expected ~%d (%d..%d)", offered, want, lo, hi)
	}
	if shed := res.Counters.Shed.Load(); shed > 0 {
		t.Errorf("control run should never shed, got %d", shed)
	}
}

// TestNoAckMeansRedeliveryNotLoss proves the failure path: a handler that
// returns an error must NOT have its batch acked, so the messages come back
// rather than vanishing. This is what keeps a mid-run abort from reading as
// data loss in the verifier.
func TestNoAckMeansRedeliveryNotLoss(t *testing.T) {
	b := mem.New()
	tp := workload.DefaultTopology()
	tp.Properties = 2
	ctx := context.Background()
	if err := b.Setup(ctx, tp, broker.SetupOpts{PhysicalLanes: tp.Properties}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	topic := tp.Topics()[0]
	for seq := int64(1); seq <= 3; seq++ {
		p := workload.EncodeIngress(workload.Stamp{Prop: 1, Flow: workload.FlowA, Seq: seq, TS: 1}, nil)
		if err := b.Publish(ctx, topic, "p1", p); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstSeqs, secondSeqs []int64
	attempt := 0
	stats := &workload.StageCounters{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = b.Consume(cctx, topic, workload.GroupDB,
			broker.ConsumeOpts{Lanes: 4, BatchSize: 10, Stats: stats},
			func(ctx context.Context, batch *broker.Batch) error {
				attempt++
				for _, m := range batch.Msgs {
					if attempt == 1 {
						firstSeqs = append(firstSeqs, m.Stamp.Seq)
					} else {
						secondSeqs = append(secondSeqs, m.Stamp.Seq)
					}
				}
				if attempt == 1 {
					return errAborted // refuse the batch
				}
				cancel()
				return nil
			})
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("consume did not settle")
	}

	if len(firstSeqs) == 0 {
		t.Fatal("nothing was delivered on the first attempt")
	}
	if len(secondSeqs) < len(firstSeqs) {
		t.Fatalf("refused batch was not redelivered: first=%v second=%v", firstSeqs, secondSeqs)
	}
	for i := range firstSeqs {
		if secondSeqs[i] != firstSeqs[i] {
			t.Fatalf("redelivery reordered the batch: first=%v second=%v", firstSeqs, secondSeqs)
		}
	}
}
