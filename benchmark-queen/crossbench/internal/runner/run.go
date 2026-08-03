package runner

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"crossbench/internal/broker"
	"crossbench/internal/verify"
	"crossbench/internal/workload"
)

// Config is one run.
type Config struct {
	Topology workload.Topology
	System   string
	LogDir   string

	DurationSec int
	RampSec     int
	DrainSec    int // grace after the producers stop, for the pipeline to finish
	ReportSec   int

	// LaneHeadroom multiplies the Little's law lane count (SPEC.md §2) to
	// absorb jitter and broker RTT. Identical for every system.
	LaneHeadroom float64

	BatchSize   int
	Prefetch    int
	PushShards  int
	PushChanCap int

	Warmup     bool
	WarmupConc int

	Setup broker.SetupOpts
	Out   io.Writer
}

// DefaultConfig is the campaign's standard run: 5k ev/s for 20 minutes.
func DefaultConfig() Config {
	return Config{
		Topology:     workload.DefaultTopology(),
		LogDir:       "./cmlogs",
		DurationSec:  1200,
		RampSec:      30,
		DrainSec:     60,
		ReportSec:    1,
		LaneHeadroom: 1.5,
		BatchSize:    100,
		PushShards:   64,
		PushChanCap:  1024,
		Warmup:       true,
		WarmupConc:   96,
		Setup:        broker.SetupOpts{Durability: "default", Reset: true},
		Out:          os.Stdout,
	}
}

// Result carries everything a report needs.
type Result struct {
	System      string
	Topology    workload.Topology
	Invariants  workload.Invariants
	Provisioned broker.Provisioned
	Verify      verify.Report
	Counters    *workload.Counters
	Stages      []*workload.StageCounters
	BrokerStats map[string]any
	Elapsed     time.Duration
	SlowDecodes int64
}

// Run executes the whole workload against one broker and verifies it.
func Run(ctx context.Context, b broker.Broker, cfg Config) (*Result, error) {
	out := cfg.Out
	if out == nil {
		out = os.Stdout
	}
	t := cfg.Topology
	inv := t.Invariants()

	fmt.Fprintf(out, "=== CM-BENCH  system=%s  R=%d ev/s  P=%d  duration=%ds ===\n",
		b.Name(), t.RateEvents, t.Properties, cfg.DurationSec)
	fmt.Fprintf(out, "invariants: %d deliveries/s, %d ordered lanes, publishes/s native=%d copied=%d\n",
		inv.DeliveriesPerSec, inv.OrderedLanes, inv.PublishNativeFan, inv.PublishCopiedFan)

	if cfg.Setup.PhysicalLanes == 0 {
		cfg.Setup.PhysicalLanes = t.Properties
	}
	if err := b.Setup(ctx, t, cfg.Setup); err != nil {
		return nil, fmt.Errorf("setup: %w", err)
	}

	recs, err := workload.NewRecorderSet(cfg.LogDir, t)
	if err != nil {
		return nil, fmt.Errorf("recorders: %w", err)
	}
	defer recs.Close()

	counters := &workload.Counters{}

	// ---- stages -----------------------------------------------------------
	stageCtx, stopStages := context.WithCancel(context.Background())
	defer stopStages()

	defs := t.Stages()
	stages := make([]*Stage, 0, len(defs))
	stageStats := make([]*workload.StageCounters, 0, len(defs))
	var stageWg sync.WaitGroup

	for _, def := range defs {
		sc := &workload.StageCounters{Topic: def.Topic, Group: def.Group}
		lanes := laneBudget(t, def, cfg.LaneHeadroom)
		st := NewStage(def, recs.For(def.Topic, def.Group), counters, sc, lanes)
		stages = append(stages, st)
		stageStats = append(stageStats, sc)

		stageWg.Add(1)
		go func(st *Stage) {
			defer stageWg.Done()
			if err := st.Run(stageCtx, b, cfg.BatchSize, cfg.Prefetch); err != nil && stageCtx.Err() == nil {
				fmt.Fprintf(out, "stage %s/%s exited: %v\n", st.Def.Topic, st.Def.Group, err)
			}
		}(st)
	}
	fmt.Fprintf(out, "started %d stages, lane budget %d (headroom %.2fx)\n",
		len(stages), totalLanes(stages), cfg.LaneHeadroom)

	// ---- warm-up ----------------------------------------------------------
	// Pre-hydrate every property through the REAL publish path before the rated
	// window. A channel manager's properties pre-exist; cold mass-creation of
	// lanes under rated load is a measured broker wedge and would be measuring
	// provisioning, not steady state. Everyone gets the same treatment.
	baseSeq := int64(1)
	maxSeqA := workload.NewMaxSeq(t.Properties)
	maxSeqB := workload.NewMaxSeq(t.Properties)
	if cfg.Warmup {
		start := time.Now()
		if err := warmup(ctx, b, t, cfg.WarmupConc, counters, maxSeqA, maxSeqB); err != nil {
			return nil, fmt.Errorf("warmup: %w", err)
		}
		want := int64(t.Properties) * int64(len(defs))
		drained := waitFor(ctx, 120*time.Second, func() bool {
			return counters.Processed.Load() >= want
		})
		fmt.Fprintf(out, "warmup: %d properties x 2 flows in %s, pipeline drained=%v (%d/%d)\n",
			t.Properties, time.Since(start).Round(time.Millisecond), drained,
			counters.Processed.Load(), want)
		baseSeq = 0
	}

	// ---- producers --------------------------------------------------------
	runCtx, stopProducers := context.WithCancel(ctx)
	defer stopProducers()

	seqA := make([]int64, t.Properties)
	seqB := make([]int64, t.Properties)
	pad := workload.RatesPad(t.PayloadB)
	var prodWg sync.WaitGroup

	rateA := t.RateEvents / 2
	rateB := t.RateEvents - rateA
	workload.RunProducer(runCtx, &prodWg, b.Publish, workload.ProducerConfig{
		Topic: t.Topics()[0], Flow: workload.FlowA, Rate: rateA, RampSec: cfg.RampSec,
		Properties: t.Properties, Shards: cfg.PushShards, ChanCap: cfg.PushChanCap,
	}, seqA, counters, maxSeqA)
	workload.RunProducer(runCtx, &prodWg, b.Publish, workload.ProducerConfig{
		Topic: t.Topics()[1], Flow: workload.FlowB, Rate: rateB, RampSec: cfg.RampSec,
		Properties: t.Properties, Shards: cfg.PushShards, ChanCap: cfg.PushChanCap,
		RatesPad: pad,
	}, seqB, counters, maxSeqB)

	// ---- report + wait ----------------------------------------------------
	begin := time.Now()
	repDone := make(chan struct{})
	go func() {
		defer close(repDone)
		report(runCtx, out, counters, stageStats, cfg.ReportSec)
	}()

	select {
	case <-ctx.Done():
	case <-time.After(time.Duration(cfg.DurationSec) * time.Second):
	}
	stopProducers()
	prodWg.Wait()
	<-repDone
	elapsed := time.Since(begin)

	// ---- drain ------------------------------------------------------------
	if cfg.DrainSec > 0 {
		fmt.Fprintf(out, "draining up to %ds...\n", cfg.DrainSec)
		deadline := time.Now().Add(time.Duration(cfg.DrainSec) * time.Second)
		last := counters.Processed.Load()
		stable := 0
		for time.Now().Before(deadline) {
			time.Sleep(time.Second)
			now := counters.Processed.Load()
			if now == last {
				if stable++; stable >= 3 {
					break // nothing moved for 3s: the pipeline is done
				}
			} else {
				stable = 0
			}
			last = now
		}
	}

	stopStages()
	stageWg.Wait()
	recs.Close()

	// ---- verify -----------------------------------------------------------
	var ackErr int64
	for _, sc := range stageStats {
		ackErr += sc.AckErr.Load()
	}
	if err := workload.WriteMeta(cfg.LogDir, maxSeqA, maxSeqB, ackErr, baseSeq, b.Name(), t); err != nil {
		fmt.Fprintf(out, "WARNING: produced.meta: %v\n", err)
	}
	rep, err := verify.Run(cfg.LogDir, verifyStages(t), ackErr, true)
	if err != nil {
		return nil, fmt.Errorf("verify: %w", err)
	}
	rep.Print(out, cfg.LogDir)

	res := &Result{
		System:      b.Name(),
		Topology:    t,
		Invariants:  inv,
		Provisioned: b.Provisioned(),
		Verify:      rep,
		Counters:    counters,
		Stages:      stageStats,
		BrokerStats: b.Stats(),
		Elapsed:     elapsed,
		SlowDecodes: workload.SlowDecodes(),
	}
	printSummary(out, res)
	return res, nil
}

// laneBudget is Little's law for one stage plus the run's headroom. It depends
// only on the workload, never on the broker.
func laneBudget(t workload.Topology, s workload.Stage, headroom float64) int {
	rate := t.RateEvents / 2
	if s.Flow == workload.FlowB {
		rate = t.RateEvents - t.RateEvents/2
	}
	if headroom <= 0 {
		headroom = 1
	}
	n := int(math.Ceil(float64(rate) * s.AvgWorkMs() / 1000.0 * headroom))
	if n < 4 {
		n = 4
	}
	return n
}

func totalLanes(ss []*Stage) int {
	n := 0
	for _, s := range ss {
		n += cap(s.lanes)
	}
	return n
}

func verifyStages(t workload.Topology) []verify.Stage {
	out := make([]verify.Stage, 0, 12)
	for _, s := range t.Stages() {
		out = append(out, verify.Stage{Topic: s.Topic, Group: s.Group, Flow: string(s.Flow)})
	}
	return out
}

// waitFor polls cond until true or the timeout expires.
func waitFor(ctx context.Context, timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
	return cond()
}

// warmup publishes seq 0 for every property on both ingress flows at bounded
// concurrency, through the same Publish path the rated window uses.
func warmup(ctx context.Context, b broker.Broker, t workload.Topology, conc int,
	c *workload.Counters, maxSeqA, maxSeqB []int64) error {

	if conc < 1 {
		conc = 1
	}
	pad := workload.RatesPad(t.PayloadB)
	topics := t.Topics()
	ts := time.Now().UnixMicro()

	var wg sync.WaitGroup
	sem := make(chan struct{}, conc)
	errCh := make(chan error, 1)

	push := func(topic string, flow workload.Flow, prop int, rates []byte, maxSeq []int64) {
		defer wg.Done()
		defer func() { <-sem }()
		p := workload.EncodeIngress(workload.Stamp{Prop: prop, Flow: flow, Seq: 0, TS: ts}, rates)
		if err := b.Publish(ctx, topic, workload.PartitionKey(prop), p); err != nil {
			select {
			case errCh <- err:
			default:
			}
			return
		}
		c.Published.Add(1)
		maxSeq[prop] = 0
	}

	for prop := 0; prop < t.Properties; prop++ {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		wg.Add(1)
		go push(topics[0], workload.FlowA, prop, nil, maxSeqA)

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		wg.Add(1)
		go push(topics[1], workload.FlowB, prop, pad, maxSeqB)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
