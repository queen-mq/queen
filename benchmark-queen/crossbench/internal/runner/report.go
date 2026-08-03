package runner

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"crossbench/internal/workload"
)

type snapshot struct {
	offered, published, derived, delivered, processed, acked int64
	shed, pushErr, ackErr                                    int64
}

func take(c *workload.Counters, stages []*workload.StageCounters) snapshot {
	s := snapshot{
		offered:   c.Offered.Load(),
		published: c.Published.Load(),
		derived:   c.Derived.Load(),
		delivered: c.Delivered.Load(),
		processed: c.Processed.Load(),
		shed:      c.Shed.Load(),
		pushErr:   c.PushErr.Load(),
	}
	for _, sc := range stages {
		s.acked += sc.Acked.Load()
		s.ackErr += sc.AckErr.Load()
	}
	return s
}

// report prints one line per interval with per-second deltas. Read these
// series, not the final totals: the totals include teardown, where drain errors
// land in a lump and misrepresent steady state.
func report(ctx context.Context, w io.Writer, c *workload.Counters,
	stages []*workload.StageCounters, everySec int) {

	if everySec <= 0 {
		everySec = 1
	}
	tk := time.NewTicker(time.Duration(everySec) * time.Second)
	defer tk.Stop()

	prev := take(c, stages)
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		cur := take(c, stages)
		d := float64(everySec)
		el := int(time.Since(start).Seconds())

		// Backlog: every ingress message is consumed once by its intermediate
		// group, and every derived message is consumed FanOut times.
		expected := cur.published + int64(workload.FanOut)*cur.derived
		lag := expected - cur.processed
		if lag < 0 {
			lag = 0
		}
		p50a, _, p99a := c.E2E(workload.FlowA)
		p50b, _, p99b := c.E2E(workload.FlowB)

		fmt.Fprintf(w, "t=%4d  off=%6.0f pub=%6.0f der=%6.0f del=%7.0f proc=%7.0f ack=%7.0f  lag=%8d  shed=%d pushErr=%d ackErr=%d  e2eA p50=%.0f p99=%.0f  e2eB p50=%.0f p99=%.0f\n",
			el,
			float64(cur.offered-prev.offered)/d,
			float64(cur.published-prev.published)/d,
			float64(cur.derived-prev.derived)/d,
			float64(cur.delivered-prev.delivered)/d,
			float64(cur.processed-prev.processed)/d,
			float64(cur.acked-prev.acked)/d,
			lag,
			cur.shed-prev.shed,
			cur.pushErr-prev.pushErr,
			cur.ackErr-prev.ackErr,
			p50a, p99a, p50b, p99b,
		)
		prev = cur
	}
}

// printSummary writes the run's contribution to the cost table (SPEC.md §6.1).
// It prints what the harness knows; CPU, RSS and disk come from the 1 Hz VM
// samplers and are joined in afterwards.
func printSummary(w io.Writer, r *Result) {
	c := r.Counters
	secs := r.Elapsed.Seconds()
	if secs <= 0 {
		secs = 1
	}

	fmt.Fprintf(w, "\n=== SUMMARY  system=%s  R=%d  P=%d  elapsed=%s ===\n",
		r.System, r.Topology.RateEvents, r.Topology.Properties, r.Elapsed.Round(time.Second))

	fmt.Fprintf(w, "\n-- workload demand (broker-independent) --\n")
	fmt.Fprintf(w, "  deliveries/s required      %d\n", r.Invariants.DeliveriesPerSec)
	fmt.Fprintf(w, "  ordered lanes required     %d\n", r.Invariants.OrderedLanes)

	fmt.Fprintf(w, "\n-- what actually flowed --\n")
	fmt.Fprintf(w, "  offered        %12d  (%.0f/s)\n", c.Offered.Load(), float64(c.Offered.Load())/secs)
	fmt.Fprintf(w, "  published      %12d  (%.0f/s)\n", c.Published.Load(), float64(c.Published.Load())/secs)
	fmt.Fprintf(w, "  derived        %12d  (%.0f/s)\n", c.Derived.Load(), float64(c.Derived.Load())/secs)
	fmt.Fprintf(w, "  delivered      %12d  (%.0f/s)\n", c.Delivered.Load(), float64(c.Delivered.Load())/secs)
	fmt.Fprintf(w, "  processed      %12d  (%.0f/s)\n", c.Processed.Load(), float64(c.Processed.Load())/secs)
	fmt.Fprintf(w, "  shed           %12d\n", c.Shed.Load())
	fmt.Fprintf(w, "  pushErr        %12d   pushRetry %d\n", c.PushErr.Load(), c.PushRetry.Load())

	var acked, ackErr, dups int64
	for _, sc := range r.Stages {
		acked += sc.Acked.Load()
		ackErr += sc.AckErr.Load()
		dups += sc.Dups.Load()
	}
	fmt.Fprintf(w, "  acked          %12d   ackErr %d\n", acked, ackErr)
	fmt.Fprintf(w, "  broker-reported redeliveries %d\n", dups)
	if r.SlowDecodes > 0 {
		fmt.Fprintf(w, "  NOTE: %d payloads missed the stamp fast path — the system re-serialised the document\n", r.SlowDecodes)
	}

	p50a, p95a, p99a := c.E2E(workload.FlowA)
	p50b, p95b, p99b := c.E2E(workload.FlowB)
	fmt.Fprintf(w, "\n-- end-to-end latency (CO-corrected, terminal stages) --\n")
	fmt.Fprintf(w, "  flow A  p50 %.1f ms   p95 %.1f ms   p99 %.1f ms\n", p50a, p95a, p99a)
	fmt.Fprintf(w, "  flow B  p50 %.1f ms   p95 %.1f ms   p99 %.1f ms\n", p50b, p95b, p99b)

	p := r.Provisioned
	fmt.Fprintf(w, "\n-- cost to serve: what this system needed --\n")
	fmt.Fprintf(w, "  ordered lanes provisioned  %d\n", p.OrderedLanes)
	fmt.Fprintf(w, "  physical queues/topics     %d\n", p.PhysicalQueues)
	fmt.Fprintf(w, "  consumer members           %d\n", p.ConsumerMembers)
	fmt.Fprintf(w, "  connections                %d\n", p.Connections)
	fmt.Fprintf(w, "  publishes / ingress event  %.1f\n", p.PublishesPerIngressEvent)
	if len(p.BuiltSemantics) == 0 {
		fmt.Fprintf(w, "  semantics built in the app none\n")
	} else {
		fmt.Fprintf(w, "  semantics built in the app:\n")
		for _, s := range p.BuiltSemantics {
			fmt.Fprintf(w, "      - %s\n", s)
		}
	}

	if len(r.BrokerStats) > 0 {
		fmt.Fprintf(w, "\n-- adapter counters --\n")
		keys := make([]string, 0, len(r.BrokerStats))
		for k := range r.BrokerStats {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "  %-26s %v\n", k, r.BrokerStats[k])
		}
	}

	fmt.Fprintf(w, "\n-- per-stage entry age (ms from producer schedule; decomposes e2e) --\n")
	fmt.Fprintf(w, "  %-30s %9s %9s %9s\n", "stage", "p50", "p95", "p99")
	for _, sc := range r.Stages {
		fmt.Fprintf(w, "  %-30s %9.1f %9.1f %9.1f\n",
			sc.Topic+"/"+sc.Group, sc.AgePct(0.50), sc.AgePct(0.95), sc.AgePct(0.99))
	}

	fmt.Fprintf(w, "\n-- worker cycle phases, p50/p95 ms (popRTT + barrier + push + ackDisp ~= cycle) --\n")
	fmt.Fprintf(w, "  %-30s %15s %15s %15s %15s\n", "stage", "popRTT", "barrier", "push", "ackDisp")
	for _, sc := range r.Stages {
		a50, b50, c50, d50 := sc.PhasePct(0.50)
		a95, b95, c95, d95 := sc.PhasePct(0.95)
		fmt.Fprintf(w, "  %-30s %6.1f / %6.1f %6.1f / %6.1f %6.1f / %6.1f %6.1f / %6.1f\n",
			sc.Topic+"/"+sc.Group, a50, a95, b50, b95, c50, c95, d50, d95)
	}

	fmt.Fprintf(w, "\n-- correctness --\n")
	fmt.Fprintf(w, "  gaps %d   order violations %d   dups %d   in-flight at cutoff %d\n",
		r.Verify.Gaps, r.Verify.Viols, r.Verify.Dups, r.Verify.Inflight)
	if r.Verify.Pass {
		fmt.Fprintf(w, "  VERDICT: PASS\n")
	} else {
		fmt.Fprintf(w, "  VERDICT: FAIL\n")
	}
}
