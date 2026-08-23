package workload

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestStampFastPathRoundTrip(t *testing.T) {
	before := SlowDecodes()
	in := Stamp{Prop: 731, Flow: FlowB, Seq: 918273645, TS: 1785000000123456}
	p := EncodeIngress(in, RatesPad(2048))
	out, err := DecodeStamp(p)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("round trip lost data: %+v -> %+v", in, out)
	}
	if SlowDecodes() != before {
		t.Fatalf("2KB flow-B payload should decode on the fast path, not unmarshal")
	}
	if len(p) < 2048 {
		t.Fatalf("payload padding short: %d bytes", len(p))
	}
}

func TestStampWireIsValidJSON(t *testing.T) {
	// Postgres-backed brokers parse and store this document; if it is not valid
	// JSON the whole comparison is measuring the wrong thing.
	for _, p := range [][]byte{
		EncodeIngress(Stamp{Prop: 0, Flow: FlowA, Seq: 1, TS: 1}, nil),
		EncodeIngress(Stamp{Prop: 42, Flow: FlowB, Seq: 7, TS: 99}, RatesPad(512)),
		EncodeDerived(Stamp{Prop: 999, Flow: FlowA, Seq: 12345, TS: 1785000000000000}),
	} {
		if !json.Valid(p) {
			t.Fatalf("not valid JSON: %s", p)
		}
	}
}

func TestDecodeStampSlowPathFallback(t *testing.T) {
	// A broker that re-serialises the document reorders keys. The stamp must
	// still decode — just off the fast path, which we count as a finding.
	reordered := []byte(`{"ts":1785000000000000,"seq":5,"flow":"A","prop":17}`)
	before := SlowDecodes()
	out, err := DecodeStamp(reordered)
	if err != nil {
		t.Fatalf("slow path must handle reordered keys: %v", err)
	}
	want := Stamp{Prop: 17, Flow: FlowA, Seq: 5, TS: 1785000000000000}
	if out != want {
		t.Fatalf("got %+v want %+v", out, want)
	}
	if SlowDecodes() != before+1 {
		t.Fatalf("slow decode should have been counted")
	}
}

func TestDecodeStampRejectsGarbage(t *testing.T) {
	if _, err := DecodeStamp([]byte(`{"nope":1}`)); err == nil {
		t.Fatal("expected error on a payload with no stamp")
	}
}

func TestDerivedPayloadDropsPadding(t *testing.T) {
	s := Stamp{Prop: 1, Flow: FlowB, Seq: 2, TS: 3}
	full := EncodeIngress(s, RatesPad(2048))
	derived := EncodeDerived(s)
	if bytes.Contains(derived, []byte("rates")) {
		t.Fatal("derived hop must not re-transmit the rates blob")
	}
	if len(derived) >= len(full) {
		t.Fatalf("derived payload should be far smaller: %d vs %d", len(derived), len(full))
	}
}

// TestInvariantsMatchJulyReference pins the SPEC.md §2 arithmetic to the numbers
// certified by the July 2026 run. If the topology or the work times drift, this
// fails and the spec table has to be re-derived rather than silently rotting.
func TestInvariantsMatchJulyReference(t *testing.T) {
	tp := DefaultTopology()
	tp.RateEvents = 25000
	inv := tp.Invariants()

	if inv.DeliveriesPerSec != 150000 {
		t.Fatalf("deliveries: got %d want 150000", inv.DeliveriesPerSec)
	}
	if inv.OrderedLanes != 4063 { // 0.1625 x 25000, ceil per stage
		t.Fatalf("ordered lanes: got %d want 4063", inv.OrderedLanes)
	}
	if inv.PublishNativeFan != 50000 {
		t.Fatalf("native-fanout publishes: got %d want 50000", inv.PublishNativeFan)
	}
	if inv.PublishCopiedFan != 150000 {
		t.Fatalf("copied-fanout publishes: got %d want 150000", inv.PublishCopiedFan)
	}
	if got := inv.PublishCopiedFan / inv.PublishNativeFan; got != 3 {
		t.Fatalf("materialised fan-out should cost 3x the writes, got %dx", got)
	}
}

func TestInvariantsAtCampaignRate(t *testing.T) {
	tp := DefaultTopology() // R = 5000
	inv := tp.Invariants()
	if inv.DeliveriesPerSec != 30000 {
		t.Fatalf("deliveries: got %d want 30000", inv.DeliveriesPerSec)
	}
	if inv.OrderedLanes != 813 {
		t.Fatalf("ordered lanes: got %d want 813", inv.OrderedLanes)
	}
	// The two fan-out stages dominate: 5 groups x 30ms each, both flows.
	if got := inv.LanesPerStage[TopicOtaSync+"/ota-1"]; got != 75 {
		t.Fatalf("ota-1 lanes: got %d want 75", got)
	}
	if got := inv.LanesPerStage[TopicAvail+"/"+GroupDB]; got != 38 {
		t.Fatalf("cm-db lanes: got %d want 38", got)
	}
}

func TestTopologyShape(t *testing.T) {
	tp := DefaultTopology()
	stages := tp.Stages()
	if len(stages) != 12 {
		t.Fatalf("expected 12 consumer streams, got %d", len(stages))
	}
	var terminal, intermediate int
	for _, s := range stages {
		if s.Terminal() {
			terminal++
		} else {
			intermediate++
		}
	}
	if terminal != 10 || intermediate != 2 {
		t.Fatalf("expected 10 terminal + 2 intermediate, got %d + %d", terminal, intermediate)
	}
	if g := tp.GroupsFor(TopicOtaSync); len(g) != FanOut {
		t.Fatalf("cm-ota-sync should carry %d groups, got %d", FanOut, len(g))
	}
	if len(tp.Topics()) != 4 {
		t.Fatalf("expected 4 topics")
	}
}

func TestTopologyPrefixIsolatesRuns(t *testing.T) {
	tp := DefaultTopology()
	tp.Prefix = "run7-"
	for _, tc := range tp.Topics() {
		if len(tc) < 5 || tc[:5] != "run7-" {
			t.Fatalf("topic %q missing prefix", tc)
		}
	}
	if got := tp.Stages()[0].OutTopic; got != "run7-"+TopicOtaSync {
		t.Fatalf("stage OutTopic not prefixed: %q", got)
	}
}

func TestPartitionKeyStable(t *testing.T) {
	if got := PartitionKey(0); got != "p0" {
		t.Fatalf("got %q", got)
	}
	if got := PartitionKey(999); got != "p999" {
		t.Fatalf("got %q", got)
	}
}

func TestLatencyHistogramPercentiles(t *testing.T) {
	var c Counters
	// 99 observations at 1ms, 1 at 1s: p50 ~1ms, p99 well above.
	for i := 0; i < 99; i++ {
		c.ObserveE2E(FlowA, CohortCold, 1000)
	}
	c.ObserveE2E(FlowA, CohortCold, 1000000)
	p50, _, p99 := c.E2E(FlowA)
	if p50 < 0.9 || p50 > 1.3 {
		t.Fatalf("p50 should be ~1ms, got %.3fms", p50)
	}
	if p99 < 1.0 {
		t.Fatalf("p99 should be well above p50, got %.3fms", p99)
	}
}

// ---------------------------------------------------------------------------
// hot-entity skew
// ---------------------------------------------------------------------------

// The scheduler's whole job is to hand the hot cohort exactly its configured
// share. If it drifts, every isolation number is measured against a workload
// nobody declared, so this is checked exactly rather than approximately.
func TestPropSelectorHotShareIsExact(t *testing.T) {
	const props, hot, factor = 1000, 1, 100
	s := newPropSelector(props, hot, factor)
	total := hot*factor + (props - hot) // one full Bresenham window
	counts := map[int]int{}
	for i := 0; i < total*7; i++ {
		counts[s.next()]++
	}
	if got, want := counts[0], factor*7; got != want {
		t.Fatalf("hot key got %d events over 7 windows, want exactly %d", got, want)
	}
	for p := hot; p < props; p++ {
		if counts[p] != 7 {
			t.Fatalf("cold key %d got %d events, want 7", p, counts[p])
		}
	}
}

// A hot entity delivered in a burst is a buffering test, not a head-of-line
// test. Assert the hot key is spread through the window instead of clumped.
func TestPropSelectorInterleavesHotKey(t *testing.T) {
	s := newPropSelector(1000, 1, 100)
	var maxRun, run int
	for i := 0; i < 1099; i++ {
		if s.next() == 0 {
			run++
			if run > maxRun {
				maxRun = run
			}
		} else {
			run = 0
		}
	}
	if maxRun > 1 {
		t.Fatalf("hot key emitted in runs of %d; expected it interleaved one at a time", maxRun)
	}
}

// Uniform must stay byte-identical to the pre-skew round robin, so a baseline
// cell is the same workload it always was.
func TestPropSelectorUniformIsRoundRobin(t *testing.T) {
	for _, s := range []*propSelector{
		newPropSelector(5, 0, 1),   // no hot keys
		newPropSelector(5, 1, 1),   // factor 1 = no concentration
		newPropSelector(5, 5, 100), // every key hot = no cold cohort
	} {
		for i := 0; i < 12; i++ {
			if got, want := s.next(), i%5; got != want {
				t.Fatalf("uniform selector gave %d, want %d", got, want)
			}
		}
	}
}

func TestCohortAndLaneMath(t *testing.T) {
	tp := DefaultTopology()
	tp.Properties, tp.RateEvents = 1000, 2000
	tp.HotProps, tp.HotFactor = 1, 100

	if tp.CohortOf(0) != CohortHot || tp.CohortOf(1) != CohortCold {
		t.Fatal("cohort boundary is wrong")
	}
	hot, cold := tp.PerLaneRate()
	// flow rate 1000/s over weight 100 + 999 cold: hot ~91/s, cold ~0.91/s.
	if hot < 90 || hot > 92 {
		t.Fatalf("hot lane rate %.2f/s, want ~91", hot)
	}
	if cold < 0.85 || cold > 0.95 {
		t.Fatalf("cold lane rate %.2f/s, want ~0.91", cold)
	}
	// Serial (batch 1) the lane drains at ~33/s; a batch of 10 raises it 10x.
	if ceil := tp.LaneCeiling(1); ceil < 33 || ceil > 34 {
		t.Fatalf("serial lane ceiling %.1f/s, want ~33.3 at 30ms of work", ceil)
	}
	if ceil := tp.LaneCeiling(10); ceil < 333 || ceil > 334 {
		t.Fatalf("batch-10 lane ceiling %.1f/s, want ~333", ceil)
	}
	if !tp.HotSaturated(1) {
		t.Fatal("91/s offered onto a serial 33/s lane must report as saturated")
	}
	if tp.HotSaturated(10) {
		t.Fatal("91/s against a batch-10 ceiling of 333/s is not saturated")
	}
	tp.HotFactor = 10 // ~9.9/s: comfortably under even the serial ceiling
	if tp.HotSaturated(1) {
		t.Fatal("10x should be below the serial lane ceiling")
	}
}

// The flow totals must keep counting every observation regardless of cohort,
// so skew builds stay comparable with pre-skew results.
func TestCohortSplitIsAdditive(t *testing.T) {
	var c Counters
	for i := 0; i < 40; i++ {
		c.ObserveE2E(FlowA, CohortCold, 1000)
	}
	for i := 0; i < 10; i++ {
		c.ObserveE2E(FlowA, CohortHot, 500000)
	}
	if got := c.DeliveredCohort(CohortHot); got != 10 {
		t.Fatalf("hot deliveries %d, want 10", got)
	}
	if got := c.DeliveredCohort(CohortCold); got != 40 {
		t.Fatalf("cold deliveries %d, want 40", got)
	}
	_, _, cold99 := c.E2ECohortBoth(CohortCold)
	_, _, hot99 := c.E2ECohortBoth(CohortHot)
	if cold99 > 2 {
		t.Fatalf("cold p99 %.2fms should stay ~1ms and not absorb hot observations", cold99)
	}
	if hot99 < 100 {
		t.Fatalf("hot p99 %.2fms should reflect the 500ms observations", hot99)
	}
	// p99 of the merged flow-A histogram must see the hot tail.
	if _, _, all99 := c.E2E(FlowA); all99 < 100 {
		t.Fatalf("flow total p99 %.2fms lost the hot tail", all99)
	}
}
