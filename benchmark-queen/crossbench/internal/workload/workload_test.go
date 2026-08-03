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
		c.ObserveE2E(FlowA, 1000)
	}
	c.ObserveE2E(FlowA, 1000000)
	p50, _, p99 := c.E2E(FlowA)
	if p50 < 0.9 || p50 > 1.3 {
		t.Fatalf("p50 should be ~1ms, got %.3fms", p50)
	}
	if p99 < 1.0 {
		t.Fatalf("p99 should be well above p50, got %.3fms", p99)
	}
}
