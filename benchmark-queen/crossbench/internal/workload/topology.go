// Package workload holds the broker-independent half of CM-BENCH: the topology,
// the event stamp, the open-loop pacer, the per-stage recorder and the run meta.
//
// Nothing in this package may import a broker client. Everything a system under
// test needs is behind the broker.Broker interface, so all four systems are
// driven by byte-identical workload code — the single biggest fairness lever we
// have (SPEC.md §5.5).
package workload

import (
	"math"
	"strconv"
)

// Logical topic names. A run may prefix them (see Topology.Prefix) so repeated
// local runs don't inherit each other's backlog.
const (
	TopicAvail     = "cm-avail"
	TopicPrices    = "cm-prices"
	TopicOtaSync   = "cm-ota-sync"
	TopicOtaPrices = "cm-ota-prices"

	GroupDB  = "cm-db"  // intermediate, flow A
	GroupCal = "cm-cal" // intermediate, flow B

	FanOut = 5 // ota-1..ota-5 and otap-1..otap-5
)

// Flow identifies one of the two independent pipelines.
type Flow string

const (
	FlowA Flow = "A" // availability, small payload
	FlowB Flow = "B" // prices, ~2KB payload
)

// Stage describes one consumer stream: a (topic, group) pair, its per-message
// work time, and where it re-publishes to (empty for terminal stages).
type Stage struct {
	Topic     string
	Group     string
	Flow      Flow
	WorkMinMs int
	WorkMaxMs int    // == WorkMinMs for a fixed cost
	OutTopic  string // "" = terminal stage, records only
}

// Terminal reports whether the stage is a leaf (no re-publish).
func (s Stage) Terminal() bool { return s.OutTopic == "" }

// AvgWorkMs is the mean simulated service time of one message at this stage.
func (s Stage) AvgWorkMs() float64 { return float64(s.WorkMinMs+s.WorkMaxMs) / 2 }

// Topology is the full workload shape. Zero value is not usable; use Default.
type Topology struct {
	Prefix     string // optional name prefix on all four topics
	Properties int    // P: ordering-key cardinality
	RateEvents int    // R: total offered events/s, split 50/50 across flows
	PayloadB   int    // flow-B rates padding target, bytes

	DBSleepMinMs int
	DBSleepMaxMs int
	CalSleepMs   int
	OtaSleepMs   int
}

// DefaultTopology is the shape of SPEC.md §1: the July channel-manager run.
func DefaultTopology() Topology {
	return Topology{
		Properties:   1000,
		RateEvents:   5000,
		PayloadB:     2048,
		DBSleepMinMs: 10,
		DBSleepMaxMs: 20,
		CalSleepMs:   10,
		OtaSleepMs:   30,
	}
}

func (t Topology) topic(name string) string { return t.Prefix + name }

// Topics returns the four logical topics, prefixed.
func (t Topology) Topics() []string {
	return []string{
		t.topic(TopicAvail), t.topic(TopicPrices),
		t.topic(TopicOtaSync), t.topic(TopicOtaPrices),
	}
}

// Stages returns all 12 consumer streams in a stable order: the two
// intermediates first, then the two fan-outs. Terminal stages have OutTopic "".
func (t Topology) Stages() []Stage {
	out := []Stage{
		{
			Topic: t.topic(TopicAvail), Group: GroupDB, Flow: FlowA,
			WorkMinMs: t.DBSleepMinMs, WorkMaxMs: t.DBSleepMaxMs,
			OutTopic: t.topic(TopicOtaSync),
		},
		{
			Topic: t.topic(TopicPrices), Group: GroupCal, Flow: FlowB,
			WorkMinMs: t.CalSleepMs, WorkMaxMs: t.CalSleepMs,
			OutTopic: t.topic(TopicOtaPrices),
		},
	}
	for i := 1; i <= FanOut; i++ {
		out = append(out, Stage{
			Topic: t.topic(TopicOtaSync), Group: groupName("ota-", i), Flow: FlowA,
			WorkMinMs: t.OtaSleepMs, WorkMaxMs: t.OtaSleepMs,
		})
	}
	for i := 1; i <= FanOut; i++ {
		out = append(out, Stage{
			Topic: t.topic(TopicOtaPrices), Group: groupName("otap-", i), Flow: FlowB,
			WorkMinMs: t.OtaSleepMs, WorkMaxMs: t.OtaSleepMs,
		})
	}
	return out
}

func groupName(prefix string, i int) string {
	return prefix + strconv.Itoa(i)
}

// GroupsFor returns the consumer groups subscribed to one topic. A system
// without native consumer groups has to materialise one physical queue per
// entry here — that multiplication is exactly the fan-out cost of SPEC.md §2.
func (t Topology) GroupsFor(topic string) []string {
	var gs []string
	for _, s := range t.Stages() {
		if s.Topic == topic {
			gs = append(gs, s.Group)
		}
	}
	return gs
}

// ---------------------------------------------------------------------------
// derived invariants (SPEC.md §2)
// ---------------------------------------------------------------------------

// Invariants are the workload's hardware- and broker-independent demands. Every
// system must meet the same ones; how much it costs to meet them is the result.
type Invariants struct {
	RateEvents       int // R
	RatePerFlow      int // R/2
	DeliveriesPerSec int // R × (1+FanOut)
	OrderedLanes     int // Little's law over the work sleeps
	PublishNativeFan int // 2R   — systems with real consumer groups
	PublishCopiedFan int // 6R   — systems that materialise fan-out
	LanesPerStage    map[string]int
}

// Invariants computes SPEC.md §2 from the topology. The lane count is Little's
// law on the simulated work alone (broker RTT excluded), which is why it is
// broker-independent and cannot be argued with.
func (t Topology) Invariants() Invariants {
	rateA := t.RateEvents / 2
	rateB := t.RateEvents - rateA

	inv := Invariants{
		RateEvents:       t.RateEvents,
		RatePerFlow:      rateA,
		DeliveriesPerSec: t.RateEvents * (1 + FanOut),
		PublishNativeFan: 2 * t.RateEvents,
		PublishCopiedFan: t.RateEvents * (1 + FanOut),
		LanesPerStage:    map[string]int{},
	}
	for _, s := range t.Stages() {
		rate := rateA
		if s.Flow == FlowB {
			rate = rateB
		}
		lanes := int(math.Ceil(float64(rate) * s.AvgWorkMs() / 1000.0))
		inv.LanesPerStage[s.Topic+"/"+s.Group] = lanes
		inv.OrderedLanes += lanes
	}
	return inv
}
