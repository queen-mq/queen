// Package broker is the seam between the workload and the systems under test.
//
// The interface is stated at the level of what the APPLICATION needs — ordered
// batch delivery per key, explicit batch ack, keyed ordered publish — and not at
// the level of any one broker's mechanism. That is deliberate: how a system
// satisfies the contract is that system's business, the best-known idiom for it
// must be used (SPEC.md §5.5), and how much machinery that idiom costs is
// exactly what the campaign is measuring.
package broker

import (
	"context"

	"crossbench/internal/workload"
)

// Message is one delivered event with its decoded stamp.
type Message struct {
	Stamp   workload.Stamp
	Payload []byte

	// Redelivery is set when the system itself reports this as a redelivery
	// (Queen delivery count, Rabbit's redelivered flag). It is informational:
	// the verifier detects duplicates from the logs regardless, and a system
	// that redelivers silently must not be advantaged by staying quiet.
	Redelivery bool
}

// Batch is a run of messages that all share ONE ordering key, delivered in seq
// order.
//
// The contract on the adapter is strict and is the whole basis of the
// comparison: within a group, a given key must never be in flight in two
// handlers at once. Across keys the adapter is free — and is expected — to be as
// concurrent as the system allows, because reaching the workload's lane count
// (SPEC.md §2) is the adapter's job.
type Batch struct {
	Key  string
	Msgs []Message
}

// Handler processes one key-batch. Returning nil means the batch is complete
// and the adapter must ack it. Returning an error means the batch was NOT
// applied: the adapter must not ack, so the system's own redelivery path runs.
// Nothing may be half-applied — the verifier reads a partial batch as a gap.
type Handler func(ctx context.Context, b *Batch) error

// ConsumeOpts tunes one stage's consumption.
type ConsumeOpts struct {
	// Lanes is the target ordered concurrency for this stage, from Little's law
	// on the workload (SPEC.md §2). The adapter should provision enough
	// machinery — members, threads, connections, queues — to sustain it, and
	// report in Stats how much that took.
	Lanes int

	// BatchSize caps how many messages of one key a single Batch may carry.
	BatchSize int

	// Prefetch hints how much the adapter may hold in flight per lane. Zero
	// lets the adapter choose its idiomatic default.
	Prefetch int

	// Stats is the stage's accounting. The adapter owns the ack path, so the
	// adapter bumps Acked and AckErr; the runner reads them for the report and
	// for produced.meta (a non-zero AckErr is what makes an ordering violation
	// excusable as a redelivery, so it must be recorded honestly).
	//
	// Resolve it once at Consume entry with AckStats() rather than touching the
	// field directly: a nil here should degrade to lost accounting, never to a
	// panic that ends a twenty-minute run.
	Stats *workload.StageCounters
}

// AckStats returns the stage counters, substituting a throwaway when the caller
// left them unset. Call it ONCE per Consume and keep the result.
func (o ConsumeOpts) AckStats() *workload.StageCounters {
	if o.Stats != nil {
		return o.Stats
	}
	return &workload.StageCounters{}
}

// SetupOpts parameterises topology creation.
type SetupOpts struct {
	// PhysicalLanes is how many ordered lanes to provision per topic. It
	// defaults to Topology.Properties (1 property = 1 lane, Queen's bet).
	// Setting it lower deliberately makes properties share a lane, which is how
	// head-of-line blocking gets measured on systems with a fixed lane count.
	PhysicalLanes int

	// DedupWindowSec enables broker-side dedup where the system has it. The
	// headline comparison runs with 0 everywhere (SPEC.md §5.4).
	DedupWindowSec int

	// Durability selects the system's durability tier: "default" is how the
	// system ships, "fsync" forces per-write flush. Reported in every result;
	// never compared across tiers silently (SPEC.md §5.3).
	Durability string

	// Reset drops and recreates the topology before the run.
	Reset bool
}

// Broker is one system under test.
type Broker interface {
	// Name identifies the system in reports, e.g. "queen", "kafka".
	Name() string

	// Setup creates topics/queues/partitions/groups for the topology. It must
	// be idempotent and must leave the system ready to accept the full rate:
	// any lazy creation the system does under load is part of what we measure,
	// so adapters must not hide it behind their own pre-warming unless the
	// production deployment would also pre-create (see the run's -warmup flag,
	// which pre-hydrates through the real publish path for everyone equally).
	Setup(ctx context.Context, t workload.Topology, o SetupOpts) error

	// Publish sends one keyed message. Successive calls for the same key from
	// the same goroutine must arrive in call order.
	Publish(ctx context.Context, topic, key string, payload []byte) error

	// PublishBatch sends several messages to ONE key as a single unit, in slice
	// order. Systems that can append a batch atomically should do so; systems
	// that cannot must still preserve order, and the extra round trips they
	// need are part of their cost.
	PublishBatch(ctx context.Context, topic, key string, payloads [][]byte) error

	// Consume runs one stage until ctx is done, calling h with key-batches and
	// acking each batch the handler completes. It blocks.
	Consume(ctx context.Context, topic, group string, o ConsumeOpts, h Handler) error

	// Provisioned reports what the adapter had to create and hold open to meet
	// the contract — the "lanes provisioned" and "members / connections" rows
	// of the cost table (SPEC.md §6.1).
	Provisioned() Provisioned

	// Stats returns adapter-native counters for the report.
	Stats() map[string]any

	Close() error
}

// Provisioned is the machinery a system needed to serve the workload. These are
// result values, not configuration: they answer "what did it cost to get N
// ordered lanes out of this system".
type Provisioned struct {
	// OrderedLanes is the number of physical ordered lanes across all topics
	// (Queen partitions, Kafka partitions, Rabbit queues, pgmq groups).
	OrderedLanes int

	// PhysicalQueues counts materialised queues/topics. A system without native
	// consumer groups multiplies this by the fan-out width.
	PhysicalQueues int

	// ConsumerMembers counts group members / independent consumer instances.
	ConsumerMembers int

	// Connections counts open connections to the system.
	Connections int

	// PublishesPerIngressEvent is 2 for native fan-out and 1+FanOut for
	// materialised fan-out (SPEC.md §2).
	PublishesPerIngressEvent float64

	// BuiltSemantics lists what the ADAPTER had to implement because the system
	// does not provide it — the "semantics you must build" row. Honest entries
	// only: things the application genuinely carries.
	BuiltSemantics []string
}
