package main

// M7 F4's CreatePartitions scenario (key 37).
//
// This API cannot widen a topic. Queen declares no width per queue, so the
// number a client sees is `max(live lanes, the topic's declared floor or
// QUEEN_KAFKA_DEFAULT_PARTITIONS)`. A topic may carry its own floor since M7 —
// but it is declared ONCE, at CreateTopics, and this API is not that writer, so
// there is still no write HERE that widens a topic. What the facade CAN do is
// refuse in the same words a real broker uses for the two cases where a real
// broker also refuses, and that is what this scenario measures.
//
// So the acceptance is split, deliberately:
//
//   - `decrease.*`, `equal.*` and `below_one.*` must be IDENTICAL, message
//     included. Those sentences were recorded off apache/kafka:3.9.1 in KRaft
//     mode and copied into `src/handlers/create_partitions.rs`; a divergence
//     here is a drift in the copy, not a deviation.
//   - `increase.*` is the one deliberate difference: the oracle widens the
//     topic and answers 0, the facade refuses INVALID_PARTITIONS and names the
//     broker knob. Classified in main.go.
//   - `assignments.error` must be identical (both INVALID_REPLICA_ASSIGNMENT);
//     only the sentence differs, because the oracle is complaining about the
//     assignment COUNT while the facade is refusing manual placement outright.
//
// Note `below_one`: a count of 0 is not a case of its own on either side. The
// oracle takes the DECREASE branch for it, because the width is never negative
// and the comparison catches every non-positive count first, which is why the
// facade has no separate branch either.
//
// The three mutating shapes each get their OWN topic, so no key here can move
// another one: the increase widens the oracle's topic, and a widened topic
// would change every width printed in every later message.

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	scenarios = append(scenarios, scenario{
		name: "createpartitions",
		desc: "widening, narrowing and matching a topic's partition count",
		run:  scenCreatePartitions,
	})
}

// The advertised window is the schema's whole `0..=3`, and no field varies
// inside it, so the walk below is an ENCODING walk: v3 is the flexible form of
// the same bytes. v3 alone is used for the observations, because a per-version
// walk here would create partitions on the oracle three times over.
const createPartitionsV int16 = 3

// createPartitions issues one CreatePartitions for one topic and returns the
// single result.
func createPartitions(
	k *conn,
	topic string,
	count int32,
	assignment []kmsg.CreatePartitionsRequestTopicAssignment,
	validateOnly bool,
) (kmsg.CreatePartitionsResponseTopic, error) {
	rt := kmsg.NewCreatePartitionsRequestTopic()
	rt.Topic = topic
	rt.Count = count
	rt.Assignment = assignment

	req := kmsg.NewCreatePartitionsRequest()
	req.TimeoutMillis = 30_000
	req.ValidateOnly = validateOnly
	req.Topics = []kmsg.CreatePartitionsRequestTopic{rt}

	resp, _, err := k.doT(&req, createPartitionsV, 30*time.Second)
	if err != nil {
		return kmsg.CreatePartitionsResponseTopic{}, err
	}
	cr := resp.(*kmsg.CreatePartitionsResponse)
	if len(cr.Topics) != 1 {
		return kmsg.CreatePartitionsResponseTopic{}, fmt.Errorf("%d results, want 1", len(cr.Topics))
	}
	return cr.Topics[0], nil
}

// recordPartitions writes the two fields a tool acts on: the code it branches
// on, and the message `kafka-topics.sh` prints to the operator.
func recordPartitions(c *runctx, key string, got kmsg.CreatePartitionsResponseTopic, err error) {
	if err != nil {
		c.rec.bad(key, err)
		return
	}
	c.rec.add(key+".error", "%s", errName(got.ErrorCode))
	c.rec.add(key+".message", "%s", showStrPtr(got.ErrorMessage))
}

func scenCreatePartitions(c *runctx) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial", err)
		return
	}
	defer k.Close()

	// One topic for the three NON-mutating shapes. Both brokers make it
	// `c.parts` wide, which is also the facade's QUEEN_KAFKA_DEFAULT_PARTITIONS
	// in rig-diff.sh, so the width printed in every message below is the same
	// number on both sides or the scenario has found something.
	stable := c.topic("cp-stable")
	if err := ensureTopic(k, stable, c.parts); err != nil {
		c.rec.bad("setup.stable", err)
		return
	}

	// A DECREASE: the case a provisioner declaring fewer partitions than the
	// facade's default actually produces, and the one where the facade's answer
	// is indistinguishable from a real broker's.
	got, err := createPartitions(k, stable, 2, nil, false)
	recordPartitions(c, "decrease", got, err)

	// EQUAL to the current width: the other of Kafka's own two sentences.
	got, err = createPartitions(k, stable, c.parts, nil, false)
	recordPartitions(c, "equal", got, err)

	// A non-positive count, which both sides answer as a decrease.
	got, err = createPartitions(k, stable, 0, nil, false)
	recordPartitions(c, "below_one", got, err)

	// ...and the same decrease under `validate_only`, which changes nothing on
	// either side because neither of them was going to write anything.
	got, err = createPartitions(k, stable, 2, nil, true)
	recordPartitions(c, "validate_only", got, err)

	// A topic neither broker has. Kafka answers UNKNOWN_TOPIC_OR_PARTITION with
	// a null message, and so does the facade.
	got, err = createPartitions(k, c.topic("cp-absent"), c.parts+1, nil, false)
	recordPartitions(c, "unknown", got, err)

	// An INCREASE, on its own topic because the oracle actually performs it.
	// The delta is small on purpose: this widens the oracle's topic by four
	// partitions and nothing else in the runner reads it.
	grow := c.topic("cp-grow")
	if err := ensureTopic(k, grow, c.parts); err != nil {
		c.rec.bad("setup.grow", err)
		return
	}
	got, err = createPartitions(k, grow, c.parts+4, nil, false)
	recordPartitions(c, "increase", got, err)

	// An increase carrying a MISMATCHED replica assignment: two new partitions
	// asked for, one assignment given. Both brokers refuse
	// INVALID_REPLICA_ASSIGNMENT, and neither widens anything, which is why
	// this shape is safe to send and the well-formed one is not.
	assigned := c.topic("cp-assigned")
	if err := ensureTopic(k, assigned, c.parts); err != nil {
		c.rec.bad("setup.assigned", err)
		return
	}
	one := kmsg.NewCreatePartitionsRequestTopicAssignment()
	one.Replicas = []int32{brokerIDInt(c)}
	got, err = createPartitions(k, assigned, c.parts+2,
		[]kmsg.CreatePartitionsRequestTopicAssignment{one}, false)
	recordPartitions(c, "assignments", got, err)
}

// brokerIDInt is `brokerID` as the number a replica assignment carries: the
// oracle's single node is 1 and the facade's single logical broker is 0, so an
// assignment naming "the broker that is there" is a different integer on each
// side. It never reaches a comparison — only the answer does.
func brokerIDInt(c *runctx) int32 {
	if c.target.label == "kafka" {
		return 1
	}
	return 0
}
