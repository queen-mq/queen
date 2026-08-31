package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// A FIFO queue is native here: `MessageGroupId` IS the Queen partition name,
// group-blocked-while-in-flight IS the partition claim, and
// `MessageDeduplicationId` IS the `transactionId` (PLAN_QUEEN_SQS.md, M2). The
// three scenarios below are the three things that buys, in the order a client
// meets them: order within a group, the sequence number on both sides, and the
// deduplication window.

func tFifoGroupOrdering(ctx context.Context, r *rig) {
	queue, made := r.newQueue(ctx, "Fifo", "fifo-order", map[string]string{
		attrFifo: "true", attrVisibility: "300",
	}, nil)
	if !made {
		return
	}

	// INTERLEAVED on purpose: two groups, sent alternately, so that "in order"
	// cannot be satisfied by handing back the queue's arrival order by luck.
	type publish struct{ group, body string }
	published := []publish{
		{"ga", "a-0"}, {"gb", "b-0"},
		{"ga", "a-1"}, {"gb", "b-1"},
		{"ga", "a-2"}, {"gb", "b-2"},
	}
	for i, item := range published {
		_, err := r.sqs.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:               &queue.url,
			MessageBody:            aws.String(item.body),
			MessageGroupId:         aws.String(item.group),
			MessageDeduplicationId: aws.String(fmt.Sprintf("%s-order-%d", r.run, i)),
		})
		if !checkNoErr("Fifo.send_"+item.body, err) {
			return
		}
	}

	got := r.drainDeleting(ctx, "Fifo", queue.url, len(published), 60*time.Second, allAttributes())
	if !checkEq("Fifo.every_message_arrives", len(got), len(published)) {
		return
	}

	// Ordering is asserted WITHIN a group and never across them: SQS promises
	// nothing about the relative order of two groups, and neither does a
	// partition claim.
	arrived := map[string][]string{}
	for _, message := range got {
		group := message.Attributes[sysGroupID]
		arrived[group] = append(arrived[group], aws.ToString(message.Body))
	}
	checkEq("Fifo.group_a_arrives_in_publish_order", arrived["ga"], []string{"a-0", "a-1", "a-2"})
	checkEq("Fifo.group_b_arrives_in_publish_order", arrived["gb"], []string{"b-0", "b-1", "b-2"})
	checkEq("Fifo.no_third_group_appeared", len(arrived), 2)
}

// tFifoSequenceNumber is C-SQS-3 seen from a second client: the SequenceNumber
// a send answered comes back on the RECEIVE.
//
// The number is the absolute offset the push allocated, and until C-SQS-3 the
// pop wire did not carry it at all — so a facade could only answer it on the
// way in. It is asserted here as well as in `smoke_m0.py` because the two
// clients read it out of two different wire renderings of the same map.
func tFifoSequenceNumber(ctx context.Context, r *rig) {
	queue, made := r.newQueue(ctx, "SequenceNumber", "fifo-seq", map[string]string{
		attrFifo: "true", attrVisibility: "300",
	}, nil)
	if !made {
		return
	}

	sent := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		answer, err := r.sqs.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:               &queue.url,
			MessageBody:            aws.String(fmt.Sprintf("ordered-%d", i)),
			MessageGroupId:         aws.String("g-seq"),
			MessageDeduplicationId: aws.String(fmt.Sprintf("%s-seq-%d", r.run, i)),
		})
		if !checkNoErr(fmt.Sprintf("SequenceNumber.send_%d", i), err) {
			return
		}
		if answer.SequenceNumber == nil {
			fail("SequenceNumber.send_answers_one_per_message",
				fmt.Sprintf("message %d came back without a SequenceNumber", i))
			return
		}
		sent = append(sent, aws.ToString(answer.SequenceNumber))
	}
	ok("SequenceNumber.send_answers_one_per_message")
	check("SequenceNumber.send_side_is_ascending_within_the_group", ascending(sent),
		fmt.Sprintf("got %v", sent))

	// ONE group, so all three arrive together and in order.
	got := r.drainDeleting(ctx, "SequenceNumber", queue.url, 3, 45*time.Second, allAttributes())
	if !checkEq("SequenceNumber.received_the_whole_group", len(got), 3) {
		return
	}
	checkEq("SequenceNumber.bodies_in_publish_order", bodies(got),
		[]string{"ordered-0", "ordered-1", "ordered-2"})

	received := make([]string, 0, len(got))
	groups := make([]string, 0, len(got))
	dedups := make([]string, 0, len(got))
	for _, message := range got {
		received = append(received, message.Attributes[sysSequenceNumber])
		groups = append(groups, message.Attributes[sysGroupID])
		dedups = append(dedups, message.Attributes[sysDedupID])
	}
	checkEq("SequenceNumber.receive_answers_what_the_send_answered", received, sent)
	checkEq("SequenceNumber.group_id_comes_back", groups, []string{"g-seq", "g-seq", "g-seq"})
	checkEq("SequenceNumber.dedup_ids_come_back", dedups, []string{
		r.run + "-seq-0", r.run + "-seq-1", r.run + "-seq-2",
	})
}

// tFifoDeduplication: a repeated MessageDeduplicationId is a SUCCESS whose
// message is never delivered.
//
// ABSENCE IS NEVER ASSERTED BY ITSELF (`smoke_m4_sns.py`'s rule): "the
// duplicate did not arrive" is unfalsifiable on its own, because an empty
// receive is also what a slow broker looks like. The duplicate is followed by a
// MARKER that must arrive, and the assertion is on the whole set.
func tFifoDeduplication(ctx context.Context, r *rig) {
	queue, made := r.newQueue(ctx, "FifoDedup", "fifo-dedup", map[string]string{
		attrFifo: "true", attrVisibility: "300",
	}, nil)
	if !made {
		return
	}

	dedup := "dedup-" + r.run
	first, err := r.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               &queue.url,
		MessageBody:            aws.String("the original"),
		MessageGroupId:         aws.String("g-dup"),
		MessageDeduplicationId: &dedup,
	})
	if !checkNoErr("FifoDedup.first_send_succeeds", err) {
		return
	}

	// The body is deliberately different, so a delivery of it would be
	// unmistakable.
	second, err := r.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               &queue.url,
		MessageBody:            aws.String("the duplicate, which must not be delivered"),
		MessageGroupId:         aws.String("g-dup"),
		MessageDeduplicationId: &dedup,
	})
	checkNoErr("FifoDedup.repeated_dedup_id_is_a_success", err)

	_, err = r.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               &queue.url,
		MessageBody:            aws.String("the marker"),
		MessageGroupId:         aws.String("g-dup"),
		MessageDeduplicationId: aws.String("marker-" + r.run),
	})
	if !checkNoErr("FifoDedup.marker_send_succeeds", err) {
		return
	}

	got, extra := r.collectExactly(ctx, "FifoDedup", queue.url, 2, 4*time.Second, 45*time.Second)
	checkEq("FifoDedup.the_duplicate_is_not_delivered", bodies(got),
		[]string{"the original", "the marker"})
	checkEq("FifoDedup.nothing_followed_the_marker", len(extra), 0)

	// AWS answers a repeated deduplication id with the ORIGINAL message's id.
	// Whether this facade does is a broker question — the push's `duplicate`
	// status carries the original's id — and `smoke_m4_sns.py` records the SNS
	// side of it going the other way. RECORDED, not asserted: only a run against
	// real AWS settles what the right answer is, and a verdict invented here
	// would be a verdict about nothing.
	if err == nil && second != nil {
		same := "DIFFERENT"
		if aws.ToString(second.MessageId) == aws.ToString(first.MessageId) {
			same = "same"
		}
		note("SQS FIFO dedup MessageId: first=%s deduplicated-send=%s (%s)",
			aws.ToString(first.MessageId), aws.ToString(second.MessageId), same)
	}
}

func ascending(values []string) bool {
	for i := 1; i < len(values); i++ {
		if !lessAsNumber(values[i-1], values[i]) {
			return false
		}
	}
	return true
}

// SequenceNumber is 128 bits at AWS and an absolute offset here, so it is
// compared as a number when both sides parse as one and as text otherwise —
// never as text alone, which would make 10 sort before 9.
func lessAsNumber(a, b string) bool {
	na, aok := parseInt(a)
	nb, bok := parseInt(b)
	if aok && bok {
		return na < nb
	}
	return a < b
}
