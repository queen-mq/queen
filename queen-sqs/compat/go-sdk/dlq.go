package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Redrive, end to end, on a dead-letter queue that is a REAL SQS queue a client
// can read.
//
// The threshold is AWS's: "the number of times a consumer can receive a message
// from a source queue before it is moved", so a `maxReceiveCount` of 2 means the
// consumer gets exactly two deliveries and the THIRD pop is the move — a
// delivery no client is ever handed. The two failures below are spelled the way
// a consumer library spells them, `ChangeMessageVisibility(0)`, which is the nack
// every one of them issues.
//
// THE COUNT CONTINUES ON THE COPY. It does not restart, which is AWS's
// behaviour and the thing people are most often surprised by: the number says
// how many times processing failed. Here the copy carries `received - 1` (the
// move's own pop was never handed to anybody) and the DLQ's first delivery adds
// its own attempt back, so `ApproximateReceiveCount` reads 3 — two deliveries a
// consumer saw, plus this one.
//
// The original MessageId cannot survive a move — the copy is a new row in a
// different queue and the broker mints ids — so it rides in the envelope and is
// surfaced as `queen.originalMessageId`, beside `queen.sourceQueue`. Both are
// asserted: without them a DLQ consumer has no correlation back to the message
// it is holding the remains of, which is the first thing anybody debugging a
// dead-letter queue asks for.
func tDlqRedrive(ctx context.Context, r *rig) {
	// ONE LANE on both queues. This scenario counts deliveries of one specific
	// message, and a hash across eight lanes would make "the next receive gets
	// it" a matter of luck rather than of the thing being tested.
	dlq, made := r.newQueue(ctx, "Redrive", "dlq", map[string]string{
		attrPartitions: "1", attrVisibility: "30",
	}, nil)
	if !made {
		return
	}
	policy := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":2}`, dlq.arn)
	source, made := r.newQueue(ctx, "Redrive", "src", map[string]string{
		attrPartitions: "1", attrVisibility: "30", attrRedrivePolicy: policy,
	}, nil)
	if !made {
		return
	}

	stored := r.attributes(ctx, "Redrive", source.url, attrRedrivePolicy)
	var readBack map[string]any
	if err := json.Unmarshal([]byte(stored[attrRedrivePolicy]), &readBack); err != nil {
		fail("Redrive.policy_reads_back_as_json", fmt.Sprintf("%q: %v", stored[attrRedrivePolicy], err))
	} else {
		checkEq("Redrive.policy_target_reads_back", fmt.Sprint(readBack["deadLetterTargetArn"]), dlq.arn)
		checkEq("Redrive.policy_count_reads_back", fmt.Sprint(readBack["maxReceiveCount"]), "2")
	}

	body := "dead-letter-me"
	attributes := map[string]types.MessageAttributeValue{"why": stringAttr("poison")}
	sent, err := r.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: &source.url, MessageBody: &body, MessageAttributes: attributes,
	})
	if !checkNoErr("Redrive.send_succeeds", err) {
		return
	}
	originalID := aws.ToString(sent.MessageId)

	// TWO FAILED DELIVERIES, each nacked the way a consumer library nacks.
	for attempt := 1; attempt <= 2; attempt++ {
		who := fmt.Sprintf("Redrive.delivery_%d", attempt)
		got := r.drain(ctx, who, source.url, 1, 30*time.Second, allAttributes())
		if !checkEq(who+"_arrives", len(got), 1) {
			return
		}
		checkEq(who+"_receive_count", got[0].Attributes[sysReceiveCount], strconv.Itoa(attempt))
		checkEq(who+"_is_the_same_message", aws.ToString(got[0].MessageId), originalID)
		_, err := r.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl: &source.url, ReceiptHandle: got[0].ReceiptHandle, VisibilityTimeout: 0,
		})
		if !checkNoErr(who+"_is_nacked", err) {
			return
		}
	}

	// THE THIRD POP IS THE MOVE. It is driven from here — a redrive happens
	// between a pop and its answer, so nothing moves until somebody receives —
	// and the loop ends when the copy shows up on the dead-letter queue.
	var dead *types.Message
	handedOut := 0
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		over := r.drain(ctx, "Redrive.over_threshold", source.url, 1, 3*time.Second, allAttributes())
		if len(over) > 0 {
			// A delivery past the threshold: the move did not happen, and this
			// loop would otherwise spin until the deadline for nothing.
			handedOut += len(over)
			r.deleteMessage(ctx, "Redrive.over_threshold", source.url, over[0].ReceiptHandle)
			break
		}
		arrived := r.drain(ctx, "Redrive.dead_letter", dlq.url, 1, 3*time.Second, allAttributes())
		if len(arrived) > 0 {
			dead = &arrived[0]
			break
		}
	}
	check("Redrive.the_over_threshold_delivery_is_never_handed_to_a_client", handedOut == 0,
		fmt.Sprintf("%d delivery(ies) past maxReceiveCount=2", handedOut))
	if !check("Redrive.the_message_arrives_on_the_dead_letter_queue", dead != nil,
		"nothing reached the dead-letter queue in 90s") {
		return
	}

	checkEq("Redrive.the_body_travels_verbatim", aws.ToString(dead.Body), body)
	checkEq("Redrive.the_message_attributes_travel_verbatim",
		aws.ToString(dead.MessageAttributes["why"].StringValue), "poison")
	checkEq("Redrive.the_copy_names_the_message_it_was_made_from",
		dead.Attributes[sysOriginalMessageID], originalID)
	checkEq("Redrive.the_copy_names_the_queue_it_came_from",
		dead.Attributes[sysSourceQueue], source.name)
	// The continuation: two deliveries a consumer saw, plus this one. A count
	// that read 1 here would mean the copy restarted, which is the one thing
	// AWS's rule forbids.
	info("dead-letter copy: ApproximateReceiveCount=%s", dead.Attributes[sysReceiveCount])
	checkEq("Redrive.receive_count_continues_rather_than_restarting",
		dead.Attributes[sysReceiveCount], "3")

	// DIVERGENCE, `accepted` (`actions/dlq.rs`): AWS keeps a message's id across
	// a move and this facade cannot — the copy is a new row in a different queue
	// and the broker mints the ids. Pinned rather than left in a comment, so
	// that a change to it is loud.
	check("Divergence.the_dead_letter_copy_has_a_new_message_id",
		aws.ToString(dead.MessageId) != originalID,
		"the copy kept the original MessageId, which this facade cannot do")

	// The whole point of a dead-letter QUEUE: it is receivable, and deletable.
	if r.deleteMessage(ctx, "Redrive.dead_letter", dlq.url, dead.ReceiptHandle) {
		ok("Redrive.the_dead_letter_copy_is_deletable")
	}
	checkEq("Redrive.the_dead_letter_queue_is_empty_afterwards",
		len(r.drain(ctx, "Redrive.dead_letter_after", dlq.url, 1, 6*time.Second, noAttributes())), 0)

	// ...and the source queue kept nothing: the move acked the original inside
	// the same transaction that pushed the copy.
	//
	// Polled rather than read once. The two counters are what KEDA and every
	// autoscaler read, so they are worth asserting exactly — and a single read
	// taken the instant a transaction commits is asserting the read path's
	// freshness as much as the move's correctness, which is not what this
	// scenario is about.
	var depth map[string]string
	for waited := time.Duration(0); waited < 10*time.Second; waited += time.Second {
		depth = r.attributes(ctx, "Redrive.depth", source.url, attrMessages, attrNotVisible)
		if depth[attrMessages] == "0" && depth[attrNotVisible] == "0" {
			break
		}
		time.Sleep(time.Second)
	}
	checkEq("Redrive.the_source_queue_holds_nothing",
		[2]string{depth[attrMessages], depth[attrNotVisible]}, [2]string{"0", "0"})

	// The registry read that names the relationship from the other end.
	sources, err := r.sqs.ListDeadLetterSourceQueues(ctx, &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl: &dlq.url,
	})
	if checkNoErr("ListDeadLetterSourceQueues.succeeds", err) {
		found := false
		for _, url := range sources.QueueUrls {
			if url == source.url {
				found = true
			}
		}
		check("ListDeadLetterSourceQueues.names_the_source", found,
			"got "+joinURLs(sources.QueueUrls))
	}
}
