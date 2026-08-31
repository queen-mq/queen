package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"
)

// The system attributes a delivery carries. `ApproximateReceiveCount` is the
// one the redrive threshold is compared against, so the two live in one place.
const (
	sysReceiveCount      = "ApproximateReceiveCount"
	sysSentTimestamp     = "SentTimestamp"
	sysGroupID           = "MessageGroupId"
	sysDedupID           = "MessageDeduplicationId"
	sysSequenceNumber    = "SequenceNumber"
	sysTraceHeader       = "AWSTraceHeader"
	sysOriginalMessageID = "queen.originalMessageId"
	sysSourceQueue       = "queen.sourceQueue"
)

func tSendReceiveDelete(ctx context.Context, r *rig) {
	queue, made := r.newQueue(ctx, "SendMessage", "sendattrs", nil, nil)
	if !made {
		return
	}

	body := "hello, queen-sqs"
	attributes := map[string]types.MessageAttributeValue{
		"plain":  stringAttr("a string"),
		"count":  typedAttr("Number", "42"),
		"blob":   binaryAttr([]byte{0x00, 0x01, 0x02, 0xff}),
		"custom": typedAttr("String.email", "alice@example.com"),
	}
	trace := "Root=1-63441c4a-abcdef012345678912345678"
	system := map[string]types.MessageSystemAttributeValue{
		sysTraceHeader: {DataType: aws.String("String"), StringValue: aws.String(trace)},
	}

	sent, err := r.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:                &queue.url,
		MessageBody:             &body,
		MessageAttributes:       attributes,
		MessageSystemAttributes: system,
	})
	if !checkNoErr("SendMessage.succeeds", err) {
		return
	}

	// The body digest is checked twice over: once by the SDK, inside the call
	// above (a mismatch would have come back as an error), and once here.
	checkEq("SendMessage.md5_of_body", aws.ToString(sent.MD5OfMessageBody), md5OfBody(body))
	// These two the Go SDK does NOT check — see [awsmd5.go]. They are ours.
	checkEq("SendMessage.md5_of_attributes",
		aws.ToString(sent.MD5OfMessageAttributes), md5OfMessageAttributes(attributes))
	checkEq("SendMessage.md5_of_system_attributes",
		aws.ToString(sent.MD5OfMessageSystemAttributes), md5OfMessageSystemAttributes(system))
	check("SendMessage.message_id_is_a_uuid", isUUID(aws.ToString(sent.MessageId)),
		"got "+aws.ToString(sent.MessageId))
	// A standard queue has no SequenceNumber on either side, which is AWS's own
	// shape: answering one would be answering a field AWS does not send.
	check("SendMessage.no_sequence_number_on_a_standard_queue", sent.SequenceNumber == nil,
		"got "+aws.ToString(sent.SequenceNumber))

	got := r.drain(ctx, "ReceiveMessage", queue.url, 1, 25*time.Second, allAttributes())
	if !checkEq("SendMessage.round_trips", len(got), 1) {
		return
	}
	message := got[0]
	checkEq("ReceiveMessage.body", aws.ToString(message.Body), body)
	checkEq("ReceiveMessage.md5_of_body", aws.ToString(message.MD5OfBody), md5OfBody(body))
	checkEq("ReceiveMessage.md5_of_attributes",
		aws.ToString(message.MD5OfMessageAttributes), md5OfMessageAttributes(attributes))
	checkEq("ReceiveMessage.message_id_matches_send",
		aws.ToString(message.MessageId), aws.ToString(sent.MessageId))

	received := message.MessageAttributes
	checkEq("ReceiveMessage.attribute_names", sortedKeys(received), sortedKeys(attributes))
	checkEq("ReceiveMessage.string_attribute",
		aws.ToString(received["plain"].StringValue), "a string")
	checkEq("ReceiveMessage.number_attribute_keeps_its_type",
		aws.ToString(received["count"].DataType), "Number")
	checkEq("ReceiveMessage.number_attribute",
		aws.ToString(received["count"].StringValue), "42")
	checkEq("ReceiveMessage.binary_attribute",
		base64.StdEncoding.EncodeToString(received["blob"].BinaryValue),
		base64.StdEncoding.EncodeToString([]byte{0x00, 0x01, 0x02, 0xff}))
	checkEq("ReceiveMessage.custom_data_type_survives",
		aws.ToString(received["custom"].DataType), "String.email")

	systemView := message.Attributes
	checkEq("ReceiveMessage.first_delivery_is_one", systemView[sysReceiveCount], "1")
	check("ReceiveMessage.sent_timestamp_is_epoch_millis",
		looksLikeEpochMillis(systemView[sysSentTimestamp]),
		"got "+systemView[sysSentTimestamp])
	checkEq("ReceiveMessage.trace_header_round_trips", systemView[sysTraceHeader], trace)
	check("ReceiveMessage.no_sequence_number_on_a_standard_queue",
		systemView[sysSequenceNumber] == "", "got "+systemView[sysSequenceNumber])

	r.deleteMessage(ctx, "ReceiveMessage", queue.url, message.ReceiptHandle)

	// The DEPRECATED spelling of the same selection. Every SDK major before the
	// rename sends `AttributeNames`, aws-sdk-go-v2 still carries it, and the
	// facade merges both members — asserted here because nothing else in the
	// compat tree drives the old name from a client that means it.
	if _, ok := r.send(ctx, "ReceiveMessage.deprecated", queue.url, "old-spelling"); ok {
		old := r.drain(ctx, "ReceiveMessage.deprecated", queue.url, 1, 25*time.Second,
			deprecatedAttributeNames())
		if checkEq("ReceiveMessage.deprecated_attribute_names_selection", len(old), 1) {
			checkEq("ReceiveMessage.deprecated_attribute_names_answer",
				old[0].Attributes[sysReceiveCount], "1")
			r.deleteMessage(ctx, "ReceiveMessage.deprecated", queue.url, old[0].ReceiptHandle)
		}
	}

	r.deleteMessageAssertions(ctx)
}

// The DeleteMessage half, on a queue of its own: the assertions need a SHORT
// visibility (a deleted message that was merely released would be back inside
// the window) and the queue above is configured for the long reads.
func (r *rig) deleteMessageAssertions(ctx context.Context) {
	queue, made := r.newQueue(ctx, "DeleteMessage", "delete", map[string]string{attrVisibility: "2"}, nil)
	if !made {
		return
	}
	if _, ok := r.send(ctx, "DeleteMessage", queue.url, "delete-me"); !ok {
		return
	}

	got := r.drain(ctx, "DeleteMessage", queue.url, 1, 25*time.Second, noAttributes())
	if !checkEq("DeleteMessage.received", len(got), 1) {
		return
	}
	handle := got[0].ReceiptHandle

	_, err := r.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl: &queue.url, ReceiptHandle: handle,
	})
	checkNoErr("DeleteMessage.succeeds", err)

	// The visibility is 2s: a deleted message that was merely released would be
	// back inside this window.
	back := r.drain(ctx, "DeleteMessage.after", queue.url, 1, 8*time.Second, noAttributes())
	checkEq("DeleteMessage.does_not_come_back", len(back), 0)

	// Deleting twice is a normal consumer's retry after a timed-out response,
	// and AWS answers it with a success.
	_, err = r.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl: &queue.url, ReceiptHandle: handle,
	})
	checkNoErr("DeleteMessage.double_delete_is_idempotent", err)

	// A handle this facade never minted is a forgery, not a stale lease.
	forged := base64.RawURLEncoding.EncodeToString([]byte("not-a-handle"))
	_, err = r.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl: &queue.url, ReceiptHandle: &forged,
	})
	expectAPIError("DeleteMessage.forged_handle_refused", err,
		"ReceiptHandleIsInvalid", new(*types.ReceiptHandleIsInvalid))
}

// tSdkChecksumValidation proves the Go SDK's own client-side digest check is
// ARMED, rather than assuming it because the documentation says so.
//
// Everything else in this file leans on that check: a facade that answered a
// constant `MD5OfMessageBody` fails every Send and every Receive here, which is
// the single biggest thing this rig adds over the python one. A check that had
// been silently disabled — by an SDK default flipping, by an option this suite
// set by accident — would make that protection vanish without a single
// assertion changing. So: one call has its answer DAMAGED on the way back
// through the middleware stack, and the call must fail because of it.
//
// The corruption is client-side and after the round trip, so the message the
// damaged Send answered for is really on the queue; the reads below account for
// it rather than pretending otherwise.
func tSdkChecksumValidation(ctx context.Context, r *rig) {
	// One lane: this scenario reasons about exactly which message a receive
	// gets, and a hash across eight lanes would make that luck.
	queue, made := r.newQueue(ctx, "Sdk", "checksum", map[string]string{
		attrPartitions: "1", attrVisibility: "30",
	}, nil)
	if !made {
		return
	}

	check("Sdk.checksum_validation_is_enabled_by_default",
		!r.sqs.Options().DisableMessageChecksumValidation,
		"the client was built with DisableMessageChecksumValidation")

	if _, ok := r.send(ctx, "Sdk", queue.url, "checksum-one"); !ok {
		return
	}

	body := "checksum-two"
	_, err := r.sqs.SendMessage(ctx,
		&sqs.SendMessageInput{QueueUrl: &queue.url, MessageBody: &body},
		func(o *sqs.Options) { o.APIOptions = append(o.APIOptions, corruptSendDigest) })
	check("Sdk.send_checksum_mismatch_is_caught",
		err != nil && strings.Contains(err.Error(), "checksum"),
		"got "+errText(err))

	// ...and with nothing corrupted, both messages come back and pass the same
	// check. This is the assertion that would fail against a facade whose body
	// digest is wrong, and it fails inside the SDK rather than here.
	drained := r.drainDeleting(ctx, "Sdk", queue.url, 2, 30*time.Second, noAttributes())
	checkEq("Sdk.undamaged_digests_pass_the_sdks_own_check", len(drained), 2)

	// The receive side of the same guarantee.
	if _, ok := r.send(ctx, "Sdk.receive", queue.url, "checksum-three"); !ok {
		return
	}
	var receiveErr error
	var delivered int
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		out, err := r.sqs.ReceiveMessage(ctx,
			&sqs.ReceiveMessageInput{QueueUrl: &queue.url, MaxNumberOfMessages: 1, WaitTimeSeconds: 2},
			func(o *sqs.Options) { o.APIOptions = append(o.APIOptions, corruptReceiveDigest) })
		if err != nil {
			receiveErr = err
			break
		}
		delivered += len(out.Messages)
		if len(out.Messages) > 0 {
			// A delivery the validator did not object to: it saw the damaged
			// digest and said nothing, which is the failure this test is for.
			break
		}
	}
	check("Sdk.receive_checksum_mismatch_is_caught",
		receiveErr != nil && strings.Contains(receiveErr.Error(), "checksum"),
		fmt.Sprintf("%d message(s) delivered without complaint, error %q", delivered, errText(receiveErr)))
}

// corruptSendDigest damages `MD5OfMessageBody` on the way back through the
// stack.
//
// It is added to the INITIALIZE step with `middleware.After`, which puts it
// behind the SDK's own `SQSValidateMessageChecksum` (added `Before`): on the
// return path this runs FIRST and the validator sees what it left.
func corruptSendDigest(stack *middleware.Stack) error {
	return stack.Initialize.Add(middleware.InitializeMiddlewareFunc(
		"QueenSqsCorruptSendDigest",
		func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
			middleware.InitializeOutput, middleware.Metadata, error,
		) {
			out, meta, err := next.HandleInitialize(ctx, in)
			if err != nil {
				return out, meta, err
			}
			if answer, ok := out.Result.(*sqs.SendMessageOutput); ok {
				answer.MD5OfMessageBody = aws.String("00000000000000000000000000000000")
			}
			return out, meta, err
		}), middleware.After)
}

func corruptReceiveDigest(stack *middleware.Stack) error {
	return stack.Initialize.Add(middleware.InitializeMiddlewareFunc(
		"QueenSqsCorruptReceiveDigest",
		func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
			middleware.InitializeOutput, middleware.Metadata, error,
		) {
			out, meta, err := next.HandleInitialize(ctx, in)
			if err != nil {
				return out, meta, err
			}
			if answer, ok := out.Result.(*sqs.ReceiveMessageOutput); ok {
				for i := range answer.Messages {
					answer.Messages[i].MD5OfBody = aws.String("00000000000000000000000000000000")
				}
			}
			return out, meta, err
		}), middleware.After)
}

// ------------------------------------------------------------------- batches

func tBatches(ctx context.Context, r *rig) {
	queue, made := r.newQueue(ctx, "SendMessageBatch", "sendbatch", nil, nil)
	if !made {
		return
	}

	entries := make([]types.SendMessageBatchRequestEntry, 0, 10)
	for i := 0; i < 10; i++ {
		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id:                aws.String(fmt.Sprintf("e%d", i)),
			MessageBody:       aws.String(fmt.Sprintf("batch-body-%d", i)),
			MessageAttributes: map[string]types.MessageAttributeValue{"i": typedAttr("Number", fmt.Sprint(i))},
		})
	}
	result, err := r.sqs.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: &queue.url, Entries: entries,
	})
	if !checkNoErr("SendMessageBatch.succeeds", err) {
		return
	}
	checkEq("SendMessageBatch.ten_succeeded", len(result.Successful), 10)
	checkEq("SendMessageBatch.none_failed", len(result.Failed), 0)

	echoed := make([]string, 0, len(result.Successful))
	distinct := map[string]struct{}{}
	byID := map[string]types.SendMessageBatchResultEntry{}
	for _, entry := range result.Successful {
		echoed = append(echoed, aws.ToString(entry.Id))
		distinct[aws.ToString(entry.MessageId)] = struct{}{}
		byID[aws.ToString(entry.Id)] = entry
	}
	sort.Strings(echoed)
	wanted := make([]string, 0, len(entries))
	for _, entry := range entries {
		wanted = append(wanted, aws.ToString(entry.Id))
	}
	sort.Strings(wanted)
	checkEq("SendMessageBatch.ids_echo", echoed, wanted)
	checkEq("SendMessageBatch.message_ids_are_distinct", len(distinct), 10)

	var badBody, badAttrs []string
	for _, entry := range entries {
		answer := byID[aws.ToString(entry.Id)]
		if aws.ToString(answer.MD5OfMessageBody) != md5OfBody(aws.ToString(entry.MessageBody)) {
			badBody = append(badBody, aws.ToString(entry.Id))
		}
		if aws.ToString(answer.MD5OfMessageAttributes) != md5OfMessageAttributes(entry.MessageAttributes) {
			badAttrs = append(badAttrs, aws.ToString(entry.Id))
		}
	}
	checkEq("SendMessageBatch.per_entry_body_md5", badBody, []string(nil))
	checkEq("SendMessageBatch.per_entry_attribute_md5", badAttrs, []string(nil))

	// Receive-and-delete, the only shape that drains a queue holding more
	// messages than it has lanes.
	drained := r.drainDeleting(ctx, "SendMessageBatch", queue.url, 10, 60*time.Second, noAttributes())
	checkEq("SendMessageBatch.all_ten_are_receivable", len(drained), 10)
	gotBodies := bodies(drained)
	sort.Strings(gotBodies)
	wantBodies := make([]string, 0, len(entries))
	for _, entry := range entries {
		wantBodies = append(wantBodies, aws.ToString(entry.MessageBody))
	}
	sort.Strings(wantBodies)
	checkEq("SendMessageBatch.bodies_round_trip", gotBodies, wantBodies)
	checkEq("SendMessageBatch.queue_is_empty_afterwards",
		len(r.drain(ctx, "SendMessageBatch.empty", queue.url, 1, 4*time.Second, noAttributes())), 0)

	r.batchLimits(ctx, queue)
	r.deleteBatchPartialFailure(ctx)
}

func (r *rig) batchLimits(ctx context.Context, queue queueRef) {
	_, err := r.sqs.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: &queue.url,
		Entries: []types.SendMessageBatchRequestEntry{
			{Id: aws.String("same"), MessageBody: aws.String("one")},
			{Id: aws.String("same"), MessageBody: aws.String("two")},
		},
	})
	expectAPIError("SendMessageBatch.duplicate_ids_refused", err,
		"BatchEntryIdsNotDistinct", new(*types.BatchEntryIdsNotDistinct))

	eleven := make([]types.SendMessageBatchRequestEntry, 0, 11)
	for i := 0; i < 11; i++ {
		eleven = append(eleven, types.SendMessageBatchRequestEntry{
			Id: aws.String(fmt.Sprintf("e%d", i)), MessageBody: aws.String("x"),
		})
	}
	_, err = r.sqs.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: &queue.url, Entries: eleven})
	expectAPIError("SendMessageBatch.eleven_entries_refused", err,
		"TooManyEntriesInBatchRequest", new(*types.TooManyEntriesInBatchRequest))

	// A non-nil EMPTY slice: nil would be refused by the SDK's own required-member
	// validation and would never reach the facade, which is the opposite of what
	// this asserts.
	_, err = r.sqs.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: &queue.url, Entries: []types.SendMessageBatchRequestEntry{},
	})
	expectAPIError("SendMessageBatch.empty_batch_refused", err,
		"EmptyBatchRequest", new(*types.EmptyBatchRequest))

	_, err = r.sqs.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: &queue.url, Entries: []types.DeleteMessageBatchRequestEntry{},
	})
	expectAPIError("DeleteMessageBatch.empty_batch_refused", err,
		"EmptyBatchRequest", new(*types.EmptyBatchRequest))
}

// A batch with one bad entry: AWS reports PER-ENTRY failure and does not fail
// the request. This is the shape every consumer library's ack loop depends on.
func (r *rig) deleteBatchPartialFailure(ctx context.Context) {
	queue, made := r.newQueue(ctx, "DeleteMessageBatch", "delbatch",
		map[string]string{attrVisibility: "300"}, nil)
	if !made {
		return
	}
	_, err := r.sqs.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: &queue.url,
		Entries: []types.SendMessageBatchRequestEntry{
			{Id: aws.String("e0"), MessageBody: aws.String("partial-0")},
			{Id: aws.String("e1"), MessageBody: aws.String("partial-1")},
			{Id: aws.String("e2"), MessageBody: aws.String("partial-2")},
		},
	})
	if !checkNoErr("DeleteMessageBatch.seed_succeeds", err) {
		return
	}

	// `hold` and not `drain`: three messages can share a lane, and this needs
	// three handles alive at the same moment.
	held := r.hold(ctx, "DeleteMessageBatch", queue.url, 3, 60*time.Second)
	if !checkEq("DeleteMessageBatch.received_three", len(held), 3) {
		return
	}

	forged := base64.RawURLEncoding.EncodeToString([]byte("forged"))
	partial, err := r.sqs.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: &queue.url,
		Entries: []types.DeleteMessageBatchRequestEntry{
			{Id: aws.String("a"), ReceiptHandle: held[0].ReceiptHandle},
			{Id: aws.String("b"), ReceiptHandle: held[1].ReceiptHandle},
			{Id: aws.String("bad"), ReceiptHandle: &forged},
		},
	})
	if !checkNoErr("DeleteMessageBatch.partial_request_is_not_an_error", err) {
		return
	}
	succeeded := make([]string, 0, len(partial.Successful))
	for _, entry := range partial.Successful {
		succeeded = append(succeeded, aws.ToString(entry.Id))
	}
	sort.Strings(succeeded)
	checkEq("DeleteMessageBatch.partial_success_ids", succeeded, []string{"a", "b"})

	failedIDs := make([]string, 0, len(partial.Failed))
	for _, entry := range partial.Failed {
		failedIDs = append(failedIDs, aws.ToString(entry.Id))
	}
	checkEq("DeleteMessageBatch.partial_failure_ids", failedIDs, []string{"bad"})
	if len(partial.Failed) == 1 {
		check("DeleteMessageBatch.failure_entry_has_a_code",
			aws.ToString(partial.Failed[0].Code) != "", "the entry carried no Code")
		checkEq("DeleteMessageBatch.failure_is_the_senders_fault",
			partial.Failed[0].SenderFault, true)
	}

	clean, err := r.sqs.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: &queue.url,
		Entries: []types.DeleteMessageBatchRequestEntry{
			{Id: aws.String("c"), ReceiptHandle: held[2].ReceiptHandle},
		},
	})
	if checkNoErr("DeleteMessageBatch.clean_request_succeeds", err) {
		var ids []string
		for _, entry := range clean.Successful {
			ids = append(ids, aws.ToString(entry.Id))
		}
		checkEq("DeleteMessageBatch.all_succeeded", ids, []string{"c"})
		checkEq("DeleteMessageBatch.none_failed", len(clean.Failed), 0)
	}
}

// ---------------------------------------------------------------- long poll

func tLongPoll(ctx context.Context, r *rig) {
	queue, made := r.newQueue(ctx, "LongPoll", "longpoll", map[string]string{attrWaitTime: "0"}, nil)
	if !made {
		return
	}

	started := time.Now()
	out, err := r.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: &queue.url, WaitTimeSeconds: 3,
	})
	waited := time.Since(started)
	if checkNoErr("ReceiveMessage.long_poll_succeeds", err) {
		checkEq("ReceiveMessage.long_poll_returns_empty", len(out.Messages), 0)
		// The whole point of a long poll is that it did NOT answer immediately:
		// a facade that ignored WaitTimeSeconds would return in milliseconds and
		// look identical in every other respect.
		check("ReceiveMessage.long_poll_waited",
			waited >= 2500*time.Millisecond && waited <= 8*time.Second,
			fmt.Sprintf("returned after %.2fs", waited.Seconds()))
	}

	// THE SHORT POLL, and a difference worth stating: aws-sdk-go-v2 models
	// `WaitTimeSeconds` as a non-pointer int32 and omits it when it is zero
	// (`serializers.go`: `if v.WaitTimeSeconds != 0`), so a Go client cannot
	// send an explicit 0 the way boto3 does. What it sends instead is NO wait,
	// which is the queue's own `ReceiveMessageWaitTimeSeconds` — set to 0 on
	// this queue above, so the assertion is the same one.
	started = time.Now()
	short, err := r.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: &queue.url})
	elapsed := time.Since(started)
	if checkNoErr("ReceiveMessage.short_poll_succeeds", err) {
		checkEq("ReceiveMessage.short_poll_returns_empty", len(short.Messages), 0)
		check("ReceiveMessage.short_poll_does_not_wait", elapsed < 2*time.Second,
			fmt.Sprintf("returned after %.2fs", elapsed.Seconds()))
	}

	// A long poll with a message already waiting answers at once rather than
	// sitting out its timeout.
	if _, ok := r.send(ctx, "LongPoll", queue.url, "waiting"); !ok {
		return
	}
	started = time.Now()
	got := r.drain(ctx, "LongPoll", queue.url, 1, 15*time.Second, noAttributes())
	elapsed = time.Since(started)
	if checkEq("ReceiveMessage.long_poll_finds_a_waiting_message", len(got), 1) {
		check("ReceiveMessage.long_poll_returns_early_when_it_can", elapsed < 8*time.Second,
			fmt.Sprintf("took %.2fs", elapsed.Seconds()))
		r.deleteMessage(ctx, "LongPoll", queue.url, got[0].ReceiptHandle)
	}
}

// --------------------------------------------------------------- visibility

func tVisibility(ctx context.Context, r *rig) {
	queue, made := r.newQueue(ctx, "ChangeMessageVisibility", "vis",
		map[string]string{attrVisibility: "2"}, nil)
	if !made {
		return
	}
	if _, ok := r.send(ctx, "ChangeMessageVisibility", queue.url, "visible-again"); !ok {
		return
	}

	got := r.drain(ctx, "ChangeMessageVisibility", queue.url, 1, 25*time.Second, noAttributes())
	if !checkEq("ChangeMessageVisibility.first_receive", len(got), 1) {
		return
	}
	handle := got[0].ReceiptHandle

	// EXTEND. The queue's own visibility is 2s, so without this the message
	// would be back inside the window below; with it, it must not be.
	_, err := r.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: &queue.url, ReceiptHandle: handle, VisibilityTimeout: 120,
	})
	if checkNoErr("ChangeMessageVisibility.extend_succeeds", err) {
		hidden := r.drain(ctx, "ChangeMessageVisibility.extend", queue.url, 1, 6*time.Second, noAttributes())
		checkEq("ChangeMessageVisibility.extend_hides_the_message", len(hidden), 0)
	}

	// TERMINATE. Zero releases it immediately, which is how every consumer
	// library nacks. `VisibilityTimeout` is a REQUIRED member here, so unlike
	// the receive above the SDK serializes the zero rather than omitting it.
	_, err = r.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: &queue.url, ReceiptHandle: handle, VisibilityTimeout: 0,
	})
	if !checkNoErr("ChangeMessageVisibility.terminate_succeeds", err) {
		return
	}
	back := r.drain(ctx, "ChangeMessageVisibility.terminate", queue.url, 1, 20*time.Second, allAttributes())
	if !checkEq("ChangeMessageVisibility.zero_returns_the_message", len(back), 1) {
		return
	}
	checkEq("ChangeMessageVisibility.same_message_came_back",
		aws.ToString(back[0].MessageId), aws.ToString(got[0].MessageId))
	checkEq("ChangeMessageVisibility.body_survived_the_release",
		aws.ToString(back[0].Body), "visible-again")
	// A redelivery is a second delivery, and SQS counts it.
	checkEq("ChangeMessageVisibility.receive_count_after_release",
		back[0].Attributes[sysReceiveCount], "2")
	check("ChangeMessageVisibility.a_redelivery_has_a_new_receipt_handle",
		aws.ToString(back[0].ReceiptHandle) != aws.ToString(handle),
		"the same handle came back")

	// The OLD handle belongs to a lease that no longer exists. AWS's contract
	// is that it fails — as ReceiptHandleIsInvalid or MessageNotInflight, both
	// of which are in the catalog — rather than silently moving the new
	// delivery.
	_, err = r.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: &queue.url, ReceiptHandle: handle, VisibilityTimeout: 60,
	})
	// Both are named by shape and compared through `wireCode`, because
	// `MessageNotInflight` is one of the errors whose legacy Query spelling is
	// the one this SDK reports; `ReceiptHandleIsInvalid` is one whose two
	// spellings coincide.
	code := apiCode(err)
	check("ChangeMessageVisibility.stale_handle_refused",
		err != nil && (code == wireCode("ReceiptHandleIsInvalid") ||
			code == wireCode("MessageNotInflight")),
		"got "+code)

	r.deleteMessage(ctx, "ChangeMessageVisibility", queue.url, back[0].ReceiptHandle)
}
