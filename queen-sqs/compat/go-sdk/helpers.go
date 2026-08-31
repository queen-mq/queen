package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
)

// A queue this run made: the three spellings of it every action wants.
type queueRef struct {
	name string
	url  string
	arn  string
}

func (r *rig) arnOfQueue(name string) string {
	return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", r.region, r.account, name)
}

func (r *rig) urlOfQueue(name string) string {
	return fmt.Sprintf("%s/%s/%s", r.endpoint, r.account, name)
}

// newQueue creates a queue named for this run and remembers it for teardown.
//
// The `.fifo` suffix is appended AFTER the run id when the request declares a
// FIFO queue, because the suffix is the whole of how the type is declared and
// it has to be the last thing in the name.
func (r *rig) newQueue(ctx context.Context, who, label string, attributes, tags map[string]string) (queueRef, bool) {
	name := fmt.Sprintf("go-%s-%s", label, r.run)
	if attributes["FifoQueue"] == "true" {
		name += ".fifo"
	}
	out, err := r.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  &name,
		Attributes: attributes,
		Tags:       tags,
	})
	if err != nil {
		fail(who+".create_queue", err.Error())
		return queueRef{}, false
	}
	url := aws.ToString(out.QueueUrl)
	r.queues = append(r.queues, url)
	return queueRef{name: name, url: url, arn: r.arnOfQueue(name)}, true
}

// allAttributes is the receive template for every assertion that reads one.
// Both `All`s: the system map and the message map are selected separately and
// a client that asked for neither gets neither.
func allAttributes() sqs.ReceiveMessageInput {
	return sqs.ReceiveMessageInput{
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		MessageAttributeNames:       []string{"All"},
	}
}

// deprecatedAttributeNames selects the system map through `AttributeNames`, the
// member aws-sdk-go-v2 marks Deprecated and every SDK major before the rename
// still sends. The facade reads both (`actions/messages.rs`: the two Selections
// are merged), and this is the only place in the compat tree that proves the
// old spelling still works from a client that means it.
func deprecatedAttributeNames() sqs.ReceiveMessageInput {
	return sqs.ReceiveMessageInput{
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	}
}

func noAttributes() sqs.ReceiveMessageInput { return sqs.ReceiveMessageInput{} }

// drain collects up to `count` messages WITHOUT deleting them, until `timeout`.
//
// A receive is up to N parallel `batch=1` pops across the queue's lanes, so one
// call legitimately returns fewer than it asked for even when the queue is
// full: every read of more than one message here loops. It cannot collect more
// messages than the queue has PARTITIONS (M0_SMOKE.md, D2), so it is used only
// where one or two are expected, or where the point is how few come back.
func (r *rig) drain(ctx context.Context, who string, url string, count int, timeout time.Duration, template sqs.ReceiveMessageInput) []types.Message {
	var got []types.Message
	deadline := time.Now().Add(timeout)
	for len(got) < count && time.Now().Before(deadline) {
		in := template
		in.QueueUrl = &url
		in.MaxNumberOfMessages = int32(min(10, count-len(got)))
		in.WaitTimeSeconds = 1
		out, err := r.sqs.ReceiveMessage(ctx, &in)
		if err != nil {
			// Never swallowed: the SDK's own body-MD5 check reports through
			// this error, and a suite that hid it would be exactly the suite
			// this rig exists to replace.
			fail(who+".receive_failed", err.Error())
			return got
		}
		got = append(got, out.Messages...)
	}
	return got
}

// drainDeleting is what a real consumer does: receive, delete, go round again.
// It is the only shape that can empty a queue holding more messages than it has
// lanes.
func (r *rig) drainDeleting(ctx context.Context, who string, url string, count int, timeout time.Duration, template sqs.ReceiveMessageInput) []types.Message {
	var got []types.Message
	deadline := time.Now().Add(timeout)
	for len(got) < count && time.Now().Before(deadline) {
		in := template
		in.QueueUrl = &url
		in.MaxNumberOfMessages = int32(min(10, count-len(got)))
		in.WaitTimeSeconds = 1
		out, err := r.sqs.ReceiveMessage(ctx, &in)
		if err != nil {
			fail(who+".receive_failed", err.Error())
			return got
		}
		for _, message := range out.Messages {
			r.deleteMessage(ctx, who, url, message.ReceiptHandle)
		}
		got = append(got, out.Messages...)
	}
	return got
}

// hold answers `count` messages received and NOT deleted, all in flight at once.
//
// Naively receiving `count` messages does not get there: two messages that
// hashed into the same lane cannot be in flight together. When a receive comes
// back empty this SENDS another message — a fresh MessageId is a fresh lane —
// rather than waiting on a lane that will not open. It leaves stragglers
// behind, so it is used only where nothing later asserts the queue is empty.
func (r *rig) hold(ctx context.Context, who string, url string, count int, timeout time.Duration) []types.Message {
	var held []types.Message
	deadline := time.Now().Add(timeout)
	for len(held) < count && time.Now().Before(deadline) {
		out, err := r.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &url,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			fail(who+".receive_failed", err.Error())
			return held
		}
		held = append(held, out.Messages...)
		if len(out.Messages) == 0 && len(held) < count {
			body := fmt.Sprintf("filler-%s-%d", r.run, len(held))
			if _, err := r.sqs.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl: &url, MessageBody: &body,
			}); err != nil {
				fail(who+".filler_send_failed", err.Error())
				return held
			}
		}
	}
	if len(held) > count {
		held = held[:count]
	}
	return held
}

// collectExactly answers `count` messages, and then a settling window that must
// stay empty.
//
// This is the shape every NEGATIVE assertion in this suite is written on: the
// messages that should be there, followed by proof that nothing else was behind
// them. "The filtered-out message did not arrive" is unfalsifiable on its own —
// an empty receive is also what a slow broker looks like — so a marker that
// MUST arrive dates the absence.
func (r *rig) collectExactly(ctx context.Context, who, url string, count int, settle, timeout time.Duration) ([]types.Message, []types.Message) {
	got := r.drainDeleting(ctx, who, url, count, timeout, allAttributes())
	extra := r.drainDeleting(ctx, who+".settle", url, 10, settle, allAttributes())
	return got, extra
}

func parseInt(value string) (int64, bool) {
	n, err := strconv.ParseInt(value, 10, 64)
	return n, err == nil
}

func (r *rig) deleteMessage(ctx context.Context, who, url string, handle *string) bool {
	_, err := r.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl: &url, ReceiptHandle: handle,
	})
	if err != nil {
		fail(who+".delete_failed", err.Error())
		return false
	}
	return true
}

func (r *rig) send(ctx context.Context, who, url, body string) (*sqs.SendMessageOutput, bool) {
	out, err := r.sqs.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: &url, MessageBody: &body})
	if err != nil {
		fail(who+".send_failed", err.Error())
		return nil, false
	}
	return out, true
}

func (r *rig) attributes(ctx context.Context, who, url string, names ...types.QueueAttributeName) map[string]string {
	if len(names) == 0 {
		names = []types.QueueAttributeName{types.QueueAttributeNameAll}
	}
	out, err := r.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: &url, AttributeNames: names,
	})
	if err != nil {
		fail(who+".get_queue_attributes_failed", err.Error())
		return map[string]string{}
	}
	return out.Attributes
}

// ------------------------------------------------------------------- errors

// legacyCode is SQS's OTHER spelling of an error, keyed by the shape name.
//
// SQS carries two names for most of its errors and they are usually different
// words: the shape name of the current model (`QueueDoesNotExist`) and the
// `error.code` the 2012-11-05 Query model carried
// (`AWS.SimpleQueueService.NonExistentQueue`). The facade sends both — the shape
// in the JSON `__type`, the legacy code in the `x-amzn-query-error` header —
// which is what `queen-sqs/src/error.rs`'s catalog is a table of, and what
// `smoke_m0.py`'s ERROR_CODES pins from the python side.
//
// This map is the same pairing seen from Go, and it is here because
// aws-sdk-go-v2 reads BOTH bytes and reports them in different places:
//
//   - the modelled TYPE comes from `__type`: the generated deserializer switches
//     on the sanitized shape name and builds `*types.QueueDoesNotExist`;
//   - `ErrorCode()` is then OVERRIDDEN by the header. Every generated
//     `awsAwsjson10_deserializeError<Shape>` in `service/sqs@v1.48.1` ends with
//     `awsQueryErrorCode := getAwsQueryErrorCode(response); if awsQueryErrorCode
//     != "" { output.ErrorCodeOverride = &awsQueryErrorCode }`.
//
// So a Go program that switches on `apiErr.ErrorCode()` sees the LEGACY code,
// and one that uses `errors.As` sees the shape — against real AWS as much as
// against this facade, because the header is AWS's own and the override exists
// to serve customers migrating off the Query protocol. Asserting the shape name
// here would be asserting something no Go caller ever observes.
//
// An error whose two spellings coincide is absent from the map on purpose:
// lookup falls through to the shape name, which is then also the wire code.
var legacyCode = map[string]string{
	"QueueDoesNotExist":            "AWS.SimpleQueueService.NonExistentQueue",
	"QueueNameExists":              "QueueAlreadyExists",
	"QueueDeletedRecently":         "AWS.SimpleQueueService.QueueDeletedRecently",
	"MessageNotInflight":           "AWS.SimpleQueueService.MessageNotInflight",
	"PurgeQueueInProgress":         "AWS.SimpleQueueService.PurgeQueueInProgress",
	"BatchEntryIdsNotDistinct":     "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
	"EmptyBatchRequest":            "AWS.SimpleQueueService.EmptyBatchRequest",
	"TooManyEntriesInBatchRequest": "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
	"InvalidBatchEntryId":          "AWS.SimpleQueueService.InvalidBatchEntryId",
	"BatchRequestTooLong":          "AWS.SimpleQueueService.BatchRequestTooLong",
}

// wireCode is what `ErrorCode()` will read for the shape named `shape`.
func wireCode(shape string) string {
	if legacy, ok := legacyCode[shape]; ok {
		return legacy
	}
	return shape
}

// expectAPIError requires `err` to be the API error `code`, AND to map to the
// SDK's own modelled type.
//
// `code` is named by its SHAPE, the way the catalog and every other suite in
// this tree names it; what is compared is `wireCode(code)`, because that is the
// string this SDK puts in `ErrorCode()` — see `legacyCode` above. Both halves of
// the pair are therefore pinned by one call: the shape through `as`, the legacy
// code through the comparison.
//
// The typed half is the interesting one. smithy-go picks the exception from the
// `__type` member alone — it strips the `com.amazonaws.sqs#` namespace and
// matches the SHAPE name — where botocore picks it from `QueryErrorCode`. So
// `as` failing while the code matches would mean the facade named the error in a
// way that only python understands.
//
// `as` is a pointer to a typed SDK error (`new(*types.QueueDoesNotExist)`), or
// nil where only the code is asserted.
func expectAPIError(name string, err error, code string, as any) bool {
	want := wireCode(code)
	if err == nil {
		fail(name, "the call succeeded; expected "+want)
		return false
	}
	var api smithy.APIError
	if !errors.As(err, &api) {
		fail(name, fmt.Sprintf("not an API error: %v", err))
		return false
	}
	typed := true
	if as != nil {
		typed = errors.As(err, as)
	}
	return checkEq(name, [2]any{api.ErrorCode(), typed}, [2]any{want, true})
}

func apiCode(err error) string {
	var api smithy.APIError
	if errors.As(err, &api) {
		return api.ErrorCode()
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

// ------------------------------------------------------------------ small fry

var uuidShape = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func isUUID(value string) bool { return uuidShape.MatchString(value) }

// looksLikeEpochMillis is any instant between 2001 and 2286 in MILLISECONDS.
// Seconds land far below the floor and microseconds far above the ceiling,
// which is the whole thing this is here to catch.
func looksLikeEpochMillis(value string) bool {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return false
	}
	return n >= 1_000_000_000_000 && n <= 9_999_999_999_999
}

// ISO 8601 with exactly three fractional digits and a Z, which is what SNS
// writes into a notification's Timestamp.
var iso8601Millis = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)

func bodies(messages []types.Message) []string {
	out := make([]string, 0, len(messages))
	for _, message := range messages {
		out = append(out, aws.ToString(message.Body))
	}
	return out
}

func messageIDs(messages []types.Message) []string {
	out := make([]string, 0, len(messages))
	for _, message := range messages {
		out = append(out, aws.ToString(message.MessageId))
	}
	return out
}

// The three shapes of a message attribute, spelled once. `typedAttr` is what
// carries a CUSTOM label (`String.email`, `Number.float`): the full label goes
// into the digest and the transport byte comes from the base type, which is why
// the type travels as text everywhere in this suite.
func stringAttr(value string) types.MessageAttributeValue {
	return typedAttr("String", value)
}

func typedAttr(dataType, value string) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String(dataType), StringValue: aws.String(value)}
}

func binaryAttr(value []byte) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String("Binary"), BinaryValue: value}
}

// joinURLs renders a list for a failure detail: a bare `%v` of a nil slice and
// of an empty one look the same, and which it was matters when the assertion is
// about emptiness.
func joinURLs(values []string) string {
	if len(values) == 0 {
		return "[] (nothing)"
	}
	return "[" + strings.Join(values, ", ") + "]"
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
