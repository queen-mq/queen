package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// SNS is where this suite exercises the OTHER codec. `boto3.client("sns")` and
// aws-sdk-go-v2's `service/sns` both speak the Query protocol — form-encoded in,
// XML out, `Version=2010-03-31` — because SNS never moved to JSON, while the SQS
// client on the SAME endpoint speaks AWS JSON 1.0. One listener, two codecs, two
// SigV4 scopes, and [tProtocols] turns that into a verdict rather than a claim.

func (r *rig) newTopic(ctx context.Context, who, label string, attributes map[string]string) (string, string, bool) {
	name := fmt.Sprintf("go-%s-%s", label, r.run)
	out, err := r.sns.CreateTopic(ctx, &sns.CreateTopicInput{Name: &name, Attributes: attributes})
	if err != nil {
		fail(who+".create_topic", err.Error())
		return "", "", false
	}
	arn := aws.ToString(out.TopicArn)
	r.topics = append(r.topics, arn)
	return name, arn, true
}

func (r *rig) subscribeQueue(ctx context.Context, who, topicArn, queueArn string, attributes map[string]string) (string, bool) {
	out, err := r.sns.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn:              &topicArn,
		Protocol:              aws.String("sqs"),
		Endpoint:              &queueArn,
		ReturnSubscriptionArn: true,
		Attributes:            attributes,
	})
	if err != nil {
		fail(who+".subscribe", err.Error())
		return "", false
	}
	return aws.ToString(out.SubscriptionArn), true
}

func tSnsSubscribe(ctx context.Context, r *rig) {
	name, topic, made := r.newTopic(ctx, "CreateTopic", "sns-topic",
		map[string]string{"DisplayName": "Go matrix orders"})
	if !made {
		return
	}
	checkEq("CreateTopic.arn", topic, fmt.Sprintf("arn:aws:sns:%s:%s:%s", r.region, r.account, name))

	again, err := r.sns.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: &name, Attributes: map[string]string{"DisplayName": "Go matrix orders"},
	})
	if checkNoErr("CreateTopic.idempotent_identical_request_succeeds", err) {
		checkEq("CreateTopic.idempotent_identical_request", aws.ToString(again.TopicArn), topic)
	}
	bare, err := r.sns.CreateTopic(ctx, &sns.CreateTopicInput{Name: &name})
	if checkNoErr("CreateTopic.idempotent_without_attributes_succeeds", err) {
		checkEq("CreateTopic.idempotent_without_attributes", aws.ToString(bare.TopicArn), topic)
	}

	attributes, err := r.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &topic})
	if checkNoErr("GetTopicAttributes.succeeds", err) {
		checkEq("GetTopicAttributes.arn", attributes.Attributes["TopicArn"], topic)
		checkEq("GetTopicAttributes.owner", attributes.Attributes["Owner"], r.account)
		checkEq("GetTopicAttributes.display_name", attributes.Attributes["DisplayName"], "Go matrix orders")
	}

	queue, ok := r.newQueue(ctx, "Subscribe", "sns-sub-queue", nil, nil)
	if !ok {
		return
	}
	subscription, ok := r.subscribeQueue(ctx, "Subscribe", topic, queue.arn, nil)
	if !ok {
		return
	}

	// AUTO-CONFIRMED. AWS answers the literal string "pending confirmation" for
	// a subscription that needs a handshake; a same-account SQS subscription
	// never does, so the real ARN is the only answer this action has. A client
	// that stored the placeholder would fail every later
	// SetSubscriptionAttributes.
	check("Subscribe.arn_is_not_pending_confirmation", subscription != "pending confirmation",
		"got "+subscription)
	check("Subscribe.arn_extends_the_topic_arn",
		strings.HasPrefix(subscription, topic+":") && len(strings.Split(subscription, ":")) == 7,
		"got "+subscription)

	read, err := r.sns.GetSubscriptionAttributes(ctx,
		&sns.GetSubscriptionAttributesInput{SubscriptionArn: &subscription})
	if checkNoErr("GetSubscriptionAttributes.succeeds", err) {
		got := read.Attributes
		checkEq("GetSubscriptionAttributes.identity",
			[5]string{got["SubscriptionArn"], got["TopicArn"], got["Protocol"], got["Endpoint"], got["Owner"]},
			[5]string{subscription, topic, "sqs", queue.arn, r.account})
		checkEq("GetSubscriptionAttributes.confirmed_at_creation",
			[2]string{got["PendingConfirmation"], got["ConfirmationWasAuthenticated"]},
			[2]string{"false", "true"})
		checkEq("GetSubscriptionAttributes.raw_message_delivery_defaults_off",
			got["RawMessageDelivery"], "false")
		// No policy, so no scope: AWS reports the scope only where there is a
		// policy for it to apply to, and a provisioner that read one here would
		// report drift on every reconcile.
		_, hasPolicy := got["FilterPolicy"]
		_, hasScope := got["FilterPolicyScope"]
		check("GetSubscriptionAttributes.no_filter_policy_scope_without_a_policy",
			!hasPolicy && !hasScope, "got "+strings.Join(sortedKeys(got), ", "))
	}

	repeat, ok := r.subscribeQueue(ctx, "Subscribe.repeat", topic, queue.arn, nil)
	if ok {
		checkEq("Subscribe.idempotent_per_topic_protocol_endpoint", repeat, subscription)
	}

	listing, err := r.sns.ListSubscriptionsByTopic(ctx,
		&sns.ListSubscriptionsByTopicInput{TopicArn: &topic})
	if checkNoErr("ListSubscriptionsByTopic.succeeds", err) {
		checkEq("ListSubscriptionsByTopic.count", len(listing.Subscriptions), 1)
		if len(listing.Subscriptions) == 1 {
			entry := listing.Subscriptions[0]
			checkEq("ListSubscriptionsByTopic.entry",
				[5]string{
					aws.ToString(entry.SubscriptionArn), aws.ToString(entry.TopicArn),
					aws.ToString(entry.Protocol), aws.ToString(entry.Endpoint),
					aws.ToString(entry.Owner),
				},
				[5]string{subscription, topic, "sqs", queue.arn, r.account})
		}
	}

	// An unknown topic must NOT answer an empty list: a client reads that as
	// "nothing is subscribed" rather than "you asked about the wrong topic".
	// This is also the Query/XML error path — a different codec, a different
	// deserializer and a different exception factory from every SQS error
	// above.
	ghost := fmt.Sprintf("arn:aws:sns:%s:%s:go-ghost-%s", r.region, r.account, r.run)
	_, err = r.sns.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: &ghost})
	if expectAPIError("ListSubscriptionsByTopic.unknown_topic_is_not_an_empty_list", err,
		"NotFound", new(*snstypes.NotFoundException)) {
		var response *awshttp.ResponseError
		status := 0
		if errors.As(err, &response) {
			status = response.HTTPStatusCode()
		}
		checkEq("ListSubscriptionsByTopic.unknown_topic_status", status, 404)
	}

	// The refusal a v0 subscriber meets: HTTP/S subscriptions are M6 and are
	// refused rather than accepted-and-dropped.
	_, err = r.sns.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: &topic, Protocol: aws.String("https"),
		Endpoint: aws.String("https://example.invalid/hook"), ReturnSubscriptionArn: true,
	})
	expectAPIError("Subscribe.non_sqs_protocol_refused", err,
		"InvalidParameter", new(*snstypes.InvalidParameterException))
}

func tSnsEnvelope(ctx context.Context, r *rig) {
	_, topic, made := r.newTopic(ctx, "Publish", "sns-notify", nil)
	if !made {
		return
	}
	queue, ok := r.newQueue(ctx, "Publish", "sns-notify-queue", nil, nil)
	if !ok {
		return
	}
	if _, ok := r.subscribeQueue(ctx, "Publish", topic, queue.arn, nil); !ok {
		return
	}

	body := `the payload, with spaces and a comma, and "quotes"`
	blob := []byte{0x00, 0x01, 0x02, 'r', 'i', 'g'}
	published, err := r.sns.Publish(ctx, &sns.PublishInput{
		TopicArn: &topic,
		Message:  &body,
		Subject:  aws.String("Go matrix subject"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"event": {DataType: aws.String("String"), StringValue: aws.String("order.created")},
			"count": {DataType: aws.String("Number"), StringValue: aws.String("7")},
			"blob":  {DataType: aws.String("Binary"), BinaryValue: blob},
		},
	})
	if !checkNoErr("Publish.succeeds", err) {
		return
	}
	publishID := aws.ToString(published.MessageId)
	check("Publish.message_id_is_a_uuid", isUUID(publishID), "got "+publishID)

	got := r.drainDeleting(ctx, "Publish", queue.url, 1, 30*time.Second, allAttributes())
	if !checkEq("Publish.notification_arrives", len(got), 1) {
		return
	}
	message := got[0]
	envelope := envelopeOf(message)
	if !check("Publish.body_is_json", envelope != nil, "body was "+aws.ToString(message.Body)) {
		return
	}

	checkEq("Notification.type", envelopeText(envelope, "Type"), "Notification")
	checkEq("Notification.topic_arn", envelopeText(envelope, "TopicArn"), topic)
	checkEq("Notification.message", envelopeText(envelope, "Message"), body)
	checkEq("Notification.subject", envelopeText(envelope, "Subject"), "Go matrix subject")
	// THE PUBLISH'S id, not the delivery's: it is what lets one fan-out be
	// correlated end to end, and it is AWS's own behaviour.
	checkEq("Notification.message_id_is_the_publishers", envelopeText(envelope, "MessageId"), publishID)
	check("Notification.timestamp_is_iso8601_millis",
		iso8601Millis.MatchString(envelopeText(envelope, "Timestamp")),
		"got "+envelopeText(envelope, "Timestamp"))
	checkEq("Notification.signature_version", envelopeText(envelope, "SignatureVersion"), "1")
	checkEq("Notification.message_attributes", envelopeAttributes(envelope), map[string][2]string{
		"event": {"String", "order.created"},
		"count": {"Number", "7"},
		"blob":  {"Binary", base64.StdEncoding.EncodeToString(blob)},
	})
	// The three fields AWS writes and this deployment cannot stand behind.
	// Their absence is the honest half of an unsigned notification, and it is
	// pinned so that a later change to add a Signature nothing can verify is
	// loud.
	var unverifiable []string
	for _, field := range []string{"Signature", "SigningCertURL", "UnsubscribeURL"} {
		if _, present := envelope[field]; present {
			unverifiable = append(unverifiable, field)
		}
	}
	checkEq("Notification.carries_no_unverifiable_signature_fields", unverifiable, []string(nil))
	// ...and the envelope's attributes are NOT also written as SQS message
	// attributes: two copies of one truth can disagree.
	checkEq("Notification.no_sqs_message_attributes_in_envelope_mode",
		len(message.MessageAttributes), 0)
	checkEq("Notification.body_md5", aws.ToString(message.MD5OfBody),
		md5OfBody(aws.ToString(message.Body)))
	// The SQS MessageId is the BROKER's, one per delivery, and is a different
	// id from the publish's — both are true at AWS too.
	check("Notification.sqs_message_id_is_not_the_publish_id",
		aws.ToString(message.MessageId) != publishID, "the delivery reused the publish's id")
}

func tSnsRaw(ctx context.Context, r *rig) {
	_, topic, made := r.newTopic(ctx, "RawMessageDelivery", "sns-raw", nil)
	if !made {
		return
	}
	queue, ok := r.newQueue(ctx, "RawMessageDelivery", "sns-raw-queue", nil, nil)
	if !ok {
		return
	}
	subscription, ok := r.subscribeQueue(ctx, "RawMessageDelivery", topic, queue.arn, nil)
	if !ok {
		return
	}

	_, err := r.sns.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{
		SubscriptionArn: &subscription,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	})
	if !checkNoErr("SetSubscriptionAttributes.raw_succeeds", err) {
		return
	}
	read, err := r.sns.GetSubscriptionAttributes(ctx,
		&sns.GetSubscriptionAttributesInput{SubscriptionArn: &subscription})
	if checkNoErr("SetSubscriptionAttributes.raw_reads_back_succeeds", err) {
		checkEq("SetSubscriptionAttributes.raw_message_delivery_reads_back",
			read.Attributes["RawMessageDelivery"], "true")
	}

	body := `{"order":42,"note":"raw, not enveloped"}`
	_, err = r.sns.Publish(ctx, &sns.PublishInput{
		TopicArn: &topic,
		Message:  &body,
		Subject:  aws.String("ignored in raw mode"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"event": {DataType: aws.String("String"), StringValue: aws.String("order.created")},
			"count": {DataType: aws.String("Number"), StringValue: aws.String("7")},
		},
	})
	if !checkNoErr("RawMessageDelivery.publish_succeeds", err) {
		return
	}

	got := r.drainDeleting(ctx, "RawMessageDelivery", queue.url, 1, 30*time.Second, allAttributes())
	if !checkEq("RawMessageDelivery.arrives", len(got), 1) {
		return
	}
	message := got[0]

	// THE WHOLE MEANING OF "RAW": a consumer written against a queue reads the
	// body it was sent and never learns a topic was involved.
	checkEq("RawMessageDelivery.body_is_the_message_alone", aws.ToString(message.Body), body)
	check("RawMessageDelivery.body_is_not_an_envelope",
		envelopeText(envelopeOf(message), "Type") != "Notification",
		"the body was still an SNS notification")

	forwarded := map[string][2]string{}
	for name, value := range message.MessageAttributes {
		forwarded[name] = [2]string{aws.ToString(value.DataType), aws.ToString(value.StringValue)}
	}
	checkEq("RawMessageDelivery.attributes_are_forwarded", forwarded, map[string][2]string{
		"event": {"String", "order.created"},
		"count": {"Number", "7"},
	})
	// The digest over those forwarded attributes, computed with AWS's own
	// algorithm — the one aws-sdk-go-v2 does not check for us.
	checkEq("RawMessageDelivery.attribute_md5_is_aws_shaped",
		aws.ToString(message.MD5OfMessageAttributes),
		md5OfMessageAttributes(map[string]types.MessageAttributeValue{
			"event": stringAttr("order.created"),
			"count": typedAttr("Number", "7"),
		}))
	checkEq("RawMessageDelivery.body_md5", aws.ToString(message.MD5OfBody), md5OfBody(body))
}

func tSnsFilterPolicy(ctx context.Context, r *rig) {
	_, topic, made := r.newTopic(ctx, "FilterPolicy", "sns-filter", nil)
	if !made {
		return
	}
	queue, ok := r.newQueue(ctx, "FilterPolicy", "sns-filter-queue", nil, nil)
	if !ok {
		return
	}
	subscription, ok := r.subscribeQueue(ctx, "FilterPolicy", topic, queue.arn, nil)
	if !ok {
		return
	}

	policy := `{"event":["order.created"]}`
	_, err := r.sns.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{
		SubscriptionArn: &subscription,
		AttributeName:   aws.String("FilterPolicy"),
		AttributeValue:  &policy,
	})
	if !checkNoErr("SetSubscriptionAttributes.filter_policy_succeeds", err) {
		return
	}
	read, err := r.sns.GetSubscriptionAttributes(ctx,
		&sns.GetSubscriptionAttributesInput{SubscriptionArn: &subscription})
	if checkNoErr("SetSubscriptionAttributes.filter_policy_reads_back_succeeds", err) {
		checkEq("SetSubscriptionAttributes.filter_policy_reads_back",
			normalizeJSON(read.Attributes["FilterPolicy"]), normalizeJSON(policy))
		checkEq("SetSubscriptionAttributes.filter_scope_defaults_to_attributes",
			read.Attributes["FilterPolicyScope"], "MessageAttributes")
	}

	emit := func(who, text, event string) string {
		in := &sns.PublishInput{TopicArn: &topic, Message: &text}
		if event != "" {
			in.MessageAttributes = map[string]snstypes.MessageAttributeValue{
				"event": {DataType: aws.String("String"), StringValue: aws.String(event)},
			}
		}
		out, err := r.sns.Publish(ctx, in)
		if !checkNoErr("FilterPolicy.publish_"+who, err) {
			return ""
		}
		return aws.ToString(out.MessageId)
	}

	// Non-matching FIRST, matching second: if the filter leaked, the leak is
	// already in the queue by the time the marker lands.
	emit("filtered_out", "filtered-out", "order.deleted")
	matched := emit("kept", "kept", "order.created")
	// An ABSENT attribute matches nothing but {"exists": false} — which is what
	// makes a filter policy a whitelist rather than a blacklist.
	emit("no_attributes", "no-attributes-at-all", "")
	marker := emit("marker", "marker", "order.created")

	got, extra := r.collectExactly(ctx, "FilterPolicy", queue.url, 2, 5*time.Second, 30*time.Second)
	delivered := envelopeMessages(got)
	sort.Strings(delivered)
	checkEq("FilterPolicy.only_matching_publishes_are_delivered", delivered, []string{"kept", "marker"})
	checkEq("FilterPolicy.nothing_else_was_behind_them", len(extra), 0)
	carried := make([]string, 0, len(got))
	for _, message := range got {
		carried = append(carried, envelopeText(envelopeOf(message), "MessageId"))
	}
	sort.Strings(carried)
	want := []string{matched, marker}
	sort.Strings(want)
	checkEq("FilterPolicy.matched_publish_ids_are_carried_through", carried, want)

	// An EMPTY value is SNS's spelling for taking a policy off. Storing "" would
	// leave a subscription whose policy matches nothing — a topic that silently
	// delivers to no one.
	_, err = r.sns.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{
		SubscriptionArn: &subscription,
		AttributeName:   aws.String("FilterPolicy"),
		AttributeValue:  aws.String(""),
	})
	if !checkNoErr("SetSubscriptionAttributes.empty_value_succeeds", err) {
		return
	}
	cleared, err := r.sns.GetSubscriptionAttributes(ctx,
		&sns.GetSubscriptionAttributesInput{SubscriptionArn: &subscription})
	if checkNoErr("SetSubscriptionAttributes.cleared_reads_back", err) {
		_, hasPolicy := cleared.Attributes["FilterPolicy"]
		_, hasScope := cleared.Attributes["FilterPolicyScope"]
		check("SetSubscriptionAttributes.empty_value_removes_the_policy", !hasPolicy && !hasScope,
			"got "+strings.Join(sortedKeys(cleared.Attributes), ", "))
	}

	emit("after_removal", "after-removal", "")
	after, _ := r.collectExactly(ctx, "FilterPolicy.after", queue.url, 1, 3*time.Second, 30*time.Second)
	checkEq("FilterPolicy.removal_restores_delivery", envelopeMessages(after), []string{"after-removal"})
}

// tProtocols turns the report's protocol line into a verdict. It runs last,
// because it reads the tally of everything the run put on the wire.
func tProtocols(_ context.Context, _ *rig) {
	counts := recorded.snapshot()
	spoken := map[string]int{}
	for key, n := range counts {
		service, protocol, found := strings.Cut(key, ": ")
		if !found {
			continue
		}
		switch {
		case strings.HasPrefix(protocol, "AWS JSON 1.0"):
			spoken[service+"/json"] += n
		case strings.HasPrefix(protocol, "Query/XML"):
			spoken[service+"/query"] += n
		default:
			spoken[service+"/other"] += n
		}
	}
	check("Protocol.the_sqs_client_spoke_aws_json_1_0",
		spoken["sqs/json"] > 0 && spoken["sqs/query"] == 0 && spoken["sqs/other"] == 0,
		fmt.Sprintf("%v", spoken))
	check("Protocol.the_sns_client_spoke_query_xml",
		spoken["sns/query"] > 0 && spoken["sns/json"] == 0 && spoken["sns/other"] == 0,
		fmt.Sprintf("%v", spoken))
	check("Protocol.every_request_was_sigv4_signed", spoken["unsigned/json"] == 0 &&
		spoken["unsigned/query"] == 0 && spoken["unsigned/other"] == 0,
		fmt.Sprintf("%v", spoken))
}

// ------------------------------------------------------------------ envelopes

// envelopeOf answers the SNS envelope inside an SQS body, or nil when the body
// is not a JSON object.
func envelopeOf(message types.Message) map[string]any {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(aws.ToString(message.Body)), &parsed); err != nil {
		return nil
	}
	return parsed
}

func envelopeText(envelope map[string]any, key string) string {
	if envelope == nil {
		return ""
	}
	if text, ok := envelope[key].(string); ok {
		return text
	}
	return ""
}

func envelopeMessages(messages []types.Message) []string {
	out := make([]string, 0, len(messages))
	for _, message := range messages {
		out = append(out, envelopeText(envelopeOf(message), "Message"))
	}
	return out
}

// envelopeAttributes flattens the notification's `MessageAttributes` — SNS's own
// `{"Type": …, "Value": …}` shape, which is NOT the SQS one — into something a
// single assertion can compare.
func envelopeAttributes(envelope map[string]any) map[string][2]string {
	out := map[string][2]string{}
	raw, ok := envelope["MessageAttributes"].(map[string]any)
	if !ok {
		return out
	}
	for name, value := range raw {
		entry, ok := value.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := entry["Type"].(string)
		text, _ := entry["Value"].(string)
		out[name] = [2]string{kind, text}
	}
	return out
}

// normalizeJSON re-renders a document so that two spellings of one policy
// compare equal: key order and whitespace are not part of what was stored.
func normalizeJSON(document string) string {
	var parsed any
	if err := json.Unmarshal([]byte(document), &parsed); err != nil {
		return document
	}
	rendered, err := json.Marshal(parsed)
	if err != nil {
		return document
	}
	return string(rendered)
}
