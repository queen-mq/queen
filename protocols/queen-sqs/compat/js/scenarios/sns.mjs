// `@aws-sdk/client-sns` against the same listener — and this is the half of the
// run that exercises the OTHER codec.
//
// SNS never moved to JSON: `@aws-sdk/client-sns` resolves `AwsQueryProtocol`
// (`protocol: config?.protocol ?? AwsQueryProtocol` in its runtime config), so
// every call below is form-encoded in and XML out, on the same port, through the
// same SigV4 verifier, signed for service `sns` rather than `sqs`.
// `scenarios/probe.mjs` proves that against a stub before anything is claimed,
// and `lib/stack.mjs` counts it per request off the real requests.
//
// The inventory mirrors `compat/smoke_m4_sns.py`, name for name. What this row
// adds over boto3's is the four-spelling error assertion (`expectSnsError`) and
// the modelled-class check — `catch (e) { if (e instanceof NotFoundException) }`
// is what a JS application actually writes, and it only works if the facade's
// `<Code>` is the shape's own code.

import {
  CreateTopicCommand,
  DeleteTopicCommand,
  GetSubscriptionAttributesCommand,
  GetTopicAttributesCommand,
  ListSubscriptionsByTopicCommand,
  ListTopicsCommand,
  PublishBatchCommand,
  PublishCommand,
  SetSubscriptionAttributesCommand,
  SubscribeCommand,
  UnsubscribeCommand,
  InvalidParameterException,
  NotFoundException,
} from "@aws-sdk/client-sns";
import { DeleteMessageCommand } from "@aws-sdk/client-sqs";

import { check, checkEq, fail, show } from "../lib/report.mjs";
import { bodyMd5 } from "../lib/md5.mjs";
import { makeQueue, receive } from "../lib/queue.mjs";
import { ACCOUNT, REGION, RUN, isUuid, topicArn as arnOfTopic } from "../lib/stack.mjs";

/** Every topic this run made, so the teardown can remove it. */
const TOPICS = [];

const SNS_ERRORS = {
  NotFound: [NotFoundException, "NotFoundException", 404],
  InvalidParameter: [InvalidParameterException, "InvalidParameterException", 400],
};

/**
 * SNS spells its errors ONCE — unlike SQS, which carries a legacy Query code
 * beside the shape name. The Query protocol puts the shape's own `error.code` in
 * `<Code>`, the SDK names the exception class after the SHAPE, and a facade that
 * answered `NotFoundException` in `<Code>` would still raise a
 * `SNSServiceException` and would break every modelled catch in the world.
 */
async function expectSnsError(name, code, call) {
  const [ctor, className, status] = SNS_ERRORS[code];
  try {
    await call();
  } catch (err) {
    checkEq(
      name,
      [err.constructor?.name, err.name, err.Code, err.Type, err.$metadata?.httpStatusCode],
      [className, className, code, "Sender", status],
    );
    check(`${name}.is_the_modelled_class`, err instanceof ctor, `got ${show(err.name)}: ${err.message}`);
    return err;
  }
  fail(name, `the call succeeded; expected ${code}`);
  return null;
}

async function makeTopic(sns, label, attributes) {
  const name = `js-m4-${label}-${RUN}`;
  const answer = await sns.send(new CreateTopicCommand({ Name: name, ...(attributes ? { Attributes: attributes } : {}) }));
  TOPICS.push(answer.TopicArn);
  return { name, arn: answer.TopicArn };
}

async function makeFifoTopic(sns, label, attributes) {
  const name = `js-m4-${label}-${RUN}.fifo`;
  const answer = await sns.send(
    new CreateTopicCommand({ Name: name, Attributes: { FifoTopic: "true", ...(attributes ?? {}) } }),
  );
  TOPICS.push(answer.TopicArn);
  return { name, arn: answer.TopicArn };
}

export async function teardownTopics(sns) {
  for (const arn of TOPICS.splice(0)) {
    try {
      await sns.send(new DeleteTopicCommand({ TopicArn: arn }));
    } catch {
      // A topic the scenario already deleted is not a teardown failure.
    }
  }
}

/** Receive up to `count`, DELETING each one (D2 again), until the timeout. */
async function collect(sqs, url, count, { timeoutMs = 20_000 } = {}) {
  const got = [];
  const deadline = Date.now() + timeoutMs;
  while (got.length < count && Date.now() < deadline) {
    const batch = await receive(sqs, url, {
      MaxNumberOfMessages: Math.min(10, count - got.length),
      WaitTimeSeconds: 2,
      MessageAttributeNames: ["All"],
      MessageSystemAttributeNames: ["All"],
    });
    for (const message of batch) {
      got.push(message);
      await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle }));
    }
  }
  return got;
}

/** `count` messages, and then a settling window that must stay empty. */
async function collectExactly(sqs, url, count, { settleMs = 4000 } = {}) {
  const got = await collect(sqs, url, count);
  const extra = await collect(sqs, url, 10, { timeoutMs: settleMs });
  return { got, extra };
}

/** The SNS envelope inside an SQS body, or null when the body is not JSON. */
function notification(message) {
  try {
    const parsed = JSON.parse(message.Body);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

const ISO8601_MS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

// ----------------------------------------------------------------- the topic

async function t_create_topic({ sns }) {
  const { name, arn } = await makeTopic(sns, "topic", { DisplayName: "M4 orders" });
  checkEq("CreateTopic.arn", arn, arnOfTopic(name));

  const again = await sns.send(new CreateTopicCommand({ Name: name, Attributes: { DisplayName: "M4 orders" } }));
  checkEq("CreateTopic.idempotent_identical_request", again.TopicArn, arn);
  const bare = await sns.send(new CreateTopicCommand({ Name: name }));
  checkEq("CreateTopic.idempotent_without_attributes", bare.TopicArn, arn);

  await expectSnsError("CreateTopic.conflicting_attribute_refused", "InvalidParameter", () =>
    sns.send(new CreateTopicCommand({ Name: name, Attributes: { DisplayName: "something else" } })),
  );

  const attributes = (await sns.send(new GetTopicAttributesCommand({ TopicArn: arn }))).Attributes ?? {};
  checkEq("GetTopicAttributes.arn", attributes.TopicArn, arn);
  checkEq("GetTopicAttributes.owner", attributes.Owner, ACCOUNT);
  checkEq("GetTopicAttributes.display_name", attributes.DisplayName, "M4 orders");
  checkEq(
    "GetTopicAttributes.subscription_counts",
    [attributes.SubscriptionsConfirmed, attributes.SubscriptionsPending, attributes.SubscriptionsDeleted],
    ["0", "0", "0"],
  );

  const listed = new Set();
  let token;
  for (let page = 0; page < 20; page += 1) {
    const answer = await sns.send(new ListTopicsCommand(token ? { NextToken: token } : {}));
    for (const topic of answer.Topics ?? []) listed.add(topic.TopicArn);
    token = answer.NextToken;
    if (!token) break;
  }
  check("ListTopics.contains_the_new_topic", listed.has(arn), `${arn} not among ${listed.size} topics`);
}

// ---------------------------------------------------------- the subscription

async function t_subscribe({ sns, sqs }) {
  const { arn: topic } = await makeTopic(sns, "sub-topic");
  const { arn: queueArn } = await makeQueue(sqs, "m4-sub-queue");

  const subscription = await sns.send(new SubscribeCommand({ TopicArn: topic, Protocol: "sqs", Endpoint: queueArn }));
  const arn = subscription.SubscriptionArn;

  // AUTO-CONFIRMED. AWS answers the literal string "pending confirmation" for a
  // subscription that needs a handshake; a same-account SQS subscription never
  // does, and a client that stored the placeholder would fail every later
  // `SetSubscriptionAttributes`.
  check("Subscribe.arn_is_not_pending_confirmation", arn !== "pending confirmation", `got ${arn}`);
  check(
    "Subscribe.arn_extends_the_topic_arn",
    typeof arn === "string" && arn.startsWith(`${topic}:`) && arn.split(":").length === 7,
    `got ${arn}`,
  );
  check("Subscribe.arn_id_is_a_uuid", isUuid(arn?.split(":").at(-1)), `got ${arn}`);

  const attributes = (await sns.send(new GetSubscriptionAttributesCommand({ SubscriptionArn: arn }))).Attributes ?? {};
  checkEq(
    "GetSubscriptionAttributes.identity",
    [attributes.SubscriptionArn, attributes.TopicArn, attributes.Protocol, attributes.Endpoint, attributes.Owner],
    [arn, topic, "sqs", queueArn, ACCOUNT],
  );
  checkEq(
    "GetSubscriptionAttributes.confirmed_at_creation",
    [attributes.PendingConfirmation, attributes.ConfirmationWasAuthenticated],
    ["false", "true"],
  );
  checkEq("GetSubscriptionAttributes.raw_message_delivery_defaults_off", attributes.RawMessageDelivery, "false");
  // No policy, so no scope: a provisioner that read one here would report drift
  // on every reconcile.
  check(
    "GetSubscriptionAttributes.no_filter_policy_scope_without_a_policy",
    !("FilterPolicyScope" in attributes) && !("FilterPolicy" in attributes),
    `got ${Object.keys(attributes).sort()}`,
  );

  const repeat = await sns.send(new SubscribeCommand({ TopicArn: topic, Protocol: "sqs", Endpoint: queueArn }));
  checkEq("Subscribe.idempotent_per_topic_protocol_endpoint", repeat.SubscriptionArn, arn);

  const listing = await sns.send(new ListSubscriptionsByTopicCommand({ TopicArn: topic }));
  const subscriptions = listing.Subscriptions ?? [];
  checkEq("ListSubscriptionsByTopic.count", subscriptions.length, 1);
  if (subscriptions.length) {
    checkEq(
      "ListSubscriptionsByTopic.entry",
      [
        subscriptions[0].SubscriptionArn,
        subscriptions[0].TopicArn,
        subscriptions[0].Protocol,
        subscriptions[0].Endpoint,
        subscriptions[0].Owner,
      ],
      [arn, topic, "sqs", queueArn, ACCOUNT],
    );
  }

  const counts = (await sns.send(new GetTopicAttributesCommand({ TopicArn: topic }))).Attributes ?? {};
  checkEq("GetTopicAttributes.confirmed_count_follows_subscribe", counts.SubscriptionsConfirmed, "1");

  // An unknown topic must NOT answer an empty list: a client reads that as
  // "nothing is subscribed" rather than "you asked about the wrong topic".
  await expectSnsError("ListSubscriptionsByTopic.unknown_topic_is_not_an_empty_list", "NotFound", () =>
    sns.send(new ListSubscriptionsByTopicCommand({ TopicArn: arnOfTopic(`js-ghost-${RUN}`) })),
  );

  // The two refusals a v0 subscriber meets.
  await expectSnsError("Subscribe.non_sqs_protocol_refused", "InvalidParameter", () =>
    sns.send(new SubscribeCommand({ TopicArn: topic, Protocol: "https", Endpoint: "https://example.invalid/hook" })),
  );
  await expectSnsError("Subscribe.endpoint_queue_must_exist", "InvalidParameter", () =>
    sns.send(
      new SubscribeCommand({
        TopicArn: topic,
        Protocol: "sqs",
        Endpoint: `arn:aws:sqs:${REGION}:${ACCOUNT}:js-absent-${RUN}`,
      }),
    ),
  );
}

// -------------------------------------------------------- the notification

async function t_publish_notification_envelope({ sns, sqs }) {
  const { arn: topic } = await makeTopic(sns, "notify-topic");
  const { url, arn: queueArn } = await makeQueue(sqs, "m4-notify-queue");
  await sns.send(new SubscribeCommand({ TopicArn: topic, Protocol: "sqs", Endpoint: queueArn }));

  const body = 'the payload, with spaces and a comma, and "quotes"';
  const blob = new Uint8Array([0, 1, 2, 114, 105, 103]);
  const published = await sns.send(
    new PublishCommand({
      TopicArn: topic,
      Message: body,
      Subject: "M4 subject",
      MessageAttributes: {
        event: { DataType: "String", StringValue: "order.created" },
        count: { DataType: "Number", StringValue: "7" },
        blob: { DataType: "Binary", BinaryValue: blob },
      },
    }),
  );
  check("Publish.message_id_is_a_uuid", isUuid(published.MessageId), `got ${published.MessageId}`);

  const got = await collect(sqs, url, 1);
  if (!checkEq("Publish.notification_arrives", got.length, 1)) return;
  const message = got[0];
  const envelope = notification(message);
  if (!check("Publish.body_is_json", envelope !== null, `body was ${show(message.Body)}`)) return;

  checkEq("Notification.type", envelope.Type, "Notification");
  checkEq("Notification.topic_arn", envelope.TopicArn, topic);
  checkEq("Notification.message", envelope.Message, body);
  checkEq("Notification.subject", envelope.Subject, "M4 subject");
  // THE PUBLISH's id, not the delivery's — it is what lets one fan-out be
  // correlated end to end, and it is AWS's own behaviour.
  checkEq("Notification.message_id_is_the_publishers", envelope.MessageId, published.MessageId);
  check(
    "Notification.timestamp_is_iso8601_millis",
    typeof envelope.Timestamp === "string" && ISO8601_MS.test(envelope.Timestamp),
    `got ${show(envelope.Timestamp)}`,
  );
  checkEq("Notification.signature_version", envelope.SignatureVersion, "1");
  checkEq("Notification.message_attributes", envelope.MessageAttributes, {
    event: { Type: "String", Value: "order.created" },
    count: { Type: "Number", Value: "7" },
    blob: { Type: "Binary", Value: Buffer.from(blob).toString("base64") },
  });
  // The three fields AWS writes and this deployment cannot stand behind. Their
  // absence is the honest half of an unsigned notification.
  check(
    "Notification.carries_no_unverifiable_signature_fields",
    !["Signature", "SigningCertURL", "UnsubscribeURL"].some((k) => k in envelope),
    `got ${Object.keys(envelope).sort()}`,
  );
  check(
    "Notification.no_sqs_message_attributes_in_envelope_mode",
    !message.MessageAttributes,
    `got ${show(message.MessageAttributes)}`,
  );
  checkEq("Notification.body_md5", message.MD5OfBody, bodyMd5(message.Body));
  check(
    "Notification.sqs_message_id_is_not_the_publish_id",
    message.MessageId !== published.MessageId,
    "the delivery reused the publish's id",
  );
}

// --------------------------------------------------------- raw delivery

async function t_raw_message_delivery({ sns, sqs }) {
  const { arn: topic } = await makeTopic(sns, "raw-topic");
  const { url, arn: queueArn } = await makeQueue(sqs, "m4-raw-queue");
  const { SubscriptionArn: arn } = await sns.send(
    new SubscribeCommand({ TopicArn: topic, Protocol: "sqs", Endpoint: queueArn }),
  );

  await sns.send(
    new SetSubscriptionAttributesCommand({
      SubscriptionArn: arn,
      AttributeName: "RawMessageDelivery",
      AttributeValue: "true",
    }),
  );
  const read = (await sns.send(new GetSubscriptionAttributesCommand({ SubscriptionArn: arn }))).Attributes ?? {};
  checkEq("SetSubscriptionAttributes.raw_message_delivery_reads_back", read.RawMessageDelivery, "true");

  const body = '{"order":42,"note":"raw, not enveloped"}';
  await sns.send(
    new PublishCommand({
      TopicArn: topic,
      Message: body,
      Subject: "ignored in raw mode",
      MessageAttributes: {
        event: { DataType: "String", StringValue: "order.created" },
        count: { DataType: "Number", StringValue: "7" },
      },
    }),
  );

  const got = await collect(sqs, url, 1);
  if (!checkEq("RawMessageDelivery.arrives", got.length, 1)) return;
  const message = got[0];

  // THE WHOLE MEANING OF "RAW": a consumer written against a queue reads the
  // body it was sent and never learns a topic was involved.
  checkEq("RawMessageDelivery.body_is_the_message_alone", message.Body, body);
  checkEq("RawMessageDelivery.body_is_not_an_envelope", notification(message)?.Type, undefined);
  checkEq(
    "RawMessageDelivery.attributes_are_forwarded",
    Object.fromEntries(
      Object.entries(message.MessageAttributes ?? {}).map(([name, value]) => [name, [value.DataType, value.StringValue]]),
    ),
    { event: ["String", "order.created"], count: ["Number", "7"] },
  );
  check(
    "RawMessageDelivery.attribute_md5_is_present",
    Boolean(message.MD5OfMessageAttributes),
    "no MD5OfMessageAttributes on a delivery carrying attributes",
  );
  checkEq("RawMessageDelivery.body_md5", message.MD5OfBody, bodyMd5(body));
}

// ------------------------------------------------------------ filter policy

async function t_filter_policy({ sns, sqs }) {
  const { arn: topic } = await makeTopic(sns, "filter-topic");
  const { url, arn: queueArn } = await makeQueue(sqs, "m4-filter-queue");
  const { SubscriptionArn: arn } = await sns.send(
    new SubscribeCommand({ TopicArn: topic, Protocol: "sqs", Endpoint: queueArn }),
  );

  const policy = { event: ["order.created"] };
  await sns.send(
    new SetSubscriptionAttributesCommand({
      SubscriptionArn: arn,
      AttributeName: "FilterPolicy",
      AttributeValue: JSON.stringify(policy),
    }),
  );
  const read = (await sns.send(new GetSubscriptionAttributesCommand({ SubscriptionArn: arn }))).Attributes ?? {};
  checkEq("SetSubscriptionAttributes.filter_policy_reads_back", JSON.parse(read.FilterPolicy ?? "null"), policy);
  checkEq("SetSubscriptionAttributes.filter_scope_defaults_to_attributes", read.FilterPolicyScope, "MessageAttributes");

  const emit = async (text, event) =>
    (
      await sns.send(
        new PublishCommand({
          TopicArn: topic,
          Message: text,
          ...(event ? { MessageAttributes: { event: { DataType: "String", StringValue: event } } } : {}),
        }),
      )
    ).MessageId;

  // Non-matching first, matching second: if the filter leaked, the leak is
  // already in the queue by the time the marker lands.
  await emit("filtered-out", "order.deleted");
  const matched = await emit("kept", "order.created");
  // An ABSENT attribute matches nothing but `{"exists": false}` — which is what
  // makes a filter policy a whitelist rather than a blacklist.
  await emit("no-attributes-at-all");
  const marker = await emit("marker", "order.created");

  const { got, extra } = await collectExactly(sqs, url, 2);
  checkEq(
    "FilterPolicy.only_matching_publishes_are_delivered",
    got.map((m) => notification(m)?.Message).sort(),
    ["kept", "marker"],
  );
  checkEq("FilterPolicy.nothing_else_was_behind_them", extra.map((m) => m.Body), []);
  checkEq(
    "FilterPolicy.matched_publish_ids_are_carried_through",
    got.map((m) => notification(m)?.MessageId).sort(),
    [matched, marker].sort(),
  );

  // An EMPTY value is SNS's spelling for taking a policy off. Storing "" would
  // leave a subscription whose policy matches nothing — a topic that silently
  // delivers to no one.
  await sns.send(
    new SetSubscriptionAttributesCommand({ SubscriptionArn: arn, AttributeName: "FilterPolicy", AttributeValue: "" }),
  );
  const cleared = (await sns.send(new GetSubscriptionAttributesCommand({ SubscriptionArn: arn }))).Attributes ?? {};
  check(
    "SetSubscriptionAttributes.empty_value_removes_the_policy",
    !("FilterPolicy" in cleared) && !("FilterPolicyScope" in cleared),
    `got ${Object.keys(cleared).sort()}`,
  );
  await emit("after-removal");
  const after = await collect(sqs, url, 1);
  checkEq("FilterPolicy.removal_restores_delivery", after.map((m) => notification(m)?.Message), ["after-removal"]);
}

// ------------------------------------------------------------- publish batch

async function t_publish_batch({ sns, sqs }) {
  const { arn: topic } = await makeTopic(sns, "batch-topic");
  const { url, arn: queueArn } = await makeQueue(sqs, "m4-batch-queue");
  await sns.send(new SubscribeCommand({ TopicArn: topic, Protocol: "sqs", Endpoint: queueArn }));

  const answer = await sns.send(
    new PublishBatchCommand({
      TopicArn: topic,
      PublishBatchRequestEntries: [
        { Id: "one", Message: "batch-one" },
        { Id: "two", Message: "batch-two" },
        { Id: "three", Message: "batch-three" },
      ],
    }),
  );
  checkEq("PublishBatch.three_succeeded", answer.Successful?.length, 3);
  checkEq("PublishBatch.none_failed", answer.Failed ?? [], []);
  checkEq(
    "PublishBatch.ids_echo",
    (answer.Successful ?? []).map((s) => s.Id).sort(),
    ["one", "three", "two"],
  );
  check(
    "PublishBatch.every_entry_has_its_own_message_id",
    new Set((answer.Successful ?? []).map((s) => s.MessageId)).size === 3,
    "two entries shared a MessageId",
  );

  const { got, extra } = await collectExactly(sqs, url, 3);
  checkEq(
    "PublishBatch.all_three_are_delivered",
    got.map((m) => notification(m)?.Message).sort(),
    ["batch-one", "batch-three", "batch-two"],
  );
  checkEq("PublishBatch.nothing_else_was_behind_them", extra.map((m) => m.Body), []);
  checkEq(
    "PublishBatch.delivery_carries_the_publish_ids",
    got.map((m) => notification(m)?.MessageId).sort(),
    (answer.Successful ?? []).map((s) => s.MessageId).sort(),
  );
}

// ------------------------------------------------------------------- fanout

async function t_fanout_to_two_queues({ sns, sqs }) {
  const { arn: topic } = await makeTopic(sns, "fanout-topic");
  const first = await makeQueue(sqs, "m4-fanout-a");
  const second = await makeQueue(sqs, "m4-fanout-b");
  await sns.send(new SubscribeCommand({ TopicArn: topic, Protocol: "sqs", Endpoint: first.arn }));
  await sns.send(new SubscribeCommand({ TopicArn: topic, Protocol: "sqs", Endpoint: second.arn }));

  const published = await sns.send(new PublishCommand({ TopicArn: topic, Message: "to both" }));

  const a = await collect(sqs, first.url, 1);
  const b = await collect(sqs, second.url, 1);
  checkEq("Fanout.first_queue_received", a.map((m) => notification(m)?.Message), ["to both"]);
  checkEq("Fanout.second_queue_received", b.map((m) => notification(m)?.Message), ["to both"]);
  // One publish, one MessageId, two deliveries — the fan-out is ONE
  // transaction and both copies carry the publish's own id.
  checkEq(
    "Fanout.both_copies_carry_one_publish_id",
    [notification(a[0] ?? {})?.MessageId, notification(b[0] ?? {})?.MessageId],
    [published.MessageId, published.MessageId],
  );
  check(
    "Fanout.the_two_deliveries_are_distinct_messages",
    a[0]?.MessageId !== b[0]?.MessageId,
    "the two queues answered one SQS MessageId",
  );

  const counts = (await sns.send(new GetTopicAttributesCommand({ TopicArn: topic }))).Attributes ?? {};
  checkEq("Fanout.topic_reports_two_confirmed_subscriptions", counts.SubscriptionsConfirmed, "2");
}

// --------------------------------------------------------------------- fifo

async function t_fifo_topic({ sns, sqs }) {
  const { arn: topic } = await makeFifoTopic(sns, "fifo-topic");
  const { url, arn: queueArn } = await makeQueue(sqs, "m4-fifo-queue", {
    fifo: true,
    attributes: { FifoQueue: "true" },
  });
  await sns.send(new SubscribeCommand({ TopicArn: topic, Protocol: "sqs", Endpoint: queueArn }));

  const group = "g-order";
  for (const [i, text] of ["one", "two", "three"].entries()) {
    await sns.send(
      new PublishCommand({
        TopicArn: topic,
        Message: text,
        MessageGroupId: group,
        MessageDeduplicationId: `${RUN}-fifo-${i}`,
      }),
    );
  }
  // The repeat: same dedup id, different body. Inside the window it writes
  // nothing and the original stands.
  await sns.send(
    new PublishCommand({
      TopicArn: topic,
      Message: "one again",
      MessageGroupId: group,
      MessageDeduplicationId: `${RUN}-fifo-0`,
    }),
  );

  const { got, extra } = await collectExactly(sqs, url, 3);
  checkEq(
    "FifoTopic.order_is_the_publish_order",
    got.map((m) => notification(m)?.Message),
    ["one", "two", "three"],
  );
  checkEq("FifoTopic.the_repeated_dedup_id_was_suppressed", extra.map((m) => m.Body), []);
  checkEq(
    "FifoTopic.group_id_reaches_the_queue",
    got.map((m) => m.Attributes?.MessageGroupId),
    [group, group, group],
  );
}

// ------------------------------------------------------------ delete cascade

async function t_delete_topic_cascades({ sns, sqs }) {
  const { arn: topic } = await makeTopic(sns, "cascade-topic");
  const { url, arn: queueArn } = await makeQueue(sqs, "m4-cascade-queue");
  const { SubscriptionArn: arn } = await sns.send(
    new SubscribeCommand({ TopicArn: topic, Protocol: "sqs", Endpoint: queueArn }),
  );

  await sns.send(new UnsubscribeCommand({ SubscriptionArn: arn }));
  await expectSnsError("Unsubscribe.the_subscription_is_gone", "NotFound", () =>
    sns.send(new GetSubscriptionAttributesCommand({ SubscriptionArn: arn })),
  );
  const listing = await sns.send(new ListSubscriptionsByTopicCommand({ TopicArn: topic }));
  checkEq("Unsubscribe.topic_lists_no_subscriptions", listing.Subscriptions ?? [], []);

  // A publish to a topic nobody is subscribed to succeeds and delivers nothing.
  await sns.send(new PublishCommand({ TopicArn: topic, Message: "into the void" }));
  const nothing = await collect(sqs, url, 1, { timeoutMs: 5000 });
  checkEq("Unsubscribe.publish_after_unsubscribe_delivers_nothing", nothing.map((m) => m.Body), []);

  await sns.send(new DeleteTopicCommand({ TopicArn: topic }));
  await expectSnsError("DeleteTopic.the_topic_is_gone", "NotFound", () =>
    sns.send(new GetTopicAttributesCommand({ TopicArn: topic })),
  );
  await expectSnsError("DeleteTopic.publishing_to_it_is_refused", "NotFound", () =>
    sns.send(new PublishCommand({ TopicArn: topic, Message: "too late" })),
  );
}

export const tests = [
  t_create_topic,
  t_subscribe,
  t_publish_notification_envelope,
  t_raw_message_delivery,
  t_filter_policy,
  t_publish_batch,
  t_fanout_to_two_queues,
  t_fifo_topic,
  t_delete_topic_cascades,
];
