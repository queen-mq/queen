// `@aws-sdk/client-sqs` against a live queen-sqs, a live broker and a live
// Postgres.
//
// The inventory is the python smoke's (`compat/smoke_m0.py`), assertion for
// assertion and NAME for name wherever the two suites assert the same fact.
// That is what makes a client matrix worth running: an assertion that fails in
// this row and passes in the boto3 row is a CLIENT difference, and one that
// fails in both is the facade. Where this row can assert something boto3 cannot,
// it does, and those names are new:
//
//   * the SDK's own MD5 validation is live here (boto3 has none), so a corrupt
//     body digest would raise inside the SDK before the suite ever compared it;
//     the attribute digests are still nobody's but ours (`lib/md5.mjs`);
//   * SQS's errors carry FOUR spellings to a JS caller — the modelled class,
//     `name`, the legacy `Code` and the `Type` fault — and `expectSqsError`
//     pins all four, where boto3 has three;
//   * `Binary` message attributes arrive as `Uint8Array`, so the round trip is
//     asserted over bytes rather than over base64 text.
//
// D2 (`M0_SMOKE.md`) is the one property of the facade a reader of this file
// must know: a standard queue hands out at most one message per LANE at a time,
// so nothing here reads more messages than a queue has partitions without
// deleting as it goes. See `lib/queue.mjs`.

import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageBatchCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ListDeadLetterSourceQueuesCommand,
  ListQueueTagsCommand,
  ListQueuesCommand,
  PurgeQueueCommand,
  ReceiveMessageCommand,
  SendMessageBatchCommand,
  SendMessageCommand,
  SetQueueAttributesCommand,
  TagQueueCommand,
  UntagQueueCommand,
} from "@aws-sdk/client-sqs";

import { check, checkEq, note } from "../lib/report.mjs";
import { attributesMd5, bodyMd5, systemAttributesMd5 } from "../lib/md5.mjs";
import {
  ACCOUNT,
  depth,
  drain,
  drainDeleting,
  expectOk,
  expectSqsError,
  expectUnmodelledError,
  ghostUrl,
  hold,
  makeQueue,
  receive,
  send,
  until,
} from "../lib/queue.mjs";
import { ENDPOINT, PARTITIONS, RUN, isUuid, looksLikeEpochMillis, makeSqs, queueArn, sleep } from "../lib/stack.mjs";

// ------------------------------------------------------------------ queue CRUD

async function t_create_queue({ sqs }) {
  const attributes = {
    VisibilityTimeout: "30",
    MessageRetentionPeriod: "3600",
    MaximumMessageSize: "262144",
    DelaySeconds: "0",
    ReceiveMessageWaitTimeSeconds: "0",
  };
  const { name, url } = await makeQueue(sqs, "crud", { attributes, tags: { team: "billing", env: "rig" } });
  checkEq("CreateQueue.url", url, `${ENDPOINT}/${ACCOUNT}/${name}`);

  // The idempotent create every framework performs at worker startup, in the
  // three shapes a provisioner performs it. AWS refuses only a request that
  // CONTRADICTS the queue — `M0_SMOKE.md` D1 was this rule getting it backwards.
  const identical = await sqs.send(new CreateQueueCommand({ QueueName: name, Attributes: attributes }));
  checkEq("CreateQueue.idempotent_identical_request", identical.QueueUrl, url);
  const bare = await sqs.send(new CreateQueueCommand({ QueueName: name }));
  checkEq("CreateQueue.repeat_without_attributes_is_idempotent", bare.QueueUrl, url);
  const subset = await sqs.send(
    new CreateQueueCommand({ QueueName: name, Attributes: { VisibilityTimeout: "30" } }),
  );
  checkEq("CreateQueue.repeat_with_a_subset_is_idempotent", subset.QueueUrl, url);

  await expectSqsError("CreateQueue.conflicting_attribute_refused", "QueueNameExists", () =>
    sqs.send(new CreateQueueCommand({ QueueName: name, Attributes: { VisibilityTimeout: "45" } })),
  );

  const tags = await sqs.send(new ListQueueTagsCommand({ QueueUrl: url }));
  checkEq("CreateQueue.tags_are_stored", tags.Tags, { team: "billing", env: "rig" });

  // Tags are NOT attributes: a repeat naming different ones is not a conflict,
  // and it does not rewrite them either — `TagQueue` is the action for that.
  const retagged = await sqs.send(new CreateQueueCommand({ QueueName: name, tags: { team: "other" } }));
  checkEq("CreateQueue.repeat_with_other_tags_succeeds", retagged.QueueUrl, url);
  const after = await sqs.send(new ListQueueTagsCommand({ QueueUrl: url }));
  checkEq("CreateQueue.repeat_does_not_rewrite_tags", after.Tags, { team: "billing", env: "rig" });
}

async function t_get_queue_url_and_list({ sqs }) {
  const { name, url } = await makeQueue(sqs, "lookup");

  const found = await sqs.send(new GetQueueUrlCommand({ QueueName: name }));
  checkEq("GetQueueUrl.round_trip", found.QueueUrl, url);

  const listed = await sqs.send(new ListQueuesCommand({ QueueNamePrefix: `js-lookup-${RUN}` }));
  checkEq("ListQueues.prefix_filters", listed.QueueUrls, [url]);

  const nothing = await sqs.send(new ListQueuesCommand({ QueueNamePrefix: `js-absent-${RUN}` }));
  checkEq("ListQueues.prefix_that_matches_nothing", nothing.QueueUrls ?? [], []);

  const all = await sqs.send(new ListQueuesCommand({}));
  check("ListQueues.contains_the_queue", (all.QueueUrls ?? []).includes(url), `${url} was not listed`);
}

async function t_queue_attributes({ sqs }) {
  const { name, url } = await makeQueue(sqs, "attrs", { attributes: { VisibilityTimeout: "30" } });

  const all = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: ["All"] }));
  const answered = all.Attributes ?? {};
  for (const expected of [
    "QueueArn",
    "CreatedTimestamp",
    "LastModifiedTimestamp",
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible",
    "ApproximateNumberOfMessagesDelayed",
    "VisibilityTimeout",
    "MessageRetentionPeriod",
    "MaximumMessageSize",
    "DelaySeconds",
    "ReceiveMessageWaitTimeSeconds",
  ]) {
    check(`GetQueueAttributes.all_has_${expected}`, expected in answered, `got ${Object.keys(answered).sort()}`);
  }
  checkEq("GetQueueAttributes.all_has_the_queues_arn", answered.QueueArn, queueArn(name));
  checkEq("GetQueueAttributes.all_has_the_visibility_that_was_set", answered.VisibilityTimeout, "30");
  // This facade's own extension, under `All`, which is not an AWS attribute and
  // which every client in the matrix has so far ignored cleanly.
  checkEq("GetQueueAttributes.all_has_queen_partitions", answered["queen.partitions"], String(PARTITIONS));

  const exact = await sqs.send(
    new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: ["VisibilityTimeout", "QueueArn"] }),
  );
  checkEq("GetQueueAttributes.selection_is_exact", Object.keys(exact.Attributes ?? {}).sort(), [
    "QueueArn",
    "VisibilityTimeout",
  ]);

  await expectSqsError("GetQueueAttributes.unknown_attribute_refused", "InvalidAttributeName", () =>
    sqs.send(new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: ["NotAnAttribute"] })),
  );

  await sqs.send(new SetQueueAttributesCommand({ QueueUrl: url, Attributes: { VisibilityTimeout: "45" } }));
  const changed = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: url,
      AttributeNames: ["VisibilityTimeout", "MessageRetentionPeriod"],
    }),
  );
  checkEq("SetQueueAttributes.applies", changed.Attributes?.VisibilityTimeout, "45");
  // MERGES rather than replaces: a `SetQueueAttributes` naming one attribute
  // must not reset the others to their defaults.
  checkEq(
    "SetQueueAttributes.merges_rather_than_replaces",
    changed.Attributes?.MessageRetentionPeriod,
    "345600",
  );

  await expectSqsError("SetQueueAttributes.unknown_attribute_refused", "InvalidAttributeName", () =>
    sqs.send(new SetQueueAttributesCommand({ QueueUrl: url, Attributes: { NotAnAttribute: "1" } })),
  );
}

async function t_tags({ sqs }) {
  const { url } = await makeQueue(sqs, "tags");

  await sqs.send(new TagQueueCommand({ QueueUrl: url, Tags: { owner: "alice", stage: "rig" } }));
  const added = await sqs.send(new ListQueueTagsCommand({ QueueUrl: url }));
  checkEq("TagQueue.adds", added.Tags, { owner: "alice", stage: "rig" });

  await sqs.send(new TagQueueCommand({ QueueUrl: url, Tags: { stage: "matrix" } }));
  const overwritten = await sqs.send(new ListQueueTagsCommand({ QueueUrl: url }));
  checkEq("TagQueue.overwrites_one_key", overwritten.Tags, { owner: "alice", stage: "matrix" });

  await sqs.send(new UntagQueueCommand({ QueueUrl: url, TagKeys: ["owner"] }));
  const removed = await sqs.send(new ListQueueTagsCommand({ QueueUrl: url }));
  checkEq("UntagQueue.removes", removed.Tags, { stage: "matrix" });
}

// -------------------------------------------------------------------- sending

async function t_send_with_message_attributes({ sqs }) {
  const { url } = await makeQueue(sqs, "send");
  const body = 'a body with spaces, a comma, and "quotes"';
  const binary = new Uint8Array([0, 1, 2, 255]);
  const attributes = {
    str: { DataType: "String", StringValue: "hello" },
    num: { DataType: "Number", StringValue: "42" },
    bin: { DataType: "Binary", BinaryValue: binary },
    custom: { DataType: "String.email", StringValue: "alice@example.invalid" },
  };
  const system = { AWSTraceHeader: { DataType: "String", StringValue: "Root=1-5759e988-bd862e3fe1be46a994272793" } };

  const sent = await sqs.send(
    new SendMessageCommand({
      QueueUrl: url,
      MessageBody: body,
      MessageAttributes: attributes,
      MessageSystemAttributes: system,
    }),
  );

  check("SendMessage.message_id_is_a_uuid", isUuid(sent.MessageId), `got ${sent.MessageId}`);
  // The SDK already checked this one and would have raised; it is asserted
  // anyway, because "the SDK did not raise" is not a line in the report.
  checkEq("SendMessage.md5_of_body", sent.MD5OfMessageBody, bodyMd5(body));
  // These two the SDK does NOT check — see `scenarios/probe.mjs`.
  checkEq("SendMessage.md5_of_attributes", sent.MD5OfMessageAttributes, attributesMd5(attributes));
  checkEq(
    "SendMessage.md5_of_system_attributes",
    sent.MD5OfMessageSystemAttributes,
    systemAttributesMd5(system),
  );

  const got = await drain(sqs, url, 1, {
    MessageAttributeNames: ["All"],
    MessageSystemAttributeNames: ["All"],
  });
  if (!checkEq("ReceiveMessage.one_message_came_back", got.length, 1)) return;
  const message = got[0];

  checkEq("ReceiveMessage.body", message.Body, body);
  checkEq("ReceiveMessage.message_id_matches_send", message.MessageId, sent.MessageId);
  checkEq("ReceiveMessage.md5_of_body", message.MD5OfBody, bodyMd5(body));
  checkEq("ReceiveMessage.md5_of_attributes", message.MD5OfMessageAttributes, attributesMd5(attributes));
  checkEq("ReceiveMessage.string_attribute", message.MessageAttributes?.str?.StringValue, "hello");
  checkEq("ReceiveMessage.number_attribute", message.MessageAttributes?.num?.StringValue, "42");
  checkEq("ReceiveMessage.number_attribute_keeps_its_type", message.MessageAttributes?.num?.DataType, "Number");
  // Bytes, not base64: the JS SDK hands a `Uint8Array` back, and a facade that
  // returned the base64 text would round-trip as a longer, different value.
  checkEq("ReceiveMessage.binary_attribute", message.MessageAttributes?.bin?.BinaryValue, binary);
  checkEq("ReceiveMessage.custom_data_type_survives", message.MessageAttributes?.custom?.DataType, "String.email");
  checkEq(
    "ReceiveMessage.system_attribute_round_trips",
    message.Attributes?.AWSTraceHeader,
    "Root=1-5759e988-bd862e3fe1be46a994272793",
  );
  checkEq("ReceiveMessage.receive_count_is_one", message.Attributes?.ApproximateReceiveCount, "1");
  check(
    "ReceiveMessage.sent_timestamp_is_epoch_millis",
    looksLikeEpochMillis(message.Attributes?.SentTimestamp),
    `got ${message.Attributes?.SentTimestamp}`,
  );

  // A receive that asked for no attribute names gets none — and therefore no
  // attribute digest either, since a digest over an empty map is not the same
  // as an absent field.
  await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle }));
  await send(sqs, url, { MessageBody: "plain", MessageAttributes: attributes });
  const bare = await drain(sqs, url, 1);
  if (checkEq("ReceiveMessage.bare_receive_returned_a_message", bare.length, 1)) {
    checkEq("ReceiveMessage.no_attributes_asked_none_answered", bare[0].MessageAttributes, undefined);
    checkEq("ReceiveMessage.no_attribute_digest_without_attributes", bare[0].MD5OfMessageAttributes, undefined);
    await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: bare[0].ReceiptHandle }));
  }
}

async function t_send_batch({ sqs }) {
  const { url } = await makeQueue(sqs, "batch");
  const attributes = { tag: { DataType: "String", StringValue: "batched" } };
  const entries = Array.from({ length: 10 }, (_, i) => ({
    Id: `e${i}`,
    MessageBody: `batch body ${i}`,
    ...(i % 2 === 0 ? { MessageAttributes: attributes } : {}),
  }));

  const answer = await sqs.send(new SendMessageBatchCommand({ QueueUrl: url, Entries: entries }));
  checkEq("SendMessageBatch.ten_succeeded", answer.Successful?.length, 10);
  checkEq("SendMessageBatch.none_failed", answer.Failed ?? [], []);
  checkEq(
    "SendMessageBatch.ids_echo",
    (answer.Successful ?? []).map((s) => s.Id).sort(),
    entries.map((e) => e.Id).sort(),
  );
  const ids = new Set((answer.Successful ?? []).map((s) => s.MessageId));
  checkEq("SendMessageBatch.message_ids_are_distinct", ids.size, 10);

  const byId = Object.fromEntries((answer.Successful ?? []).map((s) => [s.Id, s]));
  const bodyDigestsMatch = entries.every((e) => byId[e.Id]?.MD5OfMessageBody === bodyMd5(e.MessageBody));
  check("SendMessageBatch.per_entry_body_md5", bodyDigestsMatch, "an entry's body digest did not match");
  const attributeDigestsMatch = entries.every((e) =>
    e.MessageAttributes
      ? byId[e.Id]?.MD5OfMessageAttributes === attributesMd5(e.MessageAttributes)
      : byId[e.Id]?.MD5OfMessageAttributes === undefined,
  );
  check(
    "SendMessageBatch.per_entry_attribute_md5",
    attributeDigestsMatch,
    "an entry's attribute digest did not match, or was answered for an entry that sent none",
  );

  // Ten messages through a queue with eight lanes: only a receive-and-delete
  // loop can collect them all (D2).
  const drained = await drainDeleting(sqs, url, 10);
  checkEq("SendMessageBatch.all_ten_are_receivable", drained.length, 10);
  checkEq(
    "SendMessageBatch.bodies_round_trip",
    drained.map((m) => m.Body).sort(),
    entries.map((e) => e.MessageBody).sort(),
  );
  const empty = await until(async () => {
    const counts = await depth(sqs, url);
    return counts.messages === 0 && counts.notVisible === 0 ? counts : null;
  });
  check(
    "SendMessageBatch.queue_is_empty_afterwards",
    empty !== null,
    `depth was ${JSON.stringify(await depth(sqs, url))}`,
  );
}

async function t_batch_limits({ sqs }) {
  const { url } = await makeQueue(sqs, "limits");

  await expectSqsError("SendMessageBatch.empty_batch_refused", "EmptyBatchRequest", () =>
    sqs.send(new SendMessageBatchCommand({ QueueUrl: url, Entries: [] })),
  );
  await expectSqsError("SendMessageBatch.eleven_entries_refused", "TooManyEntriesInBatchRequest", () =>
    sqs.send(
      new SendMessageBatchCommand({
        QueueUrl: url,
        Entries: Array.from({ length: 11 }, (_, i) => ({ Id: `e${i}`, MessageBody: "x" })),
      }),
    ),
  );
  await expectSqsError("SendMessageBatch.duplicate_ids_refused", "BatchEntryIdsNotDistinct", () =>
    sqs.send(
      new SendMessageBatchCommand({
        QueueUrl: url,
        Entries: [
          { Id: "same", MessageBody: "one" },
          { Id: "same", MessageBody: "two" },
        ],
      }),
    ),
  );
  await expectSqsError("DeleteMessageBatch.empty_batch_refused", "EmptyBatchRequest", () =>
    sqs.send(new DeleteMessageBatchCommand({ QueueUrl: url, Entries: [] })),
  );

  // An oversized body is a bad PARAMETER, with AWS's own sentence — and it has
  // no modelled class, so only three of the four spellings exist.
  const { url: small } = await makeQueue(sqs, "small", { attributes: { MaximumMessageSize: "1024" } });
  await expectUnmodelledError(
    "SendMessage.body_over_the_queue_maximum_refused",
    "InvalidParameterValue",
    "InvalidParameterValue",
    400,
    () => sqs.send(new SendMessageCommand({ QueueUrl: small, MessageBody: "x".repeat(2048) })),
  );
  await expectSqsError("SendMessageBatch.batch_over_the_queue_maximum_refused", "BatchRequestTooLong", () =>
    sqs.send(
      new SendMessageBatchCommand({
        QueueUrl: small,
        Entries: [
          { Id: "a", MessageBody: "x".repeat(600) },
          { Id: "b", MessageBody: "y".repeat(600) },
        ],
      }),
    ),
  );
}

// --------------------------------------------------------------- long polling

async function t_long_poll({ sqs }) {
  const { url } = await makeQueue(sqs, "poll");

  let started = Date.now();
  const short = await sqs.send(new ReceiveMessageCommand({ QueueUrl: url, WaitTimeSeconds: 0 }));
  const shortMs = Date.now() - started;
  checkEq("ReceiveMessage.short_poll_returns_empty", short.Messages ?? [], []);
  check("ReceiveMessage.short_poll_does_not_wait", shortMs < 1500, `took ${shortMs}ms`);

  started = Date.now();
  const long = await sqs.send(new ReceiveMessageCommand({ QueueUrl: url, WaitTimeSeconds: 3 }));
  const longMs = Date.now() - started;
  checkEq("ReceiveMessage.long_poll_returns_empty", long.Messages ?? [], []);
  check("ReceiveMessage.long_poll_waited", longMs >= 2500, `took ${longMs}ms, expected about 3000`);

  await send(sqs, url, { MessageBody: "already waiting" });
  started = Date.now();
  const early = await sqs.send(new ReceiveMessageCommand({ QueueUrl: url, WaitTimeSeconds: 20 }));
  const earlyMs = Date.now() - started;
  checkEq("ReceiveMessage.long_poll_finds_a_waiting_message", (early.Messages ?? []).length, 1);
  check(
    "ReceiveMessage.long_poll_returns_early_when_it_can",
    earlyMs < 5000,
    `took ${earlyMs}ms, which is most of the 20s window`,
  );
  if (early.Messages?.length) {
    await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: early.Messages[0].ReceiptHandle }));
  }
}

// ---------------------------------------------------------------- visibility

async function t_change_message_visibility({ sqs }) {
  const { url } = await makeQueue(sqs, "vis", { attributes: { VisibilityTimeout: "2" } });
  await send(sqs, url, { MessageBody: "visible-again" });

  const first = await drain(sqs, url, 1);
  if (!checkEq("ChangeMessageVisibility.first_receive", first.length, 1)) return;
  const handle = first[0].ReceiptHandle;

  // EXTEND. The queue's own visibility is 2s, so without this the message would
  // be back inside the window below; with it, it must not be.
  await sqs.send(new ChangeMessageVisibilityCommand({ QueueUrl: url, ReceiptHandle: handle, VisibilityTimeout: 120 }));
  const hidden = await drain(sqs, url, 1, { timeoutMs: 6000 });
  checkEq("ChangeMessageVisibility.extend_hides_the_message", hidden.length, 0);

  // TERMINATE. Zero releases it at once, which is how every consumer library
  // nacks — and how sqs-consumer's `terminateVisibilityTimeout` works.
  await sqs.send(new ChangeMessageVisibilityCommand({ QueueUrl: url, ReceiptHandle: handle, VisibilityTimeout: 0 }));
  const back = await drain(sqs, url, 1, { timeoutMs: 15_000, MessageSystemAttributeNames: ["All"] });
  if (!checkEq("ChangeMessageVisibility.zero_returns_the_message", back.length, 1)) return;
  checkEq("ChangeMessageVisibility.same_message_came_back", back[0].MessageId, first[0].MessageId);
  checkEq("ChangeMessageVisibility.body_survived_the_release", back[0].Body, "visible-again");
  checkEq(
    "ChangeMessageVisibility.receive_count_after_release",
    back[0].Attributes?.ApproximateReceiveCount,
    "2",
  );

  // The OLD handle names a lease that no longer exists. AWS's contract is that
  // it fails — as ReceiptHandleIsInvalid or MessageNotInflight, both of which
  // are in the catalog — rather than silently moving the new delivery.
  try {
    await sqs.send(
      new ChangeMessageVisibilityCommand({ QueueUrl: url, ReceiptHandle: handle, VisibilityTimeout: 60 }),
    );
    check("ChangeMessageVisibility.stale_handle_refused", false, "the stale handle was accepted");
  } catch (err) {
    check(
      "ChangeMessageVisibility.stale_handle_refused",
      ["ReceiptHandleIsInvalid", "MessageNotInflight"].includes(err.name),
      `got ${err.name}`,
    );
  }

  await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: back[0].ReceiptHandle }));
}

async function t_receive_count_after_expiry({ sqs }) {
  // The visibility timeout is a real lease on the BROKER, not a facade timer:
  // nothing here calls ChangeMessageVisibility. The message is received and
  // abandoned, and the assertion is that it came back on its own.
  const { url } = await makeQueue(sqs, "expiry", { attributes: { VisibilityTimeout: "2" } });
  await send(sqs, url, { MessageBody: "abandoned" });

  const first = await drain(sqs, url, 1, { MessageSystemAttributeNames: ["All"] });
  if (!checkEq("ReceiveCount.first_delivery", first.length, 1)) return;
  checkEq("ReceiveCount.first_delivery_is_one", first[0].Attributes?.ApproximateReceiveCount, "1");

  await sleep(3000);
  const second = await drain(sqs, url, 1, { timeoutMs: 30_000, MessageSystemAttributeNames: ["All"] });
  if (!checkEq("ReceiveCount.redelivered_after_the_lease_lapsed", second.length, 1)) return;
  checkEq("ReceiveCount.same_message", second[0].MessageId, first[0].MessageId);
  checkEq("ReceiveCount.second_delivery_is_two", second[0].Attributes?.ApproximateReceiveCount, "2");
  check(
    "ReceiveCount.a_redelivery_has_a_new_receipt_handle",
    second[0].ReceiptHandle !== first[0].ReceiptHandle,
    "the same handle came back",
  );

  await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: second[0].ReceiptHandle }));
}

// ------------------------------------------------------------------ deleting

async function t_delete_message({ sqs }) {
  const { url } = await makeQueue(sqs, "delete", { attributes: { VisibilityTimeout: "2" } });
  await send(sqs, url, { MessageBody: "to be deleted" });

  const got = await drain(sqs, url, 1);
  if (!checkEq("DeleteMessage.received", got.length, 1)) return;
  const handle = got[0].ReceiptHandle;

  await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: handle }));
  // An undeleted message is equally silent for its visibility timeout, so
  // silence alone proves nothing: the depth counters are what say it is gone.
  const gone = await until(async () => {
    const counts = await depth(sqs, url);
    return counts.messages === 0 && counts.notVisible === 0 ? counts : null;
  });
  check("DeleteMessage.does_not_come_back", gone !== null, `depth was ${JSON.stringify(await depth(sqs, url))}`);

  // AWS answers 200 to a repeated delete of a handle it has already honoured.
  await expectOk("DeleteMessage.double_delete_is_idempotent", () =>
    sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: handle })),
  );

  await expectSqsError("DeleteMessage.forged_handle_refused", "ReceiptHandleIsInvalid", () =>
    sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: "not-a-handle" })),
  );
}

async function t_delete_message_batch_partial({ sqs }) {
  const { url } = await makeQueue(sqs, "delbatch", { attributes: { VisibilityTimeout: "120" } });
  await sqs.send(
    new SendMessageBatchCommand({
      QueueUrl: url,
      Entries: [
        { Id: "a", MessageBody: "one" },
        { Id: "b", MessageBody: "two" },
        { Id: "c", MessageBody: "three" },
      ],
    }),
  );

  const held = await hold(sqs, url, 3);
  if (!checkEq("DeleteMessageBatch.received_three", held.length, 3)) return;

  const good = await sqs.send(
    new DeleteMessageBatchCommand({
      QueueUrl: url,
      Entries: held.map((m, i) => ({ Id: `d${i}`, ReceiptHandle: m.ReceiptHandle })),
    }),
  );
  checkEq("DeleteMessageBatch.all_succeeded", (good.Successful ?? []).length, 3);
  checkEq("DeleteMessageBatch.none_failed", good.Failed ?? [], []);

  // A forged handle among real ones is a PER-ENTRY failure and not a refusal of
  // the request: the whole point of the batch shape is that the good entries go
  // through.
  const more = await hold(sqs, url, 2);
  if (more.length === 2) {
    const mixed = await sqs.send(
      new DeleteMessageBatchCommand({
        QueueUrl: url,
        Entries: [
          { Id: "real", ReceiptHandle: more[0].ReceiptHandle },
          { Id: "forged", ReceiptHandle: "not-a-handle" },
          { Id: "real2", ReceiptHandle: more[1].ReceiptHandle },
        ],
      }),
    );
    checkEq(
      "DeleteMessageBatch.partial_success_ids",
      (mixed.Successful ?? []).map((s) => s.Id).sort(),
      ["real", "real2"],
    );
    checkEq(
      "DeleteMessageBatch.partial_failure_ids",
      (mixed.Failed ?? []).map((f) => f.Id),
      ["forged"],
    );
    checkEq("DeleteMessageBatch.failure_is_the_senders_fault", mixed.Failed?.[0]?.SenderFault, true);
    checkEq("DeleteMessageBatch.failure_entry_has_a_code", mixed.Failed?.[0]?.Code, "ReceiptHandleIsInvalid");
  } else {
    check("DeleteMessageBatch.partial_success_ids", false, "could not hold two messages in flight");
  }
}

// ------------------------------------------------------- per-message delay

async function t_delay_seconds({ sqs }) {
  // A per-message `DelaySeconds` is a TIMER in the broker, and the delayed
  // count is what `ApproximateNumberOfMessagesDelayed` reports.
  const { url } = await makeQueue(sqs, "delay");
  await send(sqs, url, { MessageBody: "delayed", DelaySeconds: 6 });

  // The invisibility is asserted FIRST and the counter second, in that order on
  // purpose: both are true only while the timer is still pending, so a slow
  // assertion in front of the fast one would spend the window it is testing.
  const immediate = await receive(sqs, url, { WaitTimeSeconds: 1 });
  checkEq("DelaySeconds.message_is_not_visible_yet", immediate.length, 0);

  const counted = await until(async () => ((await depth(sqs, url)).delayed >= 1 ? true : null), {
    timeoutMs: 3000,
    everyMs: 250,
  });
  check("DelaySeconds.delayed_count_reports_the_message", counted === true, "the delayed counter stayed at zero");

  const late = await drain(sqs, url, 1, { timeoutMs: 30_000 });
  if (!checkEq("DelaySeconds.message_arrives_after_the_delay", late.length, 1)) return;
  checkEq("DelaySeconds.body_survived_the_timer", late[0].Body, "delayed");
  await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: late[0].ReceiptHandle }));
}

// -------------------------------------------------------------------- purge

async function t_purge_queue({ sqs }) {
  const { url } = await makeQueue(sqs, "purge");
  await sqs.send(
    new SendMessageBatchCommand({
      QueueUrl: url,
      Entries: Array.from({ length: 5 }, (_, i) => ({ Id: `p${i}`, MessageBody: `purge ${i}` })),
    }),
  );
  await until(async () => ((await depth(sqs, url)).messages >= 5 ? true : null), { timeoutMs: 10_000 });

  await sqs.send(new PurgeQueueCommand({ QueueUrl: url }));
  // AWS answers a purge asynchronously and documents up to 60 seconds; the
  // assertion is that it empties, not that it empties instantly.
  const emptied = await until(
    async () => {
      const counts = await depth(sqs, url);
      return counts.messages === 0 && counts.notVisible === 0 ? counts : null;
    },
    { timeoutMs: 30_000 },
  );
  check("PurgeQueue.empties_the_queue", emptied !== null, `depth was ${JSON.stringify(await depth(sqs, url))}`);
  checkEq("PurgeQueue.receive_afterwards_is_empty", (await receive(sqs, url, { WaitTimeSeconds: 1 })).length, 0);

  // The 60-second cooldown, emulated because SDK retry behaviour depends on it.
  await expectSqsError("PurgeQueue.second_purge_is_refused_during_the_cooldown", "PurgeQueueInProgress", () =>
    sqs.send(new PurgeQueueCommand({ QueueUrl: url })),
  );
}

// --------------------------------------------------------------------- fifo

async function t_fifo({ sqs }) {
  const { url } = await makeQueue(sqs, "fifo", {
    fifo: true,
    attributes: { FifoQueue: "true", VisibilityTimeout: "30" },
  });

  const group = "orders-1";
  const sent = [];
  for (let i = 0; i < 5; i += 1) {
    sent.push(
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: url,
          MessageBody: `fifo ${i}`,
          MessageGroupId: group,
          MessageDeduplicationId: `${RUN}-fifo-${i}`,
        }),
      ),
    );
  }

  const sequences = sent.map((s) => s.SequenceNumber);
  check(
    "SequenceNumber.send_answers_one_per_message",
    sequences.every((s) => typeof s === "string" && /^\d+$/.test(s)),
    `got ${JSON.stringify(sequences)}`,
  );
  const ascending = sequences.every((s, i) => i === 0 || BigInt(s) > BigInt(sequences[i - 1]));
  check("SequenceNumber.send_side_is_ascending_within_the_group", ascending, `got ${JSON.stringify(sequences)}`);

  // The DEDUP window: a repeat of a MessageDeduplicationId inside it writes
  // nothing and the original stands. Five minutes by default on a `.fifo`
  // queue, which is AWS's own window.
  const repeat = await sqs.send(
    new SendMessageCommand({
      QueueUrl: url,
      MessageBody: "fifo 0 again",
      MessageGroupId: group,
      MessageDeduplicationId: `${RUN}-fifo-0`,
    }),
  );
  check("Dedup.a_repeat_is_accepted", typeof repeat.MessageId === "string", `got ${repeat.MessageId}`);

  const got = await drainDeleting(sqs, url, 5, {
    timeoutMs: 60_000,
    MessageSystemAttributeNames: ["All"],
    MessageAttributeNames: ["All"],
  });
  checkEq("Fifo.exactly_the_five_distinct_messages_came_back", got.length, 5);
  checkEq(
    "Fifo.order_within_the_group_is_the_send_order",
    got.map((m) => m.Body),
    ["fifo 0", "fifo 1", "fifo 2", "fifo 3", "fifo 4"],
  );
  // Five, and then a settling window that must stay empty: a drain that STOPPED
  // at five would prove nothing about a sixth message sitting behind them, and
  // the sixth is the whole question a dedup window answers.
  const behind = await drain(sqs, url, 5, { timeoutMs: 6000, MessageSystemAttributeNames: ["All"] });
  checkEq("Dedup.the_repeat_was_suppressed", behind.map((m) => m.Body), []);

  checkEq(
    "Fifo.group_id_comes_back",
    got.map((m) => m.Attributes?.MessageGroupId),
    Array(5).fill(group),
  );
  checkEq(
    "Fifo.dedup_ids_come_back",
    got.map((m) => m.Attributes?.MessageDeduplicationId),
    sent.map((_, i) => `${RUN}-fifo-${i}`),
  );
  // C-SQS-3: the number the send answered is the number the receive answers.
  // Until the broker's pop carried an offset per message, a facade could only
  // answer it on the way IN.
  checkEq(
    "SequenceNumber.receive_answers_what_the_send_answered",
    got.map((m) => m.Attributes?.SequenceNumber),
    sequences,
  );

  // And none of it exists on a standard queue, because AWS answers none there.
  const { url: plain } = await makeQueue(sqs, "notfifo");
  const standard = await sqs.send(new SendMessageCommand({ QueueUrl: plain, MessageBody: "standard" }));
  checkEq("SequenceNumber.absent_on_a_standard_send", standard.SequenceNumber, undefined);
  const back = await drain(sqs, plain, 1, { MessageSystemAttributeNames: ["All"] });
  if (checkEq("SequenceNumber.standard_message_received", back.length, 1)) {
    checkEq("SequenceNumber.absent_on_a_standard_receive", back[0].Attributes?.SequenceNumber, undefined);
    await sqs.send(new DeleteMessageCommand({ QueueUrl: plain, ReceiptHandle: back[0].ReceiptHandle }));
  }
}

// ---------------------------------------------------------------------- dlq

async function t_dlq_redrive({ sqs }) {
  const { name: dlqName, url: dlqUrl, arn: dlqArn } = await makeQueue(sqs, "dlq");
  const { name: sourceName, url: sourceUrl } = await makeQueue(sqs, "src", {
    attributes: {
      VisibilityTimeout: "1",
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: "2" }),
    },
  });

  const sent = await send(sqs, sourceUrl, { MessageBody: "poisoned" });

  // Two deliveries the consumer actually sees, each abandoned. The THIRD pop
  // does not return the message: the facade moves it, in one transaction.
  for (const attempt of [1, 2]) {
    const got = await until(
      async () => {
        const batch = await receive(sqs, sourceUrl, {
          WaitTimeSeconds: 1,
          MessageSystemAttributeNames: ["All"],
        });
        return batch.length ? batch : null;
      },
      { timeoutMs: 20_000, everyMs: 200 },
    );
    if (!check(`Redrive.delivery_${attempt}_arrived`, got !== null, "the source queue never delivered")) return;
    checkEq(`Redrive.delivery_${attempt}_receive_count`, got[0].Attributes?.ApproximateReceiveCount, String(attempt));
    await sleep(1500); // let the 1s lease lapse rather than terminating it
  }

  // THE MOVE HAPPENS ON A POP. Nothing moves a message in the background: the
  // facade checks the threshold between the pop and the answer, so the third
  // RECEIVE ON THE SOURCE is what triggers it — and that receive returns
  // nothing, because the message it would have answered was moved instead. A
  // suite that only polled the dead-letter queue would wait for ever.
  const third = await receive(sqs, sourceUrl, { WaitTimeSeconds: 2, MessageSystemAttributeNames: ["All"] });
  checkEq(
    "Redrive.the_over_threshold_delivery_is_not_returned",
    third.map((m) => m.Body),
    [],
  );

  const moved = await until(
    async () => {
      // Keep poking the source: one receive is up to ten `batch=1` pops and a
      // lane that was busy at that instant is a lane the move has not visited.
      await receive(sqs, sourceUrl, { WaitTimeSeconds: 1 });
      const batch = await receive(sqs, dlqUrl, {
        WaitTimeSeconds: 1,
        MessageSystemAttributeNames: ["All"],
      });
      return batch.length ? batch : null;
    },
    { timeoutMs: 40_000, everyMs: 200 },
  );
  if (!check("Redrive.message_reaches_the_dead_letter_queue", moved !== null, "nothing arrived in the DLQ in 40s")) {
    return;
  }
  checkEq("Redrive.body_survived_the_move", moved[0].Body, "poisoned");
  // AWS does not reset the count on a move, and this facade carries it in the
  // envelope: two deliveries seen on the source, plus this one.
  checkEq("Redrive.receive_count_continues_on_the_copy", moved[0].Attributes?.ApproximateReceiveCount, "3");
  // The copy is a new row with a new id, so the original rides in the envelope.
  checkEq(
    "Redrive.the_copy_names_the_original_message_id",
    moved[0].Attributes?.["queen.originalMessageId"],
    sent.MessageId,
  );
  checkEq("Redrive.the_copy_names_its_source_queue", moved[0].Attributes?.["queen.sourceQueue"], sourceName);
  await sqs.send(new DeleteMessageCommand({ QueueUrl: dlqUrl, ReceiptHandle: moved[0].ReceiptHandle }));

  // The source is empty: the move ACKED the original, in the same transaction
  // that pushed the copy.
  const drained = await until(
    async () => {
      const counts = await depth(sqs, sourceUrl);
      return counts.messages === 0 && counts.notVisible === 0 ? counts : null;
    },
    { timeoutMs: 20_000 },
  );
  check("Redrive.the_source_queue_is_empty_after_the_move", drained !== null, "the original was not acked");

  const sources = await sqs.send(new ListDeadLetterSourceQueuesCommand({ QueueUrl: dlqUrl }));
  checkEq("ListDeadLetterSourceQueues.names_the_source", sources.queueUrls ?? [], [sourceUrl]);
  note(`redrive: ${sourceName} -> ${dlqName}`);
}

// -------------------------------------------------------------------- errors

async function t_errors({ sqs }) {
  const missing = ghostUrl("errors");
  await expectSqsError("Errors.send_to_a_missing_queue", "QueueDoesNotExist", () =>
    sqs.send(new SendMessageCommand({ QueueUrl: missing, MessageBody: "x" })),
  );
  await expectSqsError("Errors.receive_on_a_missing_queue", "QueueDoesNotExist", () =>
    sqs.send(new ReceiveMessageCommand({ QueueUrl: missing })),
  );
  await expectSqsError("Errors.get_attributes_on_a_missing_queue", "QueueDoesNotExist", () =>
    sqs.send(new GetQueueAttributesCommand({ QueueUrl: missing, AttributeNames: ["All"] })),
  );
  await expectSqsError("Errors.get_queue_url_on_a_missing_queue", "QueueDoesNotExist", () =>
    sqs.send(new GetQueueUrlCommand({ QueueName: `js-ghost-${RUN}` })),
  );

  // Another account's queue reads as "does not exist", which is what AWS
  // answers and is not the same thing as a malformed request.
  const { name } = await makeQueue(sqs, "acct");
  await expectSqsError("Errors.queue_url_for_another_account", "QueueDoesNotExist", () =>
    sqs.send(new ReceiveMessageCommand({ QueueUrl: `${ENDPOINT}/999999999999/${name}` })),
  );

  // A wrong secret is refused by the SIGNATURE check, and as that and nothing
  // else — the facade's own credential verification, not the broker's.
  const wrong = makeSqs({ secret: "not-the-secret", service: "sqs (bad credential)" });
  await expectUnmodelledError(
    "Errors.wrong_secret_is_refused",
    "SignatureDoesNotMatch",
    "SignatureDoesNotMatch",
    403,
    () => wrong.send(new ListQueuesCommand({})),
  );
}

async function t_delete_queue({ sqs }) {
  const { name, url } = await makeQueue(sqs, "gone");
  await send(sqs, url, { MessageBody: "will not survive" });

  await expectOk("DeleteQueue.status", () => sqs.send(new DeleteQueueCommand({ QueueUrl: url })));

  await expectSqsError("DeleteQueue.url_lookup_afterwards", "QueueDoesNotExist", () =>
    sqs.send(new GetQueueUrlCommand({ QueueName: name })),
  );
  await expectSqsError("DeleteQueue.receive_afterwards", "QueueDoesNotExist", () =>
    sqs.send(new ReceiveMessageCommand({ QueueUrl: url })),
  );
  const listed = await sqs.send(new ListQueuesCommand({ QueueNamePrefix: name }));
  checkEq("DeleteQueue.is_out_of_ListQueues", listed.QueueUrls ?? [], []);

  // The 60-second tombstone, which SDK retry behaviour depends on.
  await expectSqsError("DeleteQueue.name_is_reserved_for_sixty_seconds", "QueueDeletedRecently", () =>
    sqs.send(new CreateQueueCommand({ QueueName: name })),
  );
}

export const tests = [
  t_create_queue,
  t_get_queue_url_and_list,
  t_queue_attributes,
  t_tags,
  t_send_with_message_attributes,
  t_send_batch,
  t_batch_limits,
  t_long_poll,
  t_change_message_visibility,
  t_receive_count_after_expiry,
  t_delete_message,
  t_delete_message_batch_partial,
  t_delay_seconds,
  t_purge_queue,
  t_fifo,
  t_dlq_redrive,
  t_errors,
  t_delete_queue,
];
