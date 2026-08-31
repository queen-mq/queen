// The moves every SQS scenario makes, written once.
//
// Two of them exist because of `M0_SMOKE.md` D2 and are worth reading before
// writing a new scenario: a standard queue in this facade hands out AT MOST ONE
// message per lane at a time (a receive is N parallel `batch=1` pops, and a lane
// with a live claim serves no second pop). So:
//
//   * `drainDeleting` — receive, delete, go round again — is the ONLY shape that
//     can empty a queue holding more messages than it has partitions, and it is
//     what a real consumer does anyway;
//   * `hold`, for the assertions that genuinely need several handles alive at
//     once, SENDS another message when a receive comes back empty (a fresh
//     MessageId is a fresh lane) rather than waiting on a lane that will not
//     open before its visibility timeout.
//
// A loop that just receives `n` times and expects `n` messages is the shape that
// gets this wrong, and it is wrong in the SUITE, not in the facade.

import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  BatchEntryIdsNotDistinct,
  BatchRequestTooLong,
  EmptyBatchRequest,
  InvalidAttributeName,
  InvalidBatchEntryId,
  MessageNotInflight,
  PurgeQueueInProgress,
  QueueDeletedRecently,
  QueueDoesNotExist,
  QueueNameExists,
  ReceiptHandleIsInvalid,
  TooManyEntriesInBatchRequest,
} from "@aws-sdk/client-sqs";
import { randomUUID } from "node:crypto";

import { check, checkEq, fail, show } from "./report.mjs";
import { ACCOUNT, ENDPOINT, REGION, RUN, queueArn, sleep } from "./stack.mjs";

/** Everything this run created, so the teardown can remove it. */
export const CREATED = [];

export async function makeQueue(sqs, label, { attributes, tags, fifo = false } = {}) {
  // The `.fifo` suffix is the whole of how a FIFO queue is declared, so it has
  // to be the LAST thing in the name — after the run id, not before it.
  const name = `js-${label}-${RUN}${fifo ? ".fifo" : ""}`;
  const answer = await sqs.send(
    new CreateQueueCommand({
      QueueName: name,
      ...(attributes ? { Attributes: attributes } : {}),
      ...(tags ? { tags } : {}),
    }),
  );
  CREATED.push(answer.QueueUrl);
  return { name, url: answer.QueueUrl, arn: queueArn(name) };
}

export async function teardown(sqs) {
  for (const url of CREATED.splice(0)) {
    try {
      await sqs.send(new DeleteQueueCommand({ QueueUrl: url }));
    } catch {
      // A queue the scenario already deleted is not a teardown failure.
    }
  }
}

export async function send(sqs, url, params) {
  return sqs.send(new SendMessageCommand({ QueueUrl: url, ...params }));
}

export async function receive(sqs, url, params = {}) {
  const answer = await sqs.send(
    new ReceiveMessageCommand({
      QueueUrl: url,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 1,
      ...params,
    }),
  );
  return answer.Messages ?? [];
}

/** Up to `count` messages, NOT deleted, until the timeout. */
export async function drain(sqs, url, count, { timeoutMs = 25_000, ...params } = {}) {
  const got = [];
  const deadline = Date.now() + timeoutMs;
  while (got.length < count && Date.now() < deadline) {
    got.push(...(await receive(sqs, url, { MaxNumberOfMessages: Math.min(10, count - got.length), ...params })));
  }
  return got;
}

/**
 * What a real consumer does: receive, delete, repeat. Returns the deleted
 * messages.
 *
 * It asks for at most the number it still needs, so it cannot OVERSHOOT `count`.
 * That matters where the assertion is about a message that should not be there
 * at all — a suppressed duplicate, say: a drain that swallowed the extra one on
 * its way past would report the surplus as an unrelated count mismatch and let
 * the assertion that is actually about it pass.
 */
export async function drainDeleting(sqs, url, count, { timeoutMs = 60_000, ...params } = {}) {
  const got = [];
  const deadline = Date.now() + timeoutMs;
  while (got.length < count && Date.now() < deadline) {
    const batch = await receive(sqs, url, { MaxNumberOfMessages: Math.min(10, count - got.length), ...params });
    for (const message of batch) {
      await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle }));
    }
    got.push(...batch);
  }
  return got;
}

/**
 * `count` messages received and NOT deleted, all in flight at once.
 *
 * Sends filler when a lane will not open (D2), so it leaves stragglers behind
 * and must not be used where something later asserts the queue is empty.
 */
export async function hold(sqs, url, count, { timeoutMs = 60_000 } = {}) {
  const held = [];
  const deadline = Date.now() + timeoutMs;
  while (held.length < count && Date.now() < deadline) {
    const batch = await receive(sqs, url);
    held.push(...batch);
    if (batch.length === 0 && held.length < count) {
      await send(sqs, url, { MessageBody: `filler-${randomUUID()}` });
    }
  }
  return held.slice(0, count);
}

/** Everything that is left, deleted, so a later assertion starts from empty. */
export async function purgeByHand(sqs, url, { timeoutMs = 15_000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let removed = 0;
  while (Date.now() < deadline) {
    const batch = await receive(sqs, url);
    if (batch.length === 0) break;
    for (const message of batch) {
      await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle }));
      removed += 1;
    }
  }
  return removed;
}

/** The three depth counters, as numbers. KEDA and every autoscaler read these. */
export async function depth(sqs, url) {
  const answer = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: url,
      AttributeNames: [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
      ],
    }),
  );
  const at = (name) => Number(answer.Attributes?.[name] ?? "-1");
  return {
    messages: at("ApproximateNumberOfMessages"),
    notVisible: at("ApproximateNumberOfMessagesNotVisible"),
    delayed: at("ApproximateNumberOfMessagesDelayed"),
  };
}

/** Poll `fn` until it answers truthy, or the deadline passes. Answers the last value. */
export async function until(fn, { timeoutMs = 20_000, everyMs = 500 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let last = await fn();
  while (!last && Date.now() < deadline) {
    await sleep(everyMs);
    last = await fn();
  }
  return last;
}

// --------------------------------------------------------------------- errors

// SQS spells every error TWICE and the two spellings are usually different
// words: the shape name (which is what the SDK names the exception class after,
// and what `catch (e) { if (e instanceof QueueDoesNotExist) }` matches) and the
// legacy Query code, which arrives in the `x-amzn-query-error` header and lands
// on the exception as `.Code`. A facade that got the pair backwards would still
// raise something, and would break every modelled catch in the world.
//
// shape → [legacy Query code, the modelled class, HTTP status]
export const SQS_ERRORS = {
  QueueDoesNotExist: ["AWS.SimpleQueueService.NonExistentQueue", QueueDoesNotExist, 400],
  QueueNameExists: ["QueueAlreadyExists", QueueNameExists, 400],
  QueueDeletedRecently: ["AWS.SimpleQueueService.QueueDeletedRecently", QueueDeletedRecently, 400],
  BatchEntryIdsNotDistinct: ["AWS.SimpleQueueService.BatchEntryIdsNotDistinct", BatchEntryIdsNotDistinct, 400],
  EmptyBatchRequest: ["AWS.SimpleQueueService.EmptyBatchRequest", EmptyBatchRequest, 400],
  TooManyEntriesInBatchRequest: [
    "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
    TooManyEntriesInBatchRequest,
    400,
  ],
  BatchRequestTooLong: ["AWS.SimpleQueueService.BatchRequestTooLong", BatchRequestTooLong, 400],
  InvalidBatchEntryId: ["AWS.SimpleQueueService.InvalidBatchEntryId", InvalidBatchEntryId, 400],
  ReceiptHandleIsInvalid: ["ReceiptHandleIsInvalid", ReceiptHandleIsInvalid, 400],
  MessageNotInflight: ["AWS.SimpleQueueService.MessageNotInflight", MessageNotInflight, 400],
  InvalidAttributeName: ["InvalidAttributeName", InvalidAttributeName, 400],
  // 403 reads like a typo and is not: it is the status AWS documents for the
  // purge cooldown, and an SDK that special-cases it branches on the pair.
  PurgeQueueInProgress: ["AWS.SimpleQueueService.PurgeQueueInProgress", PurgeQueueInProgress, 403],
};

/**
 * Run `call`, require it to fail as SQS's `shape`, in ALL FOUR spellings the
 * JS SDK exposes: the modelled class, the exception's name, the legacy code and
 * the fault. Answers the error so a caller can look at its message.
 */
export async function expectSqsError(name, shape, call) {
  const [legacy, ctor, status] = SQS_ERRORS[shape];
  try {
    await call();
  } catch (err) {
    checkEq(
      name,
      [err.constructor?.name, err.name, err.Code, err.Type, err.$metadata?.httpStatusCode],
      [shape, shape, legacy, "Sender", status],
    );
    check(`${name}.is_the_modelled_class`, err instanceof ctor, `got ${show(err.name)}: ${err.message}`);
    return err;
  }
  fail(name, `the call succeeded; expected ${shape}`);
  return null;
}

/** Run `call` and assert only that it did NOT fail — the "AWS answers 200" shape. */
export async function expectOk(name, call) {
  try {
    await call();
    return check(name, true, "");
  } catch (err) {
    return check(name, false, `${err.name}: ${err.message}`);
  }
}

/**
 * For the errors SQS's own model carries no class for — the signing ones, and
 * `InvalidParameterValue`.
 *
 * They arrive as a bare `SQSServiceException` whose `name` is the shape from
 * `__type` and whose `Code`/`Type` still come from the compatibility header, so
 * three of the four spellings are assertable and only the class is not.
 */
export async function expectUnmodelledError(name, code, legacy, status, call) {
  try {
    await call();
  } catch (err) {
    checkEq(name, [err.name, err.Code, err.Type, err.$metadata?.httpStatusCode], [code, legacy, "Sender", status]);
    return err;
  }
  fail(name, `the call succeeded; expected ${code}`);
  return null;
}

/**
 * The URL of a queue that is not there.
 *
 * It is built on the ENDPOINT's own origin on purpose: the SDK's
 * `queueUrlMiddleware` re-points the client at a QueueUrl whose origin differs
 * from the configured endpoint (`useQueueUrlAsEndpoint`, on by default), so a
 * ghost URL invented on another host would not test the facade at all — it
 * would test a connection refused somewhere else.
 */
export function ghostUrl(label) {
  return `${ENDPOINT}/${ACCOUNT}/js-ghost-${label}-${RUN}`;
}

export { ACCOUNT, REGION };
