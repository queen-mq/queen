// sqs-consumer: a real worker loop, not a scripted sequence of calls.
//
// WHY THIS LANE EXISTS. Everything in `scenarios/sqs.mjs` drives the API the way
// a test drives it — one call, one assertion. sqs-consumer drives it the way an
// application does: a polling loop it owns, a handler that sometimes throws, a
// nack expressed as `ChangeMessageVisibility`, deletes issued in batches keyed by
// MessageId, and long polls running back to back for as long as the process
// lives. The facade has to be right about the SEQUENCE, not just about each call:
//
//   * a handler that throws must not lose the message — sqs-consumer's
//     `terminateVisibilityTimeout` turns the throw into a visibility change, and
//     the message has to come back, ONCE, with `ApproximateReceiveCount` at 2;
//   * a handler that succeeds must delete it, and the deletion has to stick
//     under a loop that is already long-polling for the next batch;
//   * `handleMessageBatch` acks a SUBSET by returning it, so the messages it
//     leaves out must come back on their own and the ones it returned must not.
//
// Both lanes assert their own drain at the end, because the failure this catches
// — a message redelivered for ever, or one silently dropped — is invisible to
// any single call.
//
// THE ONE FACADE PROPERTY THAT SHAPES THIS FILE is `M0_SMOKE.md` D2: a standard
// queue hands out at most one message per LANE at a time. A worker loop is
// exactly the shape that copes with it (it deletes as it goes, freeing lanes),
// which is why the batch lane asserts that batches of more than one were
// actually seen rather than assuming a full batch every poll.

import { Consumer } from "sqs-consumer";

import { check, checkEq, note } from "../lib/report.mjs";
import { depth, makeQueue, until } from "../lib/queue.mjs";
import { RUN, sleep } from "../lib/stack.mjs";
import { packageVersion } from "../lib/versions.mjs";
import { SendMessageBatchCommand } from "@aws-sdk/client-sqs";

/** Send `count` bodies through as few batches as SQS's ten-entry cap allows. */
async function seed(sqs, url, bodies) {
  for (let i = 0; i < bodies.length; i += 10) {
    const slice = bodies.slice(i, i + 10);
    await sqs.send(
      new SendMessageBatchCommand({
        QueueUrl: url,
        Entries: slice.map((body, j) => ({ Id: `s${i + j}`, MessageBody: body })),
      }),
    );
  }
}

/**
 * Run a consumer until `done()` says so, or the deadline passes.
 *
 * Everything the assertions read is collected from the consumer's OWN events
 * rather than from the handler's closure alone: `message_processed` is the
 * library saying it deleted the message, which is a different claim from "the
 * handler returned".
 */
async function runConsumer(options, { done, timeoutMs = 90_000 }) {
  const events = {
    received: [],
    processed: [],
    processingErrors: [],
    errors: [],
    timeoutErrors: [],
    empty: 0,
  };

  const consumer = Consumer.create(options);
  consumer.on("message_received", (message) => events.received.push(message));
  consumer.on("message_processed", (message) => events.processed.push(message));
  consumer.on("processing_error", (err, message) => events.processingErrors.push({ err, message }));
  consumer.on("error", (err, message) => events.errors.push({ err, message }));
  consumer.on("timeout_error", (err, message) => events.timeoutErrors.push({ err, message }));
  consumer.on("empty", () => {
    events.empty += 1;
  });

  const stopped = new Promise((resolve) => consumer.once("stopped", resolve));
  consumer.start();

  const finished = await until(async () => (done(events) ? true : null), { timeoutMs, everyMs: 200 });
  consumer.stop();
  await Promise.race([stopped, sleep(10_000)]);
  return { events, finished: finished === true, consumer };
}

// ------------------------------------------------- handleMessage, with throws

async function t_worker_loop({ sqs }) {
  const total = 20;
  const bodies = Array.from({ length: total }, (_, i) => `job-${i}`);
  // The three that fail on their FIRST delivery and succeed on their second.
  const poison = new Set(["job-3", "job-9", "job-16"]);

  const { url } = await makeQueue(sqs, "worker", { attributes: { VisibilityTimeout: "30" } });
  await seed(sqs, url, bodies);

  /** body → the ApproximateReceiveCount of each delivery of it, in order. */
  const deliveries = new Map();
  const thrownFor = new Set();

  const { events, finished } = await runConsumer(
    {
      queueUrl: url,
      sqs,
      batchSize: 10,
      waitTimeSeconds: 1,
      visibilityTimeout: 30,
      // The nack: on a throw, put the message back after a second rather than
      // holding it for the queue's full visibility timeout. This is the option
      // every sqs-consumer deployment sets, and it is a `ChangeMessageVisibility`
      // on the facade.
      terminateVisibilityTimeout: 1,
      messageSystemAttributeNames: ["ApproximateReceiveCount"],
      pollingWaitTimeMs: 0,
      async handleMessage(message) {
        const body = message.Body;
        const seen = deliveries.get(body) ?? [];
        seen.push(message.Attributes?.ApproximateReceiveCount);
        deliveries.set(body, seen);
        if (poison.has(body) && !thrownFor.has(body)) {
          thrownFor.add(body);
          throw new Error(`deliberate failure for ${body}`);
        }
        return message;
      },
    },
    { done: (e) => e.processed.length >= total, timeoutMs: 120_000 },
  );

  check("Consumer.the_loop_finished", finished, `only ${events.processed.length}/${total} were processed in 120s`);
  checkEq(
    "Consumer.every_message_was_processed_exactly_once",
    events.processed.map((m) => m.Body).sort(),
    [...bodies].sort(),
  );
  checkEq("Consumer.three_handlers_threw", events.processingErrors.length, 3);
  checkEq(
    "Consumer.the_throws_were_the_marked_messages",
    events.processingErrors.map((e) => e.message.Body).sort(),
    [...poison].sort(),
  );
  // sqs-consumer routes a handler throw to `processing_error` and an SQS API
  // failure to `error`. Anything on `error` here is the facade, not the handler.
  checkEq(
    "Consumer.no_sqs_api_errors",
    events.errors.map((e) => `${e.err.name}: ${e.err.message}`),
    [],
  );
  checkEq("Consumer.no_handler_timeouts", events.timeoutErrors.length, 0);

  // The redelivery, which is the point of the lane.
  const marked = [...poison].sort();
  checkEq(
    "Consumer.marked_messages_were_delivered_twice",
    marked.map((body) => (deliveries.get(body) ?? []).length),
    marked.map(() => 2),
  );
  checkEq(
    "Consumer.the_redelivery_reports_receive_count_two",
    marked.map((body) => (deliveries.get(body) ?? [])[1]),
    marked.map(() => "2"),
  );
  checkEq(
    "Consumer.the_first_delivery_reports_receive_count_one",
    marked.map((body) => (deliveries.get(body) ?? [])[0]),
    marked.map(() => "1"),
  );
  const cleanBodies = bodies.filter((b) => !poison.has(b));
  checkEq(
    "Consumer.unmarked_messages_were_delivered_once",
    cleanBodies.filter((b) => (deliveries.get(b) ?? []).length !== 1),
    [],
  );

  const drained = await until(
    async () => {
      const counts = await depth(sqs, url);
      return counts.messages === 0 && counts.notVisible === 0 ? counts : null;
    },
    { timeoutMs: 30_000 },
  );
  check("Consumer.the_queue_drains", drained !== null, `depth was ${JSON.stringify(await depth(sqs, url))}`);
  note(`worker lane: ${events.received.length} deliveries for ${total} messages, ${events.empty} empty polls`);
}

// -------------------------------------------- handleMessageBatch, partial ack

async function t_worker_batch({ sqs }) {
  const total = 12;
  const bodies = Array.from({ length: total }, (_, i) => `task-${i}`);
  // Held back on their first appearance: the batch handler returns the OTHERS,
  // which acks only those, and these must come back on their own.
  const held = new Set(["task-2", "task-5", "task-9"]);

  // A short visibility timeout, because a message this handler declines to ack
  // is not nacked — nothing calls ChangeMessageVisibility for it — so the only
  // thing that brings it back is the lease lapsing.
  const { url } = await makeQueue(sqs, "batchworker", { attributes: { VisibilityTimeout: "5" } });
  await seed(sqs, url, bodies);

  const deliveries = new Map();
  const seenOnce = new Set();
  const batchSizes = [];

  const { events, finished } = await runConsumer(
    {
      queueUrl: url,
      sqs,
      batchSize: 10,
      waitTimeSeconds: 1,
      visibilityTimeout: 5,
      messageSystemAttributeNames: ["ApproximateReceiveCount"],
      pollingWaitTimeMs: 0,
      async handleMessageBatch(messages) {
        batchSizes.push(messages.length);
        const acked = [];
        for (const message of messages) {
          const body = message.Body;
          const seen = deliveries.get(body) ?? [];
          seen.push(message.Attributes?.ApproximateReceiveCount);
          deliveries.set(body, seen);
          if (held.has(body) && !seenOnce.has(body)) {
            seenOnce.add(body);
            continue; // NOT acked: left out of the returned array
          }
          acked.push(message);
        }
        return acked;
      },
    },
    { done: (e) => e.processed.length >= total, timeoutMs: 120_000 },
  );

  check("ConsumerBatch.the_loop_finished", finished, `only ${events.processed.length}/${total} were acked in 120s`);
  checkEq(
    "ConsumerBatch.every_message_was_acked_exactly_once",
    events.processed.map((m) => m.Body).sort(),
    [...bodies].sort(),
  );
  checkEq(
    "ConsumerBatch.no_sqs_api_errors",
    events.errors.map((e) => `${e.err.name}: ${e.err.message}`),
    [],
  );
  // The batch really was a batch: `handleMessageBatch` is only worth anything
  // if a poll can return more than one message, which on this facade means more
  // than one free lane answered one receive.
  check(
    "ConsumerBatch.at_least_one_poll_returned_more_than_one_message",
    Math.max(0, ...batchSizes) > 1,
    `batch sizes were ${JSON.stringify(batchSizes)}`,
  );
  check(
    "ConsumerBatch.no_batch_exceeded_the_batch_size",
    batchSizes.every((n) => n <= 10),
    `batch sizes were ${JSON.stringify(batchSizes)}`,
  );

  const heldBodies = [...held].sort();
  checkEq(
    "ConsumerBatch.unacked_messages_came_back",
    heldBodies.map((body) => (deliveries.get(body) ?? []).length),
    heldBodies.map(() => 2),
  );
  checkEq(
    "ConsumerBatch.the_redelivery_reports_receive_count_two",
    heldBodies.map((body) => (deliveries.get(body) ?? [])[1]),
    heldBodies.map(() => "2"),
  );
  // ...and the ones the handler DID return were deleted, so they never came
  // back even though the whole batch shared one receive.
  const ackedBodies = bodies.filter((b) => !held.has(b));
  checkEq(
    "ConsumerBatch.acked_messages_did_not_come_back",
    ackedBodies.filter((b) => (deliveries.get(b) ?? []).length !== 1),
    [],
  );

  const drained = await until(
    async () => {
      const counts = await depth(sqs, url);
      return counts.messages === 0 && counts.notVisible === 0 ? counts : null;
    },
    { timeoutMs: 30_000 },
  );
  check("ConsumerBatch.the_queue_drains", drained !== null, `depth was ${JSON.stringify(await depth(sqs, url))}`);
  note(`batch lane: ${batchSizes.length} batches, sizes ${JSON.stringify(batchSizes)}`);
}

// --------------------------------------------------- the library's own facts

async function t_consumer_reports_its_version() {
  // Which sqs-consumer this row exercised, read from the installed package: a
  // matrix row that does not name its client version is a row nobody can
  // reproduce, and this one is `^11`, which moves.
  const version = packageVersion("sqs-consumer");
  check("Consumer.version_is_known", version !== "unknown", "sqs-consumer's package.json could not be read");
  note(`sqs-consumer ${version}, run ${RUN}`);
}

export const tests = [t_consumer_reports_its_version, t_worker_loop, t_worker_batch];
