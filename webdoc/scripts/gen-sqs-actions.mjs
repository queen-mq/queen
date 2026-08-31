/**
 * Generate the SQS/SNS facade's action coverage from its dispatch table.
 *
 * `queen-sqs/src/actions/mod.rs` is the compatibility contract in one place.
 * The `Action` set is CLOSED: `Action::from_name` scans `Action::ALL` and a name
 * that is not in it is `InvalidAction`, never a plausible empty success. So the
 * const is simultaneously what the facade answers and what it refuses, and a
 * hand-written matrix beside it would be a third copy, and the only one nothing
 * checks.
 *
 * Three facts per row, two of them derived:
 *   the action           — parsed out of `Action::ALL`
 *   which service it is  — parsed out of `Action::is_sns`
 *   what it is HERE      — mirrored below behind a fingerprint guard, because
 *                          the sentence is prose ABOUT the table and a changed
 *                          table makes the prose a lie
 *
 * The inverse half is arithmetic rather than a curated list, which is what
 * makes it worth publishing. AWS's own published action sets are written down
 * here (they are AWS's, so nothing in this repository can derive them), and the
 * generator subtracts: what SQS publishes and this facade does not answer must
 * be EMPTY, and what SNS publishes and this facade does not answer must be
 * exactly the three families named below. Add an action to the Rust and forget
 * this file, or name an absent action that quietly landed, and the subtraction
 * stops balancing and this generator fails.
 */

import {
  assertFingerprint,
  cell,
  emitPartial,
  fingerprint,
  fnBody,
  isCheck,
  repoRead,
  sliceBlock,
} from "./lib/source.mjs";

const ACTIONS = "queen-sqs/src/actions/mod.rs";

// ---------------------------------------------------------------------------
// 1. The set, straight out of the const and the classifier
// ---------------------------------------------------------------------------

/** The `Action::ALL` block: the one spelling of the closed set. */
function allBlock(text) {
  return sliceBlock(text, "pub const ALL: [Action;", "\n    ];");
}

/** The body of `Action::is_sns`, which is the only thing that says whose an action is. */
function snsBody(text) {
  return fnBody(text, "pub fn is_sns(self) -> bool");
}

function namesIn(block) {
  const out = [];
  const re = /Action::(\w+)/g;
  let m;
  while ((m = re.exec(block))) out.push(m[1]);
  return out;
}

// Bump these after re-reading `actions/mod.rs` when a guard trips. Everything
// below is prose ABOUT that table, so a changed table with unchanged prose
// publishes a description of a facade that no longer exists.
//
// 2026-08-31: first read, for PLAN_QUEEN_SQS.md M5. Forty actions: twenty-three
// SQS and seventeen SNS. The twenty-three are ALL of SQS, which is the fact this
// partial exists to publish and the reason the SQS half of the subtraction below
// is asserted empty rather than rendered. `AddPermission` and `RemovePermission`
// are the two rows that are answered without being enforced, and they are the
// two AWS publishes under BOTH services: `is_sns` classifies them as SQS's, so
// the only thing their namespace decides is which of two empty envelopes a
// client parses.
const ALL_FINGERPRINT = "ff8d2f776313aaa6";
const IS_SNS_FINGERPRINT = "8d3f51f95bec201b";

/**
 * The two actions that are answered and NOT enforced, with the sentence that
 * must travel with each. PLAN_QUEEN_SQS.md's first stated non-goal: authorization
 * here is Queen's, over the SigV4 keypair, and a facade that emulated an SQS
 * policy would produce the single worst outcome available, a client told its
 * policy is in force when nothing reads it.
 *
 * Asserted PRESENT in `Action::ALL` and asserted to route to the shared
 * `permission` implementation, so the day either grows a real implementation
 * this generator fails rather than publishing a refusal that no longer happens.
 */
const ACCEPTED_NOT_ENFORCED = ["AddPermission", "RemovePermission"];

/** The dispatch arm that proves the two above still do nothing. */
const PERMISSION_ARM = "Action::AddPermission | Action::RemovePermission => permission(";

// ---------------------------------------------------------------------------
// 2. What each action is HERE
// ---------------------------------------------------------------------------

/**
 * One sentence per action: what it maps onto in Queen, or what about it is not
 * AWS's. Not what the action does at AWS, which the reader can get from AWS.
 */
const NOTE = {
  // ------------------------------------------------------------------- queues
  CreateQueue:
    "Creates the Queen queue and the registry record together, registry first, so two instances racing for one name produce one queue and one loser. A standard queue synthesizes `queen.partitions` lanes and that width is fixed for the life of the queue; a `.fifo` suffix declares a FIFO queue instead, where a lane is a `MessageGroupId` and no width is synthesized at all. Idempotent unless an attribute the request SUPPLIES differs from the queue's current value.",
  DeleteQueue:
    "Removes the Queen queue first and the registry record second, then holds a 60 second `QueueDeletedRecently` tombstone, which is emulated because SDK retry behaviour depends on it.",
  GetQueueUrl:
    "Answers `<scheme>://<the host the client reached>/<account>/<name>`. The host is the request's, not the one this process bound, so a queue URL is usable from where it was asked for.",
  ListQueues:
    "A prefix walk of the registry, paged, capped at 10,000 records. It lists queues this facade created: a native Queen queue nobody created through SQS is not in the registry and is not in the answer.",
  GetQueueAttributes:
    "Answers what the record stores plus what is computed on read. `ApproximateNumberOfMessages` and `ApproximateNumberOfMessagesNotVisible` are the broker's depth and in-flight counts, and `ApproximateNumberOfMessagesDelayed` is the timer count; those three are what KEDA and every autoscaler read, so they are load-bearing rather than decoration.",
  SetQueueAttributes:
    "Merges onto the stored record under a compare-and-set, never a replacement. `FifoQueue` and `queen.partitions` are fixed at create and are answered `InvalidAttributeName`, which is what AWS answers for an attribute that exists and cannot be set.",
  // ----------------------------------------------------------------- messages
  SendMessage:
    "One push. The lane is chosen by hashing the send's own deduplication key across the queue's width, because the MessageId is the broker's message uuid and does not exist until the push has landed. On a FIFO queue that key is the `MessageDeduplicationId` (or the SHA-256 of the body under `ContentBasedDeduplication`) and the lane is the `MessageGroupId`.",
  SendMessageBatch:
    "Up to ten entries, with a per-entry result. An empty batch and an eleventh entry are DIFFERENT errors, as they are at AWS, because an SDK's batching helper branches on which.",
  ReceiveMessage:
    "Up to `MaxNumberOfMessages` pops of one message each. Claim width one is what makes every later verb exact, and it is also the ceiling in [the divergence about concurrency](/reference/sqs#the-divergence-to-read-first-a-standard-queues-concurrency-is-its-width). `WaitTimeSeconds` is the broker's own long poll rather than a facade timer.",
  DeleteMessage:
    "Ack `completed`, under the lease the receipt handle names. A stale handle answers success, which is AWS's own documented contract; only a handle this facade did not mint, or minted for another queue, is `ReceiptHandleIsInvalid`.",
  DeleteMessageBatch:
    "Per-entry deletes with per-entry failures. On a FIFO queue the entries of one claim are grouped and the contiguous prefix is acked, with the remainder recorded in Queen's key/value store so that any instance can complete the job.",
  // ---------------------------------------------------------------- lifecycle
  ChangeMessageVisibility:
    "A lease extension, or at zero a terminate: ack `retry`, which releases the message and charges nothing against the retry budget. Exact because the lease being extended holds exactly one message.",
  ChangeMessageVisibilityBatch:
    "The same, per entry, and answered concurrently. On a FIFO queue the entries are grouped by CLAIM first: ten entries of one claim are one release, and ten independent calls would answer the first and refuse the other nine `MessageNotInflight`.",
  PurgeQueue:
    "Delete and recreate, re-applying the record's attributes, with AWS's 60 second cooldown emulated. It is synchronous where AWS answers immediately, and every receipt handle minted before it stops addressing anything.",
  ListQueueTags:
    "Tags live in the registry record. They are not attributes and never travel to the Queen queue.",
  TagQueue:
    "The only action that changes a queue's tags: a `CreateQueue` naming an existing queue neither compares them nor applies them.",
  UntagQueue: "The inverse of `TagQueue`, on the same record.",
  // ---------------------------------------------------------------------- dlq
  ListDeadLetterSourceQueues:
    "Reads the registry for the queues whose `RedrivePolicy` names this one. Redrive itself is not an action: it happens on receive, as an atomic push-to-dead-letter plus ack-original in one `POST /api/v1/transaction`.",
  StartMessageMoveTask:
    "The redrive move run backwards, as a facade loop whose progress is in Queen's key/value store and whose rate is capped by `MaxNumberOfMessagesPerSecond`. With no `DestinationArn` a message goes back to the queue named in the copy's own envelope, which is AWS's documented default.",
  CancelMessageMoveTask:
    "Stops that loop. The progress record is in the store rather than in a process, so the instance that cancels need not be the one that started it.",
  ListMessageMoveTasks: "Reads those progress records, newest first.",
  // ---------------------------------------------------------------------- sns
  CreateTopic:
    "A key in Queen's key/value store. There is no Queen object called a topic and nothing is configured on the broker. Idempotent in all three shapes a provisioner uses, and `FifoTopic` must agree with the `.fifo` suffix in both directions.",
  DeleteTopic:
    "Removes the topic and cascades to its subscriptions. Idempotent, as AWS documents it.",
  ListTopics: "A prefix walk of the same store, paged.",
  GetTopicAttributes:
    "`SubscriptionsPending` is structurally `0`: no subscription this facade can create is ever unconfirmed.",
  SetTopicAttributes: "A compare-and-set onto the topic record.",
  Subscribe:
    "`Protocol=sqs` is the only protocol v0 accepts, and anything else is refused BY NAME rather than as a malformed endpoint. Idempotent per (topic, protocol, endpoint), and a repeat answers the existing ARN without applying the attributes it carries, which is the one thing a declarative provisioner should know about this action.",
  Unsubscribe: "Removes the subscription record. The queue itself is untouched.",
  ConfirmSubscription:
    "Can never succeed here, and says so. Every subscription this facade can create is same-account SQS, which AWS itself confirms at `Subscribe`, so no confirmation token is ever minted; the answer is `InvalidParameter` naming the token rather than a plausible success.",
  ListSubscriptions: "The account-wide listing, paged.",
  ListSubscriptionsByTopic:
    "An unknown topic ARN is `NotFound` rather than an empty list: a client reads an empty list as \"nothing is subscribed\" rather than as \"you asked about the wrong topic\".",
  GetSubscriptionAttributes:
    "A subscription with no filter policy reports no `FilterPolicyScope`, which is AWS's behaviour and the one that does not make a provisioner reconcile for ever.",
  SetSubscriptionAttributes:
    "Where a `FilterPolicy` is written, validated at write time rather than at publish. Setting an empty one removes it.",
  TagResource:
    "SNS's own tag actions are not the queue ones under another name: the resource is an ARN rather than a URL, the answer is a list of pairs rather than a map, and a missing resource is `ResourceNotFound`.",
  UntagResource: "The inverse, on the same records.",
  ListTagsForResource: "Reads them back.",
  Publish:
    "One `POST /api/v1/transaction` bundling one push per matched subscription, so a fan-out commits whole or not at all, which is stronger than SNS promises. Filter policies are evaluated here, at publish, against the registry another instance may also be writing.",
  PublishBatch:
    "The same transaction per entry, with per-entry failures. A batch's entries are independent: one refused entry does not stop the others.",
  // --------------------------------------------------- accepted, NOT enforced
  AddPermission:
    "Validated as far as SQS validates it, the queue must exist and the label is required, and then it does nothing. Authorization here is Queen's, over the SigV4 keypair; there is no principal model for an SQS policy to apply to, and a client told its policy is in force when nothing reads it is the worst answer available. The `Policy` queue attribute is stored on the same terms.",
  RemovePermission: "The same, and the same sentence.",
};

// ---------------------------------------------------------------------------
// 3. The inverse, by subtraction
// ---------------------------------------------------------------------------

/**
 * AWS's own published action sets. Hand-maintained, because they are AWS's:
 * nothing in this repository derives them, and the honest thing is to say so
 * rather than to imply the subtraction below is checked at both ends.
 *
 * Read from the SQS API Reference (2012-11-05, and the JSON protocol that
 * replaced it) and the SNS API Reference (2010-03-31) on 2026-08-31.
 */
const PUBLISHED_SQS = [
  "AddPermission",
  "CancelMessageMoveTask",
  "ChangeMessageVisibility",
  "ChangeMessageVisibilityBatch",
  "CreateQueue",
  "DeleteMessage",
  "DeleteMessageBatch",
  "DeleteQueue",
  "GetQueueAttributes",
  "GetQueueUrl",
  "ListDeadLetterSourceQueues",
  "ListMessageMoveTasks",
  "ListQueueTags",
  "ListQueues",
  "PurgeQueue",
  "ReceiveMessage",
  "RemovePermission",
  "SendMessage",
  "SendMessageBatch",
  "SetQueueAttributes",
  "StartMessageMoveTask",
  "TagQueue",
  "UntagQueue",
];

const PUBLISHED_SNS = [
  "AddPermission",
  "CheckIfPhoneNumberIsOptedOut",
  "ConfirmSubscription",
  "CreatePlatformApplication",
  "CreatePlatformEndpoint",
  "CreateSMSSandboxPhoneNumber",
  "CreateTopic",
  "DeleteEndpoint",
  "DeletePlatformApplication",
  "DeleteSMSSandboxPhoneNumber",
  "DeleteTopic",
  "GetDataProtectionPolicy",
  "GetEndpointAttributes",
  "GetPlatformApplicationAttributes",
  "GetSMSAttributes",
  "GetSMSSandboxAccountStatus",
  "GetSubscriptionAttributes",
  "GetTopicAttributes",
  "ListEndpointsByPlatformApplication",
  "ListOriginationNumbers",
  "ListPhoneNumbersOptedOut",
  "ListPlatformApplications",
  "ListSMSSandboxPhoneNumbers",
  "ListSubscriptions",
  "ListSubscriptionsByTopic",
  "ListTagsForResource",
  "ListTopics",
  "OptInPhoneNumber",
  "Publish",
  "PublishBatch",
  "PutDataProtectionPolicy",
  "RemovePermission",
  "SetEndpointAttributes",
  "SetPlatformApplicationAttributes",
  "SetSMSAttributes",
  "SetSubscriptionAttributes",
  "SetTopicAttributes",
  "Subscribe",
  "TagResource",
  "Unsubscribe",
  "UntagResource",
  "VerifySMSSandboxPhoneNumber",
];

/**
 * What SNS publishes and this facade does not answer, in the three families it
 * falls into. The union of these must equal `PUBLISHED_SNS` minus what the
 * dispatch table answers, exactly, and the generator checks it: a name typed
 * wrongly here, or an action quietly implemented, unbalances the subtraction.
 *
 * There is no SQS half of this list, and that absence is the claim: every action
 * Amazon SQS publishes is answered, so the subtraction on that side must come
 * out empty or this generator fails.
 */
const ABSENT_SNS = [
  {
    family: "Mobile push",
    why: "A platform endpoint is a device token registered with APNs, FCM or ADM, and a publish to one is a push notification delivered by Apple or Google. There is no queue anywhere in it and no part of it a message broker can stand in for. This is the largest single family of SNS and the least related to what the facade is.",
    actions: [
      "CreatePlatformApplication",
      "CreatePlatformEndpoint",
      "DeleteEndpoint",
      "DeletePlatformApplication",
      "GetEndpointAttributes",
      "GetPlatformApplicationAttributes",
      "ListEndpointsByPlatformApplication",
      "ListPlatformApplications",
      "SetEndpointAttributes",
      "SetPlatformApplicationAttributes",
    ],
  },
  {
    family: "SMS and the SMS sandbox",
    why: "Sending an SMS needs a carrier, an origination number and an opt-out register, all of them AWS the service rather than SNS the API. A facade that accepted these would accept a message it has no way to deliver and no way to report undeliverable.",
    actions: [
      "CheckIfPhoneNumberIsOptedOut",
      "CreateSMSSandboxPhoneNumber",
      "DeleteSMSSandboxPhoneNumber",
      "GetSMSAttributes",
      "GetSMSSandboxAccountStatus",
      "ListOriginationNumbers",
      "ListPhoneNumbersOptedOut",
      "ListSMSSandboxPhoneNumbers",
      "OptInPhoneNumber",
      "SetSMSAttributes",
      "VerifySMSSandboxPhoneNumber",
    ],
  },
  {
    family: "Data protection policies",
    why: "The policy inspects message bodies for sensitive data and masks or blocks them in flight. Storing one and not applying it would be the `Policy` attribute's mistake made twice, and applying one would be a content classifier written from scratch inside a wire facade. Neither is a thing this milestone gets to decide.",
    actions: ["GetDataProtectionPolicy", "PutDataProtectionPolicy"],
  },
];

// ---------------------------------------------------------------------------

function main() {
  const check = isCheck();
  const text = repoRead(ACTIONS);

  const all = allBlock(text);
  const isSns = snsBody(text);
  const actions = namesIn(all);
  const snsActions = new Set(namesIn(isSns));

  if (actions.length < 30) {
    throw new Error(`only parsed ${actions.length} actions out of ${ACTIONS} — the parser is broken`);
  }
  assertFingerprint(`${ACTIONS} :: Action::ALL`, all, ALL_FINGERPRINT);
  assertFingerprint(`${ACTIONS} :: Action::is_sns`, isSns, IS_SNS_FINGERPRINT);

  const answered = new Set(actions);
  const published = { sqs: new Set(PUBLISHED_SQS), sns: new Set(PUBLISHED_SNS) };

  // Every answered action must be one AWS publishes, under the service this
  // facade files it as. A name that is in neither list is either an action AWS
  // does not have or a name missing from the two lists above, and both are
  // reasons to stop.
  for (const action of actions) {
    const service = snsActions.has(action) ? "sns" : "sqs";
    if (!published[service].has(action)) {
      throw new Error(
        `${action} is answered and classified ${service.toUpperCase()}, but it is not in ` +
          `PUBLISHED_${service.toUpperCase()} in this script. Either AWS publishes it and this ` +
          `script's copy of that list is short, or the action is not AWS's at all.`,
      );
    }
  }

  const missingNote = actions.filter((a) => !NOTE[a]);
  if (missingNote.length) {
    throw new Error(`answered with no sentence in this script: ${missingNote.join(", ")}`);
  }
  const orphanNote = Object.keys(NOTE).filter((a) => !answered.has(a));
  if (orphanNote.length) {
    throw new Error(`this script describes an action that is no longer answered: ${orphanNote.join(", ")}`);
  }

  for (const action of ACCEPTED_NOT_ENFORCED) {
    if (!answered.has(action)) {
      throw new Error(
        `${action} is listed here as accepted-and-not-enforced but is not in Action::ALL. ` +
          `Either it is gone, or its row belongs in the absent list instead.`,
      );
    }
  }
  if (!text.includes(PERMISSION_ARM)) {
    throw new Error(
      `the dispatch arm \`${PERMISSION_ARM}…\` is gone from ${ACTIONS}. This script publishes ` +
        `AddPermission and RemovePermission as ACCEPTED AND NOT ENFORCED; if either now does ` +
        `something, that sentence is a lie and has to be rewritten before this can regenerate.`,
    );
  }

  // The subtraction. The SQS half must come out empty, which is the fact this
  // partial exists to state.
  const absentSqs = PUBLISHED_SQS.filter((a) => !answered.has(a));
  if (absentSqs.length) {
    throw new Error(
      `this generator publishes "every action Amazon SQS defines is answered", and these are ` +
        `not: ${absentSqs.join(", ")}. Implement them, or rewrite the claim.`,
    );
  }
  const absentSns = PUBLISHED_SNS.filter((a) => !answered.has(a));
  const claimedAbsent = ABSENT_SNS.flatMap((g) => g.actions);
  const unclaimed = absentSns.filter((a) => !claimedAbsent.includes(a));
  const overclaimed = claimedAbsent.filter((a) => !absentSns.includes(a));
  if (unclaimed.length || overclaimed.length) {
    throw new Error(
      [
        `the SNS subtraction does not balance.`,
        unclaimed.length ? `  published, not answered, and not explained here: ${unclaimed.join(", ")}` : "",
        overclaimed.length ? `  explained here as absent but answered (or misspelled): ${overclaimed.join(", ")}` : "",
      ]
        .filter(Boolean)
        .join("\n"),
    );
  }

  // -------------------------------------------------------------------- body

  const sqsRows = actions.filter((a) => !snsActions.has(a));
  const snsRows = actions.filter((a) => snsActions.has(a));
  const status = (a) =>
    ACCEPTED_NOT_ENFORCED.includes(a) ? "accepted, not enforced" : "answered";

  const lines = [];
  lines.push(
    `The facade answers **${actions.length} actions**: ${sqsRows.length} of SQS's and ` +
      `${snsRows.length} of SNS's. Every row is read out of \`${ACTIONS}\` at build time, which is ` +
      `the same table \`Action::from_name\` scans, so an action that is listed is an action that ` +
      `is dispatched. The set is CLOSED: a name outside it is \`InvalidAction\` rather than ` +
      `something plausible, because "plausible" for a client that asked to purge a queue means it ` +
      `believes the queue is empty.`,
    "",
    `**Every action Amazon SQS defines is one of these.** That is checked rather than claimed: ` +
      `the table below is derived from the dispatch table, SQS's own published action set is ` +
      `subtracted from it, and the build fails if anything is left over. SNS is the opposite ` +
      `shape and deliberately so, and what it leaves out is ` +
      `[below](#what-sns-publishes-and-this-does-not).`,
    "",
    "### SQS",
    "",
    "| Action | Status | What it is here |",
    "| --- | --- | --- |",
  );
  for (const a of sqsRows) {
    lines.push(`| \`${a}\` | ${status(a)} | ${cell(NOTE[a])} |`);
  }
  lines.push(
    "",
    "### SNS",
    "",
    `SNS here is a facade-level construct and the broker needs nothing for it: a topic is a key ` +
      `in Queen's key/value store, a subscription is another, and a publish is one transaction. ` +
      `v0 subscribes SQS queues and nothing else.`,
    "",
    "| Action | Status | What it is here |",
    "| --- | --- | --- |",
  );
  for (const a of snsRows) {
    lines.push(`| \`${a}\` | ${status(a)} | ${cell(NOTE[a])} |`);
  }

  const absentCount = claimedAbsent.length;
  lines.push(
    "",
    "### What SNS publishes and this does not",
    "",
    `${absentCount} actions, in three families, and none of them is a gap waiting for a patch ` +
      `release: each is refused \`InvalidAction\` by the closed set above, and each is excluded ` +
      `because it is AWS the platform rather than SNS the API. A client that sends one gets the ` +
      `same answer it would get for a typo, which is the honest one.`,
    "",
    "| Family | Actions | Why it is not here |",
    "| --- | --- | --- |",
  );
  for (const g of ABSENT_SNS) {
    const names = g.actions.map((a) => `\`${a}\``).join(", ");
    lines.push(`| ${cell(g.family)} | ${cell(names)} | ${cell(g.why)} |`);
  }
  lines.push(
    "",
    `\`AddPermission\` and \`RemovePermission\` are the two names AWS publishes under BOTH ` +
      `services. They are answered, filed as SQS's, and enforced by nothing, which is the row ` +
      `they carry in the first table.`,
  );

  const res = emitPartial({
    name: "sqs-action-matrix",
    title: "SQS and SNS action matrix",
    description:
      "Every SQS and SNS action the queen-sqs facade answers, which of them are accepted without being enforced, and the SNS families it deliberately does not implement.",
    sources: [`${ACTIONS} (Action::ALL, Action::is_sns)`],
    body: lines.join("\n"),
    check,
  });
  return res;
}

const result = main();
if (result.drifted) {
  console.error(`DRIFT: ${result.file} is behind its source`);
  process.exit(1);
}
console.log(`${result.drifted === false ? "ok" : "wrote"}  ${result.title}`);

// Printed by `--fingerprint`, so the numbers to paste back in never have to be
// computed by hand from a failure message.
if (process.argv.includes("--fingerprint")) {
  const text = repoRead(ACTIONS);
  console.log(`Action::ALL     ${fingerprint(allBlock(text))}`);
  console.log(`Action::is_sns  ${fingerprint(snsBody(text))}`);
}
