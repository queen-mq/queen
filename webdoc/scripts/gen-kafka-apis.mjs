/**
 * Generate the Kafka facade's support matrix from its advertised-versions table.
 *
 * `protocols/queen-kafka/src/versions.rs` is the compatibility contract in one
 * place: the
 * ApiVersions response is built from it and every incoming request is gated on
 * it, so the table is simultaneously what the facade promises and what it
 * accepts. A hand-written matrix beside it would be a third copy, and the only
 * one nothing checks.
 *
 * Two facts per row, both derived:
 *   API + version window  — parsed out of the `ADVERTISED` const
 *   why the window ends where it does — mirrored below behind a fingerprint
 *                           guard, because the reason is prose about a NUMBER
 *                           and a changed number makes the prose a lie
 *
 * The second table is the inverse and is derived the same way: an API named in
 * `ABSENT` must NOT appear in `ADVERTISED`, so the day one of them is
 * implemented this generator fails rather than publishing a refusal that no
 * longer happens.
 */

import {
  assertFingerprint,
  cell,
  emitPartial,
  fingerprint,
  isCheck,
  repoRead,
  sliceBlock,
} from "./lib/source.mjs";

const VERSIONS = "protocols/queen-kafka/src/versions.rs";

// ---------------------------------------------------------------------------
// 1. The table, straight out of the const
// ---------------------------------------------------------------------------

function parseAdvertised(text) {
  const block = sliceBlock(text, "pub const ADVERTISED: &[Api] = &[", "\n];");
  const rows = [];
  const re = /Api\s*\{\s*key:\s*ApiKey::(\w+)\s*,\s*min:\s*(-?\d+)\s*,\s*max:\s*(-?\d+)\s*,\s*\}/g;
  let m;
  while ((m = re.exec(block))) {
    rows.push({ api: m[1], min: Number(m[2]), max: Number(m[3]) });
  }
  return { rows, block };
}

// ---------------------------------------------------------------------------
// 2. Why each window ends where it does — mirror of the `ADVERTISED` doc block
// ---------------------------------------------------------------------------

// Bump this after re-reading `versions.rs` when the guard trips. The windows
// below are prose ABOUT the numbers in that const, so a raised ceiling with an
// unchanged sentence publishes a reason for a boundary that has moved.
// 2026-08-28: first read, for PLAN_QUEEN_KAFKA.md M6. Fourteen rows. The five
// group APIs share one rule and it is the load-bearing one: each stops one
// version below where `group_instance_id` appears, because static membership is
// out of scope and a client that could negotiate the field would send it and be
// given ordinary dynamic behaviour back.
// 2026-08-29: M7 F1 appended three rows — CreateTopics 2-6, DeleteTopics 1-5,
// DescribeConfigs 1-4 — and moved those three out of ABSENT. Seventeen rows.
// They share a ceiling rule of their own: each stops one version below where a
// topic can be named by a UUID, which is the same boundary Metadata stops at
// and for the same reason (no topic-id registry).
// 2026-08-29: M7 F2 appended three more — ListGroups 0-4, DescribeGroups 0-3,
// DeleteGroups 0-2 — and moved those three out of ABSENT. Twenty rows. Their
// three ceilings are three DIFFERENT boundaries: the KIP-848 group type, static
// membership, and the end of the schema. DeleteGroups is also the first API
// here that REMOVES a committed offset, which is why its absence note said what
// it said.
// 2026-08-29: M7 F3 appended ONE row — InitProducerId 0-4 — and moved it out of
// ABSENT. Twenty-one rows. It is the row that removes the largest onboarding
// papercut the facade had (enable.idempotence has defaulted to true in the Java
// client since 3.0), and the only one whose window reaches a version for a
// FAILURE path: v3 is KIP-360's epoch bump, without which a sequence window the
// facade lost is a fatal error in the producer instead of a reset. The
// transaction APIs beside it in ABSENT are untouched and stay excluded.
// 2026-08-30: M7 F4 appended SEVEN rows — DescribeAcls, CreateAcls and
// DeleteAcls 1-3, AlterConfigs 0-2, IncrementalAlterConfigs 0-1,
// CreatePartitions 0-3, OffsetDelete 0 — and moved AlterConfigs and OffsetDelete
// out of ABSENT. Twenty-eight rows, and the admin surface is finished. Every one
// of the seven is the SCHEMA'S WHOLE WINDOW, which is new for this table and is
// an argument rather than an omission: for six of them no field varies anywhere
// inside the window (only the flexible encoding does), so there is no version at
// which the API starts asking for something the facade would have to invent, and
// OffsetDelete has exactly one version. Two of the seven advertise a REFUSAL
// rather than a capability — the ACL trio, and CreatePartitions — and both are
// justified in the const's own doc block by the refusal being Apache Kafka's own
// answer rather than "this broker is too old".
// 2026-08-30: M9 appended FOUR rows — AddPartitionsToTxn, AddOffsetsToTxn,
// EndTxn and TxnOffsetCommit, all 0-3 — and moved three of them out of ABSENT
// (AddOffsetsToTxn was never listed there). Thirty-two rows. They share one
// ceiling argument and it is a new one for this table: KIP-896 dropped no
// version of any of the four, so every floor is the schema's own 0, and every
// ceiling is KIP-890's transaction protocol 2, which this facade does not
// perform. Two of the four stop for a stronger reason than "the version adds
// nothing": AddPartitionsToTxn v4 is a DIFFERENT request that only another
// broker sends, and TxnOffsetCommit's FLOOR of 3 is mandatory rather than
// preferred, because kafka-clients throws below it whenever group metadata is
// set and every consume-transform-produce loop sets it. InitProducerId's 0-4 is
// untouched: M9 changed what that handler does with a transactional id, not
// what is advertised.
const ADVERTISED_FINGERPRINT = "40da963cf0ec746c";

/** One sentence per API: what the boundary is, not what the API does. */
const WINDOW_REASON = {
  Produce:
    "Floor: v3 is the first version whose records are RecordBatch v2. Ceiling: v10 adds the leader-change hint `current_leader`, which has no meaning against one broker with no elections, and v13 addresses topics by UUID.",
  Fetch:
    "Ceiling: v7 introduces fetch sessions (KIP-227), which are per-connection broker state. The facade keeps none by design, so the cap deletes `session_id`, `session_epoch` and `forgotten_topics_data` rather than half-answering them. v4 is the schema's own floor.",
  ListOffsets:
    "Ceiling: v7 adds the MAX_TIMESTAMP sentinel, a time-index question Queen cannot answer. v5 is the last version whose whole surface is the two watermark sentinels. v1 is the schema's own floor.",
  ApiVersions:
    "Ceiling: one below the schema, on purpose. v3 is what every client in the compatibility matrix negotiates against a 3.x broker, and it keeps the v0 fallback on a path a real client reaches.",
  Metadata:
    "Ceiling: v10 adds topic ids, and a client may then address a topic by a UUID this facade has no registry to resolve. v9 is already the flexible encoding and carries every field a client needs.",
  OffsetCommit:
    "Ceiling: v7 carries `group_instance_id` (static membership, out of scope). Floor: v0 and v1 are the ZooKeeper-era offset store.",
  OffsetFetch:
    "Ceiling: v8 fetches offsets for several groups in one request and changes the response shape. v7's `require_stable` is answered honestly rather than ignored: it asks the broker to withhold offsets belonging to an open transaction, and since M9 an offset belonging to an open transaction is not in the store at all \u2014 the store write happens at COMMIT, in the same Postgres transaction as the records. Every offset returned is stable by construction, so UNSTABLE_OFFSET_COMMIT (88) is a code this facade never needs.",
  FindCoordinator:
    "Ceiling: v4 is the batched form, which exists for clusters where groups live on different brokers. In single-node mode the answer is this process for every key, group or transaction. In cluster mode a GROUP key resolves to the rendezvous owner over the live node set, and a TRANSACTION key is refused TRANSACTIONAL_ID_AUTHORIZATION_FAILED \u2014 fatal on purpose, so `initTransactions()` stops instead of looping on a retriable code for the whole of `max.block.ms`.",
  JoinGroup:
    "Ceiling: v5 carries `group_instance_id` (static membership, out of scope). v4 is also where MEMBER_ID_REQUIRED lands, and that is implemented.",
  Heartbeat: "Ceiling: v3 carries `group_instance_id` (static membership, out of scope).",
  LeaveGroup:
    "Ceiling: v3 carries `group_instance_id`, and is also where one request may remove several members at once. Below it a request is exactly one member, which is the shape the coordinator has.",
  SyncGroup: "Ceiling: v3 carries `group_instance_id` (static membership, out of scope).",
  SaslHandshake:
    "Both versions, because they are the two SASL flows: after v0 the tokens travel as raw bytes in ordinary frames, after v1 inside SaslAuthenticate requests. Both are implemented.",
  SaslAuthenticate:
    "Ceiling: v2 is the flexible encoding and adds no field. v1's `session_lifetime_ms` is answered 0, which is what stops every client re-authenticating on a timer this facade does not run.",
  CreateTopics:
    "Ceiling: v7 answers a `topic_id` UUID this facade has no registry to mint, the same boundary Metadata stops at. v4 (KIP-464) is where -1 means 'you choose' for the partition count and replication factor, v5 carries the created topic's real configs back, and v6 is where a client understands THROTTLING_QUOTA_EXCEEDED. Floor: the schema's own.",
  DeleteTopics:
    "Ceiling: v6 replaces the name list with entries carrying a name or a topic id, and an id is a name this facade cannot resolve. v5 adds `error_message`, which is where 'there is no such queue' gets to say so. Floor: the schema's own.",
  DescribeConfigs:
    "The whole schema, and the only row here with no ceiling to argue: nothing in the window asks for something the facade cannot answer. v3's `config_type` and `documentation` are answered truthfully for the keys reported, and the answer is short because a key is reported only where the facade can name what enforces it.",
  ListGroups:
    "Ceiling: v5 adds `group_type`, the KIP-848 discriminator between a classic group and a consumer-protocol one, and KIP-848 is excluded by plan \u2014 answering a group TYPE question would claim a taxonomy the facade does not implement. v4 is KIP-518's `states_filter` and `group_state`, and both are honoured rather than ignored.",
  DescribeGroups:
    "Ceiling: v4 carries `group_instance_id` (static membership, out of scope), the same rule that caps JoinGroup at 4. v3's `include_authorized_operations` is answered with Kafka's own omitted sentinel, because the facade has no ACL model and a computed bitfield would be an invented permission set.",
  DeleteGroups:
    "The whole schema. v2 is the flexible encoding and adds no field, and there is no version of this API that asks for something the facade cannot answer.",
  InitProducerId:
    "Ceiling: v5 exists for KIP-890's transaction protocol 2, which this facade does not perform \u2014 the same ceiling argument the four transaction rows make. Since M9 this key grants a `transactional.id` as well as an idempotent one, so what stops at v5 is a PROTOCOL the facade does not run rather than a feature it refuses; in cluster mode a transactional id is still answered TRANSACTIONAL_ID_AUTHORIZATION_FAILED. v3 is load-bearing rather than a nicety: it is KIP-360's epoch bump, and it is what turns a sequence window this facade has lost (a restart, an evicted entry) into a reset the producer recovers from instead of a fatal error. Read the REQUEST schema for the cap, not the key's `valid_versions()`, which answers wider because it takes the maximum of request and response.",
  DescribeAcls:
    "The whole schema window, and no ceiling to argue: every field of the request and the response is marked v1-v3, so nothing varies inside it but the flexible encoding (v2). Floor: the schema's own, which is KIP-896's, since v0 was dropped. What is advertised is a REFUSAL. Every call answers SECURITY_DISABLED, which is what an Apache Kafka broker with no authorizer answers, because Queen has no ACL model to answer anything else from.",
  CreateAcls:
    "The same window and the same refusal as DescribeAcls, with one difference on the wire that a client can read: the error is carried PER CREATION rather than at the top level, because Kafka's own error response maps over the request. An empty creations list therefore answers an empty result list and no error at all.",
  DeleteAcls:
    "The same window and the same refusal, per FILTER, each with an empty matching_acls. Advertising these three rather than leaving them out is what turns \"this broker is too old\" into the sentence a real Kafka with security off prints.",
  AlterConfigs:
    "The whole schema window: every field of both schemas is marked v0-v2, so nothing varies inside it but the flexible encoding (v2). Floor: the schema's own, and it is 0, because KIP-896 dropped nothing from this key and inventing a floor above 0 would refuse a version a real broker serves. This is the deprecated FULL-REPLACEMENT form, honoured literally: a key the request does not name is reset to its default. Prefer IncrementalAlterConfigs.",
  IncrementalAlterConfigs:
    "The whole schema window (v1 is the flexible encoding and adds no field), and the key that matters: `kafka-configs.sh --alter` has sent this since Kafka 2.3 and has no fallback to the deprecated key 33, so this is what an operator's command actually lands on. What it can write is bounded by what the facade can write LOSSLESSLY. Queen's configure route is a whole-row upsert whose columns mostly cannot be read back, so an alter lands only on a topic this facade created and every other topic is refused with the reason.",
  CreatePartitions:
    "The whole schema window; nothing varies inside it but the flexible encoding (v2). What is advertised is a REFUSAL. Queen declares no width per queue — a partition exists once something has been written to it — and while a topic may carry its own width floor, that floor is declared ONCE, at CreateTopics, and this API is not that writer. Two of the three answers are Apache Kafka's own sentences byte for byte, since a DECREASE and an EQUAL count are refused by a real broker too, and only an increase is a capability gap. The alternative, no row at all, would tell an operator to upgrade their broker, which is the wrong diagnosis in all three cases.",
  AddPartitionsToTxn:
    "Ceiling: v4 is a DIFFERENT REQUEST, not a wider one. The flat (transactional_id, producer_id, producer_epoch, topics) of v0-v3 becomes a `transactions[]` array with a `verify_only` flag — KIP-890's coordinator-to-partition-leader verification, which a client never sends and only another broker does. Floor: the schema's own, since KIP-896 dropped nothing here.",
  AddOffsetsToTxn:
    "The whole window a client uses. Every field of the schema is marked v0-v4 and v3 is already the flexible encoding, so v3 answers everything v4 does; v4 exists for KIP-890's transaction protocol 2, in which the client stops sending this API at all and the coordinator infers the offsets partition. Advertising it would advertise a protocol this facade does not run.",
  EndTxn:
    "Ceiling: v5's RESPONSE carries `producer_id` and `producer_epoch` — the transaction-protocol-2 epoch bump performed inside EndTxn, which this facade does not perform — and v4 is the version pair that exists on the way to it. v3 is the flexible encoding and asks for nothing that cannot be answered.",
  TxnOffsetCommit:
    "The FLOOR is the load-bearing number here, and it is measured rather than preferred: TxnOffsetCommitRequest$Builder.build(short) in kafka-clients 3.9.2 throws UnsupportedVersionException below v3 whenever group metadata is set, and every KIP-447 consume-transform-produce loop sets it — so advertising this API below 3 would make the flagship use case throw before any wire traffic. Ceiling: the same transaction-protocol-2 bump as the other three, which adds no field a client fills in.",
  OffsetDelete:
    "One version, so there is no window to argue. It is the last thing `kafka-consumer-groups.sh` could not do here, and Kafka's guard for it is not membership but SUBSCRIPTION: a live consumer group's subscribed topics are refused and everything else is deletable. The facade keeps that rule exactly rather than approximating it, because the coordinator holds each member's JoinGroup metadata verbatim and those bytes are a ConsumerProtocolSubscription.",
};

// ---------------------------------------------------------------------------
// 3. The inverse: APIs a client may look for and will not find
// ---------------------------------------------------------------------------

/**
 * Each of these is asserted ABSENT from `ADVERTISED`, so the day one of them is
 * implemented this generator fails rather than publishing a refusal that no
 * longer happens.
 *
 * Since M7 F4 this is the COMPLETE decision record rather than a selection: the
 * nineteen admin keys below are the same nineteen pinned by
 * `classify_the_absent_admin_apis` in `versions.rs`, and ConsumerGroupHeartbeat
 * above them is pinned by its own test. M9 removed the three transaction keys
 * that used to head this list — AddPartitionsToTxn, EndTxn and TxnOffsetCommit
 * are advertised now, and their windows are in the table above. Each row says what
 * a client wants the key for and what its absence costs a real tool, because
 * "not implemented" and "no tool needs it" are different answers and a reader
 * arriving here is usually holding a tool.
 */
const ABSENT = [
  ["ConsumerGroupHeartbeat", "The KIP-848 broker-side rebalance protocol.", "Excluded by plan; groups use the classic Join/Sync protocol."],
  ["DeleteRecords", "Truncates a partition below an offset. `kafka-delete-records.sh`, and the \"Clear messages\" button in kafka-ui and AKHQ.", "Queen has no truncate-to-offset primitive: a queue's log start moves by retention and by dropping the queue, both time-driven. Implementing it would mean reporting a low watermark that did not move, which is a fabricated value a tool would act on. DeleteTopics then CreateTopics is the workaround, and both work."],
  ["OffsetForLeaderEpoch", "Detects log truncation after a leader change.", "Every leader epoch this facade reports is -1, in Metadata, in ListOffsets, in OffsetFetch and in every record batch, so a consumer's subscription state never holds one and the request is never built. No tool sends it directly, and the cost of the absence is nothing measurable."],
  ["DescribeLogDirs", "Per-partition storage sizes. `kafka-log-dirs.sh`, and the Size column in kafka-ui.", "Queen's storage is Postgres segments; there are no log directories, and answering would mean inventing a path and per-partition byte sizes. The best future candidate of the absences: retained bytes are real and already on the queue listing, so honest sizes under one synthetic log dir are possible once Queen reports them per partition rather than per queue. Until then kafka-ui renders the page with a blank Size column."],
  ["CreateDelegationToken", "Mints a broker-signed token from an authenticated principal.", "A delegation token is derived from a SCRAM principal and signed with a cluster secret. This facade mints no credentials; Queen does. `kafka-delegation-tokens.sh` fails, and nothing in the client matrix uses it."],
  ["RenewDelegationToken", "Extends a delegation token's life.", "Same reason."],
  ["ExpireDelegationToken", "Revokes a delegation token early.", "Same reason."],
  ["DescribeDelegationToken", "Lists the tokens a principal holds.", "Same reason."],
  ["ElectLeaders", "Moves a partition's leadership to its preferred replica. `kafka-leader-election.sh`.", "One logical broker and no replicas: every Metadata answer is replicas=[0], isr=[0]. In cluster mode a partition's leader is a rendezvous hash over the live set, which is deterministic and not movable. There is no preferred replica to elect and no unclean election to permit."],
  ["AlterPartitionReassignments", "Moves replicas between brokers. `kafka-reassign-partitions.sh`, Cruise Control.", "There are no replicas to move; durability is Postgres's. A reassignment API over one logical broker would accept a plan and then have nothing to do with it."],
  ["ListPartitionReassignments", "Reports reassignments in flight.", "Same reason. No UI in the client matrix calls it on a render path."],
  ["DescribeClientQuotas", "Reads the produce/fetch quotas for a (user, client-id).", "The facade DOES have quotas, as Queen's 429 with Retry-After surfaced as throttle_time_ms, but they are the Cloud proxy's and they are per TENANT, which is not expressible in Kafka's entity model. Describing them would mean inventing an entity mapping. The one absence with a real future story: a read-only version of this key mapping the tenant onto a user entity, once the proxy exposes the cap."],
  ["AlterClientQuotas", "Writes those quotas.", "Same mapping problem, and a second and independent reason it must never work: it would let a tenant raise its own rate cap from a Kafka client, which is a privilege escalation. This key stays absent even if the read half ever lands."],
  ["DescribeUserScramCredentials", "Lists SCRAM users.", "SASL here is PLAIN only and the credential is a Queen bearer token verified by Queen. There is no local user store to describe."],
  ["AlterUserScramCredentials", "Creates or rotates a SCRAM credential.", "Supporting SCRAM would require the facade to hold salted password verifiers: to become a credential store, with its own rotation, its own secrets at rest and its own blast radius. That is a security posture change rather than a protocol gap, and it is not a decision this milestone could take."],
  ["DescribeQuorum", "Describes the KRaft metadata quorum. `kafka-metadata-quorum.sh`.", "No Raft log and no voters, so every field would be invented. The one thing UIs actually render from it, a controller id, is already in every Metadata answer. kafka-ui feature-detects and hides its KRaft panel."],
  ["DescribeCluster", "Cluster id, controller and broker list in one call.", "Answerable truthfully, and deliberately not answered: every client in the compatibility matrix already resolves describeCluster() from a plain Metadata request, so advertising it would move five live suites onto a code path none of them exercises today for a measured gain of zero. The trigger that flips this is the first client whose describeCluster() stops falling back."],
  ["DescribeProducers", "Per-partition producer state: id, epoch, last sequence. `kafka-transactions.sh find-hanging`.", "The facade's idempotence window is PROCESS state and is deliberately lost on restart. Answering from it would advertise durable producer state the facade does not have; answering an empty list would say nothing is producing while producers produce. Both are lies."],
  ["DescribeTransactions", "Describes one transaction's state.", "Transactions landed in M9 and this key still does not, for the reason that survived them: an open transaction is a stage held by ONE facade process, on the connection that opened it. A node can describe only its own, so an operator asking a load-balanced address would get a different answer per connection, and `kafka-transactions.sh` would report a transaction as absent because it asked the wrong node."],
  ["ListTransactions", "Lists transactions in flight.", "Same reason, and it is the one that bites harder: a LIST that is per node reads as the whole cluster's and is not."],
];

// ---------------------------------------------------------------------------

function main() {
  const check = isCheck();
  const text = repoRead(VERSIONS);
  const { rows, block } = parseAdvertised(text);

  if (rows.length < 10) {
    throw new Error(`only parsed ${rows.length} rows out of ${VERSIONS} — the parser is broken`);
  }
  assertFingerprint(`${VERSIONS} :: ADVERTISED`, block, ADVERTISED_FINGERPRINT);

  const advertised = new Set(rows.map((r) => r.api));
  const unexplained = rows.filter((r) => !WINDOW_REASON[r.api]).map((r) => r.api);
  if (unexplained.length) {
    throw new Error(
      `advertised with no reason for its window in this script: ${unexplained.join(", ")}`,
    );
  }
  const orphaned = Object.keys(WINDOW_REASON).filter((k) => !advertised.has(k));
  if (orphaned.length) {
    throw new Error(`this script explains a window that is no longer advertised: ${orphaned.join(", ")}`);
  }
  const contradicted = ABSENT.filter(([api]) => advertised.has(api)).map(([api]) => api);
  if (contradicted.length) {
    throw new Error(
      `listed as not offered but present in ADVERTISED: ${contradicted.join(", ")}. ` +
        `Move the row out of ABSENT in this script and give it a window reason.`,
    );
  }

  const lines = [];
  lines.push(
    `The facade advertises **${rows.length} Kafka APIs**. Every row is read out of ` +
      `\`${VERSIONS}\` at build time, which is the same table the ApiVersions response is built ` +
      `from and the same table every incoming request is checked against.`,
    "",
    "| API | Versions | Where the window ends, and why |",
    "| --- | --- | --- |",
  );
  for (const r of [...rows].sort((a, b) => a.api.localeCompare(b.api))) {
    // En dash, which is what a numeric range takes; the site's prose check
    // bans em dashes and leaves this one alone.
    const window = r.min === r.max ? `v${r.min}` : `v${r.min}–v${r.max}`;
    lines.push(`| \`${r.api}\` | ${window} | ${cell(WINDOW_REASON[r.api])} |`);
  }
  lines.push(
    "",
    "### Not offered",
    "",
    "A client that sends one of these gets no response frame at all: the connection closes, " +
      "with the reason in the facade's log. That is Apache Kafka's own behaviour for an " +
      "unparseable request, and it is unreachable for a client that read the ApiVersions " +
      "answer, which is every client. Each row is a decision with a test behind it: the " +
      "facade's own suite asserts that none of these keys is advertised, so offering one by " +
      "accident fails a test rather than shipping.",
    "",
    "| API | What a client wants it for | Why it is not here |",
    "| --- | --- | --- |",
  );
  for (const [api, what, why] of ABSENT) {
    lines.push(`| \`${api}\` | ${cell(what)} | ${cell(why)} |`);
  }

  const res = emitPartial({
    name: "kafka-support-matrix",
    title: "Kafka support matrix",
    description:
      "Every Kafka API the queen-kafka facade advertises, its version window, and the APIs it deliberately does not offer.",
    sources: [`${VERSIONS} (ADVERTISED)`],
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

// Printed by `--fingerprint`, so the number to paste back in never has to be
// computed by hand from a failure message.
if (process.argv.includes("--fingerprint")) {
  console.log(fingerprint(parseAdvertised(repoRead(VERSIONS)).block));
}
