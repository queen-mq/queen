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
// 2026-09-24: ONE row appended — DescribeLogDirs 1-4 — and moved out of
// ABSENT. Thirty-three rows. It answers only where it is true: a facade running
// inside a raft broker, whose data directory holds every partition. And
// CreatePartitions stopped being a refusal for an increase: it raises a tracked
// topic's declared width, which is what `kafka-topics.sh --alter --partitions`
// and kload's chunked creation need.
// 2026-10-02: no window moved (the fingerprint is unchanged), but the prose
// had gone stale under it. b51e4419 (2.0.0-beta.5) answers ListOffsets for a
// concrete time from the broker's append stamps, a transaction's stage now
// outlives its connection, and Queen 2.0's configure merges instead of
// rewriting every column. Every reason below was re-read against versions.rs
// and the handlers it names, and rewritten for a reader of the site: no
// milestone names (M7, M9) and no internal suite names.
const ADVERTISED_FINGERPRINT = "109b216bd2ff3a35";

/** One or two sentences per API: where the window stops, and why. */
const WINDOW_REASON = {
  Produce:
    "Floor: v3 is the first version that carries RecordBatch v2, the format every client of the last decade sends. Ceiling: v10 adds a leader-change hint, which has nothing to point at because every node serves every partition, and v13 names topics by id.",
  Fetch:
    "Ceiling: v7 introduces fetch sessions (KIP-227), state a broker keeps per connection. The facade keeps none, so a request names every partition it wants every time. One side effect: librdkafka compresses zstd only for brokers at Fetch v10 or later, so librdkafka-based producers send their zstd batches uncompressed here.",
  ListOffsets:
    "Ceiling: v7 adds MAX_TIMESTAMP, a question about the records' own timestamps, which the broker does not read. Up to v5 there are three questions (earliest, latest, a concrete time) and all three are answered; a time is looked up on the broker's append clock.",
  ApiVersions:
    "Ceiling: one below the schema, on purpose. v3 is what clients negotiate with a Kafka 3.x broker, and a client that opens at v4 (Java 4.x does) is answered with this window and retries at v3, as the protocol specifies.",
  Metadata:
    "Ceiling: v10 adds topic ids, and the facade has no registry to resolve a topic by id. v9 is already the flexible encoding and carries every field a client reads.",
  OffsetCommit:
    "Ceiling: v7 carries `group_instance_id`, and static membership is not supported. Floor: v0 and v1 are the ZooKeeper-era offset store.",
  OffsetFetch:
    "Ceiling: v8 asks about several groups in one request and changes the response shape. v7's `require_stable` costs nothing: a transaction's offsets are written at its commit, in the same raft entry as its records, so every offset returned is already stable.",
  FindCoordinator:
    "Ceiling: v4 is the batched form, for clusters where groups live on different brokers. In cluster mode a group key resolves to the node that coordinates it, and a transaction key is refused with TRANSACTIONAL_ID_AUTHORIZATION_FAILED, fatal on purpose, so `initTransactions()` fails at once instead of retrying for `max.block.ms`.",
  JoinGroup:
    "Ceiling: v5 carries `group_instance_id`, and static membership is not supported. v4's MEMBER_ID_REQUIRED round trip is implemented.",
  Heartbeat: "Ceiling: v3 carries `group_instance_id`.",
  LeaveGroup:
    "Ceiling: v3 carries `group_instance_id` and lets one request remove several members.",
  SyncGroup: "Ceiling: v3 carries `group_instance_id`.",
  SaslHandshake:
    "Both versions, because they are the two SASL flows (raw tokens after v0, SaslAuthenticate requests after v1), and both are implemented.",
  SaslAuthenticate:
    "Ceiling: v2 only changes the encoding. v1's `session_lifetime_ms` is answered 0, so no client re-authenticates on a timer.",
  CreateTopics:
    "Ceiling: v7 returns a topic id, which the facade does not mint. v4 lets a client send -1 for the partition count and the replication factor, v5 returns the created topic's configs, and v6 understands THROTTLING_QUOTA_EXCEEDED, the answer past 100 topics in one request.",
  DeleteTopics:
    "Ceiling: v6 accepts a topic id in place of a name. v5 adds `error_message`, which says in words that there is no such queue.",
  DescribeConfigs:
    "The whole schema. A key is reported only where the facade can name what enforces it, or where it keeps what a client set; v3's `config_type` and `documentation` are filled in for those keys.",
  ListGroups:
    "Ceiling: v5 adds `group_type`, which tells classic groups from KIP-848 ones, and KIP-848 is not supported. v4's state filter (`--state`) is honoured.",
  DescribeGroups:
    "Ceiling: v4 carries `group_instance_id`. v3's authorized operations are answered with Kafka's own 'omitted' value, because there is no ACL model.",
  DeleteGroups:
    "The whole schema; v2 only changes the encoding. Deleting a group deletes the Queen consumer group of that name, with its position on every queue.",
  InitProducerId:
    "Ceiling: v5 exists for KIP-890's transaction protocol 2, which the facade does not run. v3 matters: it is KIP-360's epoch bump, which lets a producer recover when a node has lost its sequence window (a restart, an eviction). A transactional id is refused in cluster mode.",
  DescribeAcls:
    "The whole window, and what it advertises is Kafka's own refusal: SECURITY_DISABLED, the answer of a Kafka broker with no authorizer. Authorization here is Queen's, on the token.",
  CreateAcls: "The same window and the same refusal, one result per creation.",
  DeleteAcls:
    "The same window and the same refusal, one result per filter. Advertising the three turns 'this broker is too old' into the message a Kafka with security off prints.",
  AlterConfigs:
    "The whole window. This is the deprecated full-replacement form, honoured literally: a key the request does not name goes back to its default. Prefer IncrementalAlterConfigs.",
  IncrementalAlterConfigs:
    "The whole window, and the request `kafka-configs.sh --alter` sends. It changes topics created through Kafka (by CreateTopics or on first use), whose configuration the facade keeps a record of; a queue created by a Queen client is refused, with the reason.",
  CreatePartitions:
    "The whole window. An increase raises the width of a topic created through Kafka, and the next Metadata reports it; a decrease or an equal count is refused with Kafka's own sentences.",
  DescribeLogDirs:
    "Floor: v1, since KIP-896 dropped v0. Ceiling: the schema's v4, with volume sizes answered -1. The node's data directory is listed with every partition and the bytes it holds, because every raft voter holds every partition. A facade reaching the broker over an explicit `QUEEN_URL` lists no directory.",
  AddPartitionsToTxn:
    "Ceiling: v4 is a different request, KIP-890's broker-to-broker verification, which no client sends.",
  AddOffsetsToTxn:
    "v3 answers everything v4 does. v4 exists for KIP-890's transaction protocol 2, in which clients stop sending this request.",
  EndTxn:
    "Ceiling: v5 returns a new producer epoch from inside EndTxn (KIP-890's transaction protocol 2), which the facade does not do.",
  TxnOffsetCommit:
    "v3 has to be in the window: kafka-clients refuses to build this request below v3 when group metadata is set, which every consume-transform-produce loop does. Ceiling: KIP-890's transaction protocol 2.",
  OffsetDelete:
    "One version. Kafka's rule is kept exactly: the offsets of a topic a live member is subscribed to are refused (GROUP_SUBSCRIBED_TO_TOPIC), everything else can be deleted.",
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
  ["ConsumerGroupHeartbeat", "The KIP-848 consumer group protocol (`group.protocol=consumer`).", "Not supported. Groups use the classic protocol, which every Kafka 4.x client still uses by default; a client set to `consumer` fails at once with an error that names `group.protocol=classic`."],
  ["DeleteRecords", "Truncating a partition below an offset: `kafka-delete-records.sh`, the clear-messages buttons of kafka-ui and AKHQ, and Kafka Streams purging its repartition topics.", "Queen has no trim to an offset yet: a partition's start moves by retention and when the queue is deleted. Answering would report a low watermark that did not move. Delete and recreate the topic instead."],
  ["OffsetForLeaderEpoch", "Detecting log truncation after a leader change.", "Every leader epoch the facade reports is -1, so no client ever builds this request."],
  ["CreateDelegationToken", "A broker-signed token derived from a SCRAM login.", "The facade mints no credentials: the password of a SASL/PLAIN login is a Queen token, checked by Queen."],
  ["RenewDelegationToken", "Extending a delegation token.", "Same reason."],
  ["ExpireDelegationToken", "Revoking a delegation token.", "Same reason."],
  ["DescribeDelegationToken", "Listing a principal's delegation tokens.", "Same reason."],
  ["ElectLeaders", "Moving a partition's leadership to its preferred replica (`kafka-leader-election.sh`).", "A partition leader here is advertised to spread clients over the nodes, and every node serves every partition; raft elects the one leader of the log on its own."],
  ["AlterPartitionReassignments", "Moving replicas between brokers (`kafka-reassign-partitions.sh`, Cruise Control).", "Every raft voter holds every partition, so there is nothing to move."],
  ["ListPartitionReassignments", "Reassignments in flight.", "Same reason."],
  ["DescribeClientQuotas", "Produce and fetch quotas per user and client id.", "Queen's limits are per tenant and reach a client as `throttle_time_ms`; Kafka's quota entities have nothing to map onto."],
  ["AlterClientQuotas", "Changing those quotas.", "The same mapping problem, and it would let a client raise its own tenant's limit."],
  ["DescribeUserScramCredentials", "Listing SCRAM users.", "SASL here is PLAIN, checked against Queen, and the facade keeps no user store."],
  ["AlterUserScramCredentials", "Creating or rotating a SCRAM credential.", "Supporting SCRAM would make the facade a credential store, with secrets of its own at rest."],
  ["DescribeQuorum", "KRaft's metadata quorum (`kafka-metadata-quorum.sh`).", "Queen's raft group is not a KRaft quorum. Its state is in `GET /health` and `GET /api/v1/raft/status`."],
  ["DescribeCluster", "Cluster id, controller and brokers in one call.", "Answerable, and left out because the Java and librdkafka admin clients build `describeCluster()` from a Metadata request, which works."],
  ["DescribeProducers", "Producer state per partition (`kafka-transactions.sh find-hanging`).", "The idempotent producers' sequence windows live in node memory and are lost on a restart. Answering would describe durable producer state the facade does not have."],
  ["DescribeTransactions", "One transaction's state.", "An open transaction is staged in the memory of the node that holds it, so another node would answer that it does not exist."],
  ["ListTransactions", "Transactions in flight.", "Same reason: a list from one node would read as the cluster's."],
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
      `\`${VERSIONS}\` when the site is built: the table the ApiVersions answer is made from, and ` +
      `the one every incoming request is checked against.`,
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
    "A client that sends one of these gets no answer: the connection closes, with the reason " +
      "in the node's log, which is what Apache Kafka does with a request it cannot parse. A " +
      "client that read the ApiVersions answer never sends one. Each absence is a decision with " +
      "a test behind it, so offering one of these by accident fails the facade's own suite " +
      "before it ships.",
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
    body: lines.map(noEmDash).join("\n"),
    check,
  });
  return res;
}

/**
 * The "why" texts mirror Rust doc comments, which use em dashes; the site's
 * house style bans them in anything a reader sees (scripts/check-prose.mjs).
 * Per table cell: a pair becomes a parenthesis, a single one a colon.
 */
function noEmDash(line) {
  return line
    .split(" | ")
    .map((cell) => {
      const n = (cell.match(/ — /g) || []).length;
      if (n === 2) return cell.replace(/ — (.*?) — /, " ($1) ");
      return cell.replace(/ — /g, ": ");
    })
    .join(" | ");
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
