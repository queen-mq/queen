/**
 * Generate the Kafka facade's support matrix from its advertised-versions table.
 *
 * `queen-kafka/src/versions.rs` is the compatibility contract in one place: the
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

const VERSIONS = "queen-kafka/src/versions.rs";

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
const ADVERTISED_FINGERPRINT = "c111e0e866a683de";

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
    "Ceiling: v8 fetches offsets for several groups in one request and changes the response shape. v7's `require_stable` is answered honestly, because there are no open transactions here to withhold anything from.",
  FindCoordinator:
    "Ceiling: v4 is the batched form, which exists for clusters where groups live on different brokers. Here the answer is this process, for every key, always.",
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
    "Ceiling: v5 exists for KIP-890's transaction protocol 2, and transactions are refused here \u2014 advertising the version that exists only for them would be advertising a refusal. v3 is load-bearing rather than a nicety: it is KIP-360's epoch bump, and it is what turns a sequence window this facade has lost (a restart, an evicted entry) into a reset the producer recovers from instead of a fatal error. Read the REQUEST schema for the cap, not the key's `valid_versions()`, which answers wider because it takes the maximum of request and response.",
};

// ---------------------------------------------------------------------------
// 3. The inverse: APIs a client may look for and will not find
// ---------------------------------------------------------------------------

/**
 * Each of these is asserted ABSENT from `ADVERTISED`. They are the APIs whose
 * absence a reader is most likely to be looking for, either because a plan
 * excludes them by name or because a tool sends them without being asked to.
 */
const ABSENT = [
  ["AddPartitionsToTxn", "Enrols a partition in an open transaction.", "Transactions and exactly-once are excluded by plan. InitProducerId IS advertised, for the idempotent producer alone: a non-transactional grant is answered, a `transactional.id` is refused."],
  ["EndTxn", "Commits or aborts a transaction.", "Same exclusion."],
  ["TxnOffsetCommit", "Commits consumer offsets inside a transaction.", "Same exclusion, and the reason Kafka Streams cannot run against the facade."],
  ["ConsumerGroupHeartbeat", "The KIP-848 broker-side rebalance protocol.", "Excluded by plan; groups use the classic Join/Sync protocol."],
  ["AlterConfigs", "Writes a topic's or broker's configuration.", "Queue options are a Queen surface. DescribeConfigs reads them, and reports every key `read_only` precisely because nothing here can write one back."],
  ["OffsetDelete", "Removes a group's committed offsets for named partitions.", "A partial reset with no membership guard in front of it. DeleteGroups is the whole tool, and it keeps Kafka's rule that only an empty group is deletable."],
  ["DeleteRecords", "Truncates a partition below an offset.", "Retention is a Queen queue option."],
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
      "answer, which is every client.",
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
