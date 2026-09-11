/**
 * Generate the "what can a tenant call through the proxy" table.
 *
 * `proxy/src/routes.rs` is the enforcement spec: every broker-bound
 * request is classified into exactly one class, and anything API-shaped that
 * matches nothing is blocked. This script applies the same classification to
 * the broker's real route list, so the table is a join of two derived facts
 * rather than a hand-maintained list.
 *
 * The classifier is mirrored in JavaScript behind a fingerprint guard — change
 * the Rust and this script refuses to run until the mirror is updated.
 */

import {
  assertFingerprint,
  cell,
  emitPartial,
  fnBody,
  isCheck,
  ROUTER_BUILDER,
  repoRead,
  sliceBlock,
} from "./lib/source.mjs";

const PROXY_ROUTES = "proxy/src/routes.rs";
const MAIN = "server/src/main.rs";

// Bump after re-reading the Rust when a guard trips.
// 2026-08-17: `classify` grew the KV and timer families and split every gated
// class into a `(Feature, GatedOp)` pair, where the second half is the quota
// decision and not a second feature flag. Re-read and mirrored below.
// 2026-08-21: `classify` grew the ephemeral family (EPHEMERAL_QUEUES.md §5.1),
// one more `Feature` behind one more default-false plan key, method-exact on
// every path in it. Re-read and mirrored below.
// 2026-08-27: `POST /streams/v1/cycle` moved from `Open` to `Mixed` (streams
// tenant-compat pass): the cycle's sink push_items are the produce half of the
// family, so the storage/monthly blocks refuse a cycle only when the body
// actually grows. The op half never names a class here (same as kv/timers), so
// the mirror below is unchanged and only the class meaning grew. Re-read.
// 2026-08-30: `classify` grew ONE arm, for `POST /api/v1/fetch`
// (PLAN_QUEEN_KAFKA.md C2) — the batched read-from-offset the Kafka facade
// consumes through, and the only consume path it has. It answers `Consume`
// rather than the `Read` its read-only semantics suggest, because the class is
// an authorization decision and the route hands out message payloads. POST on
// the exact path only: every other method, and the trailing-slash spelling,
// are `Blocked` rather than left to travel to a 405, the same fail-closed rule
// the kv/timer/ephemeral families state. Re-read in full: nothing else in the
// function moved, and `is_operator_route` is untouched.
// 2026-09-04: `classify` grew ONE arm, for `DELETE /api/v1/dlq` — the bulk
// dead-letter purge. `QueueAdmin`, like the single-row `DELETE` on
// `/api/v1/messages/` above it, and path-exact rather than a prefix, so the
// `GET` on the same path keeps falling through to `Read`. Re-read in full:
// nothing else in the function moved, and `is_operator_route` is untouched.
// 2026-09-04: `classify` grew ONE arm, for `POST /api/v1/partitions/changed`
// (PLAN_S3_SINK.md §5.1, §8) — the partition-discovery call the S3 sink reads
// its queue map from, and the fetch arm above copied with the path swapped. It
// answers `Consume` for the same reason the fetch does: the class is an
// authorization decision, and this route hands out the partition names,
// offsets and retention watermarks of a tenant's queues. POST on the exact path
// only, everything else `Blocked`. Re-read in full: nothing else in the
// function moved, and `is_operator_route` is untouched. The body-conditional
// KV reclassification that ships with it lives in `proxy/src/s3_kv.rs` and is
// deliberately NOT here: this table is keyed by (path, method), so a rule that
// depends on a request body cannot be stated in it without publishing a
// falsehood — the same reason `kafka_kv.rs` is absent.
// 2026-09-11: `classify` grew TWO arms, for the DLQ replay pair
// (PLAN_DASHBOARD_ACTIONS.md §1.4, §2.0) — `POST /api/v1/messages/:pid/:txid/retry`
// and the move primitive's `POST /api/v1/dlq/:id/replay`. Both answer
// `QueueAdmin`, and both are a FIX rather than a new surface: the retry route
// matched no arm and fell through the `/api/v1/messages` reads prefix, so this
// table published it as `read` — the class every user role has and that an api
// key gets from `scopes.read` alone, which let a Viewer replay a dead letter
// through the proxy. POST on the prefix+suffix pair only; every other method is
// `Blocked`, DELETE included, which is why the two arms sit ABOVE the
// `/api/v1/messages/` DELETE arm in the Rust and in the mirror below. The push
// half of a replay is not visible in this table at all: it is a GATE, not a
// class (`gateway::is_dlq_replay` runs the storage and monthly push blocks for
// it), and it is stated in the class meaning instead. Re-read in full: nothing
// else in the function moved, and `is_operator_route` is untouched.
// 2026-09-11 (second pass, from the review of the above): the two new arms match
// a SHAPE rather than a suffix — two non-empty segments between
// `/api/v1/messages/` and `/retry`, one between `/api/v1/dlq/` and `/replay` —
// because a transaction id is arbitrary caller text and `/api/v1/messages/:pid/retry`
// is the plain address of a message whose transaction id is `retry`, whose GET
// and DELETE a suffix test was taking away. And the `/api/v1/messages` READS
// prefix, the root of the P0, is now METHOD-BOUNDED: GET (with the HEAD axum
// answers from it) is a read and every other method under the family fails
// closed, so the next write route registered there cannot inherit `Read` the
// way the retry route did. The other four read prefixes stay method-agnostic —
// one of them must, since the KV browser's list is a `Read` POST (§2.5).
// Re-read in full: nothing else in the function moved, and `is_operator_route`
// is untouched.
const CLASSIFY_FINGERPRINT = "0737293f24981410";
const OPERATOR_FINGERPRINT = "04d6dea7366b466d";

// --- mirror of `is_operator_route` -----------------------------------------

const OPERATOR_ROUTES = new Set([
  "/api/v1/status",
  "/api/v1/status/buffers",
  "/api/v1/analytics/system-metrics",
  "/api/v1/analytics/worker-metrics",
  "/api/v1/analytics/postgres-stats",
  // Both maintenance kill switches, both halves. `/system/shared-state` is
  // deliberately NOT here and stays blocked.
  "/api/v1/system/maintenance",
  "/api/v1/system/maintenance/pop",
  "/metrics/prometheus",
]);

// --- mirror of `classify` --------------------------------------------------

/**
 * How many non-empty path segments sit between `prefix` and `suffix`, or -1
 * when the path does not carry both. The Rust spells this inline with
 * `strip_prefix` + `strip_suffix`; a route is matched by its SHAPE, so a
 * transaction id or a row id that happens to read like a route suffix is not
 * one.
 */
function segmentsBetween(p, prefix, suffix) {
  if (!p.startsWith(prefix) || !p.endsWith(suffix)) return -1;
  const mid = p.slice(prefix.length, p.length - suffix.length);
  const parts = mid.split("/");
  return parts.every((s) => s.length > 0) ? parts.length : -1;
}

function classify(m, p) {
  if (OPERATOR_ROUTES.has(p)) return "operator";

  if (
    p.startsWith("/api/v1/migration") ||
    p.startsWith("/api/v1/system") ||
    p.startsWith("/internal") ||
    p === "/api/v1/stats/refresh" ||
    p === "/metrics" ||
    p === "/status"
  ) {
    return "blocked";
  }
  if (p === "/api/v1/pop" || p === "/api/v1/pop/") return "blocked";

  if (p === "/api/v1/push") return "produce";
  if (p === "/api/v1/transaction") return "produce";
  if (p.startsWith("/api/v1/pop/queue/")) return "consume";
  if (p === "/api/v1/ack" || p === "/api/v1/ack/batch") return "consume";
  if (p.startsWith("/api/v1/lease/")) return "consume";
  // The C2 fetch. Method-exact on the exact path, like the gated families
  // below: the broker registers one method here and axum redirects no trailing
  // slash, so every other spelling is `Blocked` in the Rust rather than left to
  // travel to a 405.
  if (p === "/api/v1/fetch") {
    return m === "POST" ? "consume" : "blocked";
  }
  // PLAN_S3_SINK.md §5.1: the fetch arm with the path swapped, and method-exact
  // on the exact path for the same reason.
  if (p === "/api/v1/partitions/changed") {
    return m === "POST" ? "consume" : "blocked";
  }

  if (p === "/api/v1/configure") return "queue admin";
  if (p.startsWith("/api/v1/resources/queues/") && m === "DELETE") return "queue admin";
  // The DLQ replay pair, above the messages `DELETE` arm exactly as in the
  // Rust: a replay deletes the dead-letter row it re-pushes, so it carries the
  // queue-admin authority, and it is method- and SHAPE-exact so that a `DELETE`
  // on a `.../retry` address is not read as a message delete. Everything else
  // on these two shapes is `Blocked` rather than left to travel to a 405.
  //
  // Shape and not suffix: a transaction id is arbitrary caller text, so
  // `/api/v1/messages/:pid/retry` is the plain address of a message whose
  // transaction id is `retry`, and matching the tail alone would take its GET
  // and its DELETE away. Two non-empty segments for the retry route, one for
  // the row id of the replay.
  if (segmentsBetween(p, "/api/v1/messages/", "/retry") === 2) {
    return m === "POST" ? "queue admin" : "blocked";
  }
  if (segmentsBetween(p, "/api/v1/dlq/", "/replay") === 1) {
    return m === "POST" ? "queue admin" : "blocked";
  }
  if (p.startsWith("/api/v1/messages/") && m === "DELETE") return "queue admin";
  if (p === "/api/v1/dlq" && m === "DELETE") return "queue admin";
  if (p.startsWith("/api/v1/consumer-groups") && (m === "DELETE" || m === "POST")) return "queue admin";

  if (p.startsWith("/streams/")) return "gated (streams)";
  if (p === "/api/v1/traces" && m === "POST") return "gated (traces)";

  // The KV and timer families. The Rust returns `Gated(Feature, GatedOp)`; the
  // feature half is the plan gate and the op half is the quota decision, so
  // only the feature half names a class here and the op half is described in
  // the class meaning. Any method the broker does not register on these paths
  // is `Blocked` in the Rust rather than left to travel to a 405, so the same
  // fail-closed default is mirrored rather than assumed.
  if (p === "/api/v1/kv" || p === "/api/v1/kv/") {
    return m === "POST" ? "gated (kv)" : "blocked";
  }
  if (p.startsWith("/api/v1/kv/")) {
    return ["GET", "PUT", "DELETE"].includes(m) ? "gated (kv)" : "blocked";
  }
  if (p === "/api/v1/timers" || p === "/api/v1/timers/") {
    return m === "POST" ? "gated (timers)" : "blocked";
  }
  if (p.startsWith("/api/v1/timers/")) {
    return ["GET", "DELETE"].includes(m) ? "gated (timers)" : "blocked";
  }

  // The ephemeral family. Same shape as the two above: one `Feature` behind
  // one plan key, and the `GatedOp` half is the quota decision rather than a
  // second flag, so only the feature half names a class here. Method-exact on
  // every path, and everything else under the prefix is `Blocked` in the Rust
  // rather than left to travel to a 405.
  if (p.startsWith("/api/v1/ephemeral/")) {
    if (m === "POST") {
      return [
        "/api/v1/ephemeral/push",
        "/api/v1/ephemeral/ack",
        "/api/v1/ephemeral/configure",
        "/api/v1/ephemeral/reset",
      ].includes(p)
        ? "gated (ephemeral)"
        : "blocked";
    }
    if (m === "GET") {
      if (p === "/api/v1/ephemeral/pop" || p === "/api/v1/ephemeral/queues") return "gated (ephemeral)";
      return p.startsWith("/api/v1/ephemeral/queues/") && p.endsWith("/depth")
        ? "gated (ephemeral)"
        : "blocked";
    }
    if (m === "DELETE" && p.startsWith("/api/v1/ephemeral/queue/")) return "gated (ephemeral)";
    return "blocked";
  }

  // The messages family is METHOD-BOUNDED, and it is the only read prefix that
  // is: it contains writes (the DELETE and the retry POST, both classified
  // above), and a method-agnostic `Read` in front of a family that writes is
  // exactly what published the replay route as a read. GET — and the HEAD axum
  // answers from it — is the read; everything else fails closed.
  if (p === "/api/v1/messages" || p.startsWith("/api/v1/messages/")) {
    return m === "GET" || m === "HEAD" ? "read" : "blocked";
  }
  if (
    p.startsWith("/api/v1/resources") ||
    p.startsWith("/api/v1/status") ||
    p.startsWith("/api/v1/analytics") ||
    p.startsWith("/api/v1/consumer-groups") ||
    p === "/api/v1/dlq" ||
    p.startsWith("/api/v1/traces")
  ) {
    return "read";
  }

  if (p.startsWith("/api/")) return "blocked";
  return "read";
}

const CLASS_MEANING = [
  ["produce", "Counted against the message quota. May create queues and partitions implicitly."],
  [
    "consume",
    "Pop, ack, lease extension, the batched read-from-offset the Kafka facade consumes through, and the partition discovery the S3 sink maps a queue with. A `wait=true` pop also holds a parked-consumer slot; the fetch does not, since its long poll is a body field rather than a query flag. Both reads are classified for the authority they need rather than for what they write: they are non-destructive and never quota-blocked, but one hands out message payloads and the other the partition names and offsets to read them by, so they carry the authority of the pop they stand in for instead of the read level every user role already has.",
  ],
  [
    "queue admin",
    "Configuration, deletions, seeks, subscription changes and the dead-letter replay. The replay is the one route in this class that also WRITES a message: it re-pushes a dead-letter snapshot and drops the dead-letter record in the same act, so it carries the authority of the deletion and answers the storage and monthly push blocks like a push. Everything else here is never quota-blocked, since deleting and shortening retention are how a tenant that is over a quota gets back under it.",
  ],
  ["read", "Listings, status, analytics, DLQ and message reads, all tenant-scoped."],
  [
    "gated (streams)",
    "Available when the plan enables the streams feature. Registration and the state read are never quota-blocked. The cycle is the produce half of the family: a cycle whose body carries sink `push_items` answers the storage and monthly blocks (refused whole, with the same code the equivalent push would get), its sink queues and partitions pass registry admission, its items answer the per-item payload cap, and its accepted sink messages are billed as push. An ack-only or state-only cycle always passes, so a blocked tenant can keep draining its source.",
  ],
  ["gated (traces)", "Writing a trace is available when the plan enables the traces feature."],
  [
    "gated (kv)",
    "Available when the plan enables the KV feature, which a plan that has never heard of it does not. A `PUT` is the half a storage quota blocks; a `GET` is read level; a `DELETE` is how a tenant at its cap gets back under it and is never quota-blocked. The batch `POST` carries both halves in one array, so a quota refuses the whole call with a named reason rather than dropping part of it.",
  ],
  [
    "gated (timers)",
    "Available when the plan enables the timers feature. Scheduling is quota-blockable; cancelling is not, and has its own route for that reason, since a tenant blocked from cancelling would keep producing messages it can no longer stop.",
  ],
  [
    "gated (ephemeral)",
    "Available when the plan enables the ephemeral feature, which a plan that has never heard of it does not. A `push` is the half a storage quota blocks and its messages are counted like any other; a pop is metered as a delivery and holds a parked-consumer slot while it waits; ack, configure, reset and the queue `DELETE` are write level and never quota-blocked, since dropping a queue is how a tenant gets its memory back. The two status reads are read level.",
  ],
  ["operator", "Cell-wide surfaces. Not tenant-scopable, so a tenant credential gets the same 404 a blocked route returns."],
  ["blocked", "Never exposed to a tenant, whatever the credential. Returns 404."],
];

/** The broker's real routes — same parse as gen-routes.mjs. */
function brokerRoutes() {
  const block = sliceBlock(repoRead(MAIN), ROUTER_BUILDER, ".with_state(state);");
  const routes = [];
  const re = /\.route\(\s*"([^"]+)"\s*,/g;
  let m;
  while ((m = re.exec(block))) {
    const path = m[1];
    const rest = block.slice(re.lastIndex, re.lastIndex + 400);
    const stop = rest.indexOf(".route(");
    const window = stop === -1 ? rest : rest.slice(0, stop);
    for (const [, verb] of window.matchAll(/\b(get|post|put|patch|delete|head|options)\(\s*[\w:]+/g)) {
      routes.push({ path, method: verb.toUpperCase() });
    }
  }
  return routes;
}

/**
 * The proxy classifies concrete paths; the router declares `:param` patterns.
 * Substitute a representative value so prefix rules match the way they will at
 * runtime.
 */
function concrete(path) {
  return path
    .replace(/:queue/g, "orders")
    .replace(/:partition/g, "customer-42")
    .replace(/:group/g, "billing")
    .replace(/:leaseId/g, "L")
    .replace(/:partitionId/g, "P")
    .replace(/:transactionId/g, "T")
    .replace(/:traceName/g, "N");
}

/**
 * Non-GET `/api/` routes that are `read` ON PURPOSE. One entry today, and every
 * entry costs a sentence saying why the route writes nothing.
 */
const READ_WRITE_ALLOWED = new Map([
  [
    "POST /api/v1/resources/kv/list",
    "the console's KV browser (PLAN_DASHBOARD_ACTIONS.md §2.5): a read-only, " +
      "viewer-visible, cursor-paged list that is a POST only because its cursor is a " +
      "KEY, and a key in a query string is written to every access log between the " +
      "browser and the database",
  ],
]);

/**
 * The bug class §1.4 was, as a build failure.
 *
 * The reads block of `classify` matches by PREFIX. A prefix that covers a route
 * family containing a write hands that write the class every user role has and
 * that an api key gets from `scopes.read` alone, and the push blocks in
 * `plan_gates` never see it: that is exactly how `POST /api/v1/messages/:pid/:txid/retry`
 * came to be callable by a Viewer. The retry route is classified now, and the
 * messages family is method-bounded, but nothing stopped the NEXT write route
 * registered under `/api/v1/resources`, `/api/v1/status`, `/api/v1/analytics` or
 * `/api/v1/traces` from inheriting the same `read`, silently, until someone read
 * this table and noticed.
 *
 * So the join this script already computes answers it: a broker route that is
 * not a GET (nor the HEAD axum answers from one) and classifies as `read` fails
 * generation until someone either gives it an arm in `proxy/src/routes.rs` or
 * writes down here why it reads nothing. Non-`/api/` paths are out of scope:
 * they are the dashboard and static fallback, not a classified API surface.
 */
function assertNoWriteInheritsRead(routes) {
  const offenders = routes.filter(
    (r) =>
      r.class === "read" &&
      r.method !== "GET" &&
      r.method !== "HEAD" &&
      r.path.startsWith("/api/") &&
      !READ_WRITE_ALLOWED.has(`${r.method} ${r.path}`),
  );
  if (!offenders.length) return;
  throw new Error(
    [
      ``,
      `DRIFT: ${offenders.length} broker route(s) classify as \`read\` on a method that is not GET:`,
      ``,
      ...offenders.map((r) => `  ${r.method} ${r.path}`),
      ``,
      `A read prefix in front of a route that writes is the P0 of`,
      `PLAN_DASHBOARD_ACTIONS.md §1.4: \`read\` is the one class every user role has,`,
      `an api key gets it from \`scopes.read\` alone, and \`plan_gates\` runs no push`,
      `block for it. Give the route an arm of its own in ${PROXY_ROUTES}, or — if it`,
      `really only reads — add it to READ_WRITE_ALLOWED in this script with the reason.`,
      ``,
    ].join("\n"),
  );
}

function main() {
  const check = isCheck();
  const text = repoRead(PROXY_ROUTES);

  assertFingerprint(`${PROXY_ROUTES} :: classify`, fnBody(text, "pub fn classify"), CLASSIFY_FINGERPRINT);
  assertFingerprint(`${PROXY_ROUTES} :: is_operator_route`, fnBody(text, "fn is_operator_route"), OPERATOR_FINGERPRINT);

  const routes = brokerRoutes().map((r) => ({ ...r, class: classify(r.method, concrete(r.path)) }));
  assertNoWriteInheritsRead(routes);

  const lines = [];
  lines.push(
    `The proxy classifies every broker-bound request into exactly one class, and anything ` +
      `API-shaped that matches no rule is blocked. The table below applies that classifier to ` +
      `the broker's own route list.`,
    "",
    "| Class | What it means |",
    "| --- | --- |",
  );
  for (const [c, meaning] of CLASS_MEANING) lines.push(`| ${c} | ${meaning} |`);
  lines.push("");

  const byClass = new Map();
  for (const r of routes) {
    if (!byClass.has(r.class)) byClass.set(r.class, []);
    byClass.get(r.class).push(r);
  }

  for (const [c] of CLASS_MEANING) {
    const rows = byClass.get(c);
    if (!rows?.length) continue;
    lines.push(`### ${c}`, "");
    lines.push("| Method | Path |");
    lines.push("| --- | --- |");
    rows.sort((a, b) => a.path.localeCompare(b.path) || a.method.localeCompare(b.method));
    for (const r of rows) lines.push(`| \`${r.method}\` | \`${cell(r.path)}\` |`);
    lines.push("");
  }

  const reachable = routes.filter((r) => !["blocked", "operator"].includes(r.class)).length;
  lines.push(
    `${reachable} of the broker's ${routes.length} method + path pairs are reachable with a ` +
      `tenant credential; the rest are operator or blocked surfaces.`,
  );

  return emitPartial({
    name: "proxy-route-classes",
    title: "proxy route classes",
    description: "How the proxy classifies each broker route, and which ones a tenant credential can reach.",
    sources: [`${PROXY_ROUTES} (classify, is_operator_route)`, `${MAIN} (Router::new chain)`],
    body: lines.join("\n"),
    check,
  });
}

const result = main();
if (result.drifted) {
  console.error(`DRIFT: ${result.file} is behind its source`);
  process.exit(1);
}
console.log(`${result.drifted === false ? "ok" : "wrote"}  ${result.title}`);
