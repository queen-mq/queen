/**
 * Generate the "what can a tenant call through the proxy" table.
 *
 * `queen_proxy/src/routes.rs` is the enforcement spec: every broker-bound
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
  repoRead,
  sliceBlock,
} from "./lib/source.mjs";

const PROXY_ROUTES = "queen_proxy/src/routes.rs";
const MAIN = "server/src/main.rs";

// Bump after re-reading the Rust when a guard trips.
const CLASSIFY_FINGERPRINT = "06215a923e412832";
const OPERATOR_FINGERPRINT = "52b500170c565d9f";

// --- mirror of `is_operator_route` -----------------------------------------

const OPERATOR_ROUTES = new Set([
  "/api/v1/status",
  "/api/v1/status/buffers",
  "/api/v1/analytics/system-metrics",
  "/api/v1/analytics/worker-metrics",
  "/api/v1/analytics/postgres-stats",
  "/api/v1/system/maintenance",
  "/metrics/prometheus",
]);

// --- mirror of `classify` --------------------------------------------------

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

  if (p === "/api/v1/configure") return "queue admin";
  if (p.startsWith("/api/v1/resources/queues/") && m === "DELETE") return "queue admin";
  if (p.startsWith("/api/v1/messages/") && m === "DELETE") return "queue admin";
  if (p.startsWith("/api/v1/consumer-groups") && (m === "DELETE" || m === "POST")) return "queue admin";

  if (p.startsWith("/streams/")) return "gated (streams)";
  if (p === "/api/v1/traces" && m === "POST") return "gated (traces)";

  if (
    p.startsWith("/api/v1/resources") ||
    p.startsWith("/api/v1/status") ||
    p.startsWith("/api/v1/analytics") ||
    p.startsWith("/api/v1/consumer-groups") ||
    p === "/api/v1/dlq" ||
    p.startsWith("/api/v1/messages") ||
    p.startsWith("/api/v1/traces")
  ) {
    return "read";
  }

  if (p.startsWith("/api/")) return "blocked";
  return "read";
}

const CLASS_MEANING = [
  ["produce", "Counted against the message quota. May create queues and partitions implicitly."],
  ["consume", "Pop, ack and lease extension. A `wait=true` pop also holds a parked-consumer slot."],
  ["queue admin", "Configuration, deletions, seeks and subscription changes."],
  ["read", "Listings, status, analytics, DLQ and message reads — all tenant-scoped."],
  ["gated (streams)", "Available when the plan enables the streams feature."],
  ["gated (traces)", "Writing a trace is available when the plan enables the traces feature."],
  ["operator", "Cell-wide surfaces. Not tenant-scopable, so a tenant credential gets the same 404 a blocked route returns."],
  ["blocked", "Never exposed to a tenant, whatever the credential. Returns 404."],
];

/** The broker's real routes — same parse as gen-routes.mjs. */
function brokerRoutes() {
  const block = sliceBlock(repoRead(MAIN), "let app = Router::new()", ".with_state(state);");
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

function main() {
  const check = isCheck();
  const text = repoRead(PROXY_ROUTES);

  assertFingerprint(`${PROXY_ROUTES} :: classify`, fnBody(text, "pub fn classify"), CLASSIFY_FINGERPRINT);
  assertFingerprint(`${PROXY_ROUTES} :: is_operator_route`, fnBody(text, "fn is_operator_route"), OPERATOR_FINGERPRINT);

  const routes = brokerRoutes().map((r) => ({ ...r, class: classify(r.method, concrete(r.path)) }));

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
