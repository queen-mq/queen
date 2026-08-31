/**
 * Generate the broker's HTTP route table from the router itself.
 *
 * Three facts per route, each derived rather than transcribed:
 *   path + method  — parsed out of the `Router::new()` chain in main.rs
 *   access level   — `auth::route_access_level`, mirrored below behind a
 *                    fingerprint guard
 *   tenant-scoped  — whether the handler takes the `Tenant` request extension
 *
 * The old hand-written site drifted into documenting two routes that never
 * existed. This is the fix for that class of bug.
 */

import {
  assertFingerprint,
  cell,
  emitPartial,
  fnBody,
  isCheck,
  ROUTER_BUILDER,
  repoRead,
  rustFiles,
  sliceBlock,
} from "./lib/source.mjs";

const MAIN = "server/src/main.rs";
const AUTH = "server/src/auth.rs";

// ---------------------------------------------------------------------------
// 1. Routes, straight out of the router chain
// ---------------------------------------------------------------------------

function parseRoutes(text) {
  const block = sliceBlock(text, ROUTER_BUILDER, ".with_state(state);");
  const routes = [];
  const re = /\.route\(\s*"([^"]+)"\s*,/g;
  let m;
  while ((m = re.exec(block))) {
    const path = m[1];
    // The method router runs from the comma to the closing paren of `.route(`.
    // Slice generously and stop at the next `.route(` — the method calls we
    // want are always inside that window.
    const rest = block.slice(re.lastIndex, re.lastIndex + 400);
    const stop = rest.indexOf(".route(");
    const window = stop === -1 ? rest : rest.slice(0, stop);
    const verbs = [...window.matchAll(/\b(get|post|put|patch|delete|head|options)\(\s*([\w:]+)/g)];
    if (verbs.length === 0) continue;
    for (const [, verb, handlerPath] of verbs) {
      routes.push({
        path,
        method: verb.toUpperCase(),
        handler: handlerPath.split("::").pop(),
      });
    }
  }
  return routes;
}

// ---------------------------------------------------------------------------
// 2. Access level — mirror of server/src/auth.rs `route_access_level`
// ---------------------------------------------------------------------------

// Bump this after re-reading the Rust when the guard trips.
// 2026-08-17: re-read for PLAN_KV_TIMERS.md §8.1. The KV and timer families were
// answering read-write on every route including the GETs, because the Rust named
// none of them and they reached the fallthrough at the bottom of the function.
// 2026-08-21: re-read for EPHEMERAL_QUEUES.md §3.9. Three new rules, mirrored
// below in the same order the Rust evaluates them: the two ephemeral STATUS
// reads are read-only and live INSIDE the GET block (the documented trap), the
// ephemeral push is write-only like the durable push, and the rest of the family
// — pop, ack, configure, reset, the queue delete — is read-write by an explicit
// arm rather than by the fallthrough.
// 2026-08-28: re-read for PLAN_QUEEN_KAFKA.md C2. One new rule, mirrored below
// in the position the Rust evaluates it: `POST /api/v1/fetch` is read-only, and
// it sits AFTER the `m === "GET"` block rather than inside it, because a batch
// fetch carries its request in a body. Without the arm it reaches the
// read-write fallthrough and the table claims a log read needs write access.
const ACCESS_FINGERPRINT = "8a02b3abba911b6a";

function accessLevel(method, path) {
  const m = method;

  if (path === "/health" || path === "/metrics" || path === "/metrics/prometheus") return "public";
  if (path === "/" || path.startsWith("/assets/") || path.startsWith("/favicon")) return "public";
  // Broker-direct dashboard identity (handlers/standalone.rs): public by
  // design — /auth/me 401s ITSELF when auth is enabled, /auth/login is the
  // page that explains auth, /auth/logout is a session no-op.
  if (path === "/auth/me" || path === "/auth/login" || path === "/auth/logout") return "public";

  if (path.startsWith("/api/v1/system/") || path.startsWith("/internal/")) return "admin";
  if (m === "DELETE" && path.startsWith("/api/v1/consumer-groups/")) return "admin";
  if (m === "DELETE" && path.startsWith("/api/v1/resources/queues/")) return "admin";
  if (path === "/api/v1/stats/refresh") return "admin";

  if (m === "GET") {
    if (path === "/status") return "read-only";
    if (path.startsWith("/api/v1/status") || path.startsWith("/api/v1/analytics")) return "read-only";
    if (path.startsWith("/api/v1/resources/")) return "read-only";
    if (path.startsWith("/api/v1/messages")) return "read-only";
    if (path.startsWith("/api/v1/consumer-groups")) return "read-only";
    if (path.startsWith("/api/v1/dlq")) return "read-only";
    if (path.startsWith("/api/v1/traces")) return "read-only";
    // PLAN_KV_TIMERS.md §8.1: inside the GET block, never outside it.
    if (path.startsWith("/api/v1/kv")) return "read-only";
    if (path.startsWith("/api/v1/timers")) return "read-only";
    // EPHEMERAL_QUEUES.md §3.9: the two status reads only. The pop is a GET but
    // it mutates, so it is read-write further down.
    if (path.startsWith("/api/v1/ephemeral/queues")) return "read-only";
  }

  // PLAN_QUEEN_KAFKA.md C2: a pure read that takes no lease and moves no
  // cursor. Outside the GET block because the batch request is a body.
  if (m === "POST" && path === "/api/v1/fetch") return "read-only";

  if (path === "/streams/v1/state/get") return "read-only";
  if (path.startsWith("/streams/")) return "read-write";

  if (path === "/api/v1/push") return "write-only";
  // Scheduling a timer is a produce operation; cancelling one is not.
  if (m === "POST" && path === "/api/v1/timers") return "write-only";
  // An ephemeral push is a produce operation, like the durable push.
  if (m === "POST" && path === "/api/v1/ephemeral/push") return "write-only";

  if (path.startsWith("/api/v1/kv") || path.startsWith("/api/v1/timers")) return "read-write";
  // Pop, ack, configure, reset and the queue delete.
  if (path.startsWith("/api/v1/ephemeral")) return "read-write";

  return "read-write";
}

// ---------------------------------------------------------------------------
// 3. Tenant scoping — does the handler take the Tenant extension?
// ---------------------------------------------------------------------------

function tenantScopedHandlers() {
  const scoped = new Set();
  const known = new Set();
  for (const { text } of rustFiles("server/src")) {
    const re = /pub async fn (\w+)\s*\(([\s\S]*?)\)\s*->/g;
    let m;
    while ((m = re.exec(text))) {
      const [, name, args] = m;
      known.add(name);
      if (/tenant::Tenant|Extension<Tenant>/.test(args)) scoped.add(name);
    }
  }
  return { scoped, known };
}

// ---------------------------------------------------------------------------
// 4. Grouping for presentation
// ---------------------------------------------------------------------------

const GROUPS = [
  ["Message plane", (p) => /^\/api\/v1\/(push|pop|ack|transaction|lease)/.test(p)],
  ["Queues and partitions", (p) => /^\/api\/v1\/(configure|resources)/.test(p)],
  ["Consumer groups", (p) => p.startsWith("/api/v1/consumer-groups")],
  ["Messages, DLQ and traces", (p) => /^\/api\/v1\/(messages|dlq|traces)/.test(p)],
  ["Status, metrics and analytics", (p) =>
    /^\/api\/v1\/(status|analytics|stats)/.test(p) ||
    ["/health", "/status", "/metrics", "/metrics/prometheus"].includes(p)],
  ["Streams", (p) => p.startsWith("/streams/")],
  // These eight are registered unconditionally, like every other row here: the
  // boot flags that used to gate them are gone. Without this entry they land in
  // "Ungrouped".
  ["Key/value state and timers", (p) => /^\/api\/v1\/(kv|timers)(\/|$)/.test(p)],
  // EPHEMERAL_QUEUES.md §3.1. Registered unconditionally like the eight above;
  // without this entry the family lands in "Ungrouped".
  ["Ephemeral queues", (p) => /^\/api\/v1\/ephemeral(\/|$)/.test(p)],
  ["Operator surfaces", (p) => p.startsWith("/api/v1/system")],
  ["Dashboard identity (broker-direct)", (p) => p.startsWith("/auth/")],
  ["Internal (broker-to-broker)", (p) => p.startsWith("/internal/")],
];

function groupOf(path) {
  for (const [name, test] of GROUPS) if (test(path)) return name;
  return "Ungrouped";
}

// ---------------------------------------------------------------------------

function main() {
  const check = isCheck();
  const mainText = repoRead(MAIN);
  const authText = repoRead(AUTH);

  assertFingerprint(
    "server/src/auth.rs :: route_access_level",
    fnBody(authText, "pub fn route_access_level"),
    ACCESS_FINGERPRINT,
  );

  const routes = parseRoutes(mainText);
  if (routes.length < 40) {
    throw new Error(`only parsed ${routes.length} routes out of ${MAIN} — the parser is broken`);
  }

  const { scoped, known } = tenantScopedHandlers();
  const missing = routes.filter((r) => !known.has(r.handler)).map((r) => r.handler);
  if (missing.length) {
    throw new Error(`handlers referenced by the router but not found in server/src: ${[...new Set(missing)].join(", ")}`);
  }

  for (const r of routes) {
    r.level = accessLevel(r.method, r.path);
    r.tenant = scoped.has(r.handler);
    r.group = groupOf(r.path);
  }

  const byGroup = new Map();
  for (const r of routes) {
    if (!byGroup.has(r.group)) byGroup.set(r.group, []);
    byGroup.get(r.group).push(r);
  }

  const order = [...GROUPS.map(([n]) => n), "Ungrouped"];
  const lines = [];
  lines.push(
    `Queen's broker registers **${routes.length} method + path pairs**. ` +
      `Every row below is read out of the router and the authorization table at build time.`,
    "",
  );

  for (const group of order) {
    const rows = byGroup.get(group);
    if (!rows || rows.length === 0) continue;
    lines.push(`### ${group}`, "");
    lines.push("| Method | Path | Access level | Tenant-scoped |");
    lines.push("| --- | --- | --- | --- |");
    rows.sort((a, b) => a.path.localeCompare(b.path) || a.method.localeCompare(b.method));
    for (const r of rows) {
      lines.push(
        `| \`${r.method}\` | \`${cell(r.path)}\` | ${r.level} | ${r.tenant ? "yes" : "no"} |`,
      );
    }
    lines.push("");
  }

  const unscoped = routes.filter((r) => !r.tenant && r.path.startsWith("/api/v1/"));
  lines.push(
    `${routes.filter((r) => r.tenant).length} of these handlers resolve a tenant from the request; ` +
      `${unscoped.length} \`/api/v1/*\` routes do not and are therefore cell-wide reads or ` +
      `operator surfaces.`,
  );

  const res = emitPartial({
    name: "broker-routes",
    title: "broker route table",
    description: "Every HTTP route the broker registers, with its access level and whether it is tenant-scoped.",
    sources: [`${MAIN} (Router::new chain)`, `${AUTH} (route_access_level)`, "server/src/handlers/*.rs (Tenant extension)"],
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
