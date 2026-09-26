/**
 * Shared helpers for the generators in webdoc/scripts/.
 *
 * Every generator reads Rust source out of the repo and emits an MDX partial
 * under src/content/partials/generated/. Nothing here writes to a page: the
 * pages `<Render file="generated/..." />` the partials, so prose and generated
 * fact tables stay separable.
 */

import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));

/** webdoc/ */
export const WEBDOC = resolve(HERE, "..", "..");
/** the repository root (webdoc/ lives directly under it) */
export const REPO = resolve(WEBDOC, "..");
/** where every generated partial lands */
export const OUT_DIR = join(WEBDOC, "src", "content", "partials", "generated");

export function repoRead(relPath) {
  return readFileSync(join(REPO, relPath), "utf8");
}

export function repoPath(relPath) {
  return join(REPO, relPath);
}

/** Every *.rs file under a repo-relative directory, recursively. */
export function rustFiles(relDir) {
  const out = [];
  const walk = (abs) => {
    for (const name of readdirSync(abs)) {
      const p = join(abs, name);
      const st = statSync(p);
      if (st.isDirectory()) walk(p);
      else if (name.endsWith(".rs")) out.push(p);
    }
  };
  walk(join(REPO, relDir));
  return out.map((p) => ({ path: relative(REPO, p), text: readFileSync(p, "utf8") }));
}

/**
 * Slice a Rust block starting at `startNeedle` and ending at the first line
 * that matches `endNeedle`. Deliberately line-based rather than a real parser:
 * the generators only need to see the shape of literal-heavy blocks, and a
 * brace-matching walk would be defeated by braces inside string literals.
 *
 * `startNeedle` may be an array of alternatives, tried in order: a binding that
 * gains a `mut` is a refactor, not a contract change, and it should not take a
 * generator down with it.
 */
export function sliceBlock(text, startNeedle, endNeedle) {
  const needles = Array.isArray(startNeedle) ? startNeedle : [startNeedle];
  let start = -1;
  for (const needle of needles) {
    start = text.indexOf(needle);
    if (start !== -1) break;
  }
  if (start === -1) throw new Error(`could not find block start: ${needles.join(" | ")}`);
  const end = text.indexOf(endNeedle, start);
  if (end === -1) throw new Error(`could not find block end: ${endNeedle}`);
  return text.slice(start, end + endNeedle.length);
}

// ---------------------------------------------------------------------------
// The broker's HTTP surface (Queen 2.0: one storage class, one router)
// ---------------------------------------------------------------------------

/** The raft router: `build_raft_router`'s `.route(` chain. */
export const RAFT_ROUTER = "server/src/handlers/raft.rs";
/** The generic adapter every other `/api` and `/streams` request falls through to. */
export const RAFT_ADAPTER = "server/src/rsm/facade/real/phase2.rs";
/** The adapter's path-pattern arms (`api_dynamic`). */
export const RAFT_ADAPTER_DYNAMIC = "server/src/rsm/facade/real/phase2/reads.rs";

/**
 * The `/api/v1/resources/queues/:queue...` arms at the bottom of `api_impl`,
 * which dispatch on `strip_prefix`/`strip_suffix` rather than on a literal
 * (method, path) pair, so no parse below sees them. Mirrored here behind a
 * fingerprint of that block: when the Rust changes, re-read it, update the four
 * rows, and paste the new fingerprint.
 */
const QUEUE_PREFIX_ARMS = [
  { method: "GET", path: "/api/v1/resources/queues/:queue/depth", handler: "api_queue_depth", passesCtx: true },
  { method: "GET", path: "/api/v1/resources/queues/:queue/sizes", handler: "api_queue_sizes", passesCtx: true },
  { method: "GET", path: "/api/v1/resources/queues/:queue", handler: "api_get_queue", passesCtx: true },
  { method: "DELETE", path: "/api/v1/resources/queues/:queue", handler: "api_delete_queue", passesCtx: true },
];
const QUEUE_PREFIX_FINGERPRINT = "a91b716fffe1966e";

/**
 * The Rust binding names of `api_dynamic`'s path segments, spelled the way the
 * published API has always spelled those parameters. Presentation only: the
 * segment position is what the broker matches on.
 */
const PARAM_NAMES = { pid: "partitionId", txn: "transactionId", part: "partition", name: "traceName" };

/** The text between a call's `(` at `openIdx` and its matching `)`. */
function callArgs(text, openIdx) {
  let depth = 0;
  for (let i = openIdx; i < text.length; i++) {
    if (text[i] === "(") depth++;
    else if (text[i] === ")") {
      depth--;
      if (depth === 0) return text.slice(openIdx + 1, i);
    }
  }
  return "";
}

/**
 * The first `self.<method>(...)` after `from`, with whether its arguments hand
 * over the request context (which carries the tenant).
 */
function adapterCall(text, from) {
  const re = /self\.(\w+)\(/g;
  re.lastIndex = from;
  const m = re.exec(text);
  if (!m) return null;
  const args = callArgs(text, m.index + m[0].length - 1);
  return { handler: m[1], passesCtx: /\bctx\b/.test(args) };
}

/**
 * Every method + path pair the broker serves, read out of the Rust:
 *
 *   via "router"   the `.route(` chain of `build_raft_router`
 *   via "adapter"  the `(method, path)` arms of `api_impl`, the path-pattern
 *                  arms of `api_dynamic`, and the mirrored queue-prefix arms
 *
 * `{ method, path, handler, via, passesCtx? }`, method upper-case, path in
 * axum's `:param` spelling. `passesCtx` (adapter only) says whether the arm
 * hands the request context, and with it the tenant, to its handler.
 */
export function brokerRoutes() {
  const routes = [];

  // 1. The router chain.
  const raft = repoRead(RAFT_ROUTER);
  const block = sliceBlock(raft, "pub(crate) fn build_raft_router", ".fallback(raft_fallback)");
  const re = /\.route\(\s*"([^"]+)"\s*,/g;
  let m;
  while ((m = re.exec(block))) {
    const path = m[1];
    // The method router runs from the comma to the next `.route(`; the verbs
    // are always inside that window.
    const rest = block.slice(re.lastIndex, re.lastIndex + 400);
    const stop = rest.indexOf(".route(");
    const window = stop === -1 ? rest : rest.slice(0, stop);
    for (const [, verb, handlerPath] of window.matchAll(/\b(get|post|put|patch|delete|head|options)\(\s*([\w:]+)/g)) {
      routes.push({ method: verb.toUpperCase(), path, handler: handlerPath.split("::").pop(), via: "router" });
    }
  }

  // 2. The adapter's literal arms, alternatives (`| (..)`) included.
  const adapter = repoRead(RAFT_ADAPTER);
  const impl = fnBody(adapter, "async fn api_impl");
  const arm = /\(\s*"(GET|POST|PUT|PATCH|DELETE)"\s*,\s*"(\/[^"]*)"\s*\)/g;
  while ((m = arm.exec(impl))) {
    const arrow = impl.indexOf("=>", arm.lastIndex);
    if (arrow === -1) continue;
    const call = adapterCall(impl, arrow);
    if (!call) throw new Error(`${RAFT_ADAPTER}: no handler call after the arm for ${m[1]} ${m[2]}`);
    routes.push({ method: m[1], path: m[2], handler: call.handler, via: "adapter", passesCtx: call.passesCtx });
  }

  // 3. The queue-prefix arms, mirrored.
  assertFingerprint(
    `${RAFT_ADAPTER} :: api_impl queue-prefix arms`,
    sliceBlock(impl, 'strip_prefix("/api/v1/resources/queues/")', "no_such_route"),
    QUEUE_PREFIX_FINGERPRINT,
  );
  for (const r of QUEUE_PREFIX_ARMS) routes.push({ ...r, via: "adapter" });

  // 4. The path-pattern arms: `["api", "v1", "messages", pid, txn] if req.method == "GET" =>`.
  const dynamicText = repoRead(RAFT_ADAPTER_DYNAMIC);
  const dyn = fnBody(dynamicText, "async fn api_dynamic");
  const pat = /\[((?:\s*(?:"[^"]*"|[a-z_]\w*)\s*,?)+)\]\s*if\s+req\.method\s*==\s*"(\w+)"\s*=>/g;
  while ((m = pat.exec(dyn))) {
    const segments = m[1]
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean)
      .map((x) => (x.startsWith('"') ? x.slice(1, -1) : `:${PARAM_NAMES[x] ?? x}`));
    const call = adapterCall(dyn, pat.lastIndex);
    if (!call) throw new Error(`${RAFT_ADAPTER_DYNAMIC}: no handler call after /${segments.join("/")}`);
    routes.push({ method: m[2], path: `/${segments.join("/")}`, handler: call.handler, via: "adapter", passesCtx: call.passesCtx });
  }

  return routes;
}

/**
 * Adapter handlers whose context parameter is named `_ctx`: they receive the
 * tenant and never read it, so the route they serve is cell-wide.
 */
export function adapterIgnoresCtx() {
  const ignores = new Set();
  const files = [RAFT_ADAPTER, ...rustFiles("server/src/rsm/facade/real/phase2").map((f) => f.path)];
  for (const f of files) {
    const text = repoRead(f);
    for (const [, name, args] of text.matchAll(/async fn (\w+)\s*\(([\s\S]*?)\)\s*->/g)) {
      if (/\b_ctx\s*:/.test(args)) ignores.add(name);
    }
  }
  return ignores;
}

/**
 * Extract a Rust function body by brace matching from the `{` that follows the
 * signature. Good enough for the pure, literal-driven classifier functions the
 * generators fingerprint (no unbalanced braces inside their string literals).
 */
export function fnBody(text, signatureNeedle) {
  const at = text.indexOf(signatureNeedle);
  if (at === -1) throw new Error(`could not find fn: ${signatureNeedle}`);
  const open = text.indexOf("{", at);
  let depth = 0;
  for (let i = open; i < text.length; i++) {
    if (text[i] === "{") depth++;
    else if (text[i] === "}") {
      depth--;
      if (depth === 0) return text.slice(open, i + 1);
    }
  }
  throw new Error(`unbalanced braces after: ${signatureNeedle}`);
}

export function fingerprint(s) {
  // Whitespace- and comment-insensitive: a reflow or a clarified comment must
  // not trip the drift guard, but any change to a rule must.
  const normalized = s
    .replace(/\/\/[^\n]*/g, "")
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/\s+/g, " ")
    .trim();
  return createHash("sha256").update(normalized).digest("hex").slice(0, 16);
}

/**
 * Guard for logic this repo mirrors in JavaScript. When the Rust rules change,
 * the mirror is stale and every table built from it is a lie — so fail loudly
 * with the new fingerprint to paste back in.
 */
export function assertFingerprint(label, body, expected) {
  const actual = fingerprint(body);
  if (actual !== expected) {
    throw new Error(
      [
        ``,
        `DRIFT: ${label} changed in the Rust source.`,
        ``,
        `  expected fingerprint: ${expected}`,
        `  actual fingerprint:   ${actual}`,
        ``,
        `This generator mirrors that logic in JavaScript. Re-read the Rust,`,
        `update the mirror in this script, then set the expected fingerprint`,
        `to the actual value above.`,
        ``,
      ].join("\n"),
    );
  }
}

const BANNER = (sources) =>
  [
    `{/* GENERATED FILE. Do not edit by hand.`,
    `    Regenerate with: pnpm --dir webdoc gen`,
    `    Source of truth:`,
    ...sources.map((s) => `      - ${s}`),
    `*/}`,
  ].join("\n");

/**
 * Write a generated partial. `check` mode compares instead of writing, so CI
 * can fail when a partial is behind its source without touching the tree.
 */
export function emitPartial({ name, title, sources, body, check, description }) {
  mkdirSync(OUT_DIR, { recursive: true });
  const file = join(OUT_DIR, `${name}.mdx`);
  const content = [
    // `description` is here for the prose linter, which checks every .mdx
    // including partials. It never reaches a page: the including page owns the
    // metadata a reader or an agent sees.
    "---",
    `params: []`,
    `description: "${(description ?? `Generated ${title}.`).replace(/"/g, "'")}"`,
    "---",
    "",
    BANNER(sources),
    "",
    body.trimEnd(),
    "",
  ].join("\n");

  if (check) {
    let current = "";
    try {
      current = readFileSync(file, "utf8");
    } catch {
      /* missing counts as drift */
    }
    if (current !== content) {
      return { file, drifted: true, title };
    }
    return { file, drifted: false, title };
  }

  writeFileSync(file, content, "utf8");
  return { file, written: true, title };
}

/** Escape a value for a markdown table cell. */
export function cell(v) {
  if (v === undefined || v === null || v === "") return "";
  return String(v).replace(/\|/g, "\\|").replace(/\n/g, " ");
}

export const isCheck = () => process.argv.includes("--check");
