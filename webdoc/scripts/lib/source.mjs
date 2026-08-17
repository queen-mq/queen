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
 * `startNeedle` may be an array of alternatives, tried in order. The router
 * builder is the reason: a binding that gains a `mut` when routes start being
 * registered conditionally is a refactor, not a contract change, and it should
 * not take three generators down with it.
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

/**
 * The router binding, in the forms the tree has used. Conditional route
 * registration needs `mut`; unconditional registration does not.
 */
export const ROUTER_BUILDER = ["let app = Router::new()", "let mut app = Router::new()"];

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
