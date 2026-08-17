/**
 * `sourceOfTruth` is a debt list. This is the collector.
 *
 * Most pages on this site describe a running system, and the frontmatter of
 * each one names the repo-relative files that govern it. That field was never
 * enforced and never rendered, so it recorded an intention and nothing else:
 * `server/src/config.rs` could be rewritten and the page quoting its constants
 * kept publishing the old numbers, indefinitely and silently. An audit found
 * eleven factual errors of exactly that shape, including published defaults
 * that no longer existed in the code. Every one of them was already declared,
 * by the stale page itself, as depending on a file that had since moved on.
 *
 * The rule is one comparison, in git:
 *
 *   for every page P with `sourceOfTruth: [S…]`,
 *   the last commit touching P must not be older than the last commit
 *   touching any S.
 *
 * A page younger than its sources is not proof that it was checked, but a page
 * older than its sources is proof that it was not. That asymmetry is the whole
 * value: this cannot certify a page, it can only refuse to let one rot in
 * silence.
 *
 * **The escape hatch.** A frontmatter opt-out is not available (the collection
 * schema in src/content.config.ts is strict, so an undeclared key fails
 * `pnpm build` rather than disabling anything), so the hatch is the allowlist
 * beside this script. It exists so the gate can be adopted on the debt that
 * exists today instead of blocking the first build that runs it: an allowed
 * pair is printed as a reminder on success and never fails. Entries are meant
 * to be deleted, one page at a time.
 *
 * Reads git and src/, not `dist/`, so unlike check-markdown.mjs and
 * check-brief.mjs it does not need a build first.
 */

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { REPO, WEBDOC } from "./lib/source.mjs";

const DOCS = join(WEBDOC, "src", "content", "docs");
const ALLOWLIST = join(WEBDOC, "scripts", "sourceoftruth-allow.txt");

const problems = [];
const fail = (where, what) => problems.push({ where, what });

// ---------------------------------------------------------------------------
// git
// ---------------------------------------------------------------------------

function git(args) {
  return execFileSync("git", ["-C", REPO, ...args], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    maxBuffer: 32 * 1024 * 1024,
  });
}

function gitAvailable() {
  try {
    git(["rev-parse", "--is-inside-work-tree"]);
    git(["rev-parse", "HEAD"]);
    return true;
  } catch {
    return false;
  }
}

/**
 * Unix time of the last commit touching a repo-relative path, or null when
 * git has no commit for it (a file added in this working tree, or one that
 * only ever existed under a different name).
 */
const lastCommitCache = new Map();
function lastCommitTime(repoRelPath) {
  if (lastCommitCache.has(repoRelPath)) return lastCommitCache.get(repoRelPath);
  let ts = null;
  try {
    const out = git(["log", "-1", "--format=%ct", "--", repoRelPath]).trim();
    if (out) ts = Number(out);
  } catch {
    ts = null;
  }
  lastCommitCache.set(repoRelPath, ts);
  return ts;
}

/** Short sha of the last commit touching a path, for the failure message. */
function lastCommitSha(repoRelPath) {
  try {
    return git(["log", "-1", "--format=%h", "--", repoRelPath]).trim() || "?";
  } catch {
    return "?";
  }
}

/**
 * Paths with uncommitted changes. A page in this set is being edited right
 * now, so its git date is the date of the version being replaced and comparing
 * against it would flag the edit that fixes the drift. Those pages are treated
 * as current.
 */
function dirtyPaths() {
  const set = new Set();
  let out = "";
  try {
    out = git(["status", "--porcelain", "-z"]);
  } catch {
    return set;
  }
  // NUL-separated so paths with spaces survive; a rename record carries the
  // old path as a second field, and both sides count as touched.
  const fields = out.split("\0").filter(Boolean);
  for (let i = 0; i < fields.length; i++) {
    const record = fields[i];
    const status = record.slice(0, 2);
    set.add(record.slice(3));
    if (status[0] === "R" || status[0] === "C") {
      i += 1;
      if (fields[i]) set.add(fields[i]);
    }
  }
  return set;
}

// ---------------------------------------------------------------------------
// Frontmatter
// ---------------------------------------------------------------------------

function walk(dir, exts, out = []) {
  let entries;
  try {
    entries = readdirSync(dir);
  } catch {
    return out;
  }
  for (const name of entries) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) walk(p, exts, out);
    else if (exts.some((e) => name.endsWith(e))) out.push(p);
  }
  return out;
}

/**
 * The string items of a YAML sequence under `key:` in the frontmatter block.
 * Line-based for the same reason `digestOf` in check-brief.mjs is: a regex
 * that has to span an indented block is the kind that silently matches its own
 * indicator line instead of the value.
 */
function listOf(source, key) {
  if (!source.startsWith("---\n")) return [];
  const close = source.indexOf("\n---", 3);
  if (close === -1) return [];
  const lines = source.slice(4, close + 1).split("\n");

  const at = lines.findIndex((l) => l.startsWith(`${key}:`));
  if (at === -1) return [];
  if (lines[at].slice(key.length + 1).trim()) return []; // inline value, not a sequence

  const items = [];
  for (const line of lines.slice(at + 1)) {
    if (line.trim() === "") continue;
    if (!/^\s/.test(line)) break; // dedented: the next key
    const item = line.trim();
    if (!item.startsWith("- ")) break; // a nested mapping, not this sequence
    items.push(item.slice(2).trim().replace(/^["']|["']$/g, ""));
  }
  return items;
}

// ---------------------------------------------------------------------------
// Allowlist
// ---------------------------------------------------------------------------

/**
 * One entry per line. Either a page path on its own (every source it names is
 * allowed to be ahead of it) or `page :: source` for a single pair. Paths are
 * written exactly as they appear in a failure message: the page relative to
 * webdoc/, the source relative to the repository root. `#` starts a comment.
 */
function readAllowlist() {
  const pages = new Set();
  const pairs = new Set();
  const raw = existsSync(ALLOWLIST) ? readFileSync(ALLOWLIST, "utf8") : "";
  for (const line of raw.split("\n")) {
    const text = line.replace(/#.*$/, "").trim();
    if (!text) continue;
    const [page, source] = text.split("::").map((s) => s.trim());
    if (source) pairs.add(`${page}::${source}`);
    else pages.add(page);
  }
  return { pages, pairs, entries: pages.size + pairs.size };
}

// ---------------------------------------------------------------------------

if (!gitAvailable()) {
  // Not a skip that hides a failure: with no history there is no "changed
  // since" to compute, and the field this checks is about history alone.
  console.log(
    "ok  sourceOfTruth freshness skipped: no git history at " +
      `${REPO}. This check compares commit dates and has nothing to compare.`,
  );
  process.exit(0);
}

const allow = readAllowlist();
const dirty = dirtyPaths();
const usedAllowances = new Set();

let checkedPages = 0;
let checkedSources = 0;
let allowed = 0;
let untracked = 0;

for (const file of walk(DOCS, [".mdx", ".md"])) {
  const source = readFileSync(file, "utf8");
  const sources = listOf(source, "sourceOfTruth");
  if (!sources.length) continue;

  const pageRepoPath = relative(REPO, file);
  const pageLabel = relative(WEBDOC, file);
  checkedPages += 1;

  // An edited or newly added page is current by definition: the author is
  // holding it open, and its git date is the date of the version being
  // replaced.
  const pageTime = lastCommitTime(pageRepoPath);
  if (dirty.has(pageRepoPath) || pageTime === null) continue;

  for (const src of sources) {
    checkedSources += 1;

    // What is wrong with the pair, or null when nothing is. Computed before
    // the allowlist is consulted so an allowance is only ever spent on a real
    // finding: an entry whose page has since been fixed then reports itself as
    // deletable instead of sitting there forever.
    let what = null;

    if (!existsSync(join(REPO, src))) {
      what =
        `\`sourceOfTruth\` names ${src}, which does not exist in the repository. ` +
        `Point the page at the file that governs it now, or drop the entry.`;
    } else {
      const srcTime = lastCommitTime(src);
      if (srcTime === null) {
        // Tracked by nothing yet: new code arriving with the page it documents.
        untracked += 1;
      } else if (srcTime > pageTime) {
        const days = Math.round((srcTime - pageTime) / 86400);
        what =
          `${src} changed ${days === 0 ? "later the same day" : `${days} day(s) later`} ` +
          `(${lastCommitSha(src)}, ${new Date(srcTime * 1000).toISOString().slice(0, 10)}) ` +
          `than this page last did (${new Date(pageTime * 1000).toISOString().slice(0, 10)}). ` +
          `Re-read that file against the page. If the page is still correct, say so by ` +
          `touching it, or add "${pageLabel} :: ${src}" to ` +
          `${relative(WEBDOC, ALLOWLIST)} with the reason.`;
      }
    }

    if (!what) continue;

    const pairKey = `${pageLabel}::${src}`;
    if (allow.pages.has(pageLabel)) {
      usedAllowances.add(pageLabel);
      allowed += 1;
      continue;
    }
    if (allow.pairs.has(pairKey)) {
      usedAllowances.add(pairKey);
      allowed += 1;
      continue;
    }

    fail(pageLabel, what);
  }
}

const unusedAllowances = [...allow.pages, ...allow.pairs].filter(
  (entry) => !usedAllowances.has(entry),
);

if (problems.length) {
  for (const { where, what } of problems.slice(0, 40)) console.error(`${where}  ${what}`);
  if (problems.length > 40) console.error(`\n… and ${problems.length - 40} more.`);
  const stalePages = new Set(problems.map((p) => p.where)).size;
  console.error(
    `\n${problems.length} stale declaration(s) across ${stalePages} page(s), each one behind ` +
      `the code it names as its source of truth. Prose that describes an older revision is ` +
      `the failure this site has already ` +
      `shipped: published constants that no longer existed, on pages that named the file ` +
      `holding the real ones. Fix the page, or record the exemption in ` +
      `${relative(WEBDOC, ALLOWLIST)}.`,
  );
  process.exit(1);
}

console.log(
  `ok  sourceOfTruth freshness (${checkedPages} pages, ${checkedSources} source files, ` +
    `${allowed} allowed, ${untracked} not yet in git)`,
);
if (unusedAllowances.length) {
  console.log(
    `    ${unusedAllowances.length} allowlist entr(y/ies) matched nothing and can be deleted: ` +
      unusedAllowances.slice(0, 5).join(", ") +
      (unusedAllowances.length > 5 ? ", …" : ""),
  );
}
