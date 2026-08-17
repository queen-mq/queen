/**
 * `/llms-brief.txt` is the one document the site tells agents to read first.
 * This guards the two ways that promise rots.
 *
 * **Size.** The brief is useful only because it always arrives whole. The site
 * already has a surface that failed this test: `llms-full.txt` grew to about
 * 1.5 MB, past the input ceiling of every fetch pipeline, which truncate it
 * without reporting that they did. Nothing stopped that happening because
 * nothing was watching the number. A budget that fails the build is what keeps
 * the brief from becoming a second corpus one page at a time.
 *
 * **Reachability.** A summary nothing points at is worse than no summary: the
 * cost was paid and the benefit was not. So this also asserts that all three
 * agent-facing channels still lead with it. Each is one line in one file, and
 * each is exactly the kind of line a refactor drops silently.
 *
 * Runs after `pnpm build`, against `dist/`, like `check-markdown.mjs`.
 */

import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { WEBDOC } from "./lib/source.mjs";

/**
 * The budget lives here rather than beside the builder: the enforcer owns the
 * number, so there is one place to argue with. 30 KB is roughly 7,500 tokens,
 * which every pipeline in use today accepts whole with room to spare.
 */
const BUDGET_BYTES = 30 * 1024;

/**
 * A floor as well as a ceiling. If the collation silently stops finding
 * `digest` frontmatter (a renamed field, a changed helper), the brief does not
 * break, it just quietly becomes a header with no content. That failure is
 * invisible without a floor.
 */
const FLOOR_BYTES = 2 * 1024;

const DIST = join(WEBDOC, "dist");
const BRIEF = join(DIST, "llms-brief.txt");
const DOCS = join(WEBDOC, "src", "content", "docs");

const problems = [];
const fail = (where, what) => problems.push({ where, what });

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

const read = (p) => (existsSync(p) ? readFileSync(p, "utf8") : null);

/**
 * The `digest` frontmatter value of a page, or "" when it has none.
 *
 * Line-based rather than one regex. The regex this replaces looked right and
 * silently returned the block-scalar indicator (`>-`) as the value for every
 * page, because the trailing `$` could not be satisfied at the position the
 * multi-line branch ended on, so the pattern fell through to the single-line
 * branch. Every comparison downstream then passed or failed on two characters.
 */
function digestOf(source) {
  if (!source.startsWith("---\n")) return "";
  const close = source.indexOf("\n---", 3);
  if (close === -1) return "";
  const lines = source.slice(4, close + 1).split("\n");

  const at = lines.findIndex((l) => /^digest:/.test(l));
  if (at === -1) return "";

  const inline = lines[at].slice("digest:".length).trim();
  // Not a block scalar: the value is on the key's own line.
  if (inline && !/^[|>][-+]?$/.test(inline)) {
    return inline.replace(/^["']|["']$/g, "").trim();
  }

  const body = [];
  for (const line of lines.slice(at + 1)) {
    // A block scalar runs until a line that is not more indented than its key.
    if (line.trim() !== "" && !/^\s/.test(line)) break;
    body.push(line.trim());
  }
  // Folded (`>`) joins with spaces; literal (`|`) keeps newlines. Either way the
  // probe below compares on collapsed whitespace, so joining with spaces is
  // enough for both.
  return body.join(" ").trim();
}

const brief = read(BRIEF);
if (!brief) {
  fail("dist/llms-brief.txt", "missing — run `pnpm build` first");
} else {
  const bytes = new TextEncoder().encode(brief).length;

  if (bytes > BUDGET_BYTES) {
    fail(
      "dist/llms-brief.txt",
      `${(bytes / 1024).toFixed(1)} KB is over the ${BUDGET_BYTES / 1024} KB budget. ` +
        `This document earns its place by always arriving whole. Shorten or drop a ` +
        `\`digest\` in src/content/docs/, or argue the budget up in this script ` +
        `deliberately: raising it silently is how llms-full.txt got to 1.5 MB.`,
    );
  }
  if (bytes < FLOOR_BYTES) {
    fail(
      "dist/llms-brief.txt",
      `only ${bytes} B. The collation is probably finding no \`digest\` frontmatter at ` +
        `all — check src/lib/llms-brief.ts against src/content.config.ts.`,
    );
  }

  // Every authored digest has to survive collation. A page whose digest is
  // dropped is a fact the site believes it publishes to agents and does not.
  const authored = [];
  for (const file of walk(DOCS, [".mdx", ".md"])) {
    const raw = digestOf(readFileSync(file, "utf8"));
    if (raw) authored.push({ file: relative(WEBDOC, file), raw });
  }

  if (!authored.length) {
    fail(
      "src/content/docs",
      "no page declares a `digest`, so the brief has nothing to collate. Add one to " +
        "the pages that state a fact the product cannot be understood without.",
    );
  }

  for (const { file, raw } of authored) {
    // Compare on a distinctive fragment rather than the whole string: YAML
    // folding and wrapping legitimately change the whitespace.
    const probe = raw.split(/\s+/).slice(0, 8).join(" ");
    const flat = brief.replace(/\s+/g, " ");
    if (probe && !flat.includes(probe)) {
      fail(
        file,
        `its \`digest\` did not reach dist/llms-brief.txt: "${probe.slice(0, 60)}…". ` +
          `The page may be outside the collated collection, or unindexed.`,
      );
    }
  }
}

/**
 * The three channels, asserted where they are actually emitted. These are the
 * lines that make the brief findable at all.
 */
const POINTER = "/llms-brief.txt";

const index = read(join(DIST, "llms.txt"));
if (!index) {
  fail("dist/llms.txt", "missing — run `pnpm build` first");
} else if (!index.includes(POINTER)) {
  fail("dist/llms.txt", `does not link ${POINTER}. See src/pages/llms.txt.ts.`);
} else if (index.indexOf(POINTER) > index.indexOf("/llms-full.txt")) {
  fail(
    "dist/llms.txt",
    `links ${POINTER} below /llms-full.txt. Order is the point: an agent reads the ` +
      `first pointer, and the corpus is the one it should almost never take.`,
  );
}

const someMarkdown = join(DIST, "start", "quickstart", "index.md");
const md = read(someMarkdown);
if (!md) {
  fail(relative(WEBDOC, someMarkdown), "missing — run `pnpm build` first");
} else if (!md.includes(POINTER)) {
  fail(
    relative(WEBDOC, someMarkdown),
    `the injected header does not point at ${POINTER}. See src/pages/[...slug]/index.md.ts.`,
  );
}

const someHtml = join(DIST, "start", "quickstart", "index.html");
const html = read(someHtml);
if (!html) {
  fail(relative(WEBDOC, someHtml), "missing — run `pnpm build` first");
} else {
  if (!html.includes("data-ai-agent-directive")) {
    fail(
      relative(WEBDOC, someHtml),
      "the AgentDirective is gone from the page body. It is the only agent pointer that " +
        "survives HTML-to-Markdown conversion, so it is the one that reaches an agent " +
        "arriving from a search result. See src/layouts/BaseLayout.astro.",
    );
  } else if (!html.includes(POINTER)) {
    fail(
      relative(WEBDOC, someHtml),
      `the AgentDirective does not mention ${POINTER}. See src/components/AgentDirective.astro.`,
    );
  }
}

if (problems.length) {
  for (const { where, what } of problems) console.error(`${where}  ${what}`);
  console.error(
    `\n${problems.length} problem(s) with the agent summary. /llms-brief.txt is what the ` +
      `site tells agents to read first: it has to exist, fit in one fetch, and be pointed at.`,
  );
  process.exit(1);
}

const kb = (new TextEncoder().encode(brief).length / 1024).toFixed(1);
console.log(
  `ok  agent summary (dist/llms-brief.txt ${kb} KB of ${BUDGET_BYTES / 1024} KB budget, ` +
    `pointed at from llms.txt, the .md alternates and the HTML)`,
);
