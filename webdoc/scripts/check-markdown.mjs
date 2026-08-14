/**
 * The markdown alternates must carry what the HTML pages carry.
 *
 * `dist/**\/index.md` and `dist/llms-full.txt` are what agents and crawlers
 * read, and they are produced by a different path than the HTML: a regex
 * downleveler over the raw MDX, not Astro's renderer. That path used to drop
 * every `<Render file="…" />`, so 32 pages published with their partials
 * missing: the three full-examples pages lost all of their code, and the
 * configuration reference lost its table. Nothing failed. The pages built, the
 * links resolved, and only the markdown was hollow.
 *
 * This check compares the two surfaces where it is cheap to do so, over the
 * built `dist/`:
 *
 *   1. Every fenced block a page authors appears verbatim in its `.md` twin.
 *      Verbatim matters: the downleveler used to strip four leading spaces from
 *      every line of a fenced block, which breaks the indentation of any real
 *      program.
 *   2. Every partial a page includes appears in its `.md` twin, matched on the
 *      partial's own fences (or, for a prose or table partial, on its longest
 *      lines).
 *   3. The two pages the regression was worst on are asserted directly: each
 *      full-examples page carries four fenced programs, one per language, and
 *      the configuration reference carries its table.
 *   4. **No capitalised self-closing component is silently dropped.** This is
 *      the general form of the bug above. The downleveler's last rule deletes
 *      any `<Foo ... />` it was not taught, leaving nothing behind and failing
 *      nothing: that is how `<Render />` cost 32 pages their partials, and how
 *      `<Chart />` cost the four benchmark pages their figure descriptions.
 *      Rather than assert the two known ones, this enumerates every
 *      capitalised self-closing tag the content actually uses and fails on any
 *      that is neither rendered by `src/lib/markdown-partials.ts` nor listed in
 *      `ALLOWED_TO_VANISH` with a reason. Adding a component to the content is
 *      then a decision, not an accident. The built `.md` files and the corpus
 *      are also scanned for a surviving tag, which would mean the opposite
 *      failure: a component leaking into the markdown unrendered. That output
 *      scan covers the paired form too (`<Accordion>…</Accordion>`), matched
 *      against the MDX globals registry in `src/components.ts`.
 *   5. **The landing page reaches the corpus.** `/` is a hand-written
 *      `index.astro`, not a `docs` entry, so nothing above sees it;
 *      `src/pages/index.md.ts` transcribes its copy by hand. This asserts the
 *      transcription still matches, which is what keeps the duplication safe.
 *
 * Run after `pnpm build` (`pnpm verify` does).
 */

import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { WEBDOC } from "./lib/source.mjs";

const DIST = join(WEBDOC, "dist");
const DOCS = join(WEBDOC, "src", "content", "docs");
const PARTIALS = join(WEBDOC, "src", "content", "partials");
const CORPUS = join(DIST, "llms-full.txt");
const HOME_ASTRO = join(WEBDOC, "src", "pages", "index.astro");
const HOME_MARKDOWN = join(DIST, "index.md");

/** A fenced block with its opening indentation captured. */
const FENCE = /^([ \t]*)(`{3,}|~{3,})[^\n]*\n[\s\S]*?^[ \t]*\2[ \t]*$/gm;
/** A `<Render />` on a line of its own — the form pages use. */
const RENDER_TAG = /^[ \t]*<Render\b[^>]*?\bfile\s*=\s*"([^"]+)"[^>]*?(?:\/>|>)[ \t]*$/gm;
/** Any capitalised self-closing component: `<Foo ... />`, attributes over any number of lines. */
const SELF_CLOSING = /<([A-Z][A-Za-z0-9]*)\b[^>]*\/>/g;
/** An inline code span, stripped before looking for tags in prose. */
const CODE_SPAN = /`[^`\n]+`/g;

/**
 * The MDX globals, read from the registry rather than restated here.
 *
 * Used to catch the *paired* form of the leak the self-closing scan above
 * covers: `<Accordion>…</Accordion>` reached the markdown as literal tags for
 * as long as the exporter had no rule for it, and nothing failed, because a
 * paired tag is not `<Foo />`. Matching on the registry instead of on "any
 * capitalised tag" is what keeps this quiet: the docs are full of prose
 * placeholders like `<LEVEL>` and `<FILE>`, and Rust generics like
 * `<Vec<String>>`, none of which are components.
 */
const REGISTERED_COMPONENTS = (() => {
  const src = readFileSync(join(WEBDOC, "src", "components.ts"), "utf8");
  const block = /export const components = \{([\s\S]*?)\}/.exec(src)?.[1] ?? "";
  return new Set([...block.matchAll(/^\s*([A-Z][A-Za-z0-9]*),/gm)].map((m) => m[1]));
})();

/**
 * Capitalised self-closing components that reach the markdown with their
 * content intact, and where that is implemented.
 *
 * `Render` and `Chart` are ours, in `src/lib/markdown-partials.ts`.
 * `PackageManagers` and `LinkCard` are the Nimbus downleveler's own rules
 * (`applyDefaultComponentTransforms`). Teaching the exporter a new component
 * means adding it here too.
 */
const HANDLED = new Map([
  ["Render", "src/lib/markdown-partials.ts (expanded to the partial's markdown)"],
  ["Chart", "src/lib/markdown-partials.ts (componentMap: alt text, caption, source)"],
  ["Screenshot", "src/lib/markdown-partials.ts (componentMap: alt text, caption)"],
  ["PackageManagers", "nimbus-docs downleveler (rendered as a sh block)"],
  ["LinkCard", "nimbus-docs downleveler (rendered as a link list item)"],
]);

/**
 * Components it is fine to drop from the markdown, each with the reason.
 *
 * Empty on purpose. A component belongs here only when its markup carries no
 * text a reader would miss; anything with a label, an `alt` or a body needs a
 * renderer in `markdown-partials.ts` instead. Adding an entry is a decision to
 * publish a page that says less than the HTML does, so write down why.
 */
const ALLOWED_TO_VANISH = new Map([
  // ["Foo", "decorative only, carries no text"],
]);

/** Explicit assertions for the pages the regression gutted completely. */
const MUST_CARRY = [
  {
    page: "full-examples/produce-consume",
    fences: 4,
    langs: ["js", "python", "go", "rust"],
  },
  { page: "full-examples/ordering-and-dedup", fences: 4, langs: ["js", "python", "go", "rust"] },
  { page: "full-examples/pipeline", fences: 4, langs: ["js", "python", "go", "rust"] },
  { page: "reference/config", tableRows: 100 },
];

const problems = [];
const fail = (where, what) => problems.push({ where, what });

function walk(dir, ext, out = []) {
  let entries;
  try {
    entries = readdirSync(dir);
  } catch {
    return out;
  }
  for (const name of entries) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) walk(p, ext, out);
    else if (name.endsWith(ext)) out.push(p);
  }
  return out;
}

/** Strip a block's own opening indentation, the way the exporter does. */
function dedent(block, indent) {
  if (!indent) return block;
  return block
    .split("\n")
    .map((line) => (line.startsWith(indent) ? line.slice(indent.length) : line.trimStart()))
    .join("\n");
}

function fencesOf(text) {
  return [...text.matchAll(FENCE)].map((m) => dedent(m[0], m[1]));
}

/** Text outside fenced blocks — where a real `<Render />` lives. */
function outsideFences(text) {
  return text.replace(FENCE, "");
}

/** `<Render file="x" />` resolves to `x.mdx` or, for a directory, `x/index.mdx`. */
function partialFile(id) {
  for (const candidate of [`${id}.mdx`, `${id}.md`, join(id, "index.mdx"), join(id, "index.md")]) {
    const p = join(PARTIALS, candidate);
    if (existsSync(p)) return p;
  }
  return null;
}

/** `src/content/docs/a/b.mdx` → `dist/a/b/index.md`, index files folded. */
function distMarkdownFor(source) {
  const id = relative(DOCS, source).replace(/\.mdx$/, "");
  const slug = id === "index" ? "" : id.replace(/\/index$/, "");
  return join(DIST, slug, "index.md");
}

/**
 * Lines distinctive enough to prove a partial was inlined, for partials that
 * carry no code fence (the generated tables, mostly). Longest lines first, so
 * the probe is a table row rather than a heading two pages share.
 */
function proseProbes(body, count = 2) {
  return body
    .replace(FENCE, "")
    .replace(/^---\n[\s\S]*?\n---\n?/, "")
    .replace(/\{\s*\/\*[\s\S]*?\*\/\s*\}/g, "")
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length >= 40 && !line.startsWith("#") && !line.startsWith("<"))
    .sort((a, b) => b.length - a.length)
    .slice(0, count);
}

const corpus = existsSync(CORPUS) ? readFileSync(CORPUS, "utf8") : null;
if (!corpus) fail("dist/llms-full.txt", "missing — run `pnpm build` first");

const pages = walk(DOCS, ".mdx");
if (pages.length === 0) fail("src/content/docs", "no pages found");

let checkedFences = 0;
let checkedPartials = 0;

for (const source of pages) {
  const dist = distMarkdownFor(source);
  const where = relative(WEBDOC, dist);
  if (!existsSync(dist)) {
    fail(where, `no markdown alternate for ${relative(WEBDOC, source)}`);
    continue;
  }
  const body = readFileSync(source, "utf8");
  const markdown = readFileSync(dist, "utf8");

  // 1. The page's own fenced blocks, byte for byte.
  for (const fence of fencesOf(body)) {
    checkedFences++;
    if (!markdown.includes(fence)) {
      fail(where, `a fenced block is missing or altered: ${fence.split("\n")[0]}`);
    }
  }

  // 2. Every included partial.
  for (const [, id] of outsideFences(body).matchAll(RENDER_TAG)) {
    const file = partialFile(id);
    if (!file) {
      fail(relative(WEBDOC, source), `<Render file="${id}" /> resolves to no partial`);
      continue;
    }
    checkedPartials++;
    const partial = readFileSync(file, "utf8");
    const probes = fencesOf(partial);
    const kind = probes.length > 0 ? "code" : "text";
    if (probes.length === 0) probes.push(...proseProbes(partial));
    if (probes.length === 0) continue; // empty partial: nothing to assert
    for (const probe of probes) {
      const label = probe.split("\n")[0].slice(0, 60);
      if (!markdown.includes(probe)) {
        fail(where, `partial "${id}" not inlined (${kind} missing: ${label})`);
      }
      if (corpus && !corpus.includes(probe)) {
        fail("dist/llms-full.txt", `partial "${id}" not inlined on ${where} (${label})`);
      }
    }
  }
}

// 3. The pages the regression gutted, asserted by shape rather than by source.
for (const { page, fences, langs, tableRows } of MUST_CARRY) {
  const dist = join(DIST, page, "index.md");
  const where = relative(WEBDOC, dist);
  if (!existsSync(dist)) {
    fail(where, "missing");
    continue;
  }
  const markdown = readFileSync(dist, "utf8");
  if (fences !== undefined) {
    const found = fencesOf(markdown).length;
    if (found < fences) fail(where, `${found} fenced block(s), expected at least ${fences}`);
    for (const lang of langs ?? []) {
      if (!new RegExp(`^\`\`\`${lang}\\b`, "m").test(markdown)) {
        fail(where, `no \`\`\`${lang} block — the language tag was lost`);
      }
    }
  }
  if (tableRows !== undefined) {
    const found = markdown.split("\n").filter((line) => line.startsWith("|")).length;
    if (found < tableRows) fail(where, `${found} table row(s), expected at least ${tableRows}`);
  }
}

// 4. Every capitalised self-closing component, enumerated from the content.
//
// Two directions. Source side: a component the exporter was never taught is a
// page that will publish hollow, so name it before it ships. Output side: a tag
// that survived into the markdown is the same bug seen from the other end, a
// component leaking as literal `<Foo />` into what an agent reads.

/** Prose only: fenced blocks and inline code spans are examples, not markup. */
function prose(text) {
  return text.replace(FENCE, "").replace(CODE_SPAN, "");
}

/** Component name → the source files that use it. */
const usedComponents = new Map();
for (const dir of [DOCS, PARTIALS]) {
  for (const file of [...walk(dir, ".mdx"), ...walk(dir, ".md")]) {
    const body = prose(readFileSync(file, "utf8"));
    for (const [, name] of body.matchAll(SELF_CLOSING)) {
      if (!usedComponents.has(name)) usedComponents.set(name, []);
      usedComponents.get(name).push(relative(WEBDOC, file));
    }
  }
}

for (const [name, files] of [...usedComponents].sort()) {
  if (HANDLED.has(name) || ALLOWED_TO_VANISH.has(name)) continue;
  const sample = files.slice(0, 3).join(", ");
  const more = files.length > 3 ? ` (and ${files.length - 3} more)` : "";
  fail(
    "src/content",
    `<${name} /> is used in ${files.length} place(s) but nothing renders it into markdown, ` +
      `so the downleveler deletes it and the .md pages publish without it. ` +
      `Give it a rule in src/lib/markdown-partials.ts, or add it to ALLOWED_TO_VANISH ` +
      `in this script with the reason it carries nothing a reader needs. Used in: ${sample}${more}`,
  );
}

/** A registered component in its paired form: `<Foo>` or `</Foo>`. */
const PAIRED_TAG = /<\/?([A-Z][A-Za-z0-9]*)(?:\s[^>]*)?>/g;

/** A component that reached the markdown as literal markup rather than text. */
function assertNoLiveTags(text, where) {
  const text_ = prose(text);
  for (const [tag, name] of text_.matchAll(SELF_CLOSING)) {
    fail(where, `<${name} /> survived into the markdown unrendered: ${tag.split("\n")[0].slice(0, 70)}`);
  }
  const leaked = new Set();
  for (const [, name] of text_.matchAll(PAIRED_TAG)) {
    if (REGISTERED_COMPONENTS.has(name)) leaked.add(name);
  }
  for (const name of [...leaked].sort()) {
    fail(
      where,
      `<${name}> survived into the markdown as a literal tag. It is a registered MDX ` +
        `component the downleveler has no rule for, so the page publishes its markup ` +
        `instead of its text. Give it a renderer in src/lib/markdown-partials.ts.`,
    );
  }
}

for (const file of walk(DIST, ".md")) {
  assertNoLiveTags(readFileSync(file, "utf8"), relative(WEBDOC, file));
}
if (corpus) assertNoLiveTags(corpus, "dist/llms-full.txt");

// The text `<Chart />` exists to carry: its `alt` is the figure's only
// description for a reader who cannot see the two SVGs, which is every reader
// of the markdown.
const CHART_TAG = /<Chart\b([^>]*)\/>/g;
let checkedCharts = 0;
for (const source of pages) {
  const body = prose(readFileSync(source, "utf8"));
  const dist = distMarkdownFor(source);
  if (!existsSync(dist)) continue;
  const markdown = readFileSync(dist, "utf8");
  const where = relative(WEBDOC, dist);
  for (const [, rawAttrs] of body.matchAll(CHART_TAG)) {
    checkedCharts++;
    const alt = /\balt\s*=\s*"([^"]*)"/.exec(rawAttrs)?.[1];
    if (!alt) {
      fail(relative(WEBDOC, source), "<Chart /> has no literal alt=\"…\" to carry into markdown");
      continue;
    }
    if (!markdown.includes(alt)) fail(where, `<Chart /> alt text missing: ${alt.slice(0, 60)}…`);
    if (corpus && !corpus.includes(alt)) {
      fail("dist/llms-full.txt", `<Chart /> alt text missing from ${where}: ${alt.slice(0, 60)}…`);
    }
  }
}

// 5. The landing page, which no content entry covers.
//
// `src/pages/index.md.ts` transcribes the copy out of `index.astro` because the
// page is hand-written Astro with no MDX body to downlevel. Two transcriptions
// of the same prose drift; this is what stops them. `index.astro` is the source
// of truth, so every check below reads the page and looks for it in the output.

/**
 * Markup, emphasis and line wrapping removed, so JSX and markdown compare.
 *
 * Tags become a space rather than nothing, so `</p><p>` does not weld two
 * words together; the space is then taken back off in front of punctuation,
 * which is where `<strong>400,000 partitions</strong>,` and its markdown twin
 * `**400,000 partitions**,` would otherwise disagree.
 */
function flatten(text) {
  return text
    .replace(/<[^>]+>/g, " ")
    .replace(/\*\*/g, "")
    .replace(/\s+/g, " ")
    .replace(/\s+([,.;:!?)\]])/g, "$1")
    .trim();
}

/**
 * Every `key: "value"` pair inside a named array literal in `index.astro`.
 * The optional type annotation is skipped: `const xs: { … }[] = [` is the same
 * array as `const xs = [`.
 */
function astroStrings(astro, arrayName, key) {
  const block = new RegExp(`const ${arrayName}\\b[^=\\n]*= \\[([\\s\\S]*?)\\n\\];`).exec(astro);
  if (!block) return null;
  const out = [];
  if (key === null) {
    for (const [, value] of block[1].matchAll(/"((?:[^"\\]|\\.)*)"/g)) out.push(JSON.parse(`"${value}"`));
    return out;
  }
  for (const [, value] of block[1].matchAll(new RegExp(`\\b${key}:\\s*"((?:[^"\\\\]|\\\\.)*)"`, "g"))) {
    out.push(JSON.parse(`"${value}"`));
  }
  return out;
}

let checkedHome = 0;
if (!existsSync(HOME_ASTRO)) {
  fail("src/pages/index.astro", "missing — the landing page's markdown is transcribed from it");
} else if (!existsSync(HOME_MARKDOWN)) {
  fail("dist/index.md", "missing — the landing page has no markdown alternate");
} else {
  const astro = readFileSync(HOME_ASTRO, "utf8");
  const home = readFileSync(HOME_MARKDOWN, "utf8");
  const flatHome = flatten(home);
  const flatCorpus = corpus ? flatten(corpus) : null;

  const probes = [];

  const hero = /<h1\b[^>]*>([\s\S]*?)<\/h1>\s*<p\b[^>]*>([\s\S]*?)<\/p>/.exec(astro);
  if (!hero) {
    fail("src/pages/index.astro", "no <h1> followed by a hero <p> — check the probe in this script");
  } else {
    probes.push(["headline", flatten(hero[1])], ["hero paragraph", flatten(hero[2])]);
  }

  // The limits used to be a `const notFor = [ … ]` and are a paragraph now, so
  // they are probed out of the section itself. Only the prose before the link
  // is taken: the link's own text and its arrow are page furniture.
  const limits = /\{\/\* Limits \*\/\}[\s\S]*?<p\b[^>]*>([\s\S]*?)<a\b/.exec(astro);
  if (!limits) {
    fail("src/pages/index.astro", "no `{/* Limits */}` section with a paragraph — check the probe in this script");
  } else {
    probes.push(["limits paragraph", flatten(limits[1])]);
  }

  for (const [array, key, label] of [
    ["differentiators", "title", "differentiator title"],
    ["differentiators", "body", "differentiator body"],
    ["proof", "figure", "proof figure"],
    ["proof", "body", "proof body"],
  ]) {
    const values = astroStrings(astro, array, key);
    if (values === null) {
      fail("src/pages/index.astro", `no \`const ${array} = [ … ];\` — check the probe in this script`);
      continue;
    }
    if (values.length === 0) {
      fail("src/pages/index.astro", `\`${array}\` yielded no ${label} strings`);
      continue;
    }
    for (const value of values) probes.push([label, flatten(value)]);
  }

  for (const [label, probe] of probes) {
    if (!probe) continue;
    checkedHome++;
    if (!flatHome.includes(probe)) {
      fail(
        "dist/index.md",
        `${label} in index.astro is not in the landing page's markdown: "${probe.slice(0, 70)}…". ` +
          `src/pages/index.md.ts transcribes this copy by hand; update it to match the page.`,
      );
    }
    if (flatCorpus && !flatCorpus.includes(probe)) {
      fail("dist/llms-full.txt", `${label} from the landing page is missing: "${probe.slice(0, 70)}…"`);
    }
  }
}

if (problems.length) {
  for (const { where, what } of problems.slice(0, 40)) console.error(`${where}  ${what}`);
  if (problems.length > 40) console.error(`\n… and ${problems.length - 40} more.`);
  console.error(
    `\n${problems.length} problem(s) in the markdown alternates. The .md twins and ` +
      `llms-full.txt are what agents read: content that reaches the HTML has to reach them too.`,
  );
  process.exit(1);
}

const componentSummary = [...usedComponents]
  .sort()
  .map(([name, files]) => `${name}×${files.length}`)
  .join(" ");

console.log(
  `ok  markdown alternates (${pages.length} pages, ${checkedFences} fences, ` +
    `${checkedPartials} partial inclusions, ${checkedCharts} charts, ` +
    `${checkedHome} landing-page probes)\n` +
    `    self-closing components in content: ${componentSummary || "none"}`,
);
