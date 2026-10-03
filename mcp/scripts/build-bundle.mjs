#!/usr/bin/env node
/**
 * Build the knowledge bundle the MCP server answers from.
 *
 * Nothing in the bundle is written for it. Every part comes from a file the
 * docs already publish or the repo already tests:
 *
 *   pages      webdoc/dist/<slug>/index.md, the per-page markdown the docs
 *              build emits for agents (run `pnpm --dir webdoc build` first)
 *   snippets   webdoc/src/content/partials/snippets/*.mdx, which
 *              gen-snippets.mjs cuts from files the test harness runs; plus
 *              the page each one is shown on, and the whole program when the
 *              snippet comes from examples/
 *   errors     the tables of reference/errors, reference/transaction and
 *              reference/limits, and protocols/queen-kafka/compat/ERRORS.md
 *   kafka      the client matrix in protocols/queen-kafka/compat/CLIENT_MATRIX.md
 *   codes      content/kafka-error-codes.json, derived from the kafka-protocol
 *              crate's own error table (refresh: --refresh-kafka-codes)
 *   primer     content/primer.md + content/usage.md, the server instructions
 *   traps      content/traps.json
 *   prompts    content/prompts/*.md
 *
 * Output: src/bundle.generated.mjs (gitignored; every deploy rebuilds it).
 */

import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";

const MCP = join(dirname(fileURLToPath(import.meta.url)), "..");
const REPO = join(MCP, "..");
const DIST = process.env.QUEEN_DOCS_DIST ?? join(REPO, "webdoc", "dist");
const CONTENT_SRC = join(REPO, "webdoc", "src", "content");
const COMPAT = join(REPO, "protocols", "queen-kafka", "compat");
const OWN = join(MCP, "content");
const OUT = join(MCP, "src", "bundle.generated.mjs");
const SITE = "https://queenmq.com";
const SERVER_VERSION = JSON.parse(readFileSync(join(MCP, "package.json"), "utf8")).version;

const LANG = { js: "js", py: "python", go: "go", php: "php", rust: "rust", cpp: "cpp", http: "http" };
const PROGRAM_MAX_BYTES = 60_000;

const warnings = [];
const warn = (msg) => warnings.push(msg);

// ---------------------------------------------------------------- helpers

function walk(dir, keep, out = []) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, entry.name);
    if (entry.isDirectory()) walk(p, keep, out);
    else if (keep(entry.name)) out.push(p);
  }
  return out;
}

const posix = (p) => p.split(sep).join("/");

function frontmatter(text) {
  const m = text.match(/^---\n([\s\S]*?)\n---\n?/);
  if (!m) return { data: {}, body: text };
  const data = {};
  for (const line of m[1].split("\n")) {
    const kv = line.match(/^([A-Za-z]+):\s*(.*)$/);
    if (!kv) continue;
    let v = kv[2].trim();
    if (v.startsWith('"') && v.endsWith('"')) v = JSON.parse(v);
    else if (v.startsWith("'") && v.endsWith("'")) v = v.slice(1, -1).replace(/''/g, "'");
    data[kv[1]] = v;
  }
  return { data, body: text.slice(m[0].length) };
}

/** Drop the "for AI agents" blockquote every per-page markdown file opens with. */
function stripAgentHeader(body) {
  const lines = body.replace(/^\n+/, "").split("\n");
  if (!lines[0]?.startsWith("> Queen MQ documentation")) return lines.join("\n");
  let i = 0;
  while (i < lines.length && lines[i].startsWith(">")) i++;
  return lines.slice(i).join("\n").replace(/^\n+/, "");
}

/** github-slugger's rule, which Astro uses for heading ids. */
function slugify(heading) {
  return heading
    .toLowerCase()
    .replace(/<[^>]+>/g, "")
    .replace(/[^\p{L}\p{M}\p{N}\p{Pc} -]/gu, "")
    .replace(/ /g, "-");
}

const isFence = (line) => /^\s*(```|~~~)/.test(line);

/** Split a page into its intro and its `## ` sections, ignoring code blocks. */
function sectionsOf(page) {
  const out = [];
  let heading = page.title;
  let anchor = "";
  let buf = [];
  let fence = false;
  const flush = () => {
    const text = buf.join("\n").trim();
    if (text) out.push({ slug: page.slug, pageTitle: page.title, heading, url: page.url + (anchor ? `#${anchor}` : ""), text });
    buf = [];
  };
  for (const line of page.markdown.split("\n")) {
    if (isFence(line)) fence = !fence;
    if (!fence && line.startsWith("## ")) {
      flush();
      heading = line.slice(3).trim();
      anchor = slugify(heading.replace(/`/g, ""));
      continue;
    }
    if (!fence && line.startsWith("# ") && !buf.length && !out.length) continue; // the page title
    buf.push(line);
  }
  flush();
  return out;
}

function splitRow(line) {
  const s = line.trim().replace(/^\|/, "").replace(/\|$/, "");
  const cells = [];
  let cur = "";
  let tick = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === "\\" && s[i + 1] === "|") {
      cur += "|";
      i++;
      continue;
    }
    if (c === "`") tick = !tick;
    if (c === "|" && !tick) {
      cells.push(cur.trim());
      cur = "";
      continue;
    }
    cur += c;
  }
  cells.push(cur.trim());
  return cells;
}

/** Every markdown table row, with the headings it sits under. */
function tableRows(markdown) {
  const rows = [];
  const lines = markdown.split("\n");
  let h2 = "";
  let h3 = "";
  let fence = false;
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (isFence(line)) fence = !fence;
    if (fence) continue;
    if (line.startsWith("## ")) {
      h2 = line.slice(3).trim();
      h3 = "";
      continue;
    }
    if (line.startsWith("### ")) {
      h3 = line.slice(4).trim();
      continue;
    }
    if (line.startsWith("|") && /^\|\s*:?-{3,}/.test(lines[i + 1] ?? "")) {
      const header = splitRow(line);
      let j = i + 2;
      for (; j < lines.length && lines[j].startsWith("|"); j++) {
        rows.push({ h2, h3, header, cells: splitRow(lines[j]) });
      }
      i = j - 1;
    }
  }
  return rows;
}

const plain = (s) => s.replace(/`/g, "").trim();

function git(...args) {
  try {
    return execFileSync("git", ["-C", REPO, ...args], { encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

// ---------------------------------------------------------------- pages

if (!existsSync(DIST)) {
  console.error(`No docs build at ${DIST}. Run \`pnpm --dir webdoc build\` first, or set QUEEN_DOCS_DIST.`);
  process.exit(1);
}

const pages = walk(DIST, (name) => name === "index.md")
  .map((file) => {
    const slug = posix(relative(DIST, dirname(file)));
    const { data, body } = frontmatter(readFileSync(file, "utf8"));
    return {
      slug,
      url: `${SITE}/${slug ? `${slug}/` : ""}`,
      title: data.title || slug || "Queen MQ",
      description: data.description ?? "",
      markdown: stripAgentHeader(body).trim(),
    };
  })
  .sort((a, b) => a.slug.localeCompare(b.slug));

// Astro empties dist/ when a build starts, so a docs build running beside this
// one shows up as a site with most of its pages missing.
if (pages.length < 40 || !existsSync(join(DIST, "llms.txt"))) {
  console.error(`${DIST} has ${pages.length} pages and ${existsSync(join(DIST, "llms.txt")) ? "an" : "no"} llms.txt: the docs build looks incomplete. Rebuild it, or wait for the one in progress.`);
  process.exit(1);
}

// Sections are stored as offsets into their page, so the text is in the bundle once.
const sections = pages.flatMap((page) => {
  let cursor = 0;
  return sectionsOf(page).map(({ text, ...rest }) => {
    const start = page.markdown.indexOf(text, cursor);
    if (start < 0) throw new Error(`section "${rest.heading}" of ${page.slug} is not a substring of the page`);
    cursor = start + text.length;
    return { ...rest, start, end: cursor };
  });
});
const pageBySlug = new Map(pages.map((p) => [p.slug, p]));

// ---------------------------------------------------------------- snippets

const docsSrc = join(CONTENT_SRC, "docs");
const shownOn = new Map(); // snippet id -> [{ slug, heading }]
for (const file of walk(docsSrc, (name) => name.endsWith(".mdx"))) {
  const slug = posix(relative(docsSrc, file)).replace(/\.mdx$/, "").replace(/(^|\/)index$/, "");
  let heading = "";
  for (const line of readFileSync(file, "utf8").split("\n")) {
    if (line.startsWith("## ")) heading = line.slice(3).trim();
    for (const m of line.matchAll(/snippets\/([a-z0-9-]+)/g)) {
      const list = shownOn.get(m[1]) ?? [];
      if (!list.some((u) => u.slug === slug && u.heading === heading)) list.push({ slug, heading });
      shownOn.set(m[1], list);
    }
  }
}

function snippetKind(id) {
  let m = id.match(/^(app|full|tut)-(js|py|go|php|rust|cpp|http)-(.+)$/);
  if (m) return { kind: m[1], language: LANG[m[2]], task: m[3] };
  m = id.match(/^(js|py|go|php|rust|cpp|http)-(.+)$/);
  if (m) return { kind: "api", language: LANG[m[1]], task: m[2] };
  m = id.match(/^embedded-(.+)$/);
  if (m) return { kind: "embedded", language: "rust", task: `embedded broker ${m[1]}` };
  return null;
}

const snippetDir = join(CONTENT_SRC, "partials", "snippets");
const snippets = [];
const programs = {};
for (const name of readdirSync(snippetDir).filter((n) => n.endsWith(".mdx")).sort()) {
  const id = name.replace(/\.mdx$/, "");
  const kind = snippetKind(id);
  if (!kind) continue;
  const { data, body } = frontmatter(readFileSync(join(snippetDir, name), "utf8"));
  const fence = body.match(/```([A-Za-z0-9+#-]*)[^\n]*\n([\s\S]*?)\n```/);
  if (!fence) {
    warn(`snippet ${id}: no code block`);
    continue;
  }
  const source = (data.description ?? "").match(/extracted from (.+?)\.?$/i)?.[1] ?? "";
  if (source.startsWith("examples/") && existsSync(join(REPO, source)) && !(source in programs)) {
    const size = statSync(join(REPO, source)).size;
    if (size <= PROGRAM_MAX_BYTES) programs[source] = readFileSync(join(REPO, source), "utf8");
    else warn(`snippet ${id}: program ${source} is ${size} bytes, not bundled`);
  }
  const pagesShown = (shownOn.get(id) ?? [])
    .filter((u) => pageBySlug.has(u.slug))
    .map((u) => {
      const page = pageBySlug.get(u.slug);
      return { title: page.title, heading: u.heading, url: page.url + (u.heading ? `#${slugify(u.heading.replace(/`/g, ""))}` : "") };
    });
  snippets.push({ id, ...kind, fenceLang: fence[1] || kind.language, source, code: fence[2], pages: pagesShown });
}

// ---------------------------------------------------------------- install and connect
// Package names and import paths, per language, from start/clients: the page
// that owns them. Every SDK answer carries its language's block, so an agent
// never has to guess a module path.

const INSTALL_HEADINGS = { JavaScript: "js", Python: "python", Go: "go", Rust: "rust", "C++": "cpp", "PHP and Laravel": "php" };
const install = {};
{
  const page = pageBySlug.get("start/clients");
  const sec = sections.find((x) => x.slug === "start/clients" && x.heading === "Install and connect");
  if (page && sec) {
    for (const block of page.markdown.slice(sec.start, sec.end).split(/\n(?=### )/)) {
      const m = block.match(/^### (.+)\n([\s\S]*)$/);
      const language = m && INSTALL_HEADINGS[m[1].trim()];
      if (language) install[language] = { url: `${page.url}#${slugify(m[1].trim())}`, markdown: m[2].trim() };
    }
  }
  for (const language of Object.values(INSTALL_HEADINGS)) {
    if (!install[language]) warn(`start/clients: no install block for ${language}`);
  }
}

// ---------------------------------------------------------------- docs code
// The code blocks the docs pages carry themselves. A block with a title="<file>"
// is a tested snippet and is already in `snippets`; the rest were written for
// the page, and the tools say so when they hand one out.

const FENCE_LANG = {
  js: "js", javascript: "js", mjs: "js", ts: "js", typescript: "js",
  python: "python", py: "python", go: "go", php: "php", rust: "rust", rs: "rust",
  cpp: "cpp", "c++": "cpp", bash: "http", sh: "http", shell: "http", http: "http",
};
const docCode = [];
for (const s of sections) {
  const text = pageBySlug.get(s.slug).markdown.slice(s.start, s.end);
  for (const m of text.matchAll(/^```([A-Za-z+#-]*)([^\n]*)\n([\s\S]*?)\n```/gm)) {
    const [, fence, info, code] = m;
    if (/title=/.test(info)) continue;
    const language = FENCE_LANG[fence.toLowerCase()];
    if (!language || code.split("\n").length < 2) continue;
    if (language === "http" && !/\bcurl\b/.test(code)) continue;
    docCode.push({ language, fenceLang: fence, code, slug: s.slug, pageTitle: s.pageTitle, heading: s.heading, url: s.url });
  }
}

// ---------------------------------------------------------------- errors

const errors = [];
const docRows = (slug, keepSection) => {
  const page = pageBySlug.get(slug);
  if (!page) {
    warn(`errors: page ${slug} missing from the docs build`);
    return;
  }
  for (const row of tableRows(page.markdown)) {
    const section = row.h3 ? `${row.h2} / ${row.h3}` : row.h2;
    if (keepSection && !keepSection(section)) continue;
    errors.push({
      origin: "queen",
      code: plain(row.cells[0] ?? ""),
      section,
      url: page.url + (row.h2 ? `#${slugify((row.h3 || row.h2).replace(/`/g, ""))}` : ""),
      fields: row.header.map((h, i) => [plain(h), row.cells[i] ?? ""]).filter(([, v]) => v),
    });
  }
};
docRows("reference/errors");
docRows("reference/transaction", (s) => /reason|rollback/i.test(s));
docRows("reference/limits");

const errorsMd = join(COMPAT, "ERRORS.md");
if (existsSync(errorsMd)) {
  for (const row of tableRows(readFileSync(errorsMd, "utf8"))) {
    const name = (row.cells[0] ?? "").match(/^`?([A-Z][A-Z_]+)`?(?:\s*\((-?\d+)\))?/)?.[1];
    if (!name) continue;
    errors.push({
      origin: "kafka",
      code: name,
      section: `Kafka ${row.h2.replace(/\s+—.*$/, "")}`,
      url: "",
      fields: row.header.map((h, i) => [plain(h), row.cells[i] ?? ""]).filter(([, v]) => v),
    });
  }
} else warn(`errors: ${errorsMd} missing`);

// ---------------------------------------------------------------- kafka error codes

const codesFile = join(OWN, "kafka-error-codes.json");
if (process.argv.includes("--refresh-kafka-codes")) {
  const lock = readFileSync(join(REPO, "server", "Cargo.lock"), "utf8");
  const version = lock.match(/name = "kafka-protocol"\nversion = "([^"]+)"/)?.[1];
  const registry = join(process.env.CARGO_HOME ?? join(homedir(), ".cargo"), "registry", "src");
  const crate = readdirSync(registry)
    .map((d) => join(registry, d, `kafka-protocol-${version}`, "src", "error.rs"))
    .find((p) => existsSync(p));
  if (!crate) throw new Error(`kafka-protocol ${version} source not found under ${registry}`);
  const codes = [...readFileSync(crate, "utf8").matchAll(/^\s+\((\w+),\s+(-?\d+),\s+(true|false),\s+"((?:[^"\\]|\\.)*)"\)/gm)].map((m) => ({
    code: Number(m[2]),
    name: m[1].replace(/([a-z0-9])([A-Z])/g, "$1_$2").toUpperCase(),
    retriable: m[3] === "true",
    description: m[4],
  }));
  writeFileSync(codesFile, `${JSON.stringify({ source: `kafka-protocol ${version}, src/error.rs`, codes }, null, 2)}\n`);
  console.log(`wrote ${relative(MCP, codesFile)}: ${codes.length} codes`);
}
const kafkaCodes = existsSync(codesFile) ? JSON.parse(readFileSync(codesFile, "utf8")).codes : (warn("no content/kafka-error-codes.json"), []);

// ---------------------------------------------------------------- kafka clients

const kafka = { clients: [], evidence: [], updated: "" };
const matrixMd = join(COMPAT, "CLIENT_MATRIX.md");
if (existsSync(matrixMd)) {
  const text = readFileSync(matrixMd, "utf8");
  for (const row of tableRows(text)) {
    if (row.h2 !== "The matrix" || row.h3 || row.header[0] !== "Client") continue;
    const get = (name) => row.cells[row.header.indexOf(name)] ?? "";
    kafka.clients.push({
      client: plain(get("Client")),
      versions: get("Versions tested"),
      verified: get("Verified"),
      result: plain(get("Result")),
      config: get("Mandatory config"),
      caveat: get("Main caveat"),
    });
  }
  // The evidence for each non-PASS row: "### <client>: PARTIAL" under "## The non-PASS rows".
  const part = text.split(/\n## /).find((s) => s.startsWith("The non-PASS rows"));
  for (const block of (part ?? "").split(/\n### /).slice(1)) {
    const [title, ...rest] = block.split("\n");
    kafka.evidence.push({ title: title.trim(), text: rest.join("\n").trim() });
  }
  kafka.updated = git("log", "-1", "--format=%ad", "--date=short", "--", posix(relative(REPO, matrixMd)));
} else warn(`kafka: ${matrixMd} missing`);

// ---------------------------------------------------------------- own content

// The broker version the docs describe, so the primer and traps never name a stale one.
const brokerVersion = readFileSync(join(REPO, "server", "Cargo.toml"), "utf8").match(/^version = "([^"]+)"/m)?.[1] ?? "2.0";

const read = (name, fallback) => {
  const p = join(OWN, name);
  if (existsSync(p)) return readFileSync(p, "utf8").trim();
  warn(`content/${name} missing`);
  return fallback;
};

const primer = read("primer.md", "# Queen MQ\n\n(The primer is not written yet.)").replaceAll("{{broker}}", brokerVersion);
const usage = read("usage.md", "");
let traps = [];
try {
  traps = JSON.parse(read("traps.json", "[]").replaceAll("{{broker}}", brokerVersion));
} catch (e) {
  console.error(`content/traps.json is not valid JSON: ${e.message}`);
  process.exit(1);
}
const APPLIES = new Set(["all", "js", "go", "python", "php", "rust", "cpp", "http", "kafka"]);
const SEVERITY = new Set(["breaks", "surprise", "perf"]);
const ids = new Set();
for (const t of traps) {
  const where = `traps.json ${t.id ?? "(no id)"}`;
  for (const key of ["id", "title", "symptom", "cause", "fix"]) {
    if (typeof t[key] !== "string" || !t[key].trim()) throw new Error(`${where}: missing ${key}`);
  }
  if (ids.has(t.id)) throw new Error(`${where}: duplicate id`);
  ids.add(t.id);
  if (!Array.isArray(t.applies) || !t.applies.length || t.applies.some((a) => !APPLIES.has(a))) throw new Error(`${where}: bad applies ${JSON.stringify(t.applies)}`);
  if (!SEVERITY.has(t.severity)) throw new Error(`${where}: bad severity ${t.severity}`);
  if (!Array.isArray(t.source) || !t.source.length) throw new Error(`${where}: no source`);
}

// Setup recipes: each feature names its docs pages; the recipe is those pages,
// plus the API card for the asked language and the traps that mention it.
const setup = JSON.parse(read("setup.json", "[]"));
for (const f of setup) {
  for (const slug of [...f.pages, ...(f.related ?? [])]) {
    if (!pageBySlug.has(slug)) warn(`setup ${f.feature}: page ${slug} is not in the docs build`);
  }
}
const cardsFile = join(OWN, "api-cards.json");
let cards = {};
if (existsSync(cardsFile)) {
  try {
    cards = JSON.parse(readFileSync(cardsFile, "utf8"));
  } catch (e) {
    console.error(`content/api-cards.json is not valid JSON: ${e.message}`);
    process.exit(1);
  }
} else warn("content/api-cards.json missing");

const prompts = {};
const promptDir = join(OWN, "prompts");
if (existsSync(promptDir)) {
  for (const name of readdirSync(promptDir).filter((n) => n.endsWith(".md"))) {
    prompts[name.replace(/\.md$/, "")] = readFileSync(join(promptDir, name), "utf8").trim();
  }
}

// ---------------------------------------------------------------- write

const head = git("rev-parse", "--short", "HEAD");
const dirty = git("status", "--porcelain", "--", "webdoc/src", "protocols/queen-kafka/compat", "mcp/content") ? "+dirty" : "";
const bundle = {
  meta: { server: SERVER_VERSION, docs: `${head}${dirty}`, built: new Date().toISOString() },
  primer,
  usage,
  pages,
  sections,
  snippets,
  programs,
  install,
  docCode,
  errors,
  kafkaCodes,
  kafka,
  traps,
  setup,
  cards,
  prompts,
};

writeFileSync(OUT, `// GENERATED by scripts/build-bundle.mjs. Do not edit.\nexport default ${JSON.stringify(bundle)};\n`);
const kb = (statSync(OUT).size / 1024).toFixed(0);
console.log(
  `bundle: ${pages.length} pages, ${sections.length} sections, ${snippets.length} snippets, ${docCode.length} docs code blocks, ${errors.length} error rows, ` +
    `${kafkaCodes.length} Kafka codes, ${kafka.clients.length} Kafka clients, ${traps.length} traps, ${setup.length} setup recipes, ${Object.keys(cards).length} API card features, ${Object.keys(prompts).length} prompts (${kb} KB)`,
);
for (const w of warnings) console.warn(`warning: ${w}`);
