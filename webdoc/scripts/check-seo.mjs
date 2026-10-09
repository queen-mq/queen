/**
 * What a search engine reads, checked over the built `dist/`.
 *
 * Every regression below built, deployed and failed nothing. An SEO audit
 * (2026-10-08) found them on the live site:
 *
 *   1. **Breadcrumb trails.** The section crumb ("Concepts") of every
 *      BreadcrumbList had no `item` URL. Google needs `item` on every crumb but
 *      the last, so no doc page qualified for a breadcrumb in its result.
 *   2. **Source twins.** The `.md` twins carry `X-Robots-Tag: noindex`; the
 *      `.mdx` twins served by `src/pages/[...slug]/index.mdx.ts` did not, so
 *      76 copies of the pages were indexable.
 *   3. **Security headers.** No response carried one.
 *   4. **Inline scripts against the CSP.** The CSP in `public/_headers` allows
 *      inline scripts by hash only. An inline script whose hash is not listed
 *      is blocked in the browser, which breaks the theme toggle on every page
 *      and shows nothing in a build. So every hash is asserted here.
 *   5. **Sitemap dates.** No `<lastmod>`, although every page knows its date.
 *   6. **Titles.** Two pairs of pages shared a `<title>`, and most titles were
 *      a bare label ("Overview", "Compare") that says nothing in a result list.
 *   7. **Site name.** The homepage's WebSite node, which search engines read
 *      as the site name, carried the homepage's page title instead.
 *
 * Section 1 also asserts what was added with these fixes: every doc page has a
 * TechArticle with its `dateModified`.
 *
 * Run after `pnpm build`.
 */

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { WEBDOC } from "./lib/source.mjs";

const DIST = join(WEBDOC, "dist");
const SITE = "https://queenmq.com";

/**
 * Shortest `<title>` accepted, brand suffix included. "Overview | Queen MQ" is
 * 19; a title that names its subject in words a reader would search for is
 * rarely under 30.
 */
const MIN_TITLE_CHARS = 30;
/** Longest `<title>` accepted. Results cut titles at about 60 characters. */
const MAX_TITLE_CHARS = 65;

/** Headers every HTML response must carry, lower-cased. */
const REQUIRED_SECURITY_HEADERS = [
  "strict-transport-security",
  "x-content-type-options",
  "referrer-policy",
  "x-frame-options",
  "permissions-policy",
  "content-security-policy",
];

const problems = [];
const fail = (where, what) => problems.push({ where, what });

if (!existsSync(DIST)) {
  console.error("dist/ does not exist. Run `pnpm build` first.");
  process.exit(1);
}

function walk(dir, out = []) {
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) walk(p, out);
    else out.push(p);
  }
  return out;
}

const files = walk(DIST);
const rel = (abs) => relative(DIST, abs).split(sep).join("/");

/** `start/index.html` -> `/start/`, `index.html` -> `/`. */
const urlPathOfHtml = (abs) => `/${rel(abs).replace(/index\.html$/, "")}`;

// ---------------------------------------------------------------------------
// public/_headers, parsed the way Cloudflare applies it: a path line, then its
// indented `Name: value` lines. Every rule whose path matches a request
// applies, and their headers merge. `*` is a greedy splat (it crosses `/`),
// `:name` is one segment.
// ---------------------------------------------------------------------------

function parseHeaders(text) {
  const rules = [];
  let current = null;
  for (const raw of text.split("\n")) {
    const line = raw.replace(/\s+$/, "");
    if (!line.trim() || line.trim().startsWith("#")) continue;
    if (!/^\s/.test(line)) {
      current = { path: line.trim(), headers: [] };
      rules.push(current);
      continue;
    }
    const m = line.trim().match(/^([^:]+):\s*(.*)$/);
    if (current && m) current.headers.push([m[1].trim().toLowerCase(), m[2]]);
  }
  return rules;
}

function patternToRegex(path) {
  const body = path
    .split(/(\*|:[A-Za-z_][A-Za-z0-9_]*)/)
    .map((part) => {
      if (part === "*") return ".*";
      if (part.startsWith(":")) return "[^/]+";
      return part.replace(/[.+?^${}()|[\]\\]/g, "\\$&");
    })
    .join("");
  return new RegExp(`^${body}$`);
}

const headerRules = parseHeaders(readFileSync(join(DIST, "_headers"), "utf8"))
  .filter((r) => r.path.startsWith("/"))
  .map((r) => ({ ...r, re: patternToRegex(r.path) }));

// Cloudflare allows one splat per rule. The matcher above would accept more,
// and then pass a rule the platform does not apply as written.
for (const rule of headerRules) {
  if ((rule.path.match(/\*/g) ?? []).length > 1) {
    fail("public/_headers", `rule ${rule.path} has more than one splat; Cloudflare allows one per rule.`);
  }
}

/** Every header that applies to `urlPath`, as `name -> [values]`. */
function headersFor(urlPath) {
  const merged = new Map();
  for (const rule of headerRules) {
    if (!rule.re.test(urlPath)) continue;
    for (const [name, value] of rule.headers) {
      merged.set(name, [...(merged.get(name) ?? []), value]);
    }
  }
  return merged;
}

// 2. Source twins are noindex -------------------------------------------------

const twins = files.filter((f) => /\.(md|mdx)$/.test(f));
for (const twin of twins) {
  const urlPath = `/${rel(twin)}`;
  const robots = (headersFor(urlPath).get("x-robots-tag") ?? []).join(", ");
  if (!/noindex/i.test(robots)) {
    fail(
      rel(twin),
      `${urlPath} is served without \`X-Robots-Tag: noindex\`. It duplicates an HTML page ` +
        "that has the canonical. Add its pattern to public/_headers.",
    );
  }
}

// HTML pages, and which of them a search engine may index ---------------------

const htmlPages = files
  .filter((f) => f.endsWith(".html"))
  .map((abs) => {
    const html = readFileSync(abs, "utf8");
    // 404.html answers any unknown path, under that path's headers. It is
    // checked for headers and scripts like any page, and never indexed.
    const notFound = rel(abs) === "404.html";
    const urlPath = notFound ? "/no-such-page/" : urlPathOfHtml(abs);
    const metaNoindex = /<meta[^>]+name="robots"[^>]+content="[^"]*noindex/i.test(html);
    const headerNoindex = (headersFor(urlPath).get("x-robots-tag") ?? []).some((v) =>
      /noindex/i.test(v),
    );
    return { abs, html, urlPath, notFound, indexable: !notFound && !metaNoindex && !headerNoindex };
  });
const indexable = htmlPages.filter((p) => p.indexable);
const pagePaths = new Set(htmlPages.filter((p) => !p.notFound).map((p) => p.urlPath));

// 3 + 4. Security headers, and every inline script allowed by its hash ---------

const sha256 = (text) => `'sha256-${createHash("sha256").update(text).digest("base64")}'`;

for (const page of htmlPages) {
  const headers = headersFor(page.urlPath);
  const missing = REQUIRED_SECURITY_HEADERS.filter((h) => !headers.has(h));
  if (missing.length) {
    fail(rel(page.abs), `${page.urlPath} has no ${missing.join(", ")}. See the \`/*\` block in public/_headers.`);
    continue;
  }

  // Each matching rule sends its own CSP header and a browser enforces every
  // one of them, so a script has to pass each policy on its own.
  const inlineScripts = [...page.html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/g)]
    .filter(([, attrs]) => !/\bsrc=/.test(attrs) && !/type="application\/(ld\+)?json"/.test(attrs))
    .map(([, , body]) => body);
  for (const csp of headers.get("content-security-policy")) {
    const directive = (name) => (csp.match(new RegExp(`(?:^|;)\\s*${name}\\s+([^;]*)`)) ?? [])[1];
    const scriptSrc = directive("script-src") ?? directive("default-src") ?? "";
    if (/'unsafe-inline'/.test(scriptSrc)) continue;
    for (const body of inlineScripts) {
      const hash = sha256(body);
      if (scriptSrc.includes(hash)) continue;
      fail(
        rel(page.abs),
        `an inline script is not allowed by the CSP, so the browser will block it. Add ${hash} ` +
          `to script-src in public/_headers. It starts: ${body.trim().slice(0, 60).replace(/\s+/g, " ")}`,
      );
    }
  }
  const handler = page.html.match(/<[a-z][^>]*\s(on[a-z]+)=["']/i);
  if (handler) {
    fail(rel(page.abs), `inline event handler \`${handler[1]}\`: the CSP blocks it. Move it into a script.`);
  }
}

// 1. Structured data: breadcrumb trails, the site's name, one article per page --

/** Content files git tracks, as URL paths (`concepts/kv.mdx` -> `/concepts/kv/`). */
const tracked = new Set(
  execFileSync("git", ["ls-files", "src/content/docs"], { cwd: WEBDOC, encoding: "utf8" })
    .split("\n")
    .filter(Boolean)
    .map((f) => `/${f.replace(/^src\/content\/docs\//, "").replace(/(\/index)?\.mdx?$/, "")}/`),
);
const isTracked = (urlPath) => tracked.has(urlPath);
const undatedLocal = [];

/** Every JSON-LD node on a page, `@graph` members flattened. */
function jsonLdNodes(page) {
  const nodes = [];
  for (const [, json] of page.html.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/g)) {
    try {
      const data = JSON.parse(json);
      nodes.push(...(Array.isArray(data["@graph"]) ? data["@graph"] : [data]));
    } catch (e) {
      fail(rel(page.abs), `JSON-LD does not parse: ${e.message}`);
    }
  }
  return nodes;
}

for (const page of indexable) {
  const nodes = jsonLdNodes(page);

  // The homepage's WebSite node is what search engines read as the site name
  // beside every result. The framework used to fill it with the page title.
  for (const site of nodes.filter((n) => n["@type"] === "WebSite" && n.url === `${SITE}/`)) {
    if (page.urlPath === "/" && site.name !== "Queen MQ") {
      fail(rel(page.abs), `the WebSite node is named "${site.name}", not "Queen MQ". See patches/.`);
    }
  }

  if (page.urlPath !== "/") {
    const article = nodes.find((n) => n["@type"] === "TechArticle");
    if (!article) fail(rel(page.abs), "no TechArticle node. See src/layouts/DocsLayout.astro.");
    else if (!article.dateModified) {
      // The date is git's, so a page written but not committed has none yet.
      // That is a local state, not a defect: CI builds committed pages only.
      if (isTracked(page.urlPath)) fail(rel(page.abs), "the TechArticle has no dateModified.");
      else undatedLocal.push(page.urlPath);
    }
  }

  for (const data of nodes) {
    if (data["@type"] !== "BreadcrumbList") continue;
    const items = data.itemListElement ?? [];
    items.forEach((item, i) => {
      if (item.position !== i + 1) {
        fail(rel(page.abs), `breadcrumb "${item.name}" has position ${item.position}, expected ${i + 1}.`);
      }
      if (!item.item) {
        if (i < items.length - 1) {
          fail(rel(page.abs), `breadcrumb "${item.name}" has no \`item\` URL. Only the last crumb may omit it.`);
        }
        return;
      }
      const target = new URL(item.item).pathname;
      if (!pagePaths.has(target)) {
        fail(rel(page.abs), `breadcrumb "${item.name}" points at ${target}, which is not a page.`);
      }
    });
  }
}

// 6. Titles ---------------------------------------------------------------------

const titleOf = (html) => {
  const m = html.match(/<title>([\s\S]*?)<\/title>/);
  return m ? m[1].replace(/&amp;/g, "&").replace(/&#39;/g, "'").replace(/&quot;/g, '"').trim() : "";
};
const byTitle = new Map();
for (const page of indexable) {
  const title = titleOf(page.html);
  if (!title) {
    fail(rel(page.abs), "no <title>.");
    continue;
  }
  byTitle.set(title, [...(byTitle.get(title) ?? []), page.urlPath]);
  if (title.length < MIN_TITLE_CHARS) {
    fail(
      rel(page.abs),
      `<title> "${title}" is ${title.length} characters. Name the subject in the words a reader ` +
        `searches for (at least ${MIN_TITLE_CHARS}); keep the short name with \`sidebar: label:\`.`,
    );
  }
  if (title.length > MAX_TITLE_CHARS) {
    fail(rel(page.abs), `<title> "${title}" is ${title.length} characters; results cut it after about ${MAX_TITLE_CHARS}.`);
  }
}
for (const [title, paths] of byTitle) {
  if (paths.length > 1) fail(paths.join(", "), `these pages share the <title> "${title}".`);
}

// 5. Sitemap --------------------------------------------------------------------

const locs = (xml) => [...xml.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]);
const index = readFileSync(join(DIST, "sitemap-index.xml"), "utf8");
const sitemapUrls = [];
for (const child of locs(index)) {
  const file = join(DIST, new URL(child).pathname);
  if (!existsSync(file)) {
    fail("sitemap-index.xml", `lists ${child}, which is not in dist/.`);
    continue;
  }
  const xml = readFileSync(file, "utf8");
  for (const [, block] of xml.matchAll(/<url>([\s\S]*?)<\/url>/g)) {
    const loc = (block.match(/<loc>([^<]+)<\/loc>/) ?? [])[1];
    const lastmod = (block.match(/<lastmod>([^<]+)<\/lastmod>/) ?? [])[1];
    sitemapUrls.push(loc);
    if (!lastmod) {
      fail(rel(file), `${loc} has no <lastmod>. See the sitemap serializer in astro.config.ts.`);
    } else if (Number.isNaN(Date.parse(lastmod)) || Date.parse(lastmod) > Date.now() + 86_400_000) {
      fail(rel(file), `${loc} has an invalid or future <lastmod> "${lastmod}".`);
    }
  }
}
const inSitemap = new Set(sitemapUrls.map((u) => new URL(u).pathname));
for (const page of indexable) {
  if (!inSitemap.has(page.urlPath)) fail("sitemap", `${page.urlPath} is indexable but not in the sitemap.`);
}
for (const path of inSitemap) {
  if (!indexable.some((p) => p.urlPath === path)) {
    fail("sitemap", `${SITE}${path} is in the sitemap but is not an indexable page.`);
  }
}

// -------------------------------------------------------------------------------

if (problems.length) {
  for (const { where, what } of problems) console.error(`${where}  ${what}`);
  console.error(`\n${problems.length} problem(s) with what search engines read.`);
  process.exit(1);
}

if (undatedLocal.length) {
  console.log(`note  not committed yet, so no date until they are: ${undatedLocal.join(", ")}`);
}
console.log(
  `ok  search surface (${indexable.length} indexable pages with unique titles, ` +
    `${sitemapUrls.length} sitemap URLs with <lastmod>, ${twins.length} source twins noindex, ` +
    "breadcrumbs linked, security headers and CSP hashes in place)",
);
