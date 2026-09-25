/**
 * The dashboard (app/src/router/index.js) puts a `docs` path on route meta to
 * render a "?" next to the page title, next to whatever in the UI is not
 * self-explanatory. check-sourceoftruth.mjs already guards the opposite
 * direction — a doc page going stale against the code it describes. Nothing
 * guarded this one: a page moved or removed under webdoc/src/content/docs/
 * left the dashboard linking to a 404, silently, because nothing outside this
 * script ever builds the app and the site in the same place.
 *
 * The rule is two comparisons:
 *
 *   for every route with `docs: '/some/path'`, that path must resolve to a
 *   real page under webdoc/src/content/docs/, and it must be a whole page,
 *   never `#anchor` — a heading rename must not be able to break this link
 *   the way a moved file can be caught here but a renamed heading cannot.
 *
 *   the docs origin hardcoded in Header.vue (DOCS_BASE_URL) must match
 *   `site` in astro.config.ts — the one place that value is supposed to live.
 */

import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { REPO, WEBDOC } from "./lib/source.mjs";

const ROUTER = join(REPO, "app", "src", "router", "index.js");
const HEADER = join(REPO, "app", "src", "components", "Header.vue");
const ASTRO_CONFIG = join(WEBDOC, "astro.config.ts");
const DOCS = join(WEBDOC, "src", "content", "docs");

const problems = [];
const fail = (what) => problems.push(what);

// ---------------------------------------------------------------------------
// 1. Every `docs:` path on a route resolves to a real page.
// ---------------------------------------------------------------------------

function routeDocLinks(source) {
  const links = [];
  let currentName = "(unnamed route)";
  source.split("\n").forEach((line, i) => {
    const name = line.match(/^\s*name:\s*'([^']+)'/);
    if (name) currentName = name[1];
    const docs = line.match(/docs:\s*'([^']+)'/);
    if (docs) links.push({ name: currentName, path: docs[1], line: i + 1 });
  });
  return links;
}

function pageExists(docPath) {
  return (
    existsSync(join(DOCS, `${docPath}.mdx`)) ||
    existsSync(join(DOCS, `${docPath}.md`)) ||
    existsSync(join(DOCS, docPath, "index.mdx")) ||
    existsSync(join(DOCS, docPath, "index.md"))
  );
}

const routerSource = readFileSync(ROUTER, "utf8");
const links = routeDocLinks(routerSource);

for (const { name, path, line } of links) {
  const where = `app/src/router/index.js:${line}`;
  if (!path.startsWith("/")) {
    fail(`${where}  route "${name}" has docs: '${path}', which is not repo-of-site-rooted (must start with '/').`);
    continue;
  }
  if (path.includes("#")) {
    fail(
      `${where}  route "${name}" has docs: '${path}' with a '#' anchor. Doc links here are whole ` +
        `pages only, so a heading rename can't break them silently — point this at the page instead.`,
    );
    continue;
  }
  if (!pageExists(path)) {
    fail(
      `${where}  route "${name}" has docs: '${path}', which does not exist under ` +
        `webdoc/src/content/docs/. The page moved or was removed — update the route's meta.docs.`,
    );
  }
}

// ---------------------------------------------------------------------------
// 2. The hardcoded docs origin in Header.vue matches astro.config.ts.
// ---------------------------------------------------------------------------

const headerSource = readFileSync(HEADER, "utf8");
const astroSource = readFileSync(ASTRO_CONFIG, "utf8");

const headerBase = headerSource.match(/DOCS_BASE_URL\s*=\s*'([^']+)'/)?.[1];
const siteUrl = astroSource.match(/site:\s*"([^"]+)"/)?.[1];

if (!headerBase) {
  fail(`app/src/components/Header.vue  no DOCS_BASE_URL constant found; check-doclinks.mjs can't verify it.`);
} else if (!siteUrl) {
  fail(`webdoc/astro.config.ts  no top-level \`site:\` found; check-doclinks.mjs can't verify Header.vue against it.`);
} else if (headerBase !== siteUrl) {
  fail(
    `app/src/components/Header.vue  DOCS_BASE_URL is '${headerBase}' but webdoc/astro.config.ts ` +
      `\`site\` is '${siteUrl}'. Every doc link in the dashboard points at the wrong origin.`,
  );
}

// ---------------------------------------------------------------------------

if (problems.length) {
  for (const p of problems) console.error(p);
  console.error(`\n${problems.length} broken dashboard doc link(s).`);
  process.exit(1);
}

console.log(`ok  dashboard doc links (${links.length} route(s) with a docs: path)`);
