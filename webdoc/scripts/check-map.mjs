/**
 * Every link of the map page (/map/) leads somewhere, checked over the built `dist/`.
 *
 * The map (src/lib/map.ts, drawn by src/components/QueenMap.astro) restates the
 * docs in one place and sends the reader to each page for the rest: a dot's
 * "Read it in the docs" link and a goal's pattern page. A page that moves, or a
 * heading that is renamed, would leave the map pointing at nothing, and nothing
 * else would notice: the link checker of the docs reads the docs' own links.
 *
 * For every such link: the page must exist in dist/, and when the link names a
 * heading (`#leases-and-long-work`), an element with that id must be on it.
 * And the page's preview image, the map drawn at build time, must be there.
 */

import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const WEBDOC = join(dirname(fileURLToPath(import.meta.url)), "..");
const DIST = join(WEBDOC, "dist");
const MAP = join(DIST, "map", "index.html");

if (!existsSync(MAP)) {
  console.error("dist/map/index.html does not exist. Run `pnpm build` first.");
  process.exit(1);
}

const html = readFileSync(MAP, "utf8");
const decode = (s) => s.replace(/&quot;/g, '"').replace(/&#34;/g, '"').replace(/&amp;/g, "&").replace(/&#39;/g, "'");

const links = new Set();
for (const [, href] of html.matchAll(/<a class="qm-link"[^>]*?href="([^"]+)"/g)) links.add(decode(href));
for (const [, href] of html.matchAll(/<a[^>]*?href="([^"]+)"[^>]*?class="qm-link"/g)) links.add(decode(href));
const dataAttr = html.match(/data-qm-data="([^"]*)"/);
if (!dataAttr) {
  console.error("check-map: the map page has no data-qm-data attribute. Did QueenMap.astro change?");
  process.exit(1);
}
const data = JSON.parse(decode(dataAttr[1]));
for (const goal of Object.values(data.goals)) links.add(goal.href);

const entries = Object.keys(data.entries).length;
if (links.size < 20) {
  console.error(`check-map: found only ${links.size} links on the map page. The page or this script changed.`);
  process.exit(1);
}

const problems = [];
const pages = new Map();
for (const link of [...links].sort()) {
  if (!link.startsWith("/")) {
    problems.push(`${link}: not a link inside the site`);
    continue;
  }
  const [path, hash] = link.split("#");
  const file = join(DIST, path.replace(/^\/+/, ""), "index.html");
  if (!existsSync(file)) {
    problems.push(`${link}: there is no page ${path} in dist/`);
    continue;
  }
  if (!hash) continue;
  if (!pages.has(file)) pages.set(file, readFileSync(file, "utf8"));
  if (!pages.get(file).includes(`id="${hash}"`)) problems.push(`${link}: the page ${path} has no heading #${hash}`);
}

// The page's preview image is the map itself (src/pages/og/map.png.ts): a PNG of
// the size social sites show, 1200 x 630.
const OG = join(DIST, "og", "map.png");
if (!existsSync(OG)) {
  console.error("check-map: dist/og/map.png is missing: src/pages/og/map.png.ts drew nothing.");
  process.exit(1);
}
const png = readFileSync(OG);
const [ogW, ogH] = [png.readUInt32BE(16), png.readUInt32BE(20)];
if (png.toString("latin1", 1, 4) !== "PNG" || ogW !== 1200 || ogH !== 630) {
  console.error(`check-map: dist/og/map.png is not a 1200 x 630 PNG (${ogW} x ${ogH}).`);
  process.exit(1);
}

if (problems.length) {
  console.error(`check-map: ${problems.length} link(s) of src/lib/map.ts lead nowhere:\n`);
  for (const p of problems) console.error(`  ${p}`);
  console.error("\nFix the `docs` or `href` in src/lib/map.ts, or the heading it names.");
  process.exit(1);
}
console.log(`ok  check-map: ${links.size} links of ${entries} boxes and dots lead to a page and a heading; the preview image is 1200 x 630`);
