// Render every figure of the book and the website.
//
//   node figures/build.mjs                 -> figures/out/<id>.svg (light) and <id>.dark.svg
//   node figures/build.mjs --pdf <dir>     -> also <dir>/<id>.pdf, for the book (rsvg-convert)
//   node figures/build.mjs --png <dir>     -> also <dir>/<id>.png, previews
//   node figures/build.mjs --site <dir>    -> also copies the SVGs and index.json to <dir>, for the website
//   node figures/build.mjs --only <id,...> -> just those figures
//
// A figure is a module in figures/src/<chapter>/<id>.mjs whose default export is
// { id, height, alt, draw(canvas, palette) }. The light SVG names its colours (the
// book's renderer reads no CSS variables); the site shows the light or the dark file
// with its theme. Text is set in Inter and Source Code Pro: rsvg-convert finds them
// in figures/fonts through a fontconfig file written next to the output.
import { copyFileSync, mkdirSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { execFileSync } from "node:child_process";
import { Canvas, PALETTES } from "./lib/draw.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const OUT = join(HERE, "out");
const args = process.argv.slice(2);
const opt = (name) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : null;
};
const only = opt("--only")?.split(",");

function modules(dir) {
  const found = [];
  for (const name of readdirSync(dir).sort()) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) found.push(...modules(p));
    else if (name.endsWith(".mjs")) found.push(p);
  }
  return found;
}

mkdirSync(OUT, { recursive: true });
const index = {};
const ids = new Set();
for (const file of modules(join(HERE, "src"))) {
  const fig = (await import(pathToFileURL(file).href)).default;
  if (!fig?.id || !fig.height || !fig.alt || typeof fig.draw !== "function") {
    throw new Error(`${relative(HERE, file)}: a figure exports { id, height, alt, draw }`);
  }
  if (ids.has(fig.id)) throw new Error(`figure id ${fig.id} is defined twice`);
  ids.add(fig.id);
  index[fig.id] = { alt: fig.alt, height: fig.height, source: relative(join(HERE, ".."), file) };
  if (only && !only.includes(fig.id)) continue;
  for (const [theme, palette] of Object.entries(PALETTES)) {
    const c = new Canvas(palette, fig.height);
    fig.draw(c, palette);
    writeFileSync(join(OUT, theme === "light" ? `${fig.id}.svg` : `${fig.id}.dark.svg`), c.svg(fig.id, fig.alt));
  }
}
writeFileSync(join(OUT, "index.json"), JSON.stringify(index, null, 2) + "\n");

// The book's renderer: rsvg-convert with fontconfig pointed at figures/fonts.
function rsvg(format, dir) {
  mkdirSync(dir, { recursive: true });
  const conf = join(OUT, "fonts.conf");
  writeFileSync(conf, `<?xml version="1.0"?>\n<!DOCTYPE fontconfig SYSTEM "fonts.dtd">\n<fontconfig>\n  <dir>${join(HERE, "fonts")}</dir>\n  <cachedir>${join(OUT, ".fontcache")}</cachedir>\n</fontconfig>\n`);
  for (const id of Object.keys(index)) {
    if (only && !only.includes(id)) continue;
    const extra = format === "png" ? ["-w", "1600", "-b", "white"] : [];
    execFileSync("rsvg-convert", ["-f", format, ...extra, "-o", join(dir, `${id}.${format}`), join(OUT, `${id}.svg`)], {
      env: { ...process.env, FONTCONFIG_FILE: conf },
    });
  }
}
if (opt("--pdf")) rsvg("pdf", opt("--pdf"));
if (opt("--png")) rsvg("png", opt("--png"));
// The website serves the files as they are: one <img> per theme (webdoc/src/components/Fig.astro).
if (opt("--site")) {
  const dir = opt("--site");
  mkdirSync(dir, { recursive: true });
  for (const id of Object.keys(index)) {
    for (const name of [`${id}.svg`, `${id}.dark.svg`]) copyFileSync(join(OUT, name), join(dir, name));
  }
  copyFileSync(join(OUT, "index.json"), join(dir, "index.json"));
}
console.log(`${only ? only.length : ids.size} figures`);
