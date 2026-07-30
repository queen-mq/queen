/**
 * House style checks the Nimbus linter does not cover.
 *
 * Right now that is one rule: no em dashes in anything a reader sees. They were
 * removed from the whole site once; without a check they come back one page at
 * a time. En dashes are left alone — they are correct in numeric ranges like
 * `0.43–0.77`.
 *
 * Surfaces checked are the ones that reach a reader: page content, the pages
 * and components that render it, and the published OpenAPI documents. Source
 * comments are not prose and are not checked.
 */

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { WEBDOC } from "./lib/source.mjs";

/**
 * Scope: text a reader reads, and the files we author that carry copy.
 *
 * Deliberately NOT the whole of `src/`. Most files there came from the Nimbus
 * scaffolder or from `nimbus-docs add`, and `nimbus-docs diff` compares them
 * against upstream to offer updates. Rewriting their doc comments to satisfy a
 * house style would register as permanent drift and bury every real upstream
 * change. Source comments are not prose anyway.
 */
const TARGETS = [
  { dir: join(WEBDOC, "src", "content", "docs"), exts: [".mdx", ".md"] },
  { dir: join(WEBDOC, "public", "openapi"), exts: [".json"] },
];

/** Authored here, and carrying copy rather than only code. */
const FILES = [
  join(WEBDOC, "src", "pages", "index.astro"),
  join(WEBDOC, "src", "components", "Chart.astro"),
  join(WEBDOC, "src", "components", "Header.astro"),
];

const BANNED = [
  {
    char: "—",
    name: "em dash",
    hint: "recast the sentence: a comma, a colon, a full stop, or parentheses",
  },
];

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

const findings = [];
const targets = [...TARGETS.flatMap(({ dir, exts }) => walk(dir, exts)), ...FILES];
{
  for (const file of targets) {
    const lines = readFileSync(file, "utf8").split("\n");
    lines.forEach((line, i) => {
      for (const b of BANNED) {
        let col = line.indexOf(b.char);
        while (col !== -1) {
          findings.push({
            file: relative(WEBDOC, file),
            line: i + 1,
            col: col + 1,
            name: b.name,
            hint: b.hint,
            excerpt: line.trim().slice(Math.max(0, col - 40), col + 40),
          });
          col = line.indexOf(b.char, col + 1);
        }
      }
    });
  }
}

if (findings.length) {
  const shown = findings.slice(0, 40);
  for (const f of shown) {
    console.error(`${f.file}:${f.line}:${f.col}  ${f.name}: ${f.hint}`);
    console.error(`    …${f.excerpt}…`);
  }
  if (findings.length > shown.length) {
    console.error(`\n… and ${findings.length - shown.length} more.`);
  }
  console.error(`\n${findings.length} banned character(s) in reader-facing text.`);
  process.exit(1);
}

console.log("ok  prose style");
