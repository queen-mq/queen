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

const TARGETS = [
  { dir: join(WEBDOC, "src", "content", "docs"), exts: [".mdx", ".md"] },
  { dir: join(WEBDOC, "src", "pages"), exts: [".astro", ".ts"] },
  { dir: join(WEBDOC, "src", "components"), exts: [".astro"] },
  { dir: join(WEBDOC, "public", "openapi"), exts: [".json"] },
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
for (const { dir, exts } of TARGETS) {
  for (const file of walk(dir, exts)) {
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
