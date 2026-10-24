// Write every example region of the JavaScript programs as a fenced
// markdown file, one per step, for build.sh to highlight with pandoc:
//
//   node book/latex/code.mjs <out-dir>     -> <out-dir>/<step>.md
//
// The regions are cut by the same code the homepage and the README use
// (webdoc/scripts/lib/guide-regions.mjs), so \guide{push} in a chapter shows
// exactly what the homepage shows, and what examples/tutorials/run.sh runs.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { guideRegions } from "../../webdoc/scripts/lib/guide-regions.mjs";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
// The homepage's five-minute guide, then the book's own examples. A step id must
// be unique across both.
const PROGRAMS = ["examples/tutorials/js/00-five-minutes.mjs", "examples/tutorials/js/book-client-guide.mjs",
  "examples/tutorials/js/book-use-cases.mjs"];
const out = process.argv[2];
if (!out) throw new Error("usage: node book/latex/code.mjs <out-dir>");
mkdirSync(out, { recursive: true });

const seen = new Map();
for (const program of PROGRAMS) {
  const regions = guideRegions(readFileSync(join(ROOT, program), "utf8"), program);
  for (const [step, code] of regions) {
    if (seen.has(step)) throw new Error(`guide:start(${step}) is in both ${seen.get(step)} and ${program}`);
    seen.set(step, program);
    writeFileSync(join(out, `${step}.md`), "```js\n" + code + "\n```\n");
  }
}
console.log(`${seen.size} steps from ${PROGRAMS.join(", ")}`);
