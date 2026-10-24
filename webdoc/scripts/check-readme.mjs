/**
 * The README's code is the code that ran.
 *
 * The README carries a condensed, JavaScript-only version of the homepage's
 * five-minute guide. Each of its code blocks is announced by a marker on the
 * line before it:
 *
 *   <!-- guide: push -->
 *   ```js
 *   ...
 *   ```
 *
 * and must equal that step's region in the JavaScript guide program,
 * `examples/tutorials/js/00-five-minutes.mjs`, as `lib/guide-regions.mjs` cuts
 * it out: the same text the homepage shows. The program runs every step
 * against a broker (`examples/tutorials/run.sh js`), so a README that matches
 * it cannot show code that does not work. Edit the program, then refresh the
 * README with:
 *
 *   node scripts/check-readme.mjs --write
 *
 * Another markdown file with the same markers (the book's `book/index.md`) is
 * checked or written by naming it: `node scripts/check-readme.mjs --write book/index.md`.
 */
import { readFileSync, writeFileSync } from "node:fs";
import { isAbsolute, join, relative } from "node:path";
import { REPO } from "./lib/source.mjs";
import { guideRegions } from "./lib/guide-regions.mjs";

const target = process.argv.slice(2).find((a) => !a.startsWith("--"));
const README = target ? (isAbsolute(target) ? target : join(process.cwd(), target)) : join(REPO, "README.md");
const NAME = relative(REPO, README);
const PROGRAM = "examples/tutorials/js/00-five-minutes.mjs";
const MARKER = /^<!--\s*guide:\s*([a-z][a-z-]*)\s*-->\s*$/;
const OPEN = /^```(js|javascript)\s*$/;
const write = process.argv.includes("--write");

const regions = guideRegions(readFileSync(join(REPO, PROGRAM), "utf8"), PROGRAM);
const lines = readFileSync(README, "utf8").split("\n");
const problems = [];
let checked = 0;
let rewritten = 0;

for (let i = 0; i < lines.length; i++) {
  const marker = lines[i].match(MARKER);
  if (!marker) continue;
  const step = marker[1];
  const at = `${NAME}:${i + 1}`;
  const region = regions.get(step);
  if (region === undefined) {
    problems.push(`${at}: no guide:start(${step}) region in ${PROGRAM}`);
    continue;
  }
  if (!OPEN.test(lines[i + 1] ?? "")) {
    problems.push(`${at}: the marker must sit on the line right before a \`\`\`js fence`);
    continue;
  }
  let close = i + 2;
  while (close < lines.length && lines[close] !== "```") close++;
  if (close >= lines.length) {
    problems.push(`${at}: the code block after the marker never closes`);
    continue;
  }
  checked++;
  const shown = lines.slice(i + 2, close).join("\n");
  if (shown === region) continue;
  if (write) {
    lines.splice(i + 2, close - (i + 2), ...region.split("\n"));
    rewritten++;
    continue;
  }
  const a = shown.split("\n");
  const b = region.split("\n");
  let n = 0;
  while (n < a.length && n < b.length && a[n] === b[n]) n++;
  problems.push(
    `${at}: the ${step} block differs from ${PROGRAM} at its line ${n + 1}:\n` +
      `    README:  ${a[n] ?? "(nothing)"}\n` +
      `    program: ${b[n] ?? "(nothing)"}\n` +
      `  run \`node webdoc/scripts/check-readme.mjs --write${target ? " " + NAME : ""}\` to copy the program's regions in`,
  );
}

if (checked === 0 && problems.length === 0) {
  problems.push(`${NAME}: no <!-- guide: <step> --> markers; its code would go unchecked`);
}

if (write && rewritten) {
  writeFileSync(README, lines.join("\n"));
  console.log(`wrote ${rewritten} block(s) of ${NAME} from ${PROGRAM}`);
}

if (problems.length) {
  for (const p of problems) console.error(p);
  console.error(`\n${problems.length} problem(s) with the code of ${NAME}.`);
  process.exit(1);
}
console.log(`ok  ${NAME === "README.md" ? "README" : NAME} code (${checked} blocks equal to the regions of ${PROGRAM})`);
