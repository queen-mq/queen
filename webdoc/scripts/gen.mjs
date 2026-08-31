/**
 * Run every generator. `--check` compares instead of writing and exits
 * non-zero on drift, which is what CI runs: a page built from source must
 * never be published behind that source.
 */

import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const check = process.argv.includes("--check");

const GENERATORS = [
  "gen-routes.mjs",
  "gen-config.mjs",
  "gen-metrics.mjs",
  "gen-proxy-routes.mjs",
  "gen-openapi.mjs",
  "gen-kafka-apis.mjs",
  "gen-sqs-actions.mjs",
  "gen-charts.mjs",
  "gen-snippets.mjs",
];

let failed = 0;
for (const g of GENERATORS) {
  const args = [join(HERE, g)];
  if (check) args.push("--check");
  const r = spawnSync(process.execPath, args, { stdio: "inherit" });
  if (r.status !== 0) failed++;
}

if (failed) {
  console.error(
    check
      ? `\n${failed} generated artifact(s) are behind their source. Run \`pnpm --dir webdoc gen\` and commit the result.`
      : `\n${failed} generator(s) failed.`,
  );
  process.exit(1);
}
console.log(check ? "\nall generated artifacts are current" : "\nall generated artifacts written");
