/**
 * Render the benchmark figures from the archived artifacts.
 *
 * A thin wrapper around `charts.py`: the plotting lives in Python because
 * matplotlib is what the benchmark sessions themselves already use, and the
 * figures should be produced by the same tool that produced the runs' own
 * reports. This script exists so the figures join the same drift discipline as
 * every other generated artifact — `--check` re-renders into a temporary
 * directory and compares, so CI fails when a committed figure no longer matches
 * the data it claims to show.
 *
 * Requires python3 with matplotlib. If it is missing the script says so and
 * fails rather than silently leaving stale figures in place.
 */

import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { WEBDOC, isCheck } from "./lib/source.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const SCRIPT = join(HERE, "charts.py");
const OUT = join(WEBDOC, "public", "charts");

function render(dir) {
  const r = spawnSync("python3", [SCRIPT, "--out", dir], { encoding: "utf8" });
  if (r.error?.code === "ENOENT") {
    throw new Error("python3 not found — the benchmark figures need python3 with matplotlib.");
  }
  if (r.status !== 0) {
    const err = (r.stderr || "").trim();
    if (/ModuleNotFoundError.*matplotlib/.test(err)) {
      throw new Error("matplotlib is not installed — `pip install matplotlib` to render the figures.");
    }
    throw new Error(`charts.py failed:\n${err}`);
  }
  return (r.stdout || "").trim();
}

function readAll(dir) {
  const out = new Map();
  for (const name of readdirSync(dir)) out.set(name, readFileSync(join(dir, name), "utf8"));
  return out;
}

function main() {
  const check = isCheck();

  if (!check) {
    rmSync(OUT, { recursive: true, force: true });
    mkdirSync(OUT, { recursive: true });
    const summary = render(OUT);
    return { written: true, drifted: false, title: `charts: ${summary}` };
  }

  const tmp = mkdtempSync(join(tmpdir(), "queen-charts-"));
  try {
    const summary = render(tmp);
    const fresh = readAll(tmp);
    let current = new Map();
    try {
      current = readAll(OUT);
    } catch {
      /* missing counts as drift */
    }
    let drifted = fresh.size !== current.size;
    for (const [name, content] of fresh) {
      if (current.get(name) !== content) drifted = true;
    }
    return { drifted, title: `charts: ${summary}` };
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

const result = main();
if (result.drifted) {
  console.error(`DRIFT: ${OUT} is behind the benchmark artifacts`);
  process.exit(1);
}
console.log(`${result.written ? "wrote" : "ok"}  ${result.title}`);
