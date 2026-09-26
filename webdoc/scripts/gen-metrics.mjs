/**
 * Generate the Prometheus family reference from the code that emits it.
 *
 * `GET /metrics/prometheus` (`handle_prometheus` in server/src/handlers/raft.rs)
 * concatenates four emitters, in several shapes:
 *   server/src/metrics.rs          `ht(&mut s, name, help, type)` helper calls
 *   server/src/rsm/timing.rs       `(name, help, &histogram)` tuples rendered as
 *                                  summaries, split `# HELP` / `# TYPE` literals,
 *                                  and one `let pname = "..."` template
 *   server/src/rsm/facade/real.rs  raw `# HELP` / `# TYPE` strings
 *   server/src/rsm/admit.rs        raw `# HELP` / `# TYPE` strings
 *
 * All are parsed. Every `"queen_*"` string literal in those files is then
 * checked against what was parsed, so a family can never quietly vanish from
 * the reference. The debug counters of server/src/rsm/dbgctr.rs carry no HELP
 * and are left out on purpose.
 */

import { cell, emitPartial, isCheck, repoRead } from "./lib/source.mjs";

const SOURCES = [
  "server/src/metrics.rs",
  "server/src/rsm/timing.rs",
  "server/src/rsm/facade/real.rs",
  "server/src/rsm/admit.rs",
];

function collect(text, families) {
  const put = (name, help, type) => {
    if (!families.has(name)) families.set(name, { name, help, type });
  };

  // ht(&mut s, "name", "help", "type"), and the `ht(s, …)` spelling used where
  // the emitter is a closure parameter rather than a free function. Requiring
  // the `&mut` was not a stricter parse, it was a blind spot: 24 families of the
  // kv/timers/sweeper block are emitted through a `&dyn Fn(&mut String, …)`
  // argument already named `ht`, so they parsed as nothing and were published
  // with empty Type and Help cells.
  // rustfmt splits a long call over lines and leaves a trailing comma.
  for (const m of text.matchAll(/\bht\(\s*(?:&mut\s+)?\w+\s*,\s*"([^"]+)"\s*,\s*"([^"]*)"\s*,\s*"(\w+)"\s*,?\s*\)/g)) {
    put(m[1], m[2], m[3]);
  }

  // An array of (name, help, &counter) tuples fed to `ht(name, help, "type")`
  // by the loop right after it, or to `render_summary`, which always writes
  // `# TYPE {name} summary`. The trailing comma is rustfmt's, on a tuple split
  // over several lines.
  for (const m of text.matchAll(/\(\s*"(queen_[a-z_]+)"\s*,\s*"([^"]*)"\s*,\s*&[\w.]+\s*,?\s*\)/g)) {
    const after = text.slice(m.index, m.index + 9000);
    const typed = after.match(/ht\(\s*&mut\s+\w+\s*,\s*\w+\s*,\s*\w+\s*,\s*"(\w+)"\s*\)/);
    const summary = /render_summary\(/.test(after);
    put(m[1], m[2], typed ? typed[1] : summary ? "summary" : "");
  }

  // "# HELP name help\n# TYPE name type", in one literal.
  for (const m of text.matchAll(/# HELP (queen_[a-z_]+) ([^\\"]*)\\n# TYPE \1 (\w+)/g)) {
    put(m[1], m[2].trim(), m[3]);
  }

  // The same pair split over two literals: "# HELP name help\n" then, a
  // statement later, "# TYPE name type\n".
  for (const m of text.matchAll(/# HELP (queen_[a-z_]+) ([^\\"]*)\\n"[\s\S]{0,240}?# TYPE \1 (\w+)/g)) {
    put(m[1], m[2].trim(), m[3]);
  }

  // `let pname = "queen_x";` followed by a `# HELP {pname} help\n# TYPE {pname} type`
  // template.
  for (const m of text.matchAll(/let (\w+) = "(queen_[a-z_]+)";[\s\S]{0,400}?# HELP \{\1\} ([^\\"]*)\\n# TYPE \{\1\} (\w+)/g)) {
    put(m[2], m[3].trim(), m[4]);
  }

  // "# HELP {ident} help\n# TYPE {ident} type" — templated over a nearby array
  // of family names. Attach the help/type to every queen_* name in the closest
  // preceding array literal.
  for (const m of text.matchAll(/# HELP \{(\w+)\} ([^\\"]*)\\n# TYPE \{\1\} (\w+)/g)) {
    const [, , help, type] = m;
    const before = text.slice(Math.max(0, m.index - 900), m.index);
    const arrStart = before.lastIndexOf("[");
    if (arrStart === -1) continue;
    const names = [...before.slice(arrStart).matchAll(/"(queen_[a-z_]+)"/g)].map((x) => x[1]);
    for (const n of names) put(n, help.trim(), type);
  }
}

/**
 * Every `queen_*` literal in the file, minus the ones that are not family names.
 *
 * A literal ending in `_` is a PREFIX, not a family. The broker used to carry
 * three of them, `queen_kv_`, `queen_timers_` and `queen_sweeper_`, to probe its
 * own exposition and assert that a switched-off feature emitted nothing at all;
 * those probes went with the boot flags. The filter stays because a prefix
 * published as a family is an invented metric with no type and no help, on the
 * reference page that exists so nobody has to invent one.
 */
function universe(text) {
  return new Set(
    [...text.matchAll(/"(queen_[a-z_]+)"/g)].map((m) => m[1]).filter((n) => !n.endsWith("_")),
  );
}

const GROUPS = [
  ["Process (this broker instance)", (n) => n.startsWith("queen_process_") || ["queen_uptime_seconds", "queen_event_loop_lag_avg_milliseconds", "queen_parked_long_polls", "queen_malloc_bytes"].includes(n)],
  ["Replicated log and storage", (n) =>
    n.startsWith("queen_raft_store_") || n.startsWith("queen_raft_index") || n === "queen_raft_inflight" ||
    n === "queen_raft_proposals_total" || n === "queen_raft_log_storage" || n === "queen_raft_storage_full"],
  ["Admission", (n) => n.startsWith("queen_raft_admit_")],
  ["Pipeline timing", (n) => n.startsWith("queen_raft_")],
  ["Per-queue rates and depth", (n) => n.startsWith("queen_queue_") || n.startsWith("queen_dlq_")],
  ["Engine internals", (n) => n.startsWith("queen_seg_") || n.startsWith("queen_batch") || n.startsWith("queen_fusion") || n.startsWith("queen_pop_")],
  ["Ephemeral queues", (n) => n.startsWith("queen_ephemeral_")],
  // Emitted by every broker, including one that has never seen a key or a timer:
  // the exposition gates that used to hide this block are gone with the boot flags.
  ["Key/value state, timers and the sweeper", (n) =>
    n.startsWith("queen_kv_") || n.startsWith("queen_timers_") || n.startsWith("queen_sweeper_")],
];

function groupOf(n) {
  for (const [g, t] of GROUPS) if (t(n)) return g;
  return "Other";
}

function main() {
  const check = isCheck();
  const texts = SOURCES.map((s) => repoRead(s));

  const families = new Map();
  for (const t of texts) collect(t, families);

  const all = new Set();
  for (const t of texts) for (const n of universe(t)) all.add(n);

  // A family whose HELP/TYPE did not parse used to be published with empty
  // cells behind a `console.warn` and an exit status of zero, which is a silent
  // failure by any definition: the reference kept its row count and lost its
  // content, and nothing in CI noticed for as long as anyone cared to look. It
  // is a hard stop now. The remedy is never to add a row here by hand — it is to
  // declare the family the way its neighbours do, or to teach `collect()` the
  // new shape and say in a comment which shape that is.
  const undocumented = [...all].filter((n) => !families.has(n));
  if (undocumented.length) {
    throw new Error(
      `${undocumented.length} metric famil${undocumented.length === 1 ? "y is" : "ies are"} ` +
        `emitted as a literal but expose no parsable HELP/TYPE, so the reference would publish ` +
        `${undocumented.length === 1 ? "it" : "them"} with empty cells: ${undocumented.join(", ")}. ` +
        `Declare the family the way the others in metrics.rs are declared, or extend collect().`,
    );
  }

  if (families.size < 25) {
    throw new Error(`only parsed ${families.size} metric families — the parser is broken`);
  }

  const byGroup = new Map();
  for (const f of [...families.values()].sort((a, b) => a.name.localeCompare(b.name))) {
    const g = groupOf(f.name);
    if (!byGroup.has(g)) byGroup.set(g, []);
    byGroup.get(g).push(f);
  }

  const lines = [];
  lines.push(
    `\`GET /metrics/prometheus\` exposes **${families.size} families**. Every one describes ` +
      `the node that answered: \`queen_process_*\` counts what this node did since it ` +
      `started, and the \`queen_raft_*\` families describe its replicated log, its store and ` +
      `its pipeline. Scrape every node. The pipeline timing and admission families are ` +
      `skipped when \`QUEEN_RAFT_METRICS\` is \`0\`, \`false\`, \`off\` or \`no\`; it is on by default.`,
    "",
  );

  for (const [g] of [...GROUPS, ["Other"]]) {
    const rows = byGroup.get(g);
    if (!rows?.length) continue;
    lines.push(`### ${g}`, "");
    lines.push("| Family | Type | Help |");
    lines.push("| --- | --- | --- |");
    for (const f of rows) lines.push(`| \`${f.name}\` | ${cell(f.type)} | ${cell(f.help)} |`);
    lines.push("");
  }

  const res = emitPartial({
    name: "broker-metrics",
    title: "Prometheus families",
    description: "Every Prometheus metric family the broker exposes, with its type and help text.",
    sources: SOURCES,
    body: lines.join("\n"),
    check,
  });
  return res;
}

const result = main();
if (result.drifted) {
  console.error(`DRIFT: ${result.file} is behind its source`);
  process.exit(1);
}
console.log(`${result.drifted === false ? "ok" : "wrote"}  ${result.title}`);
