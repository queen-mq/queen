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
 *   server/src/handlers/raft.rs    raw `# HELP` / `# TYPE` strings: the three
 *                                  per-queue families `handle_prometheus`
 *                                  writes itself
 *   server/src/rsm/replicator/raft/forward.rs
 *                                  raw `# HELP` / `# TYPE` strings: the batched
 *                                  forwarding counter, rendered by admit.rs
 *   server/src/rsm/facade/real/phase2/link.rs
 *                                  raw `# HELP` / `# TYPE` strings: the standby
 *                                  cluster's families, appended by real.rs and
 *                                  present only on a cluster that is part of a
 *                                  link
 *
 * All are parsed. Every `"queen_*"` string literal in those files is then
 * checked against what was parsed, so a family can never quietly vanish from
 * the reference. The debug counters of server/src/rsm/dbgctr.rs carry no HELP
 * and are left out on purpose.
 *
 * 2026-10-02 (2.0.0-beta.6): the last two sources were missing, so the four
 * families they emit (`queen_queue_pop_lag_milliseconds`,
 * `queen_dlq_depth_by_queue`, `queen_queue_conflated_per_minute`,
 * `queen_raft_forward_total`) were served and not published. And a family can
 * be declared, exposed and never written: DORMANT below lists the ones whose
 * recorder nothing calls in this release, and the table says so.
 */

import { cell, emitPartial, isCheck, repoRead, rustFiles } from "./lib/source.mjs";

const SOURCES = [
  "server/src/metrics.rs",
  "server/src/rsm/timing.rs",
  "server/src/rsm/facade/real.rs",
  "server/src/rsm/admit.rs",
  "server/src/handlers/raft.rs",
  "server/src/rsm/replicator/raft/forward.rs",
  "server/src/rsm/facade/real/phase2/link.rs",
];

/**
 * Families the broker declares and exposes but that nothing in this release
 * writes. They are instruments of the PostgreSQL and segment engines (the
 * timer fire loop and its sweeper, fusion batching, the pop hint mailbox) and
 * a few KV gauges, kept on the exposition so existing dashboards do not break.
 * In 2.0 timers fire through the planner and pops through the consumption
 * engine, and no code feeds these, so their series read 0 (a per-tenant family
 * has no samples at all).
 *
 * Each entry names what WOULD write the family: a recorder method of
 * server/src/metrics.rs (`.name(`), searched in production code outside
 * metrics.rs, or a field (`.name.fetch_add(` and the like), searched in every
 * production file. `verifyDormant` fails generation when it finds one, or when
 * a listed family no longer exists: a family that comes alive must lose its
 * note, and the note must never outlive its family.
 */
// rustfmt breaks a long chain before each `.`, so whitespace may sit between them.
const FIELD_WRITE = "\\s*\\.(?:fetch_add|fetch_sub|fetch_max|store|swap)\\(";
const DORMANT = [
  { writer: "set_kv_expiry", families: ["queen_kv_expired_not_pruned", "queen_kv_expired_not_pruned_capped", "queen_kv_expiry_lag_seconds"] },
  { writer: "set_kv_pool", families: ["queen_kv_pool"] },
  { writer: "kv_singleflight_coalesced", families: ["queen_kv_singleflight_coalesced_total"] },
  { writer: "set_timers_due", families: ["queen_timers_due", "queen_timers_due_capped", "queen_timers_oldest_late_seconds"] },
  { writer: "fire_lag", families: ["queen_timers_fire_lag_seconds", "queen_timers_fire_lag_tenants_dropped_total"] },
  { writer: "timers_fired", families: ["queen_timers_fired_total"] },
  { writer: "timers_dlq", families: ["queen_timers_dlq_total"] },
  { writer: "timers_fire_failure", families: ["queen_timers_fire_failures_total"] },
  { writer: "timers_poisoned", families: ["queen_timers_poisoned_total"] },
  { writer: "sweeper_cycle", families: ["queen_sweeper_cycle_milliseconds", "queen_sweeper_rows_total"] },
  { writer: "sweeper_skip_locked", families: ["queen_sweeper_skip_locked_total"] },
  { writer: "sweeper_phase_skipped", families: ["queen_sweeper_phase_skipped_total"] },
  { writer: "set_sweeper_sleep", families: ["queen_sweeper_sleep_milliseconds"] },
  { writer: "record_batch", families: ["queen_batches_fired_total", "queen_batch_items_fired_total", "queen_fusion_items_per_batch", "queen_batch_rtt_milliseconds"] },
  { field: "pop_targeted", families: ["queen_pop_targeted_total"] },
  { field: "pop_wildcard", families: ["queen_pop_wildcard_total"] },
  { field: "pop_fill_wait", families: ["queen_pop_fill_wait_total"] },
  { field: "pop_fill_wait_us", families: ["queen_pop_fill_wait_microseconds_total"] },
];

/** Per-tenant families that, never written, expose no sample at all. */
const DORMANT_NO_SERIES = new Set(["queen_timers_fire_lag_seconds"]);

/** The production Rust under server/src: not the test or fuzzing trees. */
function productionFiles() {
  return rustFiles("server/src").filter(
    ({ path }) => !/\/tests(\/|_|\.rs)|tests_unit|fuzzing/.test(path),
  );
}

/** Where `entry`'s family would be written, if anywhere. */
function writersOf(entry, files) {
  const re = entry.writer
    ? new RegExp(`\\.${entry.writer}\\(`)
    : new RegExp(`\\.${entry.field}${FIELD_WRITE}`);
  return files
    .filter(({ path }) => !entry.writer || path !== "server/src/metrics.rs")
    .filter(({ text }) => re.test(text))
    .map(({ path }) => path);
}

function verifyDormant(families) {
  const files = productionFiles();
  const notes = new Map();
  for (const entry of DORMANT) {
    const writers = writersOf(entry, files);
    if (writers.length) {
      throw new Error(
        `DORMANT is stale: \`${entry.writer ?? entry.field}\` (which feeds ${entry.families.join(", ")}) ` +
          `is now written from ${writers.join(", ")}. Re-read it and take its families off the ` +
          `DORMANT list in this script.`,
      );
    }
    for (const n of entry.families) {
      if (!families.has(n)) {
        throw new Error(`DORMANT names ${n}, which the broker no longer declares: remove it.`);
      }
      notes.set(
        n,
        DORMANT_NO_SERIES.has(n)
          ? "Nothing in this release writes it, so it has no samples."
          : "Nothing in this release writes it, so it reads 0.",
      );
    }
  }
  return notes;
}

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
  ["Forwarding between nodes", (n) => n === "queen_raft_forward_total"],
  // Only on a cluster that is a standby, was one, or is read by one.
  ["Standby cluster", (n) => n.startsWith("queen_link_")],
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

  const dormant = verifyDormant(families);

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
      `its pipeline. Scrape every node. The pipeline timing, admission and forwarding families ` +
      `are skipped when \`QUEEN_RAFT_METRICS\` is \`0\`, \`false\`, \`off\` or \`no\`; it is on by ` +
      `default. The per-queue families are written only for queues that have something to report. ` +
      `${dormant.size} families are declared but not written by this release; their Help says so.`,
    "",
  );

  for (const [g] of [...GROUPS, ["Other"]]) {
    const rows = byGroup.get(g);
    if (!rows?.length) continue;
    lines.push(`### ${g}`, "");
    lines.push("| Family | Type | Help |");
    lines.push("| --- | --- | --- |");
    for (const f of rows) {
      const help = dormant.has(f.name) ? `${f.help}. ${dormant.get(f.name)}` : f.help;
      lines.push(`| \`${f.name}\` | ${cell(f.type)} | ${cell(help)} |`);
    }
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
