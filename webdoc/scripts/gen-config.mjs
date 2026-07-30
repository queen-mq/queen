/**
 * Generate the broker's environment-variable reference from config.rs.
 *
 * Every `env_bool/env_int/env_f64/env_str` call site becomes a row: name,
 * type, default. Nothing is transcribed by hand, so a new knob cannot ship
 * undocumented — an unclassified variable lands in the main table by default.
 *
 * Three curated lists shape presentation only, never content:
 *   GROUPS       which section a variable belongs to
 *   EXPERIMENTAL variables the source itself marks as experiment-only
 *   INERT        variables still read (and logged) but no longer wired to
 *                anything
 * A variable in EXPERIMENTAL or INERT is still published — in its own table,
 * labelled — because silence is how the previous docs site went stale.
 */

import { cell, emitPartial, isCheck, repoRead } from "./lib/source.mjs";

const CONFIG = "server/src/config.rs";
const EXTRA_SOURCES = ["server/src/fusion.rs (fusion-only overrides)"];

// ---------------------------------------------------------------------------
// Parse
// ---------------------------------------------------------------------------

const TYPE_OF = { env_bool: "boolean", env_int: "integer", env_f64: "number", env_str: "string" };

/** Read a balanced argument list starting at the char after `(`. */
function readArgs(text, openParenIdx) {
  let depth = 0;
  for (let i = openParenIdx; i < text.length; i++) {
    const ch = text[i];
    if (ch === "(") depth++;
    else if (ch === ")") {
      depth--;
      if (depth === 0) return text.slice(openParenIdx + 1, i);
    }
  }
  throw new Error("unbalanced parens in config.rs");
}

/** Split a Rust argument list on top-level commas. */
function splitArgs(s) {
  const out = [];
  let depth = 0;
  let cur = "";
  let inStr = false;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (inStr) {
      cur += ch;
      if (ch === '"' && s[i - 1] !== "\\") inStr = false;
      continue;
    }
    if (ch === '"') {
      inStr = true;
      cur += ch;
    } else if (ch === "(" || ch === "[") {
      depth++;
      cur += ch;
    } else if (ch === ")" || ch === "]") {
      depth--;
      cur += ch;
    } else if (ch === "," && depth === 0) {
      out.push(cur.trim());
      cur = "";
    } else {
      cur += ch;
    }
  }
  if (cur.trim()) out.push(cur.trim());
  return out;
}

function parseEnvVars(text) {
  const vars = new Map();
  const re = /\b(env_bool|env_int|env_f64|env_str)\(/g;
  let m;
  while ((m = re.exec(text))) {
    const kind = m[1];
    const open = m.index + m[1].length;
    const args = splitArgs(readArgs(text, open));
    const nameLit = args[0]?.match(/^"([^"]+)"$/);
    if (!nameLit) continue;
    const name = nameLit[1];
    let def = args[1] ?? "";

    // Nested default: env_int("A", env_int("B", 6633)) — B is an alias that is
    // consulted when A is unset, and the innermost literal is the real default.
    const aliases = [];
    let inner = def;
    while (/^env_(bool|int|f64|str)\(/.test(inner)) {
      const innerArgs = splitArgs(readArgs(inner, inner.indexOf("(")));
      const innerName = innerArgs[0]?.match(/^"([^"]+)"$/);
      if (innerName) aliases.push(innerName[1]);
      inner = innerArgs[1] ?? "";
    }
    def = inner;

    // Strip trailing Rust noise: `.max(1) as usize`, `.to_string()`, casts.
    def = def
      .replace(/\)\s*$/, "")
      .replace(/\.max\([^)]*\)/g, "")
      .replace(/\.min\([^)]*\)/g, "")
      .replace(/\s+as\s+\w+.*$/, "")
      .trim();
    if (def === '""') def = "(empty)";
    else def = def.replace(/^"|"$/g, "");

    // First call site wins; later ones are re-reads of the same knob.
    if (!vars.has(name)) vars.set(name, { name, type: TYPE_OF[kind], def, aliases });
    else if (aliases.length && !vars.get(name).aliases.length) vars.get(name).aliases = aliases;
  }
  return vars;
}

/** `std::env::var("X")` sites outside the helpers — fusion's local overrides. */
function parseRawEnvVars(text) {
  const names = new Set();
  const re = /std::env::var\(\s*"([A-Z][A-Z0-9_]*)"\s*\)/g;
  let m;
  while ((m = re.exec(text))) names.add(m[1]);
  return names;
}

// ---------------------------------------------------------------------------
// Curated presentation
// ---------------------------------------------------------------------------

const GROUPS = [
  ["Server", (n) => ["PORT", "QUEEN_SERVER_ID", "HOSTNAME", "QUEEN_MAX_BODY_BYTES", "QUEEN_APPLY_SCHEMA"].includes(n)],
  ["PostgreSQL", (n) => n.startsWith("PG_") || n.startsWith("DB_") || n === "QUEEN_STMT_TIMEOUT_MS"],
  ["Authentication", (n) => n.startsWith("JWT_")],
  ["Multi-broker mesh", (n) => n.startsWith("QUEEN_MESH_") || n.startsWith("QUEEN_SYNC_") || n.startsWith("QUEEN_UDP_") || n === "QUEEN_CACHE_REFRESH_INTERVAL_MS"],
  ["Consume and long-poll", (n) => n.startsWith("POP_") || n === "DEFAULT_TIMEOUT" || n.startsWith("QUEEN_POP_")],
  ["Storage engine", (n) => n.startsWith("QUEEN_V2_") || n.startsWith("QUEEN_DEDUP") || n.startsWith("QUEEN_ACK_") || n.startsWith("QUEEN_HOTLIST")],
  ["Flow control", (n) => n.startsWith("QUEEN_VEGAS") || n.startsWith("QUEEN_LIMIT") || n.startsWith("QUEEN_SEG_PUSH_") || n.startsWith("QUEEN_SEG_POP_")],
  ["Background jobs", (n) => n.startsWith("RETENTION") || n.startsWith("STATS_") || n.startsWith("PARTITION_CLEANUP") || n.startsWith("METRICS_")],
  ["Durability spool", (n) => n.startsWith("FILE_BUFFER")],
  ["Security", (n) => n.startsWith("QUEEN_ENCRYPTION") || n === "QUEEN_TENANCY_HEADER"],
  ["Logging", (n) => n === "LOG_LEVEL" || n === "RUST_LOG" || n.startsWith("QUEEN_LOG")],
];

/** The source itself calls these experiment knobs, not product contracts. */
const EXPERIMENTAL = new Map([
  ["QUEEN_V2_FUSION_MIN_FRAMES", "fusion.rs: “a knob for experiments, not a product contract”"],
  ["QUEEN_V2_FUSION_MIN_WAIT_MS", "fusion.rs: “a knob for experiments, not a product contract”"],
  ["QUEEN_V2_BUNDLE_MAX", "fusion.rs: internal override only"],
  ["QUEEN_V2_FUSION_MAX_INFLIGHT", "fusion.rs: internal override only"],
]);

/**
 * Variables resolved outside the `env_*` helpers, so the parser cannot see
 * them, plus the one whose config.rs call site is a boot-log placeholder
 * rather than the real default.
 */
const EXTRA_VARS = [
  {
    name: "PG_DATABASE",
    type: "string",
    def: "postgres",
    aliases: ["PG_DB"],
    // config.rs :: resolve_db_name — an explicitly empty value falls through.
  },
  {
    name: "QUEEN_MAX_BODY_BYTES",
    type: "integer",
    def: "67108864 (64 MiB)",
    aliases: [],
    // Applied in main.rs as a DefaultBodyLimit layer; config.rs only echoes it
    // into the boot log.
  },
];

/** Still parsed and logged at boot, but wired to nothing. */
const INERT = new Map([
  ["QUEEN_V2_FUSION_FRAMES", "kept for env compatibility; no longer a flush trigger (fusion.rs)"],
  ["RETENTION_PARALLELISM", "read and ignored; retention runs one bounded step at a time"],
  ["PARTITION_CLEANUP_DAYS", "read and ignored; partitions are never auto-deleted"],
]);

function groupOf(name) {
  for (const [g, test] of GROUPS) if (test(name)) return g;
  return "Other";
}

// ---------------------------------------------------------------------------

function main() {
  const check = isCheck();
  const configText = repoRead(CONFIG);
  const fusionText = repoRead("server/src/fusion.rs");

  const vars = parseEnvVars(configText);
  if (vars.size < 60) {
    throw new Error(`only parsed ${vars.size} env vars out of ${CONFIG} — the parser is broken`);
  }

  // fusion.rs reads a few knobs directly rather than through Config.
  for (const name of parseRawEnvVars(fusionText)) {
    if (!vars.has(name)) vars.set(name, { name, type: "integer", def: "see notes", aliases: [] });
  }
  for (const v of EXTRA_VARS) vars.set(v.name, v);

  const all = [...vars.values()].sort((a, b) => a.name.localeCompare(b.name));
  const main_ = all.filter((v) => !EXPERIMENTAL.has(v.name) && !INERT.has(v.name));
  const experimental = all.filter((v) => EXPERIMENTAL.has(v.name));
  const inert = all.filter((v) => INERT.has(v.name));

  const byGroup = new Map();
  for (const v of main_) {
    const g = groupOf(v.name);
    if (!byGroup.has(g)) byGroup.set(g, []);
    byGroup.get(g).push(v);
  }

  const lines = [];
  lines.push(
    `The broker is configured entirely through environment variables — ` +
      `${main_.length} of them, listed below with the defaults the code actually applies. ` +
      `Booleans go through one strict parser: an unparseable value is a fatal boot error, ` +
      `while unset and empty both fall back to the default.`,
    "",
  );

  for (const [g] of [...GROUPS, ["Other"]]) {
    const rows = byGroup.get(g);
    if (!rows?.length) continue;
    lines.push(`### ${g}`, "");
    lines.push("| Variable | Type | Default | Also read as |");
    lines.push("| --- | --- | --- | --- |");
    for (const v of rows) {
      lines.push(`| \`${v.name}\` | ${v.type} | \`${cell(v.def)}\` | ${v.aliases.length ? v.aliases.map((a) => `\`${a}\``).join(", ") : "—"} |`);
    }
    lines.push("");
  }

  if (experimental.length) {
    lines.push(`### Experiment-only`, "");
    lines.push(
      `These exist to run experiments against the storage engine. The source marks them as ` +
        `such, they are not part of any compatibility promise, and a deployment should not set them.`,
      "",
    );
    lines.push("| Variable | Default | Why it is not a product knob |");
    lines.push("| --- | --- | --- |");
    for (const v of experimental) {
      lines.push(`| \`${v.name}\` | \`${cell(v.def)}\` | ${cell(EXPERIMENTAL.get(v.name))} |`);
    }
    lines.push("");
  }

  if (inert.length) {
    lines.push(`### Read but inert`, "");
    lines.push(
      `The broker still parses these and still prints them in its boot configuration block, ` +
        `so they look live in a log. They change nothing.`,
      "",
    );
    lines.push("| Variable | Default | Status |");
    lines.push("| --- | --- | --- |");
    for (const v of inert) {
      lines.push(`| \`${v.name}\` | \`${cell(v.def)}\` | ${cell(INERT.get(v.name))} |`);
    }
    lines.push("");
  }

  const res = emitPartial({
    name: "broker-config",
    title: "broker environment variables",
    sources: [CONFIG, ...EXTRA_SOURCES],
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
