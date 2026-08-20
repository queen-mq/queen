/**
 * Generate the broker's environment-variable reference from config.rs.
 *
 * Every `env_bool/env_int/env_f64/env_str` call site becomes a row: name,
 * type, default. Nothing is transcribed by hand, so a new knob cannot ship
 * undocumented — an unclassified variable lands in the main table by default.
 *
 * Four curated lists shape presentation only, never content:
 *   GROUPS       which section a variable belongs to
 *   EXPERIMENTAL variables the source itself marks as experiment-only
 *   INERT        variables still read (and logged) but no longer wired to
 *                anything
 *   INHERITS     nested `env_int("A", env_int("B", n))` sites where B is a
 *                different knob whose value A defaults to, not an older name
 *                for A. The parser cannot tell the two apart (both read as a
 *                nested call), and printing an inherited default under "Also
 *                read as" told operators to set a variable that does not
 *                configure the thing they were reading about. Every entry is
 *                checked against the parse below, so the list cannot drift
 *                away from config.rs without failing.
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
  ["Consume and long-poll", (n) => n.startsWith("POP_") || n === "DEFAULT_TIMEOUT" || n === "DEFAULT_SUBSCRIPTION_MODE" || n.startsWith("QUEEN_POP_")],
  ["Storage engine", (n) => n.startsWith("QUEEN_V2_") || n.startsWith("QUEEN_DEDUP") || n.startsWith("QUEEN_ACK_") || n.startsWith("QUEEN_HOTLIST")],
  // The `QUEEN_VEGAS_*` and `QUEEN_SEG_*` knobs this group used to name went
  // out with the Vegas limiter; config.rs now only warns at boot when one is
  // still set, so none of them is an `env_*` call site and this group matched
  // nothing at all. Admission is what governs concurrency now, and its twelve
  // knobs were landing in the unclassified "Other" bucket.
  ["Admission and flow control", (n) => n.startsWith("QUEEN_ADMISSION")],
  ["Background jobs", (n) => n.startsWith("RETENTION") || n.startsWith("STATS_") || n === "RETAINED_BYTES_INTERVAL_MS" || n.startsWith("PARTITION_CLEANUP") || n === "QUEEN_PARTITION_CLEANUP_ENABLED" || n.startsWith("METRICS_")],
  ["Durability spool", (n) => n.startsWith("FILE_BUFFER")],
  ["Security", (n) => n.startsWith("QUEEN_ENCRYPTION") || n === "QUEEN_TENANCY_HEADER"],
  // Roughly forty knobs, and without this group every one of them lands under
  // "Other" next to the pool gauges. `QUEEN_SWEEPER` has no underscore suffix
  // and would fall out of a prefix-only test.
  ["Key/value state, timers and the sweeper", (n) =>
    n.startsWith("QUEEN_KV_") || n.startsWith("QUEEN_TIMERS_") || n === "QUEEN_SWEEPER" || n.startsWith("QUEEN_SWEEPER_")],
  ["Logging", (n) => n === "LOG_LEVEL" || n === "RUST_LOG" || n.startsWith("QUEEN_LOG")],
];

/**
 * Nested defaults that are an inheritance, not an alias.
 *
 * `env_int("QUEEN_ACK_FUSION_SHARDS", env_int("QUEEN_V2_FUSION_SHARDS", 8))`
 * parses identically to `env_int("QUEEN_MESH_PORT", env_int("QUEEN_UDP_NOTIFY_PORT",
 * 6633))`, but the two mean opposite things. `QUEEN_UDP_NOTIFY_PORT` is the
 * older name for the same port and setting either one configures the mesh.
 * `QUEEN_V2_FUSION_SHARDS` is the push fusion shard count, a live knob of its
 * own: the ack fusion and hot-list shard counts merely start from whatever it
 * is set to. Setting it moves three things, and setting the outer name moves
 * only one. `verifyInherits` below asserts each entry against the parse, so
 * the list fails the build rather than outliving the code.
 */
const INHERITS = new Map([
  ["QUEEN_ACK_FUSION_SHARDS", "QUEEN_V2_FUSION_SHARDS"],
  ["QUEEN_HOTLIST_SHARDS", "QUEEN_V2_FUSION_SHARDS"],
]);

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
  {
    name: "QUEEN_ADMISSION_INIT",
    type: "integer",
    def: "96 (two thirds of DB_POOL_SIZE minus QUEEN_ADMISSION_POOL_RESERVE)",
    aliases: [],
    // config.rs :: admission_floor — derived from the pool rather than a
    // literal, so the parser reads back the binding name. Stated here as the
    // value the defaults actually produce (160 - 16, two thirds).
  },
  {
    name: "QUEEN_ADMISSION_MIN",
    type: "integer",
    def: "96 (two thirds of DB_POOL_SIZE minus QUEEN_ADMISSION_POOL_RESERVE)",
    aliases: [],
    // Same derived floor as QUEEN_ADMISSION_INIT.
  },
  {
    name: "QUEEN_KV_POOL_SIZE",
    type: "integer",
    def: "16 (DB_POOL_SIZE / 10, clamped to 4..32)",
    aliases: [],
    // config.rs :: kv_pool_default — the call site's second argument is a
    // function call, so the parser read back `kv_pool_default(pool_size` (an
    // unbalanced fragment, which is what a truncated expression looks like when
    // it reaches a table cell). Derived on purpose, with the same precedent as
    // admission_floor: this pool IS the bulkhead, and a bulkhead sized
    // independently of the pool it protects stops protecting it the moment
    // DB_POOL_SIZE moves.
  },
  {
    name: "QUEEN_KV_REQUIRE_GRANT",
    type: "boolean",
    def: "the value of QUEEN_TENANCY_HEADER (so: false)",
    aliases: [],
    // config.rs binds the default to the resolved `tenancy_header`, and the
    // parser published the binding NAME as the default. Derived so that turning
    // tenancy on makes a missing quota row a denial rather than a permission,
    // while a self-hosted operator — who is their own customer — configures
    // nothing.
  },
  {
    name: "QUEEN_TIMERS_MAX_PAYLOAD_BYTES",
    type: "integer",
    def: "1048576 (1 MiB), further narrowed to the plan's max_payload_bytes",
    aliases: [],
    // The literal in config.rs is only the absolute half of
    // min(1 MiB, plan.max_payload_bytes); the plan half is applied in the proxy.
    // Publishing the literal alone would read as a ceiling a tenant can rely on,
    // and the whole reason the value is a minimum is that a timer becomes a
    // message: an independent ceiling here would be a service entrance past the
    // plan's own payload limit.
  },
  {
    name: "QUEEN_HOTLIST_RESEED_WINDOW_MS",
    type: "integer",
    def: "120000 (max of 4x QUEEN_HOTLIST_RESEED_MS and 120000)",
    aliases: [],
    // config.rs parses a literal 0 and then REWRITES it before load() returns,
    // so the scraped default would publish 0 — which reads as "no window" and
    // is the opposite of what the broker applies. Stated here as the value the
    // defaults actually produce.
  },
];

/** Still parsed and logged at boot, but wired to nothing. */
const INERT = new Map([
  ["QUEEN_V2_FUSION_FRAMES", "kept for env compatibility; no longer a flush trigger (fusion.rs)"],
]);

function groupOf(name) {
  for (const [g, test] of GROUPS) if (test(name)) return g;
  return "Other";
}

/**
 * INHERITS is a claim about config.rs, so it is checked against config.rs.
 * A renamed knob, a removed nesting or a changed inner name fails the
 * generator here instead of publishing a column that points at nothing.
 */
function verifyInherits(vars) {
  for (const [outer, inner] of INHERITS) {
    const v = vars.get(outer);
    if (!v) {
      throw new Error(`INHERITS names ${outer}, which ${CONFIG} no longer defines`);
    }
    if (!v.aliases.includes(inner)) {
      throw new Error(
        `INHERITS says ${outer} inherits its default from ${inner}, but ${CONFIG} nests ` +
          `${v.aliases.length ? v.aliases.join(", ") : "nothing"} there`,
      );
    }
    if (!vars.has(inner)) {
      throw new Error(
        `${inner} is published as an inherited default of ${outer} but has no row of its own, ` +
          `so it is an alias rather than a knob: move it back to "Also read as"`,
      );
    }
  }
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

  verifyInherits(vars);

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
    `The broker is configured entirely through environment variables: ` +
      `${main_.length} of them, listed below with the defaults the code actually applies. ` +
      `Booleans go through one strict parser: an unparseable value is a fatal boot error, ` +
      `while unset and empty both fall back to the default.`,
    "",
    `Two columns record what a variable falls back to. **Also read as** is an older name for ` +
      `the same setting: either name configures the same thing, and the row's name wins when ` +
      `both are set. **Default inherited from** is a different knob whose value this one starts ` +
      `at when it is unset: setting that knob moves this variable and everything else that ` +
      `inherits from it, while setting this variable moves only this one.`,
    "",
  );

  for (const [g] of [...GROUPS, ["Other"]]) {
    const rows = byGroup.get(g);
    if (!rows?.length) continue;
    lines.push(`### ${g}`, "");
    lines.push("| Variable | Type | Default | Default inherited from | Also read as |");
    lines.push("| --- | --- | --- | --- | --- |");
    for (const v of rows) {
      const inherited = INHERITS.has(v.name) ? `\`${INHERITS.get(v.name)}\`` : "";
      const aliases = inherited ? "" : v.aliases.map((a) => `\`${a}\``).join(", ");
      lines.push(`| \`${v.name}\` | ${v.type} | \`${cell(v.def)}\` | ${inherited} | ${aliases} |`);
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
    description: "Every environment variable the broker reads, with the default the code applies.",
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
