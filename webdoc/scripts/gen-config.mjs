/**
 * Generate the broker's environment-variable reference.
 *
 * Every `env_bool/env_int/env_f64/env_str/env_pct` call site in config.rs
 * becomes a row: name, type, default. Nothing is transcribed by hand, so a new
 * knob there cannot ship undocumented: an unclassified variable lands in the
 * "Other" table by default.
 *
 * The raft engine, the embedded proxy switch and a few request ceilings read
 * their variables where they are used (`std::env::var` in the rsm modules and
 * handlers) rather than through config.rs. Those are listed in EXTRA_VARS,
 * each with the file that reads it and the default that file applies, and
 * `verifyExtraSources` fails the generator when a listed file no longer names
 * the variable, so the curated list cannot outlive the code.
 *
 * Two curated lists shape presentation only, never content:
 *   GROUPS    which section a variable belongs to
 *   INHERITS  nested `env_int("A", env_int("B", n))` sites where B is a
 *             different knob whose value A defaults to, not an older name for
 *             A. The parser cannot tell the two apart (both read as a nested
 *             call), so every entry is checked against the parse below.
 */

import { cell, emitPartial, isCheck, repoRead } from "./lib/source.mjs";

const CONFIG = "server/src/config.rs";

// ---------------------------------------------------------------------------
// Parse
// ---------------------------------------------------------------------------

const TYPE_OF = {
  env_bool: "boolean",
  env_int: "integer",
  env_f64: "number",
  env_str: "string",
  env_pct: "number (percent)",
};

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
  const re = /\b(env_bool|env_int|env_f64|env_str|env_pct)\(/g;
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
    while (/^&?env_(bool|int|f64|str|pct)\(/.test(inner)) {
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

// ---------------------------------------------------------------------------
// Curated presentation
// ---------------------------------------------------------------------------

const GROUPS = [
  ["Server", (n) => ["PORT", "QUEEN_BIND_ADDR", "QUEEN_SERVER_ID", "HOSTNAME", "QUEEN_MAX_BODY_BYTES"].includes(n)],
  // Before the raft group: the timer fire and the KV sweep are `QUEEN_RAFT_*`
  // names, but an operator looks for them next to the rest of KV and timers.
  ["Key/value state, timers and the sweeper", (n) =>
    n.startsWith("QUEEN_KV_") || n.startsWith("QUEEN_TIMERS_") || n.startsWith("QUEEN_SWEEPER_") ||
    n.startsWith("QUEEN_RAFT_TIMER_") || n.startsWith("QUEEN_RAFT_KV_SWEEP") || n === "QUEEN_STMT_TIMEOUT_MS"],
  ["Retention and background jobs", (n) =>
    n.startsWith("RETENTION") || n.startsWith("PARTITION_CLEANUP") || n === "QUEEN_PARTITION_CLEANUP_ENABLED" ||
    n === "QUEEN_RAFT_RETENTION_VISIT" || n === "QUEEN_RAFT_TXN_WINDOW_MIN_S" || n === "QUEEN_RAFT_TRACE_RETENTION_S" ||
    n.startsWith("METRICS_") || n.startsWith("QUEEN_DASH_")],
  ["Storage and replication", (n) => n.startsWith("QUEEN_RAFT_") || n === "QUEEN_QLOG_SHARDS" || n === "QUEEN_TENANT_GROUPS"],
  ["Authentication", (n) => n.startsWith("JWT_")],
  ["Consume and long-poll", (n) => n.startsWith("POP_") || n === "DEFAULT_TIMEOUT" || n === "DEFAULT_SUBSCRIPTION_MODE"],
  ["Security and tenancy", (n) => n.startsWith("QUEEN_ENCRYPTION") || n === "QUEEN_TENANCY_HEADER"],
  ["Ephemeral queues", (n) => n.startsWith("QUEEN_EPHEMERAL_")],
  ["Embedded proxy and Kafka facade", (n) => n === "QUEEN_PROXY_EMBEDDED" || n === "QUEEN_PROXY_PORT" || n.startsWith("QUEEN_KAFKA_")],
  ["Logging", (n) => n === "LOG_LEVEL" || n === "RUST_LOG" || n.startsWith("QUEEN_LOG")],
];

/**
 * Nested defaults that are an inheritance, not an alias. None today: the one
 * nested site left in config.rs, `env_int("DEFAULT_TIMEOUT",
 * env_int("POP_DEFAULT_TIMEOUT_MS", 30000))`, is a genuine alias (either name
 * sets the pop wait). `verifyInherits` still checks every entry against the
 * parse, so a future entry fails the build rather than outliving the code.
 */
const INHERITS = new Map();

/**
 * Variables resolved outside the `env_*` helpers of config.rs, so the parser
 * cannot see them, plus the ones whose config.rs call site binds a derived
 * value the parser would publish by name. `source` is the file that reads the
 * variable; `verifyExtraSources` checks it still does.
 */
const EXTRA_VARS = [
  // --- Server
  {
    name: "QUEEN_SERVER_ID",
    type: "string",
    def: "HOSTNAME, else a random queen-<hex> name",
    aliases: [],
    source: CONFIG, // resolve_server_id reads it with std::env::var
  },
  {
    name: "QUEEN_MAX_BODY_BYTES",
    type: "integer",
    def: "67108864 (64 MiB)",
    aliases: [],
    source: "server/src/handlers/raft.rs", // the router's DefaultBodyLimit
  },
  // --- Storage and replication: server/src/rsm
  {
    name: "QUEEN_RAFT_REPLICATOR",
    type: "string",
    def: "local (openraft for a cluster)",
    aliases: [],
    source: "server/src/rsm/replicator/node.rs",
  },
  {
    name: "QUEEN_RAFT_NODE_ID",
    type: "string",
    def: "1 (a number from 1, or ordinal)",
    aliases: [],
    source: "server/src/rsm/replicator/raft/cluster.rs",
  },
  {
    name: "QUEEN_RAFT_PEERS",
    type: "string",
    def: "(empty: a single voter)",
    aliases: [],
    source: "server/src/rsm/replicator/raft/cluster.rs",
  },
  {
    name: "QUEEN_RAFT_LISTEN",
    type: "string",
    def: "0.0.0.0 at this node's raft port in QUEEN_RAFT_PEERS",
    aliases: [],
    source: "server/src/rsm/replicator/raft/cluster.rs",
  },
  {
    name: "QUEEN_RAFT_TOKEN",
    type: "string",
    def: "(empty)",
    aliases: [],
    source: "server/src/rsm/replicator/raft/cluster.rs",
  },
  {
    name: "QUEEN_RAFT_JOIN",
    type: "boolean",
    def: "false",
    aliases: [],
    source: "server/src/rsm/replicator/raft/mod.rs",
  },
  {
    name: "QUEEN_RAFT_FORCE_RECOVER",
    type: "integer",
    def: "(empty)",
    aliases: [],
    source: "server/src/rsm/replicator/raft/mod.rs",
  },
  {
    name: "QUEEN_RAFT_ELECTION_MS",
    type: "integer",
    def: "1000 on a cluster, 150 on a single node",
    aliases: [],
    source: "server/src/rsm/replicator/raft/mod.rs",
  },
  {
    name: "QUEEN_RAFT_HEARTBEAT_MS",
    type: "integer",
    def: "100 on a cluster, 50 on a single node",
    aliases: [],
    source: "server/src/rsm/replicator/raft/mod.rs",
  },
  {
    name: "QUEEN_RAFT_PURGE_HOLD_S",
    type: "integer",
    def: "600",
    aliases: [],
    source: "server/src/rsm/replicator/raft/mod.rs",
  },
  {
    name: "QUEEN_RAFT_GROUPS",
    type: "integer",
    def: "1 (at most 64)",
    aliases: [],
    source: "server/src/rsm/facade/groups.rs",
  },
  {
    name: "QUEEN_TENANT_GROUPS",
    type: "string",
    def: "(empty: placement by hash)",
    aliases: [],
    source: "server/src/rsm/facade/groups.rs",
  },
  {
    name: "QUEEN_QLOG_SHARDS",
    type: "integer",
    def: "0 (one log per queue; at most 4096)",
    aliases: [],
    source: "server/src/rsm/qlog/set.rs",
  },
  // --- Retention and background jobs: the leader's maintenance cadence
  {
    name: "RETENTION_INTERVAL",
    type: "integer",
    def: "5000",
    aliases: [],
    source: "server/src/rsm/batcher.rs",
  },
  {
    name: "RETENTION_BATCH_SIZE",
    type: "integer",
    def: "1000",
    aliases: [],
    source: "server/src/rsm/batcher.rs",
  },
  {
    name: "PARTITION_CLEANUP_DAYS",
    type: "integer",
    def: "30",
    aliases: [],
    source: "server/src/rsm/batcher.rs",
  },
  {
    name: "QUEEN_PARTITION_CLEANUP_ENABLED",
    type: "boolean",
    def: "true",
    aliases: [],
    source: "server/src/rsm/batcher.rs",
  },
  {
    name: "QUEEN_RAFT_RETENTION_VISIT",
    type: "integer",
    def: "8192",
    aliases: [],
    source: "server/src/rsm/batcher.rs",
  },
  {
    name: "QUEEN_RAFT_TXN_WINDOW_MIN_S",
    type: "integer",
    def: "900",
    aliases: [],
    source: "server/src/rsm/batcher.rs",
  },
  {
    name: "QUEEN_RAFT_TRACE_RETENTION_S",
    type: "integer",
    def: "604800 (7 days)",
    aliases: [],
    source: "server/src/rsm/batcher.rs",
  },
  {
    name: "METRICS_FLUSH_MS",
    type: "integer",
    def: "60000 (at least 1000)",
    aliases: [],
    source: "server/src/rsm/dashboard/collector.rs",
  },
  {
    name: "QUEEN_DASH_NODE_RETENTION_H",
    type: "integer",
    def: "168",
    aliases: [],
    source: "server/src/rsm/dashboard/store.rs",
  },
  {
    name: "QUEEN_DASH_QUEUE_RETENTION_H",
    type: "integer",
    def: "24",
    aliases: [],
    source: "server/src/rsm/dashboard/store.rs",
  },
  {
    name: "QUEEN_DASH_MAX_QUEUE_ROWS",
    type: "integer",
    def: "500000",
    aliases: [],
    source: "server/src/rsm/dashboard/store.rs",
  },
  // --- Key/value state, timers and the sweeper
  {
    name: "QUEEN_KV_REQUIRE_GRANT",
    type: "boolean",
    def: "the value of QUEEN_TENANCY_HEADER (so: false)",
    aliases: [],
    source: CONFIG,
    // config.rs binds the default to the resolved `tenancy_header`, and the
    // parser would publish the binding NAME as the default.
  },
  {
    name: "QUEEN_KV_MAX_VALUE_BYTES",
    type: "integer",
    def: "65536",
    aliases: [],
    source: "server/src/handlers/kv.rs",
  },
  {
    name: "QUEEN_KV_MAX_OPS_PER_CALL",
    type: "integer",
    def: "256",
    aliases: [],
    source: "server/src/handlers/kv.rs",
  },
  {
    name: "QUEEN_KV_MAX_KEYS_PER_CALL",
    type: "integer",
    def: "1024",
    aliases: [],
    source: "server/src/handlers/kv.rs",
  },
  {
    name: "QUEEN_TIMERS_MAX_HORIZON_S",
    type: "integer",
    def: "7776000 (90 days)",
    aliases: [],
    source: "server/src/handlers/timers.rs",
  },
  {
    name: "QUEEN_TIMERS_MAX_OPS_PER_CALL",
    type: "integer",
    def: "256",
    aliases: [],
    source: "server/src/handlers/timers.rs",
  },
  {
    name: "QUEEN_RAFT_TIMER_TICK_MS",
    type: "integer",
    def: "50 (0: timers never fire)",
    aliases: [],
    source: "server/src/rsm/batcher.rs",
  },
  {
    name: "QUEEN_RAFT_TIMER_FIRE_BATCH",
    type: "integer",
    def: "256",
    aliases: [],
    source: "server/src/rsm/planner/timers.rs",
  },
  {
    name: "QUEEN_RAFT_TIMER_FIRE_MAX_BYTES",
    type: "integer",
    def: "4194304 (4 MiB)",
    aliases: [],
    source: "server/src/rsm/planner/timers.rs",
  },
  {
    name: "QUEEN_SWEEPER_BACKOFF_MIN_MS",
    type: "integer",
    def: "1000",
    aliases: [],
    source: "server/src/rsm/planner/timers.rs",
  },
  {
    name: "QUEEN_SWEEPER_BACKOFF_MAX_MS",
    type: "integer",
    def: "60000",
    aliases: [],
    source: "server/src/rsm/planner/timers.rs",
  },
  {
    name: "QUEEN_SWEEPER_TRANSIENT_BACKOFF_MS",
    type: "integer",
    def: "1000",
    aliases: [],
    source: "server/src/rsm/planner/timers.rs",
  },
  {
    name: "QUEEN_SWEEPER_MAX_ATTEMPTS",
    type: "integer",
    def: "5",
    aliases: [],
    source: "server/src/rsm/planner/timers.rs",
  },
  {
    name: "QUEEN_RAFT_KV_SWEEP_MS",
    type: "integer",
    def: "1000",
    aliases: [],
    source: "server/src/rsm/batcher.rs",
  },
  {
    name: "QUEEN_RAFT_KV_SWEEP_LIMIT",
    type: "integer",
    def: "512",
    aliases: [],
    source: "server/src/rsm/batcher.rs",
  },
  // --- Ephemeral queues
  {
    name: "QUEEN_EPHEMERAL_REQUIRE_GRANT",
    type: "boolean",
    def: "the value of QUEEN_TENANCY_HEADER (so: false)",
    aliases: [],
    source: CONFIG,
  },
  // --- Embedded proxy and Kafka facade
  {
    name: "QUEEN_PROXY_EMBEDDED",
    type: "boolean",
    def: "false",
    aliases: [],
    source: "server/src/proxy_embed.rs",
  },
  {
    name: "QUEEN_PROXY_PORT",
    type: "string",
    def: "(empty: the proxy fronts PORT)",
    aliases: [],
    source: "server/src/proxy_embed.rs",
  },
  {
    name: "QUEEN_KAFKA_THREADS",
    type: "integer",
    def: "min(4, cores / 2), at least 1",
    aliases: [],
    source: "server/src/kafka_inproc.rs",
  },
  {
    name: "QUEEN_KAFKA_OFFSET_STORE",
    type: "string",
    def: "positions (or kv)",
    aliases: [],
    source: "server/src/kafka_inproc.rs",
  },
];

/** Files EXTRA_VARS cites, for the partial's source header. */
const EXTRA_SOURCES = [...new Set(EXTRA_VARS.map((v) => v.source).filter((s) => s !== CONFIG))].sort();

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

/**
 * Every EXTRA_VARS row is a claim that a file reads a variable. Check it: a
 * renamed or removed knob fails the generator instead of staying documented.
 */
function verifyExtraSources() {
  const cache = new Map();
  for (const v of EXTRA_VARS) {
    if (!cache.has(v.source)) cache.set(v.source, repoRead(v.source));
    if (!cache.get(v.source).includes(`"${v.name}"`)) {
      throw new Error(`EXTRA_VARS says ${v.source} reads ${v.name}, and it no longer names it`);
    }
  }
}

// ---------------------------------------------------------------------------

function main() {
  const check = isCheck();
  const configText = repoRead(CONFIG);

  const vars = parseEnvVars(configText);
  if (vars.size < 40) {
    throw new Error(`only parsed ${vars.size} env vars out of ${CONFIG}: the parser is broken`);
  }

  verifyExtraSources();
  for (const v of EXTRA_VARS) vars.set(v.name, v);

  verifyInherits(vars);

  const main_ = [...vars.values()].sort((a, b) => a.name.localeCompare(b.name));

  const byGroup = new Map();
  for (const v of main_) {
    const g = groupOf(v.name);
    if (!byGroup.has(g)) byGroup.set(g, []);
    byGroup.get(g).push(v);
  }

  const lines = [];
  const inheritColumn = INHERITS.size > 0;
  lines.push(
    `The broker is configured entirely through environment variables: ` +
      `${main_.length} of them, listed below with the defaults the code actually applies. ` +
      `Booleans go through one strict parser: an unparseable value is a fatal boot error, ` +
      `while unset and empty both fall back to the default.`,
    "",
    inheritColumn
      ? `Two columns record what a variable falls back to. **Also read as** is an older name for ` +
          `the same setting: either name configures the same thing, and the row's name wins when ` +
          `both are set. **Default inherited from** is a different knob whose value this one starts ` +
          `at when it is unset: setting that knob moves this variable and everything else that ` +
          `inherits from it, while setting this variable moves only this one.`
      : `**Also read as** is an older name for the same setting: either name configures the same ` +
          `thing, and the row's name wins when both are set.`,
    "",
  );

  for (const [g] of [...GROUPS, ["Other"]]) {
    const rows = byGroup.get(g);
    if (!rows?.length) continue;
    lines.push(`### ${g}`, "");
    if (inheritColumn) {
      lines.push("| Variable | Type | Default | Default inherited from | Also read as |");
      lines.push("| --- | --- | --- | --- | --- |");
    } else {
      lines.push("| Variable | Type | Default | Also read as |");
      lines.push("| --- | --- | --- | --- |");
    }
    for (const v of rows) {
      const inherited = INHERITS.has(v.name) ? `\`${INHERITS.get(v.name)}\`` : "";
      const aliases = inherited ? "" : v.aliases.map((a) => `\`${a}\``).join(", ");
      lines.push(
        inheritColumn
          ? `| \`${v.name}\` | ${v.type} | \`${cell(v.def)}\` | ${inherited} | ${aliases} |`
          : `| \`${v.name}\` | ${v.type} | \`${cell(v.def)}\` | ${aliases} |`,
      );
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
