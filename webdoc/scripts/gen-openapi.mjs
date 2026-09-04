/**
 * Generate OpenAPI 3.1 documents for the broker and the proxy, from the code.
 *
 * Nothing here is transcribed. Every part of each operation comes from a
 * specific construct in the Rust source:
 *
 *   path + method      the `Router::new()` chain (broker main.rs; proxy main.rs
 *                      plus the nested console and auth routers)
 *   path parameters    the `:name` segments of the registered path
 *   query parameters   the handler's `Query<T>` extractor — the fields of `T`
 *                      with their serde renames and optionality; or, for
 *                      handlers that take `Query<HashMap<..>>`, the literal
 *                      keys they read out of the map
 *   request body       the struct the handler feeds to `serde_json::from_slice`
 *   response codes     every `StatusCode::` variant reachable in the handler
 *   security           `auth::route_access_level`, mirrored under a fingerprint
 *                      guard in gen-routes.mjs and re-derived here
 *   tenant scoping     whether the handler takes the `Tenant` extension
 *
 * What is NOT derivable is marked as such in the document rather than invented.
 * The hot paths build their response JSON by string concatenation
 * (`render_push_results`, `render_pop_parts`, `render_ack_results`), and several
 * management handlers pass a stored procedure's JSON through verbatim; for those
 * the response schema is an open object carrying `x-queen-schema: "opaque"` and
 * a pointer to the page that documents the shape. A spec that says "object" is
 * honest; a spec that invents field names is not.
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { ROUTER_BUILDER, WEBDOC, cell, emitPartial, isCheck, repoRead, rustFiles, sliceBlock } from "./lib/source.mjs";

const OUT = join(WEBDOC, "public", "openapi");
const VERSION = "1.0.0";

const STATUS = {
  OK: "200",
  CREATED: "201",
  NO_CONTENT: "204",
  BAD_REQUEST: "400",
  UNAUTHORIZED: "401",
  FORBIDDEN: "403",
  NOT_FOUND: "404",
  CONFLICT: "409",
  PAYLOAD_TOO_LARGE: "413",
  INTERNAL_SERVER_ERROR: "500",
  SERVICE_UNAVAILABLE: "503",
};

const STATUS_TEXT = {
  200: "OK",
  201: "Created",
  204: "No content. The broker returns 204 with an empty body: no content-length, no payload.",
  400: "Malformed request.",
  401: "Missing or invalid credentials (only when JWT authentication is enabled).",
  403: "The credential's access level does not cover this route.",
  404: "Not found.",
  409: "Conflict.",
  413: "Body larger than QUEEN_MAX_BODY_BYTES.",
  500: "Internal error.",
  503: "Dependency unavailable, typically PostgreSQL.",
};

// ---------------------------------------------------------------------------
// Rust source model
// ---------------------------------------------------------------------------

/** Every `pub async fn NAME(args) -> Response { .. }` in the crate, with its body. */
function indexHandlers(files) {
  const out = new Map();
  for (const { path: file, text } of files) {
    const re = /pub async fn (\w+)\s*\(([\s\S]*?)\)\s*->\s*Response\s*\{/g;
    let m;
    while ((m = re.exec(text))) {
      const [, name, args] = m;
      const open = text.indexOf("{", m.index + m[0].length - 1);
      let depth = 0;
      let end = text.length;
      for (let i = open; i < text.length; i++) {
        if (text[i] === "{") depth++;
        else if (text[i] === "}") {
          depth--;
          if (depth === 0) {
            end = i;
            break;
          }
        }
      }
      out.set(name, { name, file, args, body: text.slice(open, end) });
    }
  }
  return out;
}

/** Every `#[derive(..Deserialize..)] struct NAME { fields }`, with serde attrs. */
function indexStructs(files) {
  const out = new Map();
  for (const { text } of files) {
    const re = /#\[derive\([^)]*Deserialize[^)]*\)\]\s*(?:#\[[^\]]*\]\s*)*(?:pub(?:\([^)]*\))?\s+)?struct (\w+)(?:<[^>]*>)?\s*\{([\s\S]*?)\n\}/g;
    let m;
    while ((m = re.exec(text))) {
      const [, name, block] = m;
      const fields = [];
      let pendingRename = null;
      let pendingDefault = false;
      for (const rawLine of block.split("\n")) {
        const line = rawLine.trim();
        if (!line || line.startsWith("//")) continue;
        if (line.startsWith("#[")) {
          const r = line.match(/rename\s*=\s*"([^"]+)"/);
          if (r) pendingRename = r[1];
          if (/\bdefault\b/.test(line)) pendingDefault = true;
          continue;
        }
        const f = line.match(/^(?:pub(?:\([^)]*\))?\s+)?(\w+)\s*:\s*(.+?),?$/);
        if (!f) continue;
        const [, rustName, rustType] = f;
        fields.push({
          rustName,
          wireName: pendingRename ?? rustName,
          rustType: rustType.replace(/,$/, "").trim(),
          hasDefault: pendingDefault,
        });
        pendingRename = null;
        pendingDefault = false;
      }
      if (!out.has(name)) out.set(name, { name, fields });
    }
  }
  return out;
}

/** Rust type -> JSON Schema. Unknown or dynamic types become an open schema. */
function schemaOf(rustType, structs, seen = new Set()) {
  let t = rustType.replace(/&'\w+\s*/g, "").replace(/^&/, "").trim();

  const opt = t.match(/^Option<(.+)>$/);
  if (opt) return { schema: schemaOf(opt[1], structs, seen).schema, optional: true };

  const vec = t.match(/^Vec<(.+)>$/);
  if (vec) {
    return {
      schema: { type: "array", items: schemaOf(vec[1], structs, seen).schema },
      optional: false,
    };
  }

  const prim = {
    String: { type: "string" },
    str: { type: "string" },
    bool: { type: "boolean" },
    i32: { type: "integer", format: "int32" },
    u32: { type: "integer", format: "int32", minimum: 0 },
    i64: { type: "integer", format: "int64" },
    u64: { type: "integer", format: "int64", minimum: 0 },
    usize: { type: "integer", minimum: 0 },
    f64: { type: "number" },
  };
  if (prim[t]) return { schema: { ...prim[t] }, optional: false };

  // A JSON value the handler forwards without inspecting it.
  if (/RawValue|serde_json::Value|^Value$/.test(t)) {
    return { schema: { description: "Any JSON value." }, optional: false };
  }

  const named = t.replace(/<.*>$/, "");
  if (structs.has(named) && !seen.has(named)) {
    return { schema: structSchema(named, structs, new Set([...seen, named])), optional: false };
  }

  return { schema: { description: `Rust type \`${rustType}\`.` }, optional: false };
}

function structSchema(name, structs, seen = new Set([name])) {
  const st = structs.get(name);
  if (!st) return { type: "object" };
  const properties = {};
  const required = [];
  for (const f of st.fields) {
    const { schema, optional } = schemaOf(f.rustType, structs, seen);
    properties[f.wireName] = schema;
    if (!optional && !f.hasDefault) required.push(f.wireName);
  }
  const out = { type: "object", properties };
  if (required.length) out.required = required;
  return out;
}

// ---------------------------------------------------------------------------
// Handler analysis
// ---------------------------------------------------------------------------

function queryParams(handler, structs) {
  const q = handler.args.match(/Query<([^>]+(?:<[^>]*>)?)>/);
  if (!q) return { params: [], mode: "none" };
  const inner = q[1].trim();

  if (structs.has(inner)) {
    const st = structs.get(inner);
    return {
      mode: "typed",
      source: inner,
      params: st.fields.map((f) => {
        const { schema } = schemaOf(f.rustType, structs);
        return {
          name: f.wireName,
          in: "query",
          required: false,
          schema,
        };
      }),
    };
  }

  // `Query<HashMap<String, String>>`: the keys the handler actually reads.
  //
  // Bound to the extractor's OWN binding name (`Query(q): Query<HashMap<…>>`
  // gives `q`) instead of any identifier, and tolerant of the receiver and the
  // `.get` sitting on different lines. Both halves are corrections, and the
  // second is the one that was losing parameters: rustfmt breaks
  // `q.get("limit").and_then(…)` across lines whenever the chain is long, and a
  // pattern anchored to `\w+\.get\(` then matches the short accesses and misses
  // the long ones — so `GET /api/v1/timers/:queue` published `after` and
  // silently dropped `limit`. A spec that lists three of a route's four
  // parameters is worse than one that lists none, because it reads complete.
  const binding = handler.args.match(/Query\(\s*(\w+)\s*\)\s*:\s*Query</)?.[1];
  const access = binding
    ? new RegExp(`\\b${binding}\\s*\\.\\s*get\\(\\s*"([a-zA-Z_][\\w]*)"\\s*\\)`, "g")
    : /\b\w+\s*\.\s*get\(\s*"([a-zA-Z_][\w]*)"\s*\)/g;
  const keys = [...handler.body.matchAll(access)].map((m) => m[1]);
  const uniq = [...new Set(keys)];
  if (uniq.length) {
    return {
      mode: "adhoc",
      params: uniq.map((k) => ({ name: k, in: "query", required: false, schema: { type: "string" } })),
    };
  }
  // A handler that takes the map only to REFUSE it. The KV path routes do this
  // on purpose (a prefix in a URL is recorded by every access log between the
  // client and the database), and the "forwarded" note below — "parameters are
  // forwarded to a stored procedure" — would be the opposite of the truth on
  // exactly the routes where the rule is a privacy boundary.
  if (/\breject_query\s*\(/.test(handler.body)) return { mode: "rejected", params: [] };
  return { mode: "forwarded", params: [] };
}

function requestBody(handler, structs) {
  if (!/body:\s*Bytes/.test(handler.args)) return null;

  // A handler that branches on `body.is_empty()` accepts no body at all, so the
  // body is optional. Every route that had one until now required it, which is
  // why `required: true` could be a constant; `DELETE /api/v1/kv/:ns/*key` is
  // the first where an empty body is the ordinary case and `{"expect":N}` the
  // exception, and publishing that as required would tell a generated client to
  // send `{}` on every unconditional delete.
  const required = !/\bbody\s*\.\s*is_empty\(\)/.test(handler.body);

  // `let x: T = serde_json::from_slice(&body)` or `from_slice::<T>(&body)`
  const typed =
    handler.body.match(/let\s+\w+\s*:\s*(\w+)\s*=\s*(?:match\s+)?serde_json::from_slice/) ??
    handler.body.match(/serde_json::from_slice::<(\w+)>/);
  const name = typed?.[1];

  if (name && structs.has(name)) {
    return {
      required,
      content: {
        "application/json": {
          schema: structSchema(name, structs),
        },
      },
      "x-queen-schema": "derived",
      "x-queen-schema-source": name,
    };
  }

  // The handler parses the body as a dynamic value and inspects it key by key.
  const keys = [...handler.body.matchAll(/\bget\(\s*"([a-zA-Z][\w]*)"\s*\)/g)].map((m) => m[1]);
  const properties = {};
  for (const k of [...new Set(keys)]) properties[k] = { description: "See the reference page for this route." };
  return {
    required,
    content: {
      "application/json": {
        schema: Object.keys(properties).length
          ? { type: "object", properties, additionalProperties: true }
          : { type: "object", additionalProperties: true },
      },
    },
    "x-queen-schema": "opaque",
    description:
      "The handler reads this body as a dynamic JSON document, so the exact shape is not " +
      "recoverable from a type. The listed keys are the ones the handler reads by name.",
  };
}

// Per-handler wording where the shared STATUS_TEXT would be wrong for the code
// this specific route emits. Keyed by handler fn name, then status code.
const RESPONSE_TEXT_OVERRIDES = {
  // The streams registration 403 is the grant/quota denial (queen_streams.quota
  // absent, disabled, or at max_queries), minted by the handler itself, so it
  // exists with authentication off too. Describing it as a credential-level
  // failure would send the reader to fix the wrong thing.
  handle_streams_register: {
    403: "Streams not granted for this tenant, or its max_queries cap is reached. The body carries denied: true.",
  },
};

function responses(handler) {
  const codes = new Set(
    [...handler.body.matchAll(/StatusCode::([A-Z_]+)/g)]
      .map((m) => STATUS[m[1]])
      .filter(Boolean),
  );
  // Every handler can fail the same two ways once authentication is on, and the
  // body limit is a layer above every route.
  const out = {};
  for (const code of [...codes].sort()) {
    const r = {
      description:
        RESPONSE_TEXT_OVERRIDES[handler.name]?.[code] ?? STATUS_TEXT[code] ?? "",
    };
    if (code !== "204") {
      // Success bodies on these routes are assembled as text, not serialized
      // from a type, so the shape is not recoverable here. The schema is left
      // UNCONSTRAINED rather than typed as an object: several of these routes
      // answer with a top-level array, and `type: "object"` would be a false
      // assertion — worse than no assertion.
      r.content = {
        "application/json": {
          schema:
            code >= "400"
              ? { $ref: "#/components/schemas/Error" }
              : {
                  description:
                    "Shape not derivable from a Rust type. See the route's page under " +
                    "/reference/http for the field-by-field contract.",
                },
        },
      };
      if (code < "400") r["x-queen-schema"] = "opaque";
    }
    out[code] = r;
  }
  if (!Object.keys(out).length) out["200"] = { description: STATUS_TEXT[200] };
  return out;
}

// ---------------------------------------------------------------------------
// Route parsing
// ---------------------------------------------------------------------------

function parseRouterChain(block) {
  const routes = [];
  const re = /\.route\(\s*"([^"]+)"\s*,/g;
  let m;
  while ((m = re.exec(block))) {
    const path = m[1];
    const rest = block.slice(re.lastIndex, re.lastIndex + 400);
    const stop = rest.indexOf(".route(");
    const window = stop === -1 ? rest : rest.slice(0, stop);
    for (const [, verb, handler] of window.matchAll(
      /\b(get|post|put|patch|delete|head|options)\(\s*([\w:]+)/g,
    )) {
      routes.push({ path, method: verb, handler: handler.split("::").pop() });
    }
  }
  return routes;
}

/** Mirror of auth::route_access_level — see gen-routes.mjs for the drift guard.
 *
 * Kept in step with that mirror by hand and pinned by the SAME fingerprint: this
 * script derives `security` for every operation from it, so a rule that lands in
 * one mirror and not the other publishes a spec whose auth requirements disagree
 * with the route table on the same site. 2026-08-21: the three EPHEMERAL_QUEUES
 * §3.9 rules added below. 2026-08-28: the PLAN_QUEEN_KAFKA.md C2 fetch arm,
 * placed after the GET block exactly as the Rust places it. The kv/timer GET
 * arms are still absent here and are still a KNOWN gap of this mirror (they only
 * widen `read-only` to `read-write` in the spec, never the reverse), left
 * untouched by this change so that its diff says exactly one thing.
 * 2026-09-04: the bulk DLQ purge arm, in the admin block above the GET block
 * exactly as the Rust places it, so `GET /api/v1/dlq` stays read-only.
 * 2026-09-04: the partition-discovery arm (PLAN_S3_SINK.md §5.1), beside the
 * fetch arm it is the twin of and outside the GET block for the same reason.
 */
function accessLevel(method, path) {
  const m = method.toUpperCase();
  if (path === "/health" || path === "/metrics" || path === "/metrics/prometheus") return "public";
  if (path === "/" || path.startsWith("/assets/") || path.startsWith("/favicon")) return "public";
  // Broker-direct dashboard identity — public by design (see gen-routes.mjs).
  if (path === "/auth/me" || path === "/auth/login" || path === "/auth/logout") return "public";
  if (path.startsWith("/api/v1/system/") || path.startsWith("/internal/")) return "admin";
  if (m === "DELETE" && path.startsWith("/api/v1/consumer-groups/")) return "admin";
  if (m === "DELETE" && path.startsWith("/api/v1/resources/queues/")) return "admin";
  if (m === "DELETE" && path === "/api/v1/dlq") return "admin";
  if (path === "/api/v1/stats/refresh") return "admin";
  if (m === "GET") {
    if (path === "/status") return "read-only";
    if (path.startsWith("/api/v1/status") || path.startsWith("/api/v1/analytics")) return "read-only";
    if (path.startsWith("/api/v1/resources/")) return "read-only";
    if (path.startsWith("/api/v1/messages")) return "read-only";
    if (path.startsWith("/api/v1/consumer-groups")) return "read-only";
    if (path.startsWith("/api/v1/dlq")) return "read-only";
    if (path.startsWith("/api/v1/traces")) return "read-only";
    // EPHEMERAL_QUEUES.md §3.9: the two status reads, inside the GET block.
    if (path.startsWith("/api/v1/ephemeral/queues")) return "read-only";
  }
  // PLAN_QUEEN_KAFKA.md C2: a pure read, outside the GET block because the
  // batch request is a body.
  if (m === "POST" && path === "/api/v1/fetch") return "read-only";
  // PLAN_S3_SINK.md §5.1: the fetch arm's twin, and read-only for the same
  // reason.
  if (m === "POST" && path === "/api/v1/partitions/changed") return "read-only";
  if (path === "/streams/v1/state/get") return "read-only";
  if (path.startsWith("/streams/")) return "read-write";
  if (path === "/api/v1/push") return "write-only";
  if (m === "POST" && path === "/api/v1/ephemeral/push") return "write-only";
  if (path.startsWith("/api/v1/ephemeral")) return "read-write";
  return "read-write";
}

const TAGS = [
  ["Message plane", (p) => /^\/api\/v1\/(push|pop|ack|transaction|lease)/.test(p)],
  ["Queues", (p) => /^\/api\/v1\/(configure|resources)/.test(p)],
  ["Consumer groups", (p) => p.startsWith("/api/v1/consumer-groups")],
  ["Messages, DLQ and traces", (p) => /^\/api\/v1\/(messages|dlq|traces)/.test(p)],
  ["Status and metrics", (p) =>
    /^\/api\/v1\/(status|analytics|stats)/.test(p) ||
    ["/health", "/status", "/metrics", "/metrics/prometheus"].includes(p)],
  ["Streams", (p) => p.startsWith("/streams/")],
  // EPHEMERAL_QUEUES.md §3.1 — its own tag, not folded into the message plane:
  // the two families share no storage, no durability contract and no verbs.
  ["Ephemeral queues", (p) => /^\/api\/v1\/ephemeral(\/|$)/.test(p)],
  ["Operator", (p) => p.startsWith("/api/v1/system")],
  ["Dashboard identity", (p) => p.startsWith("/auth/")],
  ["Internal", (p) => p.startsWith("/internal/")],
];

function tagOf(path) {
  for (const [t, test] of TAGS) if (test(path)) return t;
  return "Other";
}

/**
 * `/api/v1/pop/queue/:queue` -> `/api/v1/pop/queue/{queue}` plus its params.
 * Also handles axum's catch-all `*path` segment, which OpenAPI has no notion of:
 * it becomes a normal templated parameter carrying a note.
 */
function toOpenApiPath(path) {
  const params = [...path.matchAll(/[:*](\w+)/g)].map((m) => m[1]);
  const wildcard = /\*\w+/.test(path);
  return {
    oapiPath: path.replace(/:(\w+)/g, "{$1}").replace(/\*(\w+)/g, "{$1}"),
    params,
    wildcard,
  };
}

/**
 * Drop a path that only differs from one already registered by a trailing
 * slash and resolves to the same handler and method. Both are really
 * registered — axum does not treat them as equal — but documenting both
 * produces two identical operations and no reader learns anything from the
 * second.
 */
function dropTrailingSlashTwins(routes) {
  const key = (r) => `${r.method} ${r.path.replace(/\/$/, "")} ${r.handler}`;
  const seen = new Set();
  const out = [];
  for (const r of routes) {
    // Prefer the slashless spelling: sort it first so it wins the key.
    const k = key(r);
    if (r.path.endsWith("/") && r.path !== "/" && seen.has(k)) continue;
    seen.add(k);
    out.push(r);
  }
  return out;
}

const ERROR_SCHEMA = {
  type: "object",
  properties: { error: { type: "string" } },
  required: ["error"],
  description: "The broker's error body.",
};

// ---------------------------------------------------------------------------
// Broker document
// ---------------------------------------------------------------------------

function brokerSpec(handlers, structs) {
  const chain = sliceBlock(repoRead("server/src/main.rs"), ROUTER_BUILDER, ".with_state(state);");
  const routes = dropTrailingSlashTwins(parseRouterChain(chain));
  if (routes.length < 40) throw new Error(`only parsed ${routes.length} broker routes`);

  const paths = {};
  let derivedBodies = 0;
  let opaqueBodies = 0;

  for (const r of routes) {
    const h = handlers.get(r.handler);
    if (!h) throw new Error(`router references handler ${r.handler}, not found in server/src`);

    const { oapiPath, params, wildcard } = toOpenApiPath(r.path);
    const level = accessLevel(r.method, r.path);
    const tenantScoped = /tenant::Tenant|Extension<Tenant>/.test(h.args);
    const q = queryParams(h, structs);
    // GET is excluded by rule; DELETE is not any more. `requestBody` already
    // gates on the handler taking `body: Bytes`, and until now no DELETE handler
    // did, so excluding the method was free — it is not free now. `DELETE
    // /api/v1/kv/:ns/*key` reads an optional `{"expect":N}`, which is the
    // difference between an unconditional delete and a compare-and-delete, and
    // suppressing it by method would drop the only way to express the second.
    const body = r.method === "get" ? null : requestBody(h, structs);
    if (body?.["x-queen-schema"] === "derived") derivedBodies++;
    else if (body) opaqueBodies++;

    const op = {
      operationId: r.handler.replace(/^handle_/, ""),
      tags: [tagOf(r.path)],
      summary: `${r.method.toUpperCase()} ${r.path}`,
      parameters: [
        ...params.map((p) => ({
          name: p,
          in: "path",
          required: true,
          schema: { type: "string" },
        })),
        ...q.params,
        ...(tenantScoped
          ? [
              {
                name: "x-queen-tenant",
                in: "header",
                required: false,
                schema: { type: "string", format: "uuid" },
                description:
                  "Tenant to scope this request to. Read only when QUEEN_TENANCY_HEADER is on; " +
                  "unauthenticated by design, so the broker must not be reachable directly by tenants.",
              },
            ]
          : []),
      ],
      responses: responses(h),
      "x-queen-access-level": level,
      "x-queen-tenant-scoped": tenantScoped,
      "x-queen-handler": `${h.file}::${h.name}`,
      "x-queen-query-params": q.mode,
    };
    if (level !== "public") op.security = [{ bearerAuth: [] }];
    if (body) op.requestBody = body;
    if (wildcard) {
      op["x-queen-catch-all"] = true;
    }
    if (q.mode === "forwarded") {
      op["x-queen-note"] =
        "Query parameters are forwarded to a stored procedure without being read by name in " +
        "Rust, so they are not recoverable from the handler. See the reference page for this route.";
    }
    if (q.mode === "rejected") {
      op["x-queen-note"] =
        "This route takes no query parameters and refuses any query string outright, rather " +
        "than ignoring one: a prefix or key in a URL is recorded by every access log, proxy " +
        "sample and tracing span between the client and the database.";
    }

    paths[oapiPath] ??= {};
    paths[oapiPath][r.method] = op;
  }

  return {
    doc: {
      openapi: "3.1.0",
      info: {
        title: "Queen MQ broker API",
        version: VERSION,
        summary: "The HTTP API of the Queen MQ broker.",
        description: [
          "Generated from the broker's own source at documentation build time by",
          "`webdoc/scripts/gen-openapi.mjs`: paths and methods come from the axum router,",
          "query parameters from each handler's extractor, request bodies from the structs",
          "handlers deserialize into, response codes from the `StatusCode` variants each",
          "handler can return, and authorization from `auth::route_access_level`.",
          "",
          "Response bodies on the hot paths are assembled by string concatenation rather than",
          "serialized from a type, and several management routes forward a stored procedure's",
          "JSON verbatim. Those responses carry `x-queen-schema: \"opaque\"` and are typed as an",
          "open object: the field-by-field shape is documented on the reference pages instead of",
          "guessed at here.",
          "",
          "Every 204 response has no body at all, not an empty JSON document.",
        ].join("\n"),
        license: { name: "Apache-2.0", identifier: "Apache-2.0" },
      },
      servers: [{ url: "http://localhost:6632", description: "A broker with default settings" }],
      tags: TAGS.map(([name]) => ({ name })).concat([{ name: "Other" }]),
      security: [{ bearerAuth: [] }],
      components: {
        securitySchemes: {
          bearerAuth: {
            type: "http",
            scheme: "bearer",
            bearerFormat: "JWT",
            description:
              "Only enforced when JWT_ENABLED is on. Access levels are a role set, not a " +
              "ladder: a write-only credential passes POST /api/v1/push and is rejected " +
              "everywhere else.",
          },
        },
        schemas: { Error: ERROR_SCHEMA },
      },
      paths,
    },
    stats: { routes: routes.length, paths: Object.keys(paths).length, derivedBodies, opaqueBodies },
  };
}

// ---------------------------------------------------------------------------
// Proxy document
// ---------------------------------------------------------------------------

/** `Router::new().route(..)` inside a `pub fn router()` in the given file. */
function subRouter(file) {
  const text = repoRead(file);
  const body = sliceBlock(text, "Router::new()", "\n}");
  return parseRouterChain(body);
}

function proxySpec(handlers, structs) {
  const mainText = repoRead("proxy/src/main.rs");
  const chain = sliceBlock(mainText, ROUTER_BUILDER, ";");
  const own = parseRouterChain(chain);

  const nested = [
    ...subRouter("proxy/src/oauth.rs").map((r) => ({ ...r, path: `/auth${r.path}` })),
    ...subRouter("proxy/src/console.rs").map((r) => ({ ...r, path: `/api/console${r.path}` })),
  ];

  const routes = dropTrailingSlashTwins(
    [...own, ...nested].sort((a, b) => a.path.length - b.path.length),
  );
  if (routes.length < 10) throw new Error(`only parsed ${routes.length} proxy routes`);

  const paths = {};
  for (const r of routes) {
    const { oapiPath, params, wildcard } = toOpenApiPath(r.path);
    const isConsole = r.path.startsWith("/api/console");
    const isAuth = r.path.startsWith("/auth");
    const op = {
      operationId: `${r.method}_${oapiPath.replace(/[^\w]+/g, "_").replace(/^_|_$/g, "") || "root"}`,
      tags: [isConsole ? "Console" : isAuth ? "Authentication" : "Service"],
      summary: `${r.method.toUpperCase()} ${r.path}`,
      parameters: params.map((p) => ({ name: p, in: "path", required: true, schema: { type: "string" } })),
      responses: { 200: { description: "OK" } },
      "x-queen-handler": r.handler,
    };
    if (wildcard) {
      op["x-queen-note"] =
        "Registered in axum as a catch-all segment: every path below this prefix resolves here.";
    }
    if (isConsole) {
      op.security = [{ sessionCookie: [] }, { bearerAuth: [] }];
      op.description =
        "Console API. Requires a proxy-minted user session (cookie or bearer token); a cluster " +
        "API key is not a console credential.";
    }
    paths[oapiPath] ??= {};
    paths[oapiPath][r.method] = op;
  }

  return {
    doc: {
      openapi: "3.1.0",
      info: {
        title: "Queen MQ proxy API",
        version: VERSION,
        summary: "The proxy's own surface: service endpoints, login, and the cluster console.",
        description: [
          "Generated from `proxy/`'s router at documentation build time.",
          "",
          "This document covers only the endpoints the proxy *serves*. Every other path is",
          "forwarded to the broker of the cluster addressed by the request's first DNS label,",
          "after classification, authentication, quota and rate-limit checks. For the broker's",
          "own surface use the broker document; for which of those routes a tenant credential",
          "can reach, see the route-class table in the documentation.",
          "",
          "Response shapes are not derivable here: console handlers build their JSON inline.",
          "Only paths, methods, path parameters and the credential kind are asserted.",
        ].join("\n"),
        license: { name: "Apache-2.0", identifier: "Apache-2.0" },
      },
      servers: [{ url: "https://{cluster}.example.com", variables: { cluster: { default: "my-cluster" } } }],
      tags: [{ name: "Service" }, { name: "Authentication" }, { name: "Console" }],
      components: {
        securitySchemes: {
          bearerAuth: {
            type: "http",
            scheme: "bearer",
            description: "A cluster API key (`qk_<env>_...`) or a proxy-minted user JWT.",
          },
          sessionCookie: {
            type: "apiKey",
            in: "cookie",
            name: "queen_session",
            description: "The httpOnly session cookie the proxy sets at login.",
          },
        },
        schemas: { Error: { type: "object", properties: { error: { type: "string" }, code: { type: "string" } } } },
      },
      paths,
    },
    stats: { routes: routes.length, paths: Object.keys(paths).length },
  };
}

// ---------------------------------------------------------------------------
// Self-check: a spec that is structurally wrong is worse than none
// ---------------------------------------------------------------------------

function validate(label, doc) {
  const problems = [];
  if (doc.openapi !== "3.1.0") problems.push("openapi version");
  if (!doc.info?.title || !doc.info?.version) problems.push("info");
  const seenIds = new Set();
  for (const [p, item] of Object.entries(doc.paths)) {
    if (!p.startsWith("/")) problems.push(`path does not start with /: ${p}`);
    if (/:[a-z]/i.test(p)) problems.push(`path keeps a Rust-style parameter: ${p}`);
    for (const [method, op] of Object.entries(item)) {
      if (!/^(get|put|post|delete|options|head|patch|trace)$/.test(method)) {
        problems.push(`${p}: unknown method ${method}`);
      }
      if (!op.responses || !Object.keys(op.responses).length) problems.push(`${p} ${method}: no responses`);
      if (seenIds.has(op.operationId)) problems.push(`duplicate operationId ${op.operationId}`);
      seenIds.add(op.operationId);
      // Every declared path parameter must be declared in the operation.
      for (const name of [...p.matchAll(/\{(\w+)\}/g)].map((m) => m[1])) {
        if (!op.parameters?.some((x) => x.in === "path" && x.name === name)) {
          problems.push(`${p} ${method}: path parameter ${name} not declared`);
        }
      }
      for (const ref of JSON.stringify(op).matchAll(/"#\/components\/schemas\/(\w+)"/g)) {
        if (!doc.components?.schemas?.[ref[1]]) problems.push(`${p} ${method}: dangling $ref ${ref[1]}`);
      }
    }
  }
  if (problems.length) {
    throw new Error(`${label} spec failed self-check:\n  - ${problems.join("\n  - ")}`);
  }
}

// ---------------------------------------------------------------------------

function main() {
  const check = isCheck();
  const brokerFiles = rustFiles("server/src");
  const proxyFiles = rustFiles("proxy/src");
  const handlers = indexHandlers([...brokerFiles, ...proxyFiles]);
  const structs = indexStructs([...brokerFiles, ...proxyFiles]);

  const broker = brokerSpec(handlers, structs);
  const proxy = proxySpec(handlers, structs);
  validate("broker", broker.doc);
  validate("proxy", proxy.doc);

  const files = [
    ["queen-broker.json", broker.doc],
    ["queen-proxy.json", proxy.doc],
  ];

  let drifted = false;
  if (!check) mkdirSync(OUT, { recursive: true });
  for (const [name, doc] of files) {
    const content = JSON.stringify(doc, null, 2) + "\n";
    const file = join(OUT, name);
    if (check) {
      let current = "";
      try {
        current = readFileSync(file, "utf8");
      } catch {
        /* missing counts as drift */
      }
      if (current !== content) drifted = true;
    } else {
      writeFileSync(file, content, "utf8");
    }
  }

  // A partial so a page can state the coverage without hard-coding counts.
  const lines = [
    "| Document | Operations | Paths | Request bodies derived from a type | Request bodies opaque |",
    "| --- | --- | --- | --- | --- |",
    `| [\`queen-broker.json\`](/openapi/queen-broker.json) | ${broker.stats.routes} | ${broker.stats.paths} | ${broker.stats.derivedBodies} | ${broker.stats.opaqueBodies} |`,
    `| [\`queen-proxy.json\`](/openapi/queen-proxy.json) | ${proxy.stats.routes} | ${proxy.stats.paths} | 0 | 0 |`,
  ].join("\n");

  const partial = emitPartial({
    name: "openapi-coverage",
    title: "OpenAPI coverage",
    description: "How many operations each generated OpenAPI document covers, and how much of each was derived from a Rust type.",
    sources: [
      "server/src/main.rs, server/src/handlers/*.rs (broker)",
      "proxy/src/{main,console,oauth}.rs (proxy)",
    ],
    body: lines,
    check,
  });

  if (partial.drifted) drifted = true;
  return {
    written: !check,
    drifted,
    title: `OpenAPI: ${broker.stats.routes} broker + ${proxy.stats.routes} proxy operations`,
    file: OUT,
  };
}

const result = main();
if (result.drifted) {
  console.error(`DRIFT: ${result.file} is behind its source`);
  process.exit(1);
}
console.log(`${result.written ? "wrote" : "ok"}  ${result.title}`);
