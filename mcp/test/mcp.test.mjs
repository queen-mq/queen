// The protocol and the tools, through the same handler the Worker runs.
// `npm test` rebuilds the bundle first.

import assert from "node:assert/strict";
import { test } from "node:test";
import bundle from "../src/bundle.generated.mjs";
import { handle, PROTOCOL_VERSIONS } from "../src/mcp.mjs";

let next = 0;
const post = (body, headers = {}) =>
  handle(
    new Request("https://queenmq.com/mcp", {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json, text/event-stream", ...headers },
      body: typeof body === "string" ? body : JSON.stringify(body),
    }),
  );
const rpc = async (method, params) => (await post({ jsonrpc: "2.0", id: ++next, method, params })).json();
const call = async (name, args) => {
  const reply = await rpc("tools/call", { name, arguments: args });
  return { text: reply.result.content[0].text, isError: reply.result.isError === true };
};

// ---------------------------------------------------------------- protocol

test("initialize echoes a supported version, falls back to the newest otherwise", async () => {
  const known = await rpc("initialize", { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "t", version: "0" } });
  assert.equal(known.result.protocolVersion, "2025-06-18");
  assert.equal(known.result.serverInfo.name, "queen");
  assert.ok(known.result.capabilities.tools && known.result.capabilities.prompts && known.result.capabilities.resources);
  assert.ok(known.result.instructions.includes("## Using this server"));
  const future = await rpc("initialize", { protocolVersion: "2099-01-01", capabilities: {} });
  assert.equal(future.result.protocolVersion, PROTOCOL_VERSIONS[0]);
});

test("notifications and client responses get 202 and no body", async () => {
  const r = await post({ jsonrpc: "2.0", method: "notifications/initialized" });
  assert.equal(r.status, 202);
  assert.equal(await r.text(), "");
  assert.equal((await post({ jsonrpc: "2.0", id: 7, result: {} })).status, 202);
});

test("batches answer every request and skip notifications", async () => {
  const r = await post([
    { jsonrpc: "2.0", id: "a", method: "ping" },
    { jsonrpc: "2.0", method: "notifications/initialized" },
    { jsonrpc: "2.0", id: "b", method: "tools/list" },
  ]);
  const replies = await r.json();
  assert.deepEqual(replies.map((x) => x.id), ["a", "b"]);
});

test("JSON-RPC errors: parse, unknown method, unknown tool, unknown prompt", async () => {
  const parse = await post("{nope");
  assert.equal(parse.status, 400);
  assert.equal((await parse.json()).error.code, -32700);
  assert.equal((await rpc("nope/nope")).error.code, -32601);
  assert.equal((await rpc("tools/call", { name: "nope", arguments: {} })).error.code, -32602);
  assert.equal((await rpc("prompts/get", { name: "nope" })).error.code, -32602);
  assert.equal((await rpc("resources/read", { uri: "https://queenmq.com/nope/" })).error.code, -32002);
});

test("HTTP edges: GET is 405 with help, OPTIONS is CORS, bad version header is 400, wrong type is 415", async () => {
  const get = await handle(new Request("https://queenmq.com/mcp"));
  assert.equal(get.status, 405);
  assert.match(await get.text(), /claude mcp add --transport http queen https:\/\/queenmq\.com\/mcp/);
  const options = await handle(new Request("https://queenmq.com/mcp", { method: "OPTIONS" }));
  assert.equal(options.status, 204);
  assert.equal(options.headers.get("access-control-allow-origin"), "*");
  assert.equal((await post({ jsonrpc: "2.0", id: 1, method: "ping" }, { "mcp-protocol-version": "1999-01-01" })).status, 400);
  assert.equal((await post({ jsonrpc: "2.0", id: 1, method: "ping" }, { "content-type": "text/plain" })).status, 415);
  assert.equal((await handle(new Request("https://queenmq.com/elsewhere"))).status, 404);
});

test("AGENTS.md is the primer as a plain file", async () => {
  const r = await handle(new Request("https://queenmq.com/mcp/AGENTS.md"));
  assert.equal(r.status, 200);
  assert.match(r.headers.get("content-type"), /text\/markdown/);
  const text = await r.text();
  assert.ok(text.startsWith(bundle.primer.slice(0, 40)));
  assert.match(text, /llms\.txt/);
});

test("lists: six read-only tools with schemas, three prompts, every page as a resource", async () => {
  const { tools } = (await rpc("tools/list")).result;
  assert.deepEqual(tools.map((t) => t.name), ["guide", "example", "check", "explain_error", "kafka_client", "setup"]);
  for (const t of tools) {
    assert.equal(t.inputSchema.type, "object");
    assert.equal(t.annotations.readOnlyHint, true);
  }
  const { prompts } = (await rpc("prompts/list")).result;
  assert.deepEqual(prompts.map((p) => p.name), ["design", "review", "from-kafka"]);
  const { resources } = (await rpc("resources/list")).result;
  assert.equal(resources.length, bundle.pages.length + 1);
  const read = (await rpc("resources/read", { uri: "https://queenmq.com/concepts/transactions/" })).result;
  assert.match(read.contents[0].text, /transaction/i);
});

// ---------------------------------------------------------------- tools

test("guide: a topic finds its section; a page and a section read in full", async () => {
  const topic = await call("guide", { topic: "where does a new consumer group start" });
  assert.match(topic.text, /concepts\/consuming/);
  assert.match(topic.text, /subscriptionMode/);
  const page = await call("guide", { page: "https://queenmq.com/concepts/timers/" });
  assert.match(page.text, /^https:\/\/queenmq\.com\/concepts\/timers\//);
  const missing = await call("guide", { page: "concepts/nope" });
  assert.equal(missing.isError, true);
  assert.equal((await call("guide", {})).isError, true);
});

test("example: the tested snippet named after the task comes first", async () => {
  const go = await call("example", { task: "consume with a consumer group", language: "go" });
  assert.match(go.text, /^### Go: consume/);
  assert.match(go.text, /a file Queen's test suite runs/);
  const list = await call("example", { task: "list", language: "typescript" });
  assert.match(list.text, /Tested JavaScript snippets: .*transaction/);
});

test("example: docs code fills what no tested snippet covers, and says so", async () => {
  const kv = await call("example", { task: "kv put get", language: "js" });
  assert.match(kv.text, /queen\.kv\.put/);
  assert.match(kv.text, /not cut from a tested file/);
  const php = await call("example", { task: "kv state", language: "php" });
  assert.match(php.text, /^No PHP code covers/);
  assert.match(php.text, /Translate it with the PHP SDK's own names/);
  assert.equal((await call("example", { task: "x", language: "kafka" })).isError, true);
});

test("example: tested code in the asked language beats another language's docs code", async () => {
  const go = await call("example", { task: "schedule a timer in a transaction", language: "go" });
  assert.match(go.text, /^### Go: excerpt from examples\/apps\/go\/saga\/main\.go/);
  assert.match(go.text, /ScheduleTimerOp\(queen\.TimerSchedule\{/);
});

test("example answers carry the language's install line and import path", async () => {
  const go = await call("example", { task: "consume", language: "go" });
  assert.match(go.text, /go get github\.com\/smartpricing\/queen\/clients\/client-go/);
  assert.match(go.text, /\*queen\.Queen/);
  const js = await call("example", { task: "consume", language: "js" });
  assert.match(js.text, /npm install queen-mq/);
});

test("example: full returns the whole program of an example app", async () => {
  const saga = await call("example", { task: "saga", language: "go", full: true });
  assert.match(saga.text, /the whole program, examples\/apps\/go\/saga\/main\.go/);
  assert.match(saga.text, /package main/);
});

test("check: an SDK gets the broker traps and its own; kafka gets only the Kafka ones", async () => {
  const js = await call("check", { language: "js" });
  const kafka = await call("check", { language: "kafka" });
  const all = bundle.traps.filter((t) => t.applies.includes("all") || t.applies.includes("js")).length;
  if (bundle.traps.length) {
    assert.match(js.text, new RegExp(`${all} items`));
    for (const t of bundle.traps.filter((x) => x.applies.includes("kafka") && !x.applies.includes("all"))) assert.ok(kafka.text.includes(t.id));
  }
  assert.equal((await call("check", { language: "cobol" })).isError, true);
});

test("explain_error: exact codes, Kafka numbers, HTTP statuses", async () => {
  const reason = await call("explain_error", { error: "rejected_ack" });
  assert.match(reason.text, /^\*\*`rejected_ack`\*\*/);
  const kafka15 = await call("explain_error", { error: "15" });
  assert.match(kafka15.text, /COORDINATOR_NOT_AVAILABLE/);
  const named = await call("explain_error", { error: "coordinator_not_available" });
  assert.match(named.text, /Kafka error 15/);
  const status = await call("explain_error", { error: "429" });
  assert.match(status.text, /Retry-After/);
  assert.equal((await call("explain_error", { error: "" })).isError, true);
});

test("kafka_client: finds clients by name or alias, with config and evidence", async () => {
  const sarama = await call("kafka_client", { client: "sarama", version: "1.42" });
  assert.match(sarama.text, /IBM\/sarama: PARTIAL/);
  assert.match(sarama.text, /V1_0_0_0/);
  const dotnet = await call("kafka_client", { client: ".NET" });
  assert.match(dotnet.text, /Confluent\.Kafka/);
  const unknown = await call("kafka_client", { client: "zzz-unknown" });
  assert.match(unknown.text, /not in Queen's Kafka client matrix/);
});

test("setup: lists the recipes, and every recipe names pages the docs build has", async () => {
  const list = await call("setup", {});
  for (const f of bundle.setup) assert.ok(list.text.includes(`\`${f.feature}\``), f.feature);
  const pages = new Set(bundle.pages.map((p) => p.slug));
  const missing = bundle.setup.flatMap((f) => f.pages.filter((p) => !pages.has(p)).map((p) => `${f.feature}: ${p}`));
  // guides/s3 lands with the S3 sink docs; everything else must resolve.
  assert.deepEqual(missing.filter((m) => !m.startsWith("s3-sink")), []);
  assert.equal((await call("setup", { feature: "nope" })).isError, true);
});

test("setup: a recipe is its docs page, and focuses Postgres on source or sink", async () => {
  const kv = await call("setup", { feature: "kv" });
  assert.match(kv.text, /^# Set up: KV state/);
  assert.match(kv.text, /https:\/\/queenmq\.com\/concepts\/kv\//);
  if (bundle.pages.some((p) => p.slug === "guides/postgres")) {
    const sink = await call("setup", { feature: "pg-sink" });
    assert.match(sink.text, /## How a message becomes a row/);
    assert.doesNotMatch(sink.text, /## The change events/);
    const source = await call("setup", { feature: "pg-source" });
    assert.match(source.text, /## The change events/);
    assert.doesNotMatch(source.text, /## How a message becomes a row/);
  }
});

test("setup: with a language, the API card follows the docs", async () => {
  const go = await call("setup", { feature: "timers", language: "go" });
  assert.match(go.text, /## In Go/);
  const raw = await handle(new Request("https://queenmq.com/mcp/setup/timers.md"));
  assert.equal(raw.status, 200);
  assert.match(await raw.text(), /^# Set up: Timers/);
  assert.equal((await handle(new Request("https://queenmq.com/mcp/setup/nope.md"))).status, 404);
});

test("prompts fill their arguments and defaults", async () => {
  const design = (await rpc("prompts/get", { name: "design", arguments: { flow: "hotel booking with payment timeout" } })).result;
  const text = design.messages[0].content.text;
  assert.match(text, /hotel booking with payment timeout/);
  assert.doesNotMatch(text, /\{\{/);
  const bare = (await rpc("prompts/get", { name: "design", arguments: {} })).result.messages[0].content.text;
  assert.match(bare, /ask the user to describe it/);
  const review = (await rpc("prompts/get", { name: "review", arguments: {} })).result;
  assert.match(review.messages[0].content.text, /the project's language/);
});

test("the primer stays inside its budget", () => {
  const words = bundle.primer.split(/\s+/).filter(Boolean).length;
  assert.ok(words <= 760, `primer is ${words} words`);
});
