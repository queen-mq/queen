// The Model Context Protocol over Streamable HTTP, stateless: every POST is
// answered on its own with a JSON body, so the server keeps no sessions and
// never opens an SSE stream. That is all a read-only knowledge server needs,
// and it runs the same on a Worker and on `node dev.mjs`.

import {
  agentsMd,
  callTool,
  getPrompt,
  instructions,
  listPrompts,
  listResources,
  listTools,
  readResource,
  RpcError,
  serverInfo,
  setupMarkdown,
} from "./tools.mjs";

export const PROTOCOL_VERSIONS = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

const CORS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, POST, DELETE, OPTIONS",
  "access-control-allow-headers": "content-type, accept, authorization, mcp-protocol-version, mcp-session-id, last-event-id",
  "access-control-expose-headers": "mcp-session-id, mcp-protocol-version",
  "access-control-max-age": "86400",
};

const FOR_HUMANS = `This is Queen MQ's MCP server for coding agents. It answers the Model Context Protocol over POST.

Add it to your agent:

  claude mcp add --transport http queen https://queenmq.com/mcp

Every other agent: https://queenmq.com/start/ai-agents/
`;

const text = (status, body, headers = {}) =>
  new Response(body, { status, headers: { ...CORS, "content-type": "text/plain; charset=utf-8", ...headers } });

const json = (status, body) =>
  new Response(JSON.stringify(body), { status, headers: { ...CORS, "content-type": "application/json" } });

const result = (id, value) => ({ jsonrpc: "2.0", id, result: value });
const failure = (id, code, message) => ({ jsonrpc: "2.0", id, error: { code, message } });

export async function handle(request) {
  const path = new URL(request.url).pathname.replace(/\/+$/, "");

  if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });

  if (/\/agents\.md$/i.test(path)) {
    if (request.method !== "GET" && request.method !== "HEAD") return text(405, "GET only.\n", { allow: "GET, HEAD" });
    return new Response(request.method === "HEAD" ? null : agentsMd(), {
      headers: { ...CORS, "content-type": "text/markdown; charset=utf-8", "cache-control": "public, max-age=300" },
    });
  }

  const recipe = path.match(/^\/mcp\/setup\/([a-z0-9-]+)\.md$/);
  if (recipe) {
    const markdown = setupMarkdown(recipe[1]);
    if (!markdown) return text(404, "No such setup recipe.\n");
    return new Response(request.method === "HEAD" ? null : markdown, {
      headers: { ...CORS, "content-type": "text/markdown; charset=utf-8", "cache-control": "public, max-age=300" },
    });
  }

  if (path !== "/mcp") return text(404, "Not found. The MCP endpoint is /mcp.\n");
  if (request.method === "GET" || request.method === "HEAD") return text(405, FOR_HUMANS, { allow: "POST, OPTIONS" });
  if (request.method === "DELETE") return text(405, "This server keeps no sessions.\n", { allow: "POST, OPTIONS" });
  if (request.method !== "POST") return text(405, "Method not allowed.\n", { allow: "POST, OPTIONS" });

  const version = request.headers.get("mcp-protocol-version");
  if (version && !PROTOCOL_VERSIONS.includes(version)) {
    return json(400, failure(null, -32600, `Unsupported MCP-Protocol-Version "${version}". Supported: ${PROTOCOL_VERSIONS.join(", ")}.`));
  }
  if (!(request.headers.get("content-type") ?? "").toLowerCase().includes("application/json")) {
    return json(415, failure(null, -32700, "Content-Type must be application/json."));
  }

  let body;
  try {
    body = JSON.parse(await request.text());
  } catch {
    return json(400, failure(null, -32700, "Parse error."));
  }
  const batch = Array.isArray(body);
  const messages = batch ? body : [body];
  if (!messages.length) return json(400, failure(null, -32600, "Empty batch."));

  const replies = [];
  for (const message of messages) {
    const reply = await dispatch(message);
    if (reply) replies.push(reply);
  }
  if (!replies.length) return new Response(null, { status: 202, headers: CORS });
  return json(200, batch ? replies : replies[0]);
}

async function dispatch(message) {
  if (!message || typeof message !== "object" || message.jsonrpc !== "2.0") {
    return failure(message?.id ?? null, -32600, "Invalid request.");
  }
  if (typeof message.method !== "string") return null; // a response to us: we never ask, so nothing to do
  if (!("id" in message)) return null; // a notification
  const { id, method } = message;
  const params = message.params ?? {};
  try {
    switch (method) {
      case "initialize": {
        const asked = params.protocolVersion;
        return result(id, {
          protocolVersion: PROTOCOL_VERSIONS.includes(asked) ? asked : PROTOCOL_VERSIONS[0],
          capabilities: {
            tools: { listChanged: false },
            prompts: { listChanged: false },
            resources: { listChanged: false, subscribe: false },
          },
          serverInfo: serverInfo(),
          instructions: instructions(),
        });
      }
      case "ping":
        return result(id, {});
      case "tools/list":
        return result(id, { tools: listTools() });
      case "tools/call":
        return result(id, await callTool(params.name, params.arguments));
      case "prompts/list":
        return result(id, { prompts: listPrompts() });
      case "prompts/get":
        return result(id, getPrompt(params.name, params.arguments ?? {}));
      case "resources/list":
        return result(id, listResources());
      case "resources/templates/list":
        return result(id, { resourceTemplates: [] });
      case "resources/read":
        return result(id, readResource(params.uri));
      default:
        return failure(id, -32601, `Method not found: ${method}`);
    }
  } catch (e) {
    if (e instanceof RpcError) return failure(id, e.code, e.message);
    console.error(e);
    return failure(id, -32603, "Internal error.");
  }
}
