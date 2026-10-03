// The Worker's handler behind node:http, for running the server locally:
//   npm run dev   ->   claude mcp add --transport http queen-local http://localhost:8787/mcp

import { createServer } from "node:http";
import { handle } from "./src/mcp.mjs";

const port = Number(process.env.PORT ?? 8787);

createServer(async (req, res) => {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const hasBody = req.method !== "GET" && req.method !== "HEAD";
  const request = new Request(`http://localhost:${port}${req.url}`, {
    method: req.method,
    headers: Object.entries(req.headers).map(([k, v]) => [k, Array.isArray(v) ? v.join(", ") : v]),
    body: hasBody ? Buffer.concat(chunks) : undefined,
  });
  const response = await handle(request);
  res.writeHead(response.status, Object.fromEntries(response.headers));
  res.end(Buffer.from(await response.arrayBuffer()));
}).listen(port, () => console.log(`Queen MCP server on http://localhost:${port}/mcp`));
