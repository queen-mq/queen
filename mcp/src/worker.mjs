import { handle } from "./mcp.mjs";

export default {
  fetch: (request) => handle(request),
};
