import { flow } from "@/lib/figure-spec";

// operate/tenants.mdx, "Tenants and clusters"; concepts/transactions.mdx, "One
// tenant, one raft group" (QUEEN_RAFT_GROUPS, QUEEN_TENANT_GROUPS).
export default flow({
  alt: "Two requests through the proxy. One arrives for acme.queen.example.com with acme's API key, the other for globex.queen.example.com with globex's. The proxy finds each request's cluster from the first label of its Host, checks that the key belongs to it, and hands the request to the broker under that cluster's own broker tenant, a random UUID, so the queues, KV and timers of one cluster are invisible to the other. Each broker tenant lives in exactly one raft group, placed by a hash of its name or by QUEEN_TENANT_GROUPS.",
  caption: "A cluster is a broker tenant of its own, and a tenant is one raft group with one leader, which is why a transaction inside it is one entry and why none can span two.",
  source: "proxy/src/routes.rs, server/src/tenant.rs",
  cols: 3,
  rows: 2,
  colWidth: 228,
  rowHeight: 96,
  gap: 70,
  nodes: [
    { id: "r1", at: [0, 0], label: "acme.queen…", sub: "acme's API key", mono: true, tone: "ghost", shape: "pill" },
    { id: "r2", at: [0, 1], label: "globex.queen…", sub: "globex's API key", mono: true, tone: "ghost", shape: "pill" },
    { id: "proxy", at: [1, 0.5], label: "proxy", sub: "Host → cluster,\nkey checked", tone: "strong" },
    { id: "t1", at: [2, 0], label: "tenant 7f3c…", sub: "acme's queues, KV,\ntimers: one raft group", mono: true },
    { id: "t2", at: [2, 1], label: "tenant 2a91…", sub: "globex's, invisible\nto acme", mono: true },
  ],
  edges: [
    { from: "r1", to: "proxy", fromSide: "r", toSide: "l" },
    { from: "r2", to: "proxy", fromSide: "r", toSide: "l" },
    { from: "proxy", to: "t1", fromSide: "r", toSide: "l" },
    { from: "proxy", to: "t2", fromSide: "r", toSide: "l" },
  ],
});
