// Chapter 6 opening: one leader per tenant. Raft groups across three nodes.
export default {
  id: "raft-groups",
  height: 320,
  alt: "Three nodes, each holding a copy of three raft groups, g0, g1 and g2. Each group has its own log and its own leader: here g0 is led on node 1, g1 on node 2 and g2 on node 3, so every node leads one group. A tenant lives in one group: acme in g0, globex in g1. A transaction of acme across three queues is one entry in g0's log.",
  draw(c) {
    const nodes = ["node 1", "node 2", "node 3"];
    nodes.forEach((n, i) => {
      const x = 40 + i * 320;
      c.rect(x, 30, 280, 220, { tone: "strong", r: 12 });
      c.text(x + 140, 56, n, { size: 16.5, weight: 700, anchor: "middle" });
      ["g0", "g1", "g2"].forEach((g, j) => {
        const lead = i === j;
        const y = 76 + j * 56;
        c.rect(x + 20, y, 240, 44, { tone: lead ? "accent" : "plain", r: 8 });
        c.text(x + 36, y + 22, g, { size: 15, mono: true, weight: 700 });
        c.text(x + 76, y + 22, lead ? "leader" : "follower", { size: 14, tone: lead ? "accent" : "muted", weight: lead ? 600 : 400 });
        c.text(x + 244, y + 22, j === 0 ? "acme" : j === 1 ? "globex" : "…", { size: 13, anchor: "end", tone: "muted", mono: true });
      });
    });
    c.text(500, 280, "One leader per tenant: an acme transaction across ten queues is one entry in g0's log.", { size: 14.5, anchor: "middle", weight: 500 });
    c.text(500, 304, "One tenant's writes go through one leader; more groups spread tenants over more leaders.", { size: 14, anchor: "middle", tone: "muted", italic: true });
  },
};
