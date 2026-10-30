// Chapter 5.3: tenants through the proxy, placed in raft groups.
export default {
  id: "tenants-groups",
  height: 330,
  alt: "A request arrives at the proxy with an API key on the host acme.queen.example.com. The proxy checks the key, finds the tenant acme from the first label of the host, and hands the request to the broker in the same process under acme's broker tenant id. Each tenant lives in one raft group: acme in group 0 and globex, pinned with QUEEN_TENANT_GROUPS, in group 1, each group with its own leader and log. A key used on another tenant's host is refused with 403.",
  draw(c) {
    c.box(20, 40, 230, 66, "acme.queen.example.com", { sub: "Bearer qk_live_…", mono: true, size: 13.5, tone: "info" });
    c.box(20, 150, 230, 66, "globex.queen.example.com", { sub: "Bearer qk_live_…", mono: true, size: 13.5, tone: "info" });
    const proxy = c.box(330, 70, 220, 120, "proxy :6711", { sub: "key → tenant, plan, quota", tone: "accent" });
    c.arrow([[256, 73], [proxy.x - 6, 110]], { tone: "info" });
    c.arrow([[256, 183], [proxy.x - 6, 150]], { tone: "info" });
    const g0 = c.box(690, 50, 290, 80, "raft group 0", { sub: "acme: own leader, log, data", tone: "strong" });
    const g1 = c.box(690, 140, 290, 80, "raft group 1", { sub: "globex, pinned", tone: "strong" });
    c.arrow([[proxy.x + proxy.w + 4, g0.cy], [g0.x - 6, g0.cy]], { tone: "ink", label: "acme" });
    c.arrow([[proxy.x + proxy.w + 4, g1.cy], [g1.x - 6, g1.cy]], { tone: "ink", label: "globex", below: true });
    c.text(20, 270, "A key works only on its own tenant's host: elsewhere, 403 key/cluster mismatch.", { size: 14, tone: "danger", italic: true });
    c.text(20, 296, "Pinning: QUEEN_TENANT_GROUPS=<broker tenant uuid>=1. With as many groups as nodes, every node leads one.", { size: 14, tone: "muted", italic: true });
  },
};
