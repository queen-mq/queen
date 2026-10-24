// Chapter 3.2: publish/subscribe and fan-out with consumer groups.
export default {
  id: "pubsub",
  height: 300,
  alt: "Orders 3001, 3002 and 3003 are published once to the queue order-placed. Three consumer groups subscribe: billing and shipping from the start, and analytics months later with subscriptionMode all. Each group gets all three orders and keeps its own position. A group created later without subscriptionMode all starts at the end and gets nothing of the past.",
  draw(c) {
    c.box(20, 110, 150, 70, "publisher", { tone: "strong" });
    c.arrow([[176, 145], [256, 145]], { tone: "ink" });
    c.rect(262, 70, 230, 150, { tone: "plain", r: 12 });
    c.text(377, 96, "order-placed", { size: 15.5, weight: 700, mono: true, anchor: "middle" });
    c.strip(282, 122, [{ label: "3001" }, { label: "3002" }, { label: "3003" }], { cellW: 62, cellH: 44, gap: 7, size: 14 });
    c.text(377, 196, "published once", { size: 13.5, anchor: "middle", tone: "muted" });
    const groups = [
      ["billing", "3001, 3002, 3003", "accent", 20],
      ["shipping", "3001, 3002, 3003", "accent", 100],
      ["analytics", "joins months later: all three", "info", 180],
    ];
    groups.forEach(([g, got, tone, y]) => {
      const b = c.box(640, y, 340, 62, g, { sub: got, tone, size: 16 });
      c.arrow([[498, 145], [b.x - 6, b.cy]], { tone: tone === "info" ? "info" : "accent", dash: tone === "info" ? "6 5" : undefined });
    });
    c.text(980, 272, "a group created later without subscriptionMode('all') gets nothing of the past", { size: 13.5, anchor: "end", tone: "danger", italic: true });
  },
};
