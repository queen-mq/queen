// Chapter 2.4: where a new consumer group starts, fixed at its first pop.
export default {
  id: "subscription-start",
  height: 220,
  alt: "A partition with messages at offsets 0 to 7, pushed before a new group's first pop. subscriptionMode all starts the group before offset 0 and reads everything. subscriptionFrom a time starts it at the first message from that time, offset 4. The default, new, starts after offset 7 and skips all of them.",
  draw(c) {
    const s = c.strip(150, 96, Array.from({ length: 8 }, (_, i) => ({ label: String(i) })), { cellW: 70, cellH: 46, gap: 8 });
    const x0 = s.xs[0] - 4;
    const x4 = s.xs[4] - 4;
    const x8 = s.end + 4;
    c.cursor(x0, 52, 92, "all", { tone: "ok" });
    c.cursor(x4, 52, 92, "subscriptionFrom(10:00)", { tone: "accent" });
    c.cursor(x8, 52, 92, "new (the default)", { tone: "danger" });
    c.text(20, 119, "pushed before", { size: 14, tone: "muted" });
    c.text(20, 138, "the first pop", { size: 14, tone: "muted" });
    c.text(500, 182, "Fixed at the group's first pop. To move a group later, seek it.", { size: 14.5, anchor: "middle", tone: "muted", italic: true });
    c.text(x4 + 0, 160, "10:00", { size: 13, anchor: "start", tone: "muted", mono: true });
  },
};
