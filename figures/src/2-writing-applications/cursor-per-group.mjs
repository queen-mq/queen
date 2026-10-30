// Chapter 2.5: an ack moves a group's cursor; the message stays.
export default {
  id: "cursor-per-group",
  height: 230,
  alt: "One partition with messages at offsets 0 to 7, stored once. The billing group's cursor is after offset 5, and billing holds a lease on offsets 6 and 7. The analytics group's cursor is after offset 2. Acks moved the cursors; no message was deleted. Retention removes messages later, by age.",
  draw(c) {
    const cells = Array.from({ length: 8 }, (_, i) => ({ label: String(i), tone: i >= 6 ? "accent" : "plain" }));
    const s = c.strip(120, 92, cells, { cellW: 82, cellH: 46, gap: 8 });
    c.text(20, 115, "customer-42", { size: 14, mono: true, tone: "muted" });
    c.cursor(s.xs[6] - 4, 48, 88, "billing: acked up to 5", { tone: "accent" });
    c.text((s.xs[6] + s.end) / 2, 158, "billing's lease", { size: 13.5, anchor: "middle", tone: "accent" });
    c.cursor(s.xs[3] - 4, 182, 142, "analytics: acked up to 2", { tone: "info" });
    c.text(980, 210, "The messages stay until retention removes them.", { size: 14, anchor: "end", tone: "muted", italic: true });
  },
};
