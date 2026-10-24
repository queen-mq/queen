// Chapter 2.5: in a batch, the cursor stops at the first message that will come back.
export default {
  id: "batch-ack-rule",
  height: 210,
  alt: "A batch of four messages acked with different statuses: offset 0 completed, offset 1 failed, offsets 2 and 3 completed. The cursor moves past offset 0 only, and stops before offset 1: offsets 1, 2 and 3 are delivered again, the completed ones included, so the partition's order holds.",
  draw(c) {
    const cells = [
      { label: "0", tone: "ok", sub: "completed" },
      { label: "1", tone: "danger", sub: "failed" },
      { label: "2", tone: "ok", sub: "completed" },
      { label: "3", tone: "ok", sub: "completed" },
    ];
    const s = c.strip(260, 80, cells, { cellW: 100, cellH: 46, gap: 10 });
    c.cursor(s.xs[1] - 5, 38, 76, "cursor stops here", { tone: "accent" });
    c.rect(s.xs[1] - 3, 160, s.end - s.xs[1] + 6, 30, { tone: "danger", r: 6 });
    c.text((s.xs[1] + s.end) / 2, 175, "delivered again, completed ones too", { size: 14, anchor: "middle", weight: 500 });
    c.text(20, 103, "one batch,", { size: 14.5, tone: "muted" });
    c.text(20, 123, "one ack call", { size: 14.5, tone: "muted" });
  },
};
