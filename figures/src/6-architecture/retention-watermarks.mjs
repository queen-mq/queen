// Chapter 6.7: retention is decided on the leader, as watermarks.
export default {
  id: "retention-watermarks",
  height: 260,
  alt: "Retention is decided once, on the leader. A scanner walks every partition, 50,000 a second, and proposes what can go. The planner checks each proposal and logs watermarks for the partition: below one the messages are gone, below another their dedup records are gone. Every node applies the watermarks. Moving a watermark frees no disk by itself: each node then frees its own files, one whole file at a time.",
  draw(c) {
    c.box(20, 30, 200, 70, "scanner", { sub: "50,000 partitions/s", tone: "plain" });
    c.arrow([[224, 65], [300, 65]], { tone: "ink", label: "proposals" });
    c.box(306, 30, 200, 70, "planner", { sub: "checks, logs watermarks", tone: "strong" });
    c.arrow([[510, 65], [586, 65]], { tone: "ink", label: "an entry" });
    c.box(592, 30, 200, 70, "every node", { sub: "applies them", tone: "ok" });
    c.text(20, 150, "partition", { size: 14.5, mono: true, tone: "muted" });
    const cells = Array.from({ length: 12 }, (_, i) => ({ label: String(i), tone: i < 4 ? "muted" : "plain" }));
    const s = c.strip(120, 128, cells, { cellW: 58, cellH: 42, gap: 6, size: 14 });
    c.cursor(s.xs[4] - 3, 196, 172, "watermarks: below, gone", { tone: "danger" });
    c.text(980, 244, "A watermark frees no disk by itself: files go one whole file at a time.", { size: 14, anchor: "end", tone: "muted", italic: true });
  },
};
