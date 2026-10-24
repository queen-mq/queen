// Chapter 4.3: the PostgreSQL sink: offsets commit with the rows.
export default {
  id: "pg-sink",
  height: 280,
  alt: "The sink is a consumer group the broker runs, pg-orders-copy. A worker takes a batch of the queue orders and writes it in one PostgreSQL transaction: it locks the partition's row in queen.sink_progress, drops every message at or below the last applied offset, writes the rest to the table, and moves the offset. Only after that commit does it ack the batch in Queen. A redelivered message finds its offset already there and is skipped.",
  draw(c) {
    const q = c.box(20, 90, 170, 70, "orders", { sub: "a Queen queue", mono: true });
    const w = c.box(250, 80, 200, 90, "sink worker", { sub: "group pg-orders-copy", tone: "strong" });
    c.arrow([[q.x + q.w + 4, 125], [w.x - 6, 125]], { tone: "ink", label: "batch" });
    c.rect(520, 30, 460, 190, { tone: "info", r: 12 });
    c.text(750, 56, "one PostgreSQL transaction", { size: 15.5, weight: 700, anchor: "middle" });
    ["lock the partition's row in queen.sink_progress", "drop what is at or below its last offset", "write the rest to the table", "move the offset forward"].forEach((s, i) => {
      c.step(548, 92 + i * 32, i + 1, { r: 11 });
      c.text(568, 92 + i * 32, s, { size: 14 });
    });
    c.arrow([[w.x + w.w + 4, 125], [514, 125]], { tone: "info", width: 2.2 });
    c.arrow([[750, 226], [750, 256], [350, 256], [350, 176]], { tone: "ok", label: "then: ack the batch in Queen", labelAt: [550, 268], labelBg: false });
  },
};
