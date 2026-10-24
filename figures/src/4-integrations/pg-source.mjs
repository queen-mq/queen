// Chapter 4.3: the PostgreSQL source, and why it is exactly once.
export default {
  id: "pg-source",
  height: 360,
  alt: "Changes to the table orders go to PostgreSQL's WAL. The source, running inside the broker on the node that holds its lease, reads them through a replication slot with pgoutput. Each committed PostgreSQL transaction becomes one Queen transaction holding the messages, one per row in the partition named after the row's key, and the WAL position they reach. Only after Queen has committed does the source tell PostgreSQL it may recycle that WAL.",
  draw(c) {
    c.rect(20, 30, 300, 230, { tone: "info", r: 12 });
    c.text(170, 56, "PostgreSQL", { size: 16, weight: 700, anchor: "middle" });
    const t = c.box(40, 76, 260, 46, "table orders", { mono: true, size: 15 });
    const wal = c.box(40, 140, 260, 46, "WAL", { size: 15 });
    const slot = c.box(40, 204, 260, 42, "replication slot, pgoutput", { size: 14.5, weight: 500 });
    c.arrow([[t.cx, t.y + t.h + 2], [t.cx, wal.y - 4]], { tone: "muted", label: "commit" });
    c.arrow([[wal.cx, wal.y + wal.h + 2], [wal.cx, slot.y - 4]], { tone: "muted" });
    const src = c.box(400, 190, 200, 70, "source", { sub: "in the broker, one owner", tone: "strong" });
    c.arrow([[slot.x + slot.w + 4, slot.cy], [src.x - 6, slot.cy]], { tone: "ink", label: "changes" });
    c.rect(680, 150, 300, 150, { tone: "accent", r: 12 });
    c.text(830, 176, "one Queen transaction", { size: 15.5, weight: 700, anchor: "middle" });
    c.text(700, 208, "· messages, one per row,", { size: 14 });
    c.text(712, 228, "partition = the row's key", { size: 14, tone: "muted" });
    c.text(700, 256, "· the WAL position they reach", { size: 14 });
    c.text(712, 276, "(the pointer, in KV)", { size: 14, tone: "muted" });
    c.arrow([[src.x + src.w + 4, src.cy], [674, src.cy]], { tone: "accent", width: 2.2 });
    c.arrow([[830, 306], [830, 330], [170, 330], [170, 266]], { tone: "ok", label: "only then: PostgreSQL may recycle the WAL", labelAt: [500, 346], labelBg: false });
  },
};
