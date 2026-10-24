// Chapter 1.2: the model the Postgres hack turned into, as the first schema wrote
// it (src/database/schema-v2.sql, 2025-10-14): four tables.
function table(c, x, y, w, name, cols, o = {}) {
  const rowH = 25;
  const h = 40 + cols.length * rowH + 8;
  c.rect(x, y, w, h, { tone: o.tone ?? "plain", r: 8 });
  c.rect(x, y, w, 38, { tone: o.tone === "accent" ? "solid" : "strong", r: 8 });
  c.raw(`<rect x="${x + 1}" y="${y + 26}" width="${w - 2}" height="12" fill="${o.tone === "accent" ? c.p.accent : c.p.paper}"/>`);
  c.text(x + 14, y + 20, name, { size: 16.5, weight: 700, mono: true, color: o.tone === "accent" ? "#1b1916" : c.p.ink });
  cols.forEach((col, i) => {
    const cy = y + 40 + 8 + i * rowH + 10;
    c.text(x + 14, cy, col[0], { size: 14.5, mono: true, weight: col[2] ? 600 : 400 });
    if (col[1]) c.text(x + w - 12, cy, col[1], { size: 13, anchor: "end", tone: "muted" });
  });
  return { x, y, w, h };
}
export default {
  id: "sql-schema",
  height: 400,
  alt: "Four tables. queues (id, name, lease_time, retry_limit, retry_delay) has many partitions (id, queue_id, name, unique per queue). A partition has many messages (id, transaction_id unique, partition_id, payload, created_at) and many partition_consumers (partition_id, consumer_group, last_consumed_id as the cursor, lease_expires_at and worker_id as the lease, unique per partition and group).",
  draw(c) {
    const q = table(c, 20, 30, 250, "queues", [["id", "uuid", 1], ["name", "unique"], ["lease_time", "300"], ["retry_limit", "3"], ["retry_delay", "1000"], ["…", ""]]);
    const p = table(c, 360, 30, 270, "partitions", [["id", "uuid", 1], ["queue_id", "→ queues"], ["name", "'Default'"], ["unique (queue_id, name)", ""]], { tone: "accent" });
    const m = table(c, 720, 30, 260, "messages", [["id", "uuid", 1], ["transaction_id", "unique"], ["partition_id", "→ partitions"], ["payload", "jsonb"], ["created_at", ""]]);
    const pc = table(c, 360, 220, 340, "partition_consumers", [["partition_id", "→ partitions"], ["consumer_group", "'__QUEUE_MODE__'"], ["last_consumed_id", "the cursor"], ["lease_expires_at, worker_id", "the lease"], ["unique (partition_id, consumer_group)", ""]]);
    c.arrow([[q.x + q.w, 69], [p.x - 4, 69]], { tone: "muted", label: "1 : n" });
    c.arrow([[p.x + p.w, 69], [m.x - 4, 69]], { tone: "muted", label: "1 : n" });
    c.arrow([[495, p.y + 154], [495, pc.y - 4]], { tone: "muted", label: "1 : n", at: 0.45 });
    c.text(20, 300, "A partition is a row,", { size: 15, tone: "muted" });
    c.text(20, 322, "created by the first message", { size: 15, tone: "muted" });
    c.text(20, 344, "that names it.", { size: 15, tone: "muted" });
    c.text(980, 300, "One cursor and one lease", { size: 15, anchor: "end", tone: "muted" });
    c.text(980, 322, "per partition and group:", { size: 15, anchor: "end", tone: "muted" });
    c.text(980, 344, "order without a fixed owner.", { size: 15, anchor: "end", tone: "muted" });
  },
};
