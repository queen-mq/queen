// Chapter 2.6: the life of a message: retries, the dead-letter queue, replay.
export default {
  id: "retry-dlq",
  height: 400,
  alt: "A message is delivered, leased to a worker, and ends in one of four ways. Ack completed: the cursor moves past it. Ack failed with retries left: it comes back after retryDelay and one retry is spent. Ack retry, or a lease that ran out: it comes back at once and no retry is spent. The last try failed, or ack dlq: it goes to the dead-letter queue with its payload, error and retries. From the dead-letter queue, a replay pushes a copy to the end of the queue.",
  draw(c) {
    const d = c.box(380, 16, 240, 62, "delivered", { sub: "leased to a worker", tone: "accent" });
    const cols = [
      { x: 20, label: "ack completed", tone: "ok", title: "done", sub: "the cursor moves past it", btone: "ok" },
      { x: 270, label: "ack failed,\nretries left", tone: "ink", title: "back after retryDelay", sub: "one retry spent", btone: "plain" },
      { x: 520, label: "ack retry, or the\nlease ran out", tone: "muted", title: "back at once", sub: "no retry spent", btone: "plain", dash: true },
      { x: 770, label: "last try failed,\nor ack dlq", tone: "danger", title: "dead-letter queue", sub: "payload, error, retries", btone: "danger" },
    ];
    cols.forEach((col, i) => {
      const cx = col.x + 105;
      c.arrow([[d.x + 40 + i * 53, d.y + d.h + 4], [cx, 196]], { tone: col.tone, dash: col.dash ? "6 5" : undefined });
      c.text(cx, 132 - (i === 0 ? 0 : 9), col.label, { size: 14, anchor: "middle", tone: col.tone === "ink" ? "ink" : col.tone, lineHeight: 18 });
      c.box(col.x, 200, 210, 62, col.title, { sub: col.sub, tone: col.btone, size: 16 });
    });
    // the two "back" columns loop to delivered
    c.rect(270, 274, 460, 30, { tone: "muted", r: 6 });
    c.text(500, 289, "both come back in their place: delivered again, in order", { size: 14, anchor: "middle", weight: 500 });
    // replay
    const r = c.box(770, 320, 210, 60, "a copy at the end", { sub: "of the queue: dlq:<id>", tone: "info", size: 15.5 });
    c.arrow([[875, 266], [875, r.y - 6]], { tone: "info", label: "replay", left: false });
    c.text(20, 300, "A message that crashes its worker", { size: 14, tone: "muted", italic: true });
    c.text(20, 320, "never reaches the dead-letter queue:", { size: 14, tone: "muted", italic: true });
    c.text(20, 340, "its lease just runs out, again and again.", { size: 14, tone: "muted", italic: true });
  },
};
