// Chapter 1.3 (Transactions): a worker step without and with a transaction.
export default {
  id: "one-step-one-commit",
  height: 400,
  alt: "Left, without transactions: a worker's step talks to four systems, each with its own commit: the broker (ack), the database (new state and an outbox row), a scheduler (tomorrow's reminder) and an idempotency table. A crash between two commits leaves the step half done. Right, with Queen: the worker sends one transaction, which is one log entry holding the ack, the KV write, the push of the next event and the timer, all or nothing.",
  draw(c) {
    c.text(20, 26, "Without: four systems, four commits", { size: 18, weight: 600 });
    const w = c.box(30, 168, 110, 60, "worker", { tone: "strong" });
    const targets = [
      ["broker", "ack the input"],
      ["database", "state + outbox row"],
      ["scheduler", "reminder in 24 h"],
      ["idempotency table", "mark the step done"],
    ];
    targets.forEach(([name, sub], i) => {
      const y = 58 + i * 74;
      const b = c.box(270, y, 200, 56, name, { sub, size: 16 });
      c.arrow([[w.x + w.w + 4, w.cy], [214, b.cy], [b.x - 6, b.cy]], { tone: "muted" });
      c.text(222, b.cy - 11, `commit ${i + 1}`, { size: 13, tone: "muted" });
    });
    c.tag(30, 372, "a crash between two commits: the step is half done", { tone: "danger", size: 13.5 });

    c.line(500, 20, 500, 385, { tone: "grid", width: 1.4 });

    c.text(530, 26, "With Queen: one transaction, one entry", { size: 18, weight: 600 });
    const w2 = c.box(530, 160, 100, 60, "worker", { tone: "strong" });
    c.rect(720, 70, 260, 240, { tone: "accent", r: 10 });
    c.text(850, 96, "one log entry", { size: 16, weight: 700, anchor: "middle" });
    const lines = [
      ["ack", "orders, group fulfillment"],
      ["kv put", "orders/order-1001 = paid"],
      ["push", "shipments: ship-1001"],
      ["timer", "review-1001 in 24 h"],
    ];
    lines.forEach(([op, what], i) => {
      const y = 132 + i * 44;
      c.rect(736, y - 17, 228, 34, { tone: "plain", r: 6 });
      c.text(748, y, op, { size: 14.5, mono: true, weight: 600 });
      c.text(812, y, what, { size: 13, tone: "muted" });
    });
    c.arrow([[w2.x + w2.w + 4, w2.cy], [716, w2.cy]], { tone: "accent", label: "commit()", width: 2.2, mono: true });
    c.tag(850, 372, "all or nothing", { tone: "ok", anchor: "middle", size: 13.5 });
  },
};
