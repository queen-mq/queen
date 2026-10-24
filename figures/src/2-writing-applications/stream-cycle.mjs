// Chapter 2.10: one cycle of a stream is one transaction.
export default {
  id: "stream-cycle",
  height: 300,
  alt: "One cycle of a stream. The runner pops a batch of the source queue for one partition, under a lease of the stream's consumer group. It runs the window and the aggregate, reading and writing the window state, which lives in the broker. Then it commits one transaction: the ack of the batch, the new window state, and the push of the closed windows to the output queue. A crash before the commit repeats the cycle; after it, nothing is lost and nothing is counted twice.",
  draw(c) {
    const src = c.box(20, 40, 200, 70, "orders", { sub: "partition customer-42", mono: true });
    const run = c.box(330, 30, 300, 90, "stream runner", { sub: "window + aggregate", tone: "strong" });
    const state = c.disk(380, 170, 200, 86, "window state", { sub: "kept in the broker", tone: "muted" });
    const out = c.box(760, 40, 220, 70, "revenue-per-minute", { sub: "closed windows", mono: true, size: 15 });
    c.arrow([[src.x + src.w + 4, 75], [run.x - 6, 75]], { label: "1. pop a batch", tone: "ink" });
    c.arrow([[run.cx, run.y + run.h + 4], [run.cx, state.y - 6]], { label: "2. read, update", tone: "muted", both: true });
    c.rect(680, 160, 300, 110, { tone: "accent", r: 12 });
    c.text(830, 184, "3. one transaction", { size: 15.5, weight: 700, anchor: "middle" });
    ["ack the batch", "save the window state", "push the closed windows"].forEach((t, i) => c.text(700, 212 + i * 22, `· ${t}`, { size: 14 }));
    c.arrow([[run.x + run.w + 4, 95], [700, 155]], { tone: "accent" });
    c.arrow([[870, 155], [870, out.y + out.h + 6]], { tone: "accent" });
  },
};
