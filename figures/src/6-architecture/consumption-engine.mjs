// Chapter 6.5: pops, acks and leases served from the leader's memory.
export default {
  id: "consumption-engine",
  height: 330,
  alt: "Pops, acks and leases do not go through the planner. The consumption engine on the leader holds each group's positions and leases in memory and gives out the oldest ready partition first. The positions that changed go into the log as one small checkpoint, at once when an answer is waiting and at least every 5 milliseconds; a pop is answered only once that checkpoint is committed. A pop that finds nothing waits and is woken when a message arrives in its partition. A new leader waits about half a second before it serves pops.",
  draw(c) {
    c.box(20, 40, 170, 60, "pop", { tone: "plain" });
    c.box(20, 120, 170, 60, "ack", { tone: "plain" });
    c.box(20, 200, 170, 60, "renew", { tone: "plain" });
    c.rect(260, 30, 380, 240, { tone: "accent", r: 14 });
    c.text(450, 58, "consumption engine, in memory", { size: 16, weight: 700, anchor: "middle" });
    ["positions and leases per group", "oldest ready partition first", "a pop that finds nothing waits:", "woken when a message arrives"].forEach((t, i) => c.text(286, 98 + i * 34, i === 3 ? `   ${t}` : `· ${t}`, { size: 14.5, tone: i === 3 ? "muted" : "ink" }));
    [70, 150, 230].forEach((y) => c.arrow([[194, y], [254, y]], { tone: "muted" }));
    c.box(720, 90, 260, 80, "one checkpoint", { sub: "at once, or ≤ 5 ms", tone: "strong" });
    c.arrow([[644, 130], [714, 130]], { tone: "ink", width: 2.2 });
    c.box(720, 200, 260, 60, "the log", { tone: "ok", sub: "answers wait for its commit" });
    c.arrow([[850, 174], [850, 196]], { tone: "ok" });
    c.text(500, 306, "Many acks travel in one checkpoint; a new leader waits ~0.5 s before serving pops.", { size: 14, anchor: "middle", tone: "muted", italic: true });
  },
};
