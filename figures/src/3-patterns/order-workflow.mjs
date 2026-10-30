// Chapter 3.5: a state machine whose steps cannot race.
export default {
  id: "order-workflow",
  height: 350,
  alt: "The state of order-2001 lives in KV: paid, at version 1. The workflow step reads it, does its work, and commits one transaction: the ack of the event, the KV write to shipping only if the version is still 1, and the push of the next event to shipping. The same step sent again from the old version finds version 2: the whole transaction rolls back and nothing is written, so there is still one shipping event.",
  draw(c) {
    const states = [["paid", "v1"], ["shipping", "v2"], ["shipped", "v3"]];
    states.forEach(([s, v], i) => {
      const b = c.box(40 + i * 330, 30, 230, 60, s, { sub: `order-2001, version ${v.slice(1)}`, tone: i === 1 ? "accent" : "plain" });
      if (i < 2) c.arrow([[b.x + b.w + 4, b.cy], [b.x + 324, b.cy]], { tone: i === 0 ? "ok" : "faint", label: i === 0 ? "the step" : "next step", dash: i === 1 ? "6 5" : undefined });
    });
    c.rect(40, 130, 560, 192, { tone: "plain", r: 12 });
    c.text(60, 156, "the step: one transaction", { size: 16, weight: 700 });
    c.text(60, 186, "// Do the work of this step here: charge the card, ...", { size: 13, mono: true, tone: "faint" });
    [["ack", "order-events, group workflow"], ["kv put", "orders/order-2001 = shipping, expect: v1"], ["push", "shipping: ship-order-2001"]].forEach(([op, what], i) => {
      const y = 222 + i * 32;
      c.text(60, y, op, { size: 14.5, mono: true, weight: 600 });
      c.text(140, y, what, { size: 14, tone: "muted" });
    });
    c.rect(640, 130, 340, 192, { tone: "danger", r: 12 });
    c.text(660, 156, "the same step, sent again", { size: 16, weight: 700 });
    c.text(660, 192, "expects v1, finds v2", { size: 14.5 });
    c.text(660, 222, "the whole transaction rolls back:", { size: 14.5 });
    c.text(660, 248, "no state write, no second event", { size: 14.5, tone: "muted" });
    c.text(660, 280, "commit() returns success: false", { size: 13.5, mono: true, tone: "danger" });
  },
};
