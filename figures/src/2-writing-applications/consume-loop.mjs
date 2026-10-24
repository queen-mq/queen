// Chapter 2.4: what consume() does for you.
export default {
  id: "consume-loop",
  height: 260,
  alt: "The consume loop. It pops a batch, which leases the partition; calls your handler; acks when the handler returns, or fails the message when it throws; and goes back to pop. With autoAck off, your handler acks itself, often inside a transaction. pop() is one turn of the loop, done by hand.",
  draw(c) {
    const pop = c.box(60, 96, 170, 64, "pop", { sub: "lease a batch", tone: "accent" });
    const h = c.box(410, 96, 190, 64, "your handler", { sub: "msg or messages", tone: "strong" });
    const ack = c.box(780, 40, 180, 60, "ack completed", { sub: "handler returned", tone: "ok", size: 16 });
    const fail = c.box(780, 156, 180, 60, "ack failed", { sub: "handler threw", tone: "danger", size: 16 });
    c.arrow([[pop.x + pop.w + 4, 128], [h.x - 6, 128]], { tone: "ink" });
    c.arrow([[h.x + h.w + 4, 116], [ack.x - 6, 70]], { tone: "ok" });
    c.arrow([[h.x + h.w + 4, 140], [fail.x - 6, 186]], { tone: "danger" });
    c.arrow([[870, 222], [870, 244], [145, 244], [145, 166]], { tone: "muted", label: "next batch", labelAt: [500, 244] });
    c.arrow([[870, 36], [870, 18], [145, 18], [145, 90]], { tone: "muted" });
    c.text(500, 76, "autoAck(false): the handler acks itself", { size: 14, anchor: "middle", tone: "muted", italic: true });
  },
};
