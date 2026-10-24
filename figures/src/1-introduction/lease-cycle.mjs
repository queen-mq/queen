// Chapter 1.3 (Leases): a partition is free, leased to one worker of a group, and
// free again after the ack or when the lease runs out. No static assignment.
export default {
  id: "lease-cycle",
  height: 345,
  alt: "A partition's cycle for one consumer group. Free: any worker that asks gets its next batch, and the partition becomes leased to that worker for leaseTime; no other worker of the group gets it. The worker acks: the group's cursor moves past the batch and the partition is free again. Or the worker dies: the lease runs out and the same messages go to the next worker that asks.",
  draw(c) {
    const free = c.box(60, 120, 200, 76, "free", { sub: "any worker may take it", tone: "plain" });
    const leased = c.box(400, 120, 220, 76, "leased to w2", { sub: "for leaseTime (60 s)", tone: "accent" });
    const done = c.box(760, 120, 200, 76, "acked", { sub: "cursor moves past it", tone: "ok" });
    c.arrow([[free.x + free.w + 4, 145], [leased.x - 6, 145]], { label: "w2 asks: pop", tone: "ink" });
    c.arrow([[leased.x + leased.w + 4, 145], [done.x - 6, 145]], { label: "ack completed", tone: "ok" });
    c.arrow([[done.cx, done.y - 6], [done.cx, 50], [free.cx, 50], [free.cx, free.y - 8]], { label: "free again, for whoever asks next", tone: "muted", labelAt: [510, 36], labelBg: false });
    c.arrow([[leased.cx, leased.y + leased.h + 6], [leased.cx, 270], [free.cx, 270], [free.cx, free.y + free.h + 8]], { label: "w2 dies: the lease runs out, the same batch goes to the next worker", tone: "danger", dash: "6 5", labelAt: [380, 292], labelBg: false });
    c.text(500, 330, "While leased, other workers of the same group skip the partition, so its order holds.", { size: 14.5, anchor: "middle", tone: "muted", italic: true });
  },
};
