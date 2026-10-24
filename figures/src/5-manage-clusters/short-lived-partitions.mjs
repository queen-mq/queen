// Chapter 5.3.7: the life of a short-lived partition with a 60-second cleanup.
export default {
  id: "short-lived-partitions",
  height: 270,
  alt: "A timeline for a partition like trace-12. Its last message is pushed and acked. A minute later retention removes the message (completedRetentionSeconds 60): the partition is empty. A minute after its last write it is deleted (PARTITION_CLEANUP_SECONDS 60), but never inside the queue's dedup window nor before QUEEN_RAFT_TXN_WINDOW_MIN_S, so both must be short too. In our test, 50 such partitions were all gone within 76 seconds.",
  draw(c) {
    const x0 = 60, x1 = 960;
    const T = (s) => x0 + (x1 - x0) * (s / 150);
    const ev = [[0, "last push, acked", "ok"], [60, "message removed: empty", "plain"], [72, "partition deleted", "accent"]];
    c.rect(T(0), 110, T(60) - T(0), 28, { tone: "muted", r: 5 });
    c.text((T(0) + T(60)) / 2, 124, "completedRetentionSeconds: 60", { size: 13.5, anchor: "middle", mono: true });
    c.rect(T(0), 150, T(60) - T(0), 28, { tone: "accent", r: 5 });
    c.text((T(0) + T(60)) / 2, 164, "PARTITION_CLEANUP_SECONDS=60", { size: 13.5, anchor: "middle", mono: true });
    c.rect(T(0), 190, T(60) - T(0), 28, { tone: "info", r: 5 });
    c.text((T(0) + T(60)) / 2, 204, "dedup window + TXN_WINDOW_MIN_S: 60", { size: 13.5, anchor: "middle", mono: true });
    ev.forEach(([t, l, tone], i) => {
      c.line(T(t), 70, T(t), 236, { tone: "line", dash: "4 4" });
      c.tag(T(t), 40 + (i === 1 ? 26 : 0), l, { tone, anchor: i === 0 ? "start" : "middle", size: 13 });
    });
    c.text(T(100), 164, "all three must be short:", { size: 14, weight: 600 });
    c.text(T(100), 186, "the longest one wins", { size: 14, tone: "muted" });
    c.axis(x0, x1, 240, [0, 30, 60, 90, 120].map((s) => ({ at: T(s), label: `${s} s` })), {});
  },
};
