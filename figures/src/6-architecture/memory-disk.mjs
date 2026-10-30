// Chapters 6.4, 6.6, 6.7: what lives in memory and what lives on disk.
export default {
  id: "memory-disk",
  height: 330,
  alt: "In memory, on every node: the state, which is queues, partitions, consumer positions, KV, timers, request ids and the dedup records of recent pushes, loaded once at boot. On disk: the queue log files, which hold every message and are the log itself; store/data.mdb, the LMDB checkpoint of the state, written four times a second and read at boot; and traces/data.mdb, read from disk. Messages are never loaded into memory: they are read from the queue log files.",
  draw(c) {
    c.rect(20, 30, 960, 120, { tone: "accent", r: 12 });
    c.text(40, 56, "memory: the state, loaded once at boot", { size: 16, weight: 700 });
    ["queues", "partitions", "consumer positions", "KV + timers", "request ids", "recent dedup records"].forEach((t, i) => c.box(40 + i * 155, 76, 145, 48, t, { size: 13.5, weight: 500 }));
    c.line(20, 170, 980, 170, { tone: "line", dash: "5 5" });
    c.text(40, 190, "disk", { size: 16, weight: 700 });
    const q = c.file(40, 210, 290, 90, "qlog/…/r00000001.qlog", { sub: "every message; the log itself" });
    const s = c.file(370, 210, 280, 90, "store/data.mdb", { sub: "checkpoint of the state, 4×/s" });
    const t = c.file(690, 210, 290, 90, "traces/data.mdb", { sub: "message traces, read from disk" });
    c.arrow([[s.cx, 150], [s.cx, s.y - 4]], { tone: "muted", both: true, label: "checkpoint / boot" });
    void q; void t;
  },
};
