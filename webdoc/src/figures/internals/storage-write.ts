import { flow } from "@/lib/figure-spec";

// internals/storage.mdx, "The queue logs are the write-ahead log" and "When a
// write is durable" (rsm/qlog/mod.rs).
export default flow({
  alt: "How an entry that touches two queues reaches the disk. The writer puts the payloads into each queue's own log, the whole entry record into the lowest of the touched logs and a 61-byte stub naming it into the other, then fsyncs each touched log once. Only then does the entry go to apply, which updates the store in RAM. Once a second a checkpoint writes the changed store rows to store/data.mdb, off the answer path.",
  caption: "A message is written once, in its queue's log, and that write is the one that makes it durable. The store checkpoint only bounds what a restart replays; no answer waits for it.",
  source: "server/src/rsm/qlog/mod.rs, server/src/rsm/store/",
  cols: 3,
  rows: 4,
  colWidth: 228,
  rowHeight: 86,
  gap: 60,
  nodes: [
    { id: "entry", at: [1, 0], label: "one entry", sub: "orders and receipts", tone: "strong" },
    { id: "la", at: [0, 1], label: "qlog/q12/", sub: "payloads + entry record", mono: true, shape: "disk" },
    { id: "lb", at: [2, 1], label: "qlog/q31/", sub: "payloads + 61-byte stub", mono: true, shape: "disk" },
    { id: "store", at: [1, 2], label: "store, in RAM", sub: "cursors, KV, timers" },
    { id: "mdb", at: [1, 3], label: "store/data.mdb", sub: "checkpoint, once a second", mono: true, shape: "disk" },
  ],
  edges: [
    { from: "entry", to: "la", label: "write, fsync once", tone: "strong" },
    { from: "entry", to: "lb", label: "write, fsync once", tone: "strong" },
    { from: "entry", to: "store", label: "then apply" },
    { from: "store", to: "mdb", label: "off the answer path", dashed: true },
  ],
});
