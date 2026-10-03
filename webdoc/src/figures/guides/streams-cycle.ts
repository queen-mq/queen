import { flow } from "@/lib/figure-spec";

// guides/streams.mdx, "What one cycle commits". The cycle is one
// Command::Transaction (server/src/rsm/facade/real/phase2/streams.rs).
export default flow({
  alt: "One cycle of a stream. The runner pops a batch of the source queue sales under a lease, for example from partition cust-17, and runs the chain (filter, window, aggregate) on it. It posts one cycle for that partition, which the broker commits as one transaction: the positional ack of the source batch under its lease, the upserts and deletes of the window state, and the pushes of the results to partition cust-17 of the sink queue sales-per-minute.",
  caption: "The window state, the results and the ack of the events they came from are one transaction, under the batch's lease, so the counter cannot drift from the stream it counted.",
  source: "server/src/rsm/facade/real/phase2/streams.rs, clients/client-js/client-v2/streams/runtime/Runner.js",
  cols: 3,
  rows: 3,
  colWidth: 228,
  rowHeight: 92,
  gap: 64,
  nodes: [
    { id: "source", at: [0, 0], label: "sales", sub: "partition cust-17", mono: true, shape: "disk" },
    { id: "runner", at: [1, 0], label: "runner", sub: "filter, window, aggregate", tone: "ghost", shape: "pill" },
    { id: "cycle", at: [1, 1], label: "one cycle", sub: "one transaction", tone: "strong" },
    { id: "sink", at: [2, 1], label: "sales-per-minute", sub: "partition cust-17", mono: true, shape: "disk" },
    { id: "state", at: [1, 2], label: "window state", sub: "per source partition", shape: "disk" },
  ],
  edges: [
    { from: "source", to: "runner", label: "pop" },
    { from: "runner", to: "cycle", label: "POST /streams/v1/cycle" },
    { from: "cycle", to: "source", fromSide: "l", toSide: "b", label: "ack the batch", tone: "strong" },
    { from: "cycle", to: "sink", label: "push", tone: "strong" },
    { from: "cycle", to: "state", label: "upsert, delete", tone: "strong" },
  ],
});
