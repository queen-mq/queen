import { flow } from "@/lib/figure-spec";

// concepts/index.mdx, "Entity, partition, step, log".
export default flow({
  alt: "Producers push events for order-9137 into that order's own partition, which keeps them in order. A worker of the consumer group leases the partition's next batch, and its step ends in one commit: one log entry holding the ack, the state change, the next events and any timer. Other entities have partitions of their own, leased to other workers in parallel, and their steps commit into the same log.",
  caption: "An entity's events wait in its own partition, one worker at a time takes the next batch, and the step it runs ends in a single entry of the log.",
  cols: 3,
  rows: 3,
  colWidth: 226,
  rowHeight: 96,
  gap: 64,
  nodes: [
    { id: "producers", at: [0, 0], label: "producers", sub: "push to order-9137", tone: "ghost", shape: "pill" },
    { id: "part", at: [1, 0], label: "order-9137", sub: "one entity, in order", mono: true },
    { id: "others", at: [2, 0], label: "other entities", sub: "a partition each", shape: "stack" },
    { id: "worker", at: [1, 1], label: "worker", sub: "holds the lease", tone: "ghost", shape: "pill" },
    { id: "workers", at: [2, 1], label: "other workers", sub: "in parallel", tone: "ghost", shape: "stack" },
    { id: "entry", at: [1, 2], label: "one log entry", sub: "ack, state, events, timer", shape: "disk", tone: "strong" },
  ],
  edges: [
    { from: "producers", to: "part", label: "push" },
    { from: "part", to: "worker", label: "next batch" },
    { from: "others", to: "workers" },
    { from: "worker", to: "entry", label: "one commit", tone: "strong" },
    { from: "workers", to: "entry", fromSide: "b", toSide: "r", label: "theirs" },
  ],
});
