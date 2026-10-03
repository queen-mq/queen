import { flow } from "@/lib/figure-spec";

// concepts/partitions.mdx, "Order and parallelism".
export default flow({
  alt: "Partitions of one queue, each leased to at most one worker of the consumer group. Partition cust-4 is stuck in a slow step on worker 1, and only cust-4 waits. Partitions cust-9 and cust-2 are leased to workers 2 and 3 and move on. The partitions of every other customer wait for a free worker, not for cust-4.",
  caption: "A group leases one batch per partition at a time, so a slow entity holds up its own partition and its own worker, never the customers behind it.",
  cols: 2,
  rows: 4,
  colWidth: 300,
  rowHeight: 70,
  gap: 120,
  nodes: [
    { id: "p4", at: [0, 0], label: "cust-4", sub: "a slow step", mono: true, tone: "warn" },
    { id: "p9", at: [0, 1], label: "cust-9", mono: true },
    { id: "p2", at: [0, 2], label: "cust-2", mono: true },
    { id: "rest", at: [0, 3], label: "every other customer", sub: "waiting for a free worker", shape: "stack" },
    { id: "w1", at: [1, 0], label: "worker 1", sub: "busy with cust-4", tone: "ghost", shape: "pill" },
    { id: "w2", at: [1, 1], label: "worker 2", tone: "ghost", shape: "pill" },
    { id: "w3", at: [1, 2], label: "worker 3", tone: "ghost", shape: "pill" },
  ],
  edges: [
    { from: "p4", to: "w1", label: "leased batch", tone: "warn" },
    { from: "p9", to: "w2", label: "leased batch" },
    { from: "p2", to: "w3", label: "leased batch" },
  ],
});
