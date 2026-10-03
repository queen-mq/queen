import { flow } from "@/lib/figure-spec";

// guides/laravel/concepts.mdx, "Queues and stripes".
export default flow({
  alt: "How Laravel jobs map onto Queen. dispatch() pushes an ordinary job into one of the queue's stripes, laravel-0000 to laravel-0063, chosen by a hash of the job's UUID. A job that implements QueenPartitionable names its own partition, for example customer:42, so one entity's jobs run one at a time in dispatch order. Workers of the consumer group each lease one partition at a time.",
  caption: "Stripes spread ordinary jobs; a partition per entity orders one customer's jobs with one method and no lock.",
  source: "clients/client-php",
  cols: 3,
  rows: 2,
  colWidth: 228,
  rowHeight: 92,
  gap: 64,
  nodes: [
    { id: "app", at: [0, 0.5], label: "Laravel app", sub: "dispatch()", tone: "ghost", shape: "pill" },
    { id: "stripes", at: [1, 0], label: "64 stripes", sub: "laravel-0000 to 0063,\nby job UUID", shape: "stack" },
    { id: "entity", at: [1, 1], label: "customer:42", sub: "QueenPartitionable", mono: true },
    { id: "workers", at: [2, 0.5], label: "workers", sub: "one partition each\nat a time", tone: "ghost", shape: "stack" },
  ],
  edges: [
    { from: "app", to: "stripes", fromSide: "r", toSide: "l" },
    { from: "app", to: "entity", fromSide: "r", toSide: "l" },
    { from: "stripes", to: "workers", fromSide: "r", toSide: "l" },
    { from: "entity", to: "workers", fromSide: "r", toSide: "l" },
  ],
});
