import { flow } from "@/lib/figure-spec";

// concepts/consuming.mdx, "Ack, nack and the retry budget".
export default flow({
  alt: "What happens to a delivered message. Acked completed, it moves the group's cursor past it. Acked failed with retries left, it releases the lease, spends one retry and comes back from that point with deliveryAttempt one higher. Acked failed with the retry budget spent, or acked dlq, it is filed in the dead-letter queue with its payload, error, retry count and group. A replay pushes a copy to the end of the partition under the transactionId dlq:<id>.",
  caption: "Only an explicit failed spends the budget; a lease that expires delivers again without spending it. A replay is a new message, so every group reading the partition sees it.",
  cols: 3,
  rows: 2,
  colWidth: 226,
  rowHeight: 92,
  gap: 84,
  nodes: [
    { id: "part", at: [0, 0], label: "partition", sub: "at the cursor" },
    { id: "worker", at: [1, 0], label: "worker", sub: "group billing", tone: "ghost", shape: "pill" },
    { id: "done", at: [2, 0], label: "cursor moves", sub: "on to the next", tone: "strong" },
    { id: "dlq", at: [1, 1], label: "dead-letter queue", sub: "payload, error, retries", tone: "warn", shape: "disk" },
  ],
  edges: [
    { from: "part", to: "worker", label: "delivered", offset: -7 },
    { from: "worker", to: "part", label: "failed: retry", offset: 7, labelSide: "below", dashed: true },
    { from: "worker", to: "done", label: "completed", tone: "strong" },
    { from: "worker", to: "dlq", label: "failed, budget spent, or dlq", tone: "warn" },
    { from: "dlq", to: "part", fromSide: "l", toSide: "b", label: "replay: a copy at the end", dashed: true },
  ],
});
