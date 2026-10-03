import { flow } from "@/lib/figure-spec";

// guides/state-machines.mdx, "The machine": the table's rows as states and events.
export default flow({
  alt: "The order's states and the events that move it. With no state in KV, order_created makes it created. From created, payment_succeeded makes it paid and payment_timeout, the timer scheduled at creation, makes it cancelled. From paid, shipped makes it shipped. A payment_succeeded that arrives in state cancelled leaves it cancelled and pushes a refund to the refunds queue. Any other event is acked and changes nothing.",
  caption: "The table above as a picture. Every arrow is one transaction: the ack of the event, the new state, and whatever the step pushes or schedules.",
  source: "examples/apps/js/saga.mjs",
  cols: 3,
  rows: 4,
  colWidth: 210,
  rowHeight: 80,
  gap: 60,
  nodes: [
    { id: "none", at: [0, 0], label: "no state", tone: "ghost", shape: "pill" },
    { id: "created", at: [0, 1], label: "created", shape: "pill", tone: "strong" },
    { id: "paid", at: [0, 2], label: "paid", shape: "pill", tone: "strong" },
    { id: "shipped", at: [0, 3], label: "shipped", shape: "pill", tone: "strong" },
    { id: "cancelled", at: [1, 2], label: "cancelled", shape: "pill" },
    { id: "refunds", at: [1, 3], label: "refunds", sub: "a queue", shape: "disk", mono: true },
  ],
  edges: [
    { from: "none", to: "created", label: "order_created" },
    { from: "created", to: "paid", label: "payment_succeeded", tone: "strong" },
    { from: "paid", to: "shipped", label: "shipped", tone: "strong" },
    { from: "created", to: "cancelled", label: "payment_timeout (the timer)", labelAt: 0.82 },
    { from: "cancelled", to: "refunds", label: "payment_succeeded: push refund", dashed: true },
  ],
  notes: [{ at: [2, 0.55], text: "any other event:\nack it, change nothing", anchor: "middle" }],
});
