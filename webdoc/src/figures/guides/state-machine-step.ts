import { flow } from "@/lib/figure-spec";

// guides/state-machines.mdx, "The worker": the payment_succeeded step of order
// ord-1042 in state created, as the worker on the page commits it.
export default flow({
  alt: "One step of the machine. A worker in group order-machine receives payment_succeeded from partition ord-1042 of the orders queue and reads the order's state, created, from KV namespace order-state. It commits one transaction that acks the event, puts the state paid, pushes ship_order to the shipping queue and cancels the payment-timeout timer. The transaction is one raft log entry, refused whole if the worker's lease on the partition has expired.",
  caption: "The state sits beside the queue, in the same broker, so the read happens before the step and the write commits with the ack. A stalled worker's commit carries an expired lease and is refused whole.",
  source: "clients/client-js/client-v2/builders/TransactionBuilder.js, examples/apps/js/saga.mjs",
  cols: 3,
  rows: 3,
  colWidth: 225,
  rowHeight: 112,
  gap: 80,
  nodes: [
    { id: "part", at: [0, 0], label: "ord-1042", sub: "partition of orders", mono: true },
    { id: "worker", at: [1, 0], label: "worker", sub: "group order-machine", tone: "ghost", shape: "pill" },
    { id: "kv", at: [2, 0], label: "order-state", sub: "ord-1042: created", shape: "disk", mono: true },
    { id: "tx", at: [0.5, 1], span: 2, label: "one transaction", sub: "ack payment_succeeded\nput ord-1042: paid\npush ship_order to shipping\ncancel payment-timeout", tone: "strong" },
    { id: "log", at: [1, 2], label: "raft log", sub: "one entry", shape: "disk" },
  ],
  edges: [
    { from: "part", to: "worker", label: "next event" },
    { from: "kv", to: "worker", label: "kv.get", dashed: true },
    { from: "worker", to: "tx", label: "commit", tone: "strong" },
    { from: "tx", to: "log", label: "all or nothing", tone: "strong" },
  ],
});
