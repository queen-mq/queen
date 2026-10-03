import { flow } from "@/lib/figure-spec";

// examples/kafka-bridge.mdx, "How it works".
export default flow({
  alt: "The Kafka bridge on one Queen node. An order service with a Kafka producer writes orders, keyed by customer, to the topic orders, which is the Queen queue orders with four partitions. Billing, a Queen consumer group, pops each order and commits one transaction: the ack of the order, the invoice pushed into the partition of the invoices topic with the same number, and a KV incr of the customer's running total. Analytics, a Kafka consumer group, fetches the invoices from the beginning.",
  caption: "There is one copy of each record, so there is no connector to run and no lag between copies to watch.",
  cols: 3,
  rows: 2,
  colWidth: 228,
  rowHeight: 112,
  gap: 64,
  nodes: [
    { id: "orders-svc", at: [0, 0], label: "order service", sub: "Kafka producer", tone: "ghost", shape: "pill" },
    { id: "orders", at: [1, 0], label: "orders", sub: "topic = queue,\n4 partitions", mono: true, shape: "disk" },
    { id: "billing", at: [2, 0], label: "billing", sub: "Queen consumer", tone: "ghost", shape: "pill" },
    { id: "txn", at: [2, 1], label: "one transaction", sub: "ack the order\npush the invoice\nincr the total", tone: "strong" },
    { id: "invoices", at: [1, 1], label: "invoices", sub: "same partition\nnumber", mono: true, shape: "disk" },
    { id: "analytics", at: [0, 1], label: "analytics", sub: "Kafka consumer", tone: "ghost", shape: "pill" },
  ],
  edges: [
    { from: "orders-svc", to: "orders", label: "produce" },
    { from: "orders", to: "billing", label: "pop" },
    { from: "billing", to: "txn", label: "commit", tone: "strong" },
    { from: "txn", to: "invoices", label: "push", tone: "strong" },
    { from: "invoices", to: "analytics", label: "fetch" },
  ],
});
