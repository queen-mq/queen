import { flow } from "@/lib/figure-spec";

// examples/rate-limiter.mdx, "The admission controller".
export default flow({
  alt: "The admission controller. Each request is decided by one transaction: an incr of the tenant's counter with max set to the quota, a putIfAbsent of an admitted entry for the request, the push of the work and the ack, both KV operations with required true. If it commits, the request is admitted and its work pushed. If the counter is at the quota (kvReason limit), nothing is written, and a second transaction schedules a timer that pushes the request back at the end of the window, together with the ack. If the admitted entry already exists (kvReason exists), it is a redelivery and is only acked.",
  caption: "A refused request spends no budget, because the incr rolled back with everything else. Deferred work comes back on its own when the window rolls over, with nobody retrying.",
  cols: 3,
  rows: 3,
  colWidth: 228,
  rowHeight: 92,
  gap: 64,
  nodes: [
    { id: "req", at: [0, 1], label: "request R-4", sub: "tenant acme", tone: "ghost", shape: "pill" },
    { id: "txn", at: [1, 1], label: "one transaction", sub: "incr counter, max 3\nputIfAbsent admitted:R-4\npush the work, ack", tone: "strong" },
    { id: "ok", at: [2, 0], label: "admitted", sub: "the work is pushed" },
    { id: "limit", at: [2, 1], label: "deferred", sub: "kvReason limit: a timer\npushes it back later", tone: "warn" },
    { id: "dup", at: [2, 2], label: "already admitted", sub: "kvReason exists: ack" },
  ],
  edges: [
    { from: "req", to: "txn" },
    { from: "txn", to: "ok", fromSide: "r", toSide: "l" },
    { from: "txn", to: "limit", fromSide: "r", toSide: "l" },
    { from: "txn", to: "dup", fromSide: "r", toSide: "l" },
  ],
});
