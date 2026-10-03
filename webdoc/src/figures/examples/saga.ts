import { flow } from "@/lib/figure-spec";

// examples/saga.mdx, "How it works": the three transactions.
export default flow({
  alt: "The booking saga as three workers, each ending in one transaction. The reserver, for a submitted booking, commits the saga state held behind a putIfAbsent gate, the release timer keyed by the booking id, the payment request pushed into the booking's partition of the payments queue, and the ack of the submission. The payer, for the payment request, commits the state confirmed with an expect on its version, the cancel of the release timer and the ack. The compensator, when the release timer fires, reads the state and releases the booking only if it is still held, with an expect on the version, so a confirmation that landed in between cannot be overwritten.",
  caption: "Every step is one transaction, so no crash leaves a hold without its timer or a payment without its hold. The compensator checks the state because a cancel that comes too late finds the timer already fired.",
  cols: 2,
  rows: 3,
  colWidth: 350,
  rowHeight: 112,
  gap: 70,
  nodes: [
    { id: "r", at: [0, 0], label: "reserver", sub: "a booking is submitted", tone: "ghost", shape: "pill" },
    { id: "t1", at: [1, 0], label: "one transaction", sub: "state held, behind a gate\nrelease timer, by booking id\npush the payment request\nack the submission", tone: "strong" },
    { id: "p", at: [0, 1], label: "payer", sub: "the payment request", tone: "ghost", shape: "pill" },
    { id: "t2", at: [1, 1], label: "one transaction", sub: "state confirmed, expect version\ncancel the release timer\nack the payment request", tone: "strong" },
    { id: "c", at: [0, 2], label: "compensator", sub: "the release timer fired", tone: "ghost", shape: "pill" },
    { id: "t3", at: [1, 2], label: "release, if still held", sub: "read the state first\nexpect version on the write", tone: "warn" },
  ],
  edges: [
    { from: "r", to: "t1", tone: "strong" },
    { from: "p", to: "t2", tone: "strong" },
    { from: "c", to: "t3", tone: "warn" },
    { from: "t1", to: "p", fromSide: "b", toSide: "t", label: "payment request", dashed: true },
  ],
});
