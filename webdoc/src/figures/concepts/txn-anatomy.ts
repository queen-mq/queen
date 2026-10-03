import { flow } from "@/lib/figure-spec";

// concepts/transactions.mdx, "What can go in" and "Why a transaction rolls back".
export default flow({
  alt: "One call to POST /api/v1/transaction carries four parts: acks of leased messages, pushes of the next events, KV operations and timer operations. The broker checks them in that order, acks first, then the pushes and their dedup, then KV, then timers. If every check passes, all four parts are written as one log entry and apply together. The first refusal decides the reason (rejected_ack, duplicate, kv_precondition and others), and the call rolls back with nothing written.",
  caption: "Four parts, one entry, one verdict. The checks run in a fixed order, so the first refusal names the reason.",
  source: "server/src/rsm/planner/txn.rs, server/src/rsm/consume/txn.rs",
  cols: 4,
  rows: 3,
  colWidth: 166,
  rowHeight: 98,
  gap: 34,
  nodes: [
    { id: "acks", at: [0, 0], label: "acks", sub: "leased messages" },
    { id: "pushes", at: [1, 0], label: "pushes", sub: "next events" },
    { id: "kv", at: [2, 0], label: "KV", sub: "state" },
    { id: "timers", at: [3, 0], label: "timers", sub: "schedule, cancel" },
    { id: "txn", at: [1, 1], span: 2, label: "one transaction", sub: "checked: acks, pushes, KV, timers", tone: "strong" },
    { id: "entry", at: [0.5, 2], label: "one log entry", sub: "every part applies", shape: "disk", tone: "strong" },
    { id: "rollback", at: [2.5, 2], label: "rolled back", sub: "nothing written", tone: "warn" },
  ],
  edges: [
    { from: "acks", to: "txn", fromSide: "b", toSide: "t" },
    { from: "pushes", to: "txn", fromSide: "b", toSide: "t" },
    { from: "kv", to: "txn", fromSide: "b", toSide: "t" },
    { from: "timers", to: "txn", fromSide: "b", toSide: "t" },
    { from: "txn", to: "entry", fromSide: "b", toSide: "t", label: "all pass", tone: "strong" },
    { from: "txn", to: "rollback", fromSide: "b", toSide: "t", label: "first refusal", tone: "warn" },
  ],
});
