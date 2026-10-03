import { flow } from "@/lib/figure-spec";

// guides/webhooks.mdx, "The shape" and "The dead letters".
export default flow({
  alt: "Deliveries in one queue, one partition per endpoint. Senders of the group sender each lease one endpoint's partition at a time. The endpoint hooks.acme.io is down and answers 503: its sender keeps failing that delivery, each failure spends one retry, and after retryLimit failures the delivery is filed in the dead-letter queue with its last error, and the endpoint's next delivery goes out. Meanwhile the senders of hooks.globex.io and hooks.initech.io keep delivering, in order.",
  caption: "A dead endpoint backs up its own partition and nothing else. The retry count lives in the broker, so it survives a sender dying halfway.",
  cols: 3,
  rows: 4,
  colWidth: 224,
  rowHeight: 78,
  gap: 64,
  nodes: [
    { id: "p1", at: [0, 1], label: "hooks.acme.io", sub: "partition", mono: true, tone: "warn" },
    { id: "p2", at: [0, 2], label: "hooks.globex.io", sub: "partition", mono: true },
    { id: "p3", at: [0, 3], label: "hooks.initech.io", sub: "partition", mono: true },
    { id: "s1", at: [1, 1], label: "sender", sub: "retrying", tone: "ghost", shape: "pill" },
    { id: "s2", at: [1, 2], label: "sender", tone: "ghost", shape: "pill" },
    { id: "s3", at: [1, 3], label: "sender", tone: "ghost", shape: "pill" },
    { id: "e1", at: [2, 1], label: "acme", sub: "503", tone: "danger" },
    { id: "e2", at: [2, 2], label: "globex", sub: "200" },
    { id: "e3", at: [2, 3], label: "initech", sub: "200" },
    { id: "dlq", at: [1, 0], label: "dead-letter queue", sub: "after retryLimit failures", tone: "warn", shape: "disk" },
  ],
  edges: [
    { from: "p1", to: "s1", tone: "warn" },
    { from: "p2", to: "s2" },
    { from: "p3", to: "s3" },
    { from: "s1", to: "e1", label: "POST", tone: "danger" },
    { from: "s2", to: "e2", label: "POST" },
    { from: "s3", to: "e3", label: "POST" },
    { from: "s1", to: "dlq", label: "budget spent", tone: "warn" },
  ],
});
