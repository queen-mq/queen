import { flow } from "@/lib/figure-spec";

// examples/rate-limiter.mdx, "The counter".
export default flow({
  alt: "The rate limiter's counter. Requests land in the requests queue, in a partition per API key. A stream counts them in tumbling two-second windows, and each of its cycles commits the window state, the windows it closed and the ack of the requests it counted as one entry. A gate consumes the closed windows, and for each window over the quota it pushes a throttle decision to the queue a gateway reads, in the same transaction as the ack of the window.",
  caption: "Counting and policy are separate consumers, each ending in one transaction: the count stays exact through restarts, and the gate's rules change on their own schedule.",
  cols: 3,
  rows: 2,
  colWidth: 228,
  rowHeight: 104,
  gap: 76,
  nodes: [
    { id: "req", at: [0, 0], label: "requests", sub: "a partition per API key", shape: "disk" },
    { id: "stream", at: [1, 0], label: "counting stream", sub: "2 s windows, count", tone: "ghost", shape: "pill" },
    { id: "windows", at: [2, 0], label: "closed windows", sub: "per key and window", shape: "disk" },
    { id: "gate", at: [2, 1], label: "gate", sub: "over the quota?", tone: "ghost", shape: "pill" },
    { id: "dec", at: [1, 1], label: "throttle decisions", sub: "read by a gateway", shape: "disk", tone: "strong" },
  ],
  edges: [
    { from: "req", to: "stream", label: "pop" },
    { from: "stream", to: "windows", label: "cycle" },
    { from: "windows", to: "gate", label: "pop" },
    { from: "gate", to: "dec", label: "push + ack", tone: "strong" },
  ],
});
