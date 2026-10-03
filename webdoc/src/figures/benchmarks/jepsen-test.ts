import { flow } from "@/lib/figure-spec";

// benchmarks/jepsen.mdx, "How a test runs".
export default flow({
  alt: "One Jepsen test. Clients run one workload against a fresh five-node Queen cluster for 300 seconds at 100 to 200 operations a second while the nemesis injects one class of fault: kills, pauses, partitions, clock jumps or power loss. Every call and its answer is recorded in a history. After the faults heal and the clients drain the queues, the workload's checkers read the whole history, and the test is valid only when every checker is.",
  caption: "Every test starts from a fresh cluster and ends with the whole history checked, not a sample of it.",
  source: "test/jepsen/",
  cols: 3,
  rows: 3,
  colWidth: 226,
  rowHeight: 88,
  gap: 64,
  nodes: [
    { id: "clients", at: [0, 0], label: "clients", sub: "one workload, 300 s", tone: "ghost", shape: "stack" },
    { id: "cluster", at: [1, 0], label: "five Queen nodes", sub: "fresh for each test", tone: "strong", shape: "stack" },
    { id: "nemesis", at: [2, 0], label: "nemesis", sub: "one class of fault", tone: "danger" },
    { id: "history", at: [0, 1], label: "history", sub: "every call, every answer", shape: "disk" },
    { id: "checkers", at: [0, 2], label: "checkers", sub: "after heal and drain" },
    { id: "verdict", at: [1, 2], label: "valid", sub: "only if every checker is", tone: "strong" },
  ],
  edges: [
    { from: "clients", to: "cluster", label: "calls" },
    { from: "nemesis", to: "cluster", label: "faults", tone: "danger" },
    { from: "clients", to: "history", label: "recorded" },
    { from: "history", to: "checkers", label: "read whole" },
    { from: "checkers", to: "verdict" },
  ],
});
