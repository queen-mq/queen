import { flow } from "@/lib/figure-spec";

export default flow({
  alt: "A command's path through a node: a client calls the HTTP edge of any node, which hands the command to the leader's intake. Pops, acks and renews go to the consumption engine; pushes, transactions, KV and timers go to the planner and its lanes, which also takes the engine's checkpoints. The planner writes one entry per cycle to the raft log, and apply, on every node, executes committed entries into the store.",
  caption: "Two ways in on the leader, one way down. The consumption engine answers from memory and reaches the log only through its checkpoints; everything else is planned, and every node applies what the log commits.",
  source: "server/src/rsm/facade/intake.rs, server/src/rsm/batcher_lanes.rs, server/src/rsm/apply.rs",
  cols: 3,
  rows: 5,
  colWidth: 220,
  rowHeight: 84,
  gap: 70,
  nodes: [
    { id: "client", at: [0, 0], label: "Client", sub: "HTTP or Kafka", tone: "ghost", shape: "pill" },
    { id: "edge", at: [1, 0], label: "HTTP edge", sub: "any node" },
    { id: "intake", at: [1, 1], label: "Intake", sub: "leader" },
    { id: "engine", at: [2, 1], label: "Consumption engine", sub: "leader, in memory" },
    { id: "planner", at: [1, 2], label: "Planner + 8 lanes", sub: "leader", tone: "strong" },
    { id: "log", at: [1, 3], label: "Raft log", sub: "queue log files", shape: "disk" },
    { id: "apply", at: [1, 4], label: "Apply", sub: "every node" },
    { id: "store", at: [2, 4], label: "Store", sub: "RAM + LMDB", shape: "disk" },
  ],
  edges: [
    { from: "client", to: "edge" },
    { from: "edge", to: "intake", label: "command" },
    { from: "intake", to: "engine", label: "pop, ack" },
    { from: "intake", to: "planner", label: "push, txn, KV, timers", tone: "strong" },
    { from: "engine", to: "planner", label: "checkpoints", fromSide: "b", toSide: "r", dashed: true },
    { from: "planner", to: "log", label: "one entry per cycle", tone: "strong" },
    { from: "log", to: "apply", label: "committed, in order", tone: "strong" },
    { from: "apply", to: "store", label: "only writer" },
  ],
});
