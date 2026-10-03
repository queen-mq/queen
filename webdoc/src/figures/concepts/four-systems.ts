import { flow } from "@/lib/figure-spec";

// concepts/index.mdx, "The five roles of a step": the usual stack against one log.
export default flow({
  alt: "Two ways to run one step. On the left, the usual stack: the worker acks and publishes the next events on a broker, writes the state and an outbox row in a database, schedules a reminder in a scheduler, and records the event id in an idempotency table, four systems with four separate commits. On the right, Queen: the worker sends one transaction, and the ack, the next events, the state, the timer and the identity check land in one log entry.",
  caption: "Four commits can disagree when something crashes between two of them. One entry cannot: it is written whole or not at all.",
  cols: 4,
  rows: 4,
  colWidth: 166,
  rowHeight: 90,
  gap: 40,
  nodes: [
    { id: "w1", at: [0, 1.5], label: "worker", tone: "ghost", shape: "pill" },
    { id: "broker", at: [1, 0], label: "broker", sub: "ack, next events" },
    { id: "db", at: [1, 1], label: "database", sub: "state, outbox" },
    { id: "sched", at: [1, 2], label: "scheduler", sub: "reminder" },
    { id: "idem", at: [1, 3], label: "idempotency\ntable" },
    { id: "w2", at: [2, 1.5], label: "worker", tone: "ghost", shape: "pill" },
    { id: "queen", at: [3, 1.5], label: "one log entry", sub: "ack, events, state,\ntimer, identity", tone: "strong", shape: "disk" },
  ],
  edges: [
    { from: "w1", to: "broker", fromSide: "r", toSide: "l" },
    { from: "w1", to: "db", fromSide: "r", toSide: "l" },
    { from: "w1", to: "sched", fromSide: "r", toSide: "l" },
    { from: "w1", to: "idem", fromSide: "r", toSide: "l" },
    { from: "w2", to: "queen", tone: "strong" },
  ],
  groups: [
    { label: "four systems, four commits", nodes: ["w1", "broker", "db", "sched", "idem"] },
    { label: "Queen, one commit", nodes: ["w2", "queen"] },
  ],
});
