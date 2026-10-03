import { flow } from "@/lib/figure-spec";

// internals/index.mdx, "The planner and its lanes" (rsm/batcher_lanes.rs).
export default flow({
  alt: "One planning cycle on the leader. It drains up to 4,096 commands or 4 MiB. Commands that touch only existing partitions of one lane, the partitions whose id modulo 8 is that lane's number, are planned by the eight lanes in parallel: pushes, multi-partition pushes and single-lane transactions, and the engine's checkpoints. Then a control step plans everything else on one thread with the full view: queue and group changes, new partitions, KV, timers and commands that cross lanes. The result is one entry, proposed to raft, with up to eight entries in flight while the next cycle plans.",
  caption: "Lanes plan disjoint partitions at the same time and the control step comes after them in the entry, so the entry applies exactly as if one thread had planned it in that order.",
  source: "server/src/rsm/batcher_lanes.rs, server/src/rsm/tests/multipush.rs",
  cols: 3,
  rows: 3,
  colWidth: 228,
  rowHeight: 94,
  gap: 70,
  nodes: [
    { id: "cmds", at: [0, 0], label: "commands", sub: "up to 4,096 or 4 MiB", shape: "stack" },
    { id: "lanes", at: [1, 0], label: "8 lanes", sub: "in parallel: pushes,\ncheckpoints", shape: "stack" },
    { id: "control", at: [1, 1], label: "control step", sub: "new partitions,\nKV, timers", tone: "strong" },
    { id: "entry", at: [1, 2], label: "one entry", sub: "up to 8 in flight", shape: "disk", tone: "strong" },
  ],
  edges: [
    { from: "cmds", to: "lanes", label: "pid % 8" },
    { from: "cmds", to: "control", fromSide: "b", toSide: "l", label: "everything else" },
    { from: "lanes", to: "control", label: "then" },
    { from: "control", to: "entry", label: "propose", tone: "strong" },
  ],
});
