import { sequence } from "@/lib/figure-spec";

// internals/life-of-a-step.mdx, "Pop" and "Ack": the consumption engine on the
// leader (server/src/rsm/consume/) and its checkpoints.
export default sequence({
  alt: "A pop and its ack through the consumption engine on the leader. The worker pops for group billing; the engine claims the oldest claimable partition and leases its batch. The lease is a change to the group's cursor row, logged in the engine's next checkpoint, at most 5 ms later, and the pop is answered with the messages and a leaseId once that checkpoint has committed. After the worker's step, its ack is checked against the live lease, moves the cursor, rides the next checkpoint, and is answered once that checkpoint commits.",
  caption: "Pops and acks never go through the planner. They change cursor rows in the engine's memory, and a few checkpoint commands every 5 ms make those changes durable, however many acks they carry.",
  source: "server/src/rsm/consume/pop.rs, server/src/rsm/consume/ack.rs",
  gap: 200,
  actors: [
    { id: "w", label: "worker", tone: "ghost" },
    { id: "e", label: "consumption engine", sub: "leader", tone: "strong" },
    { id: "r", label: "raft log", sub: "majority fsync" },
  ],
  steps: [
    { from: "w", to: "e", label: "pop, group billing" },
    { from: "e", to: "e", label: "claim oldest, lease" },
    { from: "e", to: "r", label: "checkpoint, within 5 ms" },
    { from: "r", to: "e", label: "committed", reply: true },
    { from: "e", to: "w", label: "200: messages, leaseId", reply: true, tone: "strong" },
    { divider: "the worker runs its step" },
    { from: "w", to: "e", label: "ack completed, leaseId" },
    { from: "e", to: "e", label: "check lease, move cursor" },
    { from: "e", to: "r", label: "next checkpoint" },
    { from: "r", to: "e", label: "committed", reply: true },
    { from: "e", to: "w", label: "200", reply: true, tone: "strong" },
  ],
});
