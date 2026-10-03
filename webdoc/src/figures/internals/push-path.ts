import { sequence } from "@/lib/figure-spec";

// internals/life-of-a-step.mdx, "Push" and "When you are answered".
export default sequence({
  alt: "A push sent to a follower of a three-node cluster. The follower admits it, mints the message ids, hashes the transactionIds and packs one frame per message, then forwards only the prepared command to the leader over a long-lived stream. The leader plans it (request id, dedup, offsets), writes it to its queue logs and fsyncs them, and sends the entry to both followers, which write and fsync it too. Once a majority has it the entry is committed. The answering follower learns the commit index from the leader and answers the client 201.",
  caption: "The edge work, parsing, ids and hashing, stays on the node the client called; the leader receives a prepared command and spends its time planning, ordering and writing.",
  source: "server/src/rsm/facade/real.rs, server/src/rsm/planner/, server/src/rsm/qlog/mod.rs",
  gap: 160,
  actors: [
    { id: "c", label: "client", tone: "ghost" },
    { id: "f", label: "follower", sub: "node 2" },
    { id: "l", label: "leader", sub: "node 1", tone: "strong" },
    { id: "g", label: "follower", sub: "node 3" },
  ],
  steps: [
    { from: "c", to: "f", label: "POST /api/v1/push" },
    { note: "admit, mint ids,\nhash, pack frames", over: "f" },
    { from: "f", to: "l", label: "prepared command" },
    { note: "plan: request id,\ndedup, offsets", over: "l", tone: "strong" },
    { from: "l", to: "l", label: "write, fsync" },
    { from: "l", to: "f", label: "append" },
    { from: "l", to: "g", label: "append" },
    { note: "write, fsync", over: ["f", "g"] },
    { from: "g", to: "l", label: "done", reply: true },
    { note: "majority: committed", over: "l", tone: "strong" },
    { from: "l", to: "f", label: "commit index", reply: true },
    { from: "f", to: "c", label: "201", reply: true, tone: "strong" },
  ],
});
