import { sequence } from "@/lib/figure-spec";

// concepts/guarantees.mdx, "What an answered write means".
export default sequence({
  alt: "A client pushes to the leader of a three-node cluster. The leader writes the entry to its log and fsyncs it, and sends it to both followers, which write and fsync it too. As soon as one follower confirms, two of three nodes hold the entry on disk: it is committed, and the leader answers the client 201. The other follower's confirmation arrives later and changes nothing.",
  caption: "An answer means a majority has the entry on disk. On a single node the same rule is one fsync before the answer.",
  gap: 160,
  actors: [
    { id: "c", label: "client", tone: "ghost" },
    { id: "l", label: "leader", tone: "strong" },
    { id: "f1", label: "follower" },
    { id: "f2", label: "follower" },
  ],
  steps: [
    { from: "c", to: "l", label: "push" },
    { from: "l", to: "l", label: "write + fsync" },
    { from: "l", to: "f1", label: "append" },
    { from: "l", to: "f2", label: "append" },
    { note: "write + fsync", over: ["f1", "f2"] },
    { from: "f1", to: "l", label: "done", reply: true },
    { note: "2 of 3 on disk: committed", over: "l", tone: "strong" },
    { from: "l", to: "c", label: "201", reply: true, tone: "strong" },
    { from: "f2", to: "l", label: "done, later", reply: true, tone: "faint" },
  ],
});
