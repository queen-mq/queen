import { flow } from "@/lib/figure-spec";

// guides/ephemeral.mdx, "In a cluster": the three nodes of the page's own example
// (queen-2 owns room-4 and room-5, queen-3 owns room-1, 2, 3 and 6). A push that
// lands on queen-1 and a pop that lands on queen-2 both reach room-1's owner.
export default flow({
  alt: "Three Queen nodes serving one ephemeral queue. A producer pushes to presence/room-1 through queen-1 and a consumer pops room-1 through queen-2. Neither node owns room-1, so both forward the request to queen-3, which owns room-1 (with room-2, room-3 and room-6) and keeps its messages in memory. queen-2 owns room-4 and room-5, queen-1 owns none of the six rooms. The owner of a (queue, partition) is a highest-random-weight hash over the live members, which every node computes the same way.",
  caption: "Clients can use any node. The node that receives a request hashes the partition, finds the owner and forwards to it, and the owner answers through the same hop. A long-poll pop waits at the owner.",
  source: "server/src/ephemeral.rs (hrw_pick, Ephemeral::route), server/src/handlers/ephemeral.rs (forward_to_owner)",
  cols: 3,
  rows: 3,
  colWidth: 220,
  rowHeight: 92,
  gap: 60,
  nodes: [
    { id: "producer", at: [0, 0], label: "producer", sub: "push room-1", tone: "ghost", shape: "pill" },
    { id: "consumer", at: [2, 0], label: "consumer", sub: "pop room-1, long poll", tone: "ghost", shape: "pill" },
    { id: "q1", at: [0, 1], label: "queen-1", sub: "owns none of the rooms" },
    { id: "q2", at: [2, 1], label: "queen-2", sub: "owns room-4, room-5" },
    { id: "q3", at: [1, 2], label: "queen-3", sub: "owns room-1, 2, 3, 6\nholds them in memory", tone: "strong" },
  ],
  edges: [
    { from: "producer", to: "q1", label: "any node" },
    { from: "consumer", to: "q2", label: "any node" },
    { from: "q1", to: "q3", fromSide: "b", toSide: "l", label: "forward", labelAt: 0.75, labelSide: "above", tone: "strong" },
    { from: "q2", to: "q3", fromSide: "b", toSide: "r", label: "forward", labelAt: 0.75, labelSide: "above", tone: "strong" },
  ],
  notes: [{ at: [1, 0.4], text: "owner = highest-random-weight hash\nof (queue, partition)\nover the live members" }],
});
