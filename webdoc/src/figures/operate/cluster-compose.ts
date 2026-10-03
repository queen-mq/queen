import { flow } from "@/lib/figure-spec";

// operate/cluster.mdx, "Try it on a laptop": deploy/compose/three-node/compose.yaml.
export default flow({
  alt: "The three-node Compose cluster. A client holds the URLs of all three nodes. queen-1, queen-2 and queen-3 each publish their HTTP port 6632 on the host as 127.0.0.1:16632, 26632 and 36632, talk raft to each other on port 7400 with one shared QUEEN_RAFT_TOKEN, and keep their data directory in a volume of their own.",
  caption: "Any node takes any request: followers forward writes to the leader, so a client can hold all three URLs without knowing which one leads.",
  source: "deploy/compose/three-node/compose.yaml",
  cols: 3,
  rows: 3,
  colWidth: 228,
  rowHeight: 112,
  gap: 56,
  nodes: [
    { id: "client", at: [1, 0], label: "your app", sub: "urls: all three nodes", tone: "ghost", shape: "pill" },
    { id: "n1", at: [0, 1], label: "queen-1", sub: "127.0.0.1:16632", mono: true },
    { id: "n2", at: [1, 1], label: "queen-2", sub: "127.0.0.1:26632", mono: true },
    { id: "n3", at: [2, 1], label: "queen-3", sub: "127.0.0.1:36632", mono: true },
    { id: "v1", at: [0, 2], label: "volume queen-1", sub: "/var/lib/queen/raft", shape: "disk" },
    { id: "v2", at: [1, 2], label: "volume queen-2", sub: "/var/lib/queen/raft", shape: "disk" },
    { id: "v3", at: [2, 2], label: "volume queen-3", sub: "/var/lib/queen/raft", shape: "disk" },
  ],
  edges: [
    { from: "client", to: "n1", fromSide: "b", toSide: "t" },
    { from: "client", to: "n2", fromSide: "b", toSide: "t", label: "HTTP" },
    { from: "client", to: "n3", fromSide: "b", toSide: "t" },
    { from: "n1", to: "v1", plain: true },
    { from: "n2", to: "v2", plain: true },
    { from: "n3", to: "v3", plain: true },
  ],
  groups: [{ label: "raft on :7400, one QUEEN_RAFT_TOKEN", nodes: ["n1", "n2", "n3"] }],
});
