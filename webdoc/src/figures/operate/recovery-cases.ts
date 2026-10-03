import { flow } from "@/lib/figure-spec";

// operate/recovery.mdx, "A cluster: start with a write" and "Find your case".
export default flow({
  alt: "How to find your case. First send a write, a KV put with a 10-second timeout, because /health cannot tell whether the cluster still commits. If the write is answered, the cluster commits and one node is sick: a node that restarted and is healthy needs nothing, a node with damaged or missing files is replaced, a node with a full disk gets a bigger one, and a node with a wrong token or peer list gets its configuration fixed. If the write is not answered, the cluster does not commit: with a majority down and its disks intact, bring the nodes back; with a majority of the disks gone for good, force-recover one survivor, which can lose acknowledged writes; with every node stopped on the same entry, skip the poisoned entry; with every disk gone, restore a backup.",
  caption: "One write decides which half of the page you are on. The red boxes are the procedures that can lose acknowledged writes.",
  cols: 2,
  rows: 6,
  colWidth: 340,
  rowHeight: 70,
  gap: 90,
  nodes: [
    { id: "probe", at: [0.5, 0], label: "send one write", sub: "a KV put, 10 s timeout", tone: "strong" },
    { id: "yes", at: [0, 1], label: "it commits", sub: "one node is sick" },
    { id: "no", at: [1, 1], label: "it does not commit", sub: "the cluster is stuck", tone: "warn" },
    { id: "y1", at: [0, 2], label: "nothing to do", sub: "it restarted and is healthy" },
    { id: "y2", at: [0, 3], label: "replace the node", sub: "damaged or missing files" },
    { id: "y3", at: [0, 4], label: "grow the disk", sub: "507, or no space left" },
    { id: "y4", at: [0, 5], label: "fix the configuration", sub: "token, peers, encryption key" },
    { id: "n1", at: [1, 2], label: "bring them back", sub: "a majority down, disks intact" },
    { id: "n2", at: [1, 3], label: "skip the poisoned entry", sub: "every node stopped on one entry" },
    { id: "n3", at: [1, 4], label: "force-recover a survivor", sub: "a majority of the disks gone", tone: "danger" },
    { id: "n4", at: [1, 5], label: "restore a backup", sub: "every disk gone", tone: "danger" },
  ],
  edges: [
    { from: "probe", to: "yes", fromSide: "l", toSide: "t", label: "answered" },
    { from: "probe", to: "no", fromSide: "r", toSide: "t", label: "no answer", tone: "warn" },
    { from: "yes", to: "y1", fromSide: "l", toSide: "l" },
    { from: "yes", to: "y2", fromSide: "l", toSide: "l" },
    { from: "yes", to: "y3", fromSide: "l", toSide: "l" },
    { from: "yes", to: "y4", fromSide: "l", toSide: "l" },
    { from: "no", to: "n1", fromSide: "r", toSide: "r" },
    { from: "no", to: "n2", fromSide: "r", toSide: "r" },
    { from: "no", to: "n3", fromSide: "r", toSide: "r", tone: "danger" },
    { from: "no", to: "n4", fromSide: "r", toSide: "r", tone: "danger" },
  ],
});
