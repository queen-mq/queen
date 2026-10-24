// Chapter 5.1: the three-node Compose cluster on one machine.
export default {
  id: "compose-cluster",
  height: 320,
  alt: "Three containers on one machine, queen-1, queen-2 and queen-3, on a private Docker network. Each publishes its HTTP port on localhost as 16632, 26632 and 36632, and keeps its data in its own volume. They talk to each other on the raft port 7400, which is not published, with the shared QUEEN_RAFT_TOKEN. One of them is the leader.",
  draw(c) {
    c.frame(20, 30, 960, 270, "one machine, Docker network", { tone: "ghost" });
    const nodes = [["queen-1", "follower", "16632"], ["queen-2", "follower", "26632"], ["queen-3", "leader", "36632"]];
    nodes.forEach(([n, role, port], i) => {
      const x = 60 + i * 310;
      c.box(x, 110, 260, 80, n, { sub: role, tone: role === "leader" ? "accent" : "strong" });
      c.tag(x + 130, 72, `localhost:${port} → 6632`, { tone: "plain", anchor: "middle", mono: true, size: 13 });
      c.line(x + 130, 84, x + 130, 108, { tone: "line" });
      c.disk(x + 70, 214, 120, 64, `volume`, { sub: n, size: 14 });
      c.line(x + 130, 190, x + 130, 214, { tone: "line" });
    });
    c.arrow([[324, 150], [366, 150]], { tone: "muted", both: true, label: "7400" });
    c.arrow([[634, 150], [676, 150]], { tone: "muted", both: true, label: "7400" });
    c.text(500, 310, "Raft port 7400 is not published; every node carries the same QUEEN_RAFT_TOKEN.", { size: 13.5, anchor: "middle", tone: "muted", italic: true });
  },
};
