// Chapter 5.3: what listens where, and what faces the internet.
export default {
  id: "front-doors",
  height: 340,
  alt: "Only the proxy's port faces the internet: 6711, with TLS, API keys and sessions. Everything else stays on the private network: the broker port 6632, which checks nothing unless broker JWT is on, used by the nodes, probes and trusted clients; the raft port 7400, plain HTTP with the shared token, between nodes only; and the Kafka port 9092, with no TLS in 2.6.",
  draw(c) {
    c.box(20, 40, 170, 70, "internet", { tone: "ghost" });
    c.rect(250, 20, 730, 300, { tone: "plain", r: 14 });
    c.text(270, 44, "a node", { size: 15, weight: 700 });
    const ports = [
      ["6711", "proxy", "TLS, API keys, people, quotas", "accent"],
      ["6632", "broker", "no check unless JWT is on: nodes, probes, trusted clients", "strong"],
      ["7400", "raft", "plain HTTP + QUEEN_RAFT_TOKEN: nodes only", "strong"],
      ["9092", "Kafka", "no TLS in 2.6: private network", "info"],
    ];
    ports.forEach(([port, what, note, tone], i) => {
      const y = 64 + i * 62;
      c.tag(330, y + 22, port, { tone, anchor: "middle", mono: true, size: 15 });
      c.text(380, y + 14, what, { size: 15.5, weight: 600 });
      c.text(380, y + 36, note, { size: 13.5, tone: "muted" });
    });
    c.arrow([[196, 75], [296, 86]], { tone: "accent", width: 2.2 });
    c.rect(260, 128, 710, 182, { tone: "ghost", r: 10 });
    c.text(960, 300, "private network only", { size: 13.5, anchor: "end", tone: "muted", italic: true });
  },
};
