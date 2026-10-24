// Chapter 4.1: a cluster is one Kafka cluster; never one advertised address.
export default {
  id: "kafka-cluster",
  height: 330,
  alt: "A client finds the cluster through bootstrap.servers, which may be a load balancer, used once. The metadata it gets back lists each node at its own advertised address and names the raft leader, node 2, as the leader of every partition, so the client then talks to node 2 directly. If every node advertised the load balancer's address, a client sent to the leader would land on another node, be told to go to the leader, and loop.",
  draw(c) {
    const client = c.box(20, 118, 150, 70, "Kafka client", { tone: "info" });
    const lb = c.box(250, 30, 170, 56, "load balancer", { sub: "bootstrap only", size: 15.5 });
    c.arrow([[client.cx, client.y - 6], [client.cx, 58], [lb.x - 6, 58]], { tone: "muted", label: "1. bootstrap", labelAt: [168, 46], labelBg: false });
    const nodes = [["node 1", "advertises 10.0.0.11:9092", "plain"], ["node 2: the raft leader", "10.0.0.12:9092, leads every partition", "accent"], ["node 3", "advertises 10.0.0.13:9092", "plain"]];
    nodes.forEach(([n, addr, tone], i) => {
      const b = c.box(600, 30 + i * 90, 380, 66, n, { sub: addr, tone, size: 16 });
      if (i === 1) c.arrow([[client.x + client.w + 4, b.cy], [b.x - 6, b.cy]], { tone: "accent", width: 2.2, label: "2. metadata, then straight to the leader" });
    });
    c.arrow([[lb.x + lb.w + 4, 58], [594, 62]], { tone: "faint" });
    c.text(500, 314, "Never advertise the load balancer: a client sent to the leader would land elsewhere, and loop.", { size: 14, anchor: "middle", tone: "danger", italic: true });
  },
};
