import { flow } from "@/lib/figure-spec";

// operate/security.mdx, "What listens where".
export default flow({
  alt: "The ports of one Queen process and who should reach each. The proxy port, 6711 for example, faces the internet, with API keys, console sessions and TLS, and hands requests to the broker inside the same process. The broker's own port 6632 serves the API, the dashboard, /health and the metrics to trusted clients inside the network, probes and scrapers, under a JWT setting that is off by default. The raft port 7400 carries votes, entries and snapshots between the nodes and is protected by QUEEN_RAFT_TOKEN. The Kafka facade listens on 9092, with SASL plain if you turn it on.",
  caption: "Port 6632 never faces the internet on its own: put the proxy, or another front door, in front of it.",
  source: "server/src/config.rs, proxy/src/routes.rs",
  cols: 4,
  rows: 2,
  colWidth: 168,
  rowHeight: 96,
  gap: 28,
  nodes: [
    { id: "c1", at: [0, 0], label: "the internet", sub: "keys, sessions", tone: "ghost", shape: "pill" },
    { id: "c2", at: [1, 0], label: "your network", sub: "probes, scrapers", tone: "ghost", shape: "pill" },
    { id: "c3", at: [2, 0], label: "other nodes", sub: "raft token", tone: "ghost", shape: "pill" },
    { id: "c4", at: [3, 0], label: "Kafka clients", sub: "SASL, optional", tone: "ghost", shape: "pill" },
    { id: "p1", at: [0, 1], label: "proxy :6711", sub: "TLS", tone: "strong" },
    { id: "p2", at: [1, 1], label: "broker :6632", sub: "JWT, off by default" },
    { id: "p3", at: [2, 1], label: "raft :7400", sub: "the raft token" },
    { id: "p4", at: [3, 1], label: "Kafka :9092", sub: "the facade" },
  ],
  edges: [
    { from: "c1", to: "p1", tone: "strong" },
    { from: "c2", to: "p2" },
    { from: "c3", to: "p3" },
    { from: "c4", to: "p4" },
    { from: "p1", to: "p2" },
  ],
  groups: [{ label: "one Queen process", nodes: ["p1", "p2", "p3", "p4"] }],
});
