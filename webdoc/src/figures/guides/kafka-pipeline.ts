import { flow } from "@/lib/figure-spec";

// guides/kafka.mdx, "How it works". The facade runs in the broker process
// (server/src/kafka_inproc.rs) and calls the push and fetch paths in memory
// (rsm/facade/real.rs push_inputs, phase2/reads.rs fetch_wait).
export default flow({
  alt: "Kafka clients connect to the Kafka facade on port 9092, which runs inside the Queen broker process. Queen's own clients connect to the HTTP edge on port 6632. A produce becomes a push and a fetch a read, called in memory, and both paths go through the same admission, the same raft log and the same reads. Topic orders is the queue orders, and Kafka partition 307 is the Queen partition named 307.",
  caption: "One pipeline behind two protocols. A Kafka record is a Queen message from the moment it is written, so replication, retention, the dashboard and Queen's own consumers all apply to it.",
  source: "server/src/kafka_inproc.rs, protocols/queen-kafka/src/handlers/produce.rs",
  cols: 3,
  rows: 4,
  colWidth: 228,
  rowHeight: 88,
  gap: 60,
  nodes: [
    { id: "kc", at: [0, 0], label: "Kafka client", sub: "franz-go, librdkafka, Java", tone: "ghost", shape: "pill" },
    { id: "qc", at: [2, 0], label: "Queen client", sub: "six SDKs, or curl", tone: "ghost", shape: "pill" },
    { id: "facade", at: [0, 1], label: "Kafka facade", sub: "port 9092, in the broker" },
    { id: "http", at: [2, 1], label: "HTTP edge", sub: "port 6632" },
    { id: "pipe", at: [1, 2], label: "one pipeline", sub: "admission, raft log, reads", tone: "strong" },
    { id: "queue", at: [1, 3], label: "queue orders", sub: "a partition named 307", shape: "disk" },
  ],
  edges: [
    { from: "kc", to: "facade", label: "produce, fetch" },
    { from: "qc", to: "http", label: "push, pop" },
    { from: "facade", to: "pipe", fromSide: "b", toSide: "l", label: "in memory", tone: "strong" },
    { from: "http", to: "pipe", fromSide: "b", toSide: "r", tone: "strong" },
    { from: "pipe", to: "queue", label: "one entry per push", tone: "strong" },
  ],
});
