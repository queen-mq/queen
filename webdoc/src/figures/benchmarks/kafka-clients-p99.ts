import { line } from "@/lib/figure-spec";

// benchmarks/kafka-clients.mdx, "Partition count": p99 at 1,000,000 msg/s.
export default line({
  alt: "p99 end-to-end latency at 1,000,000 msg/s against the partition count, log scales. Kafka clients through Queen's Kafka facade: 108 ms at 200 partitions, 142 at 10,000, 200 at 50,000 and 251 at 100,000. Queen's own clients: between 103 and 163 ms. Kafka 4.3.1 itself: 18 ms at 200 partitions, 46 at 10,000, 1.5 s at 50,000, and behind at 100,000 with a p99 of 10.9 s.",
  caption: "The same Kafka clients, pointed at Queen instead of Kafka. Under 10,000 partitions Kafka is faster; past 50,000, Queen's partition-per-entity store keeps the Kafka clients under a quarter of a second while Kafka falls behind.",
  source: "benchmark-queen/2026-09-30-stage03-3node, benchmark-queen/2026-09-30-kafka-pulsar",
  x: { label: "partitions", log: true, ticks: [100, 1000, 10000, 100000], min: 150, max: 130000 },
  y: { label: "p99 end-to-end latency", log: true, format: "ms", ticks: [10, 100, 1000, 10000], min: 10, max: 20000 },
  series: [
    { label: "Kafka clients on Queen", tone: "s1", points: [[200, 108], [10000, 142], [50000, 200], [100000, 251]] },
    { label: "Queen clients", tone: "s4", dash: "dash", points: [[200, 146], [10000, 144], [50000, 103], [100000, 163]] },
    { label: "Kafka 4.3.1", tone: "s2", dash: "dot", points: [[200, 18], [10000, 46], [50000, 1466], { x: 100000, y: 10900, hollow: true }] },
  ],
});
