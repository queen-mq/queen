import { line } from "@/lib/figure-spec";

// benchmarks/throughput.mdx, "Latency by rate": the p99 halves of the table,
// up to the last rate each system kept up with.
export default line({
  alt: "p99 end-to-end latency against the offered rate, one queue or topic of 200 partitions on three nodes, log scale. Kafka goes from 10 ms at 300,000 msg/s to 75 ms at 4,000,000. Pulsar stays between 15 and 21 ms up to 2,000,000. Queen goes from 22 ms at 300,000 to 98 ms at 600,000 and 146 ms at 1,000,000, and falls behind at 1,500,000. Redpanda goes from 66 ms to between 181 and 322 ms up to 2,000,000 and falls behind at 3,000,000.",
  caption: "Each line stops at the last rate the system kept up with. In this shape Kafka's latency is the one to beat, and Queen's ceiling is about 1.5M msg/s for one queue, set by one leader.",
  source: "benchmark-queen/2026-09-30-stage03-3node (Queen), benchmark-queen/2026-09-30-kafka-pulsar/runs (the others)",
  x: { label: "offered msg/s", ticks: [0, 1000000, 2000000, 3000000, 4000000], min: 0, max: 4200000 },
  y: { label: "p99 end-to-end latency", log: true, format: "ms", ticks: [1, 10, 100, 1000], min: 5, max: 1000 },
  series: [
    { label: "Queen", points: [[300000, 22], [600000, 98], [1000000, 146]] },
    { label: "Kafka", points: [[300000, 10], [600000, 13], [1000000, 18], [1500000, 35], [2000000, 59], [3000000, 74], [4000000, 75]] },
    { label: "Redpanda", points: [[300000, 66], [600000, 181], [1000000, 187], [1500000, 322], [2000000, 253]] },
    { label: "Pulsar", points: [[300000, 15], [600000, 18], [1000000, 21], [1500000, 20], [2000000, 21]] },
  ],
});
