import { line } from "@/lib/figure-spec";

// benchmarks/throughput.mdx, "Latency by rate" and "The ceiling": consumed
// against offered. Redpanda's 4M point has no consumed rate in the table.
export default line({
  alt: "Messages consumed per second against messages offered, one queue or topic of 200 partitions on three nodes. Kafka consumes everything up to 4,000,000 msg/s. Pulsar consumes everything up to 2,000,000, the highest rate it ran. Redpanda keeps up to 2,000,000 and consumes 2.92M of 3M. Queen keeps up to 1,000,000, consumes 1.34M of 1.5M and 1.23M of 2M.",
  caption: "Where each line leaves the diagonal, the system stopped keeping up. Queen's ceiling for one queue is about 1.5M msg/s pushed, with its leader at 12 to 13 of 16 cores.",
  source: "benchmark-queen/2026-09-30-stage03-3node (Queen), benchmark-queen/2026-09-30-kafka-pulsar/runs (the others)",
  x: { label: "offered msg/s", ticks: [0, 1000000, 2000000, 3000000, 4000000], min: 0, max: 4200000 },
  y: { label: "consumed msg/s", ticks: [0, 1000000, 2000000, 3000000, 4000000], min: 0, max: 4200000 },
  series: [
    { label: "Queen", points: [[300000, 300000], [600000, 600000], [1000000, 1000000], { x: 1500000, y: 1340000, hollow: true }, { x: 2000000, y: 1230000, hollow: true }] },
    { label: "Kafka", points: [[300000, 300000], [600000, 600000], [1000000, 1000000], [1500000, 1500000], [2000000, 2000000], [3000000, 3000000], [4000000, 4000000]] },
    { label: "Redpanda", points: [[300000, 300000], [600000, 600000], [1000000, 1000000], [1500000, 1500000], [2000000, 2000000], { x: 3000000, y: 2920000, hollow: true }] },
    { label: "Pulsar", points: [[300000, 300000], [600000, 600000], [1000000, 1000000], [1500000, 1500000], [2000000, 2000000]] },
  ],
});
