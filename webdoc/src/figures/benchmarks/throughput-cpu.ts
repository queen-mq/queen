import { line } from "@/lib/figure-spec";

// benchmarks/throughput.mdx, "CPU": cores used by each system, summed over the
// three brokers (48 vCPU in all). Hollow: the system had fallen behind.
export default line({
  alt: "Cores used by each system against the offered rate, summed over three brokers with 48 vCPU in all, one queue or topic of 200 partitions. At 1,000,000 msg/s Queen used 17.2 cores, Redpanda 14.2, Kafka 9.7 and Pulsar 7.1. Queen reached 24.9 and 27.2 cores at 1.5M and 2M, where it had fallen behind; Kafka used 19.4 cores at 4M.",
  caption: "In this shape, few partitions and a high rate, Queen spends two to three times the CPU of Kafka and Pulsar per message. Hollow marks are rates the system no longer kept up with.",
  source: "benchmark-queen/2026-09-30-stage03-3node (Queen), benchmark-queen/2026-09-30-kafka-pulsar/runs (the others)",
  x: { label: "offered msg/s", ticks: [0, 1000000, 2000000, 3000000, 4000000], min: 0, max: 4200000 },
  y: { label: "cores, three brokers", ticks: [0, 10, 20, 30, 40], min: 0, max: 40, format: "int" },
  series: [
    { label: "Queen", points: [[300000, 9.0], [600000, 13.0], [1000000, 17.2], { x: 1500000, y: 24.9, hollow: true }, { x: 2000000, y: 27.2, hollow: true }] },
    { label: "Kafka", points: [[300000, 3.6], [600000, 6.3], [1000000, 9.7], [1500000, 12.6], [2000000, 14.5], [3000000, 18.4], [4000000, 19.4]] },
    { label: "Redpanda", points: [[300000, 4.1], [600000, 8.4], [1000000, 14.2], [1500000, 19.7], [2000000, 23.6], { x: 3000000, y: 31.9, hollow: true }, { x: 4000000, y: 37.7, hollow: true }] },
    { label: "Pulsar", points: [[300000, 2.7], [600000, 4.5], [1000000, 7.1], [1500000, 10.0], [2000000, 12.7]] },
  ],
});
