import { line } from "@/lib/figure-spec";

// benchmarks/transactions.mdx, "The ramp": delivered against offered, ten
// messages per transaction. Hollow: behind.
export default line({
  alt: "Messages delivered per second against messages offered through the transaction pipeline, both on log scales. Queen keeps up to 72,000 msg/s and delivers 66,700 of 144,000. Pulsar keeps up to 36,000, delivers 71,200 of 72,000 behind schedule and 69,800 of 144,000. Kafka and Redpanda keep up at 9,000 and fall behind from 18,000, delivering about 13,000 of 36,000.",
  caption: "Where a line leaves the diagonal, the system fell behind. Queen's ceiling is the one thread that plans transactions, near 7,000 transactions of ten messages a second here.",
  source: "benchmark-queen/2026-09-30-kafka-pulsar",
  x: { label: "offered msg/s", log: true, ticks: [10000, 20000, 50000, 100000], min: 7000, max: 180000 },
  y: { label: "delivered msg/s", log: true, ticks: [10000, 20000, 50000, 100000], min: 7000, max: 180000 },
  series: [
    { label: "Queen", points: [[9000, 9000], [18000, 18000], [36000, 36000], [72000, 72000], { x: 144000, y: 66700, hollow: true }] },
    { label: "Kafka", points: [[9000, 9000], { x: 18000, y: 17500, hollow: true }, { x: 36000, y: 13200, hollow: true }] },
    { label: "Redpanda", points: [[9000, 9000], { x: 18000, y: 17800, hollow: true }, { x: 36000, y: 13700, hollow: true }] },
    { label: "Pulsar", points: [[9000, 9000], [18000, 18000], [36000, 36000], { x: 72000, y: 71200, hollow: true }, { x: 144000, y: 69800, hollow: true }] },
  ],
});
