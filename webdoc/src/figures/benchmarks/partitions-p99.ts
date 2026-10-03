import { line } from "@/lib/figure-spec";

// benchmarks/partitions.mdx, "Latency at 1,000,000 msg/s": p99 of the worst load
// process over the last 30 s of each 60 s point. Hollow = behind (under 97% of
// the offered rate consumed, or p99 over 5 s). Queen's 2,000 point was not run.
export default line({
  alt: "p99 end-to-end latency against partition count, both on log scales, at 1,000,000 msg/s offered on three nodes. Queen stays between 103 and 163 ms from 200 to 10,000,000 partitions. Kafka goes from 18 ms at 200 partitions to 1.5 s at 50,000 and falls behind at 100,000. Redpanda reaches 1.8 s at 10,000 and falls behind from 50,000. Pulsar stays near 20 ms up to 10,000 and falls behind from 50,000.",
  caption: "p99 end-to-end latency with 1,000,000 msg/s offered to one queue. A hollow mark is a point where the system fell behind: it consumed under 97% of the offered rate or its p99 passed 5 seconds. Kafka, Redpanda and Pulsar were not run past 100,000 partitions.",
  source: "benchmark-queen/2026-09-30-stage03-3node, benchmark-queen/2026-09-30-kafka-pulsar",
  x: { label: "partitions in the queue", log: true, ticks: [100, 1000, 10000, 100000, 1000000, 10000000], min: 150, max: 13000000 },
  y: { label: "p99 end-to-end latency", log: true, format: "ms", ticks: [10, 100, 1000, 10000, 100000], min: 10, max: 60000 },
  series: [
    { label: "Queen", tone: "s1", points: [[200, 146], [10000, 144], [50000, 103], [100000, 163], [1000000, 123], [5000000, 138], [10000000, 122]] },
    { label: "Kafka", points: [[200, 18], [2000, 194], [10000, 46], [50000, 1466], { x: 100000, y: 10900, hollow: true }] },
    { label: "Redpanda", points: [[200, 187], [2000, 105], [10000, 1810], { x: 50000, y: 21000, hollow: true }, { x: 100000, y: 47000, hollow: true }] },
    { label: "Pulsar", points: [[200, 21], [2000, 15], [10000, 32], { x: 50000, y: 47000, hollow: true }, { x: 100000, y: 47000, hollow: true }] },
  ],
});
