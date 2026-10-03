import { bar } from "@/lib/figure-spec";

// benchmarks/partitions.mdx, "Many queues": the same million partitions spread
// over more queues, 1,000,000 msg/s offered.
export default bar({
  alt: "End-to-end latency at 1,000,000 msg/s with one million partitions arranged three ways. One queue of 1,000,000 partitions: p50 46 ms, p99 123 ms. 1,000 queues of 1,000: p50 95 ms, p99 216 ms. 10,000 queues of 100: p50 185 ms, p99 561 ms, with 76 pops timing out at their 2-second deadline.",
  caption: "A queue costs more than a partition. With about one consumer per queue, each pop carried fewer messages at 10,000 queues (about 205 against 450) and half came back empty.",
  source: "benchmark-queen/2026-09-30-stage03-3node",
  categories: ["1 × 1,000,000", "1,000 × 1,000", "10,000 × 100"],
  series: [
    { label: "p99", values: [123, 216, 561] },
    { label: "p50", tone: "s3", values: [46, 95, 185] },
  ],
  x: { label: "end-to-end latency, ms", format: "ms" },
});
