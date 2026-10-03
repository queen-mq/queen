import { line } from "@/lib/figure-spec";

// benchmarks/partitions.mdx, "What a partition costs Queen": leader anonymous RSS.
export default line({
  alt: "The Queen leader's anonymous memory against the number of partitions in the queue, 3.6 GB with 200 partitions, 3.9 GB with 100,000, 5.6 GB with 1,000,000, 11.8 GB with 5,000,000 and 19.9 GB with 10,000,000.",
  caption: "A straight line: memory is what a partition costs, about 2 KB each on the leader, and every node holds the same store, so memory sets the partition ceiling.",
  source: "benchmark-queen/2026-09-30-stage03-3node",
  x: { label: "partitions in the queue", ticks: [0, 2500000, 5000000, 7500000, 10000000], min: 0, max: 10500000 },
  y: { label: "leader memory (anonymous RSS)", ticks: [0, 5, 10, 15, 20], min: 0, max: 22, format: "gb" },
  height: 260,
  series: [{ label: "Queen leader", points: [[200, 3.6], [100000, 3.9], [1000000, 5.6], [5000000, 11.8], [10000000, 19.9]] }],
});
