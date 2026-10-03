import { bar } from "@/lib/figure-spec";

// benchmarks/transactions.mdx, "Latency at 9,000 messages a second".
export default bar({
  alt: "p99 latencies of the transaction pipeline at 9,000 messages a second on three nodes. Commit p99: Queen 4 ms, Kafka 34 ms, Redpanda 151 ms, Pulsar 32 ms. End-to-end p99: Queen 19 ms, Kafka 51 ms, Redpanda 338 ms, Pulsar 50 ms.",
  caption: "Ten messages per transaction. A Queen transaction is one log entry, so it costs about what one write costs; Kafka, Redpanda and Pulsar each go through a transaction coordinator.",
  source: "benchmark-queen/2026-09-30-kafka-pulsar",
  categories: ["commit p99", "end-to-end p99"],
  series: [
    { label: "Queen", values: [4, 19] },
    { label: "Kafka", values: [34, 51] },
    { label: "Redpanda", values: [151, 338] },
    { label: "Pulsar", values: [32, 50] },
  ],
  x: { label: "milliseconds", format: "ms" },
});
