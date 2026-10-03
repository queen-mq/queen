import { bar } from "@/lib/figure-spec";

// benchmarks/kafka-clients.mdx, "CPU": cores at 1,000,000 msg/s, three brokers.
export default bar({
  alt: "Cores used by Queen at 1,000,000 msg/s, summed over three brokers, with Kafka clients through the Kafka facade and with Queen's own clients. 200 partitions: 22.1 and 17.2. 10,000: 25.2 and 18.4. 50,000: 36.9 and 18.0. 100,000: 49.7 and 18.3.",
  caption: "The gap grows on the fetch path. The facade answers Fetch below fetch sessions, so every fetch names all of its consumer's partitions; a Queen pop asks for work and is handed partitions that have some.",
  source: "benchmark-queen/2026-09-30-stage03-3node",
  categories: ["200 partitions", "10,000", "50,000", "100,000"],
  series: [
    { label: "Kafka clients on Queen", values: [22.1, 25.2, 36.9, 49.7] },
    { label: "Queen clients", tone: "s4", values: [17.2, 18.4, 18.0, 18.3] },
  ],
  x: { label: "cores, three brokers", format: (v) => (Number.isInteger(v) ? String(v) : v.toFixed(1)) },
});
