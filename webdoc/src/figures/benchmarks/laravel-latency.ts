import { bar } from "@/lib/figure-spec";

// benchmarks/laravel.mdx, "Latency at a steady rate": the paced lanes of
// 2026-10-01 on the Linux server, 16 workers, from dispatch to the end of the job.
export default bar({
  alt: "Latency from dispatch to the end of a job with 16 workers on one 16-vCPU server. At 500 jobs/s asked, median of three runs: Queen p50 15 ms and p95 23 ms, Horizon p50 75 ms and p95 227 ms. At 400 jobs/s for 15 minutes, one run: Queen p50 14 ms and p95 22 ms, Horizon p50 71 ms and p95 249 ms.",
  caption: "Below capacity Queen answers in about a fifth of Horizon's median time and a tenth of its p95. Horizon's producer reached 440 of the 500 jobs/s asked.",
  source: "benchmark-queen/2026-10-01-linux-vm-horizon-raft",
  categories: ["500 jobs/s, p50", "500 jobs/s, p95", "400 jobs/s for 15 min, p50", "400 jobs/s for 15 min, p95"],
  series: [
    { label: "Queen", values: [15, 23, 14, 22] },
    { label: "Horizon", values: [75, 227, 71, 249] },
  ],
  x: { label: "ms, dispatch to completion", format: "int" },
});
