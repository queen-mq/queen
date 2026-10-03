import { bar } from "@/lib/figure-spec";

// benchmarks/laravel.mdx, "Worker capacity on a Linux server": medians of 3.
export default bar({
  alt: "Laravel jobs per second on one 16-vCPU server, Queen's driver and supervisor on one Queen node against Horizon on Redis, medians of three runs. 32 workers with 10 ms jobs and Redis fsyncing every write: Queen 2,753, Horizon 1,124. With Redis everysec: 2,752 and 1,247. With Redis appendfsync no: 2,714 and 1,313. 32 workers with empty jobs: 5,995 and 1,090. 64 workers with 10 ms jobs: 4,395 and 1,090.",
  caption: "Queen fsyncs every write in every lane; only Redis's setting changes. Horizon stays near 1,100 to 1,300 jobs a second however it is tuned.",
  source: "benchmark-queen/2026-10-01-linux-vm-horizon-raft",
  categories: ["32 workers, 10 ms jobs", "Redis everysec", "Redis appendfsync no", "32 workers, empty jobs", "64 workers, 10 ms jobs"],
  series: [
    { label: "Queen", values: [2753, 2752, 2714, 5995, 4395] },
    { label: "Horizon", values: [1124, 1247, 1313, 1090, 1090] },
  ],
  x: { label: "jobs per second", format: "int" },
});
