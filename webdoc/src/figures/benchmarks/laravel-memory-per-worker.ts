import { bar } from "@/lib/figure-spec";

// benchmarks/laravel.mdx, "Jobs that allocate": private memory per worker,
// 64 workers idle after the last job, medians of three runs.
export default bar({
  alt: "Private memory per worker, 64 workers, with the 10 ms job and with a job that holds 8 MiB. Horizon, spawned, opcache off: 28.9 and 37.1 MiB. Horizon, spawned, opcache on: 35.9 and 43.9. Queen, spawned, opcache off: 31.1 and 39.4. Queen, spawned, opcache on: 38.1 and 46.0. Queen, forked, opcache off: 6.2 and 14.5. Queen, forked, opcache on: 1.7 and 10.0.",
  caption: "A spawned worker holds its own booted Laravel; a forked one shares the fork server's. The job's 8 MiB lands on every worker alike, so forking still saves about 27 MiB per worker.",
  source: "benchmark-queen/2026-10-05-laravel-worker-memory",
  categories: [
    "Horizon, spawned, opcache off",
    "Horizon, spawned, opcache on",
    "Queen, spawned, opcache off",
    "Queen, spawned, opcache on",
    "Queen, forked, opcache off",
    "Queen, forked, opcache on",
  ],
  series: [
    { label: "10 ms job", values: [28.9, 35.9, 31.1, 38.1, 6.2, 1.7] },
    { label: "job holding 8 MiB", values: [37.1, 43.9, 39.4, 46.0, 14.5, 10.0] },
  ],
  x: { label: "private memory per worker, MiB", format: (v) => (Number.isInteger(v) ? String(v) : v.toFixed(1)) },
});
