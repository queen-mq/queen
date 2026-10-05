import { bar } from "@/lib/figure-spec";

// benchmarks/laravel.mdx, "Memory": the application side, master plus workers.
export default bar({
  alt: "Memory of the Laravel application processes by worker count. 16 workers: Horizon 525 MiB, Queen 83 MiB. 32 workers: 988 and 120 MiB. 64 workers: 1,892 and 183 MiB.",
  caption: "A Queen worker is forked from one booted Laravel and shares its pages; a Horizon worker boots its own. Prefork saves the booted framework, about 27 MiB per worker; the memory a job allocates is private on both, so a real application's ratio is smaller.",
  source: "benchmark-queen/2026-10-01-linux-vm-horizon-raft",
  categories: ["16 workers", "32 workers", "64 workers"],
  series: [
    { label: "Queen", values: [83, 120, 183] },
    { label: "Horizon", values: [525, 988, 1892] },
  ],
  x: { label: "application memory, MiB", format: "int" },
});
