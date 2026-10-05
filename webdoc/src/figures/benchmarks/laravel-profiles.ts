import { bar } from "@/lib/figure-spec";

// benchmarks/laravel.mdx, "Delivery profiles and prefetch auto": jobs per
// second of 32 Queen workers by profile, medians of three runs.
export default bar({
  alt: "Jobs per second of 32 Queen workers by delivery profile. Empty jobs: safe 3,131, balanced 5,675, fast 6,015. 10 ms jobs: safe 1,548, balanced 2,007, fast 2,836. 200 ms jobs: safe 139, balanced 139, fast 147.",
  caption: "Balanced and fast size each pop with prefetch 'auto'. Short jobs gain the most; jobs of 200 ms keep one job per pop and run as fast as safe.",
  source: "benchmark-queen/2026-10-05-laravel-auto-prefetch",
  categories: ["Empty jobs", "10 ms jobs", "200 ms jobs"],
  series: [
    { label: "Safe", tone: "s4", values: [3131, 1548, 139] },
    { label: "Balanced", tone: "s2", values: [5675, 2007, 139] },
    { label: "Fast", tone: "s1", values: [6015, 2836, 147] },
  ],
  x: { label: "jobs per second", format: "int" },
});
