import { line } from "@/lib/figure-spec";

// benchmarks/laravel.mdx, "The soak": the first soak (three engines), sampled
// every minute. Generated from raw/soak-memory.csv and raw/soak-events.csv in
// benchmark-queen/2026-10-02-laravel-failure-matrix; x is minutes from the start.
export default line({
  alt: "The median worker's resident memory over the 45-minute soak, sampled every minute. It grows slowly on every engine, from about 48 to 57 MiB on Horizon and from about 32 to 47 MiB on both Queen engines, and dips by about 2 MiB when the deploy after 23 minutes replaces the workers.",
  caption: "Long-lived PHP workers grow slowly on every engine; the workers forked from one booted Laravel start smaller.",
  source: "benchmark-queen/2026-10-02-laravel-failure-matrix/raw/soak-memory.csv",
  x: { label: "minutes", ticks: [0, 10, 20, 30, 40], min: 0, max: 45 },
  y: { label: "median worker resident memory, MiB", ticks: [0, 20, 40, 60], min: 0, max: 70 },
  height: 230,
  markers: [
    { x: 10.0, label: "worker killed", tone: "danger" },
    { x: 20.1, label: "worker killed", tone: "danger" },
    { x: 23.2, label: "deploy", tone: "warn" },
    { x: 30.3, label: "worker killed", tone: "danger" },
    { x: 40.3, label: "worker killed", tone: "danger" },
  ],
  series: [
    { label: "Queen Rust", tone: "s1", marks: false, points: [[0.0, 31.5], [1.0, 38.7], [2.0, 39.0], [3.0, 39.6], [4.02, 40.1], [5.02, 40.3], [6.02, 40.5], [7.02, 40.5], [8.02, 41.0], [9.02, 41.0], [10.02, 42.0], [11.03, 42.0], [12.03, 42.1], [13.03, 42.2], [14.03, 42.3], [15.03, 42.3], [16.05, 42.5], [17.05, 42.8], [18.05, 43.2], [19.05, 43.2], [20.05, 44.9], [21.05, 45.0], [22.07, 45.0], [23.07, 45.0], [24.27, 43.0], [25.27, 43.0], [26.27, 43.0], [27.27, 43.1], [28.27, 45.0], [29.27, 45.0], [30.27, 45.0], [31.28, 44.8], [32.28, 44.8], [33.28, 45.0], [34.28, 45.0], [35.28, 45.0], [36.3, 45.0], [37.3, 45.0], [38.3, 45.0], [39.3, 45.0], [40.3, 45.1], [41.32, 45.1], [42.32, 46.8], [43.32, 46.8], [44.32, 46.8]] },
    { label: "Queen PHP", tone: "s2", marks: false, points: [[0.0, 33.1], [1.0, 38.8], [2.0, 39.3], [3.0, 39.7], [4.02, 40.0], [5.02, 40.2], [6.02, 40.6], [7.02, 40.9], [8.02, 41.0], [9.02, 41.7], [10.02, 41.8], [11.03, 41.8], [12.03, 42.0], [13.03, 42.1], [14.03, 42.3], [15.03, 42.4], [16.05, 42.5], [17.05, 42.8], [18.05, 44.8], [19.05, 44.9], [20.05, 44.9], [21.07, 44.8], [22.07, 44.9], [23.07, 44.9], [24.25, 43.1], [25.25, 43.2], [26.25, 43.2], [27.25, 43.2], [28.25, 45.0], [29.25, 45.1], [30.25, 45.1], [31.27, 45.1], [32.27, 45.1], [33.27, 45.1], [34.27, 45.1], [35.27, 45.1], [36.28, 45.1], [37.28, 47.0], [38.28, 47.0], [39.28, 47.1], [40.28, 47.1], [41.28, 47.1], [42.3, 47.1], [43.3, 47.1], [44.3, 47.1]] },
    { label: "Horizon", tone: "s4", marks: false, points: [[0.0, 47.6], [1.0, 49.7], [2.0, 49.8], [3.0, 49.9], [4.02, 50.0], [5.02, 50.4], [6.02, 50.6], [7.02, 50.7], [8.02, 50.9], [9.02, 51.0], [10.02, 51.2], [11.03, 51.4], [12.03, 51.4], [13.03, 51.5], [14.03, 52.6], [15.03, 52.7], [16.03, 53.7], [17.05, 53.8], [18.05, 53.9], [19.05, 54.0], [20.05, 54.1], [21.05, 54.3], [22.07, 54.4], [23.07, 54.5], [24.27, 52.6], [25.27, 52.9], [26.27, 52.9], [27.27, 54.6], [28.27, 54.7], [29.27, 54.7], [30.28, 54.7], [31.28, 54.7], [32.28, 54.7], [33.28, 54.7], [34.28, 54.7], [35.3, 54.6], [36.3, 54.7], [37.3, 54.7], [38.3, 54.7], [39.3, 54.7], [40.3, 56.6], [41.32, 56.6], [42.32, 56.6], [43.32, 56.6], [44.32, 56.6]] },
  ],
});
