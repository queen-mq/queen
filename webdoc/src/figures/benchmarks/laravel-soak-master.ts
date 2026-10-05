import { line } from "@/lib/figure-spec";

// benchmarks/laravel.mdx, "The soak": the first soak (three engines), sampled
// every minute. Generated from raw/soak-memory.csv and raw/soak-events.csv in
// benchmark-queen/2026-10-02-laravel-failure-matrix; x is minutes from the start.
export default line({
  alt: "The master's resident memory over the 45-minute soak, sampled every minute, with a worker killed every 10 minutes and a deploy after 23 minutes. Every line is flat: Queen PHP at about 58 MiB, Horizon at about 49 MiB and Queen Rust at about 7 MiB, from the first sample to the last.",
  caption: "The masters do not grow: a worker killed, a deploy and 45 minutes of mixed jobs leave each one where it started.",
  source: "benchmark-queen/2026-10-02-laravel-failure-matrix/raw/soak-memory.csv",
  x: { label: "minutes", ticks: [0, 10, 20, 30, 40], min: 0, max: 45 },
  y: { label: "master resident memory, MiB", ticks: [0, 20, 40, 60], min: 0, max: 70 },
  height: 230,
  markers: [
    { x: 10.0, label: "worker killed", tone: "danger" },
    { x: 20.1, label: "worker killed", tone: "danger" },
    { x: 23.2, label: "deploy", tone: "warn" },
    { x: 30.3, label: "worker killed", tone: "danger" },
    { x: 40.3, label: "worker killed", tone: "danger" },
  ],
  series: [
    { label: "Queen Rust", tone: "s1", marks: false, points: [[0.0, 6.6], [1.0, 6.9], [2.0, 6.9], [3.0, 6.9], [4.02, 6.9], [5.02, 6.9], [6.02, 6.9], [7.02, 6.9], [8.02, 6.9], [9.02, 6.9], [10.02, 6.9], [11.03, 6.9], [12.03, 6.9], [13.03, 6.9], [14.03, 6.9], [15.03, 6.9], [16.05, 6.9], [17.05, 6.9], [18.05, 6.9], [19.05, 6.9], [20.05, 6.9], [21.05, 6.9], [22.07, 6.9], [23.07, 6.9], [24.27, 6.9], [25.27, 6.9], [26.27, 6.9], [27.27, 6.9], [28.27, 6.9], [29.27, 6.9], [30.27, 6.9], [31.28, 6.9], [32.28, 6.9], [33.28, 6.9], [34.28, 6.9], [35.28, 6.9], [36.3, 6.9], [37.3, 6.9], [38.3, 6.9], [39.3, 6.9], [40.3, 6.9], [41.32, 6.9], [42.32, 6.9], [43.32, 7.0], [44.32, 7.0]] },
    { label: "Queen PHP", tone: "s2", marks: false, points: [[0.0, 58.3], [1.0, 58.3], [2.0, 58.3], [3.0, 58.3], [4.02, 58.3], [5.02, 58.3], [6.02, 58.3], [7.02, 58.3], [8.02, 58.3], [9.02, 58.3], [10.02, 58.3], [11.03, 58.3], [12.03, 58.3], [13.03, 58.3], [14.03, 58.3], [15.03, 58.3], [16.05, 58.3], [17.05, 58.3], [18.05, 58.3], [19.05, 58.3], [20.05, 58.3], [21.07, 58.3], [22.07, 58.3], [23.07, 58.3], [24.25, 57.8], [25.25, 57.8], [26.25, 57.8], [27.25, 57.8], [28.25, 57.8], [29.25, 57.8], [30.25, 57.8], [31.27, 57.8], [32.27, 57.8], [33.27, 57.8], [34.27, 57.8], [35.27, 57.8], [36.28, 57.8], [37.28, 57.8], [38.28, 57.8], [39.28, 57.8], [40.28, 57.8], [41.28, 57.8], [42.3, 57.8], [43.3, 57.8], [44.3, 57.8]] },
    { label: "Horizon", tone: "s4", marks: false, points: [[0.0, 48.8], [1.0, 48.9], [2.0, 48.9], [3.0, 48.9], [4.02, 48.9], [5.02, 48.9], [6.02, 48.9], [7.02, 48.9], [8.02, 48.9], [9.02, 48.9], [10.02, 48.9], [11.03, 48.9], [12.03, 49.0], [13.03, 49.0], [14.03, 49.0], [15.03, 49.0], [16.03, 49.0], [17.05, 49.0], [18.05, 49.0], [19.05, 49.0], [20.05, 49.0], [21.05, 49.0], [22.07, 49.0], [23.07, 49.0], [24.27, 48.9], [25.27, 48.9], [26.27, 48.9], [27.27, 48.9], [28.27, 48.9], [29.27, 48.9], [30.28, 48.9], [31.28, 48.9], [32.28, 48.9], [33.28, 48.9], [34.28, 48.9], [35.3, 48.9], [36.3, 48.9], [37.3, 48.9], [38.3, 48.9], [39.3, 48.9], [40.3, 48.9], [41.32, 48.9], [42.32, 48.9], [43.32, 48.9], [44.32, 48.9]] },
  ],
});
