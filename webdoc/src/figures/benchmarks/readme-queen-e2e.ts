import { line } from "@/lib/figure-spec";

// The README's chart (assets/bench-queen-e2e-{light,dark}.svg), captured from the docs renderer on 2026-10-14 and redrawn
// for 2.4 on the same axes by benchmark-queen/2026-10-15-bytes-per-msg/charts/readme24_svg.py; not on a docs page.
// Queen 2.4 on three GCP c4-standard-24-lssd brokers: e2e p99, the slowest load process's worst 10 s window in seconds
// 30 to 60, at 3M msg/s of 256-byte messages (runs fm3-*; 1,000 partitions the median of five runs) and 1M msg/s of
// 1 KB messages (runs f1k-*). 2.3.0 read 78, 65, 56, 66, 71, 47, 54 and 68, 34, 27, 28, 27, 29, 65.
export default line({
  alt: "QueenMQ 2.4 end-to-end p99 against the partition count from 10 to 10,000,000 on three GCP nodes with local NVMe: at 3,000,000 msg/s of 256-byte messages 27, 17, 21, 26, 17, 21 and 29 ms; at 1,000,000 msg/s of 1 KB messages 24, 19, 17, 17, 21, 17 and 25 ms.",
  caption: "QueenMQ 2.4, e2e p99 by partition count at 3M msg/s of 256-byte messages and 1M msg/s of 1 KB messages.",
  source: "benchmark-queen/2026-10-15-bytes-per-msg/runs/gcp, the fm3-* and f1k-* folders",
  x: { label: "partitions in one queue", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "p99 end-to-end latency", format: "ms", ticks: [0, 20, 40, 60, 80], min: 0, max: 90 },
  width: 820,
  height: 330,
  series: [
    { label: "3M msg/s, 256 B", tone: "s1", points: [[10, 27], [100, 17], [1000, 21], [10000, 26], [100000, 17], [1000000, 21], [10000000, 29]] },
    { label: "1M msg/s, 1 KB", points: [[10, 24], [100, 19], [1000, 17], [10000, 17], [100000, 21], [1000000, 17], [10000000, 25]] },
  ],
});
