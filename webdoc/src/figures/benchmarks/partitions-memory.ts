import { line } from "@/lib/figure-spec";

// benchmarks/partitions.mdx, "What a partition costs": resident memory of Queen 2.4's leader and of a follower at the
// end of each point, 2026-10-15 (70 s of load, nothing deleted). 3,000,000 msg/s of 256-byte messages and 1,000,000
// msg/s of 1 KB messages. The 10-16 runs at 1,000 and 50,000 partitions (90 s of load) are left out: they kept 20 s
// more of messages' rows.
export default line({
  alt: "Resident memory of QueenMQ 2.4's nodes against the partition count (log scale). At 3,000,000 msg/s of 256-byte messages the leader holds 5.1 to 6.0 GB from 10 to 100,000 partitions, 7.7 GB at 1,000,000 and 17.2 GB at 10,000,000, and a follower 4.6 to 5.0 GB, 5.8 GB and 14.2 GB. At 1,000,000 msg/s of 1 KB messages the leader holds 2.3 to 4.2 GB up to 100,000 partitions, 6.5 GB at 1,000,000 and 14.1 GB at 10,000,000.",
  caption: "A partition costs QueenMQ about 1.2 KB on the leader and 0.9 KB on a follower: memory hardly moves up to 100,000 partitions and reaches 17 GB of the leader's 88 at ten million.",
  source: "benchmark-queen/2026-10-15-bytes-per-msg/runs/gcp/grid-fix-m3.jsonl and grid-fix-1m1k.jsonl (mem_gb)",
  x: { label: "partitions in the queue", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "resident memory of a node", format: "gb", ticks: [0, 5, 10, 15, 20], min: 0, max: 20 },
  series: [
    { label: "Leader, 3M msg/s of 256 B", tone: "s1", points: [[10, 5.2], [100, 5.1], [1000, 5.4], [10000, 5.6], [100000, 6.0], [1000000, 7.7], [10000000, 17.2]] },
    { label: "Follower, 3M msg/s of 256 B", tone: "s2", points: [[10, 4.8], [100, 4.6], [1000, 4.9], [10000, 4.9], [100000, 5.0], [1000000, 5.8], [10000000, 14.2]] },
    { label: "Leader, 1M msg/s of 1 KB", tone: "s3", points: [[10, 2.3], [100, 2.4], [1000, 3.3], [10000, 3.7], [100000, 4.2], [1000000, 6.5], [10000000, 14.1]] },
  ],
});
