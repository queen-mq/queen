import { sequence } from "@/lib/figure-spec";

// guides/ephemeral.mdx, "In a cluster": the SIGTERM row and the crash row of the
// table, on the page's three-node example. Order from handlers/ephemeral.rs
// `ephemeral_drain` (leaving notices, then reap_foreign + ship to `_adopt`) and
// main.rs (the drain runs before the raft leadership hand-off).
export default sequence({
  alt: "A sequence on three nodes. queen-3 receives SIGTERM and from then on owns nothing. It tells queen-2 and queen-1 that it is leaving, and both leave it out of the placement hash for 30 seconds. queen-3 then sends each of them the partitions they now own, with the messages and every consumer group's position; each answers adopted. queen-3 logs the hand-over, hands off raft leadership and exits. In the alternative where queen-3 crashes instead (kill -9, out of memory), its partitions' messages are lost with the process, and after 4 seconds without hearing from it the other nodes re-hash and its partitions start again on them, empty.",
  caption: "A stop moves the contents, a crash loses them. The hand-over runs before the node gives up raft leadership, while its peers still answer and it still serves.",
  source: "server/src/handlers/ephemeral.rs (ephemeral_drain, ship, handle_ephemeral_adopt), server/src/main.rs",
  gap: 210,
  actors: [
    { id: "q1", label: "queen-1" },
    { id: "q2", label: "queen-2" },
    { id: "q3", label: "queen-3", sub: "stopping", tone: "strong" },
  ],
  steps: [
    { note: "SIGTERM: owns nothing now", over: "q3", tone: "strong" },
    { from: "q3", to: "q2", label: "leaving" },
    { from: "q3", to: "q1", label: "leaving" },
    { note: "queen-3 left out of the hash for 30 s", over: ["q1", "q2"] },
    { from: "q3", to: "q2", label: "the partitions queen-2 now owns", sub: "messages, group positions", tone: "strong" },
    { from: "q2", to: "q3", label: "adopted", reply: true },
    { from: "q3", to: "q1", label: "the partitions queen-1 now owns", sub: "messages, group positions", tone: "strong" },
    { from: "q1", to: "q3", label: "adopted", reply: true },
    { note: "all handed over\nraft hand-off, exit", over: "q3" },
    { divider: "or a crash: kill -9, out of memory" },
    { note: "its messages die with it", over: "q3", tone: "danger" },
    { note: "4 s without a word from queen-3: re-hash,\nits partitions start again here, empty", over: ["q1", "q2"], tone: "warn" },
  ],
});
