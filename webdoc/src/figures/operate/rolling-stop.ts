import { sequence } from "@/lib/figure-spec";

// operate/cluster.mdx, "How it works" (the stop) and "Rolling upgrades".
export default sequence({
  alt: "A rolling restart of the leader. The operator sends SIGTERM to node 1, the leader. It hands its ephemeral partitions to their next owners, lets the acks already in flight commit for at most 500 ms, and transfers leadership to node 2, the most caught-up voter, waiting up to 3 seconds for it to take over. Node 1 then closes its listener, finishes its requests in flight as a follower and exits. Restarted on the same data directory, it catches up on the entries it missed from node 2, and its /health answers 200 once it is within 1,000 entries or 2 seconds of the leader. Then the operator moves to the next node.",
  caption: "A planned stop costs one transfer and a vote, about a tenth of a second in the Compose rehearsal. /health turning 200 is the signal to touch the next node.",
  gap: 196,
  actors: [
    { id: "op", label: "operator", tone: "ghost" },
    { id: "n1", label: "node 1", sub: "leader, stopping", tone: "strong" },
    { id: "n2", label: "node 2", sub: "most caught up" },
  ],
  steps: [
    { from: "op", to: "n1", label: "SIGTERM" },
    { from: "n1", to: "n2", label: "ephemeral partitions" },
    { from: "n1", to: "n1", label: "in-flight acks commit", sub: "500 ms at most" },
    { from: "n1", to: "n2", label: "transfer leadership", tone: "strong" },
    { note: "leads within 3 s", over: "n2", tone: "strong" },
    { note: "close the listener, finish\nrequests as a follower, exit", over: "n1" },
    { divider: "restarted on the same data directory" },
    { from: "n2", to: "n1", label: "the entries it missed" },
    { from: "n1", to: "op", label: "/health 200, caught up", reply: true, tone: "strong" },
    { note: "next node", over: "op" },
  ],
});
