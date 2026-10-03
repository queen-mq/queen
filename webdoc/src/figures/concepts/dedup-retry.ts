import { sequence } from "@/lib/figure-spec";

// concepts/dedup.mdx, "How the check works".
export default sequence({
  alt: "A producer pushes a message with transactionId order-9137-v3. The leader checks the partition's index of recent ids, stores the message at offset 41 and answers 201 queued, but the answer is lost and the producer sees a timeout. The leader then fails over. The producer pushes the same id again, the new leader finds it in the replicated index and answers 201 duplicate with the original offset 41, appending nothing.",
  caption: "The index of recent ids is replicated state, so the answer to a retry does not depend on which node leads. The window is the queue's dedupWindowSeconds, an hour by default.",
  gap: 200,
  actors: [
    { id: "p", label: "producer", tone: "ghost" },
    { id: "l1", label: "leader", sub: "node 1", tone: "strong" },
    { id: "l2", label: "new leader", sub: "node 2" },
  ],
  steps: [
    { from: "p", to: "l1", label: "push order-9137-v3" },
    { from: "l1", to: "l1", label: "new id: offset 41" },
    { from: "l1", to: "p", label: "201 queued, lost", reply: true, tone: "faint" },
    { note: "timeout: stored or not?", over: "p", tone: "warn" },
    { divider: "node 1 fails, node 2 leads" },
    { from: "p", to: "l2", label: "push order-9137-v3 again" },
    { from: "l2", to: "l2", label: "id seen: nothing appended" },
    { from: "l2", to: "p", label: "201 duplicate, offset 41", reply: true, tone: "strong" },
  ],
});
