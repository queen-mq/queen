import { sequence } from "@/lib/figure-spec";

// concepts/transactions.mdx, "The lease is the fence".
export default sequence({
  alt: "Worker A pops a batch and gets lease L1, then takes longer than the lease. The lease expires, and worker B's pop gets the same batch under lease L2. When worker A commits its transaction (the ack under L1, a KV write and a push), the broker refuses it with rejected_ack and writes none of it. Worker B's transaction, under L2, commits.",
  caption: "The ack carries the lease, and the lease decides for the whole call. A worker that lost its message cannot write its state late, which a compare-and-swap on a version alone would allow.",
  gap: 200,
  actors: [
    { id: "a", label: "worker A", tone: "ghost" },
    { id: "q", label: "Queen", tone: "strong" },
    { id: "b", label: "worker B", tone: "ghost" },
  ],
  steps: [
    { from: "a", to: "q", label: "pop" },
    { from: "q", to: "a", label: "batch, lease L1", reply: true },
    { note: "slow: L1 expires", over: "a", tone: "warn" },
    { from: "b", to: "q", label: "pop" },
    { from: "q", to: "b", label: "same batch, lease L2", reply: true },
    { from: "a", to: "q", label: "ack (L1) + KV + push" },
    { from: "q", to: "a", label: "rejected_ack, nothing written", reply: true, tone: "danger" },
    { from: "b", to: "q", label: "ack (L2) + KV + push", tone: "strong" },
    { from: "q", to: "b", label: "success: one entry", reply: true, tone: "strong" },
  ],
});
