import { sequence } from "@/lib/figure-spec";

// guides/exactly-once.mdx, "The fix": one order through the consumer on the page,
// then the same commit after the lease ran out during the charge.
export default sequence({
  alt: "A sequence between a worker of the charger group, Queen and the card provider. The worker pops one order with its lease, reads the marker charge:1042 from KV and finds nothing, then asks the provider to charge the card with the order id as idempotency key and gets a chargeId back. It commits one transaction that puts the marker with putIfAbsent and required: true and acks the order under the lease. Queen writes both as one log entry if the lease still holds and no marker exists, and answers success. In the alternative where the lease ran out during the charge, the commit is refused with rejected_ack: neither the marker nor the ack is written, and the order goes to the next worker.",
  caption: "The marker and the ack are one log entry, so the marker exists exactly when the order is acknowledged. The provider's call is outside that entry, which is why the order id goes with it as an idempotency key.",
  source: "examples/apps/js/exactly-once.mjs, clients/client-js/client-v2/builders/TransactionBuilder.js",
  gap: 210,
  actors: [
    { id: "w", label: "worker", sub: "group charger", tone: "ghost" },
    { id: "q", label: "Queen", tone: "strong" },
    { id: "p", label: "card provider", tone: "ghost" },
  ],
  steps: [
    { from: "w", to: "q", label: "pop orders, batch 1" },
    { from: "q", to: "w", label: "order 1042 + lease", reply: true },
    { from: "w", to: "q", label: "kv.get charge:1042" },
    { from: "q", to: "w", label: "not found", reply: true },
    { from: "w", to: "p", label: "charge the card", sub: "order id as idempotency key" },
    { from: "p", to: "w", label: "chargeId", reply: true },
    { from: "w", to: "q", label: "transaction", sub: "putIfAbsent marker + ack", tone: "strong" },
    { note: "lease held, no marker yet\n(required: true): one log entry", over: "q", tone: "strong" },
    { from: "q", to: "w", label: "success", reply: true },
    { divider: "the same commit, after the lease ran out" },
    { note: "rejected_ack: no marker, no ack;\nthe order goes to the next worker", over: "q", tone: "warn" },
  ],
});
