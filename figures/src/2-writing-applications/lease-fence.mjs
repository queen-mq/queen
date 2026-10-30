// Chapter 2.7: a transaction's ack checks its lease, so a late worker writes nothing.
export default {
  id: "lease-fence",
  height: 330,
  alt: "Worker w1 pops a batch and is paused, a long garbage-collection pause, past its lease. The lease runs out and w2 gets the same batch, processes it and commits. When w1 wakes up and commits its transaction (the ack, a KV write and a push), the ack's lease is no longer w1's, so the whole transaction rolls back: the push and the KV write are not written either.",
  draw(c) {
    const bottom = c.sequence(
      [{ id: "w1", label: "worker w1" }, { id: "q", label: "Queen", tone: "strong" }, { id: "w2", label: "worker w2" }],
      [
        { from: "w1", to: "q", label: "pop: batch leased to w1" },
        { note: "w1 is paused past its lease", over: "w1", tone: "danger" },
        { note: "the lease runs out", over: "q" },
        { from: "w2", to: "q", label: "pop: the same batch, leased to w2" },
        { from: "w2", to: "q", label: "commit: ack + kv + push", tone: "ok" },
        { from: "w1", to: "q", label: "commit: ack + kv + push" },
        { from: "q", to: "w1", label: "rolled back: the lease is not yours", reply: true, tone: "danger" },
      ],
      { y: 12, rowH: 38 },
    );
    void bottom;
  },
};
