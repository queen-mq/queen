// Chapter 2.8: a lock expires without telling its holder; the guard keeps it out.
export default {
  id: "lock-fence",
  height: 330,
  alt: "Worker A takes the lock daily-report for 30 seconds and then is paused past that. The lock expires and worker B takes it. When A wakes up, its transaction carries guard(lock), and since the lock is no longer A's, the transaction rolls back with kv_precondition and writes nothing. B's guarded transaction commits.",
  draw(c) {
    c.sequence(
      [{ id: "a", label: "worker A" }, { id: "q", label: "Queen", tone: "strong" }, { id: "b", label: "worker B" }],
      [
        { from: "a", to: "q", label: "acquire daily-report, 30 s" },
        { note: "A is paused past 30 s", over: "a", tone: "danger" },
        { note: "the lock expires", over: "q" },
        { from: "b", to: "q", label: "acquire: B holds it now" },
        { from: "b", to: "q", label: "commit with guard(lock)", tone: "ok" },
        { from: "a", to: "q", label: "commit with guard(lock)" },
        { from: "q", to: "a", label: "rolled back: kv_precondition", reply: true, tone: "danger" },
      ],
      { y: 12, rowH: 38 },
    );
  },
};
