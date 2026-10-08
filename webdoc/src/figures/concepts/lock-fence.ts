import { sequence } from "@/lib/figure-spec";

// concepts/locks.mdx, "A lease, not a mutex".
export default sequence({
  alt: "Worker A acquires the lock daily-report for 30 seconds and gets token 41, then stalls for longer than that. The lock expires, and nobody tells worker A. Worker B acquires the same lock and gets token 57, a higher one. When worker A wakes up and commits its transaction, guarded with token 41, the broker rolls it back with kv_precondition and writes none of it. Worker B's transaction, guarded with token 57, commits.",
  caption: "The lock does not stop a holder that outlived it. The guard does: a transaction commits only while the token it carries is still the lock's.",
  gap: 200,
  actors: [
    { id: "a", label: "worker A", tone: "ghost" },
    { id: "q", label: "Queen", tone: "strong" },
    { id: "b", label: "worker B", tone: "ghost" },
  ],
  steps: [
    { from: "a", to: "q", label: "acquire daily-report", sub: "for 30 s" },
    { from: "q", to: "a", label: "acquired, token 41", reply: true },
    { note: "stalled: the lock expires", over: "a", tone: "warn" },
    { from: "b", to: "q", label: "acquire daily-report" },
    { from: "q", to: "b", label: "acquired, token 57", reply: true },
    { from: "a", to: "q", label: "guard (41) + push" },
    { from: "q", to: "a", label: "kv_precondition, nothing written", reply: true, tone: "danger" },
    { from: "b", to: "q", label: "guard (57) + push", tone: "strong" },
    { from: "q", to: "b", label: "success: one entry", reply: true, tone: "strong" },
  ],
});
