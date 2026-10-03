import { sequence } from "@/lib/figure-spec";

// guides/laravel/concepts.mdx, "The life of a job" and "The lease".
export default sequence({
  alt: "The life of a Laravel job on Queen. The app dispatches it: one push, with the job's UUID as transactionId, so a repeated push is stored once. A worker pops up to its prefetch of jobs under a lease of retry_after seconds and handles each as Laravel always does, with middleware, timeout and tries. A finished job is acknowledged as completed. release() acknowledges the job and queues it again in one broker transaction. A job that fails for the last time goes to the dead-letter queue with its error, and Laravel writes its failed_jobs row. A worker that dies sends no ACK, and the job is delivered again when the lease expires.",
  caption: "Every outcome is one call to the broker, and a crash needs none: the lease running out is the retry.",
  gap: 200,
  actors: [
    { id: "app", label: "Laravel app", tone: "ghost" },
    { id: "q", label: "Queen", tone: "strong" },
    { id: "w", label: "worker", sub: "queue:work queen", tone: "ghost" },
  ],
  steps: [
    { from: "app", to: "q", label: "dispatch: one push", sub: "id = the job's UUID" },
    { from: "w", to: "q", label: "pop, up to prefetch" },
    { from: "q", to: "w", label: "jobs, lease of retry_after", reply: true },
    { from: "w", to: "w", label: "handle" },
    { from: "w", to: "q", label: "ACK completed", tone: "strong" },
    { divider: "or" },
    { from: "w", to: "q", label: "release(): ACK + queue again", sub: "one broker transaction" },
    { divider: "or the last failure" },
    { from: "w", to: "q", label: "failed: dead-letter queue", tone: "warn" },
    { divider: "or the worker dies" },
    { note: "the lease expires:\ndelivered again", over: "q", tone: "warn" },
  ],
});
