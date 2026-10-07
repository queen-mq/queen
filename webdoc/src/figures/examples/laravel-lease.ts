import { sequence } from "@/lib/figure-spec";

// examples/laravel/lease.mdx: one job of 8 s on a lease of 6 s, two workers,
// first without lease renewal, then with it.
export default sequence({
  alt: "Without renewal: worker w1 pops the job under a 6 s lease and runs it for 8 s. At 6 s the lease expires and worker w2 pops the same job, attempt 2. At 8 s w1 sends its ACK under the expired lease and the broker refuses it. At 12 s the second lease expires too; w1 pops attempt 3, which is past --tries=2, so Laravel fails it and the driver files it in the dead-letter queue. At 14 s w2's ACK is refused as well. With renewal: a worker pops the job, its renewal helper extends the lease every second, and at 8 s the broker accepts the ACK.",
  caption: "The ACK carries the lease, so a run that outlives its lease cannot complete the job. Without renewal every run of this job outlives it; with renewal the first one completes.",
  source: "examples/apps/laravel, php artisan example:lease",
  gap: 190,
  actors: [
    { id: "a", label: "w1", sub: "queue:work", tone: "ghost" },
    { id: "q", label: "Queen", tone: "strong" },
    { id: "b", label: "w2", sub: "queue:work", tone: "ghost" },
  ],
  steps: [
    { divider: "a) lease of 6 s, not renewed" },
    { from: "a", to: "q", label: "pop" },
    { from: "q", to: "a", label: "job, lease 6 s", reply: true },
    { note: "6 s: the lease expires", over: "q", tone: "warn" },
    { from: "b", to: "q", label: "pop" },
    { from: "q", to: "b", label: "same job, attempt 2", reply: true },
    { from: "a", to: "q", label: "8 s: ACK, old lease" },
    { from: "q", to: "a", label: "refused", reply: true, tone: "danger" },
    { note: "12 s: it expires again", over: "q", tone: "warn" },
    { from: "a", to: "q", label: "pop" },
    { from: "q", to: "a", label: "attempt 3", reply: true },
    { from: "a", to: "q", label: "past --tries=2: dead letter", tone: "danger" },
    { from: "b", to: "q", label: "14 s: ACK, old lease" },
    { from: "q", to: "b", label: "refused", reply: true, tone: "danger" },
    { divider: "b) the same lease, renewed every second" },
    { from: "a", to: "q", label: "pop" },
    { from: "q", to: "a", label: "job, lease 6 s", reply: true },
    { from: "a", to: "q", label: "renew, every second", sub: "the worker's helper process" },
    { from: "a", to: "q", label: "8 s: ACK", tone: "strong" },
    { from: "q", to: "a", label: "accepted", reply: true, tone: "strong" },
  ],
});
