import { sequence } from "@/lib/figure-spec";

// concepts/timers.mdx, "What a fire does" and "Time in a state machine".
export default sequence({
  alt: "A worker commits a transaction that acks a booking event, writes the booking's state held and schedules a timer keyed by the booking id, 15 minutes ahead, into the booking's own partition. Fifteen minutes later the leader's timer tick, every 50 ms, finds it due and writes one entry that appends the hold-expired message to the partition and deletes the timer. A consumer then pops that message like any other, in order with the booking's other events.",
  caption: "The timer is born in the step that wants it and fires as an ordinary push. Scheduled into the entity's partition, its message arrives in order with the entity's other events.",
  gap: 200,
  actors: [
    { id: "w", label: "worker", tone: "ghost" },
    { id: "l", label: "leader", tone: "strong" },
    { id: "c", label: "consumer", tone: "ghost" },
  ],
  steps: [
    { from: "w", to: "l", label: "ack + put held + timer", sub: "in 15 min, into booking-7" },
    { from: "l", to: "w", label: "success: one entry", reply: true },
    { divider: "15 minutes later" },
    { from: "l", to: "l", label: "tick (50 ms): due" },
    { note: "one entry: append the message,\ndelete the timer", over: "l" },
    { from: "c", to: "l", label: "pop booking-7" },
    { from: "l", to: "c", label: "hold-expired, in order", reply: true, tone: "strong" },
  ],
});
