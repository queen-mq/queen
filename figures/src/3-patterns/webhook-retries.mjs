// Chapter 3.10: a webhook with retries and an idempotency key.
export default {
  id: "webhook-retries",
  height: 370,
  alt: "The sender pops evt-9001 and calls the customer's URL with Idempotency-Key evt-9001. The receiver answers 500, so the handler throws and the message comes back after retryDelay. The second call succeeds and the receiver stores the event once under its key. If a crash had made the sender call twice, the receiver would drop the second call by its key. After the last of five tries a message goes to the dead-letter queue.",
  draw(c) {
    c.sequence(
      [{ id: "q", label: "Queen", tone: "strong" }, { id: "s", label: "sender", sub: "group sender" }, { id: "r", label: "customer's server" }],
      [
        { from: "q", to: "s", label: "evt-9001" },
        { from: "s", to: "r", label: "POST, Idempotency-Key: evt-9001" },
        { from: "r", to: "s", label: "500", reply: true, tone: "danger" },
        { note: "throw: back after retryDelay, 1 of 5 tries spent", over: ["q", "s"], tone: "muted" },
        { from: "q", to: "s", label: "evt-9001 again" },
        { from: "s", to: "r", label: "POST, Idempotency-Key: evt-9001" },
        { from: "r", to: "s", label: "200: kept once under its key", reply: true, tone: "ok" },
      ],
      { y: 12, rowH: 42 },
    );
  },
};
