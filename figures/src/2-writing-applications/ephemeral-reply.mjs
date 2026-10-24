// Chapter 2.9: request and reply over ephemeral queues, with a private inbox.
export default {
  id: "ephemeral-reply",
  height: 330,
  alt: "The caller mints a private inbox queue, inbox-<uuid>, and pushes a request to price-requests with replyTo set to it. A worker of the group pricers pops the request, pushes the answer to the inbox and acks. The caller's pop on its inbox, waiting with a timeout, returns the answer. Everything stays in memory, outside the log.",
  draw(c) {
    c.sequence(
      [{ id: "c", label: "caller" }, { id: "q", label: "Queen, in memory", tone: "accent" }, { id: "w", label: "worker", sub: "group pricers" }],
      [
        { note: "inbox = inbox-<uuid>", over: "c", mono: true },
        { from: "c", to: "q", label: "push price-requests { sku, replyTo: inbox }", mono: false },
        { from: "w", to: "q", label: "pop price-requests" },
        { from: "q", to: "w", label: "{ sku: 'A-1', replyTo }", reply: true },
        { from: "w", to: "q", label: "push inbox { price: 59.9 }, then ack" },
        { from: "c", to: "q", label: "pop inbox, wait up to 5 s" },
        { from: "q", to: "c", label: "{ sku: 'A-1', price: 59.9 }", reply: true, tone: "ok" },
      ],
      { y: 12, rowH: 38 },
    );
  },
};
