// Chapter 3.3: request and reply with a timeout fallback.
export default {
  id: "request-reply",
  height: 300,
  alt: "The booking page sends a quote request for the deluxe room to the ephemeral queue quotes, with a private inbox as replyTo, and waits at most 500 milliseconds. A pricing service instance answers price 129 into the inbox in time. If no instance answers within 500 milliseconds, the page shows ask us.",
  draw(c) {
    c.sequence(
      [{ id: "p", label: "booking page" }, { id: "q", label: "Queen, in memory", tone: "accent" }, { id: "s", label: "pricing service" }],
      [
        { from: "p", to: "q", label: "push quotes { room: 'deluxe', replyTo: inbox }" },
        { from: "s", to: "q", label: "pop quotes" },
        { from: "s", to: "q", label: "push inbox { price: 129 }" },
        { from: "q", to: "p", label: "answer in time: { price: 129 }", reply: true, tone: "ok" },
        { note: "no answer in 500 ms: price null, show \"ask us\"", over: "p", tone: "danger" },
      ],
      { y: 12, rowH: 44 },
    );
  },
};
