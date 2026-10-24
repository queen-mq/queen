// Chapter 2.3: client-side buffering turns many small pushes into a few requests.
export default {
  id: "push-buffer",
  height: 230,
  alt: "A producer calls push a thousand times, one small message each. The client collects them in a buffer per queue and partition, and sends a batch as one HTTP request when it holds 100 messages or when a second has passed, whichever comes first. flushAllBuffers sends what is left before the process exits.",
  draw(c) {
    c.box(20, 70, 170, 70, "producer", { sub: "1,000 × push()", tone: "strong" });
    for (let i = 0; i < 7; i++) c.arrow([[196, 82 + i * 8], [300, 82 + i * 8]], { tone: "faint", width: 1.2 });
    c.rect(310, 40, 300, 130, { tone: "accent", r: 12 });
    c.text(460, 64, "client buffer", { size: 16, weight: 700, anchor: "middle" });
    c.strip(330, 86, Array.from({ length: 9 }, () => ({ label: "" })), { cellW: 26, cellH: 30, gap: 4 });
    c.text(460, 146, "sent at 100 messages or after 1 s", { size: 14, anchor: "middle", tone: "muted" });
    c.arrow([[616, 105], [780, 105]], { tone: "ink", width: 2.4, label: "one request,\n100 messages" });
    c.box(790, 70, 190, 70, "broker", { tone: "strong" });
    c.text(500, 205, "flushAllBuffers() before exit: a crash loses what is still in the buffer", { size: 14.5, anchor: "middle", tone: "danger", italic: true });
  },
};
