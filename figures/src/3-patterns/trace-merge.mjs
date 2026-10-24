// Chapter 3.6: events of a trace merged by a session window.
export default {
  id: "trace-merge",
  height: 290,
  alt: "The SDK sends the events of trace-7f3a to the partition trace-7f3a: http 120 ms, db 45 ms, cache 3 ms, and a retried db event, which is a duplicate and stored once. A stream with a session window of 2 seconds closes the trace after 2 seconds of quiet, adds it up and pushes one message to the queue traces: 3 spans, 168 ms.",
  draw(c) {
    c.text(20, 26, "partition trace-7f3a", { size: 15, mono: true, weight: 600 });
    const ev = [["http", "120 ms", "plain"], ["db", "45 ms", "plain"], ["cache", "3 ms", "plain"], ["db", "retry: duplicate", "muted"]];
    ev.forEach(([n, ms, tone], i) => c.box(20 + i * 128, 46, 118, 56, n, { sub: ms, tone, mono: true, size: 15 }));
    c.text(20 + 3 * 128 + 59, 124, "stored once", { size: 13, anchor: "middle", tone: "muted", italic: true });
    c.arrow([[540, 74], [600, 74]], { tone: "ink" });
    c.box(606, 40, 200, 70, "session window", { sub: "gap: 2 s of quiet", tone: "accent" });
    c.arrow([[812, 74], [858, 74]], { tone: "accent" });
    c.box(864, 46, 116, 56, "traces", { mono: true, size: 15, tone: "strong" });
    c.rect(560, 150, 420, 110, { tone: "ok", r: 12 });
    c.text(580, 176, "one message per finished trace", { size: 15, weight: 700 });
    c.text(580, 210, "{ spans: 3, ms: 168 }", { size: 16, mono: true });
    c.text(580, 240, "2 s after the last event", { size: 14, tone: "muted" });
    c.text(20, 180, "The open traces live in the broker:", { size: 14.5, tone: "muted" });
    c.text(20, 202, "a restart of the merger loses nothing.", { size: 14.5, tone: "muted" });
    c.text(20, 236, "Empty trace partitions: Many short-lived", { size: 14.5, tone: "muted" });
    c.text(20, 258, "partitions (chapter 5). Archive: S3 sink.", { size: 14.5, tone: "muted" });
  },
};
