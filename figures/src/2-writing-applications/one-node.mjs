// Chapter 2.1: one process serves the API, the dashboard, health and metrics on one port.
export default {
  id: "one-node",
  height: 250,
  alt: "One Queen process in a container. On port 6632 it serves the HTTP API that the SDKs and curl use, the dashboard in the browser, /health and the Prometheus metrics. Its data lives in one directory, /var/lib/queen/raft, which a volume keeps.",
  draw(c) {
    const callers = [["your code", "an SDK, or curl"], ["a browser", "the dashboard"], ["monitoring", "/health, /metrics"]];
    callers.forEach(([a, b], i) => {
      const y = 22 + i * 72;
      const box = c.box(20, y, 190, 56, a, { sub: b, size: 16 });
      c.arrow([[box.x + box.w + 4, box.cy], [384, 125]], { tone: "muted" });
    });
    c.rect(390, 30, 330, 190, { tone: "strong", r: 14 });
    c.text(555, 62, "queen", { size: 20, weight: 700, anchor: "middle" });
    c.text(555, 88, "one process, one port: 6632", { size: 15, anchor: "middle", tone: "muted" });
    ["HTTP API", "dashboard", "health + metrics"].forEach((t, i) => c.box(410, 110 + i * 34, 290, 28, t, { size: 14.5, weight: 500, tone: i === 0 ? "accent" : "plain" }));
    const d = c.disk(784, 78, 206, 100, "/var/lib/queen/raft", { sub: "one directory: all its data", size: 13.5, mono: true });
    c.arrow([[724, 128], [d.x - 6, 128]], { tone: "muted", label: "volume" });
  },
};
