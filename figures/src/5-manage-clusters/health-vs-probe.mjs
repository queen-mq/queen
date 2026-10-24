// Chapter 5.4: /health trails a lost majority; a write probe does not.
export default {
  id: "health-vs-probe",
  height: 260,
  alt: "Two nodes of three stop at time zero. The third node's /health keeps answering 200 for about five seconds while quorumAckMs grows, and then answers 503 settling. A write probe sent through that node never commits from the first second: it waits until its client gives up. So alert on the write probe, and use /health for readiness.",
  draw(c) {
    const x0 = 180, x1 = 960;
    const T = (s) => x0 + (x1 - x0) * (s / 10);
    c.text(20, 50, "/health", { size: 15, mono: true, weight: 600 });
    c.rect(T(0), 36, T(5) - T(0), 30, { tone: "ok", r: 5 });
    c.text((T(0) + T(5)) / 2, 51, "200 healthy, quorumAckMs growing", { size: 13.5, anchor: "middle" });
    c.rect(T(5), 36, T(10) - T(5), 30, { tone: "danger", r: 5 });
    c.text((T(5) + T(10)) / 2, 51, "503 settling", { size: 13.5, anchor: "middle", mono: true });
    c.text(20, 120, "write probe", { size: 15, weight: 600 });
    c.rect(T(0), 106, T(10) - T(0), 30, { tone: "danger", r: 5 });
    c.text((T(0) + T(10)) / 2, 121, "no write commits: the probe waits until the client gives up", { size: 13.5, anchor: "middle" });
    c.line(T(0), 20, T(0), 170, { tone: "danger", dash: "4 4" });
    c.text(T(0) + 6, 176, "2 of 3 nodes stop", { size: 13, tone: "danger" });
    c.axis(x0, x1, 208, [0, 2, 4, 6, 8, 10].map((s) => ({ at: T(s), label: `${s} s` })), {});
    c.text(500, 250, "Alert on the write probe; use /health for readiness, TCP for liveness.", { size: 14, anchor: "middle", tone: "muted", italic: true });
  },
};
