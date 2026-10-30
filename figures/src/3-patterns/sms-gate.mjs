// Chapter 3.7: the token bucket gate over time, on a run where each refill lands on the second.
export default {
  id: "sms-gate",
  height: 340,
  alt: "Twelve SMS for customer-42 pushed at once. The gate starts with 5 tokens. At 0 seconds messages 1 to 5 pass; message 6 is refused, which stops the partition: 6 to 12 wait for the lease, 1 second. At 1 second the bucket has refilled 5 tokens (4.99 if the lease ends a few milliseconds early, and then only 4 pass) and messages 6 to 10 pass; 11 is refused. At 2 seconds 11 and 12 pass. All twelve go out, in order, 5 a second.",
  draw(c) {
    const x0 = 150, x1 = 960;
    const T = (s) => x0 + (x1 - x0) * (s / 2.6);
    const groups = [[0, [1, 2, 3, 4, 5]], [1, [6, 7, 8, 9, 10]], [2, [11, 12]]];
    c.text(20, 60, "sent", { size: 15, weight: 600 });
    groups.forEach(([t, ns]) => {
      ns.forEach((n, j) => c.box(T(t) + j * 50, 36, 46, 46, String(n), { tone: "ok", mono: true, size: 14, weight: 600 }));
    });
    c.text(20, 140, "tokens", { size: 15, weight: 600 });
    [[0, "5 → 0"], [1, "refilled 5 → 0"], [2, "refilled 5 → 3"]].forEach(([t, s]) => c.tag(T(t), 140, s, { tone: "accent", size: 13.5 }));
    c.text(20, 212, "refused", { size: 15, weight: 600 });
    [[0, "#6 refused: 6–12 wait for the lease (1 s)"], [1, "#11 refused: 11–12 wait"]].forEach(([t, s]) => {
      c.text(T(t), 212, s, { size: 13.5, tone: "danger" });
    });
    c.axis(x0, x1, 270, [0, 1, 2].map((s) => ({ at: T(s), label: `${s} s` })), {});
    c.text(20, 326, "The most a gate lets through: min(refillPerSec, capacity ÷ lease in seconds).", { size: 14, tone: "muted", italic: true });
  },
};
