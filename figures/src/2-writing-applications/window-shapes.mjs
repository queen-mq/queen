// Chapter 2.10: the four kinds of window, on one time axis.
export default {
  id: "window-shapes",
  height: 410,
  alt: "One partition's events as dots on a time axis: a burst, a quiet stretch, another burst. Tumbling windows of a fixed size sit side by side. Sliding windows overlap, so one event counts in several of them. Session windows follow the bursts: a window ends after a gap of quiet. Cron windows are aligned to the clock, one per hour.",
  draw(c) {
    const x0 = 160, x1 = 970;
    const events = [190, 215, 240, 262, 300, 330, 560, 585, 610, 650, 690, 870, 900];
    const T = (f) => x0 + (x1 - x0) * f;
    c.text(20, 40, "events", { size: 15, weight: 600 });
    c.line(x0, 40, x1, 40, { tone: "grid" });
    events.forEach((x) => c.circle(x, 40, 6, { fill: c.p.ink, stroke: "none" }));
    const row = (y, name, sub) => { c.text(20, y + 12, name, { size: 15, weight: 600 }); c.text(20, y + 32, sub, { size: 12.5, tone: "muted", mono: true }); };
    // Tumbling
    row(80, "tumbling", "seconds: 60");
    for (let i = 0; i < 6; i++) c.rect(T(i / 6) + 2, 84, (x1 - x0) / 6 - 4, 36, { tone: "plain", r: 6 });
    // Sliding
    row(150, "sliding", "size 600, slide 60");
    for (let i = 0; i < 9; i++) c.rect(T(i / 12), 150 + (i % 3) * 14, (x1 - x0) * 0.25, 12, { tone: i === 3 ? "accent" : "muted", r: 4 });
    c.text(T(0.25) + 4, 210, "one event counts in several windows", { size: 12.5, tone: "muted", italic: true });
    // Session
    row(230, "session", "gap: 1800");
    [[180, 345], [550, 705], [860, 915]].forEach(([a, b]) => c.rect(a, 234, b - a, 36, { tone: "accent", r: 6 }));
    c.arrow([[350, 252], [544, 252]], { tone: "muted", both: true, label: "a gap of quiet ends it" });
    // Cron
    row(300, "cron", "every: 'hour'");
    ["10:00", "11:00", "12:00"].forEach((t, i) => {
      c.rect(T(i / 3) + 2, 304, (x1 - x0) / 3 - 4, 36, { tone: "plain", r: 6 });
      c.text(T(i / 3) + 8, 358, t, { size: 12.5, tone: "muted", mono: true });
    });
    c.axis(x0, x1, 385, [], { label: "time" });
  },
};
