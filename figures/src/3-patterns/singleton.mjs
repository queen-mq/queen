// Chapter 3.11: one worker at a time, with a lock and a guard.
export default {
  id: "singleton",
  height: 260,
  alt: "Three replicas run the nightly scheduler at once. Worker a acquires the lock nightly-report and commits the report with guard(lock) and transactionId report-2026-10-24. Workers b and c wait up to a second for the lock, do not get it, and skip. One report is written.",
  draw(c) {
    const lock = c.box(400, 100, 200, 64, "lock nightly-report", { sub: "ttl 30 s", tone: "accent", size: 15.5 });
    [["worker a", "acquired: builds the report", "ok", 20], ["worker b", "not acquired: skips", "muted", 110], ["worker c", "not acquired: skips", "muted", 180]].forEach(([w, what, tone, y], i) => {
      const b = c.box(20, y, 260, 56, w, { sub: what, tone: i === 0 ? "ok" : "muted", size: 16 });
      c.arrow([[b.x + b.w + 4, b.cy], [lock.x - 6, lock.cy + (i - 1) * 14]], { tone: i === 0 ? "ok" : "faint", dash: i === 0 ? undefined : "5 5" });
    });
    c.arrow([[lock.x + lock.w + 4, lock.cy], [700, lock.cy]], { tone: "ok", label: "guard(lock)" });
    c.box(706, 92, 274, 80, "daily-reports", { sub: "one report: report-2026-10-24", mono: true, tone: "strong", size: 15.5 });
    c.text(843, 200, "a late worker's commit rolls back", { size: 13.5, anchor: "middle", tone: "muted", italic: true });
  },
};
