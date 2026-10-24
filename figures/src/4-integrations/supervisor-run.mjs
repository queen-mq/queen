// Chapter 4.4: the supervisor following a burst, as measured on 2026-10-24 (PHP engine;
// the Rust engine gave the same curve within a second). 300 jobs of 200 ms, one pool of
// 1 to 8 workers, target_jobs_per_process 10, fast_scale_up. Samples every second from
// `queen:supervisor status --json`; t = 0 is the end of the dispatch.
const WAITING = [[3, 290], [6, 223], [9, 127], [12, 16], [15, 0]];
const WORKERS = [[-2, 1], [3, 5], [6, 7], [9, 8], [28, 7], [31, 6], [33, 5], [37, 4], [40, 3], [42, 2], [46, 1], [50, 1]];

export default {
  id: "supervisor-run",
  height: 430,
  alt: "Measured: 300 jobs of 200 ms dispatched at time 0 onto a pool of 1 to 8 workers. The supervisor saw 290 waiting jobs 3 seconds later and grew the pool from 1 worker to 5, then 7 at 6 seconds and 8 at 9 seconds. The queue was empty at 15 seconds. The pool kept 8 workers for about 13 more seconds (scale_down_delay, 10 s, plus a cooldown), then drained them one at a time, back to 1 worker at 46 seconds.",
  draw(c) {
    const x0 = 110, x1 = 960;
    const T = (s) => x0 + ((x1 - x0) * (s + 2)) / 52;

    // Top: waiting jobs.
    const yTop = 60, yBase1 = 170;
    const Y1 = (n) => yBase1 - ((yBase1 - yTop) * n) / 300;
    c.text(20, 80, "waiting", { size: 15, weight: 600 });
    c.text(20, 100, "jobs", { size: 15, weight: 600 });
    c.line(x0, yBase1, x1, yBase1, { tone: "grid", width: 1.2 });
    for (const [s, n] of WAITING) {
      const h = yBase1 - Y1(n);
      if (h > 0) c.rect(T(s) - 14, Y1(n), 28, h, { tone: "info", r: 3 });
      c.text(T(s), Y1(n) - 8, String(n), { size: 13, anchor: "middle", tone: "muted", mono: true });
    }
    c.text(T(15) + 14, yBase1 - 30, "queue empty", { size: 13.5, tone: "ok", italic: true });

    // Bottom: workers, a step line.
    const yHigh = 230, yBase2 = 350;
    const Y2 = (n) => yBase2 - ((yBase2 - yHigh) * n) / 8;
    c.text(20, 250, "workers", { size: 15, weight: 600 });
    c.line(x0, yBase2, x1, yBase2, { tone: "grid", width: 1.2 });
    for (const n of [1, 8]) c.text(x0 - 12, Y2(n) + 5, String(n), { size: 13, anchor: "end", tone: "muted", mono: true });
    for (let i = 0; i < WORKERS.length - 1; i++) {
      const [s, n] = WORKERS[i];
      const [s2, n2] = WORKERS[i + 1];
      c.line(T(s), Y2(n), T(s2), Y2(n), { tone: "accent", width: 3 });
      c.line(T(s2), Y2(n), T(s2), Y2(n2), { tone: "accent", width: 3 });
    }

    // Time axis and the moments that matter.
    c.axis(x0, x1, 385, [0, 10, 20, 30, 40].map((s) => ({ at: T(s), label: `${s} s` })), { label: "" });
    c.line(T(0), 30, T(0), yBase2, { tone: "muted", width: 1.2, dash: true });
    c.text(T(0) + 8, 30, "300 jobs of 200 ms dispatched", { size: 13.5, tone: "muted" });
    c.text(T(5), Y2(8) - 14, "8 workers in 9 s", { size: 13.5, tone: "accent", weight: 600 });
    c.text((T(15) + T(28)) / 2, Y2(8) + 22, "scale_down_delay (10 s)", { size: 13, anchor: "middle", tone: "muted", italic: true });
    c.text(T(39), Y2(6), "then one worker at a time", { size: 13, tone: "muted", italic: true });
    c.text(20, 420, "Measured on QueenMQ 2.6.0, PHP engine; the Rust engine gave the same curve within a second.", { size: 13, tone: "muted", italic: true });
  },
};
