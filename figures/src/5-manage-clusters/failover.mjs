// Chapter 5.1: a planned stop versus a crash of the leader.
export default {
  id: "failover",
  height: 260,
  alt: "Two timelines of the leader stopping. SIGTERM: the leader hands leadership to the most up-to-date follower before it exits; in our run the hand-off took 28 milliseconds and clients saw no gap. A crash, kill -9 or a lost machine: the followers wait out the election timeout, one to two seconds, and elect a new leader, at most about four seconds in our tests. Writes in that window wait and are answered once the new leader leads.",
  draw(c) {
    const x0 = 200, x1 = 960;
    const T = (s) => x0 + (x1 - x0) * (s / 5);
    c.text(20, 60, "SIGTERM", { size: 16, weight: 700 });
    c.text(20, 82, "planned stop", { size: 13.5, tone: "muted" });
    c.rect(T(0), 50, T(0.12) - T(0), 30, { tone: "ok", r: 4 });
    c.text(T(0.12) + 10, 65, "hand-off: 28 ms, no gap for clients", { size: 14.5, tone: "ok" });
    c.text(20, 150, "crash", { size: 16, weight: 700 });
    c.text(20, 172, "kill -9, lost host", { size: 13.5, tone: "muted" });
    c.rect(T(0), 140, T(2) - T(0), 30, { tone: "danger", r: 4 });
    c.text((T(0) + T(2)) / 2, 155, "election timeout: 1–2 s", { size: 14, anchor: "middle", weight: 500 });
    c.rect(T(2), 140, T(4) - T(2), 30, { tone: "muted", r: 4 });
    c.text((T(2) + T(4)) / 2, 155, "vote, new leader", { size: 14, anchor: "middle", weight: 500 });
    c.text(T(4) + 10, 155, "≤ 4 s in tests", { size: 14, tone: "muted" });
    c.axis(x0, x1, 214, [0, 1, 2, 3, 4, 5].map((s) => ({ at: T(s), label: `${s} s` })), {});
  },
};
