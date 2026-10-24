// Chapter 5.4: one node at a time, and when the data format moves.
export default {
  id: "rolling-upgrade",
  height: 340,
  alt: "Three swimlanes, one per node, time across. Node 1 stops, starts on the new release, catches up and turns healthy; only then node 2 does the same, then node 3. A strip below counts the nodes up: 3, 2, 3, 2, 3, 2, 3, never below the majority of 2. The cluster keeps writing the old data format for the whole roll, so the way back stays open; the format moves within a second after the last node runs the new release, unless QUEEN_RAFT_CLUSTER_VERSION_MS is 0, and from then on an older release refuses to start.",
  draw(c) {
    const x0 = 140, x1 = 960;
    const seg = (x1 - x0) / 7;
    const lanes = ["node 1", "node 2", "node 3"];
    lanes.forEach((n, i) => {
      const y = 30 + i * 56;
      c.text(20, y + 18, n, { size: 15, weight: 600 });
      c.rect(x0, y, seg * (1 + 2 * i), 36, { tone: "muted", r: 5 });
      c.text(x0 + 10, y + 18, "old", { size: 13, tone: "muted" });
      const sx = x0 + seg * (1 + 2 * i);
      c.rect(sx, y, seg, 36, { tone: "danger", r: 5 });
      c.text(sx + seg / 2, y + 18, "stop, start new", { size: 12.5, anchor: "middle" });
      c.rect(sx + seg, y, x1 - sx - seg, 36, { tone: "ok", r: 5 });
      c.text(sx + seg + 10, y + 18, i < 2 ? "new: /health 200, then the next node" : "new", { size: 12.5 });
    });
    c.text(20, 222, "nodes up", { size: 14, weight: 600 });
    [3, 2, 3, 2, 3, 2, 3].forEach((n, i) => c.box(x0 + i * seg + 4, 206, seg - 8, 32, String(n), { tone: n === 2 ? "accent" : "plain", mono: true, size: 15 }));
    c.text(x1, 254, "never below 2 of 3", { size: 13, anchor: "end", tone: "muted" });
    c.text(20, 290, "format", { size: 14, weight: 600 });
    c.rect(x0, 276, seg * 6, 28, { tone: "plain", r: 5 });
    c.text(x0 + 10, 290, "old format: going back = start the old release on the same data", { size: 13 });
    c.rect(x0 + seg * 6, 276, seg, 28, { tone: "accent", r: 5 });
    c.text(x0 + seg * 6.5, 290, "new", { size: 13, anchor: "middle", weight: 600 });
    c.text(x1, 324, "moves after the last node, unless QUEEN_RAFT_CLUSTER_VERSION_MS=0", { size: 13, anchor: "end", tone: "muted", italic: true });
  },
};
