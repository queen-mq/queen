// Chapter 2.4: queue mode shares the messages; consumer groups each get all of them.
export default {
  id: "queue-mode-vs-groups",
  height: 380,
  alt: "Left, queue mode with no consumer group: the partitions of orders are shared out, and each message goes to exactly one of the workers w1, w2 and w3. Right, three consumer groups, billing, emails and analytics, on the same queue: each group gets every message and keeps its own position; inside billing, two workers split the partitions between them.",
  draw(c) {
    const parts = ["customer-42", "customer-7", "customer-13"];
    // Left
    c.text(20, 26, "No group: each message to one worker", { size: 17, weight: 600 });
    parts.forEach((p, i) => {
      const y = 70 + i * 92;
      c.text(20, y + 20, p, { size: 14, mono: true, tone: "muted" });
      c.strip(130, y, [{}, {}, {}], { cellW: 30, cellH: 40, gap: 4 });
      const w = c.box(330, y + 2, 80, 36, `w${i + 1}`, { size: 15 });
      c.arrow([[236, y + 20], [w.x - 6, y + 20]], { tone: "ink" });
    });
    c.text(20, 350, "Workers compete. A partition has one holder at a time.", { size: 14, tone: "muted", italic: true });
    c.line(470, 20, 470, 360, { tone: "grid", width: 1.4 });
    // Right
    c.text(500, 26, "Groups: each group gets every message", { size: 17, weight: 600 });
    c.rect(500, 50, 150, 290, { tone: "plain", r: 10 });
    c.text(575, 74, "orders", { size: 15, mono: true, weight: 700, anchor: "middle" });
    parts.forEach((p, i) => c.strip(520, 96 + i * 76, [{}, {}, {}], { cellW: 30, cellH: 34, gap: 4 }));
    const groups = [["billing", "w1, w2 share it", "accent"], ["emails", "w3", "plain"], ["analytics", "w4", "plain"]];
    groups.forEach(([g, who, tone], i) => {
      const y = 62 + i * 96;
      const b = c.box(800, y, 180, 70, g, { sub: who, tone });
      c.arrow([[656, 195], [b.x - 6, b.cy]], { tone: "muted" });
      c.text(990, y + 84, "", {});
    });
    c.text(740, 350, "own position, own pace", { size: 14, anchor: "middle", tone: "muted", italic: true });
  },
};
