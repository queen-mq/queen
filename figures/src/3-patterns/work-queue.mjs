// Chapter 3.1: competing consumers across partitions.
export default {
  id: "work-queue",
  height: 320,
  alt: "Twelve photos in the queue thumbnails, in four partitions, one per user. Three workers with no consumer group compete for them: each takes one user's partition at a time, so w1 holds user-0, w2 user-1, w3 user-2, and user-3 waits for the first worker that is free. Each photo is resized exactly once.",
  draw(c) {
    c.rect(20, 20, 470, 282, { tone: "plain", r: 12 });
    c.text(40, 46, "thumbnails", { size: 16, weight: 700, mono: true });
    const users = ["user-0", "user-1", "user-2", "user-3"];
    users.forEach((u, i) => {
      const y = 70 + i * 50;
      c.text(40, y + 18, u, { size: 14.5, mono: true, tone: "muted" });
      c.strip(130, y, [0, 1, 2].map((j) => ({ label: `p${i + 1 + j * 4}`, tone: i < 3 ? "accent" : "plain" })), { cellW: 84, cellH: 36, gap: 6, size: 13.5 });
    });
    const ws = ["w1", "w2", "w3"];
    ws.forEach((w, i) => {
      const y = 70 + i * 50;
      c.arrow([[400, y + 18], [740, y + 18 + (i - 1) * 12 + 12]], { tone: "accent" });
      c.box(750, 52 + i * 70, 200, 52, w, { sub: `holds ${users[i]}`, tone: "accent", size: 16 });
    });
    c.text(130, 282, "user-3 waits for the first free worker", { size: 14, tone: "muted", italic: true });
    c.text(850, 290, "no consumer group: each job once", { size: 14, anchor: "middle", tone: "muted" });
  },
};
