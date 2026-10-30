// Chapter 6.7: an entry that touches three queues is in each of their logs.
export default {
  id: "entry-copies",
  height: 290,
  alt: "A transaction touches the queues orders, shipments and reminders. Its log entry is written into each of their logs, beside the message records it adds there, and every log is synced. After a crash, recovery replays the entry only if every copy is present; the first entry with a copy missing, and everything after it, is cut from every log. That is how a transaction stays all or nothing on disk.",
  draw(c) {
    c.box(20, 100, 190, 80, "one entry", { sub: "touches 3 queues", tone: "accent" });
    const logs = ["orders", "shipments", "reminders"];
    logs.forEach((q, i) => {
      const y = 30 + i * 80;
      c.text(300, y + 24, `qlog/${q}`, { size: 14, mono: true, tone: "muted" });
      c.strip(470, y + 2, [{ label: "…", tone: "plain" }, { label: "msgs", tone: "plain" }, { label: "entry", tone: "accent" }], { cellW: 100, cellH: 44, gap: 6, mono: false, size: 13.5 });
      c.arrow([[214, 140], [296, y + 24]], { tone: "faint" });
    });
    c.rect(810, 40, 170, 200, { tone: "ok", r: 10 });
    c.text(895, 70, "after a crash", { size: 14.5, weight: 700, anchor: "middle" });
    ["replayed only", "if every copy", "is there;", "else cut from", "every log"].forEach((t, i) => c.text(895, 102 + i * 24, t, { size: 13.5, anchor: "middle" }));
    c.text(500, 278, "Each log is synced (fsync) before the entry goes to apply.", { size: 14, anchor: "middle", tone: "muted", italic: true });
  },
};
