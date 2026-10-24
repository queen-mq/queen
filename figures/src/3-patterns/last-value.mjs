// Chapter 3.8: conflation delivers only the newest value of each partition.
export default {
  id: "last-value",
  height: 220,
  alt: "Three availability updates for hotel-7: 12 rooms, 11 rooms, 9 rooms. The group screen, with conflation, gets only the newest, 9 rooms, and its cursor moves past the other two. Another group without conflation still gets all three.",
  draw(c) {
    c.text(20, 56, "hotel-7", { size: 15, mono: true, weight: 600 });
    const s = c.strip(120, 34, [{ label: "12 rooms", tone: "muted" }, { label: "11 rooms", tone: "muted" }, { label: "9 rooms", tone: "accent" }], { cellW: 130, cellH: 44, gap: 10, size: 14.5 });
    c.arrow([[s.end + 6, 56], [640, 56]], { tone: "accent", width: 2.2, label: "conflation()" });
    c.box(650, 30, 330, 52, "group screen", { sub: "gets { rooms: 9 }, skips the rest", tone: "accent", size: 16 });
    c.text(120, 112, "skipped for this group", { size: 13.5, tone: "muted", italic: true });
    c.arrow([[s.end + 6, 160], [640, 160]], { tone: "muted" });
    c.text(120, 160, "the same messages, for every other group", { size: 14, tone: "muted" });
    c.box(650, 134, 330, 52, "group audit", { sub: "gets 12, 11 and 9", size: 16 });
  },
};
