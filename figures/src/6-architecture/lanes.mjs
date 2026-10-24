// Chapter 6.2: eight lanes plan in parallel; one step plans what crosses lanes.
export default {
  id: "lanes",
  height: 330,
  alt: "One planning cycle on the leader. The commands of the batch are sorted by partition into eight lanes: a lane owns the partitions whose id modulo 8 is its number and plans the commands that touch only its own partitions, all eight at the same time. Then one control step plans everything else with the full view: new partitions, KV, timers, queue changes and commands that cross lanes. The result is still one entry, applied as if one thread had planned it.",
  draw(c) {
    c.box(20, 120, 150, 80, "batch", { sub: "≤ 4,096 commands", tone: "strong" });
    for (let i = 0; i < 8; i++) {
      const y = 22 + i * 36;
      c.box(260, y, 220, 28, `lane ${i}: pid % 8 = ${i}`, { size: 13.5, weight: 500, mono: false, tone: "accent" });
      c.arrow([[174, 160], [256, y + 14]], { tone: "faint", width: 1.2 });
      c.arrow([[484, y + 14], [566, 160]], { tone: "faint", width: 1.2 });
    }
    c.box(572, 110, 190, 100, "control step", { sub: "everything else", tone: "plain", size: 16 });
    c.text(667, 232, "new partitions, KV, timers,", { size: 13, anchor: "middle", tone: "muted" });
    c.text(667, 250, "commands across lanes", { size: 13, anchor: "middle", tone: "muted" });
    c.arrow([[766, 160], [826, 160]], { tone: "ink", width: 2.2 });
    c.box(832, 120, 148, 80, "one entry", { sub: "one log write", tone: "ok" });
    c.text(370, 316, "in parallel", { size: 13.5, anchor: "middle", tone: "muted", italic: true });
  },
};
