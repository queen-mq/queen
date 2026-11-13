// Chapter 6.2: commands that share no state plan at the same time; the entry keeps their order.
export default {
  id: "planner-groups",
  height: 330,
  alt: "One planning cycle on the leader. Each command of the batch says what it reads and writes: a partition, a queue's settings, a KV key, a timer. Commands that share any of these form one group. Groups share nothing, so eight threads plan them at the same time, each group's commands in the order they came. A check confirms that no group touched what another one wrote, and the entry holds every command in batch order, exactly as one thread would have planned it. A command that changes a queue's settings or deletes something is planned on its own, between the groups.",
  draw(c) {
    c.box(20, 120, 140, 80, "batch", { sub: "≤ 4,096 commands", tone: "strong" });
    c.arrow([[164, 160], [214, 160]], { tone: "ink", width: 2 });
    c.box(220, 120, 160, 80, "footprints", { sub: "what each touches", tone: "plain", size: 16 });
    const groups = [
      "orders / p3",
      "orders / p8, kv order-8",
      "payments / p1",
      "kv lock-a",
      "timer reminder-42",
      "orders / p11",
    ];
    for (let i = 0; i < groups.length; i++) {
      const y = 22 + i * 46;
      c.box(440, y, 230, 34, groups[i], { size: 13.5, weight: 500, mono: false, tone: "accent" });
      c.arrow([[384, 160], [436, y + 17]], { tone: "faint", width: 1.2 });
      c.arrow([[674, y + 17], [726, 160]], { tone: "faint", width: 1.2 });
    }
    c.text(555, 316, "groups share no key: eight threads plan them at once", {
      size: 13.5,
      anchor: "middle",
      tone: "muted",
      italic: true,
    });
    c.box(732, 120, 120, 80, "check", { sub: "nothing shared", tone: "plain", size: 16 });
    c.arrow([[856, 160], [868, 160]], { tone: "ink", width: 2.2 });
    c.box(872, 120, 120, 80, "one entry", { sub: "batch order", tone: "ok" });
  },
};
