// Chapter 6.2: the path of a write: admission, batch, plan, apply.
export default {
  id: "write-path",
  height: 300,
  alt: "The four steps of a write. Admission, on every node: a memory budget of 128 MiB; a push takes its share before its body is read, waits when the budget is full and gets 429 after about 15 seconds. Batch, on the leader: up to 4,096 commands or 4 MiB per entry, up to 8 entries in flight. Plan: eight lanes plan in parallel and decide ids, offsets and times once. Raft: the entry is committed when a majority has it on disk. Apply, on every node: in order, on four threads, with no clock and no random number, so every node ends with the same bytes.",
  draw(c) {
    const steps = [
      ["Admission", "every node", ["128 MiB budget", "waits, 429 after ~15 s", "pops and acks skip it"], "plain"],
      ["Batch", "leader", ["≤ 4,096 commands", "or 4 MiB per entry", "≤ 8 entries in flight"], "plain"],
      ["Plan", "leader", ["8 lanes in parallel", "ids, offsets, times", "decided once"], "accent"],
      ["Raft", "majority", ["entry on a majority", "of disks: committed"], "plain"],
      ["Apply", "every node", ["in order, 4 threads", "no clock, no random", "same bytes everywhere"], "ok"],
    ];
    steps.forEach(([name, where, lines, tone], i) => {
      const x = 20 + i * 196;
      c.rect(x, 40, 176, 210, { tone, r: 12 });
      c.text(x + 88, 70, name, { size: 17, weight: 700, anchor: "middle" });
      c.text(x + 88, 94, where, { size: 13.5, anchor: "middle", tone: "muted" });
      lines.forEach((l, j) => c.text(x + 88, 134 + j * 30, l, { size: 13.5, anchor: "middle" }));
      if (i < steps.length - 1) c.arrow([[x + 179, 145], [x + 193, 145]], { tone: "ink", width: 2 });
    });
    c.text(500, 282, "A write is answered after Apply on the node that took it.", { size: 14, anchor: "middle", tone: "muted", italic: true });
  },
};
