// Chapter 5.5: replace a node: out of the members first, then the disk, then back in.
export default {
  id: "replace-node",
  height: 230,
  alt: "Replacing node 3 in four steps. The voters start as 1, 2 and 3. Step 1 removes node 3: voters 1 and 2. Step 2 stops node 3 and moves its data away. Step 3 starts it empty with QUEEN_RAFT_JOIN: it answers 503 and waits to be added. Step 4 adds it as a learner, which copies the log but does not vote, and then promotes it: voters 1, 2 and 3 again, with lag 0.",
  draw(c) {
    const steps = [
      ["voters [1, 2, 3]", "node 3 is broken", "plain"],
      ["1. remove node 3", "voters [1, 2]", "accent"],
      ["2. stop, move data away", "node 3 empty", "plain"],
      ["3. start with JOIN", "503: waits to be added", "plain"],
      ["4. learner, then promote", "voters [1, 2, 3], lag 0", "ok"],
    ];
    steps.forEach(([a, b, tone], i) => {
      const x = 20 + i * 196;
      c.box(x, 60, 176, 74, a, { sub: b, tone, size: 14.5 });
      if (i < steps.length - 1) c.arrow([[x + 180, 97], [x + 192, 97]], { tone: "muted" });
    });
    c.text(500, 172, "Remove before you wipe: a node that comes back empty under its old number is never repaired.", { size: 14, anchor: "middle", tone: "danger", italic: true });
    c.text(500, 200, "A learner copies the log but does not vote: it counts in the majority only once caught up.", { size: 14, anchor: "middle", tone: "muted", italic: true });
  },
};
