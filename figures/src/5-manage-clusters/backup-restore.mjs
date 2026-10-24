// Chapter 5.5: one copy of one node rebuilds a whole cluster.
export default {
  id: "backup-restore",
  height: 300,
  alt: "Backup: pause a follower, copy its data directory with tar, resume it; the other two nodes keep committing. Restore after every disk is lost: unpack the copy as node 1's directory, start node 1 alone with QUEEN_RAFT_FORCE_RECOVER=1, restart it without, then start nodes 2 and 3 empty, add them as learners and promote them. The cluster comes back at the moment of the copy: a message pushed after it is gone, consumers repeat what they acked after it.",
  draw(c) {
    c.text(20, 26, "backup", { size: 16, weight: 700 });
    const f = c.box(20, 46, 200, 64, "follower", { sub: "docker pause", tone: "accent" });
    const tar = c.file(290, 50, 210, 56, "queen-….tar", { sub: "tar of the data directory" });
    c.arrow([[f.x + f.w + 4, 78], [tar.x - 6, 78]], { tone: "ink", label: "copy" });
    c.text(540, 70, "then docker unpause:", { size: 14 });
    c.text(540, 92, "the other two nodes kept committing", { size: 14, tone: "muted" });
    c.text(20, 160, "restore, every disk lost", { size: 16, weight: 700 });
    const steps = [["node 1 = the copy", "FORCE_RECOVER=1", "accent"], ["restart node 1", "without it", "plain"], ["nodes 2, 3 empty", "JOIN, learners", "plain"], ["promote", "voters [1, 2, 3]", "ok"]];
    steps.forEach(([a, b, tone], i) => {
      const x = 20 + i * 245;
      c.box(x, 180, 220, 64, a, { sub: b, tone, size: 15 });
      if (i < 3) c.arrow([[x + 224, 212], [x + 241, 212]], { tone: "muted" });
    });
    c.text(500, 280, "Back at the moment of the copy: later writes are gone; consumers repeat what they acked after it.", { size: 13.5, anchor: "middle", tone: "muted", italic: true });
  },
};
