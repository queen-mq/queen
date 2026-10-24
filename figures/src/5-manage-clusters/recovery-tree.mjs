// Chapter 5.5: start with a write, then find your case.
export default {
  id: "recovery-tree",
  height: 400,
  alt: "Start with one KV write through two nodes, with a 10-second timeout. If it is answered, the cluster commits and one node is sick: a restart or an exit code 75 needs nothing; a damaged file, an exit 139, or an applied index stuck at zero means replace the node, removing it first; a full disk means grow it; a refused token means give it the same token. If there is no answer, the cluster does not commit: a majority down with disks intact, bring them back; every node stopped on the same entry, skip the entry; a majority of disks gone for good, force a recovery on the survivor; every disk gone, promote the standby or restore a backup.",
  draw(c) {
    const root = c.box(330, 16, 340, 60, "one KV write, two nodes, 10 s", { sub: "does the cluster commit?", tone: "strong", size: 16 });
    const yes = c.box(80, 120, 300, 50, "answered: one node is sick", { tone: "ok", size: 15.5 });
    const no = c.box(620, 120, 300, 50, "no answer: it does not commit", { tone: "danger", size: 15.5 });
    c.arrow([[root.x + 60, root.y + root.h + 4], [yes.cx, yes.y - 6]], { tone: "ok" });
    c.arrow([[root.x + root.w - 60, root.y + root.h + 4], [no.cx, no.y - 6]], { tone: "danger" });
    const left = [["restarted, or exit 75 once", "nothing", "plain"], ["damaged file, exit 139, applied stuck", "replace it (remove first)", "accent"], ["507, or no space left", "grow the disk", "plain"], ["refused this node's token", "same token, restart", "plain"]];
    const right = [["majority down, disks fine", "bring them back", "plain"], ["all stopped on one entry", "skip the entry", "accent"], ["majority of disks gone", "force-recover the survivor", "danger"], ["every disk gone", "standby, or restore a backup", "danger"]];
    left.forEach(([see, doit, tone], i) => {
      const y = 196 + i * 50;
      c.text(30, y + 16, see, { size: 13.5, tone: "muted" });
      c.tag(450, y + 16, doit, { tone, anchor: "end", size: 13 });
    });
    right.forEach(([see, doit, tone], i) => {
      const y = 196 + i * 50;
      c.text(520, y + 16, see, { size: 13.5, tone: "muted" });
      c.tag(980, y + 16, doit, { tone, anchor: "end", size: 13 });
    });
    c.line(500, 130, 500, 390, { tone: "grid" });
  },
};
