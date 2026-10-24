// Chapter 6.7: reclaim: delete dead files, rewrite partly live ones.
export default {
  id: "file-rewrite",
  height: 310,
  alt: "Three kinds of log file. A closed file with no live record is deleted. A closed file that is partly live is rewritten: the node copies only the live records into a new file, without stopping the writes, and swaps it in. The active file is never touched; it is closed early, so retention can reach it, when its data is older than 10 minutes and it holds a dead message or is an eighth full. One pass looks at up to 256 files, for at most 200 milliseconds, and rewrites at most one file.",
  draw(c) {
    const rec = (x, y, pattern) => pattern.split("").forEach((p, i) => c.rect(x + 12 + i * 26, y + 40, 22, 22, { tone: p === "x" ? "muted" : "ok", r: 3 }));
    c.rect(20, 30, 300, 110, { tone: "plain", r: 10 });
    c.text(36, 54, "closed, all dead", { size: 14.5, weight: 600 });
    rec(20, 30, "xxxxxxxxxx");
    c.tag(170, 168, "deleted", { tone: "danger", anchor: "middle" });
    c.rect(350, 30, 300, 110, { tone: "plain", r: 10 });
    c.text(366, 54, "closed, partly live", { size: 14.5, weight: 600 });
    rec(350, 30, "xxoxxoooxx");
    c.arrow([[500, 146], [500, 186]], { tone: "ok" });
    c.rect(350, 190, 300, 80, { tone: "ok", r: 10 });
    c.text(366, 212, "rewritten: live records only", { size: 14, weight: 600 });
    rec(350, 180, "oooo");
    c.rect(680, 30, 300, 110, { tone: "accent", r: 10 });
    c.text(696, 54, "active file", { size: 14.5, weight: 600 });
    rec(680, 30, "oooooo");
    c.text(696, 166, "never touched; closed early", { size: 13.5 });
    c.text(696, 188, "when older than 10 min and it", { size: 13.5, tone: "muted" });
    c.text(696, 208, "holds a dead message or is 1/8 full", { size: 13.5, tone: "muted" });
    c.rect(350, 284, 16, 16, { tone: "ok", r: 3 }); c.text(372, 292, "live record", { size: 13, tone: "muted" });
    c.rect(480, 284, 16, 16, { tone: "muted", r: 3 }); c.text(502, 292, "dead record", { size: 13, tone: "muted" });
    c.text(20, 230, "one pass: ≤ 256 files,", { size: 13.5, tone: "muted" });
    c.text(20, 250, "≤ 200 ms, ≤ 1 rewrite;", { size: 13.5, tone: "muted" });
    c.text(20, 270, "4× bigger while work is left", { size: 13.5, tone: "muted" });
  },
};
