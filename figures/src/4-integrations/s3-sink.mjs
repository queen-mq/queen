// Chapter 4.2: windows on the broker's clock, and the three-step commit.
export default {
  id: "s3-sink",
  height: 400,
  alt: "Top: the records of the queue orders on the broker's clock, cut into windows 1, 2 and 3. A window closes at TARGET_MB of records, at MAX_WINDOW_MS, or at the next hour. Bottom: the node that holds the queue's lease commits window 1 in three steps: it writes the window's end to KV, uploads the data file and its manifest to the bucket, and moves the commit pointer in KV. A node that dies between two steps leaves the next one the same window to finish: the same records, the same bytes, the same file name.",
  draw(c) {
    const x0 = 80, x1 = 960;
    c.text(20, 30, "orders", { size: 15, mono: true, weight: 600 });
    const xs = [100, 130, 150, 190, 240, 290, 350, 420, 445, 470, 520, 600, 640, 700, 760, 830, 880];
    xs.forEach((x) => c.circle(x + 40, 30, 5, { fill: c.p.ink, stroke: "none" }));
    [[110, 400, "window 1", "accent"], [404, 690, "window 2", "plain"], [694, 940, "window 3: filling", "ghost"]].forEach(([a, b, l, t]) => {
      c.rect(a, 50, b - a, 34, { tone: t, r: 6 });
      c.text((a + b) / 2, 67, l, { size: 14, anchor: "middle", weight: 600 });
    });
    c.text(500, 108, "a window closes at TARGET_MB (128), at MAX_WINDOW_MS (5 min), or at the next hour", { size: 13.5, anchor: "middle", tone: "muted" });
    c.axis(x0, x1, 128, [], { label: "the broker's clock" });
    // commit
    c.text(20, 176, "committing window 1 (the node that holds the queue's lease)", { size: 15, weight: 600 });
    const kv1 = c.box(20, 200, 280, 70, "1. its end, in KV", { sub: "s3:<sink>:orders:intent", subMono: true, tone: "accent", size: 15.5 });
    const up = c.box(360, 200, 280, 70, "2. upload to the bucket", { sub: "w-0000000001-….jsonl.zst + manifest", tone: "info", size: 15.5 });
    const kv2 = c.box(700, 200, 280, 70, "3. move the pointer, in KV", { sub: "only this makes the window real", tone: "ok", size: 15.5 });
    c.arrow([[kv1.x + kv1.w + 4, 235], [up.x - 6, 235]], { tone: "ink" });
    c.arrow([[up.x + up.w + 4, 235], [kv2.x - 6, 235]], { tone: "ink" });
    c.rect(20, 300, 960, 80, { tone: "muted", r: 10 });
    c.text(40, 326, "A crash anywhere: the next owner rebuilds window 1 from the same records,", { size: 14.5 });
    c.text(40, 350, "so it writes the same bytes under the same name. An overwrite, never a second copy.", { size: 14.5 });
  },
};
