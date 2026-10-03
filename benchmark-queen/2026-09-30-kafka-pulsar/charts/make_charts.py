#!/usr/bin/env python3
"""make_charts.py — Kafka charts of 2026-09-30 as standalone SVGs (light + dark via prefers-color-scheme).

  kafka-rate.svg                 series A: 1 topic x 200 partitions, offered rate 300k -> 4M msg/s
  kafka-vs-queen-partitions.svg  series B/C/D against Queen's 09-29 grid: e2e p99 and broker cores by partitions/keys

Kafka numbers come from runs/kafka/<tag>/ through report/report.py --csv (re-run after new points).
Queen numbers are the 09-29 grid (build p34, pre-fix), copied below from runs/map-yq.json + the .out cpu lines of the
e6e72943 session scratchpad (temporary, hence copied): (queues, total partitions, in k, out k, e2e p50 ms, e2e p99 ms,
cores = Queen processes of the 3 nodes, 3-s top sample mid-run).
Palette: dataviz reference slots 1 (blue = Kafka) and 2 (orange = Queen), validated light + dark.
"""
import csv, io, math, os, subprocess, sys

H = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(H, "charts")

QUEEN = [  # 09-29 grid, 1M msg/s offered, 60 s, last 30 s
    (1, 10_000, 999, 996, 138, 522, 22.1), (1, 100_000, 1000, 998, 111, 362, 22.3),
    (1, 1_000_000, 1001, 1009, 224, 848, 22.7), (1, 10_000_000, 913, 905, 3293, 4686, 22.9),
    (10, 10_000, 1000, 997, 134, 465, 21.7), (100, 10_000, 999, 999, 289, 643, 22.4),
    (10, 100_000, 999, 994, 125, 379, 22.5), (100, 100_000, 998, 990, 395, 1237, 23.0),
    (1000, 100_000, 759, 761, 7504, 8225, 23.0),
    (10, 1_000_000, 973, 963, 889, 1729, 22.8), (100, 1_000_000, 868, 839, 8716, 16908, 22.8),
    (1000, 1_000_000, 632, 646, 8716, 12780, 21.2),
]


def kafka_rows():
    runs = os.path.join(H, "runs", "kafka")
    dirs = [os.path.join(runs, d) for d in sorted(os.listdir(runs)) if os.path.exists(os.path.join(runs, d, "DONE"))]
    out = subprocess.run([sys.executable, os.path.join(H, "report", "report.py"), "--csv", *dirs],
                         capture_output=True, text=True, check=True).stdout
    rows = {}
    for r in csv.DictReader(io.StringIO(out)):
        k = lambda s: float(s.rstrip("k")) * 1000 if s.endswith("k") else float(s)
        rows[r["run"]] = dict(offered=float(r["offered"]), push=k(r["push/s"]), cons=k(r["cons/s"]),
                              e50=float(r["e2e p50"]), e99=float(r["e2e p99"]), p99=float(r["prod p99"]),
                              cores=float(r["sys cores"]))
    return rows


CSS = """
<style>
  svg { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }
  .bg { fill: #fcfcfb; } .ink { fill: #0b0b0b; } .ink2 { fill: #52514e; } .muted { fill: #898781; }
  .grid { stroke: #e1e0d9; stroke-width: 1; } .axis { stroke: #c3c2b7; stroke-width: 1; }
  .ref { stroke: #898781; stroke-width: 1; stroke-dasharray: 3 4; }
  .k { stroke: #2a78d6; } .kf { fill: #2a78d6; } .q { stroke: #eb6834; } .qf { fill: #eb6834; }
  .ring { stroke: #fcfcfb; } .hollow { fill: #fcfcfb; }
  .line { fill: none; stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; }
  .dash { stroke-dasharray: 5 4; }
  text { font-size: 12px; } .t { font-size: 15px; font-weight: 600; } .st { font-size: 12px; }
  .lab { font-size: 12px; font-weight: 600; } .small { font-size: 11px; }
  @media (prefers-color-scheme: dark) {
    .bg { fill: #1a1a19; } .ink { fill: #ffffff; } .ink2 { fill: #c3c2b7; }
    .grid { stroke: #2c2c2a; } .axis { stroke: #383835; }
    .k { stroke: #3987e5; } .kf { fill: #3987e5; } .q { stroke: #d95926; } .qf { fill: #d95926; }
    .ring { stroke: #1a1a19; } .hollow { fill: #1a1a19; }
  }
</style>"""


class Panel:
    """one plot area: x/y scales (lin or log), grid, axes, marks"""

    def __init__(self, x0, y0, w, h, xr, yr, xlog=False, ylog=False):
        self.x0, self.y0, self.w, self.h, self.xr, self.yr, self.xlog, self.ylog = x0, y0, w, h, xr, yr, xlog, ylog
        self.o = []

    def sx(self, v):
        a, b = self.xr
        f = (math.log10(v) - math.log10(a)) / (math.log10(b) - math.log10(a)) if self.xlog else (v - a) / (b - a)
        return self.x0 + f * self.w

    def sy(self, v):
        a, b = self.yr
        v = max(v, a)
        f = (math.log10(v) - math.log10(a)) / (math.log10(b) - math.log10(a)) if self.ylog else (v - a) / (b - a)
        return self.y0 + self.h - f * self.h

    def grid(self, xt, yt, xlab, ylab, xname=None, yname=None):
        for v, s in zip(yt, ylab):
            y = self.sy(v)
            self.o.append(f'<line class="grid" x1="{self.x0}" x2="{self.x0 + self.w}" y1="{y:.1f}" y2="{y:.1f}"/>')
            self.o.append(f'<text class="muted" x="{self.x0 - 8}" y="{y + 4:.1f}" text-anchor="end">{s}</text>')
        self.o.append(f'<line class="axis" x1="{self.x0}" x2="{self.x0 + self.w}" y1="{self.y0 + self.h}" y2="{self.y0 + self.h}"/>')
        for v, s in zip(xt, xlab):
            x = self.sx(v)
            self.o.append(f'<line class="axis" x1="{x:.1f}" x2="{x:.1f}" y1="{self.y0 + self.h}" y2="{self.y0 + self.h + 4}"/>')
            self.o.append(f'<text class="muted" x="{x:.1f}" y="{self.y0 + self.h + 18}" text-anchor="middle">{s}</text>')
        if xname:
            self.o.append(f'<text class="ink2" x="{self.x0 + self.w / 2}" y="{self.y0 + self.h + 36}" text-anchor="middle">{xname}</text>')
        if yname:
            self.o.append(f'<text class="ink2" x="{self.x0}" y="{self.y0 - 12}">{yname}</text>')

    def line(self, pts, cls, dash=False):
        if len(pts) < 2: return
        d = " ".join(f"{'M' if i == 0 else 'L'}{self.sx(x):.1f},{self.sy(y):.1f}" for i, (x, y) in enumerate(pts))
        self.o.append(f'<path class="line {cls}{" dash" if dash else ""}" d="{d}"/>')

    def dot(self, x, y, fill_cls, tip, hollow=False, stroke_cls="", shape="circle"):
        cx, cy = self.sx(x), self.sy(y)
        if shape == "diamond":
            r = 6
            pts = f"{cx:.1f},{cy - r:.1f} {cx + r:.1f},{cy:.1f} {cx:.1f},{cy + r:.1f} {cx - r:.1f},{cy:.1f}"
            el = f'<polygon points="{pts}" class="{fill_cls} ring" stroke-width="2"><title>{tip}</title></polygon>'
        elif hollow:
            el = f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="4.5" class="hollow {stroke_cls}" stroke-width="2"><title>{tip}</title></circle>'
        else:
            el = f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="4.5" class="{fill_cls} ring" stroke-width="2"><title>{tip}</title></circle>'
        self.o.append(el)

    def text(self, x, y, s, cls="ink2 small", anchor="start", dx=0, dy=0):
        self.o.append(f'<text class="{cls}" x="{self.sx(x) + dx:.1f}" y="{self.sy(y) + dy:.1f}" text-anchor="{anchor}">{s}</text>')


def svg(w, h, title, parts):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" role="img" '
            f'aria-label="{title}"><title>{title}</title>{CSS}<rect class="bg" width="{w}" height="{h}" rx="8"/>'
            + "".join(parts) + "</svg>\n")


def ms(v):
    return f"{v / 1000:.1f} s" if v >= 1000 else f"{v:.0f} ms"


def rate_chart(K):
    pts = []
    for tag, r in K.items():
        if tag.startswith("1x200-") and tag.endswith("k") and tag[6:-1].isdigit():
            pts.append((int(tag[6:-1]) * 1000, r))
    pts.sort()
    W, Hh = 820, 420
    head = [f'<text class="ink t" x="24" y="32">Kafka 4.3.1 keeps up to 4M msg/s at 200 partitions</text>',
            f'<text class="ink2 st" x="24" y="52">3 brokers × 16 vCPU (RF 3, acks=all, idempotent, lz4) · 1 topic × 200 partitions · 256 B JSON in 100-message units</text>',
            f'<text class="ink2 st" x="24" y="70">every rate delivered in = out, 0 shed, 0 errors · 60 s windows, last 30 s · 2026-09-30</text>']
    L = Panel(70, 110, 310, 230, (0, 4_200_000), (0, 80))
    L.grid([1e6, 2e6, 3e6, 4e6], [0, 20, 40, 60, 80], ["1M", "2M", "3M", "4M"], ["0", "20", "40", "60", "80"],
           "offered msg/s", "end-to-end latency (ms)")
    L.grid([0], [], ["0"], [])
    L.o.append(f'<line class="ref" x1="{L.sx(1e6):.1f}" x2="{L.sx(1e6):.1f}" y1="{L.y0}" y2="{L.y0 + L.h}"/>')
    L.text(1e6, 80, "Queen's ceiling on this rig (≈1M)", "muted small", dx=5, dy=12)
    L.line([(x, r["e99"]) for x, r in pts], "k")
    L.line([(x, r["e50"]) for x, r in pts], "k", dash=True)
    for x, r in pts:
        L.dot(x, r["e99"], "kf", f"{x / 1e6:g}M msg/s: e2e p99 {ms(r['e99'])}, p50 {ms(r['e50'])}")
        L.dot(x, r["e50"], "kf", f"{x / 1e6:g}M msg/s: e2e p50 {ms(r['e50'])}")
    xl, rl = pts[-1]
    L.text(xl, rl["e99"], f"p99 {ms(rl['e99'])}", "ink lab", "start", dx=10, dy=4)
    L.text(xl, rl["e50"], f"p50 {ms(rl['e50'])}", "ink lab", "start", dx=10, dy=4)
    R = Panel(500, 110, 290, 230, (0, 4_200_000), (0, 48))
    R.grid([1e6, 2e6, 3e6, 4e6], [0, 16, 32, 48], ["1M", "2M", "3M", "4M"], ["0", "16", "32", "48"],
           "offered msg/s", "Kafka CPU, 3 brokers summed (cores of 48)")
    R.line([(x, r["cores"]) for x, r in pts], "k")
    for x, r in pts:
        R.dot(x, r["cores"], "kf", f"{x / 1e6:g}M msg/s: Kafka {r['cores']:.1f} cores (3 brokers)")
    R.text(xl, rl["cores"], f"{rl['cores']:.0f} cores", "ink lab", "end", dx=-4, dy=-14)
    foot = [f'<text class="muted small" x="24" y="{Hh - 22}">At 4M: 580 MB/s disk writes and 594 MB/s network per broker; the limit ahead is bandwidth, not CPU. '
            f'Dashed line = p50, solid = p99.</text>']
    open(os.path.join(OUT, "kafka-rate.svg"), "w").write(svg(W, Hh, "Kafka rate series", head + L.o + R.o + foot))


def partitions_chart(K):
    def kt(tag): return K.get(tag)
    k1 = [(p, kt(f"1x{p}-1000k")) for p in (200, 2000, 10000, 50000, 100000) if kt(f"1x{p}-1000k")]
    kmulti = [(10000, t, kt(f"{t}x{10000 // t}-1000k")) for t in (10, 100, 1000)] + \
             [(100000, t, kt(f"{t}x{100000 // t}-1000k")) for t in (10, 100, 1000)]
    kmulti = [x for x in kmulti if x[2]]
    keys = [(n, kt(f"1x1000-1000k-e{s}")) for n, s in ((1_000_000, "1m"), (10_000_000, "10m")) if kt(f"1x1000-1000k-e{s}")]
    q1 = [q for q in QUEEN if q[0] == 1]
    qm = [q for q in QUEEN if q[0] > 1]
    W, Hh = 900, 790
    head = [f'<text class="ink t" x="24" y="32">Kafka by partitions and keys, against Queen at 1M msg/s</text>',
            f'<text class="ink2 st" x="24" y="52">Kafka: 3 brokers × 16 vCPU, RF 3, acks=all, 8 replica fetchers · 1M msg/s offered, 256 B JSON in 100-message units, 60 s</text>',
            f'<text class="ink2 st" x="24" y="70">Queen: 09-29 grid (pre-fix build), other droplets with the same CPU mix · filled = 1 topic / queue, hollow = 10, 100 or 1000 of them</text>']
    xr = (100, 20_000_000)
    xt = [100, 1000, 10_000, 100_000, 1_000_000, 10_000_000]
    xl = ["100", "1k", "10k", "100k", "1M", "10M"]
    T = Panel(80, 150, 700, 330, xr, (5, 30_000), xlog=True, ylog=True)
    T.grid(xt, [10, 100, 1000, 10_000], xl, ["10 ms", "100 ms", "1 s", "10 s"], None, "end-to-end latency p99 (log)")
    T.line([(p, r["e99"]) for p, r in k1], "k")
    T.line([(q[1], q[5]) for q in q1], "q")
    T.line([(n, r["e99"]) for n, r in keys], "k", dash=True)
    for q in qm:
        T.dot(q[1], q[5], "", f"Queen {q[0]} queues × {q[1] // q[0]:,} = {q[1]:,} partitions: in {q[2]}k, e2e p99 {ms(q[5])}", hollow=True, stroke_cls="q")
    for p, t, r in kmulti:
        T.dot(p, r["e99"], "", f"Kafka {t} topics × {p // t:,} = {p:,} partitions: in {r['push'] / 1000:.0f}k, e2e p99 {ms(r['e99'])}", hollow=True, stroke_cls="k")
    for q in q1:
        T.dot(q[1], q[5], "qf", f"Queen 1 queue × {q[1]:,} partitions: in {q[2]}k, e2e p99 {ms(q[5])}")
    for p, r in k1:
        T.dot(p, r["e99"], "kf", f"Kafka 1 topic × {p:,} partitions: in {r['push'] / 1000:.0f}k, e2e p99 {ms(r['e99'])}")
    for n, r in keys:
        T.dot(n, r["e99"], "kf", f"Kafka 1 topic × 1,000 partitions, {n:,} keys: e2e p99 {ms(r['e99'])}", shape="diamond")
    if k1:
        p, r = k1[-1]
        T.text(p, r["e99"], "Kafka, partitions", "ink lab", "end", dx=-10, dy=-8)
    T.text(q1[0][1], q1[0][5], "Queen, partitions", "ink lab", "start", dx=10, dy=-10)
    if keys:
        n, r = keys[0]
        T.text(n, r["e99"], "Kafka, keys on 1,000 partitions", "ink lab", "middle", dx=60, dy=-14)
        T.text(n, r["e99"], "(~1k–10k keys share each partition)", "ink2 small", "middle", dx=60, dy=22)
    if kt("1x2000-1000k"):
        T.text(2000, kt("1x2000-1000k")["e99"], "one 10-s spike;", "muted small", "middle", dy=-26)
        T.text(2000, kt("1x2000-1000k")["e99"], "other windows ≤85 ms", "muted small", "middle", dy=-12)
    sat = [p for p, t, r in kmulti if p == 100_000]
    if sat:
        T.text(100_000, 12_000, "Kafka brokers CPU-bound", "muted small", "middle", dy=-6)
    B = Panel(80, 560, 700, 150, xr, (0, 48), xlog=True)
    B.grid(xt, [0, 16, 32, 48], xl, ["0", "16", "32", "48"], "partitions in total (Kafka diamonds: keys)",
           "broker CPU, 3 nodes summed (cores of 48)")
    B.line([(p, r["cores"]) for p, r in k1], "k")
    B.line([(q[1], q[6]) for q in q1], "q")
    B.line([(n, r["cores"]) for n, r in keys], "k", dash=True)
    for q in qm:
        B.dot(q[1], q[6], "", f"Queen {q[0]} queues, {q[1]:,} partitions: {q[6]} cores", hollow=True, stroke_cls="q")
    for p, t, r in kmulti:
        B.dot(p, r["cores"], "", f"Kafka {t} topics, {p:,} partitions: {r['cores']:.1f} cores", hollow=True, stroke_cls="k")
    for q in q1:
        B.dot(q[1], q[6], "qf", f"Queen 1 queue, {q[1]:,} partitions: {q[6]} cores")
    for p, r in k1:
        B.dot(p, r["cores"], "kf", f"Kafka 1 topic, {p:,} partitions: {r['cores']:.1f} cores")
    for n, r in keys:
        B.dot(n, r["cores"], "kf", f"Kafka keys ({n:,}): {r['cores']:.1f} cores", shape="diamond")
    LY = 104
    legend = [
        f'<circle cx="30" cy="{LY - 4}" r="4.5" class="kf ring" stroke-width="2"/><text class="ink2 small" x="40" y="{LY}">Kafka 1 topic</text>',
        f'<circle cx="140" cy="{LY - 4}" r="4.5" class="hollow k" stroke-width="2"/><text class="ink2 small" x="150" y="{LY}">Kafka 10/100/1000 topics</text>',
        f'<polygon points="300,{LY - 10} 306,{LY - 4} 300,{LY + 2} 294,{LY - 4}" class="kf"/><text class="ink2 small" x="312" y="{LY}">Kafka keys</text>',
        f'<circle cx="400" cy="{LY - 4}" r="4.5" class="qf ring" stroke-width="2"/><text class="ink2 small" x="410" y="{LY}">Queen 1 queue</text>',
        f'<circle cx="510" cy="{LY - 4}" r="4.5" class="hollow q" stroke-width="2"/><text class="ink2 small" x="520" y="{LY}">Queen 10/100/1000 queues</text>',
        f'<text class="muted small" x="24" y="{Hh - 34}">Kafka cores = load-window average of the JVMs; Queen cores = 3-s sample mid-run. Hover a mark for its numbers.</text>',
        f'<text class="muted small" x="24" y="{Hh - 16}">Kafka 1 topic × 100k partitions: re-run pending (the first try lost 2 of 9 loaders to OOM; 7 of 9 saw produce p99 3.6 s, e2e p99 8 s).</text>',
    ]
    open(os.path.join(OUT, "kafka-vs-queen-partitions.svg"), "w").write(
        svg(W, Hh, "Kafka vs Queen by partitions", head + T.o + B.o + legend))


# Queen FINAL build (09-30 evening, from Alice): last 30 s, dedup OFF, consumers take up to 10 partitions per pop,
# 4 loaders, 60 s per shape, same droplets. (queues, partitions total, offered, in, out, e2e p50 ms, p99 ms,
# push p99 ms, leader cores, shed)
QUEEN_FINAL = [
    (1, 200, 1_000_000, 1_000_000, 1_001_000, 55, 140, 40, 8.9, 0),
    (1, 200, 1_500_000, 1_489_000, 1_423_000, 1500, 4300, 416, 12.4, 0),
    (1, 200, 2_000_000, 1_390_000, 1_301_000, 8800, 10600, 5100, 12.7, 603_000),
    (1, 10_000, 1_000_000, 1_000_000, 1_000_000, 46, 122, 48, 9.4, 0),
    (1, 50_000, 1_000_000, 1_000_000, 999_000, 45, 129, 56, 9.5, 0),
    (1, 100_000, 1_000_000, 1_000_000, 1_001_000, 45, 129, 60, 9.4, 0),
    (1, 1_000_000, 1_000_000, 1_000_000, 1_000_000, 40, 108, 43, 9.3, 0),
    (1000, 1_000_000, 1_000_000, 1_000_000, 999_000, 88, 179, 80, 10.5, 0),
    (10000, 1_000_000, 1_000_000, 1_000_000, 1_013_000, 130, 305, 112, 11.2, 0),
]


def kafka_busiest(tags):
    """Kafka JVM cores of the busiest broker over the load window (report.py's node_stats), per run tag"""
    import importlib.util, re
    spec = importlib.util.spec_from_file_location("report", os.path.join(H, "report", "report.py"))
    R = importlib.util.module_from_spec(spec); spec.loader.exec_module(R)
    def secs(v, d):
        m = re.fullmatch(r'(\d+)(ms|s|m|h)?', (v or '').strip())
        if not m: return d
        n, u = int(m[1]), m[2] or 's'
        return n / 1000 if u == 'ms' else n * {'s': 1, 'm': 60, 'h': 3600}[u]
    out = {}
    for tag in tags:
        d = os.path.join(H, "runs", "kafka", tag)
        if not os.path.exists(os.path.join(d, "DONE")): continue
        env = R.run_env(d); t0 = int(env["GO_MS"]) / 1000
        win = (t0 + secs(env.get("RAMP"), 10), t0 + secs(env.get("DURATION"), 70))
        per = [st["kafka_cores"] for st in (R.node_stats(r, win) for r in R.samples(d).values()) if st and st.get("kafka_cores", 0) > 0.05]
        if per: out[tag] = max(per)
    return out


def final_rate_chart(K):
    kp = sorted((int(t[6:-1]) * 1000, r) for t, r in K.items() if t.startswith("1x200-") and t.endswith("k") and t[6:-1].isdigit())
    qp = [(q[2], q) for q in QUEEN_FINAL if q[1] == 200]
    W, Hh = 900, 440
    head = [f'<text class="ink t" x="24" y="32">At 200 partitions Kafka carries 4M msg/s; Queen tops out near 1.4M</text>',
            f'<text class="ink2 st" x="24" y="52">Same 3 brokers × 16 vCPU and loaders, 1 topic / queue × 200 partitions, 256 B JSON in 100-message units, 60 s, last 30 s</text>',
            f'<text class="ink2 st" x="24" y="70">Kafka 4.3.1: RF 3, acks=all, lz4, 8 replica fetchers · Queen final build: raft 3 nodes, dedup off, 4 loaders</text>']
    L = Panel(80, 120, 330, 240, (0, 4_200_000), (0, 4_200_000))
    L.grid([1e6, 2e6, 3e6, 4e6], [0, 1e6, 2e6, 3e6, 4e6], ["1M", "2M", "3M", "4M"], ["0", "1M", "2M", "3M", "4M"],
           "offered msg/s", "delivered (consumed) msg/s")
    L.o.append(f'<line class="ref" x1="{L.sx(0):.1f}" y1="{L.sy(0):.1f}" x2="{L.sx(4.2e6):.1f}" y2="{L.sy(4.2e6):.1f}"/>')
    L.text(3.3e6, 3.3e6, "delivered = offered", "muted small", "end", dx=-6, dy=-6)
    L.line([(x, r["cons"]) for x, r in kp], "k"); L.line([(x, q[4]) for x, q in qp], "q")
    for x, r in kp: L.dot(x, r["cons"], "kf", f"Kafka {x / 1e6:g}M offered: {r['cons'] / 1e6:.2f}M delivered")
    for x, q in qp: L.dot(x, q[4], "qf", f"Queen {x / 1e6:g}M offered: {q[4] / 1e6:.2f}M delivered{', ' + str(q[9] // 1000) + 'k shed' if q[9] else ''}")
    L.text(kp[-1][0], kp[-1][1]["cons"], "Kafka", "ink lab", "end", dx=-10, dy=-10)
    L.text(qp[-1][0], qp[-1][1][4], "Queen", "ink lab", "start", dx=10, dy=4)
    R_ = Panel(510, 120, 330, 240, (0, 4_200_000), (5, 20_000), ylog=True)
    R_.grid([1e6, 2e6, 3e6, 4e6], [10, 100, 1000, 10_000], ["1M", "2M", "3M", "4M"], ["10 ms", "100 ms", "1 s", "10 s"],
            "offered msg/s", "end-to-end latency p99 (log)")
    R_.line([(x, r["e99"]) for x, r in kp], "k"); R_.line([(x, q[6]) for x, q in qp], "q")
    for x, r in kp: R_.dot(x, r["e99"], "kf", f"Kafka {x / 1e6:g}M: e2e p50 {ms(r['e50'])}, p99 {ms(r['e99'])}")
    for x, q in qp: R_.dot(x, q[6], "qf", f"Queen {x / 1e6:g}M: e2e p50 {ms(q[5])}, p99 {ms(q[6])}")
    R_.text(kp[-1][0], kp[-1][1]["e99"], "Kafka", "ink lab", "end", dx=-10, dy=-10)
    R_.text(qp[-1][0], qp[-1][1][6], "Queen", "ink lab", "start", dx=10, dy=4)
    foot = [f'<text class="muted small" x="24" y="{Hh - 22}">Queen at 2M offered: 1.39M in, 603k/s shed by the loaders. Hover a mark for p50 and p99.</text>']
    open(os.path.join(OUT, "queen-vs-kafka-rate.svg"), "w").write(svg(W, Hh, "Queen vs Kafka, rate at 200 partitions", head + L.o + R_.o + foot))


def final_partitions_chart(K):
    kt = lambda t: K.get(t)
    k1 = [(p, kt(f"1x{p}-1000k"), f"1x{p}-1000k") for p in (200, 2000, 10000, 50000, 100000) if kt(f"1x{p}-1000k")]
    km = [(p, t, kt(f"{t}x{p // t}-1000k"), f"{t}x{p // t}-1000k") for p in (10000, 100000) for t in (10, 100, 1000) if kt(f"{t}x{p // t}-1000k")]
    keys = [(n, kt(f"1x1000-1000k-e{s}"), f"1x1000-1000k-e{s}") for n, s in ((1_000_000, "1m"), (10_000_000, "10m")) if kt(f"1x1000-1000k-e{s}")]
    busy = kafka_busiest([t for *_, t in k1] + [t for *_, t in km] + [t for *_, t in keys])
    q1 = [q for q in QUEEN_FINAL if q[0] == 1 and q[2] == 1_000_000]
    qm = [q for q in QUEEN_FINAL if q[0] > 1]
    W, Hh = 900, 800
    head = [f'<text class="ink t" x="24" y="32">At 1M msg/s Queen stays flat to 1M partitions; Kafka leads to ~10k, then falls off</text>',
            f'<text class="ink2 st" x="24" y="52">Same 3 brokers × 16 vCPU and loaders · 1M msg/s offered, 256 B JSON in 100-message units, 60 s, last 30 s</text>',
            f'<text class="ink2 st" x="24" y="70">Kafka 4.3.1: RF 3, acks=all, lz4, 8 replica fetchers · Queen final build: raft 3 nodes, dedup off, 4 loaders, up to 10 partitions per pop</text>']
    LY = 104
    legend = [
        f'<circle cx="30" cy="{LY - 4}" r="4.5" class="kf ring" stroke-width="2"/><text class="ink2 small" x="40" y="{LY}">Kafka 1 topic</text>',
        f'<circle cx="140" cy="{LY - 4}" r="4.5" class="hollow k" stroke-width="2"/><text class="ink2 small" x="150" y="{LY}">Kafka 10/100/1000 topics</text>',
        f'<polygon points="300,{LY - 10} 306,{LY - 4} 300,{LY + 2} 294,{LY - 4}" class="kf"/><text class="ink2 small" x="312" y="{LY}">Kafka keys on 1,000 partitions</text>',
        f'<circle cx="490" cy="{LY - 4}" r="4.5" class="qf ring" stroke-width="2"/><text class="ink2 small" x="500" y="{LY}">Queen 1 queue</text>',
        f'<circle cx="600" cy="{LY - 4}" r="4.5" class="hollow q" stroke-width="2"/><text class="ink2 small" x="610" y="{LY}">Queen 1,000 / 10,000 queues</text>']
    xr = (100, 20_000_000); xt = [100, 1000, 10_000, 100_000, 1_000_000, 10_000_000]; xl = ["100", "1k", "10k", "100k", "1M", "10M"]
    T = Panel(80, 150, 700, 330, xr, (5, 30_000), xlog=True, ylog=True)
    T.grid(xt, [10, 100, 1000, 10_000], xl, ["10 ms", "100 ms", "1 s", "10 s"], None, "end-to-end latency p99 (log)")
    T.line([(p, r["e99"]) for p, r, _ in k1], "k"); T.line([(q[1], q[6]) for q in q1], "q")
    T.line([(n, r["e99"]) for n, r, _ in keys], "k", dash=True)
    for p, t, r, _ in km: T.dot(p, r["e99"], "", f"Kafka {t} topics × {p // t:,}: in {r['push'] / 1000:.0f}k, e2e p99 {ms(r['e99'])}", hollow=True, stroke_cls="k")
    for q in qm: T.dot(q[1], q[6], "", f"Queen {q[0]:,} queues × {q[1] // q[0]:,}: in {q[3] // 1000}k, e2e p50 {ms(q[5])}, p99 {ms(q[6])}", hollow=True, stroke_cls="q")
    for p, r, _ in k1: T.dot(p, r["e99"], "kf", f"Kafka 1 topic × {p:,}: in {r['push'] / 1000:.0f}k, e2e p50 {ms(r['e50'])}, p99 {ms(r['e99'])}")
    for q in q1: T.dot(q[1], q[6], "qf", f"Queen 1 queue × {q[1]:,}: in {q[3] // 1000}k, e2e p50 {ms(q[5])}, p99 {ms(q[6])}")
    for n, r, _ in keys: T.dot(n, r["e99"], "kf", f"Kafka 1 topic × 1,000 partitions, {n:,} keys: e2e p99 {ms(r['e99'])}", shape="diamond")
    k50 = [x for x in k1 if x[0] == 50000]
    if k50: T.text(50000, k50[0][1]["e99"], "Kafka, partitions", "ink lab", "end", dx=-10, dy=-6)
    q50 = [q for q in q1 if q[1] == 50000]
    if q50: T.text(50000, q50[0][6], "Queen, partitions", "ink lab", "middle", dy=-14)
    if keys:
        T.text(keys[0][0], keys[0][1]["e99"], "Kafka, keys (~1k–10k share a partition)", "ink2 small", "middle", dx=70, dy=22)
    T.text(100_000, 16_000, "Kafka brokers CPU-bound", "muted small", "middle")
    if kt("1x2000-1000k"):
        T.text(2000, kt("1x2000-1000k")["e99"], "one 10-s spike", "muted small", "middle", dy=-14)
    B = Panel(80, 560, 700, 160, xr, (0, 16), xlog=True)
    B.grid(xt, [0, 4, 8, 12, 16], xl, ["0", "4", "8", "12", "16"], "partitions in total (Kafka diamonds: keys)",
           "CPU of the busiest node's broker process (cores of 16)")
    B.line([(p, busy[t]) for p, r, t in k1 if t in busy], "k"); B.line([(q[1], q[8]) for q in q1], "q")
    B.line([(n, busy[t]) for n, r, t in keys if t in busy], "k", dash=True)
    for p, t, r, tag in km:
        if tag in busy: B.dot(p, busy[tag], "", f"Kafka {t} topics, {p:,} partitions: busiest broker {busy[tag]:.1f} cores", hollow=True, stroke_cls="k")
    for q in qm: B.dot(q[1], q[8], "", f"Queen {q[0]:,} queues: leader {q[8]} cores", hollow=True, stroke_cls="q")
    for p, r, t in k1:
        if t in busy: B.dot(p, busy[t], "kf", f"Kafka 1 topic × {p:,}: busiest broker {busy[t]:.1f} cores")
    for q in q1: B.dot(q[1], q[8], "qf", f"Queen 1 queue × {q[1]:,}: leader {q[8]} cores")
    for n, r, t in keys:
        if t in busy: B.dot(n, busy[t], "kf", f"Kafka keys ({n:,}): busiest broker {busy[t]:.1f} cores", shape="diamond")
    foot = [f'<text class="muted small" x="24" y="{Hh - 34}">CPU: Kafka = JVM of the busiest broker, load-window average; Queen = leader process (3-s sample). Hover a mark for its numbers.</text>',
            f'<text class="muted small" x="24" y="{Hh - 16}">Kafka 1000-topic points used 297 consumers (Queen: one per queue); the fair re-run is pending. Kafka at 100k partitions caught up only after stalls.</text>']
    open(os.path.join(OUT, "queen-vs-kafka-partitions.svg"), "w").write(
        svg(W, Hh, "Queen vs Kafka by partitions", head + legend + T.o + B.o + foot))


def partitions_only_chart(K):
    """1 topic / queue x N partitions only (series B): e2e p50 + p99 and busiest-node CPU, Kafka vs Queen final"""
    kt = lambda t: K.get(t)
    k1 = [(p, kt(f"1x{p}-1000k"), f"1x{p}-1000k") for p in (200, 2000, 10000, 50000, 100000) if kt(f"1x{p}-1000k")]
    busy = kafka_busiest([t for *_, t in k1])
    q1 = [q for q in QUEEN_FINAL if q[0] == 1 and q[2] == 1_000_000]
    W, Hh = 900, 790
    head = [f'<text class="ink t" x="24" y="34">Partitions: Queen stays flat up to 1M, Kafka falls off past 10k</text>',
            f'<text class="ink2 st" x="24" y="56">1 topic / queue × N partitions · 1M msg/s offered · 256 B JSON in 100-message units · same 3 brokers × 16 vCPU · 60 s, last 30 s</text>',
            f'<text class="ink2 st" x="24" y="74">Kafka 4.3.1: RF 3, acks=all, lz4, 8 replica fetchers · Queen final build: raft 3 nodes, dedup off</text>']
    LY = 104
    legend = [
        f'<line x1="24" x2="44" y1="{LY - 4}" y2="{LY - 4}" class="line k"/><circle cx="34" cy="{LY - 4}" r="4.5" class="kf ring" stroke-width="2"/><text class="ink2 small" x="52" y="{LY}">Kafka</text>',
        f'<line x1="104" x2="124" y1="{LY - 4}" y2="{LY - 4}" class="line q"/><circle cx="114" cy="{LY - 4}" r="4.5" class="qf ring" stroke-width="2"/><text class="ink2 small" x="132" y="{LY}">Queen</text>',
        f'<line x1="190" x2="214" y1="{LY - 4}" y2="{LY - 4}" class="line" style="stroke:#898781"/><text class="ink2 small" x="220" y="{LY}">p99</text>',
        f'<line x1="258" x2="282" y1="{LY - 4}" y2="{LY - 4}" class="line dash" style="stroke:#898781"/><text class="ink2 small" x="288" y="{LY}">p50</text>']
    xr = (120, 2_000_000)
    xt = [200, 2000, 10_000, 50_000, 100_000, 1_000_000]
    xl = ["200", "2k", "10k", "50k", "100k", "1M"]
    T = Panel(80, 150, 700, 340, xr, (5, 20_000), xlog=True, ylog=True)
    T.grid(xt, [10, 100, 1000, 10_000], xl, ["10 ms", "100 ms", "1 s", "10 s"], None, "end-to-end latency, producer to consumer (log scale)")
    T.line([(p, r["e99"]) for p, r, _ in k1], "k"); T.line([(p, r["e50"]) for p, r, _ in k1], "k", dash=True)
    T.line([(q[1], q[6]) for q in q1], "q"); T.line([(q[1], q[5]) for q in q1], "q", dash=True)
    for p, r, _ in k1:
        tip = f"Kafka 1 topic × {p:,} partitions: in {r['push'] / 1000:.0f}k / out {r['cons'] / 1000:.0f}k, e2e p50 {ms(r['e50'])}, p99 {ms(r['e99'])}"
        T.dot(p, r["e99"], "kf", tip); T.dot(p, r["e50"], "kf", tip)
    for q in q1:
        tip = f"Queen 1 queue × {q[1]:,} partitions: in {q[3] // 1000}k / out {q[4] // 1000}k, e2e p50 {ms(q[5])}, p99 {ms(q[6])}"
        T.dot(q[1], q[6], "qf", tip); T.dot(q[1], q[5], "qf", tip)
    kl = k1[-1]; ql = q1[-1]
    T.text(kl[0], kl[1]["e99"], f"Kafka p99 {ms(kl[1]['e99'])}", "ink lab", "start", dx=10, dy=4)
    T.text(kl[0], kl[1]["e50"], f"p50 {ms(kl[1]['e50'])}", "ink2 small", "start", dx=10, dy=4)
    T.text(ql[1], ql[6], f"Queen p99 {ms(ql[6])}", "ink lab", "end", dx=-10, dy=-10)
    T.text(ql[1], ql[5], f"p50 {ms(ql[5])}", "ink2 small", "end", dx=-10, dy=16)
    T.text(1_000_000, 8_000, "no Kafka run:", "muted small", "middle")
    T.text(1_000_000, 8_000, "saturated at 100k", "muted small", "middle", dy=14)
    if kt("1x2000-1000k"):
        T.text(2000, kt("1x2000-1000k")["e99"], "one 10-s spike", "muted small", "middle", dy=-12)
    B = Panel(80, 560, 700, 150, xr, (0, 16), xlog=True)
    B.grid(xt, [0, 4, 8, 12, 16], xl, ["0", "4", "8", "12", "16"], "partitions (1 topic / 1 queue)", "CPU of the busiest node's broker process (cores of 16)")
    B.line([(p, busy[t]) for p, r, t in k1 if t in busy], "k"); B.line([(q[1], q[8]) for q in q1], "q")
    for p, r, t in k1:
        if t in busy: B.dot(p, busy[t], "kf", f"Kafka 1 topic × {p:,}: busiest broker JVM {busy[t]:.1f} cores (load-window average)")
    for q in q1: B.dot(q[1], q[8], "qf", f"Queen 1 queue × {q[1]:,}: leader {q[8]} cores (3-s sample)")
    kb = [(p, busy[t]) for p, r, t in k1 if t in busy]
    if kb: B.text(kb[-1][0], kb[-1][1], f"Kafka {kb[-1][1]:.1f}", "ink lab", "middle", dy=-12)
    B.text(ql[1], ql[8], f"Queen {ql[8]:.1f}", "ink lab", "end", dx=-10, dy=20)
    foot = [f'<text class="muted small" x="24" y="{Hh - 16}">Every point delivered in ≈ out; Kafka at 100k (in 1.03M, out 1.17M) caught up only after multi-second stalls. Hover a mark for its numbers.</text>']
    open(os.path.join(OUT, "partitions-queen-vs-kafka.svg"), "w").write(
        svg(W, Hh, "Queen vs Kafka by partitions", head + legend + T.o + B.o + foot))


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    K = kafka_rows()
    rate_chart(K)
    partitions_chart(K)
    final_rate_chart(K)
    final_partitions_chart(K)
    partitions_only_chart(K)
    print("wrote", os.listdir(OUT))
