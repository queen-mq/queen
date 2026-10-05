#!/usr/bin/env python3
"""slack_svg.py: the broker benchmark as a dense 1920 x 1080 technical summary, for a Slack post. The same runs as
compact_svg.py and matrix_svg.py, laid out for engineers instead of a feed: no logo, no callouts, every system drawn
with the same weight, the cases where another system wins left in, and the method in the footer.

Top row: three charts on one partitions axis at 1M msg/s offered (p99, broker cores, memory). Bottom: the rate
ladder at 200 partitions (p99 and cores per cell) and the transactions table.

Queen's own HTTP runs live in benchmark-queen/2026-09-30-stage03-3node, which is not in the repository. When
matrix_svg.py finds no rows for them, the published values below are used, copied from webdoc/src/figures/benchmarks/
partitions-p99.ts, partitions-cpu.ts, partitions-memory.ts, throughput-ceiling.ts, throughput-p99.ts and
throughput-cpu.ts. Commit-time p99 for transactions is from txn-latency.ts (its second value, end to end, is the same
run as data["C"]).

Usage: charts/slack_svg.py [outdir]  ->  brokers-slack-{light,dark}.svg and .png (PNG at 2x)"""
import base64, os, subprocess, sys, tempfile, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import matrix_svg as M  # noqa: E402
import compact_svg as C  # noqa: E402

OUTDIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join(M.H, "charts")
W, H = 1920, 1080

ORDER = ("kafka", "redpanda", "pulsar", "queen", "queenk")
NAME = {"kafka": "Kafka", "redpanda": "Redpanda", "pulsar": "Pulsar", "queen": "Queen",
        "queenk": "Queen (Kafka protocol)"}
TINT = {"light": "#f3e4dd", "dark": "#2b1e1a"}

# Published values for Queen's HTTP runs (see the docstring).
QUEEN_P99 = [(200, 146), (10_000, 144), (50_000, 103), (100_000, 163), (1_000_000, 123), (5_000_000, 138),
             (10_000_000, 122)]
QUEEN_CPU = [(200, 17.2), (10_000, 18.4), (50_000, 18.0), (100_000, 18.3), (1_000_000, 18.3), (10_000_000, 17.3)]
QUEEN_MEM = [(200, 3.6), (100_000, 3.9), (1_000_000, 5.6), (5_000_000, 11.8), (10_000_000, 19.9)]  # leader, anon
QUEEN_A = [(300_000, 300_000, 22, 9.0), (600_000, 600_000, 98, 13.0), (1_000_000, 1_000_000, 146, 17.2),
           (1_500_000, 1_340_000, None, 24.9), (2_000_000, 1_230_000, None, 27.2)]
TXN_COMMIT = {"queen": 4, "kafka": 34, "redpanda": 151, "pulsar": 32}
CAP = 48  # cores: 3 brokers x 16 vCPU (ncpu=16 in every sampler file)
RATES = (300_000, 600_000, 1_000_000, 1_500_000, 2_000_000, 3_000_000, 4_000_000)


def behind(p):
    """Fell behind: consumed under 97% of offered, or p99 past 5 s (the docs' hollow-mark rule)."""
    return p["cons"] < 0.97 * p["offered"] or (p["p99"] or 0) > 5000


def ok(p, lim=200):
    return p["cons"] >= 0.97 * p["offered"] and p["p99"] is not None and p["p99"] < lim


def fmt_rate(v):
    if v >= 1e6:
        return f"{v / 1e6:.2f}".rstrip("0").rstrip(".") + "M"
    return f"{v / 1e3:.0f}k"


def series(data):
    """{metric: {system: [(partitions, value, hollow)]}} for the top row."""
    out = {"p99": {}, "cpu": {}, "mem": {}}
    for s in ORDER:
        pts = data["B"].get(s) or []
        if pts:
            for key, field in (("p99", "p99"), ("cpu", "cpu"), ("mem", "rss")):
                out[key][s] = [(p["x"], p[field], behind(p)) for p in pts]
    if "queen" not in out["p99"]:
        out["p99"]["queen"] = [(x, v, False) for x, v in QUEEN_P99]
        out["cpu"]["queen"] = [(x, v, False) for x, v in QUEEN_CPU]
        out["mem"]["queen"] = [(x, v, False) for x, v in QUEEN_MEM]
    return out


def ladder(data):
    """{system: {offered: point}} for the 200-partition rate ladder."""
    out = {}
    for s in ORDER:
        pts = data["A"].get(s) or []
        if pts:
            out[s] = {p["offered"]: p for p in pts}
    if "queen" not in out:
        out["queen"] = {o: dict(offered=o, cons=c, p99=p, cpu=k) for o, c, p, k in QUEEN_A}
    return out


def first_fail(pts):
    best = None
    for p in sorted(pts, key=lambda p: p["x"] if "x" in p else p["offered"]):
        if ok(p):
            best = p
        else:
            break
    return best


def render(theme, data, tq, inter_b64):
    t = C.THEMES[theme]
    c = C.Card(t)
    ts = c.ts
    hi, mid, low = t["text_hi"], t["text_mid"], t["text_low"]
    o = c.o
    o.append(f'<rect x="0" y="0" width="{W}" height="{H}" fill="{t["page"]}"/>')

    def line(x1, y1, x2, y2, color=None, w=1, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{color or t["bd"]}" '
                 f'stroke-width="{w}"{d}/>')

    def panel(x, y, w, h):
        o.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{t["card"]}" '
                 f'stroke="{t["bd"]}" stroke-width="1"/>')

    def swatch(x, y, s, dashed=False):
        if dashed:
            line(x, y - 4, x + 14, y - 4, t[s], 2.5, "4 3")
        else:
            o.append(f'<rect x="{x + 2}" y="{y - 9}" width="10" height="10" rx="2" fill="{t[s]}"/>')

    # ---- header + legend
    c.text(32, 52, "Message broker benchmark: Kafka, Redpanda, Pulsar and Queen", 26, 650, hi, ls="-0.4")
    c.text(32, 78, "3 brokers on DigitalOcean (16 vCPU, 31 GB, one disk each) and the same 3 load hosts for every "
                   "system · 256-byte messages · latency end to end · October 2026", 14, 400, mid)
    lx = 1888
    for s in reversed(ORDER):
        wname = 9 + len(NAME[s]) * 7.6
        lx -= wname + 18
        swatch(lx, 52, s)
        c.text(lx + 18, 52, NAME[s], 13.5, 500, hi)

    # ---- top row: three charts on one partitions axis
    panel(32, 98, 1856, 432)
    c.text(56, 128, "As partitions grow, at 1M msg/s offered to one topic", 17, 650, hi)
    c.text(56, 149, "x: partitions in the topic, log scale · hollow: fell behind (consumed < 97% of offered, or "
                    "p99 > 5 s) · Kafka, Redpanda and Pulsar were not run above 100k partitions (Kafka could not "
                    "create 1M; Pulsar's 1M setup failed after 31 min)", 12.5, 400, low)
    S = series(data)
    charts = (
        ("p99 end-to-end latency", "p99", "log", (8, 70000),
         ((10, "10 ms"), (100, "100 ms"), (1000, "1 s"), (10000, "10 s"))),
        ("broker CPU, cores summed over 3 brokers", "cpu", "lin", (0, 50),
         ((0, "0"), (10, "10"), (20, "20"), (30, "30"), (40, "40"), (50, "50"))),
        ("memory, GB: RSS of the busiest broker (Queen: leader, anonymous)", "mem", "lin", (0, 30),
         ((0, "0"), (10, "10"), (20, "20"), (30, "30"))),
    )
    cw, gap = 578, 41
    for i, (title, key, kind, (lo, hi_), ticks) in enumerate(charts):
        x0 = 56 + i * (cw + gap)
        X0, X1, Y0, Y1 = x0 + 50, x0 + cw - 6, 196, 486
        c.text(x0, 178, title, 13.5, 600, hi)
        xs = C.logs(150, 13e6, X0, X1)
        ys = C.logs(lo, hi_, Y1, Y0) if kind == "log" else (lambda v, lo=lo, hi_=hi_: Y1 - (v - lo) / (hi_ - lo) * (Y1 - Y0))
        for v, lab in ticks:
            line(X0, ys(v), X1, ys(v))
            c.text(X0 - 8, ys(v) + 4, lab, 11.5, 400, low, "end")
        for v, lab in ((200, "200"), (1000, "1k"), (10000, "10k"), (100000, "100k"), (1e6, "1M"), (1e7, "10M")):
            line(xs(v), Y1, xs(v), Y1 + 4, t["bd_hi"])
            c.text(xs(v), Y1 + 18, lab, 11.5, 400, low, "middle")
        if key == "cpu":
            line(X0, ys(CAP), X1, ys(CAP), t["text_low"], 1, "5 4")
            c.text(X0 + 6, ys(CAP) - 6, f"{CAP} vCPU: the 3 brokers' capacity", 11.5, 500, low)
        for s in ORDER:
            pts = S[key].get(s)
            if not pts:
                continue
            if key == "cpu":
                pts = [(x, min(v, CAP), h) for x, v, h in pts]
            dash = ' stroke-dasharray="6 4"' if (key == "mem" and s == "queen") else ""
            xy = [(xs(x), ys(min(max(v, lo if kind == "log" else lo), hi_)), h) for x, v, h in pts]
            o.append('<path d="' + " ".join(("M" if j == 0 else "L") + f"{a:.1f},{b:.1f}" for j, (a, b, _) in enumerate(xy))
                     + f'" fill="none" stroke="{t[s]}" stroke-width="2.2" stroke-linejoin="round" '
                     f'stroke-linecap="round"{dash}/>')
            for a, b, h in xy:
                o.append(f'<circle cx="{a:.1f}" cy="{b:.1f}" r="3.6" fill="{t["card"] if h else t[s]}" '
                         f'stroke="{t[s]}" stroke-width="1.8"/>')
    # one honest annotation per chart, at the right edge of the data
    q = S["mem"]["queen"][-1]
    xs = C.logs(150, 13e6, 56 + 2 * (cw + gap) + 50, 56 + 3 * cw + 2 * gap - 6)
    q0 = S["mem"]["queen"][0]
    mx1 = 56 + 3 * cw + 2 * gap - 6
    c.text(mx1, 196 + 18, f"Queen leader: +{q[1] - q0[1]:.1f} GB from 200", 11.5, 500, mid, "end")
    c.text(mx1, 196 + 34, "to 10M partitions, under 2 KB each", 11.5, 500, mid, "end")
    sat = [(x, v) for x, v, _ in S["cpu"].get("queenk", []) if v > CAP]
    if sat:
        xs2 = C.logs(150, 13e6, 56 + (cw + gap) + 50, 56 + 2 * cw + gap - 6)
        x, v = sat[-1]
        c.text(xs2(x) + 10, 196 + 30, f"Queen (Kafka protocol) at {fmt_rate(x).replace('k', 'k')} partitions:", 11.5, 400, low)
        c.text(xs2(x) + 10, 196 + 46, f"saturated, sampler reads {v:.1f}", 11.5, 400, low)

    # ---- bottom left: 200 partitions, one chart: p99 as the offered rate rises
    panel(32, 546, 1060, 434)
    c.text(56, 576, "200 partitions: p99 as the offered rate rises", 17, 650, hi)
    c.text(56, 597, "x: offered msg/s · y: p99 end-to-end, log scale · hollow: fell behind (consumed < 97% of offered, "
                    "or p99 > 5 s) · Kafka acks from page cache, the others after 2 of 3 fsync", 12.5, 400, low)
    L = ladder(data)
    X0, X1, Y0, Y1 = 104, 948, 630, 936
    xs = lambda v: X0 + v / 4.2e6 * (X1 - X0)  # noqa: E731
    ys = C.logs(5, 60000, Y1, Y0)
    for v, lab in ((10, "10 ms"), (100, "100 ms"), (1000, "1 s"), (10000, "10 s")):
        line(X0, ys(v), X1, ys(v))
        c.text(X0 - 8, ys(v) + 4, lab, 11.5, 400, low, "end")
    for v in (0, 5e5, 1e6, 1.5e6, 2e6, 2.5e6, 3e6, 3.5e6, 4e6):
        line(xs(v), Y1, xs(v), Y1 + 4, t["bd_hi"])
        if v % 1e6 == 0:
            c.text(xs(v), Y1 + 18, "0" if v == 0 else fmt_rate(v), 11.5, 400, low, "middle")
    line(X0, ys(200), X1, ys(200), t["text_low"], 1, "5 4")
    c.text(X1, ys(200) - 6, "200 ms", 11.5, 500, low, "end")
    ends = {}
    for s in ORDER:
        pts = sorted((p for p in L.get(s, {}).values() if p["p99"] is not None and p["p99"] != float("inf")),
                     key=lambda p: p["offered"])
        if not pts:
            continue
        xy = [(xs(p["offered"]), ys(min(max(p["p99"], 5), 60000)), behind(p)) for p in pts]
        o.append('<path d="' + " ".join(("M" if j == 0 else "L") + f"{a:.1f},{b:.1f}" for j, (a, b, _) in enumerate(xy))
                 + f'" fill="none" stroke="{t[s]}" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/>')
        for a, b, h in xy:
            o.append(f'<circle cx="{a:.1f}" cy="{b:.1f}" r="3.6" fill="{t["card"] if h else t[s]}" '
                     f'stroke="{t[s]}" stroke-width="1.8"/>')
        ends[s] = (xy[-1][0], xy[-1][1], pts[-1])
    # end labels: the name and the last p99, placed where nothing else runs
    for s in ("kafka", "redpanda", "pulsar"):
        a, b, p = ends[s]
        c.text(a + 10, b + 4, ts(NAME[s], t[s], 600) + ts(f"  {C.fmt_ms(p['p99'])}", mid), 12.5, raw=True)
    a, b, p = ends["queen"]
    q15 = L["queen"].get(1_500_000)
    tail = f"  fell behind at 1.5M offered ({fmt_rate(q15['cons'])} consumed)" if q15 else ""
    c.text(a + 10, b + 20, ts("Queen", t["queen"], 600) + ts(f"  {C.fmt_ms(p['p99'])}", mid)
           + ts(tail, low, 400, 11.5), 12.5, raw=True)
    a, b, p = ends["queenk"]
    k15 = L["queenk"].get(1_500_000)
    if k15:
        c.text(xs(1_500_000) - 10, ys(k15["p99"]) + 4, ts("Queen (Kafka protocol)", t["queenk"], 600), 12.5,
               anchor="end", raw=True)

    # ---- bottom right: transactions
    panel(1108, 546, 780, 434)
    c.text(1132, 576, "Transactions, 10 messages each, 200 partitions", 17, 650, hi)
    c.text(1132, 597, "at 9k msg/s (900 txn/s) · commit: the commit call answered · e2e: scheduled input to "
                      "consumed committed output", 12.5, 400, low)
    tc = dict(name=1132, commit=1388, e2e=1510, cores=1584, mem=1664, rate=1864)
    hy = 630
    c.text(tc["name"], hy, "system", 12, 500, low)
    c.text(tc["commit"], hy, "commit p99", 12, 500, low, "end")
    c.text(tc["e2e"], hy, "e2e p50 / p99", 12, 500, low, "end")
    c.text(tc["cores"], hy, "cores", 12, 500, low, "end")
    c.text(tc["mem"], hy, "RSS", 12, 500, low, "end")
    c.text(tc["rate"], hy, "highest rate, p99 < 200 ms", 12, 500, low, "end")
    line(1132, hy + 9, 1864, hy + 9)
    tx = {s: sorted(data["C"][s], key=lambda p: p["x"]) for s in ("kafka", "redpanda", "pulsar", "queen")}
    t9 = {s: next(p for p in tx[s] if p["x"] == 9000) for s in tx}
    for i, s in enumerate(sorted(t9, key=lambda s: t9[s]["p99"])):
        y = hy + 34 + i * 34
        p = t9[s]
        swatch(1132, y, s)
        c.text(1152, y, NAME[s], 14, 500, hi)
        c.text(tc["commit"], y, f"{TXN_COMMIT[s]} ms", 14, 600, hi, "end")
        c.text(tc["e2e"], y, f"{p['p50']:.0f} / {p['p99']:.0f} ms", 14, 400, hi, "end")
        c.text(tc["cores"], y, f"{p['cpu']:.1f}", 14, 400, mid, "end")
        c.text(tc["mem"], y, f"{p['rss']:.1f} GB", 14, 400, mid, "end")
        best = first_fail(tx[s])
        c.text(tc["rate"], y, (fmt_rate(best["x"]) if best else "< 9k") + f"  (tested to {fmt_rate(tx[s][-1]['x'])})",
               14, 400, mid, "end")
    b100 = {s: first_fail(sorted(data["C100"][s], key=lambda p: p["x"])) for s in ("pulsar", "queen")}
    q288 = next((p for p in data["C100"]["queen"] if p["x"] == 288000), None)
    ny = hy + 34 + 4 * 34 + 8
    line(1132, ny - 18, 1864, ny - 18)
    c.text(1132, ny, ts("100 messages per transaction", hi, 600) + ts(", highest rate with p99 < 200 ms:", mid),
           13, raw=True)
    for j, s in enumerate(("pulsar", "queen")):
        p = b100[s]
        c.text(1132, ny + 21 + j * 20, ts(NAME[s], t[s], 600)
               + ts(f"  {fmt_rate(p['x'])} msg/s, p99 {C.fmt_ms(p['p99'])}, {p['cpu']:.1f} cores", mid), 13, raw=True)
    if q288:
        c.text(1132, ny + 61, f"At 288k offered Queen consumed {fmt_rate(q288['cons'])} (p99 {C.fmt_ms(q288['p99'])}); "
                              "Pulsar kept up to 288k and consumed 572k at 576k offered.", 12.5, 400, low)
    if tq:
        c.text(1132, ny + 90, ts("100k partitions", hi, 600)
               + ts(f", 10 messages each, 9k msg/s: Queen e2e p99 {C.fmt_ms(tq['p99'])}, {tq['cpu']:.1f} cores "
                    "(not run on the others)", mid), 13, raw=True)

    # ---- footer: the method
    for i, ln in enumerate((
            "Durability: Redpanda, Pulsar (E3/Qw3/Qa2) and Queen acknowledge after 2 of 3 copies are fsynced; Kafka "
            "(RF 3, acks=all, min.insync.replicas=2) acknowledges from page cache. Latency: consumer receive time minus "
            "the producer's scheduled send time.",
            "Cores: broker CPU summed over the 3 nodes, steady window. RSS: the busiest broker process; the Queen (HTTP) "
            "memory line is the leader's anonymous RSS. Clients: franz-go (Kafka, Redpanda, Queen's Kafka protocol), "
            "pulsar-client-go, HTTP (Queen).",
            "Versions: Kafka 4.3.1, Redpanda 26.2.3, Pulsar 4.2.4, Queen 2.0.0-beta.2. One run per point. "
            "Method and raw results: queenmq.com/benchmarks/methodology")):
        c.text(32, 1008 + i * 20, ln, 12, 400, low)

    style = (f"@font-face{{font-family:'Inter';src:url(data:font/woff2;base64,{inter_b64}) format('woff2');"
             "font-weight:100 900;font-style:normal}"
             "text{font-family:'Inter',ui-sans-serif,system-ui,-apple-system,'Helvetica Neue',sans-serif;"
             "font-feature-settings:'cv11','ss01','ss03','cv02','tnum'}")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}">\n'
            f"<style>{style}</style>\n" + "\n".join(o) + "\n</svg>\n")


def png(svg_path, png_path):
    """compact_svg.png() with this card's size: headless Chrome at 2x, a throwaway profile, closed once the file
    settles."""
    if os.path.exists(png_path):
        os.remove(png_path)
    with tempfile.TemporaryDirectory() as prof:
        pr = subprocess.Popen([C.CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--no-first-run",
                               f"--user-data-dir={prof}", "--force-device-scale-factor=2", f"--window-size={W},{H}",
                               f"--screenshot={png_path}", "file://" + svg_path],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        size, deadline = -1, time.time() + 90
        while time.time() < deadline and pr.poll() is None:
            time.sleep(0.5)
            if os.path.exists(png_path) and os.path.getsize(png_path) == size > 0:
                break
            size = os.path.getsize(png_path) if os.path.exists(png_path) else -1
        if pr.poll() is None:
            pr.terminate()
            try:
                pr.wait(10)
            except subprocess.TimeoutExpired:
                pr.kill()
    if not os.path.exists(png_path):
        raise SystemExit(f"no screenshot for {svg_path}")


def main():
    data = M.collect()
    tq = M.txn_rows(["runs/txn/queen/1x100000-t10-9k"]).get("runs/txn/queen/1x100000-t10-9k")
    inter_b64 = base64.b64encode(open(C.INTER, "rb").read()).decode()
    for theme in ("light", "dark"):
        svg = os.path.join(OUTDIR, f"brokers-slack-{theme}.svg")
        open(svg, "w").write(render(theme, data, tq, inter_b64))
        png(svg, svg[:-4] + ".png")
        print(svg, svg[:-4] + ".png")


if __name__ == "__main__":
    main()
