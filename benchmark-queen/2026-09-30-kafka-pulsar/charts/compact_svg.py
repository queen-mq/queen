#!/usr/bin/env python3
"""compact_svg.py: the 1080 x 1350 summary card of the broker benchmark, for LinkedIn and Slack, in the Queen
dashboard's two themes (app/src/style.css tokens: :root = dark, html.light = light). Three findings, one metric each:
latency as partitions grow (the hero), the throughput ceiling at 200 partitions and exactly-once transactions. The data
is matrix_svg.py's (same runs, same loaders), so the card and the full matrix cannot disagree.

Colours follow the dashboard's rules: neutrals from the --ink / --text ladders, chart grid --bd, ticks --text-low,
categorical series --cat-1..5, and the only warm colour is Queen's (the sunflower's amber, --warn-400). Inter is
embedded (the dashboard's face, from webdoc's @fontsource-variable/inter), with the dashboard's feature settings; the
PNGs come from headless Chrome because rsvg cannot load embedded fonts.

Usage: charts/compact_svg.py [outdir]  ->  brokers-compact-{dark,light}.svg and .png"""
import base64, math, os, subprocess, sys, tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import matrix_svg as M  # noqa: E402

REPO = os.path.dirname(os.path.dirname(M.H))
OUTDIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join(M.H, "charts")
INTER = os.path.join(REPO, "webdoc", "node_modules", ".pnpm", "@fontsource-variable+inter@5.3.0", "node_modules",
                     "@fontsource-variable", "inter", "files", "inter-latin-wght-normal.woff2")
BADGE = os.path.join(REPO, "app", "public", "queen-sunflower-badge.webp")
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
W, H = 1080, 1350

# The dashboard's tokens (app/src/style.css), dark = :root, light = html.light
THEMES = {
    "dark": dict(page="#0b0a0a", card="#111010", bd="#1f1d1b", bd_hi="#34312e", text_hi="#eeebe6", text_mid="#aaa59e",
                 text_low="#8f8a83", faint="#57534e", queen="#e8a849", queenk="#b092d0", kafka="#7f9fd0",
                 pulsar="#6fb3a3", redpanda="#d08fae"),
    "light": dict(page="#f2f0ec", card="#f9f7f4", bd="#e0dcd6", bd_hi="#cfc9c1", text_hi="#1f1d1a", text_mid="#4f4b45",
                  text_low="#6c675f", faint="#b0aaa2", queen="#9a5b07", queenk="#6f4fa0", kafka="#44689e",
                  pulsar="#2c7a6a", redpanda="#9a4d73"),
}
NAME = {"kafka": "Kafka", "redpanda": "Redpanda", "pulsar": "Pulsar", "queen": "Queen",
        "queenk": "Queen + Kafka clients"}


def esc(t):
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def logs(d0, d1, r0, r1):
    a, b = math.log10(d0), math.log10(d1)
    return lambda v: r0 + (math.log10(v) - a) / (b - a) * (r1 - r0)


def fmt_rate(v):
    return f"{v / 1e6:g}M" if v >= 1e6 else f"{int(v / 1e3 + 0.5)}k"


def fmt_ms(v):
    return f"{v / 1000:.1f} s" if v >= 1000 else f"{v:.0f} ms"


def ceiling(pts, lim=200):
    """Highest offered rate delivered in full (>= 97%) with p99 under lim ms, every lower rate too; at_top = the
    highest rate tested still passed (shown as >=). Returns (rate, at_top, p99 at that rate)."""
    best, top = None, max(p["offered"] for p in pts)
    for p in sorted(pts, key=lambda p: p["offered"]):
        if p["cons"] >= 0.97 * p["offered"] and p["p99"] < lim:
            best = p
        else:
            break
    return best["offered"], best["offered"] == top, best["p99"]


class Card:
    def __init__(self, t):
        self.t, self.o = t, []

    def text(self, x, y, s, size=18, weight=400, fill=None, anchor="start", ls=None, raw=False):
        fill = fill or self.t["text_hi"]
        lsa = f' letter-spacing="{ls}"' if ls is not None else ""
        self.o.append(f'<text xml:space="preserve" x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" '
                      f'fill="{fill}" text-anchor="{anchor}"{lsa}>{s if raw else esc(s)}</text>')

    @staticmethod
    def ts(s, fill=None, weight=None, size=None):
        a = "".join(f' {k}="{v}"' for k, v in (("fill", fill), ("font-weight", weight), ("font-size", size)) if v)
        return f"<tspan{a}>{esc(s)}</tspan>"

    def panel(self, x, y, w, h):
        self.o.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{self.t["card"]}" '
                      f'stroke="{self.t["bd"]}" stroke-width="1.5"/>')

    def bar_row(self, x, y, w_max, v, v_max, color, label_svg, value_svg, cont=False):
        """name line, then a rounded bar of v / v_max of w_max, the value after it; cont = '>=': a fading tail."""
        self.text(x, y, label_svg, 19, 600, raw=True)
        bw = max(6.0, w_max * v / v_max)
        self.o.append(f'<rect x="{x}" y="{y + 10}" width="{bw:.1f}" height="22" rx="5" fill="{color}"/>')
        end = x + bw
        if cont:
            for i, op in enumerate((0.5, 0.28, 0.12)):
                self.o.append(f'<rect x="{end + 5 + i * 12:.1f}" y="{y + 10}" width="8" height="22" rx="3" '
                              f'fill="{color}" opacity="{op}"/>')
            end += 5 + 3 * 12
        self.text(end + 10, y + 27, value_svg, 19, 600, raw=True)


def render(theme, data, tq, inter_b64, badge_b64):
    t = THEMES[theme]
    c = Card(t)
    ts = c.ts
    hi, mid, low = t["text_hi"], t["text_mid"], t["text_low"]
    c.o.append(f'<rect x="0" y="0" width="{W}" height="{H}" fill="{t["page"]}"/>')

    # ---- header: the sidebar's brand lockup (round mark + QueenMQ, version-style note on the right), then the title
    c.o.append(f'<image x="56" y="34" width="44" height="44" href="data:image/webp;base64,{badge_b64}"/>')
    c.text(114, 64, "QueenMQ", 26, 600, hi, ls="-0.26")
    c.text(W - 56, 64, "Broker benchmark · October 2026", 19, 400, low, "end")
    c.text(56, 136, "Four brokers, one rig", 48, 700, hi, ls="-1")
    c.text(56, 172, "Kafka, Redpanda, Pulsar and Queen on the same three servers, with the same load", 21, 400, mid)

    # ---- card 1: latency as partitions grow (the hero)
    c.panel(40, 200, 1000, 590)
    c.text(72, 248, "Latency as partitions grow", 29, 700, hi, ls="-0.4")
    c.text(72, 278, "p99 end-to-end at 1M msg/s, one topic · log scales · lower is better", 18, 400, low)
    X0, X1, Y0, Y1 = 150, 785, 312, 702
    xs = logs(150, 11.5e6, X0, X1)
    ys = logs(8, 70000, Y1, Y0)
    for v, lab in ((10, "10 ms"), (100, "100 ms"), (1000, "1 s"), (10000, "10 s")):
        c.o.append(f'<line x1="{X0}" y1="{ys(v):.1f}" x2="{X1 + 10}" y2="{ys(v):.1f}" stroke="{t["bd"]}" stroke-width="1.5"/>')
        c.text(X0 - 14, ys(v) + 6, lab, 16, 400, low, "end")
    for v, lab in ((200, "200"), (10000, "10k"), (100000, "100k"), (1e6, "1M"), (1e7, "10M")):
        c.text(xs(v), Y1 + 34, lab, 16, 400, low, "middle")
    c.text((X0 + X1) / 2, Y1 + 62, "partitions", 16, 400, low, "middle")
    B = data["B"]
    for s in ("redpanda", "pulsar", "kafka", "queenk", "queen"):
        pts = B.get(s) or []
        xy = [(xs(p["x"]), ys(min(max(p["p99"], 8), 70000))) for p in pts]
        c.o.append('<path d="' + " ".join(("M" if i == 0 else "L") + f"{a:.1f},{b:.1f}" for i, (a, b) in enumerate(xy))
                   + f'" fill="none" stroke="{t[s]}" stroke-width="{4.5 if s == "queen" else 3.5}" '
                   f'stroke-linejoin="round" stroke-linecap="round"/>')
        for a, b in xy:
            c.o.append(f'<circle cx="{a:.1f}" cy="{b:.1f}" r="{6.5 if s == "queen" else 5.5}" fill="{t[s]}" '
                       f'stroke="{t["card"]}" stroke-width="2.5"/>')
    last = {s: B[s][-1] for s in B if B.get(s)}
    q, k, r, pu, qk = last["queen"], last["kafka"], last["redpanda"], last["pulsar"], last["queenk"]
    c.text(xs(q["x"]) + 16, ys(q["p99"]) + 7, ts("Queen ", t["queen"], 700) + ts(fmt_ms(q["p99"]), hi, 600), 20, raw=True)
    c.text(xs(k["x"]) + 16, ys(k["p99"]) + 7, ts("Kafka ", t["kafka"], 700) + ts(fmt_ms(k["p99"]), hi, 600), 20, raw=True)
    c.text(xs(r["x"]) + 16, ys(max(r["p99"], pu["p99"])) + 7,
           ts("Redpanda", t["redpanda"], 700) + ts(" · ", low) + ts("Pulsar ", t["pulsar"], 700)
           + ts(f"~{round((r['p99'] + pu['p99']) / 2000)} s", hi, 600), 20, raw=True)
    c.text(xs(qk["x"]) + 16, ys(qk["p99"]) - 4, ts("Queen + Kafka clients ", t["queenk"], 700) + ts(fmt_ms(qk["p99"]), hi, 600),
           19, raw=True)
    yx = ys(2200)
    for i, s in enumerate(("kafka", "pulsar")):
        c.text(xs(1e6) - 9 + i * 18, yx + 8, "×", 26, 700, t[s], "middle")
    c.text(xs(1e6) + 26, yx + 6, "Kafka, Pulsar: cannot run 1M partitions", 16, 400, low)
    c.text((xs(1e6) + xs(1e7)) / 2, ys(33) + 7, "Only Queen runs 1M–10M partitions", 21, 700, t["queen"], "middle")

    # ---- card 2: throughput ceiling at 200 partitions
    c.panel(40, 810, 490, 452)
    c.text(68, 858, "Throughput ceiling", 27, 700, hi, ls="-0.3")
    c.text(68, 886, "200 partitions · higher is better", 17, 400, low)
    c.text(68, 908, "highest msg/s delivered in full, p99 under 200 ms", 17, 400, low)
    ceil = {s: ceiling(data["A"][s]) for s in M.SYS if data["A"].get(s)}
    pref = ["kafka", "pulsar", "redpanda", "queen", "queenk"]
    order = sorted(ceil, key=lambda s: (-ceil[s][0], pref.index(s)))
    vmax = max(v[0] for v in ceil.values())
    for i, s in enumerate(order):
        v, top, p99 = ceil[s]
        y = 948 + i * 62
        lab = ts(NAME[s]) + (ts("  no fsync", low, 400, 15) if s == "kafka" else "")
        c.bar_row(68, y, 300, v, vmax, t[s], lab, ts(("≥ " if top else "") + fmt_rate(v)), cont=top)
        c.text(502, y, f"p99 {fmt_ms(p99)}", 15, 400, low, "end")

    # ---- card 3: exactly-once transactions
    c.panel(550, 810, 490, 452)
    c.text(578, 858, "Exactly-once transactions", 27, 700, hi, ls="-0.3")
    c.text(578, 886, "end-to-end p99 at 9k msg/s · lower is better", 17, 400, low)
    c.text(578, 908, "10 messages per transaction, 200 partitions", 17, 400, low)
    t9 = {s: next(p for p in data["C"][s] if p["x"] == 9000) for s in ("kafka", "redpanda", "pulsar", "queen")}
    order = sorted(t9, key=lambda s: t9[s]["p99"])
    pmax = max(p["p99"] for p in t9.values())
    for i, s in enumerate(order):
        c.bar_row(578, 948 + i * 58, 300, t9[s]["p99"], pmax, t[s], ts(NAME[s]), ts(fmt_ms(t9[s]["p99"])))
    peak = {s: max(p["cons"] for p in data["C100"][s]) for s in ("pulsar", "queen")}
    y = 948 + 4 * 58 + 22
    c.o.append(f'<line x1="578" y1="{y - 8}" x2="1012" y2="{y - 8}" stroke="{t["bd"]}" stroke-width="1.5"/>')
    c.text(578, y + 22, ts("100k partitions: ", mid) + ts(f"only Queen runs ({fmt_ms(tq['p99'])} p99)", t["queen"], 700),
           17, raw=True)
    c.text(578, y + 50, ts("Peak at 100 msgs/txn: ", mid) + ts(f"Pulsar {fmt_rate(peak['pulsar'])}", t["pulsar"], 700)
           + ts(", ", mid) + ts(f"Queen {fmt_rate(peak['queen'])}", t["queen"], 700) + ts(" msg/s", mid), 17, raw=True)

    # ---- footer
    for i, line in enumerate((
            "3 DigitalOcean brokers (16 vCPU, 31 GB, one disk each) · 256-byte messages · the same 3 load hosts for all",
            "Acked after 2 of 3 copies are fsynced; Kafka acks from page cache (no fsync)",
            "Kafka 4.3.1 · Redpanda 26.2.3 · Pulsar 4.2.4 · Queen 2.0, Kafka clients via franz-go · October 2026")):
        c.text(56, 1292 + i * 20, line, 14, 400, low)

    style = (f"@font-face{{font-family:'Inter';src:url(data:font/woff2;base64,{inter_b64}) format('woff2');"
             "font-weight:100 900;font-style:normal}"
             "text{font-family:'Inter',ui-sans-serif,system-ui,-apple-system,'Helvetica Neue',sans-serif;"
             "font-feature-settings:'cv11','ss01','ss03','cv02'}")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}">\n'
            f"<style>{style}</style>\n" + "\n".join(c.o) + "\n</svg>\n")


def png(svg_path, png_path):
    """Headless Chrome at 2x (the SVG's embedded Inter needs a browser), with a throwaway profile. The new headless
    mode can stay up after writing the screenshot, so wait for the file to settle and close it."""
    import time
    if os.path.exists(png_path):
        os.remove(png_path)
    with tempfile.TemporaryDirectory() as prof:
        pr = subprocess.Popen([CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--no-first-run",
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
    inter_b64 = base64.b64encode(open(INTER, "rb").read()).decode()
    badge_b64 = base64.b64encode(open(BADGE, "rb").read()).decode()
    for theme in ("dark", "light"):
        svg = os.path.join(OUTDIR, f"brokers-compact-{theme}.svg")
        open(svg, "w").write(render(theme, data, tq, inter_b64, badge_b64))
        png(svg, svg[:-4] + ".png")
        print(svg, svg[:-4] + ".png")


if __name__ == "__main__":
    main()
