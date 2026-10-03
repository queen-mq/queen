#!/usr/bin/env python3
"""matrix_svg.py: one SVG with every system (Kafka, Redpanda, Pulsar, Queen, Queen driven by Kafka clients) and every
metric (delivered msg/s, e2e p50/p99, CPU, memory, disk) for three tests: the throughput ceiling at 1 x 200 partitions
(column A), the partition scaling at 1M msg/s (column B) and the transactional consume-transform-produce (column C).
Columns keep their own x axis; each metric row keeps ONE y scale across the columns (except delivered), so a row
compares the tests directly.

Kafka/Redpanda/Pulsar and Queen + Kafka clients come from report/report.py (sys cores = the system's own processes
summed over the 3 nodes; RSS = the busiest node's; disk = mean per node). Redpanda's CPU is its shards' real work
(redpanda_cpu_busy_seconds, summary.txt): Seastar reactors busy-poll, so /proc counts idle polling as CPU. Queen comes
from the 09-30 harness (grid_report.py for the last-30-s rates and latencies; samples-n1..3.txt for CPU, RSS and disk,
aggregated the same way). Column C comes from report/report.py --txn (out/s, e2e = input scheduled send -> committed
output consumed). Usage: charts/matrix_svg.py [out.svg]"""
import csv, io, os, re, subprocess, sys, math

H = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
Q = os.path.join(os.path.dirname(H), "2026-09-30-stage03-3node")
OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(H, "charts", "four-brokers-matrix.svg")

SYS = ["kafka", "redpanda", "pulsar", "queen", "queenk"]
NAME = {"kafka": "Kafka 4.3.1", "redpanda": "Redpanda 26.2.3", "pulsar": "Pulsar 4.2.4", "queen": "Queen 2.0 beta.2",
        "queenk": "Queen 2.0 + Kafka clients"}
DUR = {"kafka": "RF 3, acks=all, page cache (no fsync)", "redpanda": "RF 3, ack after 2 of 3 fsync",
       "pulsar": "E3/Qw3/Qa2, ack after 2 journal fsync", "queen": "3 copies, ack after 2 fsync",
       "queenk": "same cluster, Kafka protocol, build qk3"}
COL = {"kafka": "#3b4656", "redpanda": "#d0362c", "pulsar": "#12808d", "queen": "#c98500", "queenk": "#7a3f8f"}

# column A: offered (M msg/s) -> run dir; column B: partitions -> run dir (1M msg/s offered)
A = {
    "kafka":    {r: f"runs/kafka/1x200-{t}" for r, t in [(0.3, "300k"), (0.6, "600k"), (1.0, "1000k"), (1.5, "1500k"), (2.0, "2000k"), (3.0, "3000k"), (4.0, "4000k")]},
    "redpanda": {r: f"runs/redpanda/1x200-{t}" for r, t in [(0.3, "300k"), (0.6, "600k"), (1.0, "1000k"), (1.5, "1500k"), (2.0, "2000k"), (3.0, "3000k"), (4.0, "4000k")]},
    "pulsar":   {r: f"runs/pulsar/pA-r{t}" for r, t in [(0.3, "300k"), (0.6, "600k"), (1.0, "1000k"), (1.5, "1500k"), (2.0, "2000k")]},
    "queen":    {r: f"mb2c-q1-t200-{t}" for r, t in [(0.3, "r300k"), (0.6, "r600k"), (1.0, "r1m"), (1.5, "r1500k"), (2.0, "r2m")]},
    "queenk":   {r: f"runs/queen-kafka/c3-1x200-{t}" for r, t in [(0.3, "300k"), (0.6, "600k"), (1.0, "1000k"), (1.5, "1500k"), (2.0, "2000k")]},
}
B = {
    "kafka":    {p: f"runs/kafka/1x{p}-1000k" for p in (200, 2000, 10000, 50000, 100000)},
    "redpanda": {p: f"runs/redpanda/1x{p}-1000k" for p in (200, 2000, 10000, 50000, 100000)},
    "pulsar":   {200: "runs/pulsar/pA-r1000k", 2000: "runs/pulsar/pB-p2000", 10000: "runs/pulsar/pB-p10000",
                 50000: "runs/pulsar/pB-p50000-nb", 100000: "runs/pulsar/pB-p100000-nb"},
    "queen":    {200: "mb2c-q1-t200-r1m", 10000: "mb2c-q1-t10k", 50000: "mb2c-q1-t50k", 100000: "mb2c-q1-t100k",
                 1000000: "mb2c-q1-t1m", 5000000: "mb2c-q1-t5m", 10000000: "mb2c-q1-t10m"},
    "queenk":   {200: "runs/queen-kafka/c3-1x200-1000k", 10000: "runs/queen-kafka/c3-1x10k-1000k",
                 50000: "runs/queen-kafka/c3-1x50k-1000k", 100000: "runs/queen-kafka/c3-1x100k-1000k"},
}
# column C: offered msg/s -> run dir under runs/txn/<system>/. TX = 10 messages per transaction, 200 partitions in and
# out, 198 workers (the C ramp); TX100 = 100 per transaction (feeder units of 100); TXL = more partitions and workers
TX = {
    "kafka":    {r: f"1x200-t10-{r // 1000}k" for r in (9000, 18000, 36000)},
    "redpanda": {r: f"1x200-t10-{r // 1000}k" for r in (9000, 18000, 36000)},
    "pulsar":   {r: f"1x200-t10-{r // 1000}k" for r in (9000, 18000, 36000, 72000, 144000)},
    "queen":    {r: f"1x200-t10-{r // 1000}k" for r in (9000, 18000, 36000, 72000, 144000)},
}
TX100 = {
    "pulsar":   {r: f"1x200-t100-{r // 1000}k-w198-b100" for r in (144000, 288000, 576000, 1152000)},
    "queen":    {r: f"1x200-t100-{r // 1000}k-w198-b100" for r in (144000, 288000)},
}
TXL = {
    "pulsar":   [(144000, "1x900-t10-144k-w900", "900"), (288000, "1x900-t10-288k-w900", "900"),
                 (288000, "1x1800-t10-288k-w1800", "1,800")],
    "queen":    [(144000, "1x900-t10-144k-w900", "900")],
}
FAILED_B = {"kafka": "cannot create 1M partitions", "pulsar": "1M partitions: setup failed after 31 min"}


def kn(v):  # "936k" -> 936000
    v = v.strip()
    return float(v[:-1]) * 1000 if v.endswith("k") else float(v)


def report_row(d):
    out = subprocess.run([sys.executable, os.path.join(H, "report", "report.py"), os.path.join(H, d)],
                         capture_output=True, text=True).stdout.splitlines()
    rows = [l for l in out[1:] if l.strip() and not l.startswith("latencies")]
    if not rows:
        return None
    f = rows[0].split()
    r = dict(offered=float(f[-15]), push=kn(f[-14]), cons=kn(f[-13]), p50=float(f[-11]), p99=float(f[-10]),
             cpu=float(f[-6]), rss=float(f[-5]), disk=float(f[-4]))
    summ = os.path.join(H, d, "summary.txt")
    if os.path.exists(summ):
        m = re.search(r"shards busy .*?sum=([\d.]+) cores", open(summ).read())
        if m:
            r["cpu"] = float(m[1])
    return r


def queen_rows(tags):
    out = subprocess.run([sys.executable, os.path.join(Q, "grid_report.py"), os.path.join(Q, "runs")] + tags,
                         capture_output=True, text=True).stdout.splitlines()
    res = {}
    for l in out[1:]:
        f = l.split()
        if not f or f[0] not in tags:
            continue
        # run QxP live pre | push cons | last30 push cons shed | p50 p99 pushp99 | ldr anon cache disk | errs
        bars = [i for i, x in enumerate(f) if x == "|"]
        last = f[bars[1] + 1: bars[2]]
        lat = f[bars[2] + 1: bars[3]]
        res[f[0]] = dict(push=kn(last[0]), cons=kn(last[1]), p50=float(lat[0]), p99=float(lat[1]))
    for t in tags:
        if t not in res:
            continue
        cores, rss, disk = 0.0, 0.0, []
        for n in (1, 2, 3):
            p = os.path.join(Q, "runs", t, f"samples-n{n}.txt")
            s = []
            if os.path.exists(p):
                for l in open(p):
                    dd = dict(re.findall(r"(\w[\w.-]*)=([\d.]+)", l))
                    if "cpu_ticks" in dd:
                        r = {k: float(v) for k, v in dd.items()}
                        r["t"] = float(l.split()[0])
                        s.append(r)
            if len(s) > 10:
                a, b = s[len(s) // 3], s[-2]
                dt = b["t"] - a["t"]
                cores += (b["cpu_ticks"] - a["cpu_ticks"]) / 100 / dt
                disk.append((b.get("disk_wsect", 0) - a.get("disk_wsect", 0)) * 512 / dt / 1e6)
                rss = max(rss, max(x.get("RssAnon_kb", 0) + x.get("RssFile_kb", 0) for x in s) / 1048576)
        res[t].update(cpu=cores, rss=rss, disk=sum(disk) / len(disk) if disk else 0)
    return res


def txn_rows(dirs):
    """report.py --txn --csv over run dirs -> {dir: row}; e2e = input scheduled send -> committed output consumed."""
    out = subprocess.run([sys.executable, os.path.join(H, "report", "report.py"), "--txn", "--csv"]
                         + [os.path.join(H, d) for d in dirs], capture_output=True, text=True).stdout
    res = {}
    for row in csv.DictReader(io.StringIO(out)):
        key = f"runs/txn/{row['sys']}/{row['run']}"
        res[key] = dict(offered=float(row["offered"]), cons=kn(row["out/s"]), p50=float(row["e2e p50"]),
                        p99=float(row["e2e p99"]), cpu=float(row["cores"]), rss=float(row["rss GB/node"]),
                        disk=float(row["disk w MB/s/node"]), txns=float(row["txn/s"]))
    return res


def behind(r):
    return r["cons"] < 0.97 * r["offered"] or r["p99"] > 5000


def collect():
    data = {"A": {}, "B": {}, "C": {}, "C100": {}, "CL": {}}
    qtags = sorted(set(A["queen"].values()) | set(B["queen"].values()))
    qr = queen_rows(qtags)
    for col, spec in (("A", A), ("B", B)):
        for s in SYS:
            pts = []
            for x, d in sorted(spec[s].items()):
                if s == "queen":
                    r = dict(qr.get(d) or {})
                    if not r:
                        continue
                    r["offered"] = x * 1e6 if col == "A" else 1e6
                else:
                    r = report_row(d)
                    if not r:
                        continue
                r["behind"] = behind(r)
                r["x"] = x
                pts.append(r)
            data[col][s] = pts
    tdirs = [f"runs/txn/{s}/{t}" for s, m in TX.items() for t in m.values()]
    tdirs += [f"runs/txn/{s}/{t}" for s, m in TX100.items() for t in m.values()]
    tdirs += [f"runs/txn/{s}/{t}" for s, l in TXL.items() for _, t, _ in l]
    tr = txn_rows(tdirs)
    for key, spec in (("C", TX), ("C100", TX100)):
        for s, m in spec.items():
            pts = []
            for x, t in sorted(m.items()):
                r = tr.get(f"runs/txn/{s}/{t}")
                if r:
                    r = dict(r, x=x)
                    r["behind"] = behind(r)
                    pts.append(r)
            data[key][s] = pts
    for s, l in TXL.items():
        pts = []
        for x, t, lab in l:
            r = tr.get(f"runs/txn/{s}/{t}")
            if r:
                r = dict(r, x=x, lab=lab)
                r["behind"] = behind(r)
                pts.append(r)
        data["CL"][s] = pts
    return data


# ---------------------------------------------------------------- SVG
def esc(t):
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class Svg:
    def __init__(self, w, h):
        self.w, self.h, self.o = w, h, []

    def add(self, s):
        self.o.append(s)

    def text(self, x, y, t, cls="", anchor="start", extra=""):
        self.add(f'<text x="{x:.1f}" y="{y:.1f}" class="{cls}" text-anchor="{anchor}" {extra}>{esc(t)}</text>')

    def line(self, x1, y1, x2, y2, cls="", extra=""):
        self.add(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" class="{cls}" {extra}/>')

    def render(self):
        css = """
  .bg{fill:#ffffff} .ink{fill:#141820} .mut{fill:#5a6474} .faint{fill:#8a93a1}
  text{font-family:'IBM Plex Sans','Helvetica Neue',Helvetica,Arial,sans-serif;font-size:12px}
  .title{font-size:30px;font-weight:700;fill:#141820;letter-spacing:-0.3px}
  .sub{font-size:14px;fill:#5a6474}
  .colh{font-size:17px;font-weight:700;fill:#141820} .colsub{font-size:12.5px;fill:#5a6474}
  .rowh{font-size:15px;font-weight:700;fill:#141820} .rowsub{font-size:11.5px;fill:#5a6474}
  .tick{font-family:'IBM Plex Mono',Menlo,Consolas,monospace;font-size:10.5px;fill:#6b7484}
  .axl{font-size:11px;fill:#5a6474}
  .note{font-size:11.5px;fill:#5a6474} .noteb{font-size:11.5px;fill:#141820;font-weight:600}
  .grid{stroke:#e8ecf1;stroke-width:1} .axis{stroke:#c9d0da;stroke-width:1}
  .ref{stroke:#8a93a1;stroke-width:1;stroke-dasharray:4 4}
  .frame{fill:#fbfcfd;stroke:#e1e6ed;stroke-width:1}
  .leg{font-size:13px;font-weight:600;fill:#141820} .legd{font-size:11.5px;fill:#5a6474}
  .ptl{font-size:10px;font-weight:600}
"""
        return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" width="{self.w}" height="{self.h}">'
                f"<style>{css}</style><rect class=\"bg\" x=\"0\" y=\"0\" width=\"{self.w}\" height=\"{self.h}\"/>"
                + "\n".join(self.o) + "</svg>\n")


def scaler(kind, d0, d1, r0, r1):
    if kind == "log":
        a, b = math.log10(d0), math.log10(d1)
        return lambda v: r0 + (math.log10(v) - a) / (b - a) * (r1 - r0)
    return lambda v: r0 + (v - d0) / (d1 - d0) * (r1 - r0)


def fmt_m(v):
    return "0" if v == 0 else (f"{v/1e6:g}M" if v >= 1e6 else f"{v/1e3:g}k")


def path(xy, color, width, extra=""):
    return ('<path d="' + " ".join(("M" if i == 0 else "L") + f"{a:.1f},{b:.1f}" for i, (a, b) in enumerate(xy))
            + f'" fill="none" stroke="{color}" stroke-width="{width}" stroke-linejoin="round" stroke-linecap="round" {extra}/>')


def marker(g, shape, a, b, color, hollow):
    fill, sw = ("#ffffff", 2) if hollow else (color, 0)
    stroke = f' stroke="{color}" stroke-width="{sw}"' if hollow else ""
    if shape == "circle":
        g.add(f'<circle cx="{a:.1f}" cy="{b:.1f}" r="{4.3 if hollow else 3.8}" fill="{fill}"{stroke}/>')
    elif shape == "square":
        h = 4.2 if hollow else 4.0
        g.add(f'<rect x="{a - h:.1f}" y="{b - h:.1f}" width="{2 * h:.1f}" height="{2 * h:.1f}" fill="{fill}"{stroke}/>')
    else:  # diamond
        h = 5.6 if hollow else 5.4
        g.add(f'<path d="M{a:.1f},{b - h:.1f} L{a + h:.1f},{b:.1f} L{a:.1f},{b + h:.1f} L{a - h:.1f},{b:.1f} Z" fill="{fill}"{stroke}/>')


HB = {**{c: 556 for c in "0123456789abcdeghknopqsuvxy"}, **{c: 611 for c in "bdghnopqu"},
      " ": 278, "f": 333, "i": 278, "j": 278, "l": 278, "m": 889, "r": 389, "t": 333, "w": 778, "z": 500,
      "A": 722, "B": 722, "C": 722, "D": 722, "E": 667, "F": 611, "G": 778, "H": 722, "I": 278, "J": 556, "K": 722,
      "L": 611, "M": 833, "N": 722, "O": 778, "P": 667, "Q": 778, "R": 722, "S": 667, "T": 611, "U": 722, "V": 667,
      "W": 944, "X": 667, "Y": 667, "Z": 611, ".": 278, ",": 278, ":": 333, "/": 278, "–": 556, "≥": 549, "×": 584,
      "+": 584, "~": 584, "(": 333, ")": 333, "-": 333}


def tw(t, size=11.5):
    """Approximate width of bold text (Helvetica-Bold advance widths)."""
    return sum(HB.get(c, 600) for c in t) / 1000 * size


def inline(g, x, y, parts, cls="noteb", gap=12):
    """A run of (text, system-or-None) on one line; colored when a system is given. Returns the end x."""
    for t, s in parts:
        style = f' style="fill:{COL[s]}"' if s else ""
        g.add(f'<text x="{x:.1f}" y="{y:.1f}" class="{cls}"{style}>{esc(t)}</text>')
        x += tw(t) + gap
    return x


def main():
    data = collect()
    W, Hh = 1660, 1900
    g = Svg(W, Hh)
    LBL = 24                      # row labels x
    PA = (250, 650)               # plot x ranges
    PB = (740, 1140)
    PC = (1230, 1630)
    top0 = 318                    # first row's plot top
    PH, RG = 190, 74              # plot height, gap between rows
    ROWS = [
        ("Delivered", "messages consumed per second (C: committed output)", "delivered"),
        ("End-to-end latency", "p99 solid, p50 dashed, log scale", "lat"),
        ("Broker CPU", "cores, system processes, 3 nodes summed", "cpu"),
        ("Broker memory", "GB resident on the busiest node", "rss"),
        ("Disk writes", "MB/s per node, mean of the 3", "disk"),
    ]
    xa = scaler("log", 0.25, 4.6, *PA)
    xb = scaler("log", 120, 16e6, *PB)
    xc = scaler("log", 7000, 1.5e6, *PC)
    yspec = {
        "lat": ("log", 5, 1e5, [(10, "10 ms"), (100, "100 ms"), (1000, "1 s"), (1e4, "10 s"), (1e5, "100 s")]),
        "cpu": ("lin", 0, 52, [(0, "0"), (10, "10"), (20, "20"), (30, "30"), (40, "40"), (50, "50")]),
        "rss": ("lin", 0, 28, [(0, "0"), (5, "5"), (10, "10"), (15, "15"), (20, "20"), (25, "25")]),
        "disk": ("lin", 0, 800, [(0, "0"), (200, "200"), (400, "400"), (600, "600"), (800, "800")]),
    }

    # title + legend
    g.text(LBL, 46, "Four brokers on one rig", "title")
    g.text(LBL, 72, "Kafka, Redpanda, Pulsar and Queen on the same 3 DigitalOcean brokers (16 vCPU, 31 GB, one ext4 disk each), "
           "256-byte JSON messages, last 30 s of 60 to 70 s points. Queen also driven by Kafka clients.", "sub")
    lx = [LBL, LBL + 322, LBL + 644, LBL + 966, LBL + 1288]
    for i, s in enumerate(SYS):
        g.add(f'<rect x="{lx[i]}" y="94" width="14" height="14" rx="2" fill="{COL[s]}"/>')
        g.text(lx[i] + 22, 106, NAME[s], "leg")
        g.text(lx[i] + 22, 124, DUR[s], "legd")
    mk = "#5a6474"
    marker(g, "circle", LBL + 7, 147, mk, False)
    marker(g, "circle", LBL + 21, 147, mk, True)
    g.text(LBL + 32, 151, "filled: kept up · hollow: fell behind (consumed under 97% of offered, or p99 above 5 s)", "note")
    marker(g, "square", LBL + 644 + 7, 147, mk, False)
    g.text(LBL + 644 + 18, 151, "C: 100 messages per transaction", "note")
    marker(g, "diamond", LBL + 966 + 7, 147, mk, False)
    g.text(LBL + 966 + 18, 151, "C: more partitions, one worker each (900 / 1,800)", "note")
    g.text(LBL + 1288 + 2, 151, "×  could not run at that size", "note")

    # column headers
    g.text(PA[0], 198, "A · Throughput ceiling", "colh")
    g.text(PA[0], 216, "1 topic or queue × 200 partitions, offered rate raised", "colsub")
    g.text(PB[0], 198, "B · Partition scaling", "colh")
    g.text(PB[0], 216, "1 topic or queue at 1M msg/s offered, partitions raised", "colsub")
    g.text(PC[0], 198, "C · Transactions", "colh")
    g.text(PC[0], 216, "exactly-once read → transform → write, 200 partitions", "colsub")
    # A: ceilings
    inline(g, PA[0], 240, [("Ceiling:", None), ("Kafka ≥ 4M", "kafka"), ("Redpanda 2–3M", "redpanda"), ("Pulsar ≥ 2M", "pulsar")])
    inline(g, PA[0], 258, [("Queen 1–1.5M", "queen"), ("Queen + Kafka clients 1.5–2M", "queenk")])
    g.text(PA[0], 276, "Above ~1.4M: Kafka ≥ 2.9×, Redpanda 1.4–2.1×, Pulsar ≥ 1.4× Queen", "note")
    # B: p99 ratios at 100k
    g.text(PB[0], 240, "At 100k partitions, p99 is higher than Queen's by:", "noteb")
    inline(g, PB[0], 258, [("Kafka 67×", "kafka"), ("Redpanda 290×", "redpanda"), ("Pulsar 290×", "pulsar"),
                           ("Queen + Kafka clients 1.5×", "queenk")])
    g.text(PB[0], 276, "Only Queen runs from 1M partitions.", "note")
    # C: ceilings
    inline(g, PC[0], 240, [("Ceiling at 10/txn:", None), ("Kafka 9–18k", "kafka"), ("Redpanda 9–18k", "redpanda"),
                           ("Pulsar 36–72k", "pulsar")])
    inline(g, PC[0], 258, [("Queen 72–144k", "queen"), ("at 100/txn:", None), ("Pulsar 288–576k", "pulsar"),
                           ("Queen 144–288k", "queen")])
    g.text(PC[0], 276, "Most delivered at 100/txn: Pulsar 572k, Queen 251k.", "note")
    g.text(PC[0], 292, "Below its ceiling, Queen has the lowest latency.", "note")

    for ri, (title, sub, key) in enumerate(ROWS):
        top = top0 + ri * (PH + RG)
        bot = top + PH
        g.text(LBL, top + 16, title, "rowh")
        # wrap the subtitle on two lines if long
        words, line_, lines = sub.split(), "", []
        for w_ in words:
            if len(line_) + len(w_) > 26:
                lines.append(line_.strip()); line_ = ""
            line_ += w_ + " "
        lines.append(line_.strip())
        for li, l in enumerate(lines):
            g.text(LBL, top + 34 + li * 15, l, "rowsub")

        for col, (x0, x1), xs in (("A", PA, xa), ("B", PB, xb), ("C", PC, xc)):
            g.add(f'<rect class="frame" x="{x0 - 8}" y="{top - 8}" width="{x1 - x0 + 16}" height="{PH + 16}" rx="4"/>')
            if key == "delivered":
                if col == "A":
                    ys = scaler("log", 0.25e6, 4.6e6, bot, top)
                    yt = [(v * 1e6, fmt_m(v * 1e6)) for v in (0.3, 0.6, 1, 2, 4)]
                elif col == "B":
                    ys = scaler("lin", 0, 1.25e6, bot, top)
                    yt = [(v, fmt_m(v)) for v in (0, 250e3, 500e3, 750e3, 1e6)]
                else:
                    ys = scaler("log", 7000, 1.5e6, bot, top)
                    yt = [(v, fmt_m(v)) for v in (1e4, 3e4, 1e5, 3e5, 1e6)]
            else:
                kind, d0, d1, yt = yspec[key]
                ys = scaler(kind, d0, d1, bot, top)
            for v, lab in yt:
                g.line(x0, ys(v), x1, ys(v), "grid")
                g.text(x0 - 8, ys(v) + 3.5, lab, "tick", "end")
            g.line(x0, bot, x1, bot, "axis")
            # x ticks
            if col == "A":
                xt = [(0.3, "300k"), (0.6, "600k"), (1, "1M"), (2, "2M"), (3, "3M"), (4, "4M")]
            elif col == "B":
                xt = [(200, "200"), (2000, "2k"), (10000, "10k"), (100000, "100k"), (1e6, "1M"), (1e7, "10M")]
            else:
                xt = [(9000, "9k"), (18000, ""), (36000, "36k"), (72000, ""), (144000, "144k"), (288000, ""),
                      (576000, "576k"), (1152000, "1.15M")]
            for v, lab in xt:
                g.line(xs(v), bot, xs(v), bot + 4, "axis")
                if lab:
                    g.text(xs(v), bot + 17, lab, "tick", "middle")
            if ri == len(ROWS) - 1:
                g.text((x0 + x1) / 2, bot + 36, "partitions (log scale)" if col == "B" else "offered rate (msg/s, log scale)",
                       "axl", "middle")
            # reference lines
            if key == "delivered":
                if col == "A":
                    g.line(xs(0.25), ys(0.25e6), xs(4.6), ys(4.6e6), "ref")
                elif col == "B":
                    g.line(x0, ys(1e6), x1, ys(1e6), "ref")
                    g.text(x1 - 2, ys(1e6) - 5, "1M offered", "tick", "end")
                else:
                    g.line(xs(7000), ys(7000), xs(1.5e6), ys(1.5e6), "ref")

            lo = {"A": 0.25e6, "B": 0, "C": 7000}[col]

            def val(p):
                if key == "delivered":
                    return max(p["cons"], lo)
                if key == "lat":
                    return min(max(p["p99"], 5), 1e5)
                return min(p[key], {"cpu": 52, "rss": 28, "disk": 800}.get(key, 1e18))

            def series(pts, color, shape, width):
                xy = [(xs(p["x"]), ys(val(p))) for p in pts]
                if len(xy) > 1:
                    g.add(path(xy, color, width))
                if key == "lat" and len(pts) > 1:
                    xy50 = [(xs(p["x"]), ys(min(max(p["p50"], 5), 1e5))) for p in pts]
                    g.add(path(xy50, color, 1.4, 'stroke-dasharray="3 3" opacity="0.85"'))
                for p, (a, b) in zip(pts, xy):
                    marker(g, shape, a, b, color, p.get("behind"))
                    if key == "delivered" and lo and p["cons"] < lo:
                        g.add(f'<text x="{a + 8:.1f}" y="{b - 6:.1f}" class="ptl" style="fill:{color}">{fmt_m(round(p["cons"], -3))}</text>')
                return xy

            if col in ("A", "B"):
                for s in SYS:
                    series(data[col].get(s) or [], COL[s], "circle", 2.2)
            else:
                for s in SYS:
                    series(data["C"].get(s) or [], COL[s], "circle", 2.2)
                for s in ("pulsar", "queen"):
                    series(data["C100"].get(s) or [], COL[s], "square", 1.8)
                for s in ("pulsar", "queen"):
                    for p in data["CL"].get(s) or []:
                        a, b = xs(p["x"]), ys(val(p))
                        marker(g, "diamond", a, b, COL[s], p.get("behind"))
                        if key == "delivered":
                            g.add(f'<text x="{a + 8:.1f}" y="{b + 3.5:.1f}" class="ptl" style="fill:{COL[s]}">{esc(p["lab"])}</text>')
                if key == "delivered":
                    # label the 100-per-transaction lines at their right end
                    for s in ("pulsar", "queen"):
                        pts = data["C100"].get(s) or []
                        if pts:
                            p = pts[-1]
                            a, b = xs(p["x"]), ys(val(p))
                            g.add(f'<text x="{a - 8:.1f}" y="{b + (16 if s == "pulsar" else -9):.1f}" class="ptl" text-anchor="end" '
                                  f'style="fill:{COL[s]}">100/txn</text>')
            # failures at 1M partitions: column B, delivered row. A small x on the floor at 1M (they delivered
            # nothing there), the explanation in the free space under the panel.
            if col == "B" and key == "delivered":
                for k, s_ in enumerate(("kafka", "pulsar")):
                    g.add(f'<text x="{xs(1e6) + (k * 12 - 6):.1f}" y="{bot - 4:.1f}" text-anchor="middle" style="font-size:14px;font-weight:700;fill:{COL[s_]}">×</text>')
                g.add(f'<text x="{x0:.1f}" y="{bot + 36:.1f}" class="note">At 1M partitions: '
                      f'<tspan style="fill:{COL["kafka"]};font-weight:600">× Kafka cannot create them</tspan>, '
                      f'<tspan style="fill:{COL["pulsar"]};font-weight:600">× Pulsar setup failed after 31 min</tspan>.</text>')
            if col == "C" and key == "delivered":
                g.add(f'<text x="{x0:.1f}" y="{bot + 36:.1f}" class="note">Kafka, Redpanda: 100/txn and more partitions not run.</text>')

    # footnotes
    fy = top0 + len(ROWS) * (PH + RG) + 6
    notes = [
        "Durability: Redpanda, Pulsar and Queen acknowledge after two of three copies are fsynced; Kafka acknowledges from page cache on three replicas.",
        "All x axes are log scales, so equal distances are equal ratios. Ceiling ≥: still keeping up at the highest rate offered. B's ratios use p99 at 100k partitions (Queen 163 ms).",
        "CPU: the system's own processes summed over the 3 brokers (48 vCPU in all). Redpanda shows its shards' real work (its reactors busy-poll, so the OS counts more).",
        "Load: Kafka, Redpanda, Pulsar and Queen + Kafka clients used 3 load hosts (9 processes); Queen used 4. Queen: pops of up to 10 partitions, dedup off. Redpanda: classic consumer groups.",
        "Queen + Kafka clients: the same 3-node Queen cluster driven by the Kafka loader (franz-go, acks=all, idempotent, lz4, classic groups); build qk3 from the Kafka-facade performance branch (uncommitted).",
        "C: every transaction acks its inputs and publishes its outputs atomically; a separate verifier found every input exactly once in the output on every point. 198 workers; feeder batches of 10 (100 for ■).",
        "C: one transaction per partition at a time in all four. Queen plans every transaction on one thread (its ceiling); Pulsar spreads them over many threads and scales with partitions and workers.",
        "Not run: Pulsar above 2M msg/s; Queen at 2k partitions; Kafka, Redpanda and Pulsar above 100k partitions except the two 1M attempts marked ×; transactions over Kafka clients on Queen.",
        "Fell behind: Redpanda at 3M/4M and from 50k partitions; Pulsar from 50k partitions; Kafka at 100k partitions; Queen at 1.5M/2M msg/s; Queen + Kafka clients at 2M.",
        "Kafka ran on 30 Sep 2026; Redpanda, Pulsar, Queen (2.0.0-beta.2) and the transactions on 1 Oct 2026; Queen + Kafka clients on 1–2 Oct 2026.",
    ]
    for i, n in enumerate(notes):
        g.text(LBL, fy + i * 18, n, "note")
    open(OUT, "w").write(g.render())
    print(OUT)
    for col in ("A", "B", "C", "C100", "CL"):
        for s in SYS:
            for p in data[col].get(s, []):
                print(col, s, p["x"], f"cons={p['cons']:.0f} p50={p['p50']:.0f} p99={p['p99']:.0f} cpu={p['cpu']:.1f} rss={p['rss']:.1f} disk={p['disk']:.0f} behind={p['behind']}")


if __name__ == "__main__":
    main()
