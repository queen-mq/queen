#!/usr/bin/env python3
"""The docs' picture: the queen bee flying to the sunflower.

Drawn the way the logo (assets/queen_final.svg) is drawn: flat yellow shapes, a
gap between each shape and the next, no contour. A gap is the page itself. On a
dark page the dark parts (the flower's head, the bee's bands and eye) are the
page too; on a light page they are ink. Every cut is real geometry, made by
assets/pathops.swift, so a file has no mask and stands on any ground.

  the flower   Two rows of PETALS petals, round at the base and pointed, the
               back row half a step round and a little longer, so its tips
               show between the front ones.
               Each petal lies over the next one round, a GAP between them, and
               each is a little off in length and angle, the same on every run.
  the head     Dark, with SEEDS yellow seeds on Vogel's spiral (one every
               golden angle), growing from the centre outward.
  the stem     One band from behind the flower out of the bottom edge, wider
               at the foot.
  the leaves   Sunflower leaves on short stalks, drawn as a line: an outline,
               a midrib and three pairs of veins. The flower and the bee are
               the only things with weight.
  the bee      The queen: head, thorax and a long banded abdomen in the
               yellow, each cut from the next; her crown on her head; wings,
               antennae and legs as lines in the body colour.
  her trail    Dots along her way up from the field, growing toward her.

One file per theme, the lines in the theme's ink:
  webdoc/public/queen-scene-on-light.svg   ink, for a light page
  webdoc/public/queen-scene-on-dark.svg    paper, for a dark page

Run:  python3 assets/scene.py     (needs swift, for assets/pathops.swift)
"""
import json
import math
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import logo as L  # noqa: E402

# the yellow is the dashboard's --brand and the docs' --nb-brand
PETAL, INK, PAPER = L.YELLOW, L.INK, L.PAPER
WIDTH, HEIGHT = 1200, 1536
GAP = 8.0                               # between two shapes: the page shows through
GOLDEN = 137.50776405003785             # degrees

CENTRE, HEAD = (700.0, 632.0), 148.0    # the flower's centre, and the radius of its head
PETALS = 21                             # in each row
FRONT = (0.92, 2.20, 0.235)             # a petal's base, tip and half width, in head radii
BACK = (0.92, 2.44, 0.205)
PETAL_SHAPE = (0.62, 0.03, 0.10)        # blade's p (how round the base), bend and fuller
UNEVEN = (0.035, 1.5)                   # how far a petal is off: in length (a fraction) and in angle (degrees)
SEEDS = 150
SEED_FIELD = 0.86                       # the seeds fill the head out to here, in head radii
SEED_GROWTH = 0.42                      # how much smaller than the mean at the centre, and larger at the rim
SEED_SIZE = 0.42                        # a seed's radius, in spacings between seeds

STEM = ((694.0, 692.0), (678.0, 956.0), (636.0, 1190.0), (622.0, 1356.0), (638.0, 1620.0))
STEM_WIDTH = (22.0, 36.0)               # at the top and at the foot
# where on the stem (0 to 1), the way it points (degrees clockwise from up), stalk, length, half width, droop
LEAVES = ((0.44, -96.0, 64.0, 290.0, 92.0, 0.20), (0.69, 100.0, 58.0, 262.0, 84.0, 0.18))

BEE_AT, HEADING, BEE_SCALE = (404.0, 238.0), 112.0, 0.80
# each wing: where it starts on her, how far back from her heading it points (degrees), length, half width,
# and blade's bend and fuller
WINGS = (((16.0, -42.0), 118.0, 152.0, 35.0, -0.04, 0.10), ((-10.0, -46.0), 150.0, 118.0, 27.0, -0.03, 0.08))
WING_LINE = 8.0                         # in her own units, like everything inside bee()
CROWN_LEAN = -6.0                       # degrees, on the page

TRAIL = ((340.0, 1220.0), (244.0, 1084.0), (210.0, 924.0), (250.0, 764.0), (252.0, 624.0), (214.0, 498.0),
         (186.0, 380.0), (176.0, 312.0))
TRAIL_DOTS = 40


def num(v):
    s = f"{v:.2f}".rstrip("0").rstrip(".")
    return "0" if s == "-0" else s


# Paths, written the way pathops.swift reads them: M, L, C and Z only, absolute.

def circle(c, r):
    cx, cy = c
    k = 0.5522847498 * r
    p = [(cx + r, cy), (cx + r, cy + k), (cx + k, cy + r), (cx, cy + r), (cx - k, cy + r), (cx - r, cy + k),
         (cx - r, cy), (cx - r, cy - k), (cx - k, cy - r), (cx, cy - r), (cx + k, cy - r), (cx + r, cy - k), (cx + r, cy)]
    return (f"M {num(p[0][0])} {num(p[0][1])} "
            + " ".join("C " + " ".join(f"{num(x)} {num(y)}" for x, y in p[i:i + 3]) for i in range(1, 13, 3)) + " Z")


def controls(p0, p1, p2, p3):
    """The cubic's control points for the piece p1 to p2 of a Catmull-Rom curve."""
    return ((p1[0] + (p2[0] - p0[0]) / 6, p1[1] + (p2[1] - p0[1]) / 6),
            (p2[0] - (p3[0] - p1[0]) / 6, p2[1] - (p3[1] - p1[1]) / 6))


def pieces(pts, closed=False):
    """(p1, c1, c2, p2) for each piece of the smooth curve through pts. An open curve leaves each
    end straight toward the next point."""
    n = len(pts)
    if closed:
        return [(pts[i], *controls(pts[i - 1], pts[i], pts[(i + 1) % n], pts[(i + 2) % n]), pts[(i + 1) % n])
                for i in range(n)]
    first = (2 * pts[0][0] - pts[1][0], 2 * pts[0][1] - pts[1][1])
    last = (2 * pts[-1][0] - pts[-2][0], 2 * pts[-1][1] - pts[-2][1])
    return [(pts[i], *controls(pts[i - 1] if i else first, pts[i], pts[i + 1], pts[i + 2] if i + 2 < n else last),
             pts[i + 1]) for i in range(n - 1)]


def cubics(ps):
    return "".join(f" C {num(a[0])} {num(a[1])} {num(b[0])} {num(b[1])} {num(q[0])} {num(q[1])}" for _, a, b, q in ps)


def corners(*runs):
    """A closed shape of smooth runs that meet at sharp corners: each run starts where the one
    before it ended, and the last ends where the first began."""
    return f"M {num(runs[0][0][0])} {num(runs[0][0][1])}" + "".join(cubics(pieces(r)) for r in runs) + " Z"


def ellipse(c, rx, ry, turn, n=20):
    a = math.radians(turn)
    pts = [(c[0] + rx * math.cos(t) * math.cos(a) - ry * math.sin(t) * math.sin(a),
            c[1] + rx * math.cos(t) * math.sin(a) + ry * math.sin(t) * math.cos(a))
           for t in (2 * math.pi * i / n for i in range(n))]
    return f"M {num(pts[0][0])} {num(pts[0][1])}" + cubics(pieces(pts, closed=True)) + " Z"


def along(pts, n):
    """n points along the smooth curve through pts, about evenly spaced in its parameter."""
    out, k = [], max(2, n // (len(pts) - 1))
    for p1, c1, c2, p2 in pieces(pts):
        for j in range(k):
            t = j / k
            u = 1 - t
            out.append(tuple(u ** 3 * a + 3 * u * u * t * b + 3 * u * t * t * c + t ** 3 * d
                             for a, b, c, d in zip(p1, c1, c2, p2)))
    return out + [tuple(pts[-1])]


def evenly(pts, n):
    """n points spaced evenly by length along the smooth curve through pts, from end to end."""
    fine = along(pts, 2000)
    acc = [0.0]
    for (x0, y0), (x1, y1) in zip(fine, fine[1:]):
        acc.append(acc[-1] + math.hypot(x1 - x0, y1 - y0))
    out, j = [], 0
    for k in range(n):
        while j < len(acc) - 1 and acc[j] < acc[-1] * k / (n - 1):
            j += 1
        out.append(fine[j])
    return out


def band(centre, w0, w1, step=16.0):
    """A band along the points `centre`: w0 wide at the start, w1 at the end, square at both ends.
    Its edges keep a point every `step` along it, about: enough for a smooth curve through them."""
    left, right, n = [], [], len(centre)
    for i, (x, y) in enumerate(centre):
        a, b = centre[max(0, i - 1)], centre[min(n - 1, i + 1)]
        d = math.hypot(b[0] - a[0], b[1] - a[1]) or 1
        nx, ny = -(b[1] - a[1]) / d, (b[0] - a[0]) / d
        w = (w0 + (w1 - w0) * i / (n - 1)) / 2
        left.append((x + nx * w, y + ny * w))
        right.append((x - nx * w, y - ny * w))
    keep, run = [0], 0.0
    for i in range(1, n):
        run += math.hypot(centre[i][0] - centre[i - 1][0], centre[i][1] - centre[i - 1][1])
        if run >= step * len(keep) or i == n - 1:
            keep.append(i)
    left, right = [left[i] for i in keep], [right[i] for i in keep]
    return corners(left, [left[-1], right[-1]], right[::-1], [right[0], left[0]])


def polar(c, r, deg):
    """The point r from c, deg clockwise from straight up."""
    a = math.radians(deg)
    return (c[0] + r * math.sin(a), c[1] - r * math.cos(a))


def frame(origin, deg, scale=1.0):
    """From a local frame to the page: local x points deg clockwise from straight up, local y to
    its right, and local units are `scale` page units."""
    a = math.radians(deg)
    (dx, dy), (nx, ny) = (math.sin(a), -math.cos(a)), (math.cos(a), math.sin(a))
    return lambda p: (origin[0] + scale * (dx * p[0] + nx * p[1]), origin[1] + scale * (dy * p[0] + ny * p[1]))


def blade(to, length, half, p, q, bend=0.0, fuller=0.0, n=18):
    """A blade along local x from 0 to `length`: widest at p / (p + q) of its length, round at
    its base (p < 1), pointed at its tip (q = 1) or round there too (q < 1). `bend` swings the
    tip sideways, in lengths; `fuller` widens one side and narrows the other. `to` places it on
    the page. Gives its two edges, base to tip."""
    widest = p / (p + q)
    k = 1 / (widest ** p * (1 - widest) ** q)
    up, lo = [], []
    for i in range(n + 1):
        s = (1 - math.cos(math.pi * i / n)) / 2         # points close together at both ends
        w = half * k * s ** p * (1 - s) ** q
        mid = bend * length * s * s
        up.append(to((length * s, mid - w * (1 + fuller))))
        lo.append(to((length * s, mid + w * (1 - fuller))))
    return up, lo


class Program:
    """Steps for assets/pathops.swift on named shapes, and the shapes to give back."""

    def __init__(self):
        self.steps, self.out, self.k = [], [], 0

    def path(self, name, d):
        self.steps.append(["path", name, d])
        return name

    def union(self, name, *names):
        self.steps.append(["union", name, *names])
        return name

    def minus(self, name, a, *cuts):
        """a without each of cuts."""
        for i, b in enumerate(cuts):
            self.k += 1
            to = name if i == len(cuts) - 1 else f"_{self.k}"
            self.steps.append(["minus", to, a, b])
            a = to
        return name

    def both(self, name, a, b):
        self.steps.append(["both", name, a, b])
        return name

    def grow(self, name, a, d):
        self.steps.append(["grow", name, a, d])
        return name

    def line(self, name, pts, w0, w1=None):
        """A line along the smooth curve through pts, w0 wide at its start and w1 at its end, round
        at both ends: a thin band, grown."""
        w1 = w0 if w1 is None else w1
        cap = min(w0, w1) / 2 - 0.2
        self.path(name + "_", band(along(pts, 120), w0 - 2 * cap, w1 - 2 * cap))
        return self.grow(name, name + "_", cap)

    def run(self, *out):
        done = subprocess.run(["swift", os.path.join(HERE, "pathops.swift")],
                              input=json.dumps([{"steps": self.steps, "out": list(out)}]).encode(), capture_output=True)
        if done.returncode:
            raise SystemExit(done.stderr.decode())
        return dict(zip(out, json.loads(done.stdout)[0]))


def flower(prog):
    """Gives "petals" (what shows of them), "disc" (the head) and "flower" (all of it), and the seeds."""
    p, bend, fuller = PETAL_SHAPE
    rows = {}
    for row, turn, (base, tip, half) in (("front", 0.0, FRONT), ("back", 0.5, BACK)):
        rows[row] = []
        for i in range(PETALS):
            # the unevenness: lengths and angles off by a little, the same on every run
            deg = (i + turn) * 360.0 / PETALS + UNEVEN[1] * math.sin(i * 1.618 + 2.1)
            r0 = base * HEAD
            r1 = tip * HEAD * (1 + UNEVEN[0] * math.sin(i * 2.399 + (0.7 if row == "back" else 0.0)))
            up, lo = blade(frame(polar(CENTRE, r0, deg), deg), r1 - r0, half * HEAD, p, 1.0, bend, fuller, 10)
            rows[row].append(prog.path(f"{row}{i}", corners(up, lo[::-1])))
    prog.path("disc", circle(CENTRE, HEAD))
    prog.grow("disc+", "disc", GAP)
    prog.union("front", *rows["front"])
    prog.grow("front+", "front", GAP)
    shown = []
    for row in ("front", "back"):
        names = rows[row]
        for i, name in enumerate(names):
            # each petal lies over the next one round, all the way round, so there is no seam
            prog.grow(name + "+", names[(i + 1) % PETALS], GAP)
            shown.append(prog.minus(name + "-", name, name + "+", "disc+", *(["front+"] if row == "back" else [])))
    prog.union("petals", *shown)
    prog.union("flower", "disc", *rows["front"], *rows["back"])
    # seed k stands k golden angles round, at a distance that grows as the root of k
    step = SEED_FIELD * HEAD / math.sqrt(SEEDS)
    seeds = []
    for k in range(SEEDS):
        r = step * math.sqrt(k + 0.5)
        size = SEED_SIZE * step * (1 - SEED_GROWTH + 2 * SEED_GROWTH * r / (SEED_FIELD * HEAD))
        seeds.append((polar(CENTRE, r, k * GOLDEN), size))
    return seeds


def leaf(prog, name, base, deg, length, half, droop):
    """A sunflower leaf hanging from its stalk's end, as a line: a rounded base a little like a
    heart, a drawn-out tip, a midrib that follows the droop and three pairs of side veins."""
    down = 1 if math.sin(math.radians(deg)) > 0 else -1     # local y is the page's down when 1

    def to(p):
        s = p[0] / length
        return frame(base, deg)((p[0], p[1] + droop * length * s * s * down))

    # one side from the base to the tip, as (along, across) in lengths and half widths
    side = ((0.0, 0.0), (-0.02, 0.22), (0.03, 0.56), (0.13, 0.84), (0.28, 0.99), (0.44, 0.97), (0.60, 0.80),
            (0.75, 0.54), (0.87, 0.28), (0.95, 0.10), (1.0, 0.0))
    up = [to((length * s, -half * w)) for s, w in side]
    lo = [to((length * s, half * w * 0.92)) for s, w in side]
    vein = max(4.0, length * 0.018)
    prog.path(name, corners(up, lo[::-1]))
    edge = up + lo[::-1][1:]
    prog.line(name + "-edge", edge + [edge[0]], 2.5 * vein)
    prog.minus(name + "-in", name, name + "-edge")
    parts = [prog.minus(name + "-out", name, name + "-in"),
             prog.line(name + "-mid", [to((length * s, 0.0)) for s in (0.04, 0.3, 0.6, 0.86)], 1.25 * vein, 0.8 * vein)]
    for sign in (-1, 1):
        for j, (s0, s1, reach) in enumerate(((0.16, 0.36, 0.62), (0.36, 0.56, 0.60), (0.56, 0.74, 0.46))):
            parts.append(prog.line(f"{name}-{sign}{j}", [to((length * s0, 0.0)),
                                                         to((length * (0.4 * s0 + 0.6 * s1), sign * half * reach * 0.62)),
                                                         to((length * s1, sign * half * reach))], vein, 0.7 * vein))
    return prog.union(name + "-all", *parts)


def bee(prog):
    """The queen in her own frame (she faces local x, local y is down when she flies level), placed by
    BEE_AT, HEADING and BEE_SCALE. Gives "bee-yellow", "bee-dark" and "bee-lines" (wings, antennae
    and legs), "bee" (all of her), and where her tail is and which way is behind her."""
    s = BEE_SCALE
    to = frame(BEE_AT, HEADING, s)

    def on(*pts):
        return [to(p) for p in pts]

    head, head_r = (84.0, 14.0), 40.0
    prog.path("head", circle(to(head), head_r * s))
    prog.path("thorax", circle(to((0.0, 0.0)), 50.0 * s))
    # the abdomen: an egg from inside the thorax to a blunt tail, hanging a little
    a0, a1 = (-18.0, 6.0), (-236.0, 40.0)
    length = math.hypot(a1[0] - a0[0], a1[1] - a0[1])
    ux, uy = (a1[0] - a0[0]) / length, (a1[1] - a0[1]) / length
    up, lo = blade(lambda p: to((a0[0] + p[0] * ux - p[1] * uy, a0[1] + p[0] * uy + p[1] * ux)), length, 66.0,
                   0.6, 0.6, 0.0, 0.04, 22)
    prog.path("abdomen", corners(up, lo[::-1]))
    # the bands: rings round a point ahead of her, so each bows toward the tail
    ahead = to((a0[0] - 300 * ux, a0[1] - 300 * uy))
    rings = []
    for at, w in ((0.32, 26.0), (0.53, 26.0), (0.74, 24.0)):
        r = 300 + length * at
        prog.path(f"ring{at}+", circle(ahead, (r + w / 2) * s))
        prog.path(f"ring{at}-", circle(ahead, (r - w / 2) * s))
        rings.append(prog.minus(f"ring{at}", f"ring{at}+", f"ring{at}-"))
    prog.union("rings", *rings)
    prog.both("bands", "rings", "abdomen")
    # the sting: a small point at the tail
    tip, out, side = (a1[0] - 2, a1[1]), (16 * ux, 16 * uy), (-6 * uy, 6 * ux)
    point = (tip[0] + out[0], tip[1] + out[1])
    prog.path("sting", corners(on((tip[0] - side[0], tip[1] - side[1]), point),
                               on(point, (tip[0] + side[0], tip[1] + side[1]))))
    # the eye: a tall oval high on the front of the head
    prog.path("eye", ellipse(to((head[0] + 14, head[1] - 6)), 12.5 * s, 18.5 * s, HEADING - 90 - 12))
    # the crown: a band and three points with a ball on each, upright on the page whatever her heading
    hc, hr = to(head), head_r * s
    cw, rim, spike = 29.0 * s, 16.0 * s, 30.0 * s
    lean = math.radians(CROWN_LEAN)

    def crowned(x, y):
        x, y = x + 0.16 * hr, y - hr + 4 * s
        return (hc[0] + x * math.cos(lean) - y * math.sin(lean), hc[1] + x * math.sin(lean) + y * math.cos(lean))

    tips = ((-cw - 3 * s, -rim - 0.72 * spike), (0.0, -rim - spike), (cw + 3 * s, -rim - 0.72 * spike))
    outline = ((-cw, 6 * s), tips[0], (-0.45 * cw, -rim), tips[1], (0.45 * cw, -rim), tips[2], (cw, 6 * s))
    prog.path("crown-", "M " + " L ".join(f"{num(x)} {num(y)}" for x, y in (crowned(*p) for p in outline)) + " Z")
    prog.path("balls", " ".join(circle(crowned(*p), 7.0 * s) for p in tips))
    prog.union("crown", "crown-", "balls")
    # antennae from the front of the head, bent at the elbow; three legs hanging as she flies
    prog.line("antenna1", on((112.0, 0.0), (130.0, -26.0), (140.0, -44.0), (168.0, -50.0)), 7.5 * s, 6.0 * s)
    prog.line("antenna2", on((106.0, -8.0), (118.0, -38.0), (124.0, -56.0), (146.0, -70.0)), 7.5 * s, 6.0 * s)
    prog.union("antennae", "antenna1", "antenna2")
    legs = (((34.0, 38.0), (46.0, 62.0), (40.0, 84.0)), ((10.0, 46.0), (10.0, 74.0), (-2.0, 92.0)),
            ((-16.0, 46.0), (-30.0, 70.0), (-48.0, 80.0)))
    prog.union("legs", *(prog.line(f"leg{i}", on(*leg), 8.0 * s, 6.5 * s) for i, leg in enumerate(legs)))
    # the wings, fore and hind, from the top of the thorax and swept back: their edges, as lines
    for i, (base, back, wlen, whalf, wbend, wfull) in enumerate(WINGS, 1):
        up, lo = blade(frame(to(base), HEADING - back, s), wlen, whalf, 0.75, 0.6, wbend, wfull)
        prog.path(f"wing{i}", corners(up, lo[::-1]))
        edge = up + lo[::-1][1:-1] + [up[0], up[1]]
        prog.line(f"wing{i}-edge", edge, WING_LINE * s)

    for part in ("head", "thorax", "crown"):
        prog.grow(part + "+", part, GAP)
    prog.union("body", "abdomen", "thorax", "head", "crown", "sting")
    prog.grow("body+", "body", GAP)
    # each part cut from the one in front of it; the fore wing's edge crosses her, the body shows inside it
    prog.minus("abdomen-", "abdomen", "thorax+")
    prog.minus("thorax-", "thorax", "head+")
    prog.minus("head-", "head", "crown+")
    prog.union("parts", "abdomen-", "sting", "thorax-", "head-")
    prog.grow("wing1-edge+", "wing1-edge", GAP)
    prog.minus("parts-", "parts", "bands", "eye", "wing1-edge+")
    prog.minus("crown-shown", "crown", "wing1-edge+")
    prog.union("bee-yellow", "parts-", "crown-shown")
    prog.minus("bands-shown", "bands", "wing1-edge+")
    prog.union("bee-dark", "bands-shown", "eye")
    # the hind wing is behind her and behind the fore wing
    prog.union("before", "body", "wing1")
    prog.grow("before+", "before", GAP)
    prog.minus("wing2-shown", "wing2-edge", "before+")
    prog.minus("antennae-shown", "antennae", "head+", "crown+")
    prog.minus("legs-shown", "legs", "body+")
    prog.union("bee-lines", "wing1-edge", "wing2-shown", "antennae-shown", "legs-shown")
    prog.union("bee", "body", "wing1", "wing2", "antennae", "legs")
    return to((a1[0] - 24, a1[1] + 2)), frame((0, 0), HEADING + 180)((1, 0))


def scene():
    """The shapes of the picture, the same for both themes."""
    prog = Program()
    seeds = flower(prog)
    tail, behind = bee(prog)
    # the stem, the stalks and the leaves: one body, behind the flower and the bee
    stem = along(STEM, 240)
    prog.path("stem", band(stem, *STEM_WIDTH))
    body = ["stem"]
    for i, (t, deg, stalk, length, half, droop) in enumerate(LEAVES):
        x, y = stem[int(t * (len(stem) - 1))]
        a = math.radians(deg)
        end = (x + stalk * math.sin(a), y - stalk * math.cos(a) + 8)
        # the stalk arches out of the stem, and the leaf hangs from its end
        body.append(prog.line(f"stalk{i}", [(x, y), ((x + end[0]) / 2, min(y, end[1]) - 16), end], 15.0, 11.0))
        body.append(leaf(prog, f"leaf{i}", end, deg + math.copysign(14.0, deg), length, half, droop))
    prog.union("plant", *body)
    prog.grow("flower+", "flower", GAP)
    prog.grow("bee+", "bee", GAP)
    prog.minus("stem-shown", "plant", "flower+", "bee+")
    # the trail ends just under her tail; a dot that would touch anything is left out
    end = (tail[0] + 8 * behind[0] + 24 * behind[1], tail[1] + 8 * behind[1] - 24 * behind[0])
    way = evenly(list(TRAIL) + [end], TRAIL_DOTS)[:-1]
    dots = [(p, 1.8 + 6.8 * (k / (len(way) - 1)) ** 1.6) for k, p in enumerate(way)]
    prog.union("all", "plant", "flower", "bee")
    prog.grow("all+", "all", 2 * GAP)
    for k, (p, r) in enumerate(dots):
        prog.both(f"touch{k}", prog.path(f"dot{k}", circle(p, r)), "all+")
    got = prog.run("petals", "disc", "stem-shown", "bee-yellow", "bee-dark", "bee-lines",
                   *(f"touch{k}" for k in range(len(dots))))
    dots = [d for k, d in enumerate(dots) if not got[f"touch{k}"]]
    return got, seeds, dots


def svg(got, seeds, dots, body, dark):
    """One theme's file: `body` for the lines and the stem, `dark` for the head, the bands and the
    eye (None: the page shows through)."""
    def rounds(pts, fill):
        return (f'<g fill="{fill}">' + "".join(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r:.1f}"/>' for (x, y), r in pts)
                + "</g>")

    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}" width="{WIDTH}" height="{HEIGHT}">',
         rounds(dots, body), f'<path fill="{body}" d="{got["stem-shown"]}"/>']
    if dark:
        o.append(f'<path fill="{dark}" d="{got["disc"]}"/>')
    o += [f'<path fill="{PETAL}" d="{got["petals"]}"/>', rounds(seeds, PETAL),
          f'<path fill="{body}" d="{got["bee-lines"]}"/>', f'<path fill="{PETAL}" d="{got["bee-yellow"]}"/>']
    if dark:
        o.append(f'<path fill="{dark}" d="{got["bee-dark"]}"/>')
    return "".join(o) + "</svg>\n"


def main():
    root = os.path.dirname(HERE)
    got, seeds, dots = scene()
    for name, body, dark in (("on-light", INK, INK), ("on-dark", PAPER, None)):
        out = os.path.join(root, "webdoc", "public", f"queen-scene-{name}.svg")
        with open(out, "w") as fh:
            fh.write(svg(got, seeds, dots, body, dark))
        print("  ", os.path.relpath(out, root))


if __name__ == "__main__":
    main()
