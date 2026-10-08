#!/usr/bin/env python3
"""The Queen logo, constructed: the q is the sunflower.

The bowl of the lowercase q is the head of the flower and its descender is the
stem. The petals stand behind the letter and stop at the stem, so the space
before the next letter stays empty and the one shape reads as a sunflower, a
crest and a crown. Under "ueen", in the corner the q's tail leaves empty, runs
"message queue".

Letters are Futura Bold ("queen") and Futura Medium (the line), read from the
macOS system font and written out as outlines: the SVGs need no font.

The flower, measured in R, the radius of the q's bowl (a circle fitted to its
outline; the flower shares its centre):
  1. There are N places round the bowl, one every 360/N degrees. The first
     points straight down and the others follow clockwise. COUNT of them hold
     a petal: the last one stands over the stem, the stem itself takes the
     place of the one before the first, and the rest would lie over the word.
  2. A petal is a lens: two arcs of one radius, from BASE under the bowl to
     its tip. It is as wide as its place allows where it is widest, so every
     pair of neighbours meets at the same depth.
  3. Every tip is on one circle. Its radius is not chosen: it is the one that
     puts the last tip over the centre line of the stem.
  4. No petal is cut. It is whole or it is not there.
  5. The red is one outline: the petals' arcs and, hidden under the letter,
     a single arc at INNER and one straight line. The line is the last
     petal's side once it has passed under the stem: it drops straight to the
     inner arc, so the corner between bowl and stem is red to its very point.
  6. Round each petal runs a contour, LINE wide, in the letters' ink. Each
     petal lies over the one before it, so a contour stops where the next
     petal covers it. In one colour the red goes and the contours stay: line
     petals and a solid q.

The line: "message queue" in a small size, spread by its letter spacing alone
to run exactly from the left edge of the u to the right edge of the n, its own
tails standing on the line of the q's foot.

The small logo (small/): the q with the flower flat, no contour, for 48 px and
under, where a contour cannot be seen. Its PNGs are fitted to the pixel grid.
In one colour its petals stop MOAT short of the q all round it, so the letter
stays a letter.

The badge (badge/): the small logo in one colour, cut out of a disc that
shares the flower's centre, the way the GitHub mark is cut out of its circle.
As the cat's body does there, the q's stem runs on to the edge and out. A
solid disc weighs more than letters do, so the badge has quieter tones of its
own (MUTED), the ones the dashboard gives it (--brand in app/src/style.css).

Every contour and gap is real geometry, cut by assets/pathops.swift: no
stroke, no mask and no clip in any file.

Run on macOS:  python3 assets/wordmark.py
Needs fontTools, Pillow, rsvg-convert and swift. Writes assets/wordmark/:
  queen-tagline   the logo: the wordmark with its line (the README shows it)
  queen           the wordmark alone
  q               the q with its flower
                  each as .svg (on light), -on-dark, -mono and -mono-on-dark
  q-tile          the q on a square of paper, and -on-dark on ink; 512 px PNGs
  small/q         the small logo, in the same four files; q-auto.svg, whose
                  letter follows the viewer's colour scheme; PNGs at 16, 32, 48
  badge/q-badge   for light grounds, and -on-dark; 512 px PNGs
  overview.png    the one sheet that shows everything
and the copies the dashboard and the docs use:
  app/src/assets/q-badge.svg             the badge; the sidebar and the boot screen inline it
  app/public/favicon.svg, favicon-32.png the small logo
  app/public/queen-wordmark.svg          the wordmark and its line, flat, lettered in paper:
                                         the sign-in page (proxy/src/oauth.rs), which is dark
  webdoc/src/assets/queen-tagline.svg    the docs header on the home page: the wordmark and its
                                         line, flat, since a header is small
  webdoc/src/assets/q.svg                the docs header on every other page
  webdoc/public/favicon.svg, favicon-32.png, favicon.ico
  webdoc/public/apple-touch-icon.png     180 px tile (iOS rounds the corners itself)
  webdoc/public/queen-tile.png           512 px tile, schema.org Organization.logo
The docs' copies are the small logo, their letters painted in currentColor so
that they follow the page's theme. The dashboard shows the badge, all of it in
currentColor, beside the word queen: there, colour is kept for what needs the
operator.
"""
import json
import math
import os
import re
import shutil
import subprocess

from fontTools.pens.basePen import BasePen
from fontTools.pens.boundsPen import BoundsPen
from fontTools.pens.pointInsidePen import PointInsidePen
from fontTools.pens.recordingPen import DecomposingRecordingPen, RecordingPen
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTCollection

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.path.join(HERE, "wordmark")
FUTURA = "/System/Library/Fonts/Supplemental/Futura.ttc"
BOLD, MEDIUM = "Futura Bold", "Futura Medium"

RED = "#e03a2c"
INK = "#1f1d1a"    # the docs' and the dashboard's own text colour on a light page
PAPER = "#f4f1ec"
MUTED = ("#4f4b45", "#cdc8c0")  # the badge on light and on dark grounds: quieter than the letters

X = 200.0          # x-height of the letters, in drawing units
TRACK = -0.025     # letter spacing in x-heights, added to the font's advance widths

# the flower, in radii of the q's bowl unless noted
FLOWER = dict(n=14, count=9)    # places round the bowl, and how many hold a petal
FIRST = 180.0      # degrees clockwise from straight up: the first petal points down
BASE = 0.75        # where a petal starts, under the bowl
INNER = 0.80       # the inner edge of the red, under the bowl
EDGE = 0.06 * X    # the last tip stays at least this far inside the stem's right edge
UNDER = 0.015 * X  # how far under the stem's top its side goes before it drops
LINE = 0.04 * X    # the contour round a petal
HAIR = 0.006 * X   # a contour reaches this far under the petal that covers it, so no seam opens
MOAT = 0.11 * X    # small logo in one colour, and badge: the gap between the q and its petals
DISC = 2.45        # the badge's disc
PAD = 0.04 * X     # air round the artwork in every file

TAGLINE = "message queue"
TAG_X = 0.20 * X   # x-height of the line under the wordmark

WORD, MARK = [("queen", BOLD)], [("q", BOLD)]
LOCKUPS = (("queen-tagline", WORD, True, "Queen, message queue"), ("queen", WORD, False, "Queen"),
           ("q", MARK, False, "Queen"))


def num(v):
    return f"{v:.1f}".rstrip("0").rstrip(".")


_faces = {}


def face(name):
    if name not in _faces:
        font = next(f for f in TTCollection(FUTURA).fonts if f["name"].getDebugName(4) == name)
        glyphs, cmap = font.getGlyphSet(), font.getBestCmap()
        box = BoundsPen(glyphs)
        glyphs[cmap[ord("x")]].draw(box)
        _faces[name] = (glyphs, cmap, X / box.bounds[3])
    return _faces[name]


def outline(glyphs, name, scale, x, ops=None, y=0.0):
    pen = SVGPathPen(glyphs, ntos=num)
    placed = TransformPen(pen, (scale, 0, 0, -scale, x, y))
    if ops is None:
        glyphs[name].draw(placed)
    else:
        rec = RecordingPen()
        rec.value = ops
        rec.replay(placed)
    return pen.getCommands()


class Ops(BasePen):
    """A glyph as "M x y L x y C ... Z", every curve a cubic: what pathops.swift reads."""

    def __init__(self, glyphs):
        super().__init__(glyphs)
        self.ops = []

    def _moveTo(self, p):
        self.ops.append(f"M {p[0]:.2f} {p[1]:.2f}")

    def _lineTo(self, p):
        self.ops.append(f"L {p[0]:.2f} {p[1]:.2f}")

    def _curveToOne(self, a, b, c):
        self.ops.append(f"C {a[0]:.2f} {a[1]:.2f} {b[0]:.2f} {b[1]:.2f} {c[0]:.2f} {c[1]:.2f}")

    def _closePath(self):
        self.ops.append("Z")

    _endPath = _closePath


class Points(BasePen):
    """A glyph as points along each contour: for measuring."""

    def __init__(self, glyphs, steps=32):
        super().__init__(glyphs)
        self.contours, self.steps = [], steps

    def _moveTo(self, p):
        self.contours.append([p])

    def _lineTo(self, p):
        self.contours[-1].append(p)

    def _curveToOne(self, a, b, c):
        p = self.contours[-1][-1]
        for i in range(1, self.steps + 1):
            t = i / self.steps
            u = 1 - t
            self.contours[-1].append(tuple(u ** 3 * p[k] + 3 * u * u * t * a[k] + 3 * u * t * t * b[k] + t ** 3 * c[k]
                                           for k in (0, 1)))

    def _closePath(self):
        pass


def fit_circle(pts):
    """Least squares: the circle x^2 + y^2 = a x + b y + c through a cloud of points."""
    n = len(pts)
    z = [x * x + y * y for x, y in pts]
    sx, sy = sum(x for x, _ in pts), sum(y for _, y in pts)
    m = [[sum(x * x for x, _ in pts), sum(x * y for x, y in pts), sx],
         [sum(x * y for x, y in pts), sum(y * y for _, y in pts), sy],
         [sx, sy, n]]
    v = [sum(x * k for (x, _), k in zip(pts, z)), sum(y * k for (_, y), k in zip(pts, z)), sum(z)]

    def det(t):
        return (t[0][0] * (t[1][1] * t[2][2] - t[1][2] * t[2][1]) - t[0][1] * (t[1][0] * t[2][2] - t[1][2] * t[2][0])
                + t[0][2] * (t[1][0] * t[2][1] - t[1][1] * t[2][0]))

    a, b, c = (det([[v[r] if col == k else m[r][col] for col in range(3)] for r in range(3)]) / det(m)
               for k in range(3))
    return a / 2, b / 2, math.sqrt(c + a * a / 4 + b * b / 4)


def measure_q(glyphs, name, scale, x):
    """The numbers the flower is built on: bowl, stem and counter of the q."""
    rec = DecomposingRecordingPen(glyphs)
    glyphs[name].draw(rec)
    contours, cur = [], []
    for op in rec.value:
        cur.append(op)
        if op[0] in ("closePath", "endPath"):
            contours.append(cur)
            cur = []

    def bounds(ops):
        box, r = BoundsPen(glyphs), RecordingPen()
        r.value = ops
        r.replay(box)
        return box.bounds

    def inside(px, py, ops=None):
        pen = PointInsidePen(glyphs, (px, py))
        if ops is None:
            glyphs[name].draw(pen)
        else:
            r = RecordingPen()
            r.value = ops
            r.replay(pen)
        return pen.getResult()

    boxes = [bounds(c) for c in contours]
    area = lambda b: (b[2] - b[0]) * (b[3] - b[1])
    outer = max(boxes, key=area)
    counter = min(range(len(contours)), key=lambda i: area(boxes[i]))
    cb = boxes[counter]
    cy = (cb[1] + cb[3]) / 2
    # scan the row through the counter: the stem's right edge
    right = outer[2]
    while not inside(right, cy):
        right -= 1
    # and a row halfway down the descender: the stem's left edge
    stem = outer[0]
    while not inside(stem, outer[1] / 2):
        stem += 1
    ops = Ops(glyphs)
    glyphs[name].draw(TransformPen(ops, (scale, 0, 0, -scale, x, 0)))
    # the bowl is a circle: fit one to the outline left of the stem
    pen = Points(glyphs)
    glyphs[name].draw(pen)
    rim = max(pen.contours, key=lambda c: max(p[0] for p in c) - min(p[0] for p in c))
    bx, by, br = fit_circle([p for p in rim if p[0] < stem - 0.03 * X / scale])
    shoulder = outer[3]                       # the flat top of the stem, below the bowl's overshoot
    while not inside((stem + right) / 2, shoulder):
        shoulder -= 1
    return dict(ops=" ".join(ops.ops),
                ox=x + bx * scale, oy=-by * scale, R=br * scale,
                edge=x + right * scale, stem=x + stem * scale, mid=x + (stem + right) / 2 * scale,
                top=-outer[3] * scale, foot=-outer[1] * scale, shoulder=-shoulder * scale,
                inside=lambda px, py: inside((px - x) / scale, -py / scale),
                in_counter=lambda px, py: inside((px - x) / scale, -py / scale, contours[counter]))


def typeset(runs):
    """[(text, face)] -> the letters as one path, their box, and the q's measures."""
    x, paths, q, spans = 0.0, [], None, []
    top = bottom = right = 0.0
    for text, name in runs:
        glyphs, cmap, scale = face(name)
        for ch in text:
            g = cmap[ord(ch)]
            box = BoundsPen(glyphs)
            glyphs[g].draw(box)
            if q is None:
                q = measure_q(glyphs, g, scale, x)
                q["left"] = x + box.bounds[0] * scale
                q["spans"] = spans
            spans.append((x + box.bounds[0] * scale, x + box.bounds[2] * scale))
            paths.append(outline(glyphs, g, scale, x))
            top, bottom = min(top, -box.bounds[3] * scale), max(bottom, -box.bounds[1] * scale)
            right = x + box.bounds[2] * scale
            x += glyphs[g].width * scale + TRACK * X
    return "".join(paths), (q["left"], top, right, bottom), q


def num2(v):
    return f"{v:.2f}".rstrip("0").rstrip(".")


def meet(c0, r0, c1, r1):
    """The two points where two circles cross."""
    dx, dy = c1[0] - c0[0], c1[1] - c0[1]
    d = math.hypot(dx, dy)
    a = (d * d + r0 * r0 - r1 * r1) / (2 * d)
    h = math.sqrt(r0 * r0 - a * a)
    mx, my = c0[0] + a * dx / d, c0[1] + a * dy / d
    return (mx - h * dy / d, my + h * dx / d), (mx + h * dy / d, my - h * dx / d)


def arc(p, q, c, r, far=False, fine=6):
    """Along the circle (c, r) from p to q, the short way unless far: the SVG command, points on it
    (`fine` to each quarter turn), and the same arc as cubics for pathops.swift."""
    a0, a1 = math.atan2(p[1] - c[1], p[0] - c[0]), math.atan2(q[1] - c[1], q[0] - c[0])
    turn = (a1 - a0 + math.pi) % (2 * math.pi) - math.pi          # signed, short way; + is clockwise on screen
    if far:
        turn -= math.copysign(2 * math.pi, turn)
    cmd = f"A{num2(r)},{num2(r)} 0 {int(abs(turn) > math.pi)} {int(turn > 0)} {num2(q[0])},{num2(q[1])}"
    pieces = max(1, math.ceil(abs(turn) / (math.pi / 2)))
    pts, ops, step = [], [], turn / pieces
    k = 4 / 3 * math.tan(step / 4) * r
    for i in range(pieces):
        t0, t1 = a0 + i * step, a0 + (i + 1) * step
        e = q if i == pieces - 1 else (c[0] + r * math.cos(t1), c[1] + r * math.sin(t1))
        s0 = (c[0] + r * math.cos(t0), c[1] + r * math.sin(t0))
        ops.append(f"C {s0[0] - k * math.sin(t0):.2f} {s0[1] + k * math.cos(t0):.2f} "
                   f"{e[0] + k * math.sin(t1):.2f} {e[1] - k * math.cos(t1):.2f} {e[0]:.2f} {e[1]:.2f}")
        pts += [(c[0] + r * math.cos(t0 + (t1 - t0) * j / fine), c[1] + r * math.sin(t0 + (t1 - t0) * j / fine))
                for j in range(1, fine + 1)]
    return cmd, pts, " ".join(ops)


def ring(o, R, n, places, tip):
    """The petals at some of the n places round o: for each, its tip and the centres of its two
    arcs (trailing = the side it shows to the place before it, leading = to the place after)."""
    b, t = BASE * R, tip * R
    mid = (b + t) / 2
    hw = mid * math.tan(math.pi / n)            # as wide as its place allows, where it is widest
    rho = ((t - b) ** 2 / 4 + hw * hw) / (2 * hw)
    out = []
    for i in places:
        a = math.radians(FIRST + i * 360.0 / n)
        ax, on = (math.sin(a), -math.cos(a)), (math.cos(a), math.sin(a))
        m = (o[0] + mid * ax[0], o[1] + mid * ax[1])
        out.append(dict(tip=(o[0] + t * ax[0], o[1] + t * ax[1]), base=(o[0] + b * ax[0], o[1] + b * ax[1]),
                        trail=(m[0] + on[0] * (rho - hw), m[1] + on[1] * (rho - hw)),
                        lead=(m[0] - on[0] * (rho - hw), m[1] - on[1] * (rho - hw))))
    return out, rho


def closed(p, rho, back=None):
    """One petal as a path for pathops.swift: from its base to its tip along one arc, and back
    along the other (or along `back`, when its second side is not one arc)."""
    return (f'M {p["base"][0]:.2f} {p["base"][1]:.2f} {arc(p["base"], p["tip"], p["trail"], rho)[2]} '
            f'{back or arc(p["tip"], p["base"], p["lead"], rho)[2]} Z')


def notch(o, a, b, rho):
    """Where petal a's leading arc crosses petal b's trailing arc: the outer of the two crossings."""
    return max(meet(a["lead"], rho, b["trail"], rho), key=lambda p: math.hypot(p[0] - o[0], p[1] - o[1]))


def tip_radius(q, n, count):
    """Rule 3, in bowl radii: the last petal's tip over the centre line of the stem."""
    last = math.radians(FIRST + (count - 1) * 360.0 / n)
    return (q["mid"] - q["ox"]) / math.sin(last) / q["R"]


def flower(q, n, count):
    """The flower, flat: its path and points along it. For pathops.swift it leaves in q the same
    outline ("red") and each petal on its own ("petals")."""
    o, R = (q["ox"], q["oy"]), q["R"]
    ps, rho = ring(o, R, n, range(count), tip_radius(q, n, count))
    near = lambda pts, p: min(pts, key=lambda v: math.hypot(v[0] - p[0], v[1] - p[1]))
    start = near(meet(ps[0]["trail"], rho, o, INNER * R), ps[0]["base"])
    segs = [arc(start, ps[0]["tip"], ps[0]["trail"], rho)]
    for a, b in zip(ps, ps[1:]):
        v = notch(o, a, b, rho)
        segs += [arc(a["tip"], v, a["lead"], rho), arc(v, b["tip"], b["trail"], rho)]
    # The last petal's leading side goes under the stem. If its own arc then stays hidden all the
    # way to the inner arc, it is the edge. If it would come out again in the corner between bowl
    # and stem, it drops straight down instead, so that corner is red to its very point.
    c = ps[-1]["lead"]
    end = near(meet(c, rho, o, INNER * R), ps[-1]["base"])
    side = [arc(ps[-1]["tip"], end, c, rho, fine=96)]       # looked at closely: the corner is small
    if not all(q["inside"](x, y) for x, y in side[0][1] if y > q["shoulder"] + UNDER):
        yp = q["shoulder"] + UNDER
        under = (c[0] + math.sqrt(rho * rho - (yp - c[1]) ** 2), yp)
        end = (under[0], o[1] - math.sqrt((INNER * R) ** 2 - (under[0] - o[0]) ** 2))
        side = [arc(ps[-1]["tip"], under, c, rho),
                (f"V{num2(end[1])}", [(under[0], under[1] + (end[1] - under[1]) * j / 6) for j in range(7)],
                 f"L {end[0]:.2f} {end[1]:.2f}")]
    span = math.degrees(math.atan2(end[0] - o[0], o[1] - end[1]) - math.atan2(start[0] - o[0], o[1] - start[1])) % 360
    hidden = arc(end, start, o, INNER * R, far=span > 180)
    segs += side + [hidden]
    d = f"M{num2(start[0])},{num2(start[1])}" + "".join(s[0] for s in segs) + "Z"
    pts = [p for s in segs for p in s[1]]
    q["red"] = f"M {start[0]:.2f} {start[1]:.2f} " + " ".join(s[2] for s in segs) + " Z"
    # each petal on its own, whole, in the order they stand: the last one with its hidden side
    q["petals"] = [closed(p, rho) for p in ps[:-1]] + [closed(ps[-1], rho, " ".join(s[2] for s in side))]

    # what the rules promise, checked on the outline itself
    assert not any(q["in_counter"](x, y) for x, y in pts), "red shows inside the counter"
    below = [p for s in side for p in s[1] if p[1] > q["shoulder"] + UNDER]
    assert all(q["inside"](x, y) for x, y in hidden[1] + below), "a hidden edge of the red is not under the letter"
    assert ps[-1]["tip"][0] < q["edge"] - EDGE, "the last tip is too near the stem's right edge"
    assert all(x < q["stem"] - 0.05 * X for x, y in segs[0][1] if not q["inside"](x, y)), \
        "the first petal touches the stem"
    return d, pts


def tagline(q, text=TAGLINE, name=MEDIUM):
    """The line under the wordmark: `text` at x-height TAG_X, spread by its letter spacing alone to
    run from the second letter's left edge to the last letter's right edge, its tails on the q's foot."""
    glyphs, cmap, scale = face(name)
    k = scale * TAG_X / X
    left, right = q["spans"][1][0], q["spans"][-1][1]
    marks = []                       # glyph, advance, ink left, ink right, depth below the baseline
    for ch in text:
        g = cmap[ord(ch)]
        box = BoundsPen(glyphs)
        glyphs[g].draw(box)
        b = box.bounds or (0, 0, 0, 0)
        marks.append((g, glyphs[g].width * k, b[0] * k, b[2] * k, -b[1] * k, box.bounds is not None))
    natural = sum(m[1] for m in marks[:-1]) + marks[-1][3] - marks[0][2]
    track = (right - left - natural) / (len(marks) - 1)
    base = q["foot"] - max(m[4] for m in marks)
    x, d = left - marks[0][2], []
    for g, adv, _, _, _, inked in marks:
        if inked:
            d.append(outline(glyphs, g, k, x, y=base))
        x += adv + track
    return "".join(d)


def tight(box, pad=PAD):
    return (box[0] - pad, box[1] - pad, box[2] - box[0] + 2 * pad, box[3] - box[1] + 2 * pad)


def square(box, margin):
    """A square view around a box, with a margin given as a share of the side."""
    w, h = box[2] - box[0], box[3] - box[1]
    side = max(w, h) / (1 - 2 * margin)
    return ((box[0] + box[2] - side) / 2, (box[1] + box[3] - side) / 2, side, side)


def union(a, b):
    """The box round two boxes."""
    return (min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3]))


def pathops(programs):
    """Run programs of path arithmetic (assets/pathops.swift): for each, the paths it names in "out"."""
    run = subprocess.run(["swift", os.path.join(HERE, "pathops.swift")], input=json.dumps(programs).encode(),
                         capture_output=True)
    if run.returncode:
        raise SystemExit(run.stderr.decode())
    return json.loads(run.stdout)


def circle(o, r):
    """A full circle as a path for pathops.swift."""
    pts = [(o[0] + r * math.cos(t), o[1] + r * math.sin(t)) for t in (0, math.pi / 2, math.pi, 3 * math.pi / 2)]
    return (f"M {pts[0][0]:.2f} {pts[0][1]:.2f} "
            + " ".join(arc(a, b, o, r)[2] for a, b in zip(pts, pts[1:] + pts[:1])) + " Z")


def contours(petals, line, hair):
    """Rule 6 as steps for pathops.swift: they leave in "c" the contour of every petal, `line`
    wide, where the next petal does not cover it. Each petal lies over the one before it."""
    steps, n = [], len(petals)
    for k, petal in enumerate(petals):
        steps += [["path", f"p{k}", petal], ["line", f"b{k}", f"p{k}", line],
                  # what a petal covers stops a hair inside its own contour, so the contour it
                  # covers ends under that one and no seam can open between the two
                  ["line", "inner", f"p{k}", line - hair], ["minus", f"f{k}", f"p{k}", "inner"]]
    for k in range(n):
        steps.append(["minus", f"v{k}", f"b{k}", f"f{k + 1}"] if k + 1 < n else ["union", f"v{k}", f"b{k}"])
    return steps + [["union", "c"] + [f"v{k}" for k in range(n)]]


def drawing():
    """The shapes every file is made of, drawn once: the flower does not change from file to file,
    because the q is the first letter of each."""
    qd, box, q = typeset(MARK)
    flat, pts = flower(q, **FLOWER)
    o, R = (q["ox"], q["oy"]), q["R"]
    run = "M {0:.2f} {2:.2f} L {1:.2f} {2:.2f} L {1:.2f} {3:.2f} L {0:.2f} {3:.2f} Z".format(
        q["stem"], q["edge"], q["foot"] - 0.01 * X, o[1] + (DISC + 0.2) * R)
    steps = contours(q["petals"], LINE, HAIR) + [
        ["path", "q", q["ops"]], ["path", "flat", q["red"]],
        ["union", "ink", "c", "q"],                                 # the contours and the letter: one shape
        ["grow", "moat", "q", MOAT], ["minus", "cut", "flat", "moat"], ["keep", "cut", "cut", 2 * MOAT],
        # the badge: a disc, and cut out of it the small logo, its stem carried on past the edge
        ["path", "disc", circle(o, DISC * R)], ["path", "run", run],
        ["union", "hole", "cut", "q", "run"], ["minus", "badge", "disc", "hole"]]
    ink, cut, badge = pathops([dict(steps=steps, out=["ink", "cut", "badge"])])[0]
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    return dict(q=q, qd=qd, flat=flat, ink=ink, cut=cut, badge=badge,
                box=union(box, (min(xs), min(ys), max(xs), max(ys))))


def doc(view, title, *layers, ground=None, style=""):
    """An SVG file: its frame, its title, and (path, fill) layers from the back."""
    x, y, w, h = view
    back = f'<rect x="{num(x)}" y="{num(y)}" width="{num(w)}" height="{num(h)}" fill="{ground}"/>' if ground else ""
    body = "".join(f'<path {fill} d="{d}"/>' if fill.startswith("class=") else f'<path fill="{fill}" d="{d}"/>'
                   for d, fill in layers)
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{num(x)} {num(y)} {num(w)} {num(h)}" '
            f'width="{num(w)}" height="{num(h)}" role="img" aria-label="{title}"><title>{title}</title>'
            f'{style}{back}{body}</svg>\n')


def inline(view, *layers):
    """For a page that inlines it and sizes it with its own CSS: a layer filled "currentColor"
    takes the page's text colour."""
    x, y, w, h = view
    body = "".join(f'<path fill="{fill}" d="{d}"/>' for d, fill in layers)
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{num(x)} {num(y)} {num(w)} {num(h)}" '
            f'aria-hidden="true" focusable="false">{body}</svg>\n')


def write(text, *path, root=OUT):
    out = os.path.join(root, *path)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as fh:
        fh.write(text)
    return out


def png(src, out, size):
    subprocess.run(["rsvg-convert", src, "-w", str(size), "-h", str(size), "-o", out], check=True)
    return out


def disc(f):
    """The frame of the badge: the square round its disc."""
    q = f["q"]
    r = DISC * q["R"]
    return (q["ox"] - r, q["oy"] - r, 2 * r, 2 * r)


def fitted(f, size, ink):
    """The small logo at one pixel size, nudged so the stem and the top of the q land on whole pixels."""
    q, box = f["q"], f["box"]
    w, h = box[2] - box[0], box[3] - box[1]
    s = size * 0.94 / max(w, h)
    sx = max(1, round((q["edge"] - q["stem"]) * s)) / (q["edge"] - q["stem"])
    sy = max(1, round((q["foot"] - q["top"]) * s)) / (q["foot"] - q["top"])
    k = min(1.0, size / (w * sx), size / (h * sy))           # never larger than the frame
    sx, sy = sx * k, sy * k
    ox = round((size - w * sx) / 2 + (q["edge"] - box[0]) * sx) - q["edge"] * sx
    oy = round((size - h * sy) / 2 + (q["top"] - box[1]) * sy) - q["top"] * sy
    ox = min(max(ox, -box[0] * sx), size - box[2] * sx)
    oy = min(max(oy, -box[1] * sy), size - box[3] * sy)
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" width="{size}" height="{size}">'
            f'<g transform="matrix({sx:.5f} 0 0 {sy:.5f} {ox:.3f} {oy:.3f})">'
            f'<path fill="{RED}" d="{f["flat"]}"/><path fill="{ink}" d="{f["qd"]}"/></g></svg>\n')


def main():
    """Write assets/wordmark/ afresh, and return the drawing for place() and overview()."""
    shutil.rmtree(OUT, ignore_errors=True)
    f = drawing()
    flat, ink, qd = f["flat"], f["ink"], f["qd"]
    tones = (("", INK, PAPER), ("-on-dark", PAPER, INK))            # suffix, letters, ground

    # the logo, the wordmark alone and the q: the contours and the q are one shape, `ink`
    for name, runs, line, title in LOCKUPS:
        letters, box, q = typeset(runs)
        if line:
            letters += tagline(q)
        assert letters.startswith(qd)
        rest = letters[len(qd):]                                    # the letters after the q
        view = tight(union(box, f["box"]))
        for suffix, tone, _ in tones:
            write(doc(view, title, (flat, RED), (ink + rest, tone)), f"{name}{suffix}.svg")
            write(doc(view, title, (ink + rest, tone)), f"{name}-mono{suffix}.svg")
    for suffix, tone, ground in tones:
        src = write(doc(square(f["box"], 0.17), "Queen", (flat, RED), (ink, tone), ground=ground),
                    f"q-tile{suffix}.svg")
        png(src, os.path.join(OUT, f"q-tile{suffix}-512.png"), 512)

    # the small logo: the flower flat
    view = tight(f["box"])
    for suffix, tone, _ in tones:
        write(doc(view, "Queen", (flat, RED), (qd, tone)), "small", f"q{suffix}.svg")
        write(doc(view, "Queen", (f["cut"] + qd, tone)), "small", f"q-mono{suffix}.svg")
        for size in (16, 32, 48):
            src = write(fitted(f, size, tone), "small", f".fit-{size}.svg")
            png(src, os.path.join(OUT, "small", f"q{suffix}-{size}.png"), size)
            os.remove(src)
    scheme = f"<style>.l{{fill:{INK}}}@media (prefers-color-scheme:dark){{.l{{fill:{PAPER}}}}}</style>"
    write(doc(square(f["box"], 0.03), "Queen", (flat, RED), (qd, 'class="l"'), style=scheme), "small", "q-auto.svg")

    # the badge, in its own quieter tones
    for suffix, tone in zip(("", "-on-dark"), MUTED):
        src = write(doc(disc(f), "Queen", (f["badge"], tone)), "badge", f"q-badge{suffix}.svg")
        png(src, os.path.join(OUT, "badge", f"q-badge{suffix}-512.png"), 512)
    return f


def place(f):
    """The copies the dashboard (app/) and the docs (webdoc/) use."""
    from PIL import Image

    flat, qd, view = f["flat"], f["qd"], tight(f["box"])
    site = lambda *path: os.path.join(ROOT, *path)
    write(inline(disc(f), (f["badge"], "currentColor")), "app", "src", "assets", "q-badge.svg", root=ROOT)
    write(inline(view, (flat, RED), (qd, "currentColor")), "webdoc", "src", "assets", "q.svg", root=ROOT)
    # the wordmark with its line, flat, since it is small where these two pages show it: the docs' home page
    # and the dashboard's sign-in page, which both inline it so its letters take the text colour of the theme
    letters, box, q = typeset(WORD)
    word, frame = letters + tagline(q), tight(union(box, f["box"]))
    for site_path in (("webdoc", "src", "assets", "queen-tagline.svg"), ("app", "public", "queen-wordmark.svg")):
        write(inline(frame, (flat, RED), (word, "currentColor")), *site_path, root=ROOT)

    for name in ("app", "webdoc"):
        shutil.copyfile(os.path.join(OUT, "small", "q-auto.svg"), site(name, "public", "favicon.svg"))
        shutil.copyfile(os.path.join(OUT, "small", "q-32.png"), site(name, "public", "favicon-32.png"))
    sizes = [Image.open(os.path.join(OUT, "small", f"q-{n}.png")).convert("RGBA") for n in (48, 32, 16)]
    sizes[0].save(site("webdoc", "public", "favicon.ico"), sizes=[(16, 16), (32, 32), (48, 48)],
                  append_images=sizes[1:])
    shutil.copyfile(os.path.join(OUT, "q-tile-512.png"), site("webdoc", "public", "queen-tile.png"))
    png(os.path.join(OUT, "q-tile.svg"), site("webdoc", "public", "apple-touch-icon.png"), 180)


def overview(f):
    """One sheet with everything, for looking at: assets/wordmark/overview.png."""
    from PIL import Image, ImageDraw, ImageFont

    M, G, WIDE = 60, 24, 2400
    head = ImageFont.truetype(FUTURA, 34, index=2)
    text = ImageFont.truetype(FUTURA, 24, index=0)
    grey = "#8a847c"
    tmp = os.path.join(OUT, ".panel.svg")

    def render(markup, w, h):
        with open(tmp, "w") as fh:
            fh.write(markup)
        subprocess.run(["rsvg-convert", tmp, "-w", str(w), "-h", str(h), "-o", tmp + ".png"], check=True)
        return Image.open(tmp + ".png").convert("RGB")

    def panel(file, w, h, ground, fill=0.84):
        """A file of the set, centred in a w x h panel of the given ground."""
        src = open(os.path.join(OUT, file)).read()
        x, y, vw, vh = (float(v) for v in re.search(r'viewBox="([^"]+)"', src).group(1).split())
        inner = re.search(r"</title>(.*)</svg>", src, re.S).group(1)
        k = min(w * fill / vw, h * fill / vh)
        return render(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}"><rect width="{w}" '
                      f'height="{h}" fill="{ground}"/><svg x="{(w - vw * k) / 2}" y="{(h - vh * k) / 2}" '
                      f'width="{vw * k}" height="{vh * k}" viewBox="{x} {y} {vw} {vh}">{inner}</svg></svg>', w, h)

    def pixels(files, zoom, ground, gap=22):
        """Small renders enlarged so their pixels show, standing on one line."""
        ims = []
        for file, px in files:
            if file.endswith(".png"):
                im = Image.open(os.path.join(OUT, file)).convert("RGBA")
            else:
                subprocess.run(["rsvg-convert", os.path.join(OUT, file), "-w", str(px), "-h", str(px), "-o",
                                tmp + ".png"], check=True)
                im = Image.open(tmp + ".png").convert("RGBA")
            back = Image.new("RGBA", im.size, ground)
            back.alpha_composite(im)
            ims.append(back.convert("RGB").resize((im.width * zoom, im.height * zoom), Image.NEAREST))
        tall = max(im.height for im in ims)
        strip = Image.new("RGB", (sum(im.width + gap for im in ims) + gap, tall + 2 * gap), ground)
        x = gap
        for im in ims:
            strip.paste(im, (x, gap + tall - im.height))
            x += im.width + gap
        return strip

    rows = []                                   # (heading, note, [(image, x)])
    half = (WIDE - 2 * M - G) // 2
    rows.append(("The logo", 'the q is the sunflower; under "ueen", message queue',
                 [(panel("queen-tagline.svg", half, 430, PAPER), M),
                  (panel("queen-tagline-on-dark.svg", half, 430, INK), M + half + G)]))
    w, side = 730, 330
    rows.append(("", "in one colour, and the q on its own",
                 [(panel("queen-tagline-mono.svg", w, side, PAPER), M),
                  (panel("queen-tagline-mono-on-dark.svg", w, side, INK), M + w + G),
                  (panel("q.svg", side, side, PAPER, 0.74), M + 2 * (w + G)),
                  (panel("q-on-dark.svg", side, side, INK, 0.74), M + 2 * (w + G) + side + G)]))
    side = 300
    cells = [(panel(f"small/q{v}.svg", side, side, g, 0.72), M + i * (side + G))
             for i, (v, g) in enumerate((("", PAPER), ("-on-dark", INK), ("-mono", PAPER), ("-mono-on-dark", INK)))]
    x = M + 4 * (side + G)
    for suffix, ground in (("", PAPER), ("-on-dark", INK)):
        strip = pixels([(f"small/q{suffix}-{n}.png", n) for n in (48, 32, 16)], 4, ground)
        cells.append((strip, x))
        x += strip.width + G
    rows.append(("The small logo", "the flower flat, for 48 px and under. Right: drawn at 48, 32 and 16 px, shown 4x",
                 cells))
    cells = [(panel("badge/q-badge.svg", side, side, PAPER, 0.80), M),
             (panel("badge/q-badge-on-dark.svg", side, side, INK, 0.80), M + side + G)]
    x = M + 2 * (side + G)
    for suffix, ground in (("", PAPER), ("-on-dark", INK)):
        strip = pixels([(f"badge/q-badge{suffix}.svg", n) for n in (64, 32, 16)], 3, ground)
        cells.append((strip, x))
        x += strip.width + G
    rows.append(("The badge", "the small logo cut out of a disc. Right: at 64, 32 and 16 px, shown 3x", cells))

    # how it is built: the flower whole, the letter as an outline, the circles and lines it hangs on
    q = f["q"]
    ox, oy, R = q["ox"], q["oy"], q["R"]
    tip = tip_radius(q, **FLOWER)
    wide = (BASE + tip) / 2
    line = f'fill="none" stroke="{INK}" stroke-width="1.6"'
    dash = line + ' stroke-dasharray="7 7"'
    bh = 620
    tall = 5.4 * R
    vx, vy, vw = ox - tall * half / bh * 0.25, oy - tall * 0.5, tall * half / bh
    build = render(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vx} {vy} {vw} {tall}"><rect x="{vx}" y="{vy}" '
        f'width="{vw}" height="{tall}" fill="{PAPER}"/><path fill="{RED}" d="{f["flat"]}"/>'
        f'<path fill="{INK}" d="{f["ink"]}"/><path d="{f["qd"]}" fill="{PAPER}" fill-opacity="0.86"/>'
        f'<path d="{f["qd"]}" fill="none" stroke="{INK}" stroke-width="2.4"/>'
        + "".join(f'<circle cx="{ox}" cy="{oy}" r="{k * R}" {dash}/>' for k in (1, wide, tip))
        + f'<path d="M{q["mid"]},{oy - 2.55 * R}V{oy + 2.3 * R}M{ox},{oy}V{oy + 2.3 * R}" {line}/>'
        f'<path d="M{ox - 9},{oy}H{ox + 9}M{ox},{oy - 9}V{oy + 9}" {line}/></svg>', half, bh)
    d = ImageDraw.Draw(build)
    x0 = int((ox + tip * R - vx) / vw * half) + 60
    y = 66
    for style, lines in (("dash", ["The bowl of the q is a circle, R.", "The flower shares its centre."]),
                         ("dash", [f"Every tip is on one circle, {tip:.2f} R.",
                                   f"Petals are widest, and meet, at {wide:.2f} R."]),
                         ("solid", ["The centre line of the stem.", "The last tip stands on it."]),
                         ("solid", ["The first petal points straight down.", "The stem takes the next place."]),
                         ("none", [f"{FLOWER['n']} places, {360 / FLOWER['n']:.1f}° apart.",
                                   f"{FLOWER['count']} hold a petal. None is cut."]),
                         ("none", ["Each petal lies over the one before it.",
                                   "Its contour stops where the next covers it."])):
        if style == "solid":
            d.line([x0, y + 14, x0 + 44, y + 14], fill=INK, width=2)
        elif style == "dash":
            for k in range(4):
                d.line([x0 + k * 12, y + 14, x0 + k * 12 + 6, y + 14], fill=INK, width=2)
        for i, words in enumerate(lines):
            d.text((x0 + 64, y + i * 32), words, fill=INK if i == 0 else grey, font=text)
        y += 86
    spec = Image.new("RGB", (half, bh), PAPER)
    d = ImageDraw.Draw(spec)
    y = 66
    for name, colour, use in (("Red", RED, "the petals, and nothing else"), ("Ink", INK, "letters and contours on light"),
                              ("Paper", PAPER, "letters and contours on dark")):
        d.rectangle([64, y, 64 + 96, y + 96], fill=colour, outline=grey)
        d.text((190, y + 12), f"{name}  {colour}", fill=INK, font=head)
        d.text((190, y + 58), use, fill=grey, font=text)
        y += 124
    y += 6
    for words in ("Letters: Futura Bold. The line: Futura Medium.",
                  "R is the radius of the q's bowl. Everything in the flower is measured in it.",
                  "The small logo has the same nine petals, flat.",
                  "Files: assets/wordmark/. Rebuild: python3 assets/wordmark.py"):
        d.text((64, y), words, fill=INK, font=text)
        y += 38
    rows.append(("How it is built", "", [(build, M), (spec, M + half + G)]))

    height = M + sum((58 if title else 0) + (38 if note else 0) + max(im.height for im, _ in cells) + 40
                     for title, note, cells in rows) - 40 + M
    sheet = Image.new("RGB", (WIDE, height), "#ffffff")
    d = ImageDraw.Draw(sheet)
    y = M
    for title, note, cells in rows:
        if title:
            d.text((M, y), title, fill=INK, font=head)
            y += 58
        if note:
            d.text((M, y), note, fill=grey, font=text)
            y += 38
        for im, x in cells:
            sheet.paste(im, (x, y))
        y += max(im.height for im, _ in cells) + 40
    sheet.save(os.path.join(OUT, "overview.png"))
    os.remove(tmp)
    os.remove(tmp + ".png")


if __name__ == "__main__":
    drawn = main()
    place(drawn)
    overview(drawn)
