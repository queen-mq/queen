#!/usr/bin/env python3
"""The docs' picture: the queen bee flying to the sunflower.

In the logo's two colours (assets/logo.py): its yellow and one body colour,
every shape built from circles and arcs, and round each petal a contour in the
body colour. There is no background: the picture stands on the page, and what looks
like the page's colour inside a shape is a hole cut in it.

  the flower   Two rows of PETALS lens petals, the back row a half step round
               from the front one and a little longer, so its tips show
               between the front tips. Yellow, each with its contour.
  the head     A disc in the body colour, AIR away from the petals. Its seeds
               are holes: SEEDS of them on Vogel's spiral (one every golden
               angle), growing from the centre outward, inside a thin ring.
  the stem     One curve from behind the flower out of the bottom edge.
  the leaves   Two arcs each, drawn as a line, with a midrib. No fill: the
               flower is the only thing with weight.
  the bee      The queen, as assets/sunflower.py constructs her, flat: a body
               with its bands cut out of it, yellow wings with the petals'
               contour, a yellow crown.
  her trail    Dots along a curve, growing toward her.

One file per theme, the body in the theme's ink:
  webdoc/public/queen-scene-on-light.svg   ink, for a light page
  webdoc/public/queen-scene-on-dark.svg    paper, for a dark page

Run:  python3 assets/scene.py
"""
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sunflower as S  # noqa: E402
import logo as L  # noqa: E402

# the yellow is the dashboard's --brand and the docs' --nb-brand
PETAL, INK, PAPER = L.YELLOW, L.INK, L.PAPER
WIDTH, HEIGHT = S.SCENE

CENTRE, HEAD = (700.0, 596.0), 160.0    # the flower's centre, and the radius of its head
PETALS = 21                             # in each row
FRONT = (0.80, 2.12, 0.215)             # a petal's base, tip and half width, in head radii
BACK = (0.80, 2.36, 0.215)
LINE = 7.0                              # the contour, and every drawn line
AIR = 9.0                               # between the head and the petals, and round what passes behind
SEEDS = 233
SEED_FIELD = 0.86                       # the seeds fill the head out to here
SEED_GROWTH = 0.35                      # how much smaller than the mean at the centre, and larger at the rim
RING = (0.93, 0.02)                     # a ring cut in the head: its radius and width, in head radii

STEM = ((690.0, 596.0), (654.0, 1076.0), (500.0, 1268.0), (542.0, 1616.0))
STEM_WIDTH = 32.0
# where on the stem, which side, the way it points (degrees clockwise from up), length, half width, bow
LEAVES = ((0.56, -1, -64.0, 310.0, 66.0, 24.0), (0.74, 1, 66.0, 270.0, 58.0, -20.0))

BEE_AT = "translate(280,292) rotate(27) scale(0.50)"
TRAIL = ((360.0, 1350.0), (110.0, 1090.0), (64.0, 520.0), (180.0, 276.0))
TRAIL_DOTS = 30

_masks = [0]


def cut(holes, shapes):
    """`shapes` with `holes` cut out of them. A hole is any SVG shape; its stroke counts too."""
    _masks[0] += 1
    uid = f"m{_masks[0]}"
    return (f'<mask id="{uid}" maskUnits="userSpaceOnUse" x="-4000" y="-4000" width="8000" height="8000">'
            f'<rect x="-4000" y="-4000" width="8000" height="8000" fill="#fff"/>'
            f'<g fill="#000" stroke="#000" stroke-width="0">{holes}</g></mask><g mask="url(#{uid})">{shapes}</g>')


def polar(c, r, deg):
    """The point r from c, deg clockwise from straight up."""
    a = math.radians(deg)
    return (c[0] + r * math.sin(a), c[1] - r * math.cos(a))


def lens(p0, p1, half, bow=0.0):
    """Two arcs from p0 to p1, `half` wide each side of the line between them at its middle.
    `bow` moves both arcs the same way, so the lens bends: a leaf."""
    c = math.hypot(p1[0] - p0[0], p1[1] - p0[1])
    out = f"M{p0[0]:.2f},{p0[1]:.2f}"
    for bulge, to in ((half + bow, p1), (half - bow, p0)):
        r = (c * c / 4 + bulge * bulge) / (2 * abs(bulge))
        out += f"A{r:.2f},{r:.2f} 0 0 {int(bulge > 0)} {to[0]:.2f},{to[1]:.2f}"
    return out + "Z"


def row(turn, base, tip, half):
    """One row of petals as paths; `turn` is where the first one stands, in steps."""
    step = 360.0 / PETALS
    return "".join(f'<path d="{lens(polar(CENTRE, base * HEAD, (i + turn) * step), polar(CENTRE, tip * HEAD, (i + turn) * step), half * HEAD)}"/>'
                   for i in range(PETALS))


def flower(body, behind):
    """The flower, over `behind` (the stem), which it hides with AIR to spare."""
    cx, cy = CENTRE
    front, back = row(0, *FRONT), row(0.5, *BACK)
    petals = f'<g fill="{PETAL}" stroke="{body}" stroke-width="{LINE}" stroke-linejoin="round">{back}{front}</g>'
    air = f'<circle cx="{cx}" cy="{cy}" r="{HEAD + AIR}"/>'
    hidden = cut(f'<g stroke-width="{2 * AIR}" stroke-linejoin="round">{front}{back}</g>{air}', behind)
    # the seeds: seed k stands k golden angles round, at a distance that grows as the root of k
    step = SEED_FIELD * HEAD / math.sqrt(SEEDS)
    holes = ""
    for k in range(SEEDS):
        r = step * math.sqrt(k + 0.5)
        x, y = S._pt(cx, cy, r, k * S.GOLDEN)
        size = 0.46 * step * (1 - SEED_GROWTH + 2 * SEED_GROWTH * r / (SEED_FIELD * HEAD))
        holes += f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{size:.1f}"/>'
    holes += (f'<circle cx="{cx}" cy="{cy}" r="{RING[0] * HEAD:.1f}" fill="none" '
              f'stroke-width="{RING[1] * HEAD:.1f}"/>')
    return hidden + cut(air, petals) + cut(holes, f'<circle cx="{cx}" cy="{cy}" r="{HEAD}" fill="{body}"/>')


def on_stem(t):
    u = 1 - t
    return tuple(u ** 3 * a + 3 * u * u * t * b + 3 * u * t * t * c + t ** 3 * d for a, b, c, d in zip(*STEM))


def leaf(t, side, deg, length, half, bow):
    """A leaf as a line: its outline, and a midrib that follows its bend and touches neither end."""
    x, y = on_stem(t)
    base = (x + side * STEM_WIDTH * 0.15, y)
    tip = polar(base, length, deg)
    ax, ay = (tip[0] - base[0]) / length, (tip[1] - base[1]) / length

    def mid(s, bend):
        off = -bow * bend
        return (base[0] + ax * length * s - ay * off, base[1] + ay * length * s + ax * off)

    a, m, b = mid(0.08, 4 * 0.08 * 0.92), mid(0.46, 1.02), mid(0.84, 4 * 0.84 * 0.16)
    return (f'<path d="{lens(base, tip, half, bow)}" stroke-linejoin="round"/>'
            f'<path d="M{a[0]:.1f},{a[1]:.1f}Q{m[0]:.1f},{m[1]:.1f} {b[0]:.1f},{b[1]:.1f}" '
            f'stroke-width="{LINE * 0.8:.1f}" stroke-linecap="round"/>')


def bee(body):
    """sunflower.bee's construction, flat. She faces +x; back to front: far wing, abdomen, thorax,
    head, antenna, eye, crown, near wing."""
    wing = f'fill="{PETAL}" stroke="{body}" stroke-width="{LINE / 0.5:.1f}" stroke-linejoin="round"'
    o = [f'<g transform="translate(14,-52) rotate(-146)"><path d="{lens((0, 0), (228, 0), 46)}" {wing}/></g>']

    r0, tail = 106.0, 232.0
    whole = S._teardrop(r0, tail)[2]
    bands = "".join(f'<circle cx="330" cy="0" r="{330 - at}" fill="none" stroke-width="{w}"/>'
                    for at, w in ((-34, 30), (-102, 30), (-166, 28)))
    o.append('<g transform="translate(-60,8) rotate(8)">' + cut(bands, f'<path d="{whole}" fill="{body}"/>')
             + f'<path d="M {-tail + 8} -9 L {-tail - 20} 0 L {-tail + 8} 9 Z" fill="{body}"/></g>')

    hx, hy, hr = 150.0, 12.0, 54.0
    eye = f'<ellipse cx="{hx + 24}" cy="{hy - 8}" rx="15" ry="19"/>'
    o.append(f'<circle cx="42" cy="-2" r="76" fill="{body}"/>'
             + cut(eye, f'<circle cx="{hx}" cy="{hy}" r="{hr}" fill="{body}"/>'))
    bx, by = S._pt(hx, hy, hr - 8, -42)
    ex, ey = S._pt(bx, by, 36, -72)
    tx, ty = S._pt(ex, ey, 44, -18)
    o.append(f'<path d="M {bx:.1f} {by:.1f} L {ex:.1f} {ey:.1f} L {tx:.1f} {ty:.1f}" fill="none" stroke="{body}" '
             'stroke-width="12" stroke-linecap="round" stroke-linejoin="round"/>')
    o.append(f'<circle cx="{hx + 30}" cy="{hy - 6}" r="8" fill="{body}"/>')

    cx, cy = S._pt(hx, hy, hr - 12, -104)
    w, band, spike = 54.0, 24.0, 50.0
    side = -band - spike * 0.62
    crown = [(-w, 0), (-w, side), (-w / 2, -band), (0, -band - spike), (w / 2, -band), (w, side), (w, 0)]
    o.append(f'<g transform="translate({cx:.1f},{cy:.1f}) rotate(-22)" fill="{PETAL}">'
             f'<path d="M {" L ".join(f"{x:.1f} {y:.1f}" for x, y in crown)} Z"/>')
    o += [f'<circle cx="{x:.1f}" cy="{y:.1f}" r="10"/>' for x, y in ((-w, side), (0, -band - spike), (w, side))]
    o.append('</g>')

    o.append(f'<g transform="translate(46,-58) rotate(-104)"><path d="{lens((0, 0), (262, 0), 52)}" {wing}/></g>')
    return "".join(o)


def scene(body):
    """The picture with its bodies and lines in one colour."""
    _masks[0] = 0
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}" width="{WIDTH}" height="{HEIGHT}">']
    o.append(f'<g fill="none" stroke="{body}" stroke-width="{LINE}">' + "".join(leaf(*spec) for spec in LEAVES) + '</g>')
    (x0, y0), (x1, y1), (x2, y2), (x3, y3) = STEM
    o.append(flower(body, f'<path d="M {x0} {y0} C {x1} {y1}, {x2} {y2}, {x3} {y3}" fill="none" stroke="{body}" '
                          f'stroke-width="{STEM_WIDTH}"/>'))
    dots = S._even(TRAIL, TRAIL_DOTS)
    for k, (x, y) in enumerate(dots):
        o.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{1.6 + 8.4 * (k / (len(dots) - 1)) ** 1.4:.1f}" fill="{body}"/>')
    o.append(f'<g transform="{BEE_AT}">{bee(body)}</g>')
    return "".join(o) + "</svg>\n"


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for name, body in (("on-light", INK), ("on-dark", PAPER)):
        out = os.path.join(root, "webdoc", "public", f"queen-scene-{name}.svg")
        with open(out, "w") as fh:
            fh.write(scene(body))
        print("  ", os.path.relpath(out, root))


if __name__ == "__main__":
    main()
