"""The Queen sunflower, constructed: the badge (the queen bee flying to the
sunflower, on a sunset field), the small mark (the flower head alone) and the
square tiles. assets/generate-brand.py imports this and writes every file.

Every shape is built from circles, circular arcs and lenses. The two pieces of
the badge that are data rather than construction — the three hill lines and the
bee's dotted flight trail — are kept as they were drawn.

Coordinates: the badge is a 1024 grid; parts (flower, bee, leaf) are built in
local coordinates around their own origin and placed with an SVG transform.
"""
import math

GOLDEN = 137.50776405003785  # degrees

PLUM = "#4a2330"
RUST = "#7a3440"
ROSE = "#a8474a"
CORAL = "#e05b43"
CORAL_D = "#b8403f"
ORANGE = "#f08352"
AMBER = "#ec8a4a"
PEACH = "#f6ab69"
CREAM = "#fbd3b6"
GOLD = "#f7b955"
OLIVE = "#6b7038"
OLIVE_D = "#4f5429"
STEM = "#5a5f33"


def f(v):
    return f"{v:.2f}".rstrip("0").rstrip(".")


def _pt(cx, cy, r, deg):
    t = math.radians(deg)
    return cx + r * math.cos(t), cy + r * math.sin(t)


def lens_halves(r0, r1, width):
    """A petal (or a wing): the lens between r0 and r1 on the +x axis, split on
    its axis into two halves, each filled with its own tone."""
    c = r1 - r0
    s = width / 2
    R = (c * c / 4 + s * s) / (2 * s)
    a = f"M {f(r0)} 0 A {f(R)} {f(R)} 0 0 1 {f(r1)} 0 Z"
    b = f"M {f(r1)} 0 A {f(R)} {f(R)} 0 0 1 {f(r0)} 0 Z"
    return a, b


# ------------------------------------------------------------- the flower --

def flower(n=21, rot=0.0, head=150.0, back=(140, 345, 80), front=(140, 318, 72),
           seeds=89, ring=(14, 12), mono=None):
    """The flower head on the origin: n back petals (coral) and n front petals
    (gold) half a step later, each split in two tones on its axis; a plum head
    with a rust ring (inset, stroke width) and seeds on the golden-angle spiral,
    coloured so that 8 of every 21 parastichies are cream. `mono` draws the
    silhouette in one colour (the drop shadow)."""
    step = 360 / n
    out = []
    for r_set, cols, off in ((back, (CORAL, CORAL_D), 9), (front, (GOLD, AMBER), 9 + step / 2)):
        a, b = lens_halves(*r_set)
        ca, cb = (mono, mono) if mono else cols
        for i in range(n):
            out.append(f'<g transform="rotate({rot + off + i * step:.3f})">'
                       f'<path d="{a}" fill="{ca}"/><path d="{b}" fill="{cb}"/></g>')
    out.append(f'<circle r="{f(head)}" fill="{mono or PLUM}"/>')
    if mono:
        return "".join(out)
    if ring:
        out.append(f'<circle r="{f(head - ring[0])}" fill="none" stroke="{RUST}" stroke-width="{f(ring[1])}"/>')
    c = 118 * head / 150 / math.sqrt(max(seeds, 1))   # the seeds fill a disc 118/150 of the head
    for k in range(seeds):
        r = c * math.sqrt(k + 0.5)
        x, y = _pt(0, 0, r, k * GOLDEN)
        col = CREAM if k % 21 < 8 else ORANGE
        out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{f(6.3 * head / 150)}" fill="{col}"/>')
    return "".join(out)


# ---------------------------------------------------------------- the bee --
#
# Local frame: the bee faces +x, y points down. Back to front: far wing,
# abdomen, thorax, head, antennae, eye, crown, near wing.

def _teardrop(r0, tail):
    """The abdomen: a front circle of radius r0 on the origin, continued by two
    arcs, each tangent to it at its top or bottom, meeting on the axis at
    x = -tail. Returns (upper half, lower half, whole)."""
    d = (tail * tail - r0 * r0) / (2 * r0)
    R = r0 + d
    t, s, r = f(-tail), f(R), f(r0)
    up = f"M {t} 0 A {s} {s} 0 0 1 0 {f(-r0)} A {r} {r} 0 0 1 {r} 0 Z"
    lo = f"M {r} 0 A {r} {r} 0 0 1 0 {r} A {s} {s} 0 0 1 {t} 0 Z"
    whole = f"M {t} 0 A {s} {s} 0 0 1 0 {f(-r0)} A {r} {r} 0 0 1 0 {r} A {s} {s} 0 0 1 {t} 0 Z"
    return up, lo, whole


def bee(uid="bee", shadow=False):
    """The queen bee. A blunt teardrop abdomen with bowed bands (a sharp tail
    reads as a wasp), a two-tone thorax and head that stay visible on a plum
    sky, a crown turned back toward upright, and the eye on the flower.
    shadow=True draws the silhouette in black; the caller offsets and fades it."""
    def c(col):
        return "#000" if shadow else col

    o = []
    # far wing, one tone darker than the near one
    a, b = lens_halves(0, 228, 92)
    o.append(f'<g transform="translate(14,-52) rotate(-146)"><path d="{a}" fill="{c(PEACH)}"/>'
             f'<path d="{b}" fill="{c(ORANGE)}"/></g>')

    # abdomen: lit upper half, shaded lower half, three bands and a dark tail
    r0, tail = 106.0, 232.0
    up, lo, whole = _teardrop(r0, tail)
    o.append('<g transform="translate(-60,8) rotate(8)">')
    o.append(f'<path d="{up}" fill="{c(GOLD)}"/><path d="{lo}" fill="{c(AMBER)}"/>')
    if not shadow:
        # each band is an arc of a circle centred ahead of the abdomen, so it
        # bows toward the tail the way a body segment does
        o.append(f'<clipPath id="{uid}-abd"><path d="{whole}"/></clipPath><g clip-path="url(#{uid}-abd)">')
        bc = 330.0
        for xc, w in ((-34, 36), (-102, 36), (-166, 34)):
            o.append(f'<circle cx="{f(bc)}" cy="0" r="{f(bc - xc)}" fill="none" stroke="{PLUM}" stroke-width="{w}"/>')
        o.append(f'<circle cx="{f(bc)}" cy="0" r="{f(bc + 206 + 200)}" fill="none" stroke="{PLUM}" stroke-width="400"/>')
        o.append('</g>')
    o.append(f'<path d="M {f(-tail + 8)} -9 L {f(-tail - 20)} 0 L {f(-tail + 8)} 9 Z" fill="{c(PLUM)}"/>')
    o.append('</g>')

    # thorax and head: circles, lit on top
    for (x, y, r), (lower, upper) in (((42.0, -2.0, 76.0), (RUST, ROSE)), ((150.0, 12.0, 54.0), (PLUM, RUST))):
        o.append(f'<circle cx="{f(x)}" cy="{f(y)}" r="{f(r)}" fill="{c(lower)}"/>')
        o.append(f'<path d="M {f(x - r)} {f(y)} A {f(r)} {f(r)} 0 0 1 {f(x + r)} {f(y)} Z" fill="{c(upper)}"/>')
    hx, hy, hr = 150.0, 12.0, 54.0

    # antennae: elbowed, from the brow, clear of the crown
    bx, by = _pt(hx, hy, hr - 8, -42)
    ex, ey = _pt(bx, by, 36, -72)
    tx, ty = _pt(ex, ey, 44, -18)
    o.append(f'<path d="M {f(bx)} {f(by)} L {f(ex)} {f(ey)} L {f(tx)} {f(ty)}" fill="none" stroke="{c(PLUM)}" '
             'stroke-width="12" stroke-linecap="round" stroke-linejoin="round"/>')

    if not shadow:
        o.append(f'<ellipse cx="{f(hx + 24)}" cy="{f(hy - 8)}" rx="15" ry="19" fill="{CREAM}"/>')
        o.append(f'<circle cx="{f(hx + 30)}" cy="{f(hy - 6)}" r="8" fill="{PLUM}"/>')

    # crown: a band and three spikes, split in two tones on its axis
    cx, cy = _pt(hx, hy, hr - 12, -104)
    w, band, spike = 54.0, 24.0, 50.0
    side = -band - spike * 0.62
    left = [(-w, 0), (-w, side), (-w / 2, -band), (0, -band - spike), (0, 0)]
    right = [(0, 0), (0, -band - spike), (w / 2, -band), (w, side), (w, 0)]

    def poly(p):
        return "M " + " L ".join(f"{f(x)} {f(y)}" for x, y in p) + " Z"

    o.append(f'<g transform="translate({f(cx)},{f(cy)}) rotate(-22)">'
             f'<path d="{poly(left)}" fill="{c(GOLD)}"/><path d="{poly(right)}" fill="{c(AMBER)}"/>')
    if not shadow:
        for x, y in ((-w, side), (0, -band - spike), (w, side)):
            o.append(f'<circle cx="{f(x)}" cy="{f(y)}" r="10" fill="{CREAM}"/>')
    o.append('</g>')

    # near wing, in front of the body
    a, b = lens_halves(0, 262, 104)
    o.append(f'<g transform="translate(46,-58) rotate(-104)"><path d="{a}" fill="{c(CREAM)}"/>'
             f'<path d="{b}" fill="{c(PEACH)}"/></g>')
    return "".join(o)


# --------------------------------------------------------------- the leaf --

def leaf(length, half_w=0.25, p=0.55, q=1.45, droop=0.12, n=120):
    """A sunflower leaf from its base (the origin) along +x: the half-width
    follows t^p (1-t)^q, so the base is round (p < 1), the widest point sits
    at p/(p+q) of the length and the tip is drawn out (q > 1). The axis is then
    bent into a circular arc so the blade droops. Two tones, split on the midrib,
    like the petals."""
    tmax = p / (p + q)
    norm = tmax ** p * (1 - tmax) ** q
    R = length / (2 * droop)

    def pt(t, side):
        x = t * length
        y = side * half_w * length * t ** p * (1 - t) ** q / norm
        a = x / R
        return f"{(R - y) * math.sin(a):.1f} {R - (R - y) * math.cos(a):.1f}"

    ts = [i / n for i in range(n + 1)]
    up = " L ".join(pt(t, -1) for t in ts)
    mid = [pt(t, 0) for t in ts]
    lo = " L ".join(pt(t, 1) for t in reversed(ts))
    return (f'<path d="M {up} L {" L ".join(reversed(mid))} Z" fill="{OLIVE}"/>'
            f'<path d="M {" L ".join(mid)} L {lo} Z" fill="{OLIVE_D}"/>')


# -------------------------------------------------------------- the badge --

SKY = ["#b8403f", "#cf4b3f", "#e05b43", "#ea6e4a", "#f08352", "#f4985c", "#f6ab69"]

# The three hills, back to front: y at x = -10, 2, 14, ... 1034 (every 12 px).
HILLS = [
    ("#a8474a", [652.2, 651.5, 651.4, 651.8, 652.8, 654.2, 656.0, 658.2, 660.8, 663.5, 666.4, 669.4, 672.4, 675.4, 678.2, 681.0, 683.5, 685.8, 687.8, 689.5, 691.0, 692.2, 693.2, 693.9, 694.4, 694.8, 695.0, 695.3, 695.5, 695.7, 696.1, 696.6, 697.3, 698.3, 699.5, 701.0, 702.9, 705.1, 707.5, 710.3, 713.4, 716.6, 720.1, 723.6, 727.3, 730.9, 734.4, 737.8, 741.0, 743.8, 746.3, 748.4, 750.0, 751.0, 751.5, 751.5, 750.8, 749.5, 747.6, 745.2, 742.2, 738.7, 734.9, 730.6, 726.1, 721.4, 716.5, 711.5, 706.6, 701.8, 697.2, 692.8, 688.8, 685.1, 681.8, 678.9, 676.4, 674.4, 672.9, 671.8, 671.1, 670.8, 670.8, 671.1, 671.6, 672.3, 673.1, 674.0]),
    ("#7a3440", [799.1, 800.8, 802.2, 803.4, 804.4, 805.2, 805.8, 806.2, 806.5, 806.7, 806.7, 806.8, 806.8, 806.7, 806.8, 806.8, 807.0, 807.2, 807.6, 808.1, 808.7, 809.5, 810.5, 811.6, 812.9, 814.4, 816.0, 817.8, 819.6, 821.6, 823.7, 825.8, 827.9, 830.0, 832.1, 834.1, 836.0, 837.7, 839.3, 840.7, 841.8, 842.6, 843.2, 843.4, 843.3, 842.9, 842.0, 840.8, 839.3, 837.3, 835.0, 832.4, 829.4, 826.0, 822.4, 818.5, 814.4, 810.1, 805.6, 800.9, 796.2, 791.5, 786.7, 782.0, 777.3, 772.8, 768.5, 764.3, 760.4, 756.7, 753.3, 750.3, 747.5, 745.2, 743.1, 741.5, 740.2, 739.3, 738.7, 738.5, 738.6, 739.0, 739.8, 740.7, 741.9, 743.3, 744.9, 746.7]),
    ("#4a2330", [905.9, 906.6, 907.4, 908.3, 909.0, 909.7, 910.2, 910.5, 910.6, 910.5, 910.0, 909.1, 907.9, 906.3, 904.3, 901.9, 899.1, 895.9, 892.4, 888.6, 884.4, 880.1, 875.5, 870.8, 866.1, 861.3, 856.7, 852.1, 847.7, 843.6, 839.8, 836.4, 833.4, 830.9, 828.8, 827.2, 826.2, 825.7, 825.7, 826.3, 827.3, 828.9, 830.8, 833.1, 835.8, 838.8, 841.9, 845.3, 848.7, 852.3, 855.8, 859.2, 862.5, 865.7, 868.7, 871.4, 873.9, 876.1, 878.1, 879.7, 881.1, 882.3, 883.1, 883.8, 884.3, 884.7, 884.9, 885.1, 885.3, 885.5, 885.8, 886.2, 886.8, 887.5, 888.5, 889.7, 891.1, 892.8, 894.7, 896.8, 899.2, 901.7, 904.4, 907.2, 910.1, 913.0, 915.9, 918.6]),
]

# The bee's flight trail rising from the field: (x, y, r, opacity).
TRAIL = [(204.0, 592.0, 3.5, 0.35), (188.0, 576.4, 3.8, 0.39), (173.7, 560.2, 4.1, 0.43), (161.0, 543.5, 4.4, 0.46),
         (150.0, 526.1, 4.8, 0.50), (140.6, 508.2, 5.1, 0.54), (132.9, 489.6, 5.4, 0.58), (126.8, 470.5, 5.7, 0.61),
         (122.4, 450.8, 6.0, 0.65), (119.6, 430.5, 6.3, 0.69), (118.5, 409.6, 6.6, 0.73), (119.0, 388.1, 6.9, 0.76),
         (121.1, 366.1, 7.3, 0.80), (124.9, 343.4, 7.6, 0.84), (130.4, 320.1, 7.9, 0.88), (137.5, 296.3, 8.2, 0.91)]

# The stem: one cubic from under the flower head out of the bottom of the badge.
STEM_PATH = ((567.5676, 564.8649), (557.5676, 784.8649), (457.2973, 764.0), (397.2973, 1044.0))

FLOWER_AT = "translate(600,500) rotate(-18) scale(0.9)"
BEE_AT = "translate(236,318) rotate(26.6) scale(0.33)"


def _stem_point(t):
    u = 1 - t
    return tuple(u ** 3 * a + 3 * u * u * t * b + 3 * u * t * t * c + t ** 3 * d for a, b, c, d in zip(*STEM_PATH))


def badge_svg(sky=None, rim=True):
    """The badge. sky=None keeps the sunset bands; a colour makes the sky one
    flat fill. rim=False drops the white rim and lets the picture grow out to
    where the rim's outer edge was, so the badge keeps its size."""
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1024 1024" width="1024" height="1024">'
         f'<defs><clipPath id="c"><circle cx="512" cy="512" r="{472 if rim else 496}"/></clipPath></defs>'
         '<g clip-path="url(#c)">']
    if sky:
        o.append(f'<rect width="1024" height="1024" fill="{sky}"/>')
    else:
        for i, col in enumerate(SKY):
            o.append(f'<rect x="0" y="{i * 110}" width="1024" height="111" fill="{col}"/>')
        for i in range(1, 7):
            o.append(f'<rect x="0" y="{i * 110 - 1.5}" width="1024" height="3" fill="{CREAM}" opacity="0.45"/>')
    for col, ys in HILLS:
        pts = " ".join(f"L {-10 + 12 * i} {y}" for i, y in enumerate(ys))
        o.append(f'<path d="M -10 1100 {pts} L 1044 1100 Z" fill="{col}"/>')

    (x0, y0), (x1, y1), (x2, y2), (x3, y3) = STEM_PATH
    o.append(f'<path d="M {x0} {y0} C {x1} {y1}, {x2} {y2}, {x3} {y3}" fill="none" stroke="{STEM}" '
             'stroke-width="42.2" stroke-linecap="round"/>')
    # the leaf, on a short petiole out of the stem
    sx, sy = _stem_point(0.58)
    nx, ny = sx - 40, sy - 30
    o.append(f'<path d="M {sx:.1f} {sy:.1f} Q {(sx + nx) / 2:.1f} {min(sy, ny) - 14:.1f} {nx:.1f} {ny:.1f}" '
             f'fill="none" stroke="{STEM}" stroke-width="15" stroke-linecap="round"/>')
    o.append(f'<g transform="translate({nx:.1f},{ny:.1f}) rotate(200) scale(1,-1)">{leaf(248)}</g>')

    o.append(f'<g transform="translate(12,18)" opacity="0.25"><g transform="{FLOWER_AT}">{flower(mono="#000")}</g></g>')
    o.append(f'<g transform="{FLOWER_AT}">{flower()}</g>')
    for x, y, r, op in TRAIL:
        o.append(f'<circle cx="{x}" cy="{y}" r="{r}" fill="{CREAM}" opacity="{op:.2f}"/>')
    o.append(f'<g transform="translate(8,12)" opacity="0.25"><g transform="{BEE_AT}">{bee(shadow=True)}</g></g>')
    o.append(f'<g transform="{BEE_AT}">{bee()}</g>')
    o.append('</g>')
    if rim:
        o.append('<circle cx="512" cy="512" r="484" fill="none" stroke="#ffffff" stroke-width="24"/>')
    return "".join(o) + "</svg>"


# -------------------------------------------------------------- the scene --
#
# The badge's picture as a full-bleed panel for the docs homepage hero, which
# fills the right 45% of the screen from the header down: the same parts,
# rearranged for a column rather than a disc. A sunflower is a tall plant, so
# the panel gives it its stem: the head high, two alternate leaves, the hills at
# the foot, and the bee's trail rising from the field.
#
# The canvas is 1200 x 1536 (0.78), the middle of what that column is on real
# screens (0.65 on a 1024 x 768 laptop, 0.85 on a 1920 x 1080 monitor). The page
# crops it with object-fit: cover, so everything that matters sits inside a
# safe area that survives both ends: about 100 px off each side (narrow) and
# about 100 px off the top and bottom (wide). The sky and the hills run to the
# edges and lose nothing when they are cut.

SCENE = (1200, 1536)
SCENE_FLOWER_AT = "translate(700,640) rotate(-18) scale(0.98)"
SCENE_BEE_AT = "translate(318,362) rotate(37) scale(0.40)"
SCENE_STEM = ((668.0, 710.0), (656.0, 990.0), (535.0, 1235.0), (566.0, 1640.0))
SCENE_TRAIL = ((300.0, 1215.0), (150.0, 1000.0), (128.0, 520.0), (200.0, 310.0))
SCENE_HILLS_DY = 552  # the badge's hills, lowered to the foot of the panel


def _cubic(pts, t):
    u = 1 - t
    return tuple(u ** 3 * a + 3 * u * u * t * b + 3 * u * t * t * c + t ** 3 * d for a, b, c, d in zip(*pts))


def _even(pts, n):
    """n points spaced evenly by arc length along a cubic."""
    fine = [_cubic(pts, i / 600) for i in range(601)]
    acc = [0.0]
    for (x0, y0), (x1, y1) in zip(fine, fine[1:]):
        acc.append(acc[-1] + math.hypot(x1 - x0, y1 - y0))
    out, j = [], 0
    for k in range(n):
        target = acc[-1] * k / (n - 1)
        while j < len(acc) - 1 and acc[j] < target:
            j += 1
        out.append(fine[j])
    return out


def _leaf_on_stem(stem, t, dx, dy, ang, length, mirror):
    sx, sy = _cubic(stem, t)
    nx, ny = sx + dx, sy + dy
    pet = (f'<path d="M {sx:.1f} {sy:.1f} Q {(sx + nx) / 2:.1f} {min(sy, ny) - 14:.1f} {nx:.1f} {ny:.1f}" '
           f'fill="none" stroke="{STEM}" stroke-width="16" stroke-linecap="round"/>')
    flip = " scale(1,-1)" if mirror else ""
    return pet + f'<g transform="translate({nx:.1f},{ny:.1f}) rotate({ang}){flip}">{leaf(length)}</g>'


def scene_svg(sky=PLUM):
    W, H = SCENE
    sx = (W + 24) / 1044  # stretch the badge's hills (x from -10 to 1034) to the panel's width
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}">'
         f'<defs><clipPath id="c"><rect width="{W}" height="{H}"/></clipPath></defs><g clip-path="url(#c)">'
         f'<rect width="{W}" height="{H}" fill="{sky}"/>']
    for col, ys in HILLS:
        pts = " ".join(f"L {(-10 + 12 * i) * sx - 2:.1f} {y + SCENE_HILLS_DY:.1f}" for i, y in enumerate(ys))
        o.append(f'<path d="M -12 {H + 80} {pts} L {W + 24} {H + 80} Z" fill="{col}"/>')
    (x0, y0), (x1, y1), (x2, y2), (x3, y3) = SCENE_STEM
    o.append(f'<path d="M {x0} {y0} C {x1} {y1}, {x2} {y2}, {x3} {y3}" fill="none" stroke="{STEM}" '
             'stroke-width="46" stroke-linecap="round"/>')
    # alternate leaves: the upper one reaches left, the lower one right
    o.append(_leaf_on_stem(SCENE_STEM, 0.48, -44, -30, 200, 292, mirror=True))
    o.append(_leaf_on_stem(SCENE_STEM, 0.74, 46, -26, -18, 262, mirror=False))
    o.append(f'<g transform="translate(12,18)" opacity="0.25"><g transform="{SCENE_FLOWER_AT}">{flower(mono="#000")}</g></g>')
    o.append(f'<g transform="{SCENE_FLOWER_AT}">{flower()}</g>')
    dots = _even(SCENE_TRAIL, 26)
    for k, (x, y) in enumerate(dots):
        t = k / (len(dots) - 1)
        o.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{3.5 + 5.5 * t:.1f}" fill="{CREAM}" opacity="{0.30 + 0.62 * t:.2f}"/>')
    o.append(f'<g transform="translate(9,13)" opacity="0.25"><g transform="{SCENE_BEE_AT}">{bee(shadow=True)}</g></g>')
    o.append(f'<g transform="{SCENE_BEE_AT}">{bee()}</g>')
    return "".join(o) + "</g></svg>"


# ---------------------------------------------------------- the small mark --

UPRIGHT = -90 - 9 - 360 / 21 / 2   # turns the 21-petal head so a front petal points up


MARK_VIEWBOX = "-349 -349 698 698"


def mark_body(n=21):
    """The flower head alone, upright (a front petal points straight up), with a
    plain head: at 16-48 px the badge's seeds turn to speckle and the bee to a
    smudge. n=21 like the badge; n=13 for 16 px, where 21 petals blur into a
    ring. Both are Fibonacci numbers, like the real flower's spirals."""
    step = 360 / n
    k = 21 / n
    head = 160.0 if n == 21 else 165.0
    return flower(n=n, rot=-90 - 9 - step / 2, head=head, seeds=0, ring=(18, 20),
                  back=(head - 12, 345, 80 * k), front=(head - 12, 318, 72 * k))


def mark_svg(n=21):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{MARK_VIEWBOX}" width="256" height="256">'
            f'{mark_body(n)}</svg>')


def square_svg(bee_too=True):
    """A full-bleed plum square: the flower head with its seeds and, if asked,
    the queen bee flying in. GitHub avatar (with the bee), apple-touch-icon and
    schema.org logo (without: they are seen small)."""
    s = 512 / 345
    o = ['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1024 1024" width="1024" height="1024">'
         f'<rect width="1024" height="1024" fill="{PLUM}"/>']
    if bee_too:
        o.append(f'<g transform="translate(548,548) scale({0.74 * s:.4f})">{flower(rot=UPRIGHT)}</g>')
        o.append(f'<g transform="translate(212,214) rotate(40) scale(0.40)">{bee()}</g>')
    else:
        o.append(f'<g transform="translate(512,512) scale({0.80 * s:.4f})">{flower(rot=UPRIGHT)}</g>')
    return "".join(o) + "</svg>"
