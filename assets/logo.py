#!/usr/bin/env python3
"""Queen's logo, from Alberto's drawing: assets/queen_final.svg.

The drawing is the source, and this script never rewrites it. It reads the three
groups of the drawing and writes every file the project uses out of them, so
the shapes everywhere are the drawn ones, point for point:

  sunflower       seven petals round the q's bowl, yellow
  queen           the five letters; the q's bowl is the flower's head
  message-queue   the line under "ueen"

Only what one drawing on a dark ground cannot be is added here: the ground
taken away, the frame drawn tight, the letters in ink for a light page, the q
alone for small sizes, one colour, and the badge.

  assets/logo/queen-tagline{,-on-dark}.svg        the logo
  assets/logo/queen{,-on-dark}.svg                without its line
  assets/logo/q{,-on-dark}.svg                    the small logo: the q and its flower
  assets/logo/*-mono{,-on-dark}.svg               each of the three in one colour
  assets/logo/q-auto.svg                          the small logo, its letter following the colour scheme
  assets/logo/q{,-on-dark}-{16,32,48}.png         the small logo as icons
  assets/logo/q-tile{,-on-dark}.svg, -512.png     the small logo on a square ground
  assets/logo/q-badge{,-on-dark}.svg, -512.png    the badge: a disc with the small logo cut out of it
  assets/logo/overview.png                        everything on one sheet, for looking at

and the copies the dashboard (app/), its sign-in page and the docs (webdoc/) use.

Run:  python3 assets/logo.py     (needs rsvg-convert and Pillow)
"""
import math
import os
import re
import shutil
import subprocess
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(HERE, "queen_final.svg")
OUT = os.path.join(HERE, "logo")
SVG = {"s": "http://www.w3.org/2000/svg"}

INK = "#1f1d1a"                     # the letters on a light page: the one colour the drawing does not have
MUTED = ("#4f4b45", "#cdc8c0")      # the badge, on a light ground and on a dark one
PAD = 2.0                           # round a tight frame, in the drawing's units
BADGE = 1.12                        # the badge's disc, in radii of the flower

ARITY = {"M": 2, "L": 2, "H": 1, "V": 1, "C": 6, "Q": 4, "Z": 0}
TOKEN = re.compile(r"[A-Za-z]|-?\d*\.?\d+(?:[eE][-+]?\d+)?")


def num(v):
    return f"{v:.4f}".rstrip("0").rstrip(".")


def segments(d):
    """Path data as [(command, [numbers])] with only M, L, C, Q and Z left: repeats written out,
    H and V as the lines they are. The drawing uses absolute commands only."""
    out, tokens, i, at = [], TOKEN.findall(d), 0, (0.0, 0.0)
    while i < len(tokens):
        cmd, i = tokens[i], i + 1
        assert cmd in ARITY, f"a path command this script does not read: {cmd}"
        if cmd == "Z":
            out.append(("Z", []))
            continue
        first = True
        while i < len(tokens) and tokens[i] not in ARITY:
            v, i = [float(t) for t in tokens[i:i + ARITY[cmd]]], i + ARITY[cmd]
            if cmd == "H":
                out.append(("L", [v[0], at[1]]))
            elif cmd == "V":
                out.append(("L", [at[0], v[0]]))
            else:
                out.append(("L" if cmd == "M" and not first else cmd, v))
            at, first = tuple(out[-1][1][-2:]), False
    return out


def data(segs):
    return "".join(cmd + " ".join(num(v) for v in values) for cmd, values in segs)


def shifted(d, dx, dy):
    """The same path moved by (dx, dy)."""
    return data([(cmd, [v + (dx, dy)[k % 2] for k, v in enumerate(values)]) for cmd, values in segments(d)])


def bezier(c, t):
    while len(c) > 1:
        c = [a + (b - a) * t for a, b in zip(c, c[1:])]
    return c[0]


def turning(c):
    """Where a Bezier of one coordinate turns back, inside (0, 1)."""
    if len(c) == 3:
        a, b = c[0] - 2 * c[1] + c[2], c[0] - c[1]
        roots = [b / a] if a else []
    else:
        a, b, k = -c[0] + 3 * c[1] - 3 * c[2] + c[3], 2 * (c[0] - 2 * c[1] + c[2]), c[1] - c[0]
        if abs(a) < 1e-12:
            roots = [-k / b] if b else []
        else:
            root = b * b - 4 * a * k
            roots = [(-b + s * math.sqrt(root)) / (2 * a) for s in (1, -1)] if root >= 0 else []
    return [t for t in roots if 0 < t < 1]


def curves(d):
    """The path's pieces as lists of control points, the lines among them as two points."""
    at = start = None
    for cmd, v in segments(d):
        if cmd == "M":
            at = start = (v[0], v[1])
        elif cmd == "Z":
            if at != start:
                yield [at, start]
            at = start
        else:
            points = [at] + [(v[k], v[k + 1]) for k in range(0, len(v), 2)]
            yield points
            at = points[-1]


def extent(*ds):
    """The box of path data: (x0, y0, x1, y1), with the curves at their true extremes."""
    xs, ys = [], []
    for d in ds:
        for points in curves(d):
            for axis, out in ((0, xs), (1, ys)):
                c = [p[axis] for p in points]
                out.extend(bezier(c, t) for t in [0.0, 1.0] + (turning(c) if len(c) > 2 else []))
    return min(xs), min(ys), max(xs), max(ys)


def reach(centre, *ds):
    """How far the paths go from a point."""
    return max(math.hypot(bezier([p[0] for p in points], t / 24) - centre[0],
                          bezier([p[1] for p in points], t / 24) - centre[1])
               for d in ds for points in curves(d) for t in range(25))


def tight(box, pad=PAD):
    """A frame PAD round a box, as a viewBox."""
    x0, y0, x1, y1 = box
    return (x0 - pad, y0 - pad, x1 - x0 + 2 * pad, y1 - y0 + 2 * pad)


def square(box, margin):
    """A square frame round a box, the margin a share of its side."""
    x0, y0, x1, y1 = box
    side = max(x1 - x0, y1 - y0) / (1 - 2 * margin)
    return ((x0 + x1 - side) / 2, (y0 + y1 - side) / 2, side, side)


def drawing():
    """What the drawing holds: its ground, and each group's fill, fill rule and paths, every path
    moved to where its group and its own transform put it."""
    def offset(transform):
        if not transform:
            return 0.0, 0.0
        found = re.fullmatch(r"translate\(([-\d.]+),([-\d.]+)\)", transform)
        assert found, f"a transform this script does not read: {transform}"
        return float(found.group(1)), float(found.group(2))

    root = ET.parse(SRC).getroot()
    groups = {}
    for g in root.findall("s:g", SVG):
        gx, gy = offset(g.get("transform"))
        paths = []
        for p in g.findall("s:path", SVG):
            px, py = offset(p.get("transform"))
            paths.append(shifted(p.get("d"), gx + px, gy + py))
        groups[g.get("id")] = (g.get("fill").lower(), g.get("fill-rule", "nonzero"), paths)
    return root.find("s:rect", SVG).get("fill").lower(), groups


GROUND, GROUPS = drawing()
YELLOW, PETAL_RULE, (PETALS,) = GROUPS["sunflower"]
PAPER, LETTER_RULE, LETTERS = GROUPS["queen"]
LINE_PAPER, LINE_RULE, LINE = GROUPS["message-queue"]
Q, LINE = LETTERS[0], "".join(LINE)


def shape(d, fill, rule="nonzero"):
    paint = fill if fill.startswith("class=") else f'fill="{fill}"'
    even = ' fill-rule="evenodd"' if rule == "evenodd" else ""
    return f'<path {paint}{even} d="{d}"/>'


def doc(view, title, *shapes, ground=None, style=""):
    """An SVG file: its frame, its title, and its shapes from the back."""
    x, y, w, h = view
    back = f'<rect x="{num(x)}" y="{num(y)}" width="{num(w)}" height="{num(h)}" fill="{ground}"/>' if ground else ""
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{num(x)} {num(y)} {num(w)} {num(h)}" '
            f'width="{num(w)}" height="{num(h)}" role="img" aria-label="{title}"><title>{title}</title>'
            f'{style}{back}{"".join(shapes)}</svg>\n')


def inline(view, *shapes):
    """For a page that inlines it and sizes it with its own CSS: a shape filled "currentColor"
    takes the page's text colour."""
    x, y, w, h = view
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{num(x)} {num(y)} {num(w)} {num(h)}" '
            f'aria-hidden="true" focusable="false">{"".join(shapes)}</svg>\n')


def write(text, *path, root=OUT):
    out = os.path.join(root, *path)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as fh:
        fh.write(text)
    return out


def png(src, out, size):
    subprocess.run(["rsvg-convert", src, "-w", str(size), "-h", str(size), "-o", out], check=True)
    return out


def lockup(name):
    """One of the three forms of the logo: its frame, and its shapes given the colour of the petals,
    of the letters and of the line."""
    letters = {"queen-tagline": LETTERS, "queen": LETTERS, "q": [Q]}[name]
    line = LINE if name == "queen-tagline" else ""
    view = tight(extent(PETALS, *letters, *([line] if line else [])))

    def shapes(petals, ink, line_ink):
        return ([shape(PETALS, petals, PETAL_RULE), shape("".join(letters), ink, LETTER_RULE)]
                + ([shape(line, line_ink, LINE_RULE)] if line else []))

    return view, shapes


def badge():
    """The badge: a disc round the flower, with the q and its petals cut out of it. Even-odd does the
    cutting: the q's counter, inside the cut, is the disc again."""
    x0, y0, x1, y1 = extent(Q[Q.index("M", 1):])                     # the counter: the flower's eye
    cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
    r = BADGE * reach((cx, cy), PETALS, Q)
    disc = (f"M{num(cx - r)} {num(cy)}A{num(r)} {num(r)} 0 1 0 {num(cx + r)} {num(cy)}"
            f"A{num(r)} {num(r)} 0 1 0 {num(cx - r)} {num(cy)}Z")
    return (cx - r, cy - r, 2 * r, 2 * r), disc + Q + PETALS


def main():
    """Write assets/logo/ afresh."""
    shutil.rmtree(OUT, ignore_errors=True)
    tones = (("", INK, INK, PAPER), ("-on-dark", PAPER, LINE_PAPER, GROUND))    # suffix, letters, line, ground

    for name, title in (("queen-tagline", "Queen, message queue"), ("queen", "Queen"), ("q", "Queen")):
        view, shapes = lockup(name)
        for suffix, ink, line_ink, _ in tones:
            write(doc(view, title, *shapes(YELLOW, ink, line_ink)), f"{name}{suffix}.svg")
            write(doc(view, title, *shapes(ink, ink, ink)), f"{name}-mono{suffix}.svg")

    # the small logo in a square: icons, a tile with a ground, and the one that follows the colour scheme
    _, shapes = lockup("q")
    art = extent(PETALS, Q)
    for suffix, ink, _, ground in tones:
        src = write(doc(square(art, 0.03), "Queen", *shapes(YELLOW, ink, ink)), f".icon{suffix}.svg")
        for size in (16, 32, 48):
            png(src, os.path.join(OUT, f"q{suffix}-{size}.png"), size)
        os.remove(src)
        src = write(doc(square(art, 0.17), "Queen", *shapes(YELLOW, ink, ink), ground=ground), f"q-tile{suffix}.svg")
        png(src, os.path.join(OUT, f"q-tile{suffix}-512.png"), 512)
    scheme = f"<style>.l{{fill:{INK}}}@media (prefers-color-scheme:dark){{.l{{fill:{PAPER}}}}}</style>"
    write(doc(square(art, 0.03), "Queen", *shapes(YELLOW, 'class="l"', ""), style=scheme), "q-auto.svg")

    view, cut = badge()
    for suffix, tone in zip(("", "-on-dark"), MUTED):
        src = write(doc(view, "Queen", shape(cut, tone, "evenodd")), f"q-badge{suffix}.svg")
        png(src, os.path.join(OUT, f"q-badge{suffix}-512.png"), 512)


def place():
    """The copies the dashboard (app/) and the docs (webdoc/) use. Inlined ones are lettered
    "currentColor", so they take the text colour of the page's theme."""
    from PIL import Image

    site = lambda *path: os.path.join(ROOT, *path)
    view, shapes = lockup("q")
    for name in ("app", "webdoc"):
        write(inline(view, *shapes(YELLOW, "currentColor", "")), name, "src", "assets", "q.svg", root=ROOT)
    view, shapes = lockup("queen")
    write(inline(view, *shapes(YELLOW, "currentColor", "")), "webdoc", "src", "assets", "queen.svg", root=ROOT)
    # the logo with its line, for the dashboard's sign-in page
    view, shapes = lockup("queen-tagline")
    write(inline(view, *shapes(YELLOW, "currentColor", "currentColor")), "app", "public", "queen-wordmark.svg",
          root=ROOT)

    for name in ("app", "webdoc"):
        shutil.copyfile(os.path.join(OUT, "q-auto.svg"), site(name, "public", "favicon.svg"))
        shutil.copyfile(os.path.join(OUT, "q-32.png"), site(name, "public", "favicon-32.png"))
    sizes = [Image.open(os.path.join(OUT, f"q-{n}.png")).convert("RGBA") for n in (48, 32, 16)]
    sizes[0].save(site("webdoc", "public", "favicon.ico"), sizes=[(16, 16), (32, 32), (48, 48)],
                  append_images=sizes[1:])
    shutil.copyfile(os.path.join(OUT, "q-tile-on-dark-512.png"), site("webdoc", "public", "queen-tile.png"))
    png(os.path.join(OUT, "q-tile-on-dark.svg"), site("webdoc", "public", "apple-touch-icon.png"), 180)


def overview():
    """One sheet with everything, for looking at: assets/logo/overview.png."""
    from PIL import Image, ImageDraw, ImageFont

    M, G, WIDE = 60, 24, 2400
    text = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 22)
    grey, page, dark, light = "#8a847c", "#e9e6e1", GROUND, PAPER
    tmp = os.path.join(OUT, ".panel.png")

    def panel(file, w, h, ground, fill=0.80):
        """A file of the set, centred in a w x h panel of the given ground."""
        src = os.path.join(OUT, file)
        x, y, vw, vh = (float(v) for v in re.search(r'viewBox="([^"]+)"', open(src).read()).group(1).split())
        k = min(w * fill / vw, h * fill / vh)
        subprocess.run(["rsvg-convert", src, "-w", str(round(vw * k)), "-h", str(round(vh * k)), "-o", tmp], check=True)
        art, out = Image.open(tmp).convert("RGBA"), Image.new("RGB", (w, h), ground)
        out.paste(art, ((w - art.width) // 2, (h - art.height) // 2), art)
        return out

    def icons(suffix, w, h, ground):
        """The icons at their size, and each one enlarged pixel for pixel."""
        out, x = Image.new("RGB", (w, h), ground), 40
        for size in (16, 32, 48):
            icon = Image.open(os.path.join(OUT, f"q{suffix}-{size}.png")).convert("RGBA")
            out.paste(icon, (x, (h - size) // 2), icon)
            big = icon.resize((size * 4, size * 4), Image.NEAREST)
            out.paste(big, (x + size + 24, (h - size * 4) // 2), big)
            x += size * 5 + 70
        return out

    half, third, quarter = (WIDE - 2 * M - G) // 2, (WIDE - 2 * M - 2 * G) // 3, (WIDE - 2 * M - 3 * G) // 4
    rows = [
        ("the logo", [panel("queen-tagline-on-dark.svg", half, 620, dark), panel("queen-tagline.svg", half, 620, light)]),
        ("without its line, and the small logo",
         [panel("queen-on-dark.svg", quarter, 320, dark), panel("queen.svg", quarter, 320, light),
          panel("q-on-dark.svg", quarter, 320, dark), panel("q.svg", quarter, 320, light)]),
        ("one colour",
         [panel("queen-tagline-mono-on-dark.svg", quarter, 320, dark), panel("queen-tagline-mono.svg", quarter, 320, light),
          panel("q-mono-on-dark.svg", quarter, 320, dark), panel("q-mono.svg", quarter, 320, light)]),
        ("the badge, and the small logo on a ground",
         [panel("q-badge-on-dark.svg", quarter, 320, "#0b0a0a"), panel("q-badge.svg", quarter, 320, "#f2f0ec"),
          panel("q-tile-on-dark.svg", quarter, 320, page, fill=0.86), panel("q-tile.svg", quarter, 320, page, fill=0.86)]),
        ("icons: 16, 32 and 48 pixels, each beside itself four times enlarged",
         [icons("-on-dark", half, 260, dark), icons("", half, 260, light)]),
    ]
    height = M + sum(40 + row[1][0].height + G for row in rows) + M - G
    sheet = Image.new("RGB", (WIDE, height), page)
    pen, y = ImageDraw.Draw(sheet), M
    for label, panels in rows:
        pen.text((M, y), label, font=text, fill=grey)
        x, y = M, y + 40
        for image in panels:
            sheet.paste(image, (x, y))
            x += image.width + G
        y += panels[0].height + G
    os.remove(tmp)
    sheet.save(os.path.join(OUT, "overview.png"))


if __name__ == "__main__":
    main()
    place()
    overview()
    for name in sorted(os.listdir(OUT)):
        print("  ", os.path.join("assets", "logo", name))
