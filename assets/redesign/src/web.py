"""The site's copies of the logo (webdoc/): the header's two inline SVGs and the browser icons.

    python3 assets/redesign/src/web.py          (after make.py, which draws the icons it copies)

The header shows the word alone where it fits (her pick, 2026-11-25: the tab above it already shows
the box) and the box alone where it does not. Both are inlined, so they follow the page's theme:
- the letters and the box are "currentColor": dark on a light page, light on a dark one;
- the box's q is the page showing through the box, filled with the page's background
  (class "knock"; its fill attribute is the light-page colour, for a reader that has no CSS);
- the petals keep their two yellows.
Every shape is one <path> with its own fill, no groups or transforms: the map's preview image
(webdoc/src/pages/og/map.png.ts) draws the box from these attributes alone.

The box uses the medium flower (16 + 16 petals), the one drawn for 32 to 64 px: the header shows it
28 px tall, 56 on a retina screen."""
import os, re, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from letters import X, D1, d1_glyph, set_word, f, GOLD, YEL, LIGHT   # noqa: E402
from headonly import LETTERS                                          # noqa: E402
from e_square import e_square                                         # noqa: E402
from e_icon import E, MED                                             # noqa: E402

ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
KNOCK = f'class="knock" fill="{LIGHT}" style="fill:var(--color-background,{LIGHT})"'


def xform(d, s, tx, ty):
    """apply x' = s*x + tx, y' = s*y + ty to absolute M/L/A/Z path data"""
    out, toks, i = [], re.findall(r"[MLAZ]|-?\d+(?:\.\d+)?(?:e-?\d+)?", d), 0
    while i < len(toks):
        c = toks[i]; i += 1
        if c == "Z":
            out.append("Z"); continue
        if c in "ML":
            while i < len(toks) and toks[i] not in "MLAZ":
                x, y = float(toks[i]), float(toks[i + 1]); i += 2
                out.append(f"{c}{f(s * x + tx)},{f(s * y + ty)}")
            continue
        while i < len(toks) and toks[i] not in "MLAZ":        # A rx ry rot large sweep x y
            rx, ry, rot, la, sw, x, y = toks[i:i + 7]; i += 7
            out.append(f"A{f(s * float(rx))},{f(s * float(ry))} {rot} {la} {sw} {f(s * float(x) + tx)},{f(s * float(y) + ty)}")
    return "".join(out)


def rrect(x, y, w, h, r):
    return (f"M{f(x + r)},{f(y)}L{f(x + w - r)},{f(y)}A{f(r)},{f(r)} 0 0 1 {f(x + w)},{f(y + r)}"
            f"L{f(x + w)},{f(y + h - r)}A{f(r)},{f(r)} 0 0 1 {f(x + w - r)},{f(y + h)}"
            f"L{f(x + r)},{f(y + h)}A{f(r)},{f(r)} 0 0 1 {f(x)},{f(y + h - r)}"
            f"L{f(x)},{f(y + r)}A{f(r)},{f(r)} 0 0 1 {f(x + r)},{f(y)}Z")


def icon_paths(side_out, left_out, top_out, size=MED):
    """the icon's four shapes, scaled so its box is `side_out` square with its corner at (left_out, top_out)"""
    svg, _ = e_square(tile="dark", pad=0.0, **E, **size)
    inner = re.search(r'<g id="icon" transform="translate\(([-\d.]+),0\)">(.*?)</g><g id="letters"', svg, re.S).group(2)
    x, y, w, rx = (float(v) for v in re.search(
        r'<rect x="([-\d.]+)" y="([-\d.]+)" width="([-\d.]+)" height="[-\d.]+" rx="([-\d.]+)"', inner).groups())
    grab = lambda gid: re.search(rf'<g id="{gid}" fill="[^"]+"><path d="([^"]+)"/></g>', inner).group(1)
    s = side_out / w
    tx, ty = left_out - s * x, top_out - s * y
    return [(rrect(left_out, top_out, side_out, side_out, s * rx), 'fill="currentColor"'),
            (xform(grab("petals-back"), s, tx, ty), f'fill="{GOLD}"'),
            (xform(grab("petals-front"), s, tx, ty), f'fill="{YEL}"'),
            (xform(grab("q"), s, tx, ty), KNOCK)]


def svg(view, shapes):
    body = "".join(f'<path {attrs} d="{d}"/>' for d, attrs in shapes)
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{" ".join(f(v) for v in view)}" '
            f'aria-hidden="true" focusable="false">{body}</svg>\n')


def wordmark():
    """the word alone, the middle of its x-height at the middle of the viewBox: a bar that centres
    the SVG centres the letters' body, and the q tails hang below it"""
    p = dict(D1); p.update(LETTERS)
    letters, _, _, end = set_word(d1_glyph, p, text="queenmq", stem_w=0.0)
    mid, half = -X / 2, p["D"] + X / 2
    return svg((0.0, mid - half, end, 2 * half), [(xform(letters, 1.0, 0.0, 0.0), 'fill="currentColor"')])


def icon():
    return svg((0.0, 0.0, 224.0, 224.0), icon_paths(224.0, 0.0, 0.0))


def main():
    site = lambda *p: os.path.join(ROOT, "webdoc", *p)
    brand = lambda *p: os.path.join(ROOT, "assets", "redesign", *p)
    open(site("src", "assets", "queenmq-wordmark.svg"), "w").write(wordmark())
    open(site("src", "assets", "queenmq-icon.svg"), "w").write(icon())
    for src, dst in (("favicon.svg", "favicon.svg"), ("favicon.ico", "favicon.ico"), ("favicon-32.png", "favicon-32.png"),
                     ("apple-touch-icon.png", "apple-touch-icon.png"), ("icon-512.png", "queen-tile.png")):
        shutil.copyfile(brand("favicon", src), site("public", dst))


if __name__ == "__main__":
    main()
