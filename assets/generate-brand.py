#!/usr/bin/env python3
"""Regenerate every derived QueenMQ brand asset from the geometric mark.

The mark is CONSTRUCTED, not traced: this file is the master. Editing the
constants below and re-running is the only supported way to change the brand —
there is no hand-made source file to keep in sync any more, and no embedded
raster to extract (the previous duck-era pipeline worked that way).

The mark
--------
A ring with a machined exit PORT, and the wedge cut out of it departing along
the 45 degree diagonal: the queue, its outlet, and the message that left. The
gap and the departing piece are the same shape, so the two read as one event.
Everything is a single solid colour with no negative-space parts, so the mark
survives on any background without knockouts.

Two containers, and the rule for choosing:
  nude mark  — favicons, docs header, dashboard sidebar, sign-in badge
  tile       — app icons, apple-touch, avatars, README (self-contained: it
               carries its own background, so it needs no per-theme variant)

The tile costs ~18% of usable stroke at a given canvas size (it spends 30% of
the side on safe area), which is why anything at or below 32px uses the nude
mark instead.

Outputs (all regenerated, do not hand-edit):
  assets/queen-mark.svg              master, currentColor
  assets/queen-tile.svg              master, dark tile
  assets/queen-tile-light.svg        master, light tile
  assets/queen-tile.png              512px, repo README (GitHub is light OR dark)
  assets/queen-social-card.png       1280x640, GitHub social preview (upload only)
  app/public/favicon.svg             theme-adaptive nude mark
  app/public/favicon-32.png          raster fallback: the tile, self-contained
  app/public/queen-mark.svg          dashboard sidebar + boot + proxy sign-in badge
  webdoc/public/favicon.svg          theme-adaptive nude mark
  webdoc/public/favicon-32.png       raster fallback
  webdoc/public/favicon.ico          16/32/48 for browsers that ignore SVG icons
  webdoc/public/apple-touch-icon.png 180px tile
  webdoc/public/queen-tile.png       512px, schema.org Organization.logo

The docs header is NOT in that list: it inlines the geometry so the fill can be
currentColor and follow the site's manual theme toggle.

Run:  python3 assets/generate-brand.py
Then: cd app && npm run build     (server/webapp/dist is the artifact BOTH the
      broker and the proxy embed at compile time — a Rust rebuild ships it)
"""
import io
import math
import os

from PIL import Image, ImageDraw, ImageFont

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def P(*p):
    return os.path.join(ROOT, *p)


# --- geometry -------------------------------------------------------------
# Design grid. The "micro" cut: one departing piece, heavy stroke, wide port —
# the cut that survives a 16px favicon. A three-piece display cut exists in the
# design study but is deliberately NOT shipped: two cuts to keep in sync buys
# nothing at the sizes this project actually renders at.
GRID = 256
CX = CY = 117.0
RO = 105.0            # ring outer radius
STROKE = 39.0         # ring stroke -> inner radius 66
RI = RO - STROKE
PORT_HALF = 17.0      # half-angle of the exit port, degrees
AXIS = 45.0           # departure axis, degrees (screen coords: down-right)
WEDGE_GAP = 22.0      # clearance from the ring's outer edge
WEDGE_SCALE = 0.85    # the piece reads as receding, not as a chip

DARK = "#0d0d0f"
LIGHT = "#ffffff"

TILE = 512
TILE_N = 5            # superellipse exponent: continuous corners, no curvature break
TILE_FILL = 0.70      # mark bbox as a share of the tile side


def _pt(cx, cy, r, deg):
    a = math.radians(deg)
    return cx + r * math.cos(a), cy + r * math.sin(a)


def _arc(cx, cy, r, a0, a1, steps=192):
    return [_pt(cx, cy, r, a0 + (a1 - a0) * i / steps) for i in range(steps + 1)]


def ring_points():
    """The ring, opened by the port, with radial (not rounded) lips."""
    a0, a1 = AXIS + PORT_HALF, AXIS - PORT_HALF + 360
    return _arc(CX, CY, RO, a0, a1) + _arc(CX, CY, RI, a1, a0)


def wedge_points():
    """The annular sector removed by the port, pushed out along the axis and
    scaled about its own centroid."""
    a0, a1 = AXIS - PORT_HALF, AXIS + PORT_HALF
    pts = _arc(CX, CY, RO, a0, a1) + _arc(CX, CY, RI, a1, a0)
    t = STROKE + WEDGE_GAP
    dx, dy = t * math.cos(math.radians(AXIS)), t * math.sin(math.radians(AXIS))
    px, py = _pt(CX, CY, (RO + RI) / 2, AXIS)
    return [(px + WEDGE_SCALE * (x - px) + dx, py + WEDGE_SCALE * (y - py) + dy)
            for x, y in pts]


def superellipse_points(cx, cy, a, n, steps=480):
    out = []
    for i in range(steps):
        t = 2 * math.pi * i / steps
        c, s = math.cos(t), math.sin(t)
        out.append((cx + a * math.copysign(abs(c) ** (2 / n), c),
                    cy + a * math.copysign(abs(s) ** (2 / n), s)))
    return out


SHAPES = [ring_points(), wedge_points()]
_xs = [x for s in SHAPES for x, _ in s]
_ys = [y for s in SHAPES for _, y in s]
BBOX = (min(_xs), min(_ys), max(_xs), max(_ys))
BW, BH = BBOX[2] - BBOX[0], BBOX[3] - BBOX[1]


# --- SVG ------------------------------------------------------------------
def _svg_path(cx, cy, ro, ri, a0, a1, large):
    ox0, oy0 = _pt(cx, cy, ro, a0)
    ox1, oy1 = _pt(cx, cy, ro, a1)
    ix1, iy1 = _pt(cx, cy, ri, a1)
    ix0, iy0 = _pt(cx, cy, ri, a0)
    return (f"M {ox0:.3f} {oy0:.3f} A {ro} {ro} 0 {large} 1 {ox1:.3f} {oy1:.3f} "
            f"L {ix1:.3f} {iy1:.3f} A {ri} {ri} 0 {large} 0 {ix0:.3f} {iy0:.3f} Z")


def mark_body():
    ring = _svg_path(CX, CY, RO, RI, AXIS + PORT_HALF, AXIS - PORT_HALF + 360, 1)
    wedge = _svg_path(CX, CY, RO, RI, AXIS - PORT_HALF, AXIS + PORT_HALF, 0)
    t = STROKE + WEDGE_GAP
    dx, dy = t * math.cos(math.radians(AXIS)), t * math.sin(math.radians(AXIS))
    px, py = _pt(CX, CY, (RO + RI) / 2, AXIS)
    tf = (f"translate({dx:.3f},{dy:.3f}) translate({px:.3f},{py:.3f}) "
          f"scale({WEDGE_SCALE}) translate({-px:.3f},{-py:.3f})")
    return f'<path d="{ring}"/><g transform="{tf}"><path d="{wedge}"/></g>'


def mark_svg(fill="currentColor", margin=10.0, adaptive=False):
    """Nude mark, viewBox framed on the real ink.

    `adaptive` swaps ink colour with the viewer's theme — the docs have a light
    AND a dark theme, and a browser tab strip can be either, so one file covers
    both instead of shipping a per-theme variant.
    """
    side = max(BW, BH) + 2 * margin
    vx = BBOX[0] - (side - BW) / 2
    vy = BBOX[1] - (side - BH) / 2
    style = ("<style>path{fill:%s}@media(prefers-color-scheme:dark){path{fill:%s}}</style>"
             % (DARK, LIGHT)) if adaptive else ""
    group = f'<g fill="{fill}">' if not adaptive else "<g>"
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vx:.2f} {vy:.2f} '
            f'{side:.2f} {side:.2f}" width="{GRID}" height="{GRID}">'
            f'{style}{group}{mark_body()}</g></svg>\n')


def tile_svg(tile_col=DARK, mark_col=LIGHT):
    scale = (TILE_FILL * TILE) / max(BW, BH)
    ox = (TILE - BW * scale) / 2 - BBOX[0] * scale
    oy = (TILE - BH * scale) / 2 - BBOX[1] * scale
    pts = " L ".join(f"{x:.2f} {y:.2f}"
                     for x, y in superellipse_points(TILE / 2, TILE / 2, TILE / 2, TILE_N))
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {TILE} {TILE}" '
            f'width="{TILE}" height="{TILE}">'
            f'<path d="M {pts} Z" fill="{tile_col}"/>'
            f'<g fill="{mark_col}" transform="translate({ox:.2f},{oy:.2f}) '
            f'scale({scale:.5f})">{mark_body()}</g></svg>\n')


# --- raster (Pillow only: no SVG rasteriser needed on a contributor's box) ---
SS = 4  # supersample factor


def _draw(size, shapes_colours, bg=(0, 0, 0, 0)):
    img = Image.new("RGBA", (size * SS, size * SS), bg)
    d = ImageDraw.Draw(img)
    for pts, colour in shapes_colours:
        d.polygon([(x * SS, y * SS) for x, y in pts], fill=colour)
    return img.resize((size, size), Image.LANCZOS)


def mark_raster(size, colour):
    """Nude mark on transparent, framed exactly like mark_svg()."""
    side = max(BW, BH) + 20.0
    vx = BBOX[0] - (side - BW) / 2
    vy = BBOX[1] - (side - BH) / 2
    k = size / side
    shapes = [([((x - vx) * k, (y - vy) * k) for x, y in s], colour) for s in SHAPES]
    return _draw(size, shapes)


def tile_raster(size, tile_col=DARK, mark_col=LIGHT):
    scale = (TILE_FILL * size) / max(BW, BH)
    ox = (size - BW * scale) / 2 - BBOX[0] * scale
    oy = (size - BH * scale) / 2 - BBOX[1] * scale
    shapes = [(superellipse_points(size / 2, size / 2, size / 2, TILE_N), tile_col)]
    shapes += [([(x * scale + ox, y * scale + oy) for x, y in s], mark_col) for s in SHAPES]
    return _draw(size, shapes)


def write(text, *path):
    out = P(*path)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    open(out, "w").write(text)
    print("  ", os.path.relpath(out, ROOT), f"{len(text)} bytes")


def save(img, *path):
    out = P(*path)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    img.save(out, optimize=True)
    print("  ", os.path.relpath(out, ROOT), img.size, f"{os.path.getsize(out) // 1024}KB")


print("masters")
write(mark_svg(), "assets", "queen-mark.svg")
write(tile_svg(), "assets", "queen-tile.svg")
write(tile_svg(tile_col=LIGHT, mark_col=DARK), "assets", "queen-tile-light.svg")

print("favicons (vector adapts to the tab strip; the raster fallback carries its own ground)")
for dest in ("app/public/favicon.svg", "webdoc/public/favicon.svg"):
    write(mark_svg(adaptive=True), dest)
for dest in ("app/public/favicon-32.png", "webdoc/public/favicon-32.png"):
    save(tile_raster(32), dest)
tile_raster(256).save(P("webdoc/public/favicon.ico"), sizes=[(16, 16), (32, 32), (48, 48)])
print("  ", "webdoc/public/favicon.ico",
      f"{os.path.getsize(P('webdoc/public/favicon.ico')) // 1024}KB")
save(tile_raster(180), "webdoc/public/apple-touch-icon.png")

print("marks")
# The dashboard is dark-only, so its mark is simply white. The docs are not.
write(mark_svg(fill=LIGHT), "app/public/queen-mark.svg")
# No webdoc/public/queen-mark.svg: the docs header INLINES the geometry so its
# fill can be currentColor and track the site's manual theme toggle. See the
# comment in webdoc/src/components/Header.astro.
save(tile_raster(512), "assets", "queen-tile.png")            # README
# schema.org Organization.logo (webdoc/astro.config.ts): consumers fetch it
# blind and composite it on a ground of their choosing, so it has to be the
# self-contained tile, and a raster — several ignore SVG.
save(tile_raster(512), "webdoc/public/queen-tile.png")

print("social card (GitHub repo social preview: Settings -> Social preview, upload only)")
# 1280x640 is GitHub's declared size; it renders around 640x320 in most feeds and
# as small as 320x160 in a Slack unfurl, so everything is sized to survive a 4x
# downscale. Only Inter Bold is vendored, so "MQ" steps back by colour rather
# than by weight — same intent as the lockup, one axis instead of two.
CARD = (1280, 640)
INTER = P("webdoc", "public", "fonts", "Inter-Bold.ttf")


def _fit(draw, text, font_path, size, max_w):
    while size > 12:
        f = ImageFont.truetype(font_path, size)
        words, lines, cur = text.split(), [], ""
        for w in words:
            t = f"{cur} {w}".strip()
            if draw.textlength(t, font=f) <= max_w:
                cur = t
            else:
                if cur:
                    lines.append(cur)
                cur = w
        if cur:
            lines.append(cur)
        if len(lines) <= 2 and all(draw.textlength(l, font=f) <= max_w for l in lines):
            return f, lines
        size -= 2
    return ImageFont.truetype(font_path, 12), [text]


card = Image.new("RGB", CARD, (13, 13, 15))
d = ImageDraw.Draw(card)

badge = mark_raster(300, LIGHT)
card.paste(badge, (110, (CARD[1] - badge.height) // 2), badge)

x, right = 520, 96
col = CARD[0] - x - right

name = ImageFont.truetype(INTER, 88)
d.text((x, 196), "Queen", font=name, fill=(255, 255, 255))
d.text((x + d.textlength("Queen", font=name), 196), "MQ", font=name, fill=(138, 138, 146))

tag_font, tag_lines = _fit(d, "Postgres message queue with per-entity ordering",
                           INTER, 44, col)
y = 306
for line in tag_lines:
    d.text((x, y), line, font=tag_font, fill=(154, 160, 166))
    y += tag_font.size + 10

d.line([(x, y + 26), (x + 120, y + 26)], fill=(70, 70, 70), width=3)
foot = ImageFont.truetype(INTER, 28)
d.text((x, y + 52), "queenmq.com   ·   Apache-2.0", font=foot, fill=(120, 126, 132))

out = P("assets", "queen-social-card.png")
card.save(out, optimize=True)
print("  ", os.path.relpath(out, ROOT), CARD, f"{os.path.getsize(out) // 1024}KB")

# ---- verification contact sheet (not committed) ----
sheet = Image.new("RGB", (1180, 260), (128, 128, 132))
x = 20
for img, ground in ((tile_raster(210), None),
                    (tile_raster(210, tile_col=LIGHT, mark_col=DARK), None),
                    (mark_raster(210, LIGHT), (13, 13, 15)),
                    (mark_raster(210, DARK), (255, 255, 255))):
    cell = Image.new("RGBA", (210, 210), ground or (0, 0, 0, 0))
    cell.alpha_composite(img)
    sheet.paste(cell.convert("RGB"), (x, 25))
    x += 230
strip = Image.new("RGB", (1180, 60), (128, 128, 132))
sx = 20
for s in (16, 24, 32, 48):
    m = mark_raster(s, DARK)
    cell = Image.new("RGBA", (s, s), (255, 255, 255, 255))
    cell.alpha_composite(m)
    strip.paste(cell.convert("RGB"), (sx, 14))
    sx += s + 16
sheet.paste(strip, (0, 200))
sheet.save(P("_brand_preview_icons.png"))
print("preview: _brand_preview_icons.png")
