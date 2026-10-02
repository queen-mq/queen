#!/usr/bin/env python3
"""Regenerate every QueenMQ brand asset from the constructed sunflower.

assets/sunflower.py is the master: the badge, the small mark and the square
tiles are built there from circles, arcs and lenses. Change it and re-run this
script; every file below is an output, do not hand-edit.

Three containers, and the rule for choosing:
  badge   the queen bee flying to the sunflower, on a sunset field. For places
          that show it at 96 px and up: README, social card. Below that its
          seeds turn to speckle and the bee to a smudge. The docs homepage
          shows the same picture as a tall panel (the scene) filling a column.
  mark    the flower head alone, plain head, no ground: tab icons, the
          dashboard sidebar and boot screen, the sign-in page, the docs header.
          At 16 px it drops from 21 petals to 13, where 21 blur into a ring.
  square  a full-bleed plum square, for consumers that composite the logo
          blind: GitHub avatar (with the bee), apple-touch-icon and the
          schema.org logo (without: they are seen small).

The badge's sky is a parameter: the sunset bands are the master, and the plum
sky without the white rim is the one in use. assets/sunflower/ holds every sky,
with and without the rim.

Outputs (all regenerated, do not hand-edit):
  assets/queen-sunflower-bee.svg     the badge: sunset bands, white rim
  assets/queen-badge.svg             the badge in use: plum sky, no rim (README)
  assets/queen-mark.svg              the mark
  assets/queen-avatar.png            512px square with the bee, GitHub org avatar (upload only)
  assets/queen-social-card.png       1280x640, GitHub social preview (upload only)
  assets/sunflower/queen-sunflower-bee[-<sky>][-borderless].{svg,png}
  app/public/favicon.svg             the mark
  app/public/favicon-32.png          raster fallback
  app/public/queen-sunflower.webp    96px mark: sidebar, boot screen, and the sign-in
                                     page (proxy/src/oauth.rs inlines it from the build)
  webdoc/public/favicon.svg          the mark
  webdoc/public/favicon-32.png       raster fallback
  webdoc/public/favicon.ico          16 (13 petals) / 32 / 48 for browsers that ignore SVG icons
  webdoc/public/apple-touch-icon.png 180px square (iOS rounds the corners itself)
  webdoc/public/queen-tile.png       512px square, schema.org Organization.logo
  webdoc/public/queen-mark.svg       the docs header
  webdoc/public/queen-badge.svg      the badge, for docs pages that want it
  webdoc/public/queen-scene.svg      the tall scene on plum, the docs homepage hero in the light theme
  webdoc/public/queen-scene-cream.svg  the same on cream (rose trail), the hero in the dark theme
  clients/client-php/resources/views/dashboard/partials/mark.blade.php
                                     the mark inline, for the Laravel dashboard's header
                                     (it is served by the user's app, so nothing to fetch)

Run:  python3 assets/generate-brand.py
Then: cd app && npm run build     (server/webapp/dist is the artifact BOTH the
      broker and the proxy embed at compile time — a Rust rebuild ships it)
"""
import io
import os
import subprocess
import sys
import tempfile

from PIL import Image, ImageDraw, ImageFont

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sunflower as S  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SKY_VARIANTS = {
    "coral": S.CORAL,
    "orange": S.ORANGE,
    "peach": S.PEACH,
    "rust": S.CORAL_D,
    "cream": S.CREAM,
    "plum": S.PLUM,
}


def P(*p):
    return os.path.join(ROOT, *p)


def raster(svg, px, ss=8):
    """rsvg-convert renders at px*ss and PIL downsamples: a 16px icon comes out
    cleaner that way than rasterising the SVG straight at 16px."""
    png = subprocess.run(["rsvg-convert", "-w", str(px * ss), "-h", str(px * ss)],
                         input=svg.encode(), check=True, capture_output=True).stdout
    img = Image.open(io.BytesIO(png)).convert("RGBA")
    return img if ss == 1 else img.resize((px, px), Image.LANCZOS)


def write(text, *path):
    out = P(*path)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    open(out, "w").write(text)
    print("  ", os.path.relpath(out, ROOT), f"{len(text)} bytes")


def save(img, *path, **kw):
    out = P(*path)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    img.save(out, optimize=True, **kw)
    print("  ", os.path.relpath(out, ROOT), img.size, f"{os.path.getsize(out) // 1024}KB")


BADGE = S.badge_svg(sky=S.PLUM, rim=False)
MARK = S.mark_svg(21)
MARK16 = S.mark_svg(13)
SQUARE = S.square_svg(bee_too=False)

print("masters")
write(S.badge_svg(), "assets", "queen-sunflower-bee.svg")
write(BADGE, "assets", "queen-badge.svg")
write(MARK, "assets", "queen-mark.svg")
save(raster(S.square_svg(bee_too=True), 512, ss=2), "assets", "queen-avatar.png")

print("every sky, with and without the rim")
for name, sky in [("", None)] + [(f"-{n}", c) for n, c in SKY_VARIANTS.items()]:
    for rim, suffix in ((True, ""), (False, "-borderless")):
        if not name and rim:
            continue  # the master itself
        svg = S.badge_svg(sky=sky, rim=rim)
        write(svg, "assets", "sunflower", f"queen-sunflower-bee{name}{suffix}.svg")
        save(raster(svg, 1024, ss=1), "assets", "sunflower", f"queen-sunflower-bee{name}{suffix}.png")

print("dashboard (app/public is copied into the build the broker and the proxy embed)")
write(MARK, "app", "public", "favicon.svg")
save(raster(MARK, 32), "app", "public", "favicon-32.png")
save(raster(MARK, 96), "app", "public", "queen-sunflower.webp", lossless=True, quality=100, method=6)

print("docs")
write(MARK, "webdoc", "public", "favicon.svg")
write(MARK, "webdoc", "public", "queen-mark.svg")
write(BADGE, "webdoc", "public", "queen-badge.svg")
write(S.scene_svg(), "webdoc", "public", "queen-scene.svg")
write(S.scene_svg(sky=S.CREAM, trail=S.ROSE), "webdoc", "public", "queen-scene-cream.svg")
save(raster(MARK, 32), "webdoc", "public", "favicon-32.png")
raster(MARK, 48).save(P("webdoc", "public", "favicon.ico"), sizes=[(16, 16), (32, 32), (48, 48)],
                      append_images=[raster(MARK16, 16), raster(MARK, 32)])
print("  ", "webdoc/public/favicon.ico", f"{os.path.getsize(P('webdoc/public/favicon.ico')) // 1024}KB")
save(raster(SQUARE, 180, ss=4), "webdoc", "public", "apple-touch-icon.png")
# schema.org Organization.logo (webdoc/astro.config.ts): consumers fetch it
# blind and composite it on a ground of their choosing, so it has to carry its
# own ground, and be a raster — several ignore SVG.
save(raster(SQUARE, 512, ss=2), "webdoc", "public", "queen-tile.png")

print("laravel dashboard")
write("{{-- The Queen mark, inline. Written by assets/generate-brand.py: do not edit. --}}\n"
      f'<svg class="brand-mark" viewBox="{S.MARK_VIEWBOX}" aria-hidden="true" focusable="false">'
      f"{S.mark_body(21)}</svg>\n",
      "clients", "client-php", "resources", "views", "dashboard", "partials", "mark.blade.php")

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

badge = raster(BADGE, 340, ss=2)
card.paste(badge, (100, (CARD[1] - badge.height) // 2), badge)

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

# ---- verification contact sheet (outside the repo) ----
# The mark at real size on a light and a dark strip, then 16 and 32 px blown up
# 6x with nearest-neighbour, which is what decides whether it still reads.
sheet = Image.new("RGB", (1000, 300), (128, 128, 132))
for row, ground in enumerate(((250, 250, 250), (10, 10, 10))):
    strip = Image.new("RGB", (460, 120), ground)
    x = 16
    for s in (16, 24, 32, 48, 96):
        m = raster(MARK16 if s == 16 else MARK, s)
        strip.paste(m, (x, (120 - s) // 2), m)
        x += s + 24
    sheet.paste(strip, (16, 16 + row * 136))
    for i, (s, src) in enumerate(((16, MARK16), (32, MARK))):
        tile = Image.new("RGBA", (s, s), ground + (255,))
        tile.alpha_composite(raster(src, s))
        sheet.paste(tile.resize((96, 96), Image.NEAREST).convert("RGB"), (492 + i * 112, 28 + row * 136))
b = raster(BADGE, 264, ss=2)
sheet.paste(b, (720, 18), b)
preview = os.path.join(tempfile.gettempdir(), "queen-brand-preview.png")
sheet.save(preview)
print("preview:", preview)
