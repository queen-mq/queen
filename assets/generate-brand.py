#!/usr/bin/env python3
"""Regenerate every derived QueenMQ brand asset from the two master line-marks.

Masters (the only hand-made sources — edit those, not the outputs):
  assets/queen-duck-black-linemark.png   black line-art duck, transparent  (LIGHT backgrounds)
  assets/queen-duck-white-linemark.png   white line-art duck, transparent  (DARK backgrounds)

Outputs (all regenerated, do not hand-edit):
  docs/assets/favicon.svg                 theme-adaptive favicon (currentColor via prefers-color-scheme)
  app/public/favicon.svg                  same, for the webapp
  docs/assets/favicon-32.png              32px raster fallback (white duck on opaque dark)
  docs/assets/apple-touch-icon.png        180px iOS home-screen icon (opaque dark, padded)
  docs/assets/queen_head_64.png           64px nav mark (white, transparent)
  docs/assets/queen-duck-white-linemark.png   hero/source copy for the dark docs site
  app/public/queen-duck-white-linemark.png    sidebar mark for the dark webapp
  docs/assets/og-card.png                 1200x630 social share card

Run:  python3 assets/generate-brand.py
"""
import base64, io, os
from PIL import Image, ImageDraw, ImageFont

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
def P(*p): return os.path.join(ROOT, *p)

BLACK = Image.open(P('assets/queen-duck-black-linemark.png')).convert('RGBA')
WHITE = Image.open(P('assets/queen-duck-white-linemark.png')).convert('RGBA')

DARK   = (17, 17, 17, 255)      # #111111  — og-card / icon background
CYAN   = (34, 221, 238, 255)    # #22DDEE  — site accent
GREY   = (136, 136, 136, 255)   # subtitle
LIGHT_TAB = '#141415'           # favicon colour on light browser chrome
DARK_TAB  = '#ffffff'           # favicon colour on dark browser chrome

def contain(src, size, bg=(0, 0, 0, 0), pad=0.0):
    """src centred & contained on a square `size` canvas, `pad` fraction of margin."""
    canvas = Image.new('RGBA', (size, size), bg)
    inner = int(size * (1 - 2 * pad))
    s = src.copy(); s.thumbnail((inner, inner), Image.LANCZOS)
    canvas.alpha_composite(s, ((size - s.width) // 2, (size - s.height) // 2))
    return canvas

def save(img, *path):
    out = P(*path); os.makedirs(os.path.dirname(out), exist_ok=True)
    img.save(out); print('  ', os.path.relpath(out, ROOT), img.size)

print('favicons (theme-adaptive SVG + raster fallback)')
# SVG: the white line-mark's alpha drives a <rect> filled with currentColor;
# currentColor flips black<->white via prefers-color-scheme. No autotrace.
mask = contain(WHITE, 160)
buf = io.BytesIO(); mask.save(buf, format='PNG', optimize=True)
b64 = base64.b64encode(buf.getvalue()).decode()
svg = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160">'
    f'<style>:root{{color:{LIGHT_TAB}}}'
    f'@media(prefers-color-scheme:dark){{:root{{color:{DARK_TAB}}}}}</style>'
    '<mask id="d"><image width="160" height="160" '
    f'href="data:image/png;base64,{b64}"/></mask>'
    '<rect width="160" height="160" fill="currentColor" mask="url(#d)"/></svg>'
)
for dest in ('docs/assets/favicon.svg', 'app/public/favicon.svg', 'proxy/public/favicon.svg'):
    out = P(dest); open(out, 'w').write(svg)
    print('  ', dest, f'{len(svg)} bytes')

fav32 = contain(WHITE, 32, bg=DARK, pad=0.10)
save(fav32, 'docs/assets/favicon-32.png')
save(fav32, 'app/public/favicon-32.png')
save(fav32, 'proxy/public/favicon-32.png')
save(contain(WHITE, 180, bg=DARK, pad=0.14), 'docs/assets/apple-touch-icon.png')

print('nav / sidebar / login marks (white, transparent)')
save(contain(WHITE, 64), 'docs/assets/queen_head_64.png')
save(WHITE, 'docs/assets/queen-duck-white-linemark.png')
save(WHITE, 'app/public/queen-duck-white-linemark.png')
save(WHITE, 'proxy/public/queen-duck-white-linemark.png')

print('og-card 1200x630')
def font(bold, size):
    paths = ([
        '/System/Library/Fonts/Supplemental/Arial Bold.ttf',
        '/System/Library/Fonts/HelveticaNeue.ttc',
    ] if bold else [
        '/System/Library/Fonts/Supplemental/Arial.ttf',
        '/System/Library/Fonts/Helvetica.ttc',
    ])
    for p in paths:
        try: return ImageFont.truetype(p, size)
        except Exception: pass
    return ImageFont.load_default()

card = Image.new('RGBA', (1200, 630), DARK)
d = ImageDraw.Draw(card)
d.rounded_rectangle([92, 128, 152, 138], radius=4, fill=CYAN)          # accent dash
d.text((90, 158), 'Queen MQ', font=font(True, 132), fill=(255, 255, 255, 255))
d.text((95, 322), 'Partitioned message queue', font=font(False, 40), fill=GREY)
d.text((95, 372), 'on PostgreSQL',             font=font(False, 40), fill=GREY)
d.text((95, 476), 'queenmq.com', font=font(True, 34), fill=CYAN)
duck = WHITE.copy(); duck.thumbnail((400, 400), Image.LANCZOS)         # mark on the right
card.alpha_composite(duck, (775, 315 - duck.height // 2))
save(card.convert('RGBA'), 'docs/assets/og-card.png')

# ---- verification contact sheet (not committed) ----
def comp(img, bg, sz=210):
    t = img.copy(); t.thumbnail((sz, sz)); c = Image.new('RGBA', t.size, bg)
    c.alpha_composite(t); return c.convert('RGB')
fav = Image.open(P('docs/assets/favicon-32.png'))
ath = Image.open(P('docs/assets/apple-touch-icon.png'))
nav = Image.open(P('docs/assets/queen_head_64.png'))
ogc = Image.open(P('docs/assets/og-card.png'))
tiles = [comp(fav, (240, 240, 240, 255)), comp(ath, (240, 240, 240, 255)),
         comp(nav, (17, 17, 17, 255)), comp(nav, (240, 240, 240, 255))]
row = Image.new('RGB', (sum(t.width for t in tiles) + 40, 230), (128, 128, 128))
x = 10
for t in tiles:
    row.paste(t, (x, 10)); x += t.width + 10
row.save(P('_brand_preview_icons.png'))
ogc.convert('RGB').save(P('_brand_preview_og.png'))
print('previews: _brand_preview_icons.png, _brand_preview_og.png')
