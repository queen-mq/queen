#!/usr/bin/env python3
"""Regenerate every derived QueenMQ brand asset from the two master logos.

Masters (the only hand-made sources — edit those, not the outputs):
  assets/queen-logo-black.svg        colour duck on a black disc   (backgrounds that can be WHITE)
  assets/queen-logo-transparent.svg  colour duck, no disc          (backgrounds that are already DARK)

Both masters are Affinity exports that wrap one high-resolution PNG; the
pipeline works on those embedded rasters. Every output is quantised to a
128-colour palette — the art is flat-colour, so this is visually lossless at
the sizes anything renders it, and 4-5x smaller.

Outputs (all regenerated, do not hand-edit):
  app/public/favicon.svg                  webapp tab icon: black-disc logo, 160px PNG inlined in SVG
  app/public/favicon-32.png               32px raster fallback
  app/public/queen-logo-transparent.png   512px mark: dashboard sidebar + boot screen, and the
                                          proxy sign-in badge (proxy/src/oauth.rs inlines it
                                          out of the embedded webapp)
  webdoc/public/favicon.svg               same tab icon for the docs
  webdoc/public/favicon-32.png
  webdoc/public/favicon.ico               16/32/48 fallback for browsers that ignore SVG icons
  webdoc/public/apple-touch-icon.png      180px iOS icon: transparent duck on opaque dark, padded
  webdoc/public/queen-logo-black.png      512px mark for the docs header (light theme exists, so
                                          the disc; on the dark theme the disc melts into the page)
  assets/queen-logo-black.png             512px mark for the repo README (GitHub can be white)

Run:  python3 assets/generate-brand.py
Then: cd app && npm run build     (server/webapp/dist is the artifact BOTH the
      broker and the proxy embed at compile time — a Rust rebuild ships it)
"""
import base64, io, os, re
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
def P(*p): return os.path.join(ROOT, *p)

def master(name):
    """The high-res PNG wrapped inside an Affinity SVG export."""
    svg = open(P('assets', name)).read()
    b64 = re.search(r'base64,([^"]+)"', svg).group(1)
    return Image.open(io.BytesIO(base64.b64decode(b64))).convert('RGBA')

BLACK = master('queen-logo-black.svg')        # duck on black disc
TRANS = master('queen-logo-transparent.svg')  # duck alone, transparent

DARK = (17, 17, 17, 255)  # #111111 — apple-touch-icon background

def contain(src, size, bg=(0, 0, 0, 0), pad=0.0):
    """src centred & contained on a square `size` canvas, `pad` fraction of margin."""
    canvas = Image.new('RGBA', (size, size), bg)
    inner = int(size * (1 - 2 * pad))
    s = src.copy(); s.thumbnail((inner, inner), Image.LANCZOS)
    canvas.alpha_composite(s, ((size - s.width) // 2, (size - s.height) // 2))
    return canvas

def quant(img):
    return img.quantize(colors=128, method=Image.FASTOCTREE, dither=Image.Dither.NONE)

def save(img, *path):
    out = P(*path); os.makedirs(os.path.dirname(out), exist_ok=True)
    quant(img).save(out, optimize=True)
    print('  ', os.path.relpath(out, ROOT), img.size, f'{os.path.getsize(out)//1024}KB')

print('favicons (black-disc logo: reads on light and dark tab strips)')
# SVG wrapper around a small inlined PNG: full-colour art, so no currentColor
# tricks — the disc itself is the contrast guarantee.
buf = io.BytesIO(); quant(contain(BLACK, 160)).save(buf, format='PNG', optimize=True)
b64 = base64.b64encode(buf.getvalue()).decode()
svg = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160">'
    f'<image width="160" height="160" href="data:image/png;base64,{b64}"/></svg>'
)
for dest in ('app/public/favicon.svg', 'webdoc/public/favicon.svg'):
    open(P(dest), 'w').write(svg)
    print('  ', dest, f'{len(svg)} bytes')

for dest in ('app/public/favicon-32.png', 'webdoc/public/favicon-32.png'):
    save(contain(BLACK, 32), dest)
ico = quant(contain(BLACK, 256))
ico.save(P('webdoc/public/favicon.ico'), sizes=[(16, 16), (32, 32), (48, 48)])
print('  ', 'webdoc/public/favicon.ico', f"{os.path.getsize(P('webdoc/public/favicon.ico'))//1024}KB")
save(contain(TRANS, 180, bg=DARK, pad=0.14), 'webdoc/public/apple-touch-icon.png')

print('marks (512px, plenty for the 22-120px they render at)')
save(contain(TRANS, 512), 'app/public/queen-logo-transparent.png')   # dark webapp + proxy sign-in
save(contain(BLACK, 512), 'webdoc/public/queen-logo-black.png')      # docs header
save(contain(BLACK, 512), 'assets/queen-logo-black.png')             # README

# ---- verification contact sheet (not committed) ----
def comp(img, bg, sz=210):
    t = img.copy(); t.thumbnail((sz, sz)); c = Image.new('RGBA', t.size, bg)
    c.alpha_composite(t); return c.convert('RGB')
tiles = [comp(contain(BLACK, 32), (240, 240, 240, 255)),
         comp(Image.open(P('webdoc/public/apple-touch-icon.png')).convert('RGBA'), (240, 240, 240, 255)),
         comp(Image.open(P('app/public/queen-logo-transparent.png')).convert('RGBA'), (17, 17, 17, 255)),
         comp(Image.open(P('webdoc/public/queen-logo-black.png')).convert('RGBA'), (255, 255, 255, 255))]
row = Image.new('RGB', (sum(t.width for t in tiles) + 50, 230), (128, 128, 128))
x = 10
for t in tiles:
    row.paste(t, (x, 10)); x += t.width + 10
row.save(P('_brand_preview_icons.png'))
print('preview: _brand_preview_icons.png')
