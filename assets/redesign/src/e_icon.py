"""The app icon IS E's box: the same square, the same q (more centred, fan 14), cut out of the wordmark.
Fewer petals at small sizes (same layout). Dark box by default; on dark grounds a white box with a
black q (her rule); favicon.svg switches by itself with the browser's dark theme."""
import io, re, subprocess
from PIL import Image
from e_square import e_square, fan_for
from letters import INK, LIGHT, NIGHT, WHITE

PAPER = "#f6f2ea"
E = dict(pad_r=0.605, fan=0.0, edge=0.26, gap=0.18, behind_tail=True)   # the q centred in the box (box centre = q ring centre), a whole sunflower behind it (petals pass behind the tail), 26 round the flower, box to u = 18
BIG = dict(n=24, w_pet=16.0)       # 96 px and up (and the wordmark)
MED = dict(n=16, w_pet=22.0)       # 32-64 px
SMALL = dict(n=12, w_pet=28.0)     # 16-24 px


def box_svg(size=BIG, tile="dark", full_bleed=False, adaptive=False, mono=False, px=512):
    svg, info = e_square(tile=tile, pad=0.0, **E, **size)
    icon = re.search(r'<g id="icon" transform="translate\(([-\d.]+),0\)">(.*?)</g><g id="letters"', svg, re.S)
    tx, inner = float(icon.group(1)), icon.group(2)
    rect = re.search(r'<rect x="([-\d.]+)" y="([-\d.]+)" width="([-\d.]+)" height="([-\d.]+)" rx="([-\d.]+)"', inner)
    x, y, w = float(rect.group(1)) + tx, float(rect.group(2)), float(rect.group(3))
    inner = inner.replace("<rect ", '<rect id="box" ', 1)
    if full_bleed:          # iOS / PWA masks round the corners themselves
        inner = re.sub(r'rx="[-\d.]+"', 'rx="0"', inner, count=1)
    if mono:                # one colour: ink box, petals and q knocked out in paper, a box-coloured keyline round the q
        bgc = NIGHT
        inner = (inner.replace(f'fill="{"#f0a020"}"', f'fill="{PAPER}"').replace(f'fill="{"#fcc620"}"', f'fill="{PAPER}"'))
        qd = re.search(r'<g id="q" fill="[^"]+"><path d="([^"]+)"/></g>', inner).group(1)
        inner = inner.replace('<g id="q"', f'<path d="{qd}" fill="none" stroke="{bgc}" stroke-width="12" stroke-linejoin="round"/><g id="q"')
        inner = re.sub(r'<g id="q" fill="[^"]+">', f'<g id="q" fill="{PAPER}">', inner)
    style = ""
    if adaptive:            # dark browser theme: white box, black q (her rule for dark grounds)
        style = f'<style>@media (prefers-color-scheme: dark){{#box{{fill:{PAPER}}}#q{{fill:{INK}}}}}</style>'
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{x:.2f} {y:.2f} {w:.2f} {w:.2f}" width="{px}" height="{px}">'
            f'{style}<g transform="translate({tx:.2f},0)">{inner}</g></svg>')


def render(svg, size, bg=(255, 255, 255), dark_theme=False):
    if dark_theme:          # what a dark-theme browser shows for the adaptive favicon (the rule applied directly)
        m = re.search(r'<style>@media \(prefers-color-scheme: dark\)\{(.*)\}</style>', svg)
        if m:
            svg = svg.replace(m.group(0), f'<style>{m.group(1)}</style>')
    out = subprocess.run(["rsvg-convert", "-w", str(size), "-h", str(size)], input=svg.encode(), capture_output=True, check=True).stdout
    im = Image.open(io.BytesIO(out)).convert("RGBA")
    flat = Image.alpha_composite(Image.new("RGBA", im.size, bg + (255,)), im).convert("RGB")
    return flat, im
