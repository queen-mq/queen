"""Builds the QueenMQ redesign assets (chosen 2026-11-25: "experiment 1").

The logo = the icon box + the word "queenmq".
- The letters are drawn with one pipe: one stroke (0.24 x-height), round bends, square cuts.
- The icon is the q in bloom: a q centred in a square box, inside a whole two-row sunflower.
  On light grounds the box is dark with a light q; on dark grounds the box is white with a black q.
- In the logo the box is 1.7 x-heights tall, centred on the middle of the x-height, followed by
  the word after a gap of 0.22 box heights.

Run:  python3 assets/redesign/src/make.py [output dir]   (default: assets/redesign)
Needs rsvg-convert, ImageMagick (`magick`, for favicon.ico) and Pillow."""
import os, re, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from PIL import Image, ImageDraw                                   # noqa: E402
from letters import X, D1, d1_glyph, set_word, f, INK, LIGHT, NIGHT, WHITE   # noqa: E402
from headonly import LETTERS                                       # noqa: E402
from exp1b import lockup                                           # noqa: E402
from e_icon import box_svg, render, BIG, MED, SMALL                # noqa: E402
from build import png, FONT, GREY                                  # noqa: E402

BOX_H = 170.0          # the box in the logo, in word units (x-height = 100)


def transparent(svg):
    """drop the ground rectangle a lockup draws first"""
    return re.sub(r'(<svg[^>]*>)<rect x="[^"]+" y="[^"]+" width="[^"]+" height="[^"]+" fill="[^"]+"/>', r"\1", svg, count=1)


def wordmark(ink):
    p = dict(D1); p.update(LETTERS)
    letters, _, _, end = set_word(d1_glyph, p, text="queenmq", stem_w=0.0)
    pad = 6.0
    y0, y1 = -X - 1.5 - pad, p["D"] + pad
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{f(-pad)} {f(y0)} {f(end + 2 * pad)} {f(y1 - y0)}" '
            f'width="{round((end + 2 * pad) * 1.6)}" height="{round((y1 - y0) * 1.6)}">'
            f'<g id="letters" fill="{ink}"><path d="{letters}"/></g></svg>')


def main(out):
    for d in ("logo", "icon", "favicon"):
        os.makedirs(os.path.join(out, d), exist_ok=True)
    w = lambda rel, s: open(os.path.join(out, rel), "w").write(s)
    # logo
    w("logo/queenmq-logo.svg", transparent(lockup(box_h=BOX_H, pad=0.06)[0]))
    w("logo/queenmq-logo-on-dark.svg", transparent(lockup(box_h=BOX_H, pad=0.06, tile="light", ground=NIGHT)[0]))
    w("logo/queenmq-wordmark.svg", wordmark(INK))
    w("logo/queenmq-wordmark-on-dark.svg", wordmark(LIGHT))
    # icon
    w("icon/queenmq-icon.svg", box_svg(BIG))
    w("icon/queenmq-icon-light.svg", box_svg(BIG, tile="light"))
    w("icon/queenmq-icon-medium.svg", box_svg(MED))
    w("icon/queenmq-icon-small.svg", box_svg(SMALL))
    w("icon/queenmq-icon-square.svg", box_svg(BIG, full_bleed=True))
    w("icon/queenmq-icon-one-colour.svg", box_svg(BIG, mono=True))
    # favicons, iOS, PWA
    w("favicon/favicon.svg", box_svg(MED, adaptive=True, px=32))
    for size, spec, n in ((16, SMALL, "favicon-16.png"), (32, MED, "favicon-32.png"), (48, MED, "favicon-48.png")):
        render(box_svg(spec), size)[1].save(os.path.join(out, "favicon", n))
    for size, n, kw in ((180, "apple-touch-icon.png", dict(full_bleed=True)), (192, "icon-192.png", dict(full_bleed=True)),
                        (512, "icon-512.png", dict(full_bleed=True)), (512, "icon-512-light.png", dict(full_bleed=True, tile="light"))):
        render(box_svg(BIG, **kw), size)[0].save(os.path.join(out, "favicon", n))
    fav = os.path.join(out, "favicon")
    subprocess.run(["magick", f"{fav}/favicon-16.png", f"{fav}/favicon-32.png", f"{fav}/favicon-48.png", f"{fav}/favicon.ico"], check=True)
    # proof sheet
    Wp = 1300
    sheet = Image.new("RGB", (Wp, 1500), "white"); dr = ImageDraw.Draw(sheet)
    y = 20
    dr.text((30, y), "the logo, on light and on dark", fill=GREY, font=FONT); y += 40
    lw = png(lockup(box_h=BOX_H, pad=0.2)[0], width=700)
    sheet.paste(lw, (30, y)); y += lw.height + 10
    ld = png(lockup(box_h=BOX_H, pad=0.2, tile="light", ground=NIGHT)[0], width=700)
    sheet.paste(ld, (30, y)); y += ld.height + 30
    dr.text((30, y), "the icon: dark, white, full-bleed (iOS, PWA), one colour", fill=GREY, font=FONT); y += 40
    x = 30
    for s in (box_svg(BIG), box_svg(BIG, tile="light"), box_svg(BIG, full_bleed=True), box_svg(BIG, mono=True)):
        sheet.paste(render(s, 200, bg=(225, 225, 225))[0], (x, y)); x += 230
    y += 240
    for label, bgc, dark in (("real sizes on a light tab: 16 24 (12 petals)  32 48 64 (16)  128 (24)", (255, 255, 255), False),
                             ("on a dark tab: favicon.svg turns the box white and the q black", (32, 33, 36), True)):
        sheet.paste(Image.new("RGB", (Wp, 220), bgc), (0, y))
        dr.text((30, y + 10), label, fill=GREY if not dark else (170, 170, 170), font=FONT)
        x = 30
        for size, spec in ((16, SMALL), (24, SMALL), (32, MED), (48, MED), (64, MED), (128, BIG)):
            sheet.paste(render(box_svg(spec, adaptive=True), size, bgc, dark_theme=dark)[0], (x, y + 60 + 128 - size)); x += size + 24
        y += 240
    dr.text((30, y), "the logo at 360, 220, 140 and 100 px wide", fill=GREY, font=FONT); y += 40
    x = 30
    for wpx in (360, 220, 140, 100):
        im = png(lockup(box_h=BOX_H, pad=0.1)[0], width=wpx); sheet.paste(im, (x, y)); x += wpx + 40
    y += 90
    sheet.crop((0, 0, Wp, y)).save(os.path.join(out, "queenmq-logo-proof.png"))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(HERE))
