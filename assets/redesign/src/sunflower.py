"""The icon, chosen 2026-11-26: a sunflower on its stem, in the square box.

The flower is the q's bowl and the stem is its vertical. The stem rises from behind the petals in
line with the right edge of the seed head (as the q's tail runs in line with its counter), goes down
to the box's border and stops there. Her colour rule holds: on light grounds a dark box with a light
stem, on dark grounds a white box with a black stem. The petals and the seed head keep their colours.

In icon units (the box is 224 square, its corners rounded 0.225 of the side):
- the flower centred in the box: petals from 0.8 R to 84 (back row gold, front row yellow at 0.90 of
  that), a brown seed head of R 40, with 89 seeds on the golden angle from 96 px up;
- the stem 24 wide (the letters' stroke), its left side on the seed head's right edge, from the
  flower's centre (hidden by the petals) to the box's floor.
Three drawings for three size ranges, as before: BIG 24 + 24 petals (96 px and up), MED 16 + 16
(32 to 64 px), SMALL 12 + 12 (16 to 24 px)."""
import re
from letters import X, OV, D1, d1_glyph, set_word, f, INK, LIGHT, NIGHT, WHITE, YEL, GOLD
from headonly import LETTERS, seeds_path
from e_square import fan_petals
from e_icon import BIG, MED, SMALL                     # noqa: F401  (the size specs, re-exported)

SIDE = 224.0
RX = 0.225 * SIDE
C = SIDE / 2                  # the flower's centre, x and y
TIP = 84.0                    # the back row's tips
CORE = 40.0                   # the seed head's radius
STEM = D1["w"]                # the stem's width: the letters' stroke
BROWN, SEED = "#5b3a1e", "#2b1a0c"
PAPER = LIGHT


def shapes(size=BIG):
    """the drawing's parts, from the back: stem, back petals, front petals, seed head, seeds"""
    back, _ = fan_petals(C, C, size["n"], 0.0, 0.8 * CORE, TIP, 0.0, 2.0, size["w_pet"], [], 0.0)
    front, _ = fan_petals(C, C, size["n"], 0.5, 0.8 * CORE, TIP, 0.0, 2.0, size["w_pet"], [], 0.0, scale_tip=0.90)
    x0 = C + CORE
    stem = f"M{f(x0)},{f(C)}L{f(x0 + STEM)},{f(C)}L{f(x0 + STEM)},{f(SIDE)}L{f(x0)},{f(SIDE)}Z"
    core = (f"M{f(C - CORE)},{f(C)}A{f(CORE)},{f(CORE)} 0 1 0 {f(C + CORE)},{f(C)}"
            f"A{f(CORE)},{f(CORE)} 0 1 0 {f(C - CORE)},{f(C)}Z")
    seeds = seeds_path(C, C, CORE, 89, 0.88, 2.1) if size is BIG else ""
    return dict(stem=stem, back=back, front=front, core=core, seeds=seeds)


def box_svg(size=BIG, tile="dark", full_bleed=False, adaptive=False, mono=False, px=512):
    """the icon: tile "dark" (for light grounds) or "light" (for dark grounds); full_bleed for iOS and
    PWA, whose masks round the corners themselves; adaptive = favicon.svg, which turns the box white
    and the stem black when the browser is dark; mono = one colour (an ink box, the flower in paper)"""
    s = shapes(size)
    box, stem = (NIGHT, PAPER) if tile == "dark" else (PAPER, INK)
    gold, yel, core, seed = GOLD, YEL, BROWN, SEED
    if mono:
        box, stem, gold, yel, core, seed = NIGHT, PAPER, PAPER, PAPER, NIGHT, PAPER
    style = (f'<style>@media (prefers-color-scheme: dark){{#box{{fill:{PAPER}}}#stem{{fill:{INK}}}}}</style>'
             if adaptive else "")
    body = (f'<rect id="box" width="{f(SIDE)}" height="{f(SIDE)}" rx="{f(0 if full_bleed else RX)}" fill="{box}"/>'
            f'<path id="stem" fill="{stem}" d="{s["stem"]}"/>'
            f'<path id="petals-back" fill="{gold}" d="{s["back"]}"/>'
            f'<path id="petals-front" fill="{yel}" d="{s["front"]}"/>'
            f'<path id="core" fill="{core}" d="{s["core"]}"/>'
            + (f'<path id="seeds" fill="{seed}" d="{s["seeds"]}"/>' if s["seeds"] else ""))
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {f(SIDE)} {f(SIDE)}" width="{px}" height="{px}">'
            f'{style}{body}</svg>')


def lockup(box_h=170.0, tile="dark", ground=WHITE, scale=1.0, pad=0.4, name=None, size=BIG, gap_frac=0.22):
    """the logo: the box box_h tall (in word units, x-height = 100), centred on the middle of the
    x-height, then the word "queenmq" after gap_frac box heights; ground None = no ground"""
    inner = re.search(r"<svg[^>]*>(.*)</svg>", box_svg(size, tile), re.S).group(1)
    top = -X / 2 - box_h / 2
    icon = f'<g id="icon" transform="translate(0,{f(top)}) scale({box_h / SIDE:.5f})">{inner}</g>'
    p = dict(D1); p.update(LETTERS)
    ink = INK if ground in (WHITE, None) else LIGHT
    letters, _, _, end = set_word(d1_glyph, p, text="queenmq", stem_w=0.0)
    shift = box_h * (1 + gap_frac)
    word = f'<g id="letters" fill="{ink}" transform="translate({f(shift)},0)"><path d="{letters}"/></g>'
    y_lo, y_hi = min(top, -X - OV), max(top + box_h, p["D"])
    pp = pad * X
    vb = (-pp, y_lo - pp, shift + end + 2 * pp, y_hi - y_lo + 2 * pp)
    bg = f'<rect x="{f(vb[0])}" y="{f(vb[1])}" width="{f(vb[2])}" height="{f(vb[3])}" fill="{ground}"/>' if ground else ""
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{f(vb[0])} {f(vb[1])} {f(vb[2])} {f(vb[3])}" '
           f'width="{round(vb[2] * scale)}" height="{round(vb[3] * scale)}">{bg}{icon}{word}</svg>')
    if name:
        open(name, "w").write(svg)
    return svg, dict(box=round(box_h, 1))
