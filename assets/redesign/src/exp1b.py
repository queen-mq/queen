"""Experiment 1, round 2 (her: "if the box is not the q of queenmq, we can make the inside of the box
smaller"). The box is an icon here, so its q no longer has to be the word's size: the icon (q centred,
whole sunflower) is scaled on its own and placed next to the full word."""
import re
from e_square import e_square
from letters import X, OV, D1, d1_glyph, set_word, f, INK, WHITE, NIGHT, LIGHT
from headonly import LETTERS

CONTENT_H = 172.0          # the q + flower, top petal tip to tail end, in word units at text size


def icon_inner(content_frac, tile="dark"):
    """the box with the flower filling `content_frac` of its height; returns (inner svg, x, y, side)"""
    side = CONTENT_H / content_frac
    edge = (side - CONTENT_H) / 2 / X
    svg, _ = e_square(tile=tile, pad=0.0, pad_r=(side / 2 - 51.5) / X, fan=0.0, edge=edge, gap=0.18,
                      behind_tail=True)
    m = re.search(r'<g id="icon" transform="translate\(([-\d.]+),0\)">(.*?)</g><g id="letters"', svg, re.S)
    tx, inner = float(m.group(1)), m.group(2)
    r = re.search(r'<rect x="([-\d.]+)" y="([-\d.]+)" width="([-\d.]+)"', inner)
    return inner, float(r.group(1)), float(r.group(2)), float(r.group(3))


def lockup(box_h, content_frac=172 / 224, box_cy=-X / 2, gap_frac=0.22, tile="dark", ground=WHITE,
           scale=1.0, pad=0.4, name=None):
    """box_h: the box's height in word units (x-height = 100); box_cy: its vertical centre"""
    inner, bx, by, side = icon_inner(content_frac, tile)
    s = box_h / side
    top = box_cy - box_h / 2
    icon = f'<g id="icon" transform="translate(0,{f(top)}) scale({s:.5f}) translate({f(-bx)},{f(-by)})">{inner}</g>'
    p = dict(D1); p.update(LETTERS)
    ink = INK if ground in (WHITE, None) else LIGHT
    letters, _, _, end = set_word(d1_glyph, p, text="queenmq", stem_w=0.0)
    shift = box_h + gap_frac * box_h
    word = f'<g id="letters" fill="{ink}" transform="translate({f(shift)},0)"><path d="{letters}"/></g>'
    W = shift + end
    y_lo, y_hi = min(top, -X - OV), max(top + box_h, p["D"])
    pp = pad * X
    vb = (-pp, y_lo - pp, W + 2 * pp, y_hi - y_lo + 2 * pp)
    bg = f'<rect x="{f(vb[0])}" y="{f(vb[1])}" width="{f(vb[2])}" height="{f(vb[3])}" fill="{ground}"/>' if ground else ""
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{f(vb[0])} {f(vb[1])} {f(vb[2])} {f(vb[3])}" '
           f'width="{round(vb[2] * scale)}" height="{round(vb[3] * scale)}">{bg}{icon}{word}</svg>')
    if name:
        open(name, "w").write(svg)
    return svg, dict(q_in_box=round(s, 2), box=round(box_h, 1))
