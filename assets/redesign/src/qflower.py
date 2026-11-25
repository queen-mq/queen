"""Today's idea with the new letters: the first q IS the sunflower. The q stays a whole letter
(one-pipe ring + tail): its black ring is the flower's dark heart, its white counter stays open
so the q still reads. Petals (two rows: gold behind, yellow in front, pointed, at the letters'
weight) grow out from under the ring; a petal that would come near the u or the q's own tail is
left out, so the flower opens to the left, where the word starts."""
import math, os, subprocess
from letters import (X, OV, D1, d1_glyph, set_word, petal_ring, petal_points, box_dist, lens,
                     f, P, INK, WHITE, NIGHT, LIGHT, YEL, GOLD)
from headonly import LETTERS, _wk

RO = X / 2 + OV          # the q ring's outer radius
W = D1["w"]


def petals_kept(cx, cy, n, off, r0, r1, width, boxes, clear, rt=0.0):
    wk = _wk(width, r0, r1, n)
    keep, step = [], 2 * math.pi / n
    hw = wk * math.pi * ((r0 + r1) / 2) / n
    for k in range(n):
        a = math.pi / 2 + (k + off) * step
        pts = petal_points(cx, cy, [(a, r0, r1, hw)])
        # only the part of the petal outside the ring can touch anything
        pts = [q for q in pts if math.hypot(q[0] - cx, q[1] - cy) > RO]
        if all(box_dist(q, b) >= clear for q in pts for b in boxes):
            keep.append(k)
    skip = tuple(k for k in range(n) if k not in keep)
    d, _ = petal_ring(cx, cy, n, off, r0, r1, wk, skip=skip, rt=rt)
    return d, len(keep)


def q_sunflower(text="queenmq", ground=WHITE, mono=False, scale=1.6, pad=0.5, name=None,
                tip=84.0, front_tip=0.90, nb=24, nf=24, wb=16.0, wf=16.0, r0=34.0, clear=10.0,
                rows=2, icon=False, extra_obstacles=(), **over):
    p = dict(D1); p.update(LETTERS); p.update(over)
    D = p["D"]
    ink = INK if ground in (WHITE, None) else LIGHT
    if icon:
        letters, _ = d1_glyph("q", 0.0, p)[0], None
        qx, end, boxes_other = 0.0, 2 * RO, []
    else:
        if " " in text:                                   # a word space = an empty slot
            text = text.replace(" ", "|")
            p["pipe_gap"] = p.get("word_gap", 32.0) / 2
        letters, _, boxes, end = set_word(d1_glyph, p, text=text, stem_w=0.0)
        qx = boxes[("q", 0)][0]
        boxes_other = [(a, -X - OV, b, OV) for (ch, i), (a, b) in boxes.items() if i == 1]   # the u
    cx, cy = qx + RO, -X / 2
    tail = (qx + 2 * RO - W, cy, qx + 2 * RO, D)
    obstacles = boxes_other + [tail] + list(extra_obstacles)
    body = ""
    if rows == 2:
        d, kb = petals_kept(cx, cy, nb, 0.0, r0, tip, wb, obstacles, clear)
        body += f'<g id="petals-back" fill="{ink if mono else GOLD}"><path d="{d}"/></g>'
        d, kf = petals_kept(cx, cy, nf, 0.5, r0, front_tip * tip, wf, obstacles, clear)
        body += f'<g id="petals-front" fill="{ink if mono else YEL}"><path d="{d}"/></g>'
    else:
        d, kf = petals_kept(cx, cy, nf, 0.5, r0, tip, wf, obstacles, clear)
        body += f'<g id="petals" fill="{ink if mono else YEL}"><path d="{d}"/></g>'
    if mono:   # in one colour the ring is drawn with a ground-coloured keyline so petals and letter part
        body += (f'<circle cx="{f(cx)}" cy="{f(cy)}" r="{f(RO + 5)}" fill="{ground or WHITE}"/>'
                 f'<path d="M{P((qx + 2 * RO - W - 5, cy))}L{P((qx + 2 * RO + 5, cy))}L{P((qx + 2 * RO + 5, D + 5))}'
                 f'L{P((qx + 2 * RO - W - 5, D + 5))}Z" fill="{ground or WHITE}"/>')
    body += f'<g id="letters" fill="{ink}"><path d="{letters}"/></g>'
    pp = pad * X
    x0, x1 = min(0.0, cx - tip), end
    y0, y1 = min(-X - OV, cy - tip), max(D, cy + tip)
    if icon:
        half = max(tip, RO) + 2.0
        x0, x1 = cx - half, cx + half
        y0, y1 = cy - half, max(cy + half, D + 2)
        side = max(x1 - x0, y1 - y0)
        mx, my = (x0 + x1) / 2, (y0 + y1) / 2
        x0, x1, y0, y1 = mx - side / 2, mx + side / 2, my - side / 2, my + side / 2
        pp = 0.0
    vb = (x0 - pp, y0 - pp, x1 - x0 + 2 * pp, y1 - y0 + 2 * pp)
    bg = f'<rect x="{f(vb[0])}" y="{f(vb[1])}" width="{f(vb[2])}" height="{f(vb[3])}" fill="{ground}"/>' if ground else ""
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{f(vb[0])} {f(vb[1])} {f(vb[2])} {f(vb[3])}" '
           f'width="{round(vb[2] * scale)}" height="{round(vb[3] * scale)}">{bg}{body}</svg>')
    if name:
        os.makedirs("out", exist_ok=True)
        open(f"out/{name}.svg", "w").write(svg)
        subprocess.run(["rsvg-convert", "-o", f"out/{name}.png", f"out/{name}.svg"], check=True)
    return svg, dict(width=round(vb[2], 1), height=round(vb[3], 1))
