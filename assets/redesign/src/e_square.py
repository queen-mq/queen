"""E with a SQUARE box (her: the box must stay square) and the q close to the u.
The q sits near the box's right edge (q to u = 26); the room this frees on the left is filled by the
flower itself: the petals grow longer toward the left, a fan, so the crest reaches the left side of the
box with even padding, and the box stays a square of the same size as before."""
import math
from letters import (X, OV, D1, d1_glyph, set_word, lens, petal_points, box_dist, f, P,
                     INK, WHITE, NIGHT, LIGHT, YEL, GOLD)
from headonly import LETTERS
from qflower import RO

W = D1["w"]
PAPER = "#f6f2ea"


def tip_at(theta, t0, fan, k):
    """screen angle theta (0 = right, pi = left): tips longest toward the left"""
    return t0 + fan * ((1 - math.cos(theta)) / 2) ** k


def fan_petals(cx, cy, n, off, r0, t0, fan, k, width, obstacles, clear, scale_tip=1.0):
    out, kept = [], 0
    step = 2 * math.pi / n
    for i in range(n):
        a = math.pi / 2 + (i + off) * step
        r1 = scale_tip * tip_at(a, t0, fan, k)
        hw = (width / 2)
        pts = petal_points(cx, cy, [(a, r0, r1, hw)])
        pts = [q for q in pts if math.hypot(q[0] - cx, q[1] - cy) > RO]
        if all(box_dist(q, b) >= clear for q in pts for b in obstacles):
            d, _ = lens(cx, cy, a, r0, r1, hw)
            out.append(d); kept += 1
    return "".join(out), kept


def e_square(tile="dark", ground=WHITE, t0=84.0, fan=36.0, k=2.0, n=24, w_pet=16.0, r0=34.0,
             pad_r=0.17, gap=0.09, edge=0.14, clear=8.0, scale=1.6, pad=0.5, name=None, behind_tail=False,
             text="ueenmq"):
    p = dict(D1); p.update(LETTERS)
    D = p["D"]
    ink = INK if ground in (WHITE, None) else LIGHT
    cx, cy = RO, -X / 2
    tail_r = 2 * RO
    right = tail_r + pad_r * X                               # the box's right edge
    # the box: square; its side is set by the flower's height plus the same padding as on the left
    top = cy - tip_at(-math.pi / 2, t0, fan, k)              # top petal tip
    side = (D - top) + 2 * edge * X
    side = max(side, 0)
    box_top = top - edge * X
    left = right - side
    tail_box = (tail_r - W, cy, tail_r, D)
    wall = (right - edge * X * 0.5, -1000, right + 1000, 1000)    # keep petals inside the box on the right
    left_wall = (left - 1000, -1000, left + edge * X * 0.6, 1000)  # and on the left
    obstacles = ([] if behind_tail else [tail_box]) + [wall, left_wall]   # behind_tail: petals pass behind the q's tail
    d_back, kb = fan_petals(cx, cy, n, 0.0, r0, t0, fan, k, w_pet, obstacles, clear)
    d_front, kf = fan_petals(cx, cy, n, 0.5, r0, t0, fan, k, w_pet, obstacles, clear, scale_tip=0.90)
    q_d, _, _, _ = d1_glyph("q", 0.0, p)
    q_fill = LIGHT if tile == "dark" else INK
    bg = NIGHT if tile == "dark" else PAPER
    if tile == "dark" and ground == NIGHT:
        bg = "#2a2521"
    rx = 0.225 * side
    icon = (f'<rect x="{f(left)}" y="{f(box_top)}" width="{f(side)}" height="{f(side)}" rx="{f(rx)}" fill="{bg}"/>'
            f'<g id="petals-back" fill="{GOLD}"><path d="{d_back}"/></g>'
            f'<g id="petals-front" fill="{YEL}"><path d="{d_front}"/></g>'
            f'<g id="q" fill="{q_fill}"><path d="{q_d}"/></g>')
    letters, _, boxes, end = set_word(d1_glyph, p, text=text, stem_w=0.0)
    shift = right + gap * X
    body = (f'<g id="icon" transform="translate({f(-left)},0)">{icon}</g>'
            f'<g id="letters" fill="{ink}" transform="translate({f(shift - left)},0)"><path d="{letters}"/></g>')
    Wtot = (shift - left) + end
    pp = pad * X
    yl, yh = min(box_top, -X - OV), max(box_top + side, D)
    vb = (-pp, yl - pp, Wtot + 2 * pp, yh - yl + 2 * pp)
    bgr = f'<rect x="{f(vb[0])}" y="{f(vb[1])}" width="{f(vb[2])}" height="{f(vb[3])}" fill="{ground}"/>' if ground else ""
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{f(vb[0])} {f(vb[1])} {f(vb[2])} {f(vb[3])}" '
           f'width="{round(vb[2] * scale)}" height="{round(vb[3] * scale)}">{bgr}{body}</svg>')
    if name:
        open(name, "w").write(svg)
    crest_left = cx - tip_at(math.pi, t0, fan, k)
    return svg, dict(side=round(side, 1), q_to_u=round(pad_r * X + gap * X, 1),
                     left_pad=round(crest_left - left, 1), petals=(kb, kf))


def fan_for(pad_r, left_pad=22.0):
    """the fan that keeps `left_pad` of box left of the crest when the q's tail is `pad_r` (in X) from
    the box's right edge. Current geometry (q ring 51.5 round its centre at y -50, tails at 38, edge 0.14 X,
    t0 84, k 2: the top tip grows by fan/4): left_pad = 64.5 - pad_r*X - 0.75*fan."""
    return max(0.0, (64.5 - pad_r * X - left_pad) / 0.75)


def fan_solve(t0=84.0, edge=0.14, pad_r=0.32, left_pad=22.0, k=2.0):
    """fan (>= 0) that leaves `left_pad` between the crest's left tip and the box, for any petal length
    t0 and padding `edge` (top/bottom, in X) and `pad_r` (tail to right edge, in X)"""
    def lp(fan):
        top = -X / 2 - tip_at(-math.pi / 2, t0, fan, k)
        bottom = max(D1["D"], -X / 2 + tip_at(math.pi / 2, t0, fan, k))
        side = (bottom - top) + 2 * edge * X
        left = (2 * RO + pad_r * X) - side
        return (RO - tip_at(math.pi, t0, fan, k)) - left
    if lp(0.0) <= left_pad:
        return 0.0
    lo, hi = 0.0, 200.0
    for _ in range(60):
        mid = (lo + hi) / 2
        if lp(mid) > left_pad:
            lo = mid
        else:
            hi = mid
    return lo
