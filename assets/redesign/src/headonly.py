"""The sunflower head alone in the line (no stem), drawn so it reads as a SUNFLOWER, not a daisy or a sun.
What says sunflower without a stem: a big dark disc (about half the head), two rows of pointed
petals (the back row darker gold, half a step round, peeking between the front ones), and the
seeds in the disc set by the golden angle (137.5 deg). Head as tall as the letters, centred on the
x-height middle; the outermost tips sit on the round letters' overshoot line plus a point's extra."""
import math, os, subprocess
from letters import (X, OV, D1, d1_glyph, set_word, petal_ring, f, P, INK, WHITE, NIGHT, LIGHT, YEL, GOLD)

LETTERS = dict(q_style="tangent", e_style="tongue", e_gap=-45.0, e_tongue=0.20, e_round=12.0,
               m_style="shared", m_r=30.0, kern={("q", "u"): -4.0})


def _wk(width, r0, r1, n):
    return (width / 2) / (math.pi * ((r0 + r1) / 2) / n)


def seeds_path(cx, cy, R, n, fill, dot):
    c = R * fill / math.sqrt(n)
    out = []
    for i in range(1, n + 1):
        r = c * math.sqrt(i)
        a = i * math.radians(137.50776)
        rr = dot * (0.75 + 0.5 * r / (R * fill))          # seeds grow a little toward the rim
        x, y = cx + r * math.cos(a), cy + r * math.sin(a)
        out.append(f"M{f(x - rr)},{f(y)}a{f(rr)},{f(rr)} 0 1 0 {f(2 * rr)},0a{f(rr)},{f(rr)} 0 1 0 {f(-2 * rr)},0z")
    return "".join(out)


def head_lockup(ground=WHITE, mono=False, scale=1.6, pad=0.5, name=None, gap_in=14.0, Rd=26.0, pt_ov=1.5,
                front=dict(N=16, w=17.0, tip=0.90, rt=0.0), back=dict(N=16, w=17.0, rt=0.0),
                seeds=None, disc=None, **over):
    p = dict(D1); p.update(LETTERS); p.update(over)
    ink = INK if ground in (WHITE, None) else LIGHT
    Ho = X / 2 + OV + pt_ov                                  # outermost tip radius
    letters, (s0, s1), boxes, end = set_word(d1_glyph, dict(p, pipe_gap=gap_in), stem_w=2 * Ho)
    cx, cy = (s0 + s1) / 2, -X / 2
    r0 = 0.80 * Rd
    body_head = ""
    if back:
        # back row: a petal straight up, so its tip is the head's top
        d, _ = petal_ring(cx, cy, back["N"], 0.0, r0, Ho, _wk(back["w"], r0, Ho, back["N"]), rt=back.get("rt", 0.0))
        body_head += f'<g id="petals-back" fill="{ink if mono else GOLD}"><path d="{d}"/></g>'
        Hf = front["tip"] * Ho
        off_f = 0.5
    else:
        Hf = Ho / math.cos(math.pi / front["N"])          # one row, half-step offset: top pair on the line
        off_f = 0.5
    d, _ = petal_ring(cx, cy, front["N"], off_f, r0, Hf, _wk(front["w"], r0, Hf, front["N"]), rt=front.get("rt", 0.0))
    under = "" if back else f'<circle cx="{f(cx)}" cy="{f(cy)}" r="{f(1.22 * Rd)}"/>'   # one row: close the specks at the base
    body_head += f'<g id="petals-front" fill="{ink if mono else YEL}">{under}<path d="{d}"/></g>'
    disc_c = ink if (mono or ground in (WHITE, None)) else (disc or "#2a2521")
    if mono:
        body_head += f'<circle cx="{f(cx)}" cy="{f(cy)}" r="{f(Rd + 0.06 * X)}" fill="{ground or WHITE}"/>'
    body_head += f'<g id="disc"><circle cx="{f(cx)}" cy="{f(cy)}" r="{f(Rd)}" fill="{disc_c}"/></g>'
    if seeds and not mono:
        body_head += (f'<g id="seeds" fill="{seeds["colour"]}"><path d="'
                      f'{seeds_path(cx, cy, Rd, seeds["n"], seeds.get("fill", 0.86), seeds["dot"])}"/></g>')
    pp = pad * X
    y0, y1 = cy - Ho, p["D"]
    vb = (-pp, y0 - pp, end + 2 * pp, y1 - y0 + 2 * pp)
    bg = f'<rect x="{f(vb[0])}" y="{f(vb[1])}" width="{f(vb[2])}" height="{f(vb[3])}" fill="{ground}"/>' if ground else ""
    body = bg + body_head + f'<g id="letters" fill="{ink}"><path d="{letters}"/></g>'
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{f(vb[0])} {f(vb[1])} {f(vb[2])} {f(vb[3])}" '
           f'width="{round(vb[2] * scale)}" height="{round(vb[3] * scale)}">{body}</svg>')
    if name:
        os.makedirs("out", exist_ok=True)
        open(f"out/{name}.svg", "w").write(svg)
        subprocess.run(["rsvg-convert", "-o", f"out/{name}.png", f"out/{name}.svg"], check=True)
    return svg, dict(width=round(end, 1), cx=round(cx, 1))
