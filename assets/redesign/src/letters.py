"""Custom letters for "queen | mq", two directions side by side.

D1 "one pipe": every letter is the same pipe (one stroke width w) bent; the straight pipe
between queen and mq is the sunflower's stem, same width, cut flat on the q tails' line.
n m u are bent tubes, q = ring + straight pipe, e = ring + bar; all ends cut square to the pipe.

D2 "black round": Black weight, outer shapes from circles, notched joins (classic geometric);
the stem stays a thin divider and the flower stays as today (two rows, seeds, two leaves).

Units: x-height X = 100, baseline y = 0, y grows downward (SVG). All outlines are exact unions
(one contour per letter plus its counters), built from arcs and lines, so Affinity imports
clean editable curves."""
import math, os, subprocess

X = 100.0
OV = 1.5                       # overshoot of round shapes past the x-height and the baseline
INK, WHITE, NIGHT, LIGHT = "#111111", "#ffffff", "#151311", "#f6f2ea"
YEL, GOLD, DISC, SEED = "#fcc620", "#f0a020", "#1a1714", "#3a3330"


def f(v):
    s = f"{v:.2f}".rstrip("0").rstrip(".")
    return "0" if s in ("-0", "") else s


def P(p):
    return f"{f(p[0])},{f(p[1])}"


def arc(c, rx, ry, p0, p1, cw):
    """SVG arc command from p0 to p1 on the axis-aligned ellipse centred c; cw = clockwise on screen"""
    a0 = math.atan2((p0[1] - c[1]) / ry, (p0[0] - c[0]) / rx)
    a1 = math.atan2((p1[1] - c[1]) / ry, (p1[0] - c[0]) / rx)
    d = (a1 - a0) % (2 * math.pi) if cw else (a0 - a1) % (2 * math.pi)
    return f"A{f(rx)},{f(ry)} 0 {1 if d > math.pi + 1e-9 else 0} {1 if cw else 0} {P(p1)}"


class Path:
    """path builder; T maps points (a 180-degree turn keeps the arc directions)"""
    def __init__(self, T=None):
        self.T = T or (lambda p: p)
        self.s, self.cur = [], None

    def M(self, p):
        self.s.append("M" + P(self.T(p))); self.cur = p

    def L(self, p):
        self.s.append("L" + P(self.T(p))); self.cur = p

    def A(self, c, rx, ry, p1, cw):
        self.s.append(arc(self.T(c), rx, ry, self.T(self.cur), self.T(p1), cw)); self.cur = p1

    def Z(self):
        self.s.append("Z")

    def d(self):
        return "".join(self.s)


def turn(x0, W):
    cx, cy = x0 + W / 2, -X / 2
    return lambda p: (2 * cx - p[0], 2 * cy - p[1])


# ---------------------------------------------------------------- shared shapes

def q_shape(x0, Ro, stem, D, counter):
    """bowl (outer circle Ro) + stem on the right (flush with the bowl's right edge), stem cut flat
    at the x-height and at the descender D; counter = (cx, cy, a, b) ellipse"""
    cx, cy = x0 + Ro, -X / 2
    xs = x0 + 2 * Ro - stem
    h = math.sqrt(Ro ** 2 - (xs - cx) ** 2)
    p = Path()
    p.M((xs, cy - h)); p.A((cx, cy), Ro, Ro, (xs, cy + h), False)
    p.L((xs, D)); p.L((x0 + 2 * Ro, D)); p.L((x0 + 2 * Ro, -X)); p.L((xs, -X)); p.Z()
    ccx, ccy, a, b = counter
    p.M((ccx - a, ccy)); p.A((ccx, ccy), a, b, (ccx + a, ccy), True); p.A((ccx, ccy), a, b, (ccx - a, ccy), True); p.Z()
    return p.d()


def e_shape(x0, Ro, a, b, tb, ybc, term_deg):
    """ring (outer circle Ro, inner ellipse a x b) + bar (thickness tb, centre ybc), open at the
    lower right; the terminal is cut along the radius at term_deg (math angle, below = negative)"""
    cx, cy = x0 + Ro, -X / 2
    th = math.radians(term_deg)
    To = (cx + Ro * math.cos(th), cy - Ro * math.sin(th))
    rr = a * b / math.sqrt((b * math.cos(th)) ** 2 + (a * math.sin(th)) ** 2)
    Ti = (cx + rr * math.cos(th), cy - rr * math.sin(th))
    yt, yb = ybc - tb / 2, ybc + tb / 2
    ex = lambda y: a * math.sqrt(max(0.0, 1 - ((y - cy) / b) ** 2))
    ox = lambda y: math.sqrt(Ro ** 2 - (y - cy) ** 2)
    p = Path()
    p.M(To); p.L(Ti); p.A((cx, cy), a, b, (cx - ex(yb), yb), True)
    p.L((cx + ox(yb), yb)); p.A((cx, cy), Ro, Ro, To, False); p.Z()
    p.M((cx - ex(yt), yt)); p.A((cx, cy), a, b, (cx + ex(yt), yt), True); p.Z()
    return p.d()


# ---------------------------------------------------------------- D1: one pipe

D1 = dict(w=24.0, D=38.0, n_w=94.0, m_r=30.0, bar=0.78, bar_dy=2.0, term=-40.0,
          ss=18.0, sr=13.5, rr=8.5, pipe_gap=40.0,
          q_style="spur", e_style="bar", e_top=0.0, e_gap=-50.0, m_style="shared", m_wp=150.0)


def d1_glyph(ch, x0, p):
    w, D = p["w"], p["D"]
    Ro = X / 2 + OV
    Ri = Ro - w
    cx, cy = x0 + Ro, -X / 2
    if ch == "q" and p["q_style"] == "tangent":
        # a ring, and a straight pipe leaving its right side downward: nothing above the ring
        xs, xr = cx + Ri, cx + Ro
        h = math.sqrt(Ro ** 2 - Ri ** 2)
        pa = Path()
        pa.M((xr, cy)); pa.A((cx, cy), Ro, Ro, (xs, cy + h), False)
        pa.L((xs, D)); pa.L((xr, D)); pa.Z()
        pa.M((cx - Ri, cy)); pa.A((cx, cy), Ri, Ri, (cx + Ri, cy), True)
        pa.A((cx, cy), Ri, Ri, (cx - Ri, cy), True); pa.Z()
        return pa.d(), 2 * Ro, "r", "s"
    if ch == "q":
        return q_shape(x0, Ro, w, D, (x0 + Ro, -X / 2, Ro - w, Ro - w)), 2 * Ro, "r", "s"
    if ch == "e" and p["e_style"] == "open":
        # the ring alone, cut open at the lower right: the top end is cut flat at e_top,
        # the bottom end at e_gap (both along the radius, like a cut pipe); no bar
        ta, tb = math.radians(p["e_top"]), math.radians(p["e_gap"])
        Ao, Ai = (cx + Ro * math.cos(ta), cy - Ro * math.sin(ta)), (cx + Ri * math.cos(ta), cy - Ri * math.sin(ta))
        Bo, Bi = (cx + Ro * math.cos(tb), cy - Ro * math.sin(tb)), (cx + Ri * math.cos(tb), cy - Ri * math.sin(tb))
        pa = Path()
        pa.M(Ao); pa.A((cx, cy), Ro, Ro, Bo, False); pa.L(Bi); pa.A((cx, cy), Ri, Ri, Ai, True); pa.Z()
        return pa.d(), 2 * Ro, "r", "r"
    if ch == "e" and p["e_style"] == "tongue":
        # the ring, open at the lower right; the bar is only a short tongue from the right side
        # that stops before the left wall (no line across the circle)
        tb = math.radians(p["e_gap"])
        To, Ti = (cx + Ro * math.cos(tb), cy - Ro * math.sin(tb)), (cx + Ri * math.cos(tb), cy - Ri * math.sin(tb))
        th = p["bar"] * w
        ybc = cy + p["bar_dy"]
        yt, yb = ybc - th / 2, ybc + th / 2
        xl = cx - p["e_tongue"] * Ri
        # where the ring turns into the tongue the corner is a bend, so it is rounded (fillets);
        # cut ends (tongue tip, terminal) stay square, like every other pipe end
        rf, rfi = p.get("e_round", 0.0), p.get("e_round_in", 0.0)
        pa = Path()
        pa.M(To); pa.L(Ti)
        if rfi > 0:                                   # inside the eye: ring -> tongue top
            yc_ = yt - rfi
            xc_ = cx + math.sqrt((Ri - rfi) ** 2 - (yc_ - cy) ** 2)
            k = Ri / (Ri - rfi)
            pa.A((cx, cy), Ri, Ri, (cx + k * (xc_ - cx), cy + k * (yc_ - cy)), True)
            pa.A((xc_, yc_), rfi, rfi, (xc_, yt), True)
        else:
            pa.A((cx, cy), Ri, Ri, (cx + math.sqrt(Ri ** 2 - (yt - cy) ** 2), yt), True)
        pa.L((xl, yt)); pa.L((xl, yb))
        if rf > 0:                                    # outside: tongue underside -> ring
            yc_ = yb - rf
            xc_ = cx + math.sqrt((Ro - rf) ** 2 - (yc_ - cy) ** 2)
            k = Ro / (Ro - rf)
            pa.L((xc_, yb)); pa.A((xc_, yc_), rf, rf, (cx + k * (xc_ - cx), cy + k * (yc_ - cy)), False)
        else:
            pa.L((cx + math.sqrt(Ro ** 2 - (yb - cy) ** 2), yb))
        pa.A((cx, cy), Ro, Ro, To, False); pa.Z()
        return pa.d(), 2 * Ro, "r", "r"
    if ch == "e":
        return e_shape(x0, Ro, Ri, Ri, p["bar"] * w, -X / 2 + p["bar_dy"], p["term"]), 2 * Ro, "r", "r"
    if ch == "m" and p["m_style"] == "pillar":
        # one arch over the whole letter (as tall as the n's) and a pillar in the middle
        W = p["m_wp"]
        rx, ry = W / 2, p["n_w"] / 2
        irx, iry = rx - w, ry - w
        mx = x0 + W / 2
        yt = -X - OV + ry
        dy = iry * math.sqrt(1 - ((w / 2) / irx) ** 2)
        pa = Path()
        pa.M((x0, 0)); pa.L((x0, yt)); pa.A((mx, yt), rx, ry, (x0 + W, yt), True)
        pa.L((x0 + W, 0)); pa.L((x0 + W - w, 0)); pa.L((x0 + W - w, yt))
        pa.A((mx, yt), irx, iry, (mx + w / 2, yt - dy), False); pa.L((mx + w / 2, 0))
        pa.L((mx - w / 2, 0)); pa.L((mx - w / 2, yt - dy))
        pa.A((mx, yt), irx, iry, (x0 + w, yt), False); pa.L((x0 + w, 0)); pa.Z()
        return pa.d(), W, "s", "s"
    if ch in "nu":
        W = p["n_w"]
        ro, ri = W / 2, W / 2 - w
        yt = -X - OV + ro
        pa = Path(turn(x0, W) if ch == "u" else None)
        pa.M((x0, 0)); pa.L((x0, yt)); pa.A((x0 + W / 2, yt), ro, ro, (x0 + W, yt), True)
        pa.L((x0 + W, 0)); pa.L((x0 + W - w, 0)); pa.L((x0 + W - w, yt))
        pa.A((x0 + W / 2, yt), ri, ri, (x0 + w, yt), False); pa.L((x0 + w, 0)); pa.Z()
        return pa.d(), W, "s", "s"
    if ch == "m":
        r = p["m_r"]
        W = 4 * r + w
        mx = x0 + W / 2
        Ro, Ri = r + w / 2, r - w / 2
        c1, c2 = x0 + w / 2 + r, mx + r
        yt = -X - OV + Ro
        vy = yt - math.sqrt(Ro ** 2 - r ** 2)
        pa = Path()
        pa.M((x0, 0)); pa.L((x0, yt)); pa.A((c1, yt), Ro, Ro, (mx, vy), True)
        pa.A((c2, yt), Ro, Ro, (x0 + W, yt), True); pa.L((x0 + W, 0)); pa.L((x0 + W - w, 0))
        pa.L((x0 + W - w, yt)); pa.A((c2, yt), Ri, Ri, (mx + w / 2, yt), False); pa.L((mx + w / 2, 0))
        pa.L((mx - w / 2, 0)); pa.L((mx - w / 2, yt)); pa.A((c1, yt), Ri, Ri, (x0 + w, yt), False)
        pa.L((x0 + w, 0)); pa.Z()
        return pa.d(), W, "s", "s"
    raise ValueError(ch)


# ---------------------------------------------------------------- D2: black round

D2 = dict(V=38.0, Hh=29.0, Vb=31.0, D=38.0, n_w=114.0, m_w=174.0, Ra=50.0,
          ap_f=0.42, j_drop=9.0, shoulder=1.05,
          e_Vb=32.0, e_Hh=27.0, e_bar=19.0, e_bar_dy=3.0, e_term=-36.0,
          ss=21.0, sr=16.0, rr=11.0, pipe_gap=40.0)


def _counter(pa, left_x, right_x, p):
    """the white under an arch, drawn from the right stem's inner edge back to the left stem:
    a quarter ellipse up to the apex, a flatter quarter down to a high, thin join"""
    y_ap = -X - OV + p["Hh"]
    ax = left_x + p["ap_f"] * (right_x - left_x)
    y_j = y_ap + p["j_drop"]
    icy = y_ap + p["shoulder"] * (right_x - ax)
    pa.L((right_x, icy))
    pa.A((ax, icy), right_x - ax, icy - y_ap, (ax, y_ap), False)
    pa.A((ax, y_j), ax - left_x, y_j - y_ap, (left_x, y_j), False)


def d2_glyph(ch, x0, p):
    V, D = p["V"], p["D"]
    Ro = X / 2 + OV
    if ch == "q":
        a = (2 * Ro - V - p["Vb"]) / 2
        return q_shape(x0, Ro, V, D, (x0 + p["Vb"] + a, -X / 2, a, Ro - p["Hh"])), 2 * Ro, "r", "s"
    if ch == "e":
        return e_shape(x0, Ro, Ro - p["e_Vb"], Ro - p["e_Hh"], p["e_bar"], -X / 2 + p["e_bar_dy"], p["e_term"]), 2 * Ro, "r", "r"
    if ch in "nu":
        W, Ra = p["n_w"], p["Ra"]
        acx, acy = x0 + W - Ra, -X - OV + Ra
        Ny = acy - math.sqrt(Ra ** 2 - (x0 + V - acx) ** 2)
        pa = Path(turn(x0, W) if ch == "u" else None)
        pa.M((x0, 0)); pa.L((x0, -X)); pa.L((x0 + V, -X)); pa.L((x0 + V, Ny))
        pa.A((acx, acy), Ra, Ra, (x0 + W, acy), True); pa.L((x0 + W, 0)); pa.L((x0 + W - V, 0))
        _counter(pa, x0 + V, x0 + W - V, p)
        pa.L((x0 + V, 0)); pa.Z()
        return pa.d(), W, "s", "s"
    if ch == "m":
        W, Ra = p["m_w"], p["Ra"]
        mx = x0 + W / 2
        a1, a2 = mx + V / 2 - Ra, x0 + W - Ra
        acy = -X - OV + Ra
        N1y = acy - math.sqrt(Ra ** 2 - (x0 + V - a1) ** 2)
        Ix, Iy = (a1 + a2) / 2, acy - math.sqrt(Ra ** 2 - ((a2 - a1) / 2) ** 2)
        pa = Path()
        pa.M((x0, 0)); pa.L((x0, -X)); pa.L((x0 + V, -X)); pa.L((x0 + V, N1y))
        pa.A((a1, acy), Ra, Ra, (Ix, Iy), True); pa.A((a2, acy), Ra, Ra, (x0 + W, acy), True)
        pa.L((x0 + W, 0)); pa.L((x0 + W - V, 0))
        _counter(pa, mx + V / 2, x0 + W - V, p)
        pa.L((mx + V / 2, 0)); pa.L((mx - V / 2, 0))
        _counter(pa, x0 + V, mx - V / 2, p)
        pa.L((x0 + V, 0)); pa.Z()
        return pa.d(), W, "s", "s"
    raise ValueError(ch)


# ---------------------------------------------------------------- the word

def set_word(glyph, p, text="queen|mq", stem_w=None):
    """lay the word out; '|' is the stem's slot. Returns the letters' path, the stem's x range,
    the ink boxes of the n and m (for the flower's clearance) and the overall letter box"""
    x, prev, parts, boxes, stem = 0.0, None, [], {}, None
    gaps = {("s", "s"): p["ss"], ("s", "r"): p["sr"], ("r", "s"): p["sr"], ("r", "r"): p["rr"]}
    kern, prev_ch = p.get("kern", {}), None
    for i, ch in enumerate(text):
        if ch == "|":
            x += p["pipe_gap"]
            stem = (x, x + stem_w)
            x += stem_w + p["pipe_gap"]
            prev, prev_ch = None, None
            continue
        d, W, left, right = glyph(ch, 0, p)
        if prev is not None:
            x += gaps[(prev, left)] + kern.get((prev_ch, ch), 0.0)
        prev_ch = ch
        d, W, left, right = glyph(ch, x, p)
        parts.append(d)
        boxes[(ch, i)] = (x, x + W)
        x += W
        prev = right
    return "".join(parts), stem, boxes, x


# ---------------------------------------------------------------- the flower

def lens(cx, cy, ang, r0, r1, hw):
    ux, uy = math.cos(ang), math.sin(ang)
    b = (cx + r0 * ux, cy + r0 * uy)
    t = (cx + r1 * ux, cy + r1 * uy)
    L = r1 - r0
    r = (L * L / 4 + hw * hw) / (2 * hw)
    return f"M{P(b)}A{f(r)},{f(r)} 0 0 1 {P(t)}A{f(r)},{f(r)} 0 0 1 {P(b)}Z", (b, t, r)


def lens_round(cx, cy, ang, r0, r1, hw, rt):
    """a lens petal whose tip is rounded with radius rt (the base stays pointed: it is under the disc)"""
    L = r1 - r0
    R = (L * L / 4 + hw * hw) / (2 * hw)
    ux, uy = math.cos(ang), math.sin(ang)
    vx, vy = -uy, ux
    G = lambda u, v: (cx + (r0 + u) * ux + v * vx, cy + (r0 + u) * uy + v * vy)
    uf = L / 2 + math.sqrt((R - rt) ** 2 - (R - hw) ** 2)      # fillet centre on the axis
    pts = {}
    for sgn in (1, -1):                                         # sgn 1: the side bulging to +v
        c = (L / 2, -sgn * (R - hw))
        dx, dy = uf - c[0], 0 - c[1]
        k = R / math.hypot(dx, dy)
        pts[sgn] = (c, (c[0] + k * dx, c[1] + k * dy))
    def seg(cu, r, a, m, b):                                    # arc a -> b through m, all local
        C, A, M, B = G(*cu), G(*a), G(*m), G(*b)
        sa = lambda q: math.atan2(q[1] - C[1], q[0] - C[0])
        a0, am, a1 = sa(A), sa(M), sa(B)
        cw = ((am - a0) % (2 * math.pi)) < ((a1 - a0) % (2 * math.pi))
        return arc(C, r, r, A, B, cw)
    (c1, t1), (c2, t2) = pts[1], pts[-1]
    mid1 = (L / 4, -(R - hw) + math.sqrt(R * R - (L / 4 - L / 2) ** 2))
    mid2 = (L / 4, -mid1[1])
    d = f"M{P(G(0, 0))}" + seg(c1, R, (0, 0), mid1, t1) + seg((uf, 0), rt, t1, (uf + rt, 0), t2) + seg(c2, R, t2, mid2, (0, 0)) + "Z"
    return d


def petal_ring(cx, cy, n, off, r0, r1, wk, skip=(), rt=0.0):
    step = 2 * math.pi / n
    hw = wk * math.pi * ((r0 + r1) / 2) / n
    ds, geo = [], []
    for k in range(n):
        if k in skip:
            continue
        if rt > 0:
            a = math.pi / 2 + (k + off) * step
            ds.append(lens_round(cx, cy, a, r0, r1, hw, rt)); geo.append((a, r0, r1, hw))
            continue
        d, g = lens(cx, cy, math.pi / 2 + (k + off) * step, r0, r1, hw)
        ds.append(d); geo.append((math.pi / 2 + (k + off) * step, r0, r1, hw))
    return "".join(ds), geo


def cut_ring(cx, cy, n, r_in, r_out, gap, shoulder, off=0.5):
    """the letters' ring cut into n petals: each petal is a ring segment whose sides are flat radial
    cuts `gap` apart (constant width), and whose outer end is a point at r_out (shoulders at
    r_out - shoulder)"""
    out = []
    step = 2 * math.pi / n
    for k in range(n):
        a = math.pi / 2 + (k + off) * step
        ux, uy = math.cos(a), math.sin(a)
        vx, vy = -uy, ux
        def pt(r, side):                           # point at radius r on the petal's side (gap/2 off the ray)
            half = r * math.sin(step / 2) - gap / 2
            along = math.sqrt(max(r * r - half * half, 0))
            return (cx + along * ux + side * half * vx, cy + along * uy + side * half * vy)
        a1, a2 = pt(r_in, -1), pt(r_in, 1)
        s1, s2 = pt(r_out - shoulder, -1), pt(r_out - shoulder, 1)
        tip = (cx + r_out * ux, cy + r_out * uy)
        out.append(f"M{P(a1)}L{P(s1)}L{P(tip)}L{P(s2)}L{P(a2)}A{f(r_in)},{f(r_in)} 0 0 0 {P(a1)}Z")
    return "".join(out)


def petal_points(cx, cy, geo, n=24):
    """outline samples of lens petals (for the clearance test)"""
    pts = []
    for ang, r0, r1, hw in geo:
        L = r1 - r0
        ux, uy = math.cos(ang), math.sin(ang)
        vx, vy = -uy, ux
        for i in range(n + 1):
            s = i / n
            # lens half-width at s along the axis (circular arcs through the ends, hw at the middle)
            R = (L * L / 4 + hw * hw) / (2 * hw)
            d = s * L - L / 2
            half = math.sqrt(max(0.0, R * R - d * d)) - (R - hw)
            for sg in (-1, 1):
                pts.append((cx + (r0 + s * L) * ux + sg * half * vx, cy + (r0 + s * L) * uy + sg * half * vy))
    return pts


def box_dist(pt, bx):
    x0, y0, x1, y1 = bx
    dx = max(x0 - pt[0], 0, pt[0] - x1)
    dy = max(y0 - pt[1], 0, pt[1] - y1)
    return math.hypot(dx, dy)


def place_head(cx, rows, boxes, clear, top_min):
    """lowest head centre (largest y) whose petals keep `clear` from the letters' boxes"""
    lo, hi = -6 * X, top_min                   # lo: surely clear; hi: the lowest allowed
    def ok(cy):
        for (n, off, r0, r1, wk) in rows:
            _, geo = petal_ring(cx, cy, n, off, r0, r1, wk)
            for pt in petal_points(cx, cy, geo):
                if any(box_dist(pt, b) < clear for b in boxes):
                    return False
        return True
    if ok(hi):
        return hi
    for _ in range(40):
        mid = (lo + hi) / 2
        if ok(mid):
            lo = mid
        else:
            hi = mid
    return lo


def leaf(x, y, ang_deg, L, w):
    a = math.radians(ang_deg)
    t = (x + L * math.cos(a), y + L * math.sin(a))
    hw = w / 2
    r = (L * L / 4 + hw * hw) / (2 * hw)
    blade = f"M{P((x, y))}A{f(r)},{f(r)} 0 0 1 {P(t)}A{f(r)},{f(r)} 0 0 1 {P((x, y))}Z"
    m = (x + 0.8 * L * math.cos(a), y + 0.8 * L * math.sin(a))
    return blade, f"M{P((x, y))}L{P(m)}"


def seeds(cx, cy, R, n=89, fill=0.80):
    c = R * fill / math.sqrt(n)
    out = []
    for i in range(1, n + 1):
        r = c * math.sqrt(i); a = i * math.radians(137.50776)
        rr = c * 0.34 * (0.7 + 0.6 * r / (R * fill))
        x, y = cx + r * math.cos(a), cy + r * math.sin(a)
        out.append(f"M{f(x - rr)},{f(y)}a{f(rr)},{f(rr)} 0 1 0 {f(2 * rr)},0a{f(rr)},{f(rr)} 0 1 0 {f(-2 * rr)},0z")
    return "".join(out)


# ---------------------------------------------------------------- lockups

def lockup(which="D1", ground=WHITE, mono=False, scale=1.6, pad=0.5, name=None, **over):
    p = dict(D1 if which == "D1" else D2); p.update(over)
    glyph = d1_glyph if which == "D1" else d2_glyph
    ink = INK if ground in (WHITE, None) else LIGHT
    D = p["D"]
    if which == "D1" and p.get("inline"):
        # the flower IN the line, drawn on the letters' own grid: the head is the q's ring size
        # (top petals on the round letters' overshoot line, plus a point's extra), the disc is the
        # q's counter, the petals are as long as the stroke; the stem is a q tail (same width, same depth)
        w = p["w"]
        N = p.get("N", 26)
        Rd = p.get("Rd_in", X / 2 + OV - w)
        H = (X / 2 + OV + p.get("pt_ov", 1.5)) / math.cos(math.pi / N)
        letters, (s0, s1), boxes, end = set_word(glyph, dict(p, pipe_gap=p.get("gap_in", 18.0)), stem_w=2 * H)
        cx, cy = (s0 + s1) / 2, -X / 2
        sx0, sx1 = cx - w / 2, cx + w / 2
        r0 = 0.80 * Rd
        wk = p.get("wk", 1.10)
        if p.get("petal_w"):                       # petal width given in units (e.g. the stroke)
            wk = (p["petal_w"] / 2) / (math.pi * ((r0 + H) / 2) / N)
        rows = [(N, p.get("petal_off", 0.5), r0, H, wk)]
    else:
        stem_w = p["w"] if which == "D1" else p.get("stem_w", 8.0)
        letters, (sx0, sx1), boxes, end = set_word(glyph, p, stem_w=stem_w)
        nb = [v for (ch, i), v in boxes.items() if ch in "nm"]
        lbox = [(a, -X - OV, b, OV) for a, b in nb]
        cx = (sx0 + sx1) / 2
        if which == "D1":
            Rd, H = p.get("Rd", 0.40 * X), p.get("H", 0.86 * X)
            rows = [(p.get("N", 26), 0.5, 0.80 * Rd, H, p.get("wk", 1.10))]
        else:
            R = p.get("R", 0.28 * X)
            rows = [(18, 0.0, 0.82 * R, 2.28 * R, 0.88), (18, 0.5, 0.82 * R, 2.10 * R, 0.86)]
        cy = place_head(cx, rows, lbox, p.get("clear", 0.12 * X), -X - rows[0][3] + p.get("dip", 0.15) * X)
    petal_c = [GOLD, YEL] if which == "D2" else [YEL]
    if mono:
        petal_c = [ink] * len(rows)
    head = ""
    if which == "D1":
        if p.get("head_style") == "cut":
            d = cut_ring(cx, cy, rows[0][0], Rd + p.get("cut_gap_in", 0.0), rows[0][3], p.get("cut_gap", 4.0), p.get("cut_shoulder", 10.0))
            head += f'<g id="petals" fill="{petal_c[0]}"><path d="{d}"/></g>'
        else:
            # drop_bottom: no petal where the stem enters (petal_off 0 puts petal 0 straight down)
            d, _ = petal_ring(cx, cy, *rows[0], skip=(0,) if p.get("drop_bottom") else (), rt=p.get("petal_round", 0.0))
            ul = p.get("underlay", 1.22)
            under = f'<circle cx="{f(cx)}" cy="{f(cy)}" r="{f(ul * Rd)}"/>' if ul else ""
            head += f'<g id="petals" fill="{petal_c[0]}">{under}<path d="{d}"/></g>'
        disc_c = ink if (mono or ground in (WHITE, None)) else p.get("disc_night", "#2a2521")
        if mono:
            head += f'<circle cx="{f(cx)}" cy="{f(cy)}" r="{f(Rd + 0.06 * X)}" fill="{ground or WHITE}"/>'
        stem = f'<g id="stem" fill="{ink}"><path d="M{P((sx0, cy))}L{P((sx1, cy))}L{P((sx1, D))}L{P((sx0, D))}Z"/></g>'
        if p.get("stem_front"):
            # the stem runs up in front of the petals into the disc: disc + stem = one black form,
            # drawn like the letters (a q's bowl and its tail); in the night version the stem keeps
            # the letters' colour and the disc stays dark, so the stem stops at the disc's edge
            if disc_c == ink:
                head += stem
            else:
                y_edge = cy + Rd
                head += f'<g id="stem" fill="{ink}"><path d="M{P((sx0, y_edge))}L{P((sx1, y_edge))}L{P((sx1, D))}L{P((sx0, D))}Z"/></g>'
            stem = ""
        head += f'<g id="disc"><circle cx="{f(cx)}" cy="{f(cy)}" r="{f(Rd)}" fill="{disc_c}"/></g>'
        leaves = ""
        top = cy - H
    else:
        for (n, off, r0, r1, wk), c, gid in zip(rows, petal_c, ("petals-back", "petals-front")):
            d, _ = petal_ring(cx, cy, n, off, r0, r1, wk)
            head += f'<g id="{gid}" fill="{c}"><path d="{d}"/></g>'
        disc_c = ink if mono else DISC
        if mono:
            head += f'<circle cx="{f(cx)}" cy="{f(cy)}" r="{f(R + 0.045 * X)}" fill="{ground or WHITE}"/>'
        head += f'<g id="disc"><circle cx="{f(cx)}" cy="{f(cy)}" r="{f(R)}" fill="{disc_c}"/></g>'
        if not mono:
            head += f'<g id="seeds" fill="{SEED}"><path d="{seeds(cx, cy, R)}"/></g>'
        stem = f'<g id="stem" fill="{ink}"><path d="M{P((sx0, cy))}L{P((sx1, cy))}L{P((sx1, D))}L{P((sx0, D))}Z"/></g>'
        g = p["pipe_gap"]
        l1, r1 = leaf(sx0 + 0.01 * X, -0.80 * X, -145, 0.36 * X, 0.13 * X)
        l2, r2 = leaf(sx1 - 0.01 * X, -0.40 * X, -35, 0.34 * X, 0.12 * X)
        rib_c = ground if ground else WHITE
        leaves = (f'<g id="leaves"><path fill="{ink}" d="{l1}{l2}"/>'
                  f'<path fill="none" stroke="{rib_c}" stroke-width="{f(0.018 * X)}" stroke-linecap="round" d="{r1}{r2}"/></g>')
        top = cy - rows[0][3]
    x0, x1 = 0.0, end
    y0, y1 = min(top, -X - OV), D
    pp = pad * X
    vb = (x0 - pp, y0 - pp, x1 - x0 + 2 * pp, y1 - y0 + 2 * pp)
    bg = f'<rect x="{f(vb[0])}" y="{f(vb[1])}" width="{f(vb[2])}" height="{f(vb[3])}" fill="{ground}"/>' if ground else ""
    body = bg + stem + leaves + head + f'<g id="letters" fill="{ink}"><path d="{letters}"/></g>'
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{f(vb[0])} {f(vb[1])} {f(vb[2])} {f(vb[3])}" '
           f'width="{round(vb[2] * scale)}" height="{round(vb[3] * scale)}">{body}</svg>')
    info = dict(head_cy=round(cy, 1), head_top=round(top, 1), stem=(round(sx0, 1), round(sx1, 1)), width=round(end, 1))
    if name:
        os.makedirs("out", exist_ok=True)
        open(f"out/{name}.svg", "w").write(svg)
        subprocess.run(["rsvg-convert", "-o", f"out/{name}.png", f"out/{name}.svg"], check=True)
    return svg, info


if __name__ == "__main__":
    _, i = lockup("D1", name="d1"); print("D1", i)
    _, i = lockup("D2", name="d2"); print("D2", i)
