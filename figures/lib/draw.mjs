// A small SVG drawing kit for the figures of the book and the website.
//
// Every figure is 1000 units wide, so the book (which prints a figure at the
// text width) and the site (which scales it to the column) show every figure
// at the same type size. Text is Inter and Source Code Pro: the book renders the
// SVG with rsvg-convert against figures/fonts, the site with its own web fonts.
//
// A figure draws with semantic tones (ink, muted, accent, ok, danger...), never
// with colours: build.mjs renders each figure twice, with the light palette for
// the book and the site's light theme, and with the dark one for the site's
// dark theme.

export const W = 1000;

export const PALETTES = {
  light: {
    ink: "#1b1916", muted: "#6e6862", faint: "#a39c94", line: "#c9c2b8", grid: "#e7e2db",
    fill: "#f6f4f0", paper: "#ffffff",
    accent: "#fcc620", accentInk: "#8a5f00", accentFill: "#fef4cf",
    ok: "#2f7d4e", okFill: "#e2f1e6", danger: "#b8431a", dangerFill: "#fbe6dc",
    info: "#2c5f8a", infoFill: "#e3eef7",
  },
  dark: {
    ink: "#f2f0ec", muted: "#a8a29a", faint: "#7a746d", line: "#4d4842", grid: "#2b2825",
    fill: "#1d1b19", paper: "#0f0e0d",
    accent: "#fcc620", accentInk: "#fcc620", accentFill: "#3a300d",
    ok: "#6cc08b", okFill: "#16301f", danger: "#f08a5d", dangerFill: "#3b2016",
    info: "#7fb4e0", infoFill: "#15283a",
  },
};

const SANS = "Inter, 'Inter Variable', -apple-system, 'Helvetica Neue', Arial, sans-serif";
const MONO = "'Source Code Pro', 'JetBrains Mono Variable', ui-monospace, Menlo, monospace";

const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

/** Approximate advance width of a string, for layout (Inter and Source Code Pro). */
export function textWidth(str, size = 18, { mono = false, weight = 400 } = {}) {
  const s = String(str);
  if (mono) return s.length * size * 0.6;
  let w = 0;
  for (const ch of s) {
    if ("il.,:;'|!".includes(ch)) w += 0.27;
    else if ("ftjr()[]{} ".includes(ch)) w += 0.36;
    else if ("mwMW".includes(ch)) w += 0.86;
    else if (ch >= "A" && ch <= "Z") w += 0.66;
    else if (ch >= "0" && ch <= "9") w += 0.6;
    else w += 0.55;
  }
  return w * size * (weight >= 600 ? 1.04 : 1);
}

/** Tone name -> { stroke, fill, text } for boxes and cells. */
function toneOf(p, tone) {
  switch (tone) {
    case "strong": return { stroke: p.ink, fill: p.paper, text: p.ink, width: 2.2 };
    case "accent": return { stroke: p.accent, fill: p.accentFill, text: p.ink, width: 2 };
    case "solid": return { stroke: p.accent, fill: p.accent, text: "#1b1916", width: 2 };
    case "ok": return { stroke: p.ok, fill: p.okFill, text: p.ink, width: 1.8 };
    case "danger": return { stroke: p.danger, fill: p.dangerFill, text: p.ink, width: 1.8 };
    case "info": return { stroke: p.info, fill: p.infoFill, text: p.ink, width: 1.8 };
    case "ghost": return { stroke: p.line, fill: "none", text: p.muted, width: 1.5, dash: "5 5" };
    case "muted": return { stroke: p.line, fill: p.fill, text: p.muted, width: 1.4 };
    case "none": return { stroke: "none", fill: "none", text: p.ink, width: 0 };
    default: return { stroke: p.line, fill: p.fill, text: p.ink, width: 1.6 };
  }
}

function colorOf(p, tone) {
  return ({ muted: p.muted, faint: p.faint, accent: p.accentInk, ok: p.ok, danger: p.danger,
    info: p.info, line: p.line, grid: p.grid, ink: p.ink, strong: p.ink, gold: p.accent })[tone] ?? p.ink;
}

export class Canvas {
  constructor(palette, height) {
    this.p = palette;
    this.h = height;
    this.out = [];
    this.defs = new Map();
  }

  raw(s) { this.out.push(s); return this; }

  /** Text. anchor: start | middle | end. */
  text(x, y, str, o = {}) {
    const size = o.size ?? 18;
    const fam = o.mono ? MONO : SANS;
    const weight = o.weight ?? (o.bold ? 600 : 400);
    const fill = o.color ?? colorOf(this.p, o.tone);
    const style = o.italic ? ' font-style="italic"' : "";
    const lines = String(str).split("\n");
    const lh = o.lineHeight ?? size * 1.3;
    const y0 = o.valign === "middle" ? y - ((lines.length - 1) * lh) / 2 : y;
    lines.forEach((line, i) => {
      this.out.push(`<text x="${x}" y="${(y0 + i * lh).toFixed(1)}" font-family="${fam}" font-size="${size}" font-weight="${weight}"${style} fill="${fill}" text-anchor="${o.anchor ?? "start"}" dominant-baseline="${o.baseline ?? "middle"}">${esc(line)}</text>`);
    });
    return this;
  }

  line(x1, y1, x2, y2, o = {}) {
    const c = o.color ?? colorOf(this.p, o.tone ?? "line");
    const dash = o.dash ? ` stroke-dasharray="${o.dash}"` : "";
    this.out.push(`<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="${c}" stroke-width="${o.width ?? 1.6}"${dash} stroke-linecap="round"/>`);
    return this;
  }

  rect(x, y, w, h, o = {}) {
    const t = toneOf(this.p, o.tone);
    const dash = o.dash ?? t.dash;
    this.out.push(`<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="${o.r ?? 8}" fill="${o.fill ?? t.fill}" stroke="${o.stroke ?? t.stroke}" stroke-width="${o.strokeWidth ?? t.width}"${dash ? ` stroke-dasharray="${dash}"` : ""}/>`);
    return this;
  }

  circle(cx, cy, r, o = {}) {
    const t = toneOf(this.p, o.tone);
    this.out.push(`<circle cx="${cx}" cy="${cy}" r="${r}" fill="${o.fill ?? t.fill}" stroke="${o.stroke ?? t.stroke}" stroke-width="${o.strokeWidth ?? t.width}"/>`);
    return this;
  }

  /** A box with a centred label and an optional second line (sub). Returns its anchors. */
  box(x, y, w, h, label, o = {}) {
    this.rect(x, y, w, h, o);
    const t = toneOf(this.p, o.tone);
    const cx = x + w / 2;
    const size = o.size ?? 18;
    if (label && o.sub) {
      this.text(cx, y + h / 2 - size * 0.5, label, { size, anchor: "middle", color: t.text, weight: o.weight ?? 600, mono: o.mono });
      this.text(cx, y + h / 2 + size * 0.75, o.sub, { size: o.subSize ?? 14.5, anchor: "middle", color: this.p.muted, mono: o.subMono });
    } else if (label) {
      this.text(cx, y + h / 2, label, { size, anchor: "middle", color: t.text, weight: o.weight ?? 600, mono: o.mono, valign: "middle" });
    }
    return { x, y, w, h, cx, cy: y + h / 2, l: [x, y + h / 2], r: [x + w, y + h / 2], t: [cx, y], b: [cx, y + h] };
  }

  /** A dashed or plain frame with a title at its top-left corner. */
  frame(x, y, w, h, title, o = {}) {
    this.rect(x, y, w, h, { tone: o.tone ?? "ghost", r: o.r ?? 12, fill: o.fill });
    if (title) {
      const tw = textWidth(title, o.size ?? 15, { weight: 600 }) + 14;
      this.out.push(`<rect x="${x + 14}" y="${y - 10}" width="${tw}" height="20" fill="${this.p.paper}" stroke="none"/>`);
      this.text(x + 21, y, title, { size: o.size ?? 15, weight: 600, tone: o.titleTone ?? "muted" });
    }
    return this;
  }

  arrowHead(color) {
    const id = `ah-${color.replace("#", "")}`;
    if (!this.defs.has(id)) {
      this.defs.set(id, `<marker id="${id}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7.5" markerHeight="7.5" orient="auto-start-reverse"><path d="M0,0.8 L9.5,5 L0,9.2 z" fill="${color}"/></marker>`);
    }
    return id;
  }

  /** An arrow along points [[x,y],...]; o.label is placed at o.at (0..1 along the path). */
  arrow(points, o = {}) {
    const c = o.color ?? colorOf(this.p, o.tone ?? "ink");
    const id = this.arrowHead(c);
    const d = o.curve && points.length === 2
      ? curvePath(points[0], points[1], o.curve)
      : "M" + points.map((q) => `${q[0]},${q[1]}`).join(" L");
    const dash = o.dash ? ` stroke-dasharray="${o.dash}"` : "";
    const start = o.both ? ` marker-start="url(#${id})"` : "";
    this.out.push(`<path d="${d}" fill="none" stroke="${c}" stroke-width="${o.width ?? 1.8}"${dash} stroke-linejoin="round" stroke-linecap="round" marker-end="url(#${id})"${start}/>`);
    if (o.label) {
      const size = o.size ?? 15;
      const straight = points.length === 2 && !o.curve && !o.labelAt && o.mid !== true;
      const horiz = straight && Math.abs(points[0][1] - points[1][1]) < 1;
      const vert = straight && Math.abs(points[0][0] - points[1][0]) < 1;
      if (horiz || vert) {
        const [mx, my] = pointAt(points, o.at ?? 0.5);
        const lines = String(o.label).split("\n");
        const color = o.labelColor ?? (o.tone ? c : this.p.muted);
        if (horiz) {
          const below = o.below === true;
          const y0 = below ? my + 16 : my - 12 - (lines.length - 1) * size * 1.3;
          lines.forEach((l, i) => this.text(mx, y0 + i * size * 1.3, l, { size, anchor: "middle", color, mono: o.mono, italic: o.italic }));
        } else {
          const left = o.left === true;
          this.text(left ? mx - 10 : mx + 10, my, o.label, { size, anchor: left ? "end" : "start", color, mono: o.mono, valign: "middle", italic: o.italic });
        }
        return this;
      }
      const [lx, ly] = o.labelAt ?? pointAt(points, o.at ?? 0.5);
      if (o.labelBg !== false) {
        const lines = String(o.label).split("\n");
        const lw = Math.max(...lines.map((l) => textWidth(l, size, { mono: o.mono }))) + 10;
        const lh = lines.length * size * 1.3 + 4;
        this.out.push(`<rect x="${lx - lw / 2}" y="${ly - lh / 2}" width="${lw}" height="${lh}" rx="4" fill="${this.p.paper}" stroke="none"/>`);
      }
      this.text(lx, ly, o.label, { size, anchor: "middle", color: o.labelColor ?? (o.tone ? c : this.p.muted), mono: o.mono, valign: "middle", italic: o.italic });
    }
    return this;
  }

  /**
   * A partition strip: cells of messages at offsets. cells: [{label, tone, sub}].
   * Returns the x of each cell boundary so cursors can be placed.
   */
  strip(x, y, cells, o = {}) {
    const cw = o.cellW ?? 58;
    const ch = o.cellH ?? 44;
    const gap = o.gap ?? 4;
    const xs = [];
    cells.forEach((cell, i) => {
      const cx = x + i * (cw + gap);
      xs.push(cx);
      const tone = cell.tone ?? "plain";
      this.rect(cx, y, cw, ch, { tone, r: 6 });
      if (cell.label !== undefined) {
        this.text(cx + cw / 2, y + ch / 2, cell.label, { size: o.size ?? 16, anchor: "middle", mono: o.mono ?? true, color: toneOf(this.p, tone).text, weight: 500 });
      }
      if (cell.sub) this.text(cx + cw / 2, y + ch + 15, cell.sub, { size: 13, anchor: "middle", tone: "muted" });
    });
    return { xs, cw, ch, gap, end: x + cells.length * (cw + gap) - gap, x, y };
  }

  /** A vertical cursor marker at x, above (or below) a strip, with a label. */
  cursor(x, y1, y2, label, o = {}) {
    const c = o.color ?? colorOf(this.p, o.tone ?? "accent");
    this.line(x, y1, x, y2, { color: c, width: 2.4 });
    const up = y1 < y2;
    const ty = up ? y1 - 12 : y1 + 12;
    this.out.push(`<path d="M${x - 6},${up ? y1 - 1 : y1 + 1} L${x + 6},${up ? y1 - 1 : y1 + 1} L${x},${up ? y1 + 8 : y1 - 8} z" fill="${c}"/>`);
    if (label) this.text(x + (o.dx ?? 0), ty, label, { size: o.size ?? 14.5, anchor: o.anchor ?? "middle", color: c, weight: 600 });
    return this;
  }

  /** A horizontal time axis with ticks [{at, label}]. */
  axis(x1, x2, y, ticks = [], o = {}) {
    this.arrow([[x1, y], [x2, y]], { tone: "muted", width: 1.4 });
    for (const t of ticks) {
      this.line(t.at, y - 5, t.at, y + 5, { tone: "muted", width: 1.4 });
      if (t.label) this.text(t.at, y + 20, t.label, { size: o.size ?? 14, anchor: "middle", tone: "muted", mono: o.mono });
    }
    if (o.label) this.text(x2, y - 14, o.label, { size: 14, anchor: "end", tone: "muted" });
    return this;
  }

  /** A numbered step marker (a small filled circle with a digit). */
  step(cx, cy, n, o = {}) {
    this.circle(cx, cy, o.r ?? 13, { fill: this.p.accent, stroke: "none" });
    this.text(cx, cy + 0.5, String(n), { size: 14, anchor: "middle", weight: 700, color: "#1b1916" });
    return this;
  }

  /** A pill-shaped tag. */
  tag(x, y, label, o = {}) {
    const size = o.size ?? 14;
    const w = textWidth(label, size, { mono: o.mono, weight: 600 }) + 18;
    const t = toneOf(this.p, o.tone ?? "accent");
    const ax = o.anchor === "middle" ? x - w / 2 : o.anchor === "end" ? x - w : x;
    this.rect(ax, y - 12, w, 24, { tone: o.tone ?? "accent", r: 12 });
    this.text(ax + w / 2, y, label, { size, anchor: "middle", weight: 600, color: t.text, mono: o.mono });
    return { x: ax, w };
  }

  /** A document shape (a box with a folded corner), for files. */
  file(x, y, w, h, label, o = {}) {
    const t = toneOf(this.p, o.tone);
    const f = 12;
    this.out.push(`<path d="M${x},${y} H${x + w - f} L${x + w},${y + f} V${y + h} H${x} Z" fill="${t.fill}" stroke="${t.stroke}" stroke-width="${t.width}" stroke-linejoin="round"/>`);
    this.out.push(`<path d="M${x + w - f},${y} V${y + f} H${x + w}" fill="none" stroke="${t.stroke}" stroke-width="${t.width}"/>`);
    if (label) this.text(x + w / 2, y + h / 2 + (o.sub ? -8 : 0), label, { size: o.size ?? 15, anchor: "middle", mono: o.mono ?? true, color: t.text, weight: 500 });
    if (o.sub) this.text(x + w / 2, y + h / 2 + 12, o.sub, { size: 13, anchor: "middle", tone: "muted" });
    return { x, y, w, h, cx: x + w / 2, cy: y + h / 2 };
  }

  /** A disk (cylinder) shape. */
  disk(x, y, w, h, label, o = {}) {
    const t = toneOf(this.p, o.tone);
    const ry = 9;
    this.out.push(`<path d="M${x},${y + ry} A${w / 2},${ry} 0 0 1 ${x + w},${y + ry} V${y + h - ry} A${w / 2},${ry} 0 0 1 ${x},${y + h - ry} Z" fill="${t.fill}" stroke="${t.stroke}" stroke-width="${t.width}"/>`);
    this.out.push(`<path d="M${x},${y + ry} A${w / 2},${ry} 0 0 0 ${x + w},${y + ry}" fill="none" stroke="${t.stroke}" stroke-width="${t.width}"/>`);
    if (label) this.text(x + w / 2, y + h / 2 + 4 + (o.sub ? -8 : 0), label, { size: o.size ?? 15, anchor: "middle", weight: 600, color: t.text, mono: o.mono });
    if (o.sub) this.text(x + w / 2, y + h / 2 + 14, o.sub, { size: 13, anchor: "middle", tone: "muted" });
    return { x, y, w, h, cx: x + w / 2, cy: y + h / 2 };
  }

  /** A sequence diagram: actors across, messages down. Returns the bottom y. */
  sequence(actors, steps, o = {}) {
    const top = o.y ?? 20;
    const x0 = o.x0 ?? 90;
    const x1 = o.x1 ?? W - 90;
    const span = actors.length > 1 ? (x1 - x0) / (actors.length - 1) : 0;
    const pos = Object.fromEntries(actors.map((a, i) => [a.id, x0 + i * span]));
    const rowH = o.rowH ?? 44;
    const headH = 46;
    const bottom = top + headH + 26 + steps.length * rowH;
    for (const a of actors) {
      const w = Math.max(120, textWidth(a.label, 17, { weight: 600 }) + 28);
      this.box(pos[a.id] - w / 2, top, w, headH, a.label, { tone: a.tone, sub: a.sub, size: 17 });
      this.line(pos[a.id], top + headH, pos[a.id], bottom, { tone: "line", dash: "3 5", width: 1.4 });
    }
    steps.forEach((s, i) => {
      const y = top + headH + 34 + i * rowH;
      if (s.note) {
        const over = Array.isArray(s.over) ? s.over : [s.over];
        const xa = Math.min(...over.map((id) => pos[id]));
        const xb = Math.max(...over.map((id) => pos[id]));
        const w = Math.max(xb - xa + 150, textWidth(s.note, 15, { mono: s.mono }) + 26);
        const nx = Math.min(Math.max((xa + xb) / 2 - w / 2, 8), W - 8 - w);
        this.rect(nx, y - 15, w, 30, { tone: s.tone ?? "muted", r: 6 });
        this.text(nx + w / 2, y, s.note, { size: 15, anchor: "middle", color: s.tone === "solid" ? "#1b1916" : this.p.ink, weight: 500, mono: s.mono });
        return;
      }
      const a = pos[s.from];
      const b = pos[s.to];
      if (s.from === s.to) {
        this.arrow([[a, y - 8], [a + 46, y - 8], [a + 46, y + 10], [a + 3, y + 10]], { tone: s.tone ?? "ink", width: 1.6 });
        this.text(a + 56, y + 1, s.label, { size: 15, tone: s.tone === "faint" ? "faint" : "muted", mono: s.mono });
        return;
      }
      const dir = b > a ? 1 : -1;
      this.arrow([[a + dir * 4, y], [b - dir * 4, y]], { tone: s.tone ?? (s.reply ? "muted" : "ink"), dash: s.reply ? "6 5" : undefined, width: s.tone === "strong" ? 2.4 : 1.8 });
      this.text((a + b) / 2, y - 13, s.label, { size: 15, anchor: "middle", color: s.tone === "accent" ? this.p.accentInk : s.tone === "danger" ? this.p.danger : s.tone === "faint" ? this.p.faint : this.p.ink, mono: s.mono, weight: s.tone === "strong" ? 600 : 400 });
    });
    return bottom;
  }

  svg(id, alt) {
    const defs = this.defs.size ? `<defs>${[...this.defs.values()].join("")}</defs>` : "";
    return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${this.h}" width="${W}" height="${this.h}" role="img" aria-label="${esc(alt)}" data-figure="${id}">${defs}${this.out.join("")}</svg>\n`;
  }
}

function curvePath(a, b, bend) {
  const mx = (a[0] + b[0]) / 2;
  const my = (a[1] + b[1]) / 2;
  const dx = b[0] - a[0];
  const dy = b[1] - a[1];
  const len = Math.hypot(dx, dy) || 1;
  const cx = mx - (dy / len) * bend;
  const cy = my + (dx / len) * bend;
  return `M${a[0]},${a[1]} Q${cx},${cy} ${b[0]},${b[1]}`;
}

function pointAt(points, t) {
  if (points.length === 2) {
    return [points[0][0] + (points[1][0] - points[0][0]) * t, points[0][1] + (points[1][1] - points[0][1]) * t];
  }
  const segs = [];
  let total = 0;
  for (let i = 1; i < points.length; i++) {
    const l = Math.hypot(points[i][0] - points[i - 1][0], points[i][1] - points[i - 1][1]);
    segs.push(l);
    total += l;
  }
  let d = total * t;
  for (let i = 0; i < segs.length; i++) {
    if (d <= segs[i]) {
      const f = segs[i] ? d / segs[i] : 0;
      return [points[i][0] + (points[i + 1][0] - points[i][0]) * f, points[i][1] + (points[i + 1][1] - points[i][1]) * f];
    }
    d -= segs[i];
  }
  return points[points.length - 1];
}
