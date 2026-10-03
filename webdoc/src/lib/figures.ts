/**
 * Figures drawn from data: diagrams and charts, rendered at build time as
 * inline SVG.
 *
 * A figure is two things: a spec in `src/figures/<section>/<name>.ts`, and a
 * tag in the page, `<Figure id="<section>/<name>" />`. The spec holds the data
 * and the words (alt, caption, source). The renderers in
 * `src/components/figures/` hold every coordinate, so the site's diagrams and
 * charts share one type scale, one set of line weights and one palette, which
 * is the dashboard's: warm greys for anything healthy or neutral, amber for
 * attention, coral for failure, and nothing else that is warm.
 *
 * Why inline SVG drawn here, and not mermaid or a chart library: the figure
 * takes its colours from the theme tokens, so it follows the theme toggle with
 * no second file and no JavaScript; the text stays text (searchable, crisp at
 * any zoom); and a spec that names a node that does not exist, or a label that
 * does not fit its box, fails the build instead of shipping a broken picture.
 *
 * The markdown alternate of a page carries the spec's `alt`, caption and
 * source in place of the picture (`src/lib/markdown-partials.ts`), so write the
 * `alt` for a reader who cannot see the figure: state what it shows, not that
 * there is a figure.
 */

import type { Axis, FigureSpec, Format, SeriesTone } from "./figure-spec";
export * from "./figure-spec";

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

const SPECS = import.meta.glob<FigureSpec>("../figures/**/*.ts", { eager: true, import: "default" });

/** Every figure id, for the checks. */
export function figureIds(): string[] {
  return Object.keys(SPECS)
    .map((k) => k.replace(/^\.\.\/figures\//, "").replace(/\.ts$/, ""))
    .sort();
}

export function getFigure(id: string): FigureSpec {
  const spec = SPECS[`../figures/${id}.ts`];
  if (!spec) {
    const near = figureIds().filter((k) => k.split("/")[0] === id.split("/")[0]);
    throw new Error(
      `<Figure id="${id}" />: no spec at src/figures/${id}.ts` +
        (near.length ? ` (that section has: ${near.join(", ")})` : ""),
    );
  }
  if (!spec.alt || !spec.alt.trim()) throw new Error(`figure ${id}: alt is required`);
  return spec;
}

/** A prefix for ids inside one figure's SVG (markers, clip paths). */
export function domId(id: string): string {
  return "qf-" + id.replace(/[^A-Za-z0-9]+/g, "-");
}

// ---------------------------------------------------------------------------
// Geometry helpers
// ---------------------------------------------------------------------------

const NARROW = new Set([..."iljtfr.,:;'|!()[]{} /-"]);

/**
 * Rendered width of `text` in px at `size`, estimated from Inter's metrics.
 * No font is loaded at build time, so this errs wide: it decides whether a
 * label fits its box, and a false "does not fit" costs a shorter label while a
 * false "fits" ships text over a border.
 */
export function textWidth(text: string, size: number, mono = false): number {
  if (mono) return text.length * size * 0.61;
  let em = 0;
  for (const ch of text) {
    if (NARROW.has(ch)) em += 0.31;
    else if (/[mwMW@%]/.test(ch)) em += 0.84;
    else if (/[A-Z0-9#&]/.test(ch)) em += 0.66;
    else if (/[→←↔×·…]/.test(ch)) em += 0.8;
    else em += 0.56;
  }
  return em * size;
}

/** An orthogonal path through `pts` with its corners rounded to `r`. */
export function roundedPath(pts: Array<[number, number]>, r = 7): string {
  const f = (n: number) => +n.toFixed(2);
  let d = `M ${f(pts[0][0])} ${f(pts[0][1])}`;
  for (let i = 1; i < pts.length - 1; i++) {
    const [x0, y0] = pts[i - 1];
    const [x1, y1] = pts[i];
    const [x2, y2] = pts[i + 1];
    const d1 = Math.hypot(x1 - x0, y1 - y0);
    const d2 = Math.hypot(x2 - x1, y2 - y1);
    if (d1 === 0 || d2 === 0) continue;
    const rr = Math.min(r, d1 / 2, d2 / 2);
    const ax = x1 - ((x1 - x0) / d1) * rr;
    const ay = y1 - ((y1 - y0) / d1) * rr;
    const bx = x1 + ((x2 - x1) / d2) * rr;
    const by = y1 + ((y2 - y1) / d2) * rr;
    d += ` L ${f(ax)} ${f(ay)} Q ${f(x1)} ${f(y1)} ${f(bx)} ${f(by)}`;
  }
  const last = pts[pts.length - 1];
  return d + ` L ${f(last[0])} ${f(last[1])}`;
}

// ---------------------------------------------------------------------------
// Numbers
// ---------------------------------------------------------------------------

function trim(n: number, digits: number): string {
  return n.toFixed(digits).replace(/\.0+$/, "").replace(/(\.\d*?)0+$/, "$1");
}

export function fmt(v: number, f: Format = "si"): string {
  if (typeof f === "function") return f(v);
  const a = Math.abs(v);
  switch (f) {
    case "int":
      return Math.round(v).toLocaleString("en-US");
    case "ms":
      return a >= 1000 ? `${trim(v / 1000, a >= 10000 ? 0 : 1)} s` : `${trim(v, a < 10 ? 1 : 0)} ms`;
    case "s":
      return `${trim(v, a < 10 ? 1 : 0)} s`;
    case "pct":
      return `${trim(v, a < 10 ? 1 : 0)}%`;
    case "gb":
      return `${trim(v, a < 10 ? 1 : 0)} GB`;
    case "x":
      return `${trim(v, 1)}×`;
    case "si":
    default:
      if (a >= 1e9) return `${trim(v / 1e9, a >= 1e10 ? 0 : 1)}B`;
      if (a >= 1e6) return `${trim(v / 1e6, a >= 1e7 ? 0 : 2)}M`;
      if (a >= 1e3) return `${trim(v / 1e3, a >= 1e4 ? 0 : 1)}k`;
      return trim(v, a < 10 ? 1 : 0);
  }
}

/** Round, readable tick positions spanning [min, max]. */
export function niceTicks(min: number, max: number, count = 5): number[] {
  if (max === min) return [min];
  const step0 = (max - min) / Math.max(1, count - 1);
  const mag = Math.pow(10, Math.floor(Math.log10(step0)));
  const norm = step0 / mag;
  const step = (norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 2.5 ? 2.5 : norm <= 5 ? 5 : 10) * mag;
  const start = Math.floor(min / step) * step;
  const out: number[] = [];
  for (let v = start; v <= max + step * 0.001; v += step) out.push(+v.toFixed(10));
  return out;
}

/** Powers of ten covering [min, max]. */
export function decadeTicks(min: number, max: number): number[] {
  const out: number[] = [];
  for (let e = Math.floor(Math.log10(min)); e <= Math.ceil(Math.log10(max)); e++) out.push(Math.pow(10, e));
  return out;
}

/** A scale from data to px. */
export function scale(axis: Axis, dataMin: number, dataMax: number, px0: number, px1: number) {
  if (axis.log) {
    const ticks = axis.ticks ?? decadeTicks(axis.min ?? dataMin, axis.max ?? dataMax);
    const lo = Math.log10(axis.min ?? Math.min(ticks[0], dataMin));
    const hi = Math.log10(axis.max ?? Math.max(ticks[ticks.length - 1], dataMax));
    return {
      ticks,
      at: (v: number) => px0 + ((Math.log10(v) - lo) / (hi - lo || 1)) * (px1 - px0),
    };
  }
  const ticks = axis.ticks ?? niceTicks(axis.min ?? Math.min(0, dataMin), axis.max ?? dataMax);
  const lo = axis.min ?? Math.min(ticks[0], dataMin);
  const hi = axis.max ?? Math.max(ticks[ticks.length - 1], dataMax);
  return {
    ticks,
    at: (v: number) => px0 + ((v - lo) / (hi - lo || 1)) * (px1 - px0),
  };
}

export const SERIES_ORDER: SeriesTone[] = ["s1", "s2", "s4", "s3", "s5"];
export const DASHES: Record<string, string | undefined> = {
  solid: undefined,
  dash: "6 4",
  dot: "1.5 3.5",
  dashdot: "8 3 1.5 3",
};
export const AUTO_DASH = ["solid", "dash", "dot", "dashdot", "dash"] as const;
