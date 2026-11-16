/**
 * The map page's preview image (/og/map.png): the map itself, in the dark
 * theme, with the page's title in the drawing's empty top-left corner. A link
 * to /map/ on a chat or a social site shows it.
 *
 * Drawn at build time with CanvasKit from the numbers the page draws with
 * (src/lib/map-geometry.ts), so it always shows the map as it is. The other
 * pages get a title card from ./[...slug].ts, which leaves this one out.
 */
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { MAP_TITLE } from "@/lib/map";
import { VIEW, W, ICON, NAME_Y, boxes, flows, txFlows, NOTE, ICON_BODY } from "@/lib/map-geometry";

export const prerender = true;

const CARD = { w: 1200, h: 630 };
/** The dark theme's colours: src/styles/globals.css and the map's own tokens in QueenMap.astro. */
const C = { bg: "#202020", fg: "#f3f3f3", muted: "#9e9e9e", yellow: "#fcc620", dot: "#6f6a63", soft: "#aaa49c" };
/** Where Inter's "central" baseline sits above its alphabetic one, in ems: SVG's dominant-baseline="central". */
const CENTRAL = 0.3633;

// The build runs in webdoc/: resolve the packages from there, as ./_og-card-config.ts reads its font.
const require = createRequire(resolve("package.json"));

type CK = Awaited<ReturnType<typeof import("canvaskit-wasm/full").default>>;

async function canvasKit(): Promise<CK> {
  const { default: init } = await import("canvaskit-wasm/full");
  return init({ locateFile: (file: string) => require.resolve(`canvaskit-wasm/bin/full/${file}`) });
}

/** The attributes of each element of an icon's or the logo's SVG markup. */
function elements(svg: string): { tag: string; a: Record<string, string> }[] {
  return [...svg.matchAll(/<(rect|path|circle|ellipse)\s([^>]*?)\/?>/g)].map(([, tag, attrs]) => ({
    tag,
    a: Object.fromEntries([...attrs.matchAll(/([\w-]+)="([^"]*)"/g)].map(([, k, v]) => [k, v])),
  }));
}

export async function GET() {
  const ck = await canvasKit();
  const inter = await readFile(require.resolve("@fontsource-variable/inter/files/inter-latin-wght-normal.woff2"));
  const logo = await readFile(resolve("src/assets/q.svg"), "utf8");
  const fonts = ck.TypefaceFontProvider.Make();
  fonts.registerFont(inter.buffer.slice(inter.byteOffset, inter.byteOffset + inter.byteLength), "Inter");

  const surface = ck.MakeSurface(CARD.w, CARD.h)!;
  const canvas = surface.getCanvas();
  canvas.clear(ck.parseColorString(C.bg));

  const color = (hex: string, alpha = 1) => ck.multiplyByAlpha(ck.parseColorString(hex), alpha);
  const stroke = (hex: string, width: number, alpha = 1) => {
    const p = new ck.Paint();
    p.setAntiAlias(true);
    p.setStyle(ck.PaintStyle.Stroke);
    p.setColor(color(hex, alpha));
    p.setStrokeWidth(width);
    p.setStrokeCap(ck.StrokeCap.Round);
    p.setStrokeJoin(ck.StrokeJoin.Round);
    return p;
  };
  const fill = (hex: string, alpha = 1) => {
    const p = new ck.Paint();
    p.setAntiAlias(true);
    p.setStyle(ck.PaintStyle.Fill);
    p.setColor(color(hex, alpha));
    return p;
  };
  const path = (d: string, paints: InstanceType<CK["Paint"]>[], evenOdd = false) => {
    const p = ck.Path.MakeFromSVGString(d);
    if (!p) throw new Error(`[og/map] not a path: ${d.slice(0, 40)}`);
    if (evenOdd) p.setFillType(ck.FillType.EvenOdd);
    for (const paint of paints) canvas.drawPath(p, paint);
    p.delete();
  };

  /** One line of Inter at (x, y), anchored like SVG text; returns its width. */
  const text = (
    words: string,
    x: number,
    y: number,
    o: { size: number; weight?: number; hex: string; alpha?: number; anchor?: "start" | "middle" | "end"; central?: boolean; measure?: boolean },
  ): number => {
    const style = new ck.ParagraphStyle({
      textStyle: {
        color: color(o.hex, o.alpha ?? 1),
        fontFamilies: ["Inter"],
        fontSize: o.size,
        fontVariations: [{ axis: "wght", value: o.weight ?? 400 }],
      },
    });
    const builder = ck.ParagraphBuilder.MakeFromFontProvider(style, fonts);
    builder.addText(words);
    const para = builder.build();
    para.layout(4000);
    const w = para.getMaxIntrinsicWidth();
    if (!o.measure) {
      const left = o.anchor === "end" ? x - w : o.anchor === "middle" ? x - w / 2 : x;
      const baseline = o.central ? y + CENTRAL * o.size : y;
      canvas.drawParagraph(para, left, baseline - para.getAlphabeticBaseline());
    }
    para.delete();
    builder.delete();
    return w;
  };

  // The map, scaled into the card with a small margin.
  const s = (CARD.h - 32) / VIEW.h;
  canvas.save();
  canvas.translate((CARD.w - W * s) / 2, (CARD.h - VIEW.h * s) / 2 - VIEW.y * s);
  canvas.scale(s, s);

  const wire = stroke(C.fg, 2, 0.9);
  const hairline = stroke(C.fg, 0.9, 0.24);
  const headFill = fill(C.fg, 0.9);
  const headLine = stroke(C.fg, 1.5, 0.9);
  for (const f of [...flows, ...txFlows]) {
    path(f.d, [wire]);
    path(f.head, [headFill, headLine]);
  }
  text("sources push in", NOTE.sources.x, NOTE.sources.y, { size: 12.5, hex: C.fg, alpha: 0.72, anchor: "middle" });
  text("ack", NOTE.ack.x, NOTE.ack.y, { size: 12.5, hex: C.fg, alpha: 0.72 });
  text("next pushes", NOTE.next.x, NOTE.next.y, { size: 12.5, hex: C.fg, alpha: 0.72, anchor: "middle" });

  const fanHead = fill(C.fg, 0.35);
  const dot = fill(C.dot);
  const iconLine = stroke(C.yellow, 2);
  const iconDash = stroke(C.yellow, 2);
  iconDash.setPathEffect(ck.PathEffect.MakeDash([3, 2.6], 0));
  const iconFill = fill(C.yellow);
  for (const bx of boxes) {
    path(bx.fanHead, [fanHead]);
    // The icon, on its 48-unit grid.
    canvas.save();
    canvas.translate(bx.cx - ICON / 2, bx.cy - ICON / 2);
    canvas.scale(ICON / 48, ICON / 48);
    for (const { tag, a } of elements(ICON_BODY[bx.core.id])) {
      const paints = [a["stroke-dasharray"] ? iconDash : iconLine, ...(a.fill === "currentColor" ? [iconFill] : [])];
      for (const paint of paints) {
        if (tag === "path") path(a.d, [paint]);
        else if (tag === "circle") canvas.drawCircle(+a.cx, +a.cy, +a.r, paint);
        else if (tag === "ellipse") canvas.drawOval(ck.LTRBRect(+a.cx - +a.rx, +a.cy - +a.ry, +a.cx + +a.rx, +a.cy + +a.ry), paint);
        else canvas.drawRRect(ck.RRectXY(ck.XYWHRect(+a.x, +a.y, +a.width, +a.height), +(a.rx ?? 0), +(a.rx ?? 0)), paint);
      }
    }
    canvas.restore();
    text(bx.core.label, bx.cx, bx.cy + NAME_Y, { size: 16, weight: 600, hex: C.fg, anchor: "middle", central: true });

    for (const d of bx.dots) {
      path(d.curve, [hairline]);
      canvas.drawCircle(d.x, d.y, 5, dot);
      // The words, and the release tag after them, anchored together as in the SVG.
      const label = { size: 15, hex: C.soft, central: true } as const;
      const tag = { size: 12, weight: 600, hex: C.muted, central: true } as const;
      const lw = text(d.entry.label, 0, 0, { ...label, measure: true });
      const tw = d.entry.since ? 6 + text(d.entry.since, 0, 0, { ...tag, measure: true }) : 0;
      const x0 = d.anchor === "end" ? d.lx - lw - tw : d.lx;
      text(d.entry.label, x0, d.y, label);
      if (d.entry.since) text(d.entry.since, x0 + lw + 6, d.y, tag);
    }
  }
  canvas.restore();

  // The title, in the drawing's empty top-left corner: the logo, two lines, the address.
  const [, vx, vy, , vh] = logo.match(/viewBox="([\d.-]+) ([\d.-]+) ([\d.-]+) ([\d.-]+)"/)!.map(Number);
  const ls = 44 / vh;
  canvas.save();
  canvas.translate(34, 30);
  canvas.scale(ls, ls);
  canvas.translate(-vx, -vy);
  for (const { a } of elements(logo)) path(a.d, [fill(a.fill === "currentColor" ? C.fg : a.fill)], a["fill-rule"] === "evenodd");
  canvas.restore();
  const lines = MAP_TITLE.match(/^(.+?) (does, .+)$/)?.slice(1) ?? [MAP_TITLE];
  lines.forEach((line, i) => text(line, 78, 52 + i * 28, { size: 23, weight: 650, hex: C.fg }));
  text("queenmq.com/map", 78, 48 + lines.length * 28, { size: 14, hex: C.muted });

  const png = surface.makeImageSnapshot().encodeToBytes();
  surface.delete();
  if (!png) throw new Error("[og/map] CanvasKit returned no PNG");
  return new Response(png, { headers: { "Content-Type": "image/png" } });
}
