/**
 * The map's drawing (/map/), as numbers: where each part, dot, line and word
 * goes on the grid, and each part's icon. Two things draw from it: the page
 * (src/components/QueenMap.astro, as SVG) and the page's preview image
 * (src/pages/og/map.png.ts, with CanvasKit at build time), so the image never
 * shows an older map than the page.
 */
import { cores, type CoreId, type MapCore, type MapEntry } from "./map";

// ---------------------------------------------------------------------------
// Geometry: the sketch's proportions (about 1.9 wide for 1 high), drawn at the
// page's full width. Each part is its yellow icon with its name under it; lines
// and dot fans stop at the icon (or, from below, under the name).
// ---------------------------------------------------------------------------

export const W = 1640;
/** The part of the grid the drawing uses, top to bottom: no empty band above or under it. */
export const VIEW = { y: 6, h: 874 };
export const ICON = 56; // a part's icon, square
export const HALF = ICON / 2 + 4; // from a part's center to where a line stops: the icon and a gap
export const NAME_Y = ICON / 2 + 24; // from a part's center to the middle of its name
export const UNDER = NAME_Y + 12; // from a part's center to below its name
export const R = 5; // a dot: small and quiet; the nine parts are the landmarks
export const DY = 26; // between two dots
export const FAN = 96; // from a part's side to its column of dots
export const CHAR = 7.9; // width of a dot label's character, for hit areas
export const NAME_CHAR = 9; // width of a part name's character, for hit areas

/** Columns and rows by a part's center; the bus by its x. */
export const COL = { push: 458, queue: 838, bus: 1010, readers: 1158, apart: 1520 };
export const ROW = { connectors: 114, main: 323, stream: 575, bottom: 700 };
/** The queue's settings climb a staircase over it: tighter, as there are ten. */
export const STAIR = { dy: 22, dx: 30, gap: 40 };

export type Side = "left" | "right" | "above" | "below";
export const PLACE: Record<CoreId, { cx: number; cy: number; side: Side }> = {
  connectors: { cx: COL.readers, cy: ROW.connectors, side: "right" },
  push: { cx: COL.push, cy: ROW.main, side: "left" },
  queue: { cx: COL.queue, cy: ROW.main, side: "above" },
  pop: { cx: COL.readers, cy: ROW.main, side: "right" },
  stream: { cx: COL.readers, cy: ROW.stream, side: "right" },
  kv: { cx: COL.push, cy: ROW.bottom, side: "left" },
  transaction: { cx: COL.queue, cy: ROW.bottom, side: "below" },
  timers: { cx: COL.readers, cy: ROW.bottom, side: "below" },
  ephemeral: { cx: COL.apart, cy: ROW.bottom, side: "below" },
};

/** A small filled arrowhead with its tip at (x, y). */
export function head(x: number, y: number, dir: "up" | "down" | "left" | "right"): string {
  const a = 4;
  const l = 7;
  switch (dir) {
    case "up":
      return `M${x - a} ${y + l} L${x} ${y} L${x + a} ${y + l} Z`;
    case "down":
      return `M${x - a} ${y - l} L${x} ${y} L${x + a} ${y - l} Z`;
    case "left":
      return `M${x + l} ${y - a} L${x} ${y} L${x + l} ${y + a} Z`;
    case "right":
      return `M${x - l} ${y - a} L${x} ${y} L${x - l} ${y + a} Z`;
  }
}

export interface Dot {
  entry: MapEntry;
  x: number;
  y: number;
  lx: number;
  anchor: "start" | "end";
  curve: string;
  hit: { x: number; y: number; w: number; h: number };
}

export interface Box {
  core: MapCore;
  cx: number;
  cy: number;
  dots: Dot[];
  fanHead: string;
  hit: { x: number; y: number; w: number; h: number };
}

export const r1 = (n: number) => Math.round(n * 10) / 10;

export const boxes: Box[] = cores.map((core) => {
  const { cx, cy, side } = PLACE[core.id];
  const n = core.items.length;
  const dots: Dot[] = core.items.map((entry, i) => {
    let x: number, y: number, lx: number, anchor: "start" | "end", curve: string;
    if (side === "left") {
      x = cx - HALF - FAN;
      y = cy + (i - (n - 1) / 2) * DY;
      lx = x - 15;
      anchor = "end";
      const sx = x + R + 4;
      const ex = cx - HALF - 7;
      const mx = (sx + ex) / 2;
      curve = `M${sx} ${y} C${mx} ${y} ${mx} ${cy} ${ex} ${cy}`;
    } else if (side === "right") {
      x = cx + HALF + FAN;
      y = cy + (i - (n - 1) / 2) * DY;
      lx = x + 15;
      anchor = "start";
      const sx = cx + HALF + 7;
      const ex = x - R - 4;
      const mx = (sx + ex) / 2;
      curve = `M${sx} ${cy} C${mx} ${cy} ${mx} ${y} ${ex} ${y}`;
    } else if (side === "above") {
      // A staircase up and to the left, the shortest words at the top, each curve
      // sweeping into the middle of the icon's top.
      const k = n - 1 - i;
      x = cx - 36 - k * STAIR.dx;
      y = cy - HALF - STAIR.gap - k * STAIR.dy;
      lx = x - 15;
      anchor = "end";
      const sx = x + R + 4;
      const ay = cy - HALF - 7;
      curve = `M${sx} ${y} C${r1(sx + 0.6 * (cx - sx))} ${y} ${cx} ${r1(y + 0.4 * (ay - y))} ${cx} ${ay}`;
    } else {
      // A column under the name, each curve leaving from under it.
      x = cx - 4;
      y = cy + UNDER + 24 + i * DY;
      lx = x + 15;
      anchor = "start";
      const ax = cx - 26;
      const ay = cy + UNDER + 7;
      const ex = x - R - 4;
      curve = `M${ax} ${ay} C${ax} ${r1(ay + 0.65 * (y - ay))} ${r1(ax + 0.25 * (ex - ax))} ${y} ${ex} ${y}`;
    }
    const w = 30 + (entry.label.length + (entry.since ? 4 : 0)) * CHAR;
    const hit =
      anchor === "start"
        ? { x: x - R - 6, y: y - DY / 2, w: w, h: DY }
        : { x: lx - w + 30 - 6, y: y - DY / 2, w: w, h: DY };
    return { entry, x, y, lx, anchor, curve, hit };
  });
  const fanHead =
    side === "left"
      ? head(cx - HALF, cy, "right")
      : side === "right"
        ? head(cx + HALF, cy, "left")
        : side === "above"
          ? head(cx, cy - HALF, "down")
          : head(cx - 26, cy + UNDER, "up");
  // The icon and its name take the pointer together.
  const hw = Math.max(ICON / 2 + 8, (core.label.length * NAME_CHAR) / 2 + 8);
  const hit = { x: cx - hw, y: cy - ICON / 2 - 8, w: 2 * hw, h: ICON / 2 + 8 + UNDER };
  return { core, cx, cy, dots, fanHead, hit };
});

/**
 * The part of the grid the drawing uses, edge to edge: the page fits this, not
 * the whole grid, so the map draws as large as the window allows. A word's
 * width is estimated on the wide side (WORD per character), so none is cut.
 */
const WORD = 8.6;
export const FRAME = (() => {
  let l = Infinity;
  let r = -Infinity;
  for (const bx of boxes) {
    const ring = ICON / 2 + 13;
    const name = (bx.core.label.length * NAME_CHAR) / 2;
    l = Math.min(l, bx.cx - Math.max(ring, name));
    r = Math.max(r, bx.cx + Math.max(ring, name));
    for (const d of bx.dots) {
      const words = (d.entry.label.length + (d.entry.since ? 4 : 0)) * WORD;
      l = Math.min(l, d.anchor === "end" ? d.lx - words : d.x - R - 5);
      r = Math.max(r, d.anchor === "end" ? d.x + R + 5 : d.lx + words);
    }
  }
  const x = Math.floor(l - 8);
  return { x, y: VIEW.y, w: Math.ceil(r + 8) - x, h: VIEW.h };
})();

export const center = (id: CoreId) => PLACE[id].cx;
export const mid = (id: CoreId) => PLACE[id].cy;
export const left = (id: CoreId) => PLACE[id].cx - HALF;
export const right = (id: CoreId) => PLACE[id].cx + HALF;
export const top = (id: CoreId) => PLACE[id].cy - HALF;
export const under = (id: CoreId) => PLACE[id].cy + UNDER;

export const LOOP_Y = 30; // the connectors' line back into Push
export const NEXT_Y = 600; // the transaction's next pushes, on their way to Push

/** How a message moves: Push to Queue, the queue to its three readers, and the connectors back into Push. */
export const flows = [
  { d: `M${right("push") + 2} ${mid("push")} H${left("queue") - 7}`, head: head(left("queue"), mid("queue"), "right") },
  { d: `M${right("queue") + 2} ${mid("queue")} H${left("pop") - 7}`, head: head(left("pop"), mid("pop"), "right") },
  {
    d: `M${COL.bus} ${mid("queue")} V${mid("connectors") + 10} Q${COL.bus} ${mid("connectors")} ${COL.bus + 10} ${mid("connectors")} H${left("connectors") - 7}`,
    head: head(left("connectors"), mid("connectors"), "right"),
  },
  {
    d: `M${COL.bus} ${mid("queue")} V${mid("stream") - 10} Q${COL.bus} ${mid("stream")} ${COL.bus + 10} ${mid("stream")} H${left("stream") - 7}`,
    head: head(left("stream"), mid("stream"), "right"),
  },
  {
    // The connectors' sources push into queues like any producer.
    d:
      `M${center("connectors")} ${top("connectors") - 2} V${LOOP_Y + 12} Q${center("connectors")} ${LOOP_Y} ${center("connectors") - 12} ${LOOP_Y} ` +
      `H${center("push") + 12} Q${center("push")} ${LOOP_Y} ${center("push")} ${LOOP_Y + 12} V${top("push") - 7}`,
    head: head(center("push"), top("push"), "down"),
  },
];

/**
 * What one transaction does: from the top of its icon one stem goes up, acks the
 * queue, and turns off to Push with the next messages; it writes KV and locks on
 * its left and schedules timers on its right.
 */
export const txFlows = [
  {
    d: `M${center("transaction")} ${top("transaction") - 2} V${under("queue") + 7}`,
    head: head(center("transaction"), under("queue"), "up"),
  },
  {
    d:
      `M${center("transaction")} ${top("transaction") - 2} V${NEXT_Y + 12} Q${center("transaction")} ${NEXT_Y} ${center("transaction") - 12} ${NEXT_Y} ` +
      `H${center("push") + 12} Q${center("push")} ${NEXT_Y} ${center("push")} ${NEXT_Y - 12} V${under("push") + 7}`,
    head: head(center("push"), under("push"), "up"),
  },
  { d: `M${left("transaction") - 2} ${mid("transaction")} H${right("kv") + 7}`, head: head(right("kv"), mid("kv"), "left") },
  { d: `M${right("transaction") + 2} ${mid("transaction")} H${left("timers") - 7}`, head: head(left("timers"), mid("timers"), "right") },
];

/** The words on the lines. */
export const NOTE = {
  sources: { x: (center("push") + center("connectors")) / 2, y: LOOP_Y - 9 },
  ack: { x: center("transaction") + 9, y: (under("queue") + NEXT_Y) / 2 },
  next: { x: (center("push") + center("transaction")) / 2, y: NEXT_Y - 9 },
};

/**
 * Each part's icon, drawn for QueenMQ from the map's own pieces (Alice's pick,
 * 2026-11-16, over Phosphor's bold set, which read as toys):
 *
 *   grid      48 x 48 units, drawings inside 2..46
 *   line      2 units, round caps and joins, currentColor (the map's yellow)
 *   message   a card 9 x 17, corner 1.5, with a header line: outlined = kept,
 *             filled = the one moving, dashed = not kept
 *   arrow     a line ending in a 3.5-unit chevron
 *
 * Push, Queue and Pop share one row of messages on a lane: a message joins at
 * the tail, the row keeps them in order, the one at the head leaves.
 */
const card = (x: number, y: number, { w = 9, h = 17, fill = false, dash = false } = {}) =>
  `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="1.5"${fill ? ' fill="currentColor"' : ""}${dash ? ' stroke-dasharray="3 2.6"' : ""}/>` +
  (fill ? "" : `<path d="M${x + 2.5} ${y + 5}H${x + w - 2.5}"/>`);
const arrowRight = (x1: number, x2: number, y: number) => `<path d="M${x1} ${y}H${x2}M${x2 - 3.5} ${y - 3.5}L${x2} ${y}L${x2 - 3.5} ${y + 3.5}"/>`;
const arrowLeft = (x1: number, x2: number, y: number) => `<path d="M${x1} ${y}H${x2}M${x2 + 3.5} ${y - 3.5}L${x2} ${y}L${x2 + 3.5} ${y + 3.5}"/>`;
export const ICON_BODY: Record<CoreId, string> = {
  // A message joins the queue, at its tail.
  push: arrowRight(2, 8.5, 19.5) + card(11, 11, { fill: true }) + card(23, 11) + card(35, 11) + `<path d="M21 34.5H45.5"/>`,
  // Messages kept in a row, in order.
  queue: card(5.5, 11) + card(19.5, 11) + card(33.5, 11) + `<path d="M3 34.5H45"/>`,
  // The message at the head leaves, to a worker.
  pop: `<path d="M2.5 34.5H27"/>` + card(4.5, 11) + card(16.5, 11) + card(28.5, 11, { fill: true }) + arrowRight(39.5, 46, 19.5),
  // A database and a queue, both ways.
  connectors:
    `<ellipse cx="12" cy="13" rx="8" ry="3.5"/><path d="M4 13V35A8 3.5 0 0 0 20 35V13"/><path d="M4 24A8 3.5 0 0 0 20 24"/>` +
    `<path d="M23.5 24H31.5M27 20.5L23.5 24L27 27.5M28 20.5L31.5 24L28 27.5"/>` +
    card(35, 15.5),
  // A flow, computed as it passes: two waves, as the console draws streams.
  stream: `<path d="M4 18C9 11 14 11 19 18S29 25 34 18S41.5 13 44 15"/><path d="M4 31C9 24 14 24 19 31S29 38 34 31S41.5 26 44 28"/>`,
  // Three messages held by one bracket: all or nothing.
  transaction: card(7, 8) + card(19.5, 8) + card(32, 8) + `<path d="M4.5 31V37.5H43.5V31"/>`,
  // A key cell linked to its value cell.
  kv:
    `<rect x="4" y="13" width="14" height="22" rx="2.5"/><circle cx="11" cy="24" r="2.2" fill="currentColor"/>` +
    `<path d="M18 24H25"/><rect x="25" y="13" width="19" height="22" rx="2.5"/><path d="M30 20H39M30 28H36"/>`,
  // A dial: later, at a set time.
  timers: `<circle cx="24" cy="27" r="16"/><path d="M24 6V11M19.5 6H28.5M24 27L30 17.5"/><circle cx="24" cy="27" r="2" fill="currentColor"/>`,
  // A request and its reply, in memory: dashed, not kept.
  ephemeral:
    card(3, 11, { w: 10, h: 26, dash: true }) + card(35, 11, { w: 10, h: 26, dash: true }) + arrowRight(17, 31, 19.5) + arrowLeft(31, 17, 28.5),
};
/** An icon's drawing, in its line style, for an `<svg viewBox="0 0 48 48">` or a `<g>` scaled to 48. */
export const iconSvg = (id: CoreId) =>
  `<g fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${ICON_BODY[id]}</g>`;

/**
 * Where a goal's step number sits: beside a dot, on the side away from its
 * words; at a part's top left; or on the transaction's "ack" line.
 */
export function stepMark(id: string, onAckLine = false): { x: number; y: number } {
  if (onAckLine) return { x: center("transaction") - 22, y: NOTE.ack.y - 4 };
  for (const bx of boxes) {
    if (bx.core.id === id) return { x: bx.cx - 36, y: bx.cy - 34 };
    const d = bx.dots.find((dot) => dot.entry.id === id);
    if (d) return { x: d.anchor === "start" ? d.x - 24 : d.x + 24, y: d.y };
  }
  throw new Error(`[map] the drawing has no piece "${id}"`);
}
