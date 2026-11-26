/**
 * The homepage hero's artwork: QueenMQ in the middle of four services, each a
 * window of the five-minute guide's code. Each window shows one face of what
 * the headline promises, named in yellow in its title bar: queues (the orders
 * service pushes), locks (the daily report runs under one), memory and timers
 * committed together with the next message (the fulfillment worker), and the
 * broker it also is (an audit reader replays from any point). The services
 * that write sit on the left and the ones that read on the right, so every
 * arrow runs left to right, through QueenMQ.
 *
 * The code is the guide's (examples/tutorials/js/00-five-minutes.mjs, the
 * program that runs every step against a real broker). A window shows lines of
 * its step's region: some of a chain may be left out and short lines joined,
 * but nothing is rewritten. Every line must be found in its region, ignoring
 * spaces and comments, or the build fails, so the picture cannot show code
 * that did not run.
 */

import icon from "../../../assets/redesign/icon/queenmq-icon.svg?raw";
import { GUIDE_LANGS, guideCode } from "./home-guide";

interface TreeWindow {
  /** Where it goes: push and lock on the left (they write), txn and replay on the right (they read). */
  id: "push" | "lock" | "txn" | "replay";
  /** The face of QueenMQ it shows, in the headline's words, in yellow in its title bar. */
  face: string;
  /** The guide step whose region the lines come from. */
  region: string;
  title: string;
  lines: string[];
}

const TREE_WINDOWS: TreeWindow[] = [
  {
    id: "push",
    face: "queues",
    region: "push",
    title: "orders service: one partition per customer",
    lines: [
      "await queen.queue('orders')",
      "  .partition('customer-42')",
      "  .push([{",
      "    transactionId: 'order-1001',",
      "    data: { orderId: 1001, customer: 42, total: 59.9 },",
      "  }])",
    ],
  },
  {
    id: "lock",
    face: "locks",
    region: "lock",
    title: "daily report: one worker at a time",
    lines: [
      "const lock = queen.lock('daily-report', { ttl: '30s' })",
      "if (await lock.acquire()) {",
      "  try {",
      "    await queen.transaction().guard(lock)",
      "      .queue('reports').push([{ transactionId: 'report-2026-10-22', data: { day: '2026-10-22' } }])",
      "      .commit()",
      "  } finally { await lock.release() }",
      "}",
    ],
  },
  {
    id: "txn",
    face: "memory · timers",
    region: "transaction",
    title: "fulfillment: one step, one commit",
    lines: [
      "await queen.transaction()",
      "  .ack(msg, 'completed', { consumerGroup: 'fulfillment' })",
      "  .kv.put('orders', `order-${orderId}`, { status: 'paid' }, { ttl: '7d' })",
      "  .queue('shipments').partition(msg.partition)",
      "  .push([{ transactionId: `ship-${orderId}`, data: { orderId } }])",
      "  .timer('reminders').key(`review-${orderId}`).delay('24h')",
      "  .payload({ orderId, customer }).schedule()",
      "  .commit()",
    ],
  },
  {
    id: "replay",
    face: "the broker",
    region: "replay",
    title: "audit: a new reader, from any point",
    // the guide's .limit(1) is left out: it only lets the program end
    lines: [
      "await queen.queue('orders')",
      "  .group('audit')",
      "  .subscriptionFrom('2026-10-22T00:00:00Z')",
      "  .each()",
      "  .consume(async (msg) => {",
      "    console.log('again:', msg.data)",
      "  })",
    ],
  },
];

const JS = GUIDE_LANGS.find((l) => l.id === "js")!;
const squash = (s: string) => s.replace(/[ \t]+\/\/.*$/gm, "").replace(/\s+/g, "");

for (const w of TREE_WINDOWS) {
  const region = guideCode(JS, w.region);
  if (!region) throw new Error(`hero tree: no "${w.region}" region in ${JS.file}`);
  const hay = squash(region);
  for (const line of w.lines) {
    if (!hay.includes(squash(line))) {
      throw new Error(`hero tree: "${line.trim()}" is not in the ${w.region} region of ${JS.file}`);
    }
  }
}

type TokenKind = "kw" | "str" | "fn" | "punct" | "text";

const TOKEN = /\b(await|const|if|try|finally|async)\b|('[^']*'|`[^`]*`)|(\.[A-Za-z_]\w*)(?=\()|([{}()[\],:.])/g;

/** A line of code cut into the runs the window colours: keywords, strings, calls, punctuation. */
function tokens(line: string): { text: string; kind: TokenKind }[] {
  const out: { text: string; kind: TokenKind }[] = [];
  let at = 0;
  for (const m of line.matchAll(TOKEN)) {
    if (m.index! > at) out.push({ text: line.slice(at, m.index), kind: "text" });
    out.push({ text: m[0], kind: m[1] ? "kw" : m[2] ? "str" : m[3] ? "fn" : "punct" });
    at = m.index! + m[0].length;
  }
  if (at < line.length) out.push({ text: line.slice(at), kind: "text" });
  return out;
}

/*
 * The picture. Its own units are CSS pixels at 1360 wide: from there up it is
 * drawn 1:1, and narrower it scales down (the page shows it from 1280 up).
 *
 * The tile sits in the middle; each side has a column of two windows 150 units
 * from it, both columns centred on it. A wire runs from the line of code that
 * does the thing (the push, the guard) to the tile, or from the tile to the
 * line that takes it (the ack, the replay), as a dotted elbow whose bend sits
 * near the window. Its label sits beside the tile, on the side of the wire its
 * elbow does not take, so the two never cross. The arrow points the way the
 * data goes.
 */

const W = 1360;
const PAD_TOP = 24;
const PAD_BOTTOM = 40;
const TILE = 128;
const WIN_W = 440;
const GAP = 150; // tile to window
const COL_GAP = 34; // between the two windows of a column
const HEAD = 36; // the window's title bar
const LH = 20; // a line of code
const CODE_TOP = HEAD + 4 + LH / 2 + 3; // the first line's middle, from the window's top
const SPREAD = 34; // the wires meet the tile this far above and below its middle

const INK = "#151311";
const C = {
  card: INK,
  edge: "#2a2724",
  head: "#1c1a18",
  dots: "#3a3631",
  title: "#9a948c",
  face: "#fcc620",
  glow: "#fff4c2",
  label: "rgb(21 19 17 / 0.62)",
  kw: "#fcc620",
  str: "#c9b98f",
  fn: "#f4f2ee",
  punct: "#8f8981",
  text: "#d8d2c9",
} as const;

const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const winH = (w: TreeWindow) => HEAD + 4 + w.lines.length * LH + 18;
const colH = (a: TreeWindow, b: TreeWindow) => winH(a) + COL_GAP + winH(b);
const byId = (id: TreeWindow["id"]) => TREE_WINDOWS.find((w) => w.id === id)!;

/** The y of a window's line that holds `needle`, from the picture's top. */
function lineY(w: TreeWindow, top: number, needle: string): number {
  const k = w.lines.findIndex((l) => l.includes(needle));
  if (k < 0) throw new Error(`hero tree: no line with "${needle}" in the ${w.id} window`);
  return top + CODE_TOP + k * LH;
}

function windowSvg(w: TreeWindow, x: number, y: number): string {
  const h = winH(w);
  const clip = `qt-clip-${w.id}`;
  const code = w.lines
    .map((line, k) => {
      const runs = tokens(line)
        .map((t) => `<tspan fill="${C[t.kind]}">${esc(t.text)}</tspan>`)
        .join("");
      return `<text x="${x + 20}" y="${y + CODE_TOP + k * LH}" dominant-baseline="central" xml:space="preserve">${runs}</text>`;
    })
    .join("");
  const dots = [0, 1, 2]
    .map((k) => `<circle cx="${x + 20 + k * 15}" cy="${y + HEAD / 2}" r="4.5" fill="${C.dots}"/>`)
    .join("");
  return (
    `<clipPath id="${clip}"><rect x="${x}" y="${y}" width="${WIN_W}" height="${h}" rx="12"/></clipPath>` +
    `<rect x="${x}" y="${y}" width="${WIN_W}" height="${h}" rx="12" fill="${C.card}" stroke="${C.edge}" filter="url(#qt-shadow)"/>` +
    `<g clip-path="url(#${clip})">` +
    `<rect x="${x}" y="${y}" width="${WIN_W}" height="${HEAD}" fill="${C.head}"/>` +
    `<line x1="${x}" y1="${y + HEAD}" x2="${x + WIN_W}" y2="${y + HEAD}" stroke="${C.edge}"/>` +
    dots +
    // the face in yellow, then what this service does with it
    `<text x="${x + 74}" y="${y + HEAD / 2}" dominant-baseline="central" class="qt-title">` +
    `<tspan fill="${C.face}" font-weight="650">${esc(w.face)}</tspan>` +
    `<tspan fill="${C.title}" dx="10">${esc(w.title)}</tspan></text>` +
    `<g class="qt-code">${code}</g>` +
    // where a long line meets the right edge it fades out rather than stops
    `<rect x="${x + WIN_W - 56}" y="${y + HEAD + 1}" width="55" height="${h - HEAD - 2}" fill="url(#qt-fade)"/>` +
    `</g>`
  );
}

/** A dotted elbow from (x1, y1) to (x2, y2), bending at bx, with the arrow at the end. */
function connector(x1: number, y1: number, bx: number, x2: number, y2: number): string {
  const d = x2 > bx ? 1 : -1; // the last run's direction
  const arrow = `M${x2 - d * 9},${y2 - 5} L${x2},${y2} L${x2 - d * 9},${y2 + 5} Z`;
  return (
    `<path d="M${x1},${y1} H${bx} V${y2} H${x2}" class="qt-wire"/>` +
    `<path d="${arrow}" fill="${INK}" fill-opacity="0.7"/>`
  );
}

/** A wire's label beside the tile, on the run at `runY`: below it when the elbow climbs to `winY`, above when it drops. */
function label(x: number, runY: number, winY: number, text: string, anchor: "start" | "end"): string {
  const y = winY < runY ? runY + 13 : runY - 12;
  return `<text x="${x}" y="${y}" dominant-baseline="central" text-anchor="${anchor}" class="qt-label" fill="${C.label}">${esc(text)}</text>`;
}

/** The whole picture as one inline SVG, the QueenMQ icon in the middle. */
export function treeSvg(): string {
  const [push, lock, txn, replay] = (["push", "lock", "txn", "replay"] as const).map(byId);
  const leftH = colH(push, lock);
  const rightH = colH(txn, replay);
  const bandH = Math.max(leftH, rightH);
  const H = PAD_TOP + bandH + PAD_BOTTOM;

  const cx = W / 2;
  const cy = PAD_TOP + bandH / 2;
  const lx = cx - TILE / 2 - GAP - WIN_W; // the left column's x
  const rx = cx + TILE / 2 + GAP; // the right column's x
  const pushY = PAD_TOP + (bandH - leftH) / 2;
  const lockY = pushY + winH(push) + COL_GAP;
  const txnY = PAD_TOP + (bandH - rightH) / 2;
  const replayY = txnY + winH(txn) + COL_GAP;

  const tl = cx - TILE / 2 - 6; // where wires meet the tile
  const tr = cx + TILE / 2 + 6;
  const up = cy - SPREAD;
  const down = cy + SPREAD;
  const bendL = lx + WIN_W + 34;
  const bendR = rx - 34;

  const pushAt = lineY(push, pushY, ".push(");
  const guardAt = lineY(lock, lockY, ".guard(");
  const ackAt = lineY(txn, txnY, ".ack(");
  const replayAt = lineY(replay, replayY, ".subscriptionFrom(");
  const wires =
    // the writers, into QueenMQ: the push, and the transaction the lock guards
    connector(lx + WIN_W, pushAt, bendL, tl, up) +
    label(tl - 14, up, pushAt, "push", "end") +
    connector(lx + WIN_W, guardAt, bendL, tl, down) +
    label(tl - 14, down, guardAt, "guard(lock)", "end") +
    // the readers, out of it: the worker's step, and the audit's replay
    connector(tr, up, bendR, rx, ackAt) +
    label(tr + 14, up, ackAt, "pop · commit()", "start") +
    connector(tr, down, bendR, rx, replayAt) +
    label(tr + 14, down, replayAt, "replay", "start");

  const vb = icon.match(/viewBox="([^"]+)"/)![1].split(/\s+/).map(Number);
  const tile = icon
    .replace(/^[\s\S]*?<svg[^>]*>/, "")
    .replace(/<\/svg>\s*$/, "")
    .replace(/\s+id="[^"]*"/g, "");
  const tx = cx - TILE / 2;
  const ty = cy - TILE / 2;

  const alt =
    "QueenMQ in the middle of four services, each a window of the five-minute guide's code. " +
    "On the left, the ones that write: the orders service pushes into a queue, and the daily report runs under a lock. " +
    "On the right, the ones that read: the fulfillment worker writes memory, sets a timer and pushes the next message in one commit, " +
    "and an audit reader replays every order from a point in time.";

  return (
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="${esc(alt)}">` +
    `<defs>` +
    `<filter id="qt-shadow" x="-20%" y="-20%" width="140%" height="160%"><feDropShadow dx="0" dy="10" stdDeviation="14" flood-color="#000" flood-opacity="0.22"/></filter>` +
    `<radialGradient id="qt-glow"><stop offset="0" stop-color="${C.glow}" stop-opacity="0.6"/><stop offset="1" stop-color="${C.glow}" stop-opacity="0"/></radialGradient>` +
    `<linearGradient id="qt-fade" x1="0" x2="1"><stop offset="0" stop-color="${C.card}" stop-opacity="0"/><stop offset="1" stop-color="${C.card}"/></linearGradient>` +
    `</defs>` +
    `<ellipse cx="${cx}" cy="${cy}" rx="360" ry="${Math.min(250, cy - 4)}" fill="url(#qt-glow)"/>` +
    wires +
    windowSvg(push, lx, pushY) +
    windowSvg(lock, lx, lockY) +
    windowSvg(txn, rx, txnY) +
    windowSvg(replay, rx, replayY) +
    `<g filter="url(#qt-shadow)"><g transform="translate(${tx},${ty}) scale(${(TILE / vb[2]).toFixed(4)}) translate(${-vb[0]},${-vb[1]})">${tile}</g></g>` +
    `</svg>`
  );
}
