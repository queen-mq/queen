/**
 * The homepage hero's artwork: QueenMQ in the middle of four services, each a
 * window of the five-minute guide's code, wired to it. One branch per thing the
 * headline promises: the queue (the orders service pushes), the broker (billing
 * reads every message in its own group), locks (the daily report runs under
 * one) and the step that commits them together (the fulfillment worker).
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
  /** Where it goes: group and lock on the left, push and txn on the right. */
  id: "group" | "lock" | "push" | "txn";
  /** The guide step whose region the lines come from. */
  region: string;
  title: string;
  lines: string[];
}

const TREE_WINDOWS: TreeWindow[] = [
  {
    id: "group",
    region: "consume-group",
    title: "billing: its own group, every message",
    lines: [
      "await queen.queue('orders')",
      "  .group('billing')",
      "  .subscriptionMode('all')",
      "  .autoAck(false)",
      "  .consume(async (msg) => {",
      "    const ok = msg.data.total > 0",
      "    await queen.ack(msg, ok ? 'completed' : 'failed', { group: 'billing' })",
      "  })",
    ],
  },
  {
    id: "lock",
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
    id: "push",
    region: "push",
    title: "orders service",
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
    id: "txn",
    region: "transaction",
    title: "fulfillment worker: one step, one commit",
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
 * from it, the left column at the top, the shorter right one centred on it.
 * A connector runs from the line of code that does the thing (the push, the
 * consume, the guard, the ack) to the tile or back, as a dotted elbow whose
 * bend sits near the window, so the label beside the tile has the straight
 * run to itself. The arrow points the way the data goes.
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

const INK = "#151311";
const C = {
  card: INK,
  edge: "#2a2724",
  head: "#1c1a18",
  dots: "#3a3631",
  title: "#9a948c",
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
    `<text x="${x + 74}" y="${y + HEAD / 2}" dominant-baseline="central" class="qt-title" fill="${C.title}">${esc(w.title)}</text>` +
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

function label(x: number, y: number, text: string, anchor: "start" | "end"): string {
  return `<text x="${x}" y="${y}" dominant-baseline="central" text-anchor="${anchor}" class="qt-label" fill="${C.label}">${esc(text)}</text>`;
}

/** The whole picture as one inline SVG, the QueenMQ icon in the middle. */
export function treeSvg(): string {
  const [group, lock, push, txn] = (["group", "lock", "push", "txn"] as const).map(byId);
  const leftH = winH(group) + COL_GAP + winH(lock);
  const rightH = winH(push) + COL_GAP + winH(txn);
  const H = PAD_TOP + leftH + PAD_BOTTOM;

  const cx = W / 2;
  const cy = PAD_TOP + leftH / 2;
  const lx = cx - TILE / 2 - GAP - WIN_W; // the left column's x
  const rx = cx + TILE / 2 + GAP; // the right column's x
  const groupY = PAD_TOP;
  const lockY = groupY + winH(group) + COL_GAP;
  const pushY = PAD_TOP + (leftH - rightH) / 2;
  const txnY = pushY + winH(push) + COL_GAP;

  const tl = cx - TILE / 2 - 6; // where wires meet the tile
  const tr = cx + TILE / 2 + 6;
  const up = cy - 30;
  const down = cy + 30;
  const bendL = lx + WIN_W + 34;
  const bendR = rx - 34;

  const wires =
    // orders -> QueenMQ, from the push
    connector(rx, lineY(push, pushY, ".push("), bendR, tr, up) +
    label(tr + 14, up - 12, "push", "start") +
    // QueenMQ -> the worker, into the step's ack
    connector(tr, down, bendR, rx, lineY(txn, txnY, ".ack(")) +
    label(tr + 14, down - 12, "pop · commit()", "start") +
    // QueenMQ -> billing, into its consume
    connector(tl, up, bendL, lx + WIN_W, lineY(group, groupY, ".consume(")) +
    label(tl - 14, up - 12, "every group", "end") +
    // the daily report -> QueenMQ, from the guarded transaction
    connector(lx + WIN_W, lineY(lock, lockY, ".guard("), bendL, tl, down) +
    label(tl - 14, down - 12, "guard(lock)", "end");

  const vb = icon.match(/viewBox="([^"]+)"/)![1].split(/\s+/).map(Number);
  const tile = icon
    .replace(/^[\s\S]*?<svg[^>]*>/, "")
    .replace(/<\/svg>\s*$/, "")
    .replace(/\s+id="[^"]*"/g, "");
  const tx = cx - TILE / 2;
  const ty = cy - TILE / 2;

  const alt =
    "QueenMQ in the middle of four services, each a window of the five-minute guide's code: " +
    "the orders service pushes, billing reads every message in its own group, " +
    "the daily report runs under a lock, and the fulfillment worker commits each step in one transaction.";

  return (
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="${esc(alt)}">` +
    `<defs>` +
    `<filter id="qt-shadow" x="-20%" y="-20%" width="140%" height="160%"><feDropShadow dx="0" dy="10" stdDeviation="14" flood-color="#000" flood-opacity="0.22"/></filter>` +
    `<radialGradient id="qt-glow"><stop offset="0" stop-color="${C.glow}" stop-opacity="0.6"/><stop offset="1" stop-color="${C.glow}" stop-opacity="0"/></radialGradient>` +
    `<linearGradient id="qt-fade" x1="0" x2="1"><stop offset="0" stop-color="${C.card}" stop-opacity="0"/><stop offset="1" stop-color="${C.card}"/></linearGradient>` +
    `</defs>` +
    `<ellipse cx="${cx}" cy="${cy}" rx="360" ry="${Math.min(250, cy - 4)}" fill="url(#qt-glow)"/>` +
    wires +
    windowSvg(group, lx, groupY) +
    windowSvg(lock, lx, lockY) +
    windowSvg(push, rx, pushY) +
    windowSvg(txn, rx, txnY) +
    `<g filter="url(#qt-shadow)"><g transform="translate(${tx},${ty}) scale(${(TILE / vb[2]).toFixed(4)}) translate(${-vb[0]},${-vb[1]})">${tile}</g></g>` +
    `</svg>`
  );
}
