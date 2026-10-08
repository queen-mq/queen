/**
 * Drawing helpers shared by the inline-SVG schematics (JobLifecycle,
 * LeaseTimeline, RollingUpdate, WorkerTimeline, SupervisorTopology,
 * PreforkMemory). They follow Partition.astro: colours are theme tokens, so a
 * figure follows the reader's surface with no second asset and no script, and
 * geometry is computed rather than placed, so a label cannot drift from the
 * mark it names.
 */

/** Theme tokens by role. Status colours carry meaning, never decoration. */
export const TOKEN = {
  fg: "var(--nb-foreground)",
  muted: "var(--nb-muted-foreground)",
  card: "var(--nb-card)",
  surface: "var(--nb-muted)",
  border: "var(--nb-border)",
  borderStrong: "var(--nb-border-strong)",
  primary: "var(--nb-primary-text)",
  /** Requests in flight and the broker. */
  info: "var(--nb-info)",
  infoBg: "var(--nb-info-muted)",
  /** Acknowledged, done, shared once. */
  success: "var(--nb-success)",
  successBg: "var(--nb-success-muted)",
  /** Leases, timers, SIGTERM: time is running. */
  warning: "var(--nb-warning)",
  warningBg: "var(--nb-warning-muted)",
  /** Failure, SIGKILL, an unreachable broker. */
  danger: "var(--nb-danger)",
  dangerBg: "var(--nb-danger-muted)",
} as const;

export type Point = readonly [number, number];

/**
 * A filled triangle whose tip sits at `to`, pointing away from `from`. Drawn
 * as a path rather than an SVG marker: markers need document-unique ids, and a
 * page can carry the same figure twice.
 */
export function arrowHead(from: Point, to: Point, size = 7): string {
  const [x1, y1] = from;
  const [x2, y2] = to;
  const length = Math.hypot(x2 - x1, y2 - y1) || 1;
  const ux = (x2 - x1) / length;
  const uy = (y2 - y1) / length;
  const baseX = x2 - ux * size;
  const baseY = y2 - uy * size;
  const half = size * 0.55;
  return `M ${x2} ${y2} L ${baseX - uy * half} ${baseY + ux * half} L ${baseX + uy * half} ${baseY - ux * half} Z`;
}

/**
 * A polyline through `points` with its corners rounded by `radius`, stopping
 * `inset` short of the last point so an arrowhead can sit on the end.
 */
export function route(points: readonly Point[], radius = 8, inset = 0): string {
  const pts = points.map(([x, y]) => [x, y] as [number, number]);
  if (inset > 0 && pts.length >= 2) {
    const [ax, ay] = pts[pts.length - 2];
    const [bx, by] = pts[pts.length - 1];
    const length = Math.hypot(bx - ax, by - ay) || 1;
    pts[pts.length - 1] = [bx - ((bx - ax) / length) * inset, by - ((by - ay) / length) * inset];
  }
  let d = `M ${pts[0][0]} ${pts[0][1]}`;
  for (let i = 1; i < pts.length - 1; i++) {
    const [px, py] = pts[i - 1];
    const [cx, cy] = pts[i];
    const [nx, ny] = pts[i + 1];
    const inLength = Math.hypot(cx - px, cy - py) || 1;
    const outLength = Math.hypot(nx - cx, ny - cy) || 1;
    const r = Math.min(radius, inLength / 2, outLength / 2);
    const sx = cx - ((cx - px) / inLength) * r;
    const sy = cy - ((cy - py) / inLength) * r;
    const ex = cx + ((nx - cx) / outLength) * r;
    const ey = cy + ((ny - cy) / outLength) * r;
    d += ` L ${sx} ${sy} Q ${cx} ${cy} ${ex} ${ey}`;
  }
  const [lx, ly] = pts[pts.length - 1];
  return `${d} L ${lx} ${ly}`;
}

/** An arrow along `points`: the line, and the head on its last segment. */
export function arrow(points: readonly Point[], radius = 8, size = 7): { line: string; head: string } {
  const from = points[points.length - 2];
  const to = points[points.length - 1];
  return { line: route(points, radius, size * 0.8), head: arrowHead(from, to, size) };
}

/**
 * A rough width for a label, for sizing boxes and placing neighbours. Inter
 * averages a little over half an em per character; JetBrains Mono is 0.6 em
 * exactly. Generous on purpose: a box a few pixels wide reads better than text
 * that touches its border.
 */
export function textWidth(text: string, fontSize: number, mono = false): number {
  return text.length * fontSize * (mono ? 0.6 : 0.56);
}

/** Seconds as a tick label: `90 s`, and `−30 s` with a real minus sign. */
export function seconds(value: number): string {
  const magnitude = Math.abs(value);
  const digits = Number.isInteger(magnitude) ? `${magnitude}` : magnitude.toFixed(1);
  return `${value < 0 ? "−" : ""}${digits} s`;
}
