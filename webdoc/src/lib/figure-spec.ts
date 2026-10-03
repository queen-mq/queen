/**
 * The figure spec format: what a file under `src/figures/` may say. Kept apart
 * from the registry in `figures.ts` because the registry imports every spec,
 * and a spec importing the registry back would be a cycle (the helpers below
 * would not exist yet when the first spec runs).
 */

/** How a box reads. `strong` is the subject of the figure; `ghost` is outside the system (your app, a client); `warn` and `danger` mark the parts of a failure. */
export type NodeTone = "default" | "strong" | "ghost" | "warn" | "danger";
/** How a line reads: `strong` is the path the figure is about, `faint` is context. */
export type LineTone = "default" | "strong" | "faint" | "warn" | "danger";
/**
 * A data series' colour: `s1`..`s5` are the dashboard's grey ramp (s1 is the
 * ink, so give it the series the chart is about, usually Queen), `c1`..`c5`
 * its categorical hues, for the rare chart that needs more than greys can
 * separate. `warn` and `danger` mean what they mean everywhere else.
 */
export type SeriesTone = "s1" | "s2" | "s3" | "s4" | "s5" | "c1" | "c2" | "c3" | "c4" | "c5" | "warn" | "danger";
export type Side = "l" | "r" | "t" | "b";
export type Format = "si" | "int" | "ms" | "s" | "pct" | "gb" | "x" | ((v: number) => string);

interface Words {
  /** What the figure shows, for a reader who cannot see it. Required, and carried into the markdown alternate. */
  alt: string;
  /** One or two sentences under the figure: what to notice. */
  caption?: string;
  /** Where the data comes from: an archive path, a source file. Printed under the caption. */
  source?: string;
}

export interface FlowNode {
  id: string;
  /** Column and row on the grid, 0-based. Fractions are fine: 0.5 centres a node between two columns. */
  at: [number, number];
  /** One or two lines; "\n" breaks. */
  label: string;
  /** A quieter second line (or two). */
  sub?: string;
  tone?: NodeTone;
  /** `disk` draws a cylinder (a log, a file, a store), `pill` a rounded end (a client, a process), `stack` several boxes (many of a kind). */
  shape?: "box" | "disk" | "pill" | "stack";
  /** Width in columns, default 1. */
  span?: number;
  /** Set the label in the monospace face (a queue name, a file). */
  mono?: boolean;
}

export interface FlowEdge {
  from: string;
  to: string;
  label?: string;
  tone?: LineTone;
  /** Dashed: an answer, something asynchronous, something that may not happen. */
  dashed?: boolean;
  /** Arrowheads at both ends. */
  both?: boolean;
  /** No arrowhead: a plain connection. */
  plain?: boolean;
  /** Leave and enter on these sides instead of the ones picked from the layout. */
  fromSide?: Side;
  toSide?: Side;
  /** Shift a straight edge sideways, in px, to draw a request and its answer as two parallel lines. */
  offset?: number;
  /** Where the label sits along the path, 0 to 1 (default 0.5). */
  labelAt?: number;
  /** Which side of the line the label sits on (default: above a horizontal line, right of a vertical one). */
  labelSide?: "above" | "below" | "left" | "right";
}

export interface FlowGroup {
  label: string;
  /** The boxes the group surrounds; the frame is their bounding box plus padding. */
  nodes: string[];
  tone?: "default" | "warn" | "danger";
}

export interface FlowNote {
  /** Grid position of the text's anchor, like a node's `at`. */
  at: [number, number];
  text: string;
  anchor?: "start" | "middle" | "end";
  tone?: "default" | "warn" | "danger";
}

export interface FlowSpec extends Words {
  kind: "flow";
  cols: number;
  rows: number;
  /** Column width in px (default 170) and row height (default 100). Keep cols x colWidth under about 700: the figure scales down to the text column, and its text with it. */
  colWidth?: number;
  rowHeight?: number;
  /** Horizontal space between two boxes in neighbouring columns (default 48). A label on a straight edge between them must fit it. */
  gap?: number;
  nodes: FlowNode[];
  edges?: FlowEdge[];
  groups?: FlowGroup[];
  notes?: FlowNote[];
}

export interface SeqActor {
  id: string;
  label: string;
  sub?: string;
  tone?: NodeTone;
}

export type SeqStep =
  | {
      from: string;
      /** The same id as `from` draws a message to itself (work done locally: an fsync, an apply). */
      to: string;
      label: string;
      /** A quieter line under the arrow. */
      sub?: string;
      /** A reply: dashed. */
      reply?: boolean;
      tone?: LineTone;
      /** Draw on the same row as the previous message (two things sent at once). */
      same?: boolean;
    }
  | {
      /** A box across one lifeline or between two: state, a decision, a duration. */
      note: string;
      over: string | [string, string];
      tone?: NodeTone;
    }
  | {
      /** A line across the whole diagram: "later", "after a crash". */
      divider: string;
    };

export interface SequenceSpec extends Words {
  kind: "sequence";
  actors: SeqActor[];
  steps: SeqStep[];
  /** Distance between lifelines in px (default 170). */
  gap?: number;
}

export interface Axis {
  label?: string;
  log?: boolean;
  min?: number;
  max?: number;
  /** Explicit tick positions; computed when omitted. */
  ticks?: number[];
  format?: Format;
}

export interface Point {
  x: number;
  y: number | null;
  /** A hollow mark: the point exists but failed a condition the caption names (fell behind, partial run). */
  hollow?: boolean;
  /** A short label printed next to this point. */
  note?: string;
}

export interface LineSeries {
  label: string;
  /** `[x, y]` pairs, or points with marks. A `null` y breaks the line. */
  points: Array<[number, number | null] | Point>;
  tone?: SeriesTone;
  /** Dash pattern; the series after the first get one automatically unless this is "solid". */
  dash?: "solid" | "dash" | "dot" | "dashdot";
  /** Draw a mark on every point (default: when there are 24 points or fewer). */
  marks?: boolean;
}

export interface LineSpec extends Words {
  kind: "line";
  x: Axis;
  y: Axis;
  series: LineSeries[];
  /** Vertical lines at an x: an event (a node killed, a restart). */
  markers?: Array<{ x: number; label: string; tone?: "default" | "warn" | "danger" }>;
  /** Shaded x ranges: a phase (a partition, a slow window). */
  bands?: Array<{ from: number; to: number; label?: string; tone?: "default" | "warn" | "danger" }>;
  /** Horizontal reference lines: a target, a limit. */
  hlines?: Array<{ y: number; label: string; tone?: "default" | "warn" | "danger" }>;
  width?: number;
  height?: number;
}

export interface BarSeries {
  label: string;
  /** One value per category, in order; null prints the spec's `missing` text instead of a bar. */
  values: Array<number | null>;
  tone?: SeriesTone;
  /** Per-value notes printed after the value label ("fell behind"). Same order as `values`. */
  notes?: Array<string | null | undefined>;
}

export interface BarSpec extends Words {
  kind: "bar";
  /** One row group per category, top to bottom. */
  categories: string[];
  series: BarSeries[];
  x: Axis;
  /** What a null value says, e.g. "not run". */
  missing?: string;
  width?: number;
}

export type FigureSpec = FlowSpec | SequenceSpec | LineSpec | BarSpec;

/** Identity helpers: they type the spec in the editor and do nothing at runtime. */
export const flow = (spec: Omit<FlowSpec, "kind">): FlowSpec => ({ kind: "flow", ...spec });
export const sequence = (spec: Omit<SequenceSpec, "kind">): SequenceSpec => ({ kind: "sequence", ...spec });
export const line = (spec: Omit<LineSpec, "kind">): LineSpec => ({ kind: "line", ...spec });
export const bar = (spec: Omit<BarSpec, "kind">): BarSpec => ({ kind: "bar", ...spec });

