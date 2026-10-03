/**
 * A figure as markdown, for the page's `.md` alternate and `llms-full.txt`.
 *
 * The picture cannot travel, so what travels is everything it was drawn from:
 * the `alt` and caption, then the data itself. A chart becomes a table of its
 * numbers, a diagram a list of its arrows, a sequence its numbered steps. An
 * agent reading the markdown gets the content of the figure, not the news that
 * there was one.
 */
import { fmt, getFigure, type FlowSpec, type LineSpec, type BarSpec, type SequenceSpec, type Point } from "@/lib/figures";

const one = (s: string) => s.replace(/\s*\n\s*/g, " ");

function flowMd(spec: FlowSpec): string[] {
  const name = new Map(spec.nodes.map((n) => [n.id, one(n.label)]));
  const out = spec.nodes.map((n) => `- ${one(n.label)}${n.sub ? `: ${one(n.sub)}` : ""}`);
  for (const g of spec.groups ?? []) out.push(`- Group "${g.label}": ${g.nodes.map((n) => name.get(n)).join(", ")}`);
  for (const e of spec.edges ?? []) {
    const arrow = e.both ? "↔" : e.plain ? "connects to" : "→";
    out.push(`- ${name.get(e.from)} ${arrow} ${name.get(e.to)}${e.label ? `: ${one(e.label)}` : ""}${e.dashed ? " (dashed)" : ""}`);
  }
  for (const n of spec.notes ?? []) out.push(`- Note: ${one(n.text)}`);
  return out;
}

function sequenceMd(spec: SequenceSpec): string[] {
  const name = new Map(spec.actors.map((a) => [a.id, a.label]));
  const out: string[] = [];
  let k = 0;
  for (const s of spec.steps) {
    if ("divider" in s) out.push(`\n*${s.divider}*\n`);
    else if ("note" in s) {
      const over = (Array.isArray(s.over) ? s.over : [s.over]).map((a) => name.get(a)).join(" and ");
      out.push(`   (${over}: ${one(s.note)})`);
    } else {
      k++;
      const to = s.from === s.to ? "itself" : name.get(s.to);
      out.push(`${k}. ${name.get(s.from)} → ${to}: ${s.label}${s.sub ? ` (${s.sub})` : ""}`);
    }
  }
  return out;
}

function lineMd(spec: LineSpec): string[] {
  const xs = [...new Set(spec.series.flatMap((s) => s.points.map((p) => (Array.isArray(p) ? p[0] : p.x))))].sort((a, b) => a - b);
  const cell = (p: Point | undefined) => {
    if (!p || p.y === null) return "";
    const v = fmt(p.y, spec.y.format ?? "si");
    return p.note ? `${v} (${p.note})` : p.hollow ? `${v} (see caption)` : v;
  };
  const head = `| ${spec.x.label ?? "x"} | ${spec.series.map((s) => s.label).join(" | ")} |`;
  const rule = `|${"---|".repeat(spec.series.length + 1)}`;
  // A long series (a timeline of a thousand windows) would bury the page under
  // its table: keep about 48 evenly spaced rows plus every annotated point.
  const step = xs.length > 60 ? Math.ceil(xs.length / 48) : 1;
  const annotated = new Set(
    spec.series.flatMap((s) => s.points.filter((p) => !Array.isArray(p) && (p.note || p.hollow)).map((p) => (p as Point).x)),
  );
  const shown = xs.filter((x, i) => i % step === 0 || i === xs.length - 1 || annotated.has(x));
  const rows = shown.map((x) => {
    const cells = spec.series.map((s) => {
      const raw = s.points.find((p) => (Array.isArray(p) ? p[0] : p.x) === x);
      const p = raw === undefined ? undefined : Array.isArray(raw) ? { x: raw[0], y: raw[1] } : raw;
      return cell(p);
    });
    return `| ${fmt(x, spec.x.format ?? "si")} | ${cells.join(" | ")} |`;
  });
  const sampled = step > 1 ? ` Every ${step}th of ${xs.length} points.` : "";
  const out = [`${spec.y.label ? `Values: ${spec.y.label}.` : ""}${sampled}`.trim(), "", head, rule, ...rows];
  for (const m of spec.markers ?? []) out.push("", `Marker at ${fmt(m.x, spec.x.format ?? "si")}: ${m.label}`);
  for (const b of spec.bands ?? []) if (b.label) out.push("", `Band ${fmt(b.from, spec.x.format ?? "si")} to ${fmt(b.to, spec.x.format ?? "si")}: ${b.label}`);
  for (const h of spec.hlines ?? []) out.push("", `Reference line at ${fmt(h.y, spec.y.format ?? "si")}: ${h.label}`);
  return out;
}

function barMd(spec: BarSpec): string[] {
  const head = `| | ${spec.series.map((s) => s.label).join(" | ")} |`;
  const rule = `|${"---|".repeat(spec.series.length + 1)}`;
  const rows = spec.categories.map((c, k) => {
    const cells = spec.series.map((s) => {
      const v = s.values[k];
      if (v === null) return spec.missing ?? "not run";
      const note = s.notes?.[k];
      return note ? `${fmt(v, spec.x.format ?? "si")} (${note})` : fmt(v, spec.x.format ?? "si");
    });
    return `| ${c} | ${cells.join(" | ")} |`;
  });
  return [`${spec.x.label ? `Values: ${spec.x.label}.` : ""}`, "", head, rule, ...rows];
}

export function figureMarkdown(id: string): string {
  const spec = getFigure(id);
  const parts = [`**Figure.** ${spec.alt.trim()}`];
  if (spec.caption) parts.push(spec.caption.trim());
  const body =
    spec.kind === "flow"
      ? flowMd(spec)
      : spec.kind === "sequence"
        ? sequenceMd(spec)
        : spec.kind === "line"
          ? lineMd(spec)
          : barMd(spec);
  parts.push(body.join("\n").trim());
  if (spec.source) parts.push(`Source: \`${spec.source}\`.`);
  return parts.join("\n\n");
}
