/**
 * `/index.md` — the markdown alternate for the landing page.
 *
 * Every docs page gets one of these from `pages/[...slug]/index.md.ts`. The
 * landing page is a hand-written `index.astro`, not an entry of the `docs`
 * collection, so it gets this route instead, and `llms-full.txt.ts` prepends
 * the same block so the corpus opens with what the product is.
 *
 * The words come from `src/lib/home.ts`, the module the page itself renders,
 * so the two cannot disagree. The figures on the page come through as their
 * markdown (alt, caption and data), from the same specs. The
 * `scripts/check-markdown.mjs` probe still reads the built page and fails when
 * an element marked `data-md` is missing here.
 */

import { config } from "virtual:nimbus/config";
import { figureMarkdown } from "@/lib/figure-markdown";
import {
  HOME_SUMMARY as SUMMARY,
  hero,
  step,
  stepSection,
  roles,
  partitionSection,
  primitives,
  doorsSection,
  doors,
  dashboardSection,
  proofSection,
  proof,
  runSection,
  differentiators,
  limits,
  start,
} from "@/lib/home";

export const prerender = true;

const url = (path: string) => (config.site ? new URL(path, config.site).href : path);

/** The page's `<h1>`, verbatim. */
export const HOME_HEADLINE = `${hero.headline} ${hero.subline}`;

/** One line describing the page, for the index and corpus rows that list it. */
export const HOME_SUMMARY = SUMMARY;

/**
 * Everything under the `<h1>`. The heading itself is left to the caller so
 * this block can be dropped into `llms-full.txt`, whose own heading levels
 * differ.
 */
export function homepageBody(): string {
  const lines: string[] = [];
  const link = (l: { label: string; href: string }) => `[${l.label}](${url(l.href)})`;

  lines.push(`*${hero.eyebrow}*`, "", hero.lead, "", hero.second, "");
  lines.push(
    `${link(hero.primary)} · ${link(hero.secondary)} · [GitHub](https://github.com/queen-mq/queen)`,
    "",
  );
  lines.push("```bash", hero.run, "```", "", hero.runNote, "");

  lines.push(`## ${stepSection.title}`, "", stepSection.lead, "", "```js", step, "```", "");
  for (const r of roles) lines.push(`- **${r.role}**: ${r.primitive} ([${r.role.toLowerCase()}](${url(r.href)}))`);
  lines.push("- **One entry**: committed whole, or not at all", "");
  lines.push(figureMarkdown("concepts/four-systems"), "");
  lines.push(stepSection.after, "", link(stepSection.link), "");

  lines.push(`## ${partitionSection.title}`, "", partitionSection.lead, "", partitionSection.after, "");
  lines.push(figureMarkdown("benchmarks/partitions-p99"), "", link(partitionSection.link), "");

  lines.push("## The pieces a step needs, sharing one commit", "");
  for (const p of primitives) lines.push(`- **[${p.title}](${url(p.href)})**: ${p.body}`);
  lines.push("");

  lines.push(`## ${doorsSection.title}`, "", doorsSection.lead, "");
  for (const d of doors) lines.push(`- **[${d.title}](${url(d.href)})**: ${d.body}`);
  lines.push("", `${doorsSection.snippetTitle}:`, "", "```bash", doorsSection.snippet, "```", "");

  lines.push(`## ${dashboardSection.title}`, "", dashboardSection.lead, "");
  lines.push(`**Screenshot.** ${dashboardSection.alt}`, "", link(dashboardSection.link), "");

  lines.push(`## ${proofSection.title}`, "", proofSection.lead, "");
  for (const p of proof) lines.push(`- **${p.figure}** ${p.unit}: ${p.body} ([details](${url(p.href)}))`);
  lines.push("", link(proofSection.link), "");

  lines.push(`## ${runSection.title}`, "");
  for (const d of differentiators) lines.push(`- **[${d.title}](${url(d.href)})**: ${d.body}`);
  lines.push("");

  lines.push("## Limits", "", `${limits} [The full list](${url("/reference/limits/")}).`, "");

  lines.push("## Start here", "");
  for (const s of start) lines.push(`- [${s.title}](${url(s.href)}): ${s.body}`);

  return lines.join("\n").trim();
}

export async function GET() {
  const body = [
    "---",
    `title: ${JSON.stringify(config.title)}`,
    ...(config.description ? [`description: ${JSON.stringify(config.description)}`] : []),
    ...(config.socialImage ? [`image: ${JSON.stringify(url(config.socialImage))}`] : []),
    "---",
    "",
    // Same order as the per-page alternates: summary, then index. See
    // `pages/[...slug]/index.md.ts`.
    "> Queen MQ documentation, for AI agents",
    `> Complete self-contained summary of Queen MQ: ${url("/llms-brief.txt")}`,
    "> Fetch that first when the question is about the product rather than about this page.",
    `> Index of all pages: ${url("/llms.txt")}`,
    "",
    `# ${HOME_HEADLINE}`,
    "",
    homepageBody(),
    "",
    `Source: ${url("/")}`,
    "",
  ].join("\n");

  return new Response(body, {
    headers: { "Content-Type": "text/markdown; charset=utf-8" },
  });
}
