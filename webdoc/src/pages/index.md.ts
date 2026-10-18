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
  paradigm,
  shifts,
  step,
  stepSection,
  roles,
  removeSection,
  removals,
  includedSection,
  included,
  dashboardShot,
  proofSection,
  proof,
  provenSection,
  proven,
  trySection,
  start,
  closing,
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

  lines.push(`## ${paradigm.oldTitle}`, "");
  for (const t of paradigm.old) lines.push(t, "");
  lines.push(`## ${paradigm.newTitle}`, "");
  for (const x of shifts) lines.push(`**[${x.title}](${url(x.href)})** ${x.body}`, "");
  lines.push(figureMarkdown("concepts/four-systems"), "");

  lines.push(`## ${stepSection.title}`, "", stepSection.lead, "", "```js", step, "```", "");
  for (const r of roles) lines.push(`- **${r.role}**: ${r.primitive} ([${r.role.toLowerCase()}](${url(r.href)}))`);
  lines.push("- **One entry**: committed whole, or not at all", "", link(stepSection.link), "");

  lines.push(`## ${removeSection.title}`, "");
  for (const r of removals) lines.push(`- **${r.title}.** ${r.body}`);
  lines.push("");

  lines.push(`## ${includedSection.title}`, "");
  for (const c of included) lines.push(`- **[${c.title}](${url(c.href)})**: ${c.body}`);
  lines.push("", `**Screenshot.** ${dashboardShot.alt} ${dashboardShot.caption}`, "");

  lines.push(`## ${proofSection.title}`, "", proofSection.lead, "");
  for (const p of proof) {
    const more = p.href ? ` ([details](${url(p.href)}))` : "";
    lines.push(`- **${p.figure}** ${p.unit}: ${p.body}${more}`);
  }
  lines.push("", figureMarkdown("benchmarks/partitions-p99"), "", link(proofSection.link), "");

  lines.push(`## ${provenSection.title}`, "");
  for (const d of proven) lines.push(`- **[${d.title}](${d.href.startsWith("http") ? d.href : url(d.href)})**: ${d.body}`);
  lines.push("");

  lines.push(`## ${trySection.title}`, "", "```bash", hero.run, "```", "", trySection.pushNote, "");
  lines.push("```bash", trySection.push, "```", "", trySection.after, "", trySection.mcpNote, "");
  lines.push("```bash", trySection.mcp, "```", "");
  for (const s of start) lines.push(`- [${s.title}](${url(s.href)}): ${s.body}`);
  lines.push("", `**${closing}**`);

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
