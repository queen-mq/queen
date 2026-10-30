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
  guideSection,
  guideRun,
  guideSteps,
  guideClosing,
  dashboardShot,
  proofSection,
  proof,
  provenSection,
  proven,
  nextSection,
  start,
  book,
  closing,
} from "@/lib/home";
import { GUIDE_LANGS, guideCode } from "@/lib/home-guide";

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

  // The guide: every step in every language, since a reader of the markdown has
  // no language picker. Each language's code is the program that ran it.
  lines.push(`## ${guideSection.title}`, "");
  for (const t of guideSection.lead) lines.push(t, "");
  lines.push(`### 1. ${guideRun.title}`, "");
  for (const t of guideRun.body) lines.push(t, "");
  lines.push("```bash", hero.run, "```", "", guideRun.installNote, "");
  for (const l of GUIDE_LANGS) lines.push(`${l.label}:`, "", "```bash", l.install, "```", "");
  lines.push(`**Screenshot.** ${dashboardShot.alt} ${dashboardShot.caption}`, "");
  guideSteps.forEach((step, n) => {
    lines.push(`### ${n + 2}. ${step.title}`, "");
    for (const t of step.body) lines.push(t, "");
    step.code.forEach((c, ci) => {
      for (const l of GUIDE_LANGS) {
        const code = guideCode(l, c.id);
        if (code === null) lines.push(`${l.label}: ${l.missing?.[c.id] ?? ""}`, "");
        else lines.push(`${c.caption}, ${l.label}:`, "", "```" + l.lang, code, "```", "");
      }
      const after = step.between?.[ci];
      if (after) lines.push(after, "");
    });
    lines.push(step.links.map((k) => `[${k.label}](${url(k.href)})`).join(" · "), "");
  });
  lines.push(guideClosing, "");
  lines.push(
    `${guideSection.programLabel}: ` +
      GUIDE_LANGS.map((l) => `[${l.label}](https://github.com/queen-mq/queen/blob/master/${l.file})`).join(", "),
    "",
  );

  lines.push(`## ${proofSection.title}`, "", proofSection.lead, "");
  for (const p of proof) {
    const more = p.href ? ` ([details](${url(p.href)}))` : "";
    lines.push(`- **${p.figure}** ${p.unit}: ${p.body}${more}`);
  }
  lines.push("", figureMarkdown("benchmarks/partitions-p99"), "", link(proofSection.link), "");

  lines.push(`## ${provenSection.title}`, "");
  for (const d of proven) lines.push(`- **[${d.title}](${d.href.startsWith("http") ? d.href : url(d.href)})**: ${d.body}`);
  lines.push("");

  lines.push(`## ${nextSection.title}`, "");
  lines.push(`**[${book.title}](${url(book.href)})**: ${book.body}`, "");
  for (const x of start) lines.push(`- [${x.title}](${url(x.href)}): ${x.body}`);
  lines.push("", nextSection.mcpNote, "", "```bash", nextSection.mcp, "```");
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
