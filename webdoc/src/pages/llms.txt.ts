// Root /llms.txt — sectioned index for AI agents.
//
// The members of each section are listed inline rather than behind a link to
// `/<section>/llms.txt`. The starter shape emitted seven bare directory slugs
// and nothing else: an agent that fetches one file, which is most of them, got
// 590 bytes of undescribed folder names and no way to tell whether the site
// answers its question. The per-section files are good, they were just one hop
// too deep. Inlining costs about 27 KB and is what the llmstxt.org convention
// actually describes: a single page an agent can read end to end.
//
// `/<section>/llms.txt` still ships from `pages/[section]/llms.txt.ts` for
// agents that want to drill into one area, and is linked from each heading.
import { getIndexedTopLevel } from "@cloudflare/nimbus-docs";
import { config } from "virtual:nimbus/config";
import { HOME_HEADLINE, HOME_SUMMARY } from "./index.md.ts";
// `buildBrief` is imported to measure the summary, not to inline it: the size
// advertised beside the link has to be the size of the file that will arrive.
// `sidebarLabels()` lives there too, so one place decides that the heading an
// agent reads is the heading a reader sees.
import { buildBrief, sidebarLabels } from "@/lib/llms-brief";

export const prerender = true;

/** Rounded for a human-legible advertisement, from the real byte length. */
function approxKb(text: string): number {
  return Math.round(new TextEncoder().encode(text).length / 1024);
}

export async function GET() {
  const { leaves, groups } = await getIndexedTopLevel();
  const abs = (path: string) => new URL(path, config.site).href;
  const labels = sidebarLabels();

  const brief = await buildBrief();

  const lines = [
    `# ${config.title}`,
    "",
    `> ${config.description ?? "Documentation index for AI agents."}`,
    "",
    // Order matters more than content here. This file used to open on the full
    // corpus, which is the one pointer an agent should almost never take: at
    // about 1.5 MB it is above the input ceiling of every fetch pipeline, and
    // they truncate it silently, so following the site's own first
    // recommendation yields a partial document that reads as a complete one.
    "## Read this first",
    "",
    `Complete summary of Queen MQ, self-contained, about ${approxKb(brief)} KB: ${abs("/llms-brief.txt")}`,
    "",
    "It carries the definition, installation, the data model, every benchmark figure with",
    "the conditions it was measured under, what competing systems do better, and the",
    "documented limits. If the question is about the product rather than about one",
    "specific page, that document answers it and this index is the fallback.",
    "",
    // No byte figure here on purpose. Stating one would mean either hardcoding
    // a number that drifts as pages are added, or collating the entire corpus at
    // build time to measure a string this route then throws away. The ratio is
    // the part an agent needs to act on, and it is true at any corpus size.
    `Every page collated into one document: ${abs("/llms-full.txt")}`,
    "",
    "Roughly fifty times the size of the summary above, and above the input ceiling of",
    "most fetch pipelines, which truncate it without reporting that they did. Take it",
    "only if you know your own limit is larger.",
    "",
    "## Pages",
    "",
    // The landing page is a hand-written `index.astro`, not a `docs` entry, so
    // `getIndexedTopLevel()` does not return it. Its markdown alternate comes
    // from `src/pages/index.md.ts`.
    `- [${config.title}](${abs("/index.md")}) — ${HOME_HEADLINE} ${HOME_SUMMARY}`,
  ];

  for (const leaf of leaves) {
    const description = leaf.description ? ` — ${leaf.description}` : "";
    lines.push(`- [${leaf.title}](${abs(leaf.markdownUrl)})${description}`);
  }

  // Sections in sidebar order where the sidebar names them, so the index reads
  // in the order the site teaches itself; anything the sidebar does not cover
  // follows alphabetically.
  const order = [...labels.keys()];
  const rank = (slug: string) => {
    const at = order.indexOf(slug);
    return at === -1 ? order.length : at;
  };

  const sections = groups
    // Older doc versions have their own /<v>/llms.txt; don't list them here.
    .filter((group) => group.kind !== "version")
    .sort((a, b) => rank(a.slug) - rank(b.slug) || a.slug.localeCompare(b.slug));

  for (const group of sections) {
    const label = labels.get(group.slug) ?? group.label;
    lines.push(
      "",
      `## ${label}`,
      "",
      `Section index: ${abs(`/${group.slug}/llms.txt`)}`,
      "",
    );
    for (const item of group.members) {
      const description = item.description ? ` — ${item.description}` : "";
      lines.push(`- [${item.title}](${abs(item.markdownUrl)})${description}`);
    }
  }

  lines.push("");

  return new Response(lines.join("\n"), {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}
