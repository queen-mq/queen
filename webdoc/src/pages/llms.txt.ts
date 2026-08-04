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

export const prerender = true;

/**
 * `getIndexedTopLevel()` labels a group with its own slug (`start`,
 * `full-examples`). The sidebar in `astro.config.ts` already carries a written
 * label for each of those directories ("Start here", "Full examples"), so use
 * it: the heading an agent reads should be the heading a reader sees.
 */
interface SidebarGroupish {
  label?: unknown;
  autogenerate?: { directory?: unknown };
}

function sidebarLabels(): Map<string, string> {
  const labels = new Map<string, string>();
  const items = (config as { sidebar?: { items?: unknown } }).sidebar?.items;
  if (!Array.isArray(items)) return labels;
  for (const raw of items) {
    const item = raw as SidebarGroupish;
    const directory = item.autogenerate?.directory;
    if (typeof directory !== "string" || typeof item.label !== "string") continue;
    labels.set(directory.replace(/^\/+|\/+$/g, ""), item.label);
  }
  return labels;
}

export async function GET() {
  const { leaves, groups } = await getIndexedTopLevel();
  const abs = (path: string) => new URL(path, config.site).href;
  const labels = sidebarLabels();

  const lines = [
    `# ${config.title}`,
    "",
    `> ${config.description ?? "Documentation index for AI agents."}`,
    "",
    `Full corpus (all pages, one document): ${abs("/llms-full.txt")}`,
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
