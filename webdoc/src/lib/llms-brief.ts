/**
 * `/llms-brief.txt` — the whole product in one document an agent can fetch
 * without thinking about it.
 *
 * ## Why this exists
 *
 * The site already had two agent surfaces and neither answers the question
 * agents actually arrive with. `/llms.txt` is a 27 KB directory of 127 entries:
 * it tells an agent where things are, which costs a second round-trip it often
 * will not spend. `/llms-full.txt` is the entire corpus, and at roughly 1.5 MB
 * it is above every fetch pipeline's ceiling, so an agent that follows it reads
 * a truncated head and is told nothing about the truncation.
 *
 * Between a directory and a corpus there was nothing. This is that: one
 * self-sufficient document, small enough to always arrive whole.
 *
 * ## Where the content comes from
 *
 * Not from here. A summary written by hand beside the pages it summarises
 * drifts from them, and this site has already been audited for factual drift
 * once. So every claim below is authored in the page that owns it, as a
 * `digest` frontmatter string (see `src/content.config.ts`), and this module
 * only collates. A page earns a digest by stating something the product cannot
 * be understood without, which is a small minority of the 127.
 *
 * The consequence worth stating: adding a fact to the brief means writing it on
 * the page that proves it. There is no way to put a number in this document
 * that no page stands behind.
 *
 * ## The one editorial rule
 *
 * A figure travels with its conditions, in the same sentence. `1,000,000 msg/s`
 * is not a fact about Queen MQ; `1,000,000 msg/s per side for 24 hours with
 * 256-byte payloads on 32 vCPU / 62 GiB, explicit acks, deduplication on` is.
 * The reason is measured rather than aesthetic: a fetch pipeline asked for the
 * throughput of this product returned the figure and reported the conditions as
 * "not stated", because they were one page away.
 */

import { getIndexedEntries } from "@cloudflare/nimbus-docs";
import { config } from "virtual:nimbus/config";

// The size budget that keeps this document fetchable is not declared here. It
// belongs to its enforcer, `scripts/check-brief.mjs`, so there is one number and
// one place to argue with it.

interface SidebarGroupish {
  label?: unknown;
  autogenerate?: { directory?: unknown };
  items?: unknown;
}

/**
 * Written section labels from the sidebar, keyed by directory slug, in the order
 * the sidebar declares them.
 *
 * Shared with `pages/llms.txt.ts`: the heading an agent reads should be the
 * heading a reader sees, and there should be one place that decides which is
 * which.
 *
 * The walk recurses because two of this site's six content directories are not
 * declared at the top level: `astro.config.ts` nests Reference and Internal
 * inside one "Documentation" group, deliberately, so they do not compete with
 * the guides in the rail. A single-level scan misses exactly those two and falls
 * back to the raw slug, which is how both agent surfaces came to print a
 * lowercase `## reference` next to `## Start Here`. Recursing also keeps the
 * declaration order, so a nested section still sorts where its parent sits.
 */
export function sidebarLabels(): Map<string, string> {
  const labels = new Map<string, string>();

  const walk = (items: unknown): void => {
    if (!Array.isArray(items)) return;
    for (const raw of items) {
      const item = raw as SidebarGroupish;
      const directory = item.autogenerate?.directory;
      if (typeof directory === "string" && typeof item.label === "string") {
        labels.set(directory.replace(/^\/+|\/+$/g, ""), item.label);
      }
      // A group can both name a directory and hold children; walk regardless.
      walk(item.items);
    }
  };

  walk((config as { sidebar?: { items?: unknown } }).sidebar?.items);
  return labels;
}

/**
 * Top-level section slug for an entry id, or null for a page that belongs to no
 * section.
 *
 * Derived from the id rather than asked of `getIndexedTopLevel()`, because that
 * helper's group members expose the fields `llms.txt` needs (title,
 * description, markdownUrl) and not the raw frontmatter this module reads.
 *
 * The case worth spelling out is the one that silently dropped three pages when
 * this was a plain `split("/")`: a section's own overview page is
 * `start/index.mdx`, whose entry id is `start`, with no slash in it. That is not
 * a root-level page, it is the section's front door, and it is usually the page
 * carrying the section's most quotable fact. An id with no slash is therefore a
 * section overview when the sidebar declares a directory by that name, and a
 * genuine root-level page otherwise.
 */
function sectionOf(entryId: string, known: Set<string>): string | null {
  const slash = entryId.indexOf("/");
  if (slash !== -1) return entryId.slice(0, slash);
  return known.has(entryId) ? entryId : null;
}

interface DigestItem {
  title: string;
  url: string;
  digest: string;
  /** `sidebar.order` from frontmatter, for ordering inside a section. */
  order: number;
  /** Entry id, the tiebreak that keeps the collation deterministic. */
  id: string;
}

/**
 * `getIndexedEntries()` does not sort, and its order is the content glob's.
 * Pages inside a section are therefore ordered here, by the same
 * `sidebar.order` a reader navigates by, so the summary teaches the product in
 * the order the site does. A page with no declared order sorts last, by id.
 */
function sidebarOrder(data: Record<string, unknown>): number {
  const sidebar = data.sidebar;
  if (sidebar && typeof sidebar === "object") {
    const order = (sidebar as { order?: unknown }).order;
    if (typeof order === "number") return order;
  }
  return Number.MAX_SAFE_INTEGER;
}

/**
 * Collate the brief. Deterministic: sections in sidebar order, pages inside a
 * section in the order the sidebar teaches them, no timestamps. The document is
 * byte-identical across rebuilds, so a diff of it is a diff of the docs.
 */
export async function buildBrief(): Promise<string> {
  const abs = (path: string) => (config.site ? new URL(path, config.site).href : path);
  const labels = sidebarLabels();
  const order = [...labels.keys()];
  // The sidebar's declared directories are the authority on what is a section,
  // which is what lets a slashless entry id be recognised as a section overview
  // rather than mistaken for a root-level page.
  const knownSections = new Set(order);

  const versionScoped = (collection: string) => collection !== "docs";
  const entries = (await getIndexedEntries()).filter(
    (item) => !versionScoped(item.collection),
  );

  // Sections only. The landing page is `pages/index.astro`, not a `docs` entry,
  // so there is no root-level page to collate and no branch here for one: the
  // product's one-line identity comes from `config.description` in the header
  // below, and the fuller definition is the first section's first digest.
  const bySection = new Map<string, DigestItem[]>();

  for (const item of entries) {
    const data = (item.entry.data ?? {}) as Record<string, unknown>;
    const digest = typeof data.digest === "string" ? data.digest.trim() : "";
    if (!digest) continue;

    const record: DigestItem = {
      title: item.title,
      url: abs(item.url),
      digest,
      order: sidebarOrder(data),
      id: item.entry.id,
    };
    const section = sectionOf(item.entry.id, knownSections);
    if (section === null) continue;
    const bucket = bySection.get(section);
    if (bucket) bucket.push(record);
    else bySection.set(section, [record]);
  }

  for (const bucket of bySection.values()) {
    bucket.sort((a, b) => a.order - b.order || a.id.localeCompare(b.id));
  }

  const rank = (slug: string) => {
    const at = order.indexOf(slug);
    return at === -1 ? order.length : at;
  };
  const sections = [...bySection.keys()].sort(
    (a, b) => rank(a) - rank(b) || a.localeCompare(b),
  );

  const lines: string[] = [
    `# ${config.title}, complete summary`,
    "",
    `> ${config.description ?? ""}`,
    "",
    "This is the whole product in one document, for an agent that has a question",
    "about Queen MQ rather than about one page of its documentation. Every figure",
    "carries the conditions it was measured under, in the same sentence, because a",
    "figure without them is not a fact about this system.",
    "",
    `Canonical site: ${abs("/")}`,
    `Index of every documentation page: ${abs("/llms.txt")}`,
    // No byte figure, for the same reason `pages/llms.txt.ts` states none: it
    // would either drift as pages are added or cost a full corpus collation to
    // measure. The ratio is the actionable part and stays true at any size.
    `Every page collated into one document: ${abs("/llms-full.txt")}`,
    "",
    "That file is roughly fifty times the size of this summary, above the input ceiling of",
    "most fetch pipelines, which truncate it without reporting that they did. Prefer this",
    "summary, or the per-section indexes linked below.",
    "",
  ];

  for (const slug of sections) {
    const label = labels.get(slug) ?? slug;
    lines.push(`## ${label}`, "", `Section index: ${abs(`/${slug}/llms.txt`)}`, "");
    for (const item of bySection.get(slug) ?? []) {
      lines.push(`### ${item.title}`, "", item.digest, "", `Source: ${item.url}`, "");
    }
  }

  lines.push(
    "## What this summary does not cover",
    "",
    "Per-language SDK reference, the HTTP route reference, the SQL schema, the",
    "internals of the storage and maintenance engines, and the full configuration",
    "reference are not summarised here. Reach them from the section indexes above",
    `or from ${abs("/llms.txt")}.`,
    "",
  );

  return `${lines.join("\n").trimEnd()}\n`;
}
