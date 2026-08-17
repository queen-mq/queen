/**
 * Per-page `/<slug>/index.md` — the clean-markdown alternate for every
 * indexable entry of the primary `docs` collection.
 *
 * Non-primary collections (`api`, `blog`, …) mount under their own
 * URL namespace by convention; their `.md` alternates live at the
 * sibling route `pages/<collection>/[...slug]/index.md.ts`. This route
 * filters to the primary collection so multi-collection sites don't
 * generate conflicting `[...slug]` paths at root.
 */

import { getIndexedEntries, type IndexedEntry } from "@cloudflare/nimbus-docs";
import { config } from "virtual:nimbus/config";
// `renderEntryAsMarkdown()` from the framework deletes every `<Render />`
// instead of inlining the partial. This wrapper inlines it first; everything
// else about the downleveling is still the framework's.
import { renderEntryMarkdown } from "@/lib/markdown-partials";

export const prerender = true;

const PRIMARY_COLLECTION = "docs";

interface SlugProps {
  item: IndexedEntry;
}

export async function getStaticPaths() {
  const indexed = await getIndexedEntries();
  return indexed
    .filter((item) => item.collection === PRIMARY_COLLECTION)
    .map((item) => ({
      // Root index (`entry.id === "index"`) emits at `/index.md`; Astro's
      // rest-segment treats `undefined` as "no segment" so the URL is
      // `/index.md` rather than `/index/index.md`. Every other entry emits
      // at `/<entry.id>/index.md` — the convention `<page>/index.md`.
      params: {
        slug: item.entry.id === "index" ? undefined : item.entry.id,
      },
      props: { item } as SlugProps,
    }));
}

export async function GET({ props }: { props: SlugProps }) {
  const { item } = props;
  const { entry, title, description, markdownUrl, sourceUrl, version } = item;
  const data = (entry.data ?? {}) as Record<string, unknown>;
  const rawImage = data.socialImage;
  const socialImage =
    typeof rawImage === "string" && rawImage.length > 0
      ? rawImage
      : config.socialImage;

  const markdown = await renderEntryMarkdown(entry);

  const body = [
    "---",
    `title: ${JSON.stringify(title)}`,
    ...(description ? [`description: ${JSON.stringify(description)}`] : []),
    ...(socialImage
      ? [`image: ${JSON.stringify(new URL(socialImage, config.site).href)}`]
      : []),
    ...(version ? [`version: ${JSON.stringify(version)}`] : []),
    "---",
    "",
    // Leads with the whole-product summary, not with the index. An agent that
    // reached a `.md` has one page of a 127-page site and, most of the time, a
    // question about the product rather than about this page. A directory is
    // the wrong answer to that; `/llms-brief.txt` is the right one, and it is
    // small enough to fetch without a second thought.
    "> Queen MQ documentation, for AI agents",
    `> Complete self-contained summary of Queen MQ: ${new URL("/llms-brief.txt", config.site).href}`,
    "> Fetch that first when the question is about the product rather than about this page.",
    `> Index of all pages: ${new URL("/llms.txt", config.site).href}`,
    "",
    `# ${title}`,
    "",
    markdown,
    "",
    // Point at the authored source (`.mdx` twin) when it exists — the
    // `.md` alternate referencing itself was a placeholder.
    `Source: ${new URL(sourceUrl ?? markdownUrl, config.site).href}`,
    "",
  ].join("\n");

  return new Response(body, {
    headers: { "Content-Type": "text/markdown; charset=utf-8" },
  });
}
