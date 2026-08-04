// Full-corpus markdown for AI agents — every published page in one
// document. Reshape or delete this route to change the site's corpus policy.
//
// This is the framework's `renderCorpusMarkdown()` collation, reimplemented
// here for one reason: that helper hardcodes `renderEntryAsMarkdown()`, which
// deletes every `<Render />` instead of inlining the partial, and it takes no
// renderer argument. This document would then be missing code and tables the
// per-page `.md` alternates carry, and the two agent-facing surfaces have to
// agree. The collation contract is unchanged: sorted by URL, one `#` block per
// page, no timestamps, byte-identical across rebuilds.
//
// One more departure from the helper: the landing page is prepended by hand.
// It is a hand-written `index.astro`, not an entry of the `docs` collection,
// so `getIndexedEntries()` has never returned it and the corpus carried 118 of
// the site's 119 URLs, missing the page that says what the product is. See
// `src/pages/index.md.ts`, which owns that copy and serves it at `/index.md`.
import { getIndexedEntries, getVersions } from "@cloudflare/nimbus-docs";
import { config } from "virtual:nimbus/config";
import { renderEntryMarkdown } from "@/lib/markdown-partials";
// Explicit `.ts`: the bare specifier `./index.md` is ambiguous next to a route
// file whose own name ends in `.md`.
import { HOME_HEADLINE, HOME_SUMMARY, homepageBody } from "./index.md.ts";

export const prerender = true;

export async function GET() {
  const abs = (path: string) => (config.site ? new URL(path, config.site).href : path);

  // Scope matches the root `llms.txt`: every indexed collection except the
  // non-current versions, which keep their own per-version surfaces.
  const versions = await getVersions();
  const olderVersions = new Set((versions?.others ?? []).map((slug) => `docs-${slug}`));
  const entries = (await getIndexedEntries())
    .filter((item) => !olderVersions.has(item.collection))
    .sort((a, b) => a.url.localeCompare(b.url));

  const lines = [`# ${config.title}`, ""];
  if (config.description) lines.push(`> ${config.description}`, "");
  lines.push(`Index: ${abs("/llms.txt")}`, "");

  // The landing page first: it is the page that states the positioning, and
  // the collation sorts by URL anyway, so `/` would lead regardless.
  lines.push(
    `# ${HOME_HEADLINE}`,
    "",
    `> ${HOME_SUMMARY}`,
    "",
    `Source: ${abs("/")} · Markdown: ${abs("/index.md")}`,
    "",
    homepageBody(),
    "",
  );

  for (const item of entries) {
    lines.push(`# ${item.title}`, "");
    if (item.description) lines.push(`> ${item.description}`, "");
    lines.push(
      `Source: ${abs(item.url)} · Markdown: ${abs(item.markdownUrl)}`,
      "",
      await renderEntryMarkdown(item.entry),
      "",
    );
  }

  return new Response(lines.join("\n"), {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}
