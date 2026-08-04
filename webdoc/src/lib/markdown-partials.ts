/**
 * `<Render file="…" />` expansion and `<Chart />` rendering for the markdown
 * alternates.
 *
 * The HTML pages resolve partials through `src/components/Render.astro`, which
 * asks `astro:content` for the entry and renders it in place. The markdown
 * alternates (`/<slug>/index.md`, `/llms-full.txt`) do not go through Astro's
 * renderer at all: `renderEntryAsMarkdown()` downlevels the raw MDX body with
 * regexes, and its last two rules delete every unknown component — paired ones
 * keep their children, self-closing ones like `<Render />` leave nothing behind.
 * Every partial therefore vanished from the corpus agents and crawlers read,
 * which is most of the code on this site.
 *
 * This module inlines the partial's own markdown at that point, so the two
 * surfaces carry the same content. It runs *before* `renderEntryAsMarkdown()`
 * (on the raw MDX), which is what makes nesting work: an expanded partial is
 * just more MDX in the page body, and the downleveler sees one document.
 *
 * Three things it is careful about:
 *
 *   - **A missing partial is a build error.** Same failure mode as
 *     `Render.astro`, with the same "did you mean" hint, rather than a page
 *     that silently loses a section.
 *   - **Code fences come through byte for byte.** Fences — the page's own and
 *     the partials' — are lifted out into placeholders before the downleveler
 *     runs and put back afterwards, dedented by their own opening indentation.
 *     Left in place they would be mangled: `protectCode()` in `nimbus-docs`
 *     strips four leading spaces from every line of a fenced block (a heuristic
 *     for fences indented inside JSX), which silently breaks the indentation of
 *     any real program — fatally so for Python. Lifting the page's fences out
 *     first also means a `<Render />` shown *inside* a code sample stays a code
 *     sample: the contributing guide documents the syntax that way.
 *   - **A partial cycle fails instead of hanging.** Expansion carries the chain
 *     of ancestors; a partial that reaches itself throws with the path. The same
 *     partial included twice in one page is not a cycle and stays allowed.
 *
 * ## `<Chart />`
 *
 * Same bug class, different component. A benchmark figure is two generated
 * SVGs, and its `alt` is written for a reader who cannot see them: on
 * `/benchmarks/peak/` that is "a ramp of about half a minute, then a band
 * around one million per second held for the rest of the run". The
 * downleveler's self-closing rule deleted the tag, so the four benchmark pages
 * published their results section with the figure, its description and its
 * provenance all missing. An agent reading the markdown is exactly the reader
 * the `alt` was written for.
 *
 * This one goes through `renderEntryAsMarkdown()`'s own `componentMap` hook
 * rather than a pre-pass: `<Chart />` has no children and no nesting, so there
 * is nothing for the expansion machinery above to do, and a `componentMap`
 * entry runs before the default rules and takes the tag off the table.
 *
 * Any *other* capitalised self-closing component added to the content will be
 * deleted the same way, silently. `scripts/check-markdown.mjs` enumerates them
 * and fails the build on one that is neither handled here nor allow-listed
 * there, so the next occurrence is a build error rather than a hollow page.
 */

import { getCollection } from "astro:content";
import { renderEntryAsMarkdown } from "@cloudflare/nimbus-docs";

/** The shape this module needs from a content entry — docs page or partial. */
interface MarkdownEntry {
  id?: string;
  body?: string;
  data?: Record<string, unknown>;
}

/** A `<Render />` tag on a line of its own, indented or not. */
const RENDER_TAG = /^([ \t]*)<Render\b([^>]*?)(?:\/>|>[\s\S]*?<\/Render>)[ \t]*$/gm;

/** Any `<Render` left after expansion — used for the leftover check. */
const RENDER_MENTION = /<Render\b/;

/** A fenced code block, with its opening indentation captured. */
const FENCE = /^([ \t]*)(`{3,}|~{3,})[^\n]*\n[\s\S]*?^[ \t]*\2[ \t]*$/gm;

/** MDX expression comments — the generated-file banner every partial carries. */
const MDX_COMMENT = /\{\s*\/\*[\s\S]*?\*\/\s*\}/g;

const FENCE_TOKEN = (index: number) => `@@QUEEN_PARTIAL_FENCE_${index}@@`;
const FENCE_TOKEN_LINE = /^[ \t]*@@QUEEN_PARTIAL_FENCE_(\d+)@@[ \t]*$/gm;
const FENCE_TOKEN_ANY = /@@QUEEN_PARTIAL_FENCE_(\d+)@@/g;

let partialsCache: Promise<Map<string, MarkdownEntry>> | null = null;

/**
 * Every partial keyed by the id `<Render file="…" />` uses. Read through
 * `getCollection("partials")` so ids are resolved exactly the way
 * `Render.astro`'s `getEntry("partials", file)` resolves them — including
 * Astro's directory-index rule, which makes `snippets/index.mdx` the entry
 * `snippets`.
 */
function loadPartials(): Promise<Map<string, MarkdownEntry>> {
  partialsCache ??= getCollection("partials").then(
    (entries) => new Map(entries.map((entry) => [entry.id, entry as MarkdownEntry])),
  );
  return partialsCache;
}

/**
 * Render a docs entry as markdown with every `<Render />` inlined.
 *
 * Drop-in replacement for `renderEntryAsMarkdown(entry)` in the `.md` and
 * `llms-full.txt` routes.
 */
export async function renderEntryMarkdown(entry: MarkdownEntry): Promise<string> {
  const partials = await loadPartials();
  const fences: string[] = [];
  const where = entry.id ?? "(unknown page)";

  // The page's own fences come out first, so a `<Render />` inside a code
  // sample is never expanded and no fence is touched by the downleveler.
  const protectedBody = protectFences(entry.body ?? "", fences);
  const expanded = expand(protectedBody, { partials, fences, stack: [], where });
  assertNoUnexpandedRender(expanded, where);

  const markdown = renderEntryAsMarkdown({ ...entry, body: expanded }, { componentMap });
  return restoreFences(markdown, fences);
}

/**
 * Components this module renders itself, instead of letting the downleveler
 * delete them. Keep the key set in step with `HANDLED` in
 * `scripts/check-markdown.mjs`, which is what fails the build when the content
 * grows a component neither side knows about.
 */
const componentMap = {
  /**
   * A figure becomes its description. The image itself cannot travel into
   * markdown usefully (two theme-specific SVGs, no meaning without a
   * renderer), so what the markdown carries is the text that stands in for it:
   * the `alt`, the caption, and the artifact the figure was rendered from.
   */
  Chart: ({ attrs }: { attrs: Record<string, string | boolean> }): string => {
    const str = (key: string): string => {
      const value = attrs[key];
      return typeof value === "string" ? value.trim() : "";
    };
    const alt = str("alt");
    const caption = str("caption");
    const source = str("source");
    const src = str("src");

    const parts: string[] = [];
    if (alt) parts.push(`**Figure.** ${alt}`);
    else if (src) parts.push(`**Figure.** \`${src}\``);
    if (caption) parts.push(caption);
    if (source) parts.push(`Rendered from \`${source}\`.`);
    return parts.length > 0 ? `\n${parts.join("\n\n")}\n` : "";
  },

  /**
   * The accordion family, unwrapped to its text.
   *
   * The downleveler knows the other paired components but not these four, so
   * before this rule they reached the markdown as literal `<Accordion>` and
   * `<AccordionTrigger>` tags: the one page that uses them published its
   * headings as raw JSX to every agent reading the `.md` twin. A disclosure
   * widget has no markdown equivalent, and it does not need one. What has to
   * survive is the trigger, which is the section's label, and the content,
   * which is the section. The wrappers carry nothing and collapse to their
   * children.
   */
  AccordionGroup: ({ children }: { children: string }): string => children,
  Accordion: ({ children }: { children: string }): string => children,
  AccordionTrigger: ({ children }: { children: string }): string => `\n**${children.trim()}**\n`,
  AccordionContent: ({ children }: { children: string }): string => children,
};

interface Context {
  partials: Map<string, MarkdownEntry>;
  /** Code fences lifted out of partials, restored after downleveling. */
  fences: string[];
  /** Partial ids currently being expanded, outermost first. */
  stack: string[];
  /** Page id, for error messages. */
  where: string;
}

/** Replace every own-line `<Render />` in `text` with the partial's markdown. */
function expand(text: string, ctx: Context): string {
  return text.replace(RENDER_TAG, (_match, _indent: string, rawAttrs: string) => {
    const file = attrString(rawAttrs, "file");
    if (file === undefined) {
      throw new Error(
        `[markdown-partials] <Render> on "${ctx.where}" has no literal file="…" attribute: ` +
          `<Render${rawAttrs}/>. The markdown alternates resolve partials statically, ` +
          `so the file name has to be a plain string.`,
      );
    }

    const partial = ctx.partials.get(file);
    if (!partial) {
      const ids = [...ctx.partials.keys()].sort();
      const hint = closest(file, ids);
      const shortList = ids.slice(0, 10).join(", ") || "none";
      const tail = ids.length > 10 ? ` (and ${ids.length - 10} more)` : "";
      throw new Error(
        `[markdown-partials] Partial "${file}" not found, included on "${ctx.where}".` +
          (hint ? ` Did you mean "${hint}"?` : "") +
          ` Available: ${shortList}${tail}`,
      );
    }

    if (ctx.stack.includes(file)) {
      throw new Error(
        `[markdown-partials] Partial cycle on "${ctx.where}": ` +
          `${[...ctx.stack, file].join(" → ")}. A partial cannot include itself.`,
      );
    }

    const params = attrParams(rawAttrs);
    assertParams(partial, file, params, ctx.where);

    let body = stripFrontmatter(partial.body ?? "");
    body = protectFences(body, ctx.fences);
    body = body.replace(MDX_COMMENT, "");
    body = substituteParams(body, params);

    ctx.stack.push(file);
    body = expand(body, ctx);
    ctx.stack.pop();

    // Inlined at column zero and surrounded by blank lines: a partial is a
    // block, and the downleveler flattens JSX nesting anyway (its own cleanup
    // pass dedents fences to column zero). Trailing blank lines collapse in
    // `renderEntryAsMarkdown`'s final pass.
    return `\n${body.trim()}\n`;
  });
}

/**
 * A `<Render />` the own-line rule did not match, in text whose fences are
 * already placeholders. Mentions inside code spans are prose about the syntax
 * (the contributing guide documents it) and stay; anything else would be
 * content dropped on the floor, which is the bug this module exists to fix, so
 * it fails the build.
 */
function assertNoUnexpandedRender(text: string, where: string): void {
  for (const [index, line] of text.split("\n").entries()) {
    if (!RENDER_MENTION.test(line)) continue;
    if (RENDER_MENTION.test(line.replace(/`[^`]*`/g, ""))) {
      throw new Error(
        `[markdown-partials] Unexpanded <Render> on "${where}" line ${index + 1}: ` +
          `${line.trim()}\nPut the tag on a line of its own so the markdown ` +
          `alternates can inline it, or wrap the mention in backticks if it is prose.`,
      );
    }
  }
}

/** Lift fenced blocks out into `store`, dedented by their own indentation. */
function protectFences(body: string, store: string[]): string {
  return body.replace(FENCE, (match, indent: string) => {
    const fence = indent
      ? match
          .split("\n")
          .map((line) => (line.startsWith(indent) ? line.slice(indent.length) : line.trimStart()))
          .join("\n")
      : match;
    store.push(fence);
    return FENCE_TOKEN(store.length - 1);
  });
}

/** Put the lifted fences back, verbatim, at column zero. */
function restoreFences(markdown: string, store: string[]): string {
  if (store.length === 0) return markdown;
  return markdown
    .replace(FENCE_TOKEN_LINE, (_match, index: string) => store[Number(index)] ?? "")
    .replace(FENCE_TOKEN_ANY, (_match, index: string) => store[Number(index)] ?? "");
}

function stripFrontmatter(body: string): string {
  return body.replace(/^---\n[\s\S]*?\n---\n?/, "");
}

/** Read a string-literal attribute out of a raw tag attribute list. */
function attrString(rawAttrs: string, name: string): string | undefined {
  const match = new RegExp(`\\b${name}\\s*=\\s*(?:"([^"]*)"|'([^']*)')`).exec(rawAttrs);
  return match ? (match[1] ?? match[2]) : undefined;
}

/**
 * `params={{ key: "value" }}` — the literal pairs only. A partial that needs a
 * computed value is beyond what a static markdown export can evaluate; its
 * placeholder is left in place rather than guessed at.
 */
function attrParams(rawAttrs: string): Record<string, string> {
  const block = /\bparams\s*=\s*\{\{([\s\S]*?)\}\}/.exec(rawAttrs);
  const params: Record<string, string> = {};
  if (!block?.[1]) return params;
  for (const [, key, dq, sq, bare] of block[1].matchAll(
    /([A-Za-z_$][\w$]*)\s*:\s*(?:"([^"]*)"|'([^']*)'|([\w.+-]+))/g,
  )) {
    const value = dq ?? sq ?? bare;
    if (key && value !== undefined) params[key] = value;
  }
  return params;
}

/** Mirror `Render.astro`'s required-params check so both surfaces fail alike. */
function assertParams(
  partial: MarkdownEntry,
  file: string,
  params: Record<string, string>,
  where: string,
): void {
  const declared = partial.data?.params;
  if (!Array.isArray(declared)) return;
  const missing = declared
    .filter((param): param is string => typeof param === "string" && !param.endsWith("?"))
    .filter((param) => !(param in params));
  if (missing.length > 0) {
    throw new Error(
      `[markdown-partials] Missing required params ${JSON.stringify(missing)} for ` +
        `"${file}" on "${where}". Declared: ${JSON.stringify(declared)}`,
    );
  }
}

function substituteParams(body: string, params: Record<string, string>): string {
  let out = body;
  for (const [key, value] of Object.entries(params)) {
    out = out.replace(new RegExp(`\\{\\s*${key}\\s*\\}`, "g"), value);
  }
  return out;
}

/** Levenshtein distance — same "did you mean" hint `Render.astro` gives. */
function distance(a: string, b: string): number {
  if (a === b) return 0;
  if (!a.length) return b.length;
  if (!b.length) return a.length;
  const v0 = new Array<number>(b.length + 1);
  const v1 = new Array<number>(b.length + 1);
  for (let i = 0; i <= b.length; i++) v0[i] = i;
  for (let i = 0; i < a.length; i++) {
    v1[0] = i + 1;
    for (let j = 0; j < b.length; j++) {
      const cost = a[i] === b[j] ? 0 : 1;
      v1[j + 1] = Math.min((v1[j] ?? 0) + 1, (v0[j + 1] ?? 0) + 1, (v0[j] ?? 0) + cost);
    }
    for (let j = 0; j <= b.length; j++) v0[j] = v1[j] ?? 0;
  }
  return v1[b.length] ?? 0;
}

function closest(target: string, candidates: string[], maxDist = 3): string | null {
  const t = target.toLowerCase();
  let best: { name: string; dist: number } | null = null;
  for (const c of candidates) {
    const d = distance(t, c.toLowerCase());
    if (d <= maxDist && (!best || d < best.dist)) best = { name: c, dist: d };
  }
  return best?.name ?? null;
}
