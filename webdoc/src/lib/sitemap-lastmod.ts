import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";

/**
 * `<lastmod>` for every sitemap URL, from the same source as the "Updated"
 * line on each page: a `lastUpdated` in the page's frontmatter, else the
 * author date of the last commit that touched the page's source file (Nimbus
 * reads `%at` for `getLastUpdated`).
 *
 * Never the build date. A sitemap whose every URL changes on every deploy
 * teaches a crawler to ignore the field; one that moves only when a page does
 * tells it which pages to fetch again.
 *
 * The homepage is `src/pages/index.astro` with its copy in `src/lib/home.ts`,
 * so it takes the newer of the two. A file git does not know yet (a page
 * written but not committed) takes its modification time. A shallow clone has
 * no history to read, so it gets no dates at all rather than wrong ones; CI
 * checks out with `fetch-depth: 0` for the same reason.
 */

const HOME_SOURCES = ["src/pages/index.astro", "src/lib/home.ts"];
const DOCS_DIR = "src/content/docs";

type Dates = Map<string, string>;

function git(webdoc: string, args: string[]): string {
  return execFileSync("git", ["-c", "core.quotePath=false", ...args], {
    cwd: webdoc,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    stdio: ["ignore", "pipe", "ignore"],
  });
}

/** Repo-relative-to-webdoc path -> ISO author date of its last commit. */
function readGitDates(webdoc: string): Dates | null {
  try {
    if (git(webdoc, ["rev-parse", "--is-shallow-repository"]).trim() === "true") return null;
    const log = git(webdoc, ["log", "--format=t:%aI", "--name-only", "--relative", "--", DOCS_DIR, ...HOME_SOURCES]);
    const dates: Dates = new Map();
    let current: string | undefined;
    for (const line of log.split("\n")) {
      if (line.startsWith("t:")) current = line.slice(2);
      else if (line && current && !dates.has(line)) dates.set(line, current);
    }
    return dates;
  } catch {
    return null;
  }
}

/** `lastUpdated:` from a content file's frontmatter, as an ISO date. */
function frontmatterDate(abs: string): string | undefined {
  if (!/\.mdx?$/.test(abs)) return undefined;
  const frontmatter = readFileSync(abs, "utf8").match(/^---\n([\s\S]*?)\n---/);
  const value = frontmatter?.[1].match(/^lastUpdated:\s*["']?([^"'\n]+)["']?\s*$/m)?.[1];
  const time = value ? Date.parse(value) : Number.NaN;
  return Number.isNaN(time) ? undefined : new Date(time).toISOString();
}

/**
 * The same precedence as the "Updated" line in src/pages/[...slug].astro:
 * frontmatter wins, git is the fallback. A generated page (the changelog) is
 * gitignored and written at build time, so without its frontmatter date it
 * would fall through to its mtime, which is the build.
 */
function dateOf(webdoc: string, dates: Dates, file: string): string | undefined {
  const abs = join(webdoc, file);
  if (!existsSync(abs)) return undefined;
  return frontmatterDate(abs) ?? dates.get(file) ?? statSync(abs).mtime.toISOString();
}

/** The source files a URL path is built from. */
function sourcesOf(webdoc: string, pathname: string): string[] {
  const slug = pathname.replace(/^\/+|\/+$/g, "");
  if (!slug) return HOME_SOURCES;
  const candidates = [".mdx", ".md", "/index.mdx", "/index.md"].map((ext) => `${DOCS_DIR}/${slug}${ext}`);
  return candidates.filter((file) => existsSync(join(webdoc, file))).slice(0, 1);
}

/** Builds the `serialize` hook for `nimbus(..., { sitemap: { serialize } })`. */
export function sitemapLastmod(webdoc: string) {
  let dates: Dates | null | undefined;
  return <T extends { url: string; lastmod?: string }>(item: T): T => {
    if (dates === undefined) dates = readGitDates(webdoc);
    const known = dates;
    if (known === null) return item;
    // Dates carry their author's UTC offset, so compare instants, not strings.
    const lastmod = sourcesOf(webdoc, new URL(item.url).pathname)
      .map((file) => dateOf(webdoc, known, file))
      .filter((d): d is string => Boolean(d))
      .reduce<string | undefined>((newest, d) => (!newest || Date.parse(d) > Date.parse(newest) ? d : newest), undefined);
    return lastmod ? { ...item, lastmod } : item;
  };
}
