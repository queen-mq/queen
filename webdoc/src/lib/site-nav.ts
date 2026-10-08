/**
 * The site's top-level destinations, in the header of every page.
 *
 * The header's own "Sections" nav renders only with a section-scoped sidebar,
 * and this site shows the full tree (`scope: "full"`), so until these links
 * existed the header carried no navigation at all: on the homepage, which has
 * no sidebar drawer either, a phone reader had no way into the docs but the
 * hero buttons.
 *
 * Four doors and the hosted service: where to start, what to build, how it
 * compares, and the measurements. Everything else is one click away in the
 * sidebar.
 */

export interface SiteLink {
  label: string;
  href: string;
  external?: boolean;
}

export const SITE_NAV: readonly SiteLink[] = [
  { label: "Docs", href: "/start/" },
  { label: "Guides", href: "/guides/" },
  { label: "Compare", href: "/start/compare/" },
  { label: "Benchmarks", href: "/benchmarks/" },
];

export const CLOUD_LINK: SiteLink = {
  label: "Queen Cloud",
  href: "https://queenmq.cloud/",
  external: true,
};

/** Pages that are not part of the docs, so no nav link is current on them. */
const OUTSIDE_DOCS = new Set(["/", "/404/"]);

/**
 * The nav link the current page belongs to: the longest `href` that prefixes
 * the path, and "Docs" for any other docs page. The homepage and the 404 page
 * belong to none.
 */
export function activeNavHref(pathname: string): string | undefined {
  const path = pathname.endsWith("/") ? pathname : `${pathname}/`;
  if (OUTSIDE_DOCS.has(path)) return undefined;
  const match = SITE_NAV.filter((link) => path.startsWith(link.href)).reduce<SiteLink | undefined>(
    (best, link) => (!best || link.href.length > best.href.length ? link : best),
    undefined,
  );
  return (match ?? SITE_NAV[0]).href;
}
