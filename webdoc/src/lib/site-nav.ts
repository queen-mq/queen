/**
 * The site's top-level destinations, in the header of the pages outside the
 * docs: the homepage and the 404 page.
 *
 * The header's own "Sections" nav renders only with a section-scoped sidebar,
 * and this site shows the full tree (`scope: "full"`), so until these links
 * existed the header carried no navigation at all: on the homepage, which has
 * no sidebar drawer either, a phone reader had no way into the docs but the
 * hero buttons. A docs page has the sidebar, so its header carries none.
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

/** Pages that are not part of the docs: they have no sidebar to navigate with. */
const OUTSIDE_DOCS = new Set(["/", "/404/"]);

/** Whether the page is outside the docs, where the header carries the site's links. */
export function isOutsideDocs(pathname: string): boolean {
  const path = pathname.endsWith("/") ? pathname : `${pathname}/`;
  return OUTSIDE_DOCS.has(path);
}
