import { defineConfig } from "astro/config";
import icon from "astro-icon";
import tailwindcss from "@tailwindcss/vite";
import nimbus, { defineConfig as defineNimbusConfig } from "@cloudflare/nimbus-docs";
import { tableScroll } from "@cloudflare/nimbus-docs/markdown";

// Site-wide structured data. Nothing else on the site states what this
// software is, so the framework's WebSite node is the only machine-readable
// claim; these three add the software itself, its source and its publisher.
// Deliberately no `softwareVersion`: the repository and the registries
// disagree on it, and a hardcoded version here goes stale without failing
// anything. No `softwareRequirements` either: since 2.0 the broker needs
// nothing beside itself but a data directory on local disk.
const structuredData = {
  "@context": "https://schema.org",
  "@graph": [
    {
      "@type": "Organization",
      "@id": "https://queenmq.com/#org",
      name: "Queen MQ",
      url: "https://queenmq.com/",
      logo: "https://queenmq.com/queen-tile.png",
      sameAs: ["https://github.com/queen-mq/queen"],
    },
    {
      "@type": "SoftwareApplication",
      "@id": "https://queenmq.com/#software",
      name: "Queen MQ",
      applicationCategory: "DeveloperApplication",
      applicationSubCategory: "Message broker",
      operatingSystem: "Linux",
      programmingLanguage: "Rust",
      license: "https://www.apache.org/licenses/LICENSE-2.0",
      downloadUrl: "https://ghcr.io/queen-mq/queen",
      url: "https://queenmq.com/",
      author: { "@id": "https://queenmq.com/#org" },
      offers: { "@type": "Offer", price: "0", priceCurrency: "USD" },
    },
    {
      "@type": "SoftwareSourceCode",
      "@id": "https://queenmq.com/#source",
      codeRepository: "https://github.com/queen-mq/queen",
      programmingLanguage: "Rust",
      license: "https://www.apache.org/licenses/LICENSE-2.0",
      about: { "@id": "https://queenmq.com/#software" },
    },
  ],
};

const nimbusConfig = defineNimbusConfig({
  site: "https://queenmq.com",
  title: "Queen MQ",
  description:
    "A transactional event broker in one binary: one ordered partition per entity, and the ack, the state change, the next events and the timer of each step commit as one entry, replicated with Raft.",
  locale: "en",
  homeLabel: "Queen MQ",
  github: "https://github.com/queen-mq/queen",
  editPattern: "https://github.com/queen-mq/queen/edit/master/webdoc/{path}",
  socialImageAlt: "Queen MQ documentation",
  // Brand assets are generated: the logo, the favicons and the tile by
  // assets/logo.py, the homepage's picture by assets/scene.py. The favicon
  // is the small logo, the q that is the sunflower; the SVG switches its
  // letter with the tab strip's colour scheme.
  // No `rel="icon"` entry here: NimbusHead already emits one for whichever of
  // favicon.svg / .ico / .png it finds in public/, and repeating it produced
  // two identical <link> tags on every page.
  head: [
    { tag: "link", attrs: { rel: "apple-touch-icon", href: "/apple-touch-icon.png" } },
    {
      tag: "script",
      attrs: { type: "application/ld+json" },
      content: JSON.stringify(structuredData),
    },
  ],
  sidebar: {
    // The whole tree in every rail, with groups collapsed. Section-scoped rails
    // read shorter, but they hide the rest of the site behind the header tabs —
    // a reader who does not know a section exists never opens it. Six collapsed
    // groups fit on screen, and the group holding the current page opens itself.
    scope: "full",
    defaultCollapsed: true,
    overviewLabel: "Overview",
    indexDisplay: "overview-leaf",
    // Eight sections, in the order a reader meets them: what it is, the model,
    // what to build with it, whole programs built with it, how to run it, the
    // exhaustive tables, how it works inside, and the evidence.
    items: [
      { label: "Start", icon: "ph:rocket-launch", autogenerate: { directory: "start" } },
      { label: "Concepts", icon: "ph:graph", autogenerate: { directory: "concepts" } },
      { label: "Guides", icon: "ph:code", autogenerate: { directory: "guides" } },
      { label: "Examples", icon: "ph:app-window", autogenerate: { directory: "examples" } },
      { label: "Operate", icon: "ph:hard-drives", autogenerate: { directory: "operate" } },
      { label: "Reference", icon: "ph:book-open-text", autogenerate: { directory: "reference" } },
      { label: "Internals", icon: "ph:cpu", autogenerate: { directory: "internals" } },
      { label: "Benchmarks", icon: "ph:chart-line-up", autogenerate: { directory: "benchmarks" } },
    ],
  },
});

export default defineConfig({
  // Static output: `pnpm build` emits dist/, which Cloudflare Pages serves
  // directly (build command `pnpm build`, output directory `dist`).
  output: "static",
  // Tailwind v4 via its Vite plugin (the integration Astro recommends for
  // Tailwind v4 — replaces the PostCSS plugin, which doesn't build under
  // Astro 7's Vite 8 bundler).
  vite: {
    plugins: [tailwindcss()],
  },
  // Hover-prefetch link targets so full-page navigations feel instant without
  // a client-side router.
  prefetch: {
    prefetchAll: true,
    defaultStrategy: "hover",
  },
  integrations: [
    icon(),
    nimbus(nimbusConfig, {
      // Every rule that can catch a structural or factual regression is an
      // error. Large parts of this site are generated from source code and
      // cross-linked to it, so a broken link or a malformed frontmatter
      // block usually means a page has drifted from what it documents.
      rules: {
        "nimbus/frontmatter-shape": "error",
        // The generated OpenAPI documents are static assets under public/, not
        // pages, so the route map the rule checks against does not contain
        // them. Everything else must resolve to a real page.
        "nimbus/internal-link": ["error", { ignore: ["/openapi/**"] }],
        "nimbus/description-required": "error",
        "nimbus/single-h1": "error",
        "nimbus/heading-hierarchy": "error",
        "nimbus/code-block-lang": "error",
        "nimbus/code-block-prompt-prefix": "error",
        "nimbus/no-self-host-url": "error",
        "nimbus/image-ref": "error",
        "nimbus/duplicate-heading-text": "warn",
        "nimbus/heading-punctuation": "warn",
        "nimbus/bare-url": "warn",
      },
      // Wrap wide tables so they scroll instead of overflowing the page
      // (styled by `.nb-table-scroll` in src/styles/prose.css).
      markdown: {
        hastPlugins: [tableScroll()],
      },
    }),
  ],
});
