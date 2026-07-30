import { defineCollection } from "astro:content";
// `z` re-exported from `astro:content` is deprecated; import it from
// `astro/zod` (the pattern nimbus-docs' own schema helpers document).
import { z } from "astro/zod";
import { docsCollection, partialsCollection } from "@cloudflare/nimbus-docs/content";

/**
 * Queen MQ docs frontmatter.
 *
 * The base Nimbus schema is strict — an undeclared key fails the build. The
 * fields below exist because this site makes claims about a running system,
 * and every claim has to stay traceable:
 *
 *   type          the Nimbus content recipe the page follows. One page, one
 *                 reader question.
 *   tier          which reading tier the page belongs to: `use` (build on
 *                 Queen), `operate` (host it), `internals` (understand it).
 *   status        publication maturity. Anything other than `stable` renders
 *                 a badge, so a reader never mistakes a preview for a
 *                 contract.
 *   sourceOfTruth repo-relative paths of the code that governs the page. If
 *                 that code changes, the page is in debt.
 *   verifiedBy    repo-relative paths of tests or examples that execute the
 *                 code shown on the page. A page that promises "verified"
 *                 names what verified it.
 *   generated     the page or partial is emitted by webdoc/scripts/. Hand
 *                 edits are lost on the next regeneration.
 */
const RECIPE_TYPES = [
  "overview",
  "quickstart",
  "tutorial",
  "how-to",
  "concept",
  "reference",
  "example",
  "troubleshooting",
  "changelog",
] as const;

export const collections = {
  docs: defineCollection(
    docsCollection({
      schemaFields: {
        type: z.enum(RECIPE_TYPES).optional(),
        tier: z.enum(["use", "operate", "internals"]).optional(),
        status: z.enum(["stable", "beta", "preview", "internal"]).default("stable"),
        sourceOfTruth: z.array(z.string()).optional(),
        verifiedBy: z.array(z.string()).optional(),
        generated: z.boolean().default(false),
      },
    }),
  ),
  partials: defineCollection(partialsCollection()),
};
