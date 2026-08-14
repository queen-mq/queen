/**
 * `/index.md` — the markdown alternate for the landing page.
 *
 * Every docs page gets one of these from `pages/[...slug]/index.md.ts`, which
 * walks `getIndexedEntries()`. The landing page is not in that list: it is a
 * hand-written `index.astro`, not an entry of the `docs` collection, so it has
 * no content entry, no raw MDX body, and nothing for the downleveler to render.
 * The result was a corpus that carried 118 of the site's 119 URLs and left out
 * the one page that states what the product is: `/index.md` returned 404, and
 * the positioning paragraph, the differentiators and the measured numbers were
 * reachable only by parsing HTML.
 *
 * This route emits them as markdown, and `llms-full.txt.ts` prepends the same
 * block so the corpus opens with it.
 *
 * ## Why the copy is duplicated here
 *
 * The right shape is one shared module that `index.astro` and this route both
 * import. That means editing `index.astro`, which is outside this change's
 * scope, so the copy below is a second transcription of the arrays and the
 * prose in `src/pages/index.astro`. That is a drift risk, and it is held closed
 * by `scripts/check-markdown.mjs`: it parses `index.astro` and fails the build
 * when a headline, a differentiator, a proof figure or the limits paragraph in
 * the page is missing from `dist/index.md`. Edit the page and this file
 * together, or the check will tell you which one you forgot.
 *
 * The code samples are the exception, and they cannot drift: this file lifts
 * them out of the same generated snippet partials the page imports.
 *
 * `index.astro` stays the source of truth. Copy from it, not into it.
 */

import { config } from "virtual:nimbus/config";
import pushRaw from "../content/partials/snippets/js-push.mdx?raw";
import consumeRaw from "../content/partials/snippets/js-consume.mdx?raw";

export const prerender = true;

const url = (path: string) => (config.site ? new URL(path, config.site).href : path);

/** Same extraction as the page's, so both show the code that the suite runs. */
function snippet(raw: string, id: string): string {
  const fenced = raw.match(/```[a-z]*[^\n]*\n([\s\S]*?)```/);
  if (!fenced) throw new Error(`snippet ${id} has no fenced block`);
  return fenced[1].trimEnd();
}

/** The page's `<h1>`, verbatim. */
export const HOME_HEADLINE =
  "The queue that doesn't fall apart at the other end of your workload.";

/**
 * One line describing the page, for the index and corpus rows that list it.
 * Not `config.description`: those rows sit directly under the site
 * description, and repeating it there says nothing about this page.
 */
export const HOME_SUMMARY =
  "The landing page: what Queen MQ is, what makes it different, and the measured numbers " +
  "with the conditions they were measured under.";

/** The eyebrow above the headline. */
const HOME_EYEBROW = "Queen MQ 1.0 · Apache 2.0";

/**
 * The hero paragraph, with the page's `<strong>` spans as markdown emphasis
 * and its JSX line wrapping collapsed.
 */
const HOME_LEAD =
  "Every entity gets its own ordered FIFO lane, created on first push, so a slow consumer " +
  "on one never stalls another. One stateless binary on the PostgreSQL you already run, " +
  "measured at **1,000,000 messages a second** for 24 hours and at **1,000,000 ordered " +
  "partitions** in one database.";

/** The line of capabilities under the calls to action. */
const HOME_FEATURES = [
  "Consumer groups",
  "Replay",
  "Dead-letter queue",
  "Exact deduplication",
  "Transactional handoff",
  "Windowed aggregation",
  "A dashboard in the binary",
];

/**
 * The positioning figure's own description. The map is an inlined SVG, so its
 * `<desc>` is the only form of it that reaches a reader who cannot see it,
 * which is exactly the reader this file is for.
 */
const HOME_MAP =
  "A map of sustained message rate against ordered entities. Both axes are logarithmic, " +
  "unnumbered, and carry the same range and the same scale, so a system that reaches the " +
  "same figure on both draws a square. Kafka holds a low lane ceiling, because entities " +
  "hash onto a partition set sized in advance, and its region runs off the right of the " +
  "map. RabbitMQ closes a small corner, one live queue per entity. pgmq reaches higher in " +
  "entities at modest rate, its reads rescanning the standing backlog. SQS FIFO closes a " +
  "dashed corner at its published rate quota and its in-flight cap. Queen's region is the " +
  "largest, and it is a square: a million messages a second sustained for 24 hours, and a " +
  "million ordered lanes in one database, from two separate runs.";

const HOME_MAP_CAPTION =
  "Each frontier is where a system stops keeping one ordered lane per entity: measured on " +
  "matched hardware for Kafka, RabbitMQ and pgmq, taken from the published quotas for SQS, " +
  "and dashed wherever the edge is one we did not measure.";

const differentiators = [
  {
    title: "One ordered lane per entity",
    body: "A partition is created on first push and costs index rows, not a commit-log file and not a process. A consumer stuck on one lane never blocks another.",
  },
  {
    title: "No rebalancing, because there is nothing to rebalance",
    body: "Broker and clients hold no coordination state, so scaling is starting another copy against the same database.",
  },
  {
    title: "PostgreSQL is the storage engine",
    body: "Durability, replication, backup and SQL introspection are the ones you already operate, not a second data system.",
  },
  {
    title: "Acknowledgement is an offset commit",
    body: "One cursor per partition and consumer group, with no per-message delivery record to store, scan or clean up.",
  },
  {
    title: "Windowed aggregation, in the same transaction",
    body: "Tumbling, sliding, session and cron windows whose state, output and acks commit together or not at all: exactly-once, with no changelog topic and no state store.",
  },
  {
    title: "Exactly the semantics you already know",
    body: "Offsets, consumer groups and replay from Kafka; leases, retries and a dead-letter queue from RabbitMQ. Real pipelines need both.",
  },
  {
    title: "Plain HTTP, six SDKs, one binary",
    body: "No custom wire protocol, no JVM, no Erlang, no ZooKeeper. Anything that can make an HTTP request is a first-class client.",
  },
];

/** The dashboard section, and the screenshot's alt text with it. */
const HOME_DASHBOARD =
  "Queue health, per-group lag, message inspection and dead-letter replay. Nothing was " +
  "installed to get this: it is the same binary, on the port you already opened, and it " +
  "grows logins and roles when it runs behind the proxy.";

const HOME_DASHBOARD_IMAGE =
  "The bundled dashboard's overview: stored messages, queues, partitions, consumer groups, " +
  "pending and completed counts above a table of throughput, lag and error series with " +
  "sparklines.";

const proof = [
  {
    figure: "86.4B",
    unit: "messages",
    body: "24 hours of continuous load at ~1M msg/s per side with explicit acknowledgement, 1,000 messages touched by an error, zero restarts, broker memory flat.",
    href: "/benchmarks/soak-24h",
  },
  {
    figure: "0",
    unit: "order violations",
    body: "1,000 partitions through a four-stage pipeline at 25k events/s: 88,503,408 messages verified, zero duplicates, zero gaps.",
    href: "/benchmarks/ordered-pipeline",
  },
  {
    figure: "1M",
    unit: "ordered partitions",
    body: "A million FIFO lanes in one PostgreSQL, none preallocated, created during the run at a thousand a second and drained with zero push, pop or ack errors.",
    href: "/benchmarks/cardinality-1m",
  },
];

/**
 * The limits paragraph. It used to be a list on the page and is a paragraph
 * now; `scripts/check-markdown.mjs` reads it out of the page's Limits section
 * rather than out of an array, and looks for it here.
 */
const HOME_LIMITS =
  "Queen has real limits: ordering is per partition and never global, nothing overtakes " +
  "anything inside a lane, in-group parallelism is capped by partition count, and one " +
  "PostgreSQL is both the ceiling and the failure domain.";

/**
 * The landing page as markdown, from the eyebrow down. The `# ` headline is
 * left to the caller so this block can be dropped into `llms-full.txt`, whose
 * collation gives every page its own `#` heading.
 */
export function homepageBody(): string {
  const lines: string[] = [HOME_EYEBROW, "", HOME_LEAD, ""];

  lines.push(HOME_FEATURES.join(" · "), "");

  lines.push("## It looks like this", "");
  lines.push(
    "Queues and partitions are created on first use, so there is nothing to provision " +
      "before the first line runs.",
    "",
  );
  lines.push("Produce:", "", "```js", snippet(pushRaw, "js-push"), "```", "");
  lines.push("Consume:", "", "```js", snippet(consumeRaw, "js-consume"), "```", "");
  lines.push(
    `There are [SDKs for JavaScript, Python, Go, Rust, PHP and C++](${url("/use/clients/")}), ` +
      `[an operator CLI](${url("/reference/queenctl/")}), and a ` +
      `[plain HTTP API](${url("/reference/http")}) for everything else.`,
    "",
  );

  lines.push("## Where it sits", "", HOME_MAP, "");
  lines.push(
    `${HOME_MAP_CAPTION} The conditions behind every figure are in ` +
      `[the comparison](${url("/start/compare/")}) and in ` +
      `[the measured runs](${url("/benchmarks/comparison")}).`,
    "",
  );

  lines.push("## What makes it different", "");
  for (const item of differentiators) {
    lines.push(`### ${item.title}`, "", item.body, "");
  }

  lines.push("## The dashboard is already in there", "", HOME_DASHBOARD, "");
  lines.push(HOME_DASHBOARD_IMAGE, "");

  lines.push("## Measured, with the conditions attached", "");
  lines.push(
    "Every number on this site names the run that produced it. A figure without an " +
      "archived artifact recording its configuration does not get published here.",
    "",
  );
  for (const item of proof) {
    lines.push(`### ${item.figure} ${item.unit}`, "", item.body, "", `[The run](${url(item.href)})`, "");
  }

  lines.push("## The limits worth knowing first", "");
  lines.push(HOME_LIMITS, "");
  lines.push(`[Read the full list before you design around it](${url("/start/limits")})`, "");

  lines.push("## Start", "");
  lines.push(
    `- [Use it](${url("/start/quickstart")}): the model in one page, the SDKs, and worked examples.`,
    `- [Pick your SDK](${url("/use/clients/")}): JavaScript, Python, Go, Rust, PHP and C++, plus queenctl and plain HTTP.`,
    `- [Host it](${url("/selfhost")}): deployment, PostgreSQL, high availability, security, operations.`,
    `- [Understand it](${url("/internals")}): segments, offsets, the push and pop paths, the schema underneath.`,
    "- [Source on GitHub](https://github.com/queen-mq/queen)",
    "",
  );

  return lines.join("\n").trim();
}

export async function GET() {
  const body = [
    "---",
    `title: ${JSON.stringify(config.title)}`,
    ...(config.description ? [`description: ${JSON.stringify(config.description)}`] : []),
    ...(config.socialImage ? [`image: ${JSON.stringify(url(config.socialImage))}`] : []),
    "---",
    "",
    "> Documentation Index",
    `> Fetch the complete documentation index at: ${url("/llms.txt")}`,
    "> Use this file to discover all available pages before exploring further.",
    "",
    `# ${HOME_HEADLINE}`,
    "",
    homepageBody(),
    "",
    `Source: ${url("/")}`,
    "",
  ].join("\n");

  return new Response(body, {
    headers: { "Content-Type": "text/markdown; charset=utf-8" },
  });
}
