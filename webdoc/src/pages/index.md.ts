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
 * scope, so the copy below is a second transcription of the arrays at
 * `src/pages/index.astro:33-90` and the hero at `:107-117`. That is a drift
 * risk, and it is held closed by `scripts/check-markdown.mjs`: it parses
 * `index.astro` and fails the build when a headline, a differentiator, a proof
 * figure or a limit in the page is missing from `dist/index.md`. Edit the page
 * and this file together, or the check will tell you which one you forgot.
 *
 * `index.astro` stays the source of truth. Copy from it, not into it.
 */

import { config } from "virtual:nimbus/config";

export const prerender = true;

const url = (path: string) => (config.site ? new URL(path, config.site).href : path);

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
  "Every entity gets its own FIFO lane, created on first push, so a slow consumer on one " +
  "never stalls another. That design has two ends, and one broker holds both: " +
  "**1,000,000 messages a second** sustained for 24 hours, and **1,000,000 ordered " +
  "partitions** created during a run at a thousand a second and drained with zero errors. " +
  "Leases, explicit acks, deduplication and retention are on in both. Consumer groups, " +
  "replay and a dead-letter queue at both ends. Windowed aggregation that commits its " +
  "state, its output and its acks in one transaction. One stateless binary next to the " +
  "Postgres you already run, with the operations dashboard compiled into it: queue health, " +
  "per-group lag and message inspection, with nothing extra to deploy. No cluster, no " +
  "rebalancing, no JVM.";

/**
 * The alt text of the architecture diagram. It is written for a reader who
 * cannot see the figure, which is exactly the reader this file is for.
 */
const HOME_DIAGRAM =
  "Producers push to one queue, agent-tasks, split into ordered partitions, one per agent " +
  "session. Two consumer groups, an agent runner and a tracer, each receive every message. " +
  "One slow session stalls only its own partition.";

const differentiators = [
  {
    title: "One ordered lane per entity",
    body: "A partition is created on first push and costs index rows, not a commit-log file and not a process. One broker has held a million of them, and a consumer stuck on one never blocks another.",
  },
  {
    title: "No rebalancing, because there is nothing to rebalance",
    body: "The broker holds no cluster membership and no partition assignments; clients hold no coordination state. You scale by starting another copy against the same database, and a restarting worker costs nobody a pause.",
  },
  {
    title: "PostgreSQL is the storage engine",
    body: "Durability, replication, backup, point-in-time recovery and SQL introspection are the ones you already operate. There is no second data system to learn, size, or lose.",
  },
  {
    title: "Acknowledgement is an offset commit",
    body: "Consumption state is one cursor per partition and consumer group. There is no per-message delivery record to store, scan or clean up, which is why a million partitions and their cursors weigh 641 MB.",
  },
  {
    title: "Windowed aggregation, in the same transaction",
    body: "Tumbling, sliding, session and cron windows over a queue, where the window state, the messages you emit and the acknowledgement of the source all commit together or not at all. Exactly-once aggregation with no changelog topic and no state store to operate.",
  },
  {
    title: "Exactly the semantics you already know",
    body: "Offsets, consumer groups and replay from Kafka; leases, nacks and a dead-letter queue from RabbitMQ. Deliberately both, because real pipelines need both.",
  },
  {
    title: "Plain HTTP, six SDKs, one binary",
    body: "No custom wire protocol, no JVM, no Erlang, no ZooKeeper. Anything that can make an HTTP request is a first-class client, and the dashboard ships inside the same executable.",
  },
];

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

const notFor = [
  "Global total ordering. Order is per partition, in commit order, never across a queue.",
  "Priority or deadline scheduling. Nothing overtakes anything inside a partition.",
  "Unbounded parallelism inside one consumer group. One in-flight leased batch per partition, so the partition count is the ceiling.",
  "Surviving a long database outage. The disk spool covers a brief interruption, not a sustained one. PostgreSQL is one failure domain.",
];

/**
 * The landing page as markdown, from the eyebrow down. The `# ` headline is
 * left to the caller so this block can be dropped into `llms-full.txt`, whose
 * collation gives every page its own `#` heading.
 */
export function homepageBody(): string {
  const lines: string[] = [HOME_EYEBROW, "", HOME_LEAD, ""];

  lines.push("## How it is shaped", "", HOME_DIAGRAM, "");
  lines.push(
    "One queue, one ordered lane per session, two consumer groups reading independently. " +
      "The slow lane stalls by itself.",
    "",
  );

  lines.push("## What makes it different", "");
  for (const item of differentiators) {
    lines.push(`### ${item.title}`, "", item.body, "");
  }

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
  lines.push(
    "These are the load-bearing ones, worth checking against your requirements before " +
      "you build on them.",
    "",
  );
  for (const item of notFor) lines.push(`- ${item}`);
  lines.push("", `[The full list of limits and non-goals](${url("/start/limits")})`, "");

  lines.push("## Start", "");
  lines.push(
    `- [Push your first message](${url("/start/quickstart")}): the model in one page, the SDKs, and worked examples.`,
    `- [Host it](${url("/selfhost")}): deployment, PostgreSQL, high availability, security, operations.`,
    `- [Understand it](${url("/internals")}): segments, offsets, the push and pop paths, the schema underneath.`,
    `- [Why it exists](${url("/start/why")})`,
    `- [Plain HTTP API](${url("/reference/http")}): there are SDKs for JavaScript, Python, Go, Rust, PHP and C++, an operator CLI, and HTTP for everything else.`,
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
