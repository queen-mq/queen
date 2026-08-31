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
  "High-performance transactional messaging for applications that need an ordered stream per entity.";

/**
 * One line describing the page, for the index and corpus rows that list it.
 * Not `config.description`: those rows sit directly under the site
 * description, and repeating it there says nothing about this page.
 */
export const HOME_SUMMARY =
  "The landing page: what Queen MQ is, the entity-per-partition model it is built on, how " +
  "partitions, brokers and cells scale differently, and the measured numbers with the " +
  "conditions they were measured under.";

/** The eyebrow above the headline. */
const HOME_EYEBROW = "Queen MQ · Apache 2.0";

/**
 * The hero paragraph, with the page's `<strong>` spans as markdown emphasis
 * and its JSX line wrapping collapsed.
 */
const HOME_LEAD =
  "Queen is a message broker written in Rust that keeps every byte of its state in " +
  "PostgreSQL. Its defining abstraction is **one logical ordered partition per application " +
  "entity**, a customer, an account, a conversation, a device, a workflow, a session or a " +
  "job, created by the first push that names it and never provisioned in advance.";

/** The line of capabilities under the calls to action. */
const HOME_FEATURES = [
  "Consumer groups",
  "Replay and seek",
  "Dead-letter queue",
  "Exact deduplication",
  "Transactional ack + state + push",
  "Key/value state",
  "Cancellable timers",
  "Windowed aggregation",
  "Ephemeral queues",
  "Multi-tenancy",
  "Kafka wire protocol",
  "SQS and SNS wire protocols",
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
    title: "The entity is the partition",
    body: "Most brokers give you ordering per shard, and your requirement is ordering per entity. Hash entities onto a fixed partition count and the ones that collide block each other. Give each entity its own queue and broker-side objects grow with your customer list. Queen removes the bridge: a partition is created by the first push that names it, and a slow customer delays only itself.",
  },
  {
    title: "The partition key is your ordering boundary",
    body: "customer_id, account_id, conversation_id, device_id, workflow_id. You choose it, and it is an application decision rather than an infrastructure sizing decision. Do not pick a key because it has high cardinality: pick the boundary your application genuinely requires.",
  },
  {
    title: "Ack the input, write the state and push the output in one commit",
    body: "One transaction bundles acknowledgements, pushes, key/value writes and timer operations, across any number of partitions, queues and consumer groups. That is what replaces transactional outbox tables, a separate store for idempotency markers, and the reconciliation code that exists only because the broker's commit and the database's commit were two different commits.",
  },
  {
    title: "Application scale is not infrastructure scale",
    body: "In most brokers a per-entity ordering guarantee means a per-entity infrastructure object, either a topic partition with its own files and replicas or a live server-side queue. In Queen a partition is a row. A million of them measured 315 MB in total, and the serve path does not care how many exist.",
  },
  {
    title: "Partitions, brokers and cells scale different things",
    body: "Partitions scale application cardinality. Brokers scale serving capacity and availability inside a cell, with three replicas the designed ceiling. Cells scale the deployment: capacity grows by adding cells, not by growing one system, and there is no global cluster to join and no cross-cell coordination in the message path.",
  },
  {
    title: "PostgreSQL is the durable source of truth",
    body: "Not somewhere to put the bytes. Messaging state and application state share a transaction, which is the whole reason for the design, and durability, replication, backup and SQL introspection are the ones you already operate. The trade is plain: the database is the throughput ceiling and the single failure domain.",
  },
  {
    title: "Brokers hold nothing authoritative",
    body: "Messages, offsets, leases, deduplication state, queue configuration and dead letters are all rows, so a broker can be added, removed, restarted or rolled without a rebalance, and deduplication stays exact across replicas with no coordination protocol at all.",
  },
  {
    title: "Key/value state, timers and windows are part of the engine",
    body: "A key/value write can share the transaction with a push and an ack, which a store standing beside the broker cannot do at any price. A timer is a scheduled message you can cancel and reprogram until it fires. Tumbling, sliding, session and cron windows commit their state, their output and their acks together. None of it is a flag you turn on.",
  },
  {
    title: "Many tenants on one cell, isolation enforced in SQL",
    body: "The broker scopes queue identity natively as (tenant, name), so two tenants both owning a queue called orders own different queues. The proxy is the tenant-facing boundary that makes the identity driving that scoping trustworthy. Neither half is sufficient alone.",
  },
  {
    title: "Plain HTTP, six SDKs, one binary",
    body: "No custom wire protocol, no JVM, no Erlang, no ZooKeeper. Anything that can make an HTTP request is a first-class client, and curl is one.",
  },
  {
    title: "Kafka clients reach it by changing one line",
    body: "queen-kafka is a facade that ships in the same image as the broker and stays off until you switch it on. It advertises 32 Kafka API keys, transactions included, so an unmodified producer or consumer moves across by changing bootstrap.servers and nothing else. It holds no database connection and stores nothing durable: it is a Queen client like any SDK is, and what it deliberately does not do is written down.",
  },
  {
    title: "So do SQS and SNS clients",
    body: "queen-sqs answers both Amazon wire protocols out of that same image, so an unmodified AWS SDK moves across by changing endpoint_url. Nothing durable lives in the process, so any instance answers any request and an ordinary load balancer in front is the supported shape rather than a hazard.",
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
    unit: "messages in 24 hours",
    body: "About 1,000,000 a second in each direction, pushed, popped and acknowledged, with explicit acks and deduplication on. Zero restarts, and broker memory flat near 4.1 GB for the whole run.",
    href: "/benchmarks/soak-24h",
  },
  {
    figure: "1M",
    unit: "ordered partitions",
    body: "A million FIFO lanes in one PostgreSQL, none preallocated, created during the run at a thousand a second while serving 200,000 messages a second. Zero push, pop or ack errors over 722 million messages.",
    href: "/benchmarks/cardinality-1m",
  },
  {
    figure: "0",
    unit: "order violations",
    body: "1,000 partitions through a four-stage pipeline at 25,000 events a second: 88,503,408 messages verified by a per-stage checker, with zero duplicates and zero gaps.",
    href: "/benchmarks/ordered-pipeline",
  },
  {
    figure: "0",
    unit: "cross-tenant deliveries",
    body: "Twelve tenants sharing one queue name and one consumer group name for an hour, with enforcement on. Not one message crossed a tenant boundary. Isolation is the clean result of that run, not throughput.",
    href: "/benchmarks/multitenant-cell",
  },
];

/**
 * The limits paragraph. It used to be a list on the page and is a paragraph
 * now; `scripts/check-markdown.mjs` reads it out of the page's Limits section
 * rather than out of an array, and looks for it here.
 */
const HOME_LIMITS =
  "Queen has real limits, and some workloads are better served elsewhere. One ordered " +
  "lane is sequential, so if your ordering boundary is everything, the core idea does " +
  "nothing for you. In-group parallelism is bounded by how many distinct entities you " +
  "push to. One PostgreSQL is both the throughput ceiling and the failure domain, there " +
  "is no tiered object storage and no cross-region replication, and the Kafka facade " +
  "speaks the wire protocol but not the ecosystem around it, so log compaction, Kafka " +
  "Streams, Connect's exactly-once source and the Schema Registry's compacted topic all " +
  "stay out.";

/**
 * The landing page as markdown, from the eyebrow down. The `# ` headline is
 * left to the caller so this block can be dropped into `llms-full.txt`, whose
 * collation gives every page its own `#` heading.
 */
export function homepageBody(): string {
  const lines: string[] = [HOME_EYEBROW, "", HOME_LEAD, ""];

  lines.push(HOME_FEATURES.map((f) => `**${f}**`).join(" · "), "");

  // The README's opening argument, transcribed from index.astro. Two
  // transcriptions, like the rest of this file; edit both together.
  lines.push("## The problem", "");
  lines.push(
    "Most brokers give you ordering per *shard*. Your requirement is ordering per *entity*: " +
      "this customer's events processed in order, this conversation's messages not overtaking " +
      "each other, this account's transactions settling in sequence.",
    "",
    "Bridging the two is where the pain lives. Hash your entities onto a fixed partition " +
      "count and the ones that collide block each other: a slow customer stalls every customer " +
      "sharing its shard. Give each entity its own queue instead and broker-side objects grow " +
      "with your customer list.",
    "",
    "Queen removes the bridge: **the entity is the partition.**",
    "",
  );

  lines.push("## One entity, one ordered partition", "");
  lines.push(
    "Each partition is an independent ordered lane, created by the push that first names it. " +
      "Nothing is preallocated, nothing is assigned, nothing rebalances when a consumer restarts.",
    "",
    "```text",
    "customer A  ──►  A1 ──► A2 ──► A3     strict FIFO within a lane",
    "customer B  ──►  B1 ──► B2            B is not held up by A",
    "customer C  ──►  C1 ──► C2 ──► C3     C is not held up by A or B",
    "```",
    "",
    "A single hot partition stays sequential, by design. Parallelism comes from many active " +
      `partitions, not from splitting one. [The model, in one page](${url("/use/model/")})`,
    "",
  );

  lines.push("## Transactional processing", "");
  lines.push(
    "The second reason Queen exists, and the reason PostgreSQL is not an implementation " +
      "detail. A single call bundles acknowledgements, pushes, key/value writes and timer " +
      "operations into one PostgreSQL transaction.",
    "",
    "```text",
    "consume input",
    "     │",
    "     ├── update application state   (kv rider)",
    "     ├── produce output             (push, any queue, any partition)",
    "     ├── schedule / cancel a timer  (timers rider)",
    "     └── acknowledge input          (cursor advance)",
    "                │",
    "             COMMIT          all of it, or none of it",
    "```",
    "",
    "Atomicity covers broker state, not the network. Queen does not make an external HTTP " +
      "call exactly-once, and no broker can. The one case that is exactly-once end to end is " +
      "when the effect is itself a row in this PostgreSQL, written through the key/value " +
      "rider: marker, effect, output and cursor advance become a single commit. " +
      `[The bundle shape and every rollback cause](${url("/reference/http/transaction/")})`,
    "",
  );

  lines.push("## Where it sits", "", HOME_MAP, "");
  lines.push(
    `${HOME_MAP_CAPTION} The conditions behind every figure are in ` +
      `[the comparison](${url("/start/compare/")}) and in ` +
      `[the measured runs](${url("/benchmarks/comparison")}).`,
    "",
  );

  lines.push("## It looks like this", "");
  lines.push(
    "Queues and partitions are created on first use, so there is nothing to provision " +
      "before the first line runs.",
    "",
  );
  lines.push("Produce:", "", "```js", snippet(pushRaw, "js-push"), "```", "");
  lines.push("Consume:", "", "```js", snippet(consumeRaw, "js-consume"), "```", "");
  lines.push(
    `There are [SDKs for JavaScript, Python, Go, Rust, PHP and C++](${url("/use/js-client")}), ` +
      `[an operator CLI](${url("/reference/queenctl/")}), and a ` +
      `[plain HTTP API](${url("/reference/http")}) for everything else.`,
    "",
  );

  lines.push("## What makes it different", "");
  for (const item of differentiators) {
    lines.push(`### ${item.title}`, "", item.body, "");
  }

  lines.push("## Three kinds of scale", "");
  lines.push(
    "Three axes, frequently confused, not interchangeable. Confusing them is the most common " +
      "way to mis-size a deployment.",
    "",
    "- **Partitions scale application cardinality.** Add entities freely. Nothing is " +
      "provisioned, no process is created, no rebalance runs. Millions of logical entity " +
      "streams do not require millions of infrastructure objects.",
    "- **Brokers scale capacity inside a cell.** Stateless replicas of one binary against one " +
      "PostgreSQL, covering a process dying, a rolling restart, one node's network. Three " +
      "replicas is the designed ceiling: past that the bottleneck is the database, not the " +
      "broker count.",
    "- **Cells scale the deployment.** A cell is PostgreSQL plus one or more stateless " +
      "brokers, optionally fronted by the proxy. Capacity grows by adding cells, not by " +
      "growing one system: no global cluster to join, no cross-cell coordination in the " +
      "message path.",
    "",
    "```text",
    "                    Queen Cell",
    "     ┌────────────────────────────────────┐",
    "     │  Queen Broker ──┐                  │",
    "     │  Queen Broker ──┼──► PostgreSQL    │  the only durable state",
    "     │  Queen Broker ──┘                  │",
    "     │  Queen Proxy  (optional)           │  tenant-facing boundary",
    "     └────────────────────────────────────┘",
    "```",
    "",
    "A cell is at once the scaling boundary, the failure boundary and the unit of upgrade and " +
      "operational ownership. The failure domain is PostgreSQL: Queen does not replicate " +
      "itself, and keeping the database alive is PostgreSQL's own tooling. " +
      `[Replicas, the mesh, and surviving a database outage](${url("/deploy/ha/")})`,
    "",
  );

  lines.push("## The dashboard is already in there", "", HOME_DASHBOARD, "");
  lines.push(HOME_DASHBOARD_IMAGE, "");

  lines.push("## Measured, with the conditions attached", "");
  lines.push(
    "Every number on this site names the run that produced it. A figure without an " +
      "archived artifact recording its configuration does not get published here. These are " +
      "single-shape runs: they say nothing about your throughput, latency, PostgreSQL sizing " +
      "or retention capacity, which follow from your workload, payloads and hardware.",
    "",
  );
  for (const item of proof) {
    lines.push(`### ${item.figure} ${item.unit}`, "", item.body, "", `[The run](${url(item.href)})`, "");
  }

  lines.push("## The limits worth knowing first", "");
  lines.push(HOME_LIMITS, "");
  lines.push(`[Read the full list before you design around it](${url("/reference/limits")})`, "");

  lines.push("## Start", "");
  lines.push(
    `- [Use it](${url("/start/quickstart")}): the model in one page, the SDKs, and worked examples.`,
    `- [Pick your SDK](${url("/use/js-client")}): JavaScript, Python, Go, Rust, PHP and C++, plus queenctl and plain HTTP.`,
    `- [Host it](${url("/deploy")}): deployment, PostgreSQL, high availability, security, operations.`,
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
    // Same order as the per-page alternates: summary, then index. See
    // `pages/[...slug]/index.md.ts`.
    "> Queen MQ documentation, for AI agents",
    `> Complete self-contained summary of Queen MQ: ${url("/llms-brief.txt")}`,
    "> Fetch that first when the question is about the product rather than about this page.",
    `> Index of all pages: ${url("/llms.txt")}`,
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
