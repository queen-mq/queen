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
 * import. Until that exists, the copy below is a second transcription of the
 * arrays and the prose in `src/pages/index.astro`. That is a drift risk, and it
 * is held closed by `scripts/check-markdown.mjs`: it parses `index.astro` and
 * fails the build when the headline, the hero paragraph, a differentiator, a
 * proof figure or the limits paragraph in the page is missing from
 * `dist/index.md`. Edit the page and this file together, or the check will
 * tell you which one you forgot.
 *
 * The produce and consume samples cannot drift: this file lifts them out of
 * the same generated snippet partials the page imports. The anatomy-of-a-step
 * sample is inline in three places (this file, `index.astro` and
 * `start/index.mdx`): edit all three together.
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
export const HOME_HEADLINE = "A message queue is the smallest thing Queen MQ does.";

/**
 * One line describing the page, for the index and corpus rows that list it.
 * Not `config.description`: those rows sit directly under the site
 * description, and repeating it there says nothing about this page.
 */
export const HOME_SUMMARY =
  "The landing page: what Queen MQ is, the step that commits as one entry, one partition " +
  "per entity, what runs it, and the measured numbers with the conditions they were " +
  "measured under.";

/** The eyebrow above the headline. */
const HOME_EYEBROW = "Queen MQ · Apache 2.0";

/** The hero paragraph, with its JSX line wrapping collapsed. */
const HOME_LEAD =
  "It's a transactional event broker. Every customer, order or conversation gets its own " +
  "ordered partition, created by the first message that names it. No partition count to " +
  "choose. The ack, the state change, the next events and the timer commit as one entry. " +
  "Nothing happens halfway.";

/** The line of capabilities under the calls to action. */
const HOME_FEATURES = [
  "One partition per entity",
  "One-entry transactions",
  "Consumer groups",
  "Replay and seek",
  "Dead-letter queue",
  "transactionId dedup",
  "KV",
  "Timers",
  "Streams",
  "Ephemeral queues",
  "Tenants and quotas",
  "Kafka wire protocol",
  "Raft replication",
];

/** The anatomy of a step, verbatim from the page and from start/index.mdx. */
const HOME_STEP = `await queen.transaction()
  .ack(message)                                                    // the event is done
  .kv.put('orders', orderId, { status: 'paid' }, { ttl: '30d' })   // state
  .queue('receipts').partition(customerId)
    .push([{ transactionId: \`receipt-\${orderId}\`, data: receipt }]) // the next event, once
  .timer('reminders').key(orderId).delay('24h')
    .payload({ orderId }).schedule()                               // what comes later
  .commit()`;

/** The five roles of a step, plus the call that commits them. */
const roles = [
  { role: "Events", primitive: "Queues, one partition per entity", href: "/concepts/partitions/" },
  {
    role: "Progress",
    primitive: "Consumer groups, leases, acks, retries, dead letters, replay",
    href: "/concepts/consuming/",
  },
  { role: "State", primitive: "KV", href: "/concepts/kv/" },
  { role: "Time", primitive: "Timers and delayed delivery", href: "/concepts/timers/" },
  { role: "Identity", primitive: "transactionId dedup and once", href: "/concepts/dedup/" },
  { role: "The commit", primitive: "POST /api/v1/transaction", href: "/concepts/transactions/" },
];

/** "What runs it", transcribed from the page's `differentiators`. */
const differentiators = [
  {
    title: "One binary",
    body: "A node keeps its state in a replicated log on its own disk. No database, no ZooKeeper, no sidecars. The dashboard, Prometheus metrics, API keys, JWT, quotas and payload encryption ship in the same binary.",
    href: "/operate/",
  },
  {
    title: "One node, or a cluster of three or five",
    body: "Raft replicates every entry, and the cluster keeps serving while a majority of its nodes is up. Any node serves any client: a follower forwards writes to the leader, and a leader that stops gracefully hands leadership over first.",
    href: "/operate/cluster/",
  },
  {
    title: "Tenants in raft groups",
    body: "Each tenant lives in exactly one raft group, so a transaction is one entry with no coordinator and no two-phase commit. You scale out with more raft groups and more clusters, not by spreading one tenant's writes.",
    href: "/internals/",
  },
  {
    title: "Any client",
    body: "HTTP with JSON bodies, so curl is a client. Six SDKs: JavaScript, Python, Go, Rust, C++, and PHP with Laravel. A Rust program can also run the broker inside its own process.",
    href: "/start/clients/",
  },
  {
    title: "Kafka clients, unchanged",
    body: "A Kafka wire-protocol facade runs inside the same process, off until you set QUEEN_KAFKA_EMBEDDED=true. Existing producers and consumers connect to it on port 9092.",
    href: "/guides/kafka/",
  },
];

/** The dashboard section, and the screenshot's alt text with it. */
const HOME_DASHBOARD =
  "Queues, partitions, consumer groups, lag and dead letters. Nothing was installed to get " +
  "this: it is the same binary, on the port you already opened.";

const HOME_DASHBOARD_IMAGE =
  "The bundled dashboard's overview: stored messages, queues, partitions, consumer groups, " +
  "pending and completed counts above a table of throughput, lag and error series with " +
  "sparklines.";

const proof = [
  {
    figure: "1,000,000",
    unit: "msg/s pushed and consumed",
    body: "One queue, 500,000 partitions, deduplication on, e2e p99 ~270 ms, in 90 s runs on three nodes with 16 vCPU each, every write replicated.",
    href: "/benchmarks/",
  },
  {
    figure: "10,000,000",
    unit: "partitions in one queue",
    body: "913,000 msg/s in and 905,000 out on the same three nodes, e2e p99 4.7 s, in a 60 s run. No partition count was chosen: each partition was created by a push.",
    href: "/benchmarks/",
  },
  {
    figure: "Jepsen",
    unit: "tested",
    body: "No acked message lost or duplicated, and one order per partition, through kill -9, network partitions, clock jumps and power loss on five nodes. Testing, not proof.",
    href: "/concepts/guarantees/",
  },
];

/**
 * The limits paragraph. `scripts/check-markdown.mjs` reads it out of the
 * page's Limits section and looks for it here.
 */
const HOME_LIMITS =
  "Queen has real limits. Delivery is at-least-once: exactly-once holds for effects inside " +
  "Queen, not for a call your worker makes to another system. A hot partition is " +
  "sequential by design. A transaction is one call inside one tenant, with no interactive " +
  "BEGIN and COMMIT. One leader orders every write of a raft group. And there is no " +
  "routing: no exchanges, bindings or header matching.";

/**
 * The landing page as markdown, from the eyebrow down. The `# ` headline is
 * left to the caller so this block can be dropped into `llms-full.txt`, whose
 * collation gives every page its own `#` heading.
 */
export function homepageBody(): string {
  const lines: string[] = [HOME_EYEBROW, "", HOME_LEAD, ""];

  lines.push(HOME_FEATURES.map((f) => `**${f}**`).join(" · "), "");
  lines.push(
    `[Run it in five minutes](${url("/start/quickstart/")}) · ` +
      `[Pick a client](${url("/start/clients/")}) · ` +
      `[The model, in one page](${url("/concepts/")}) · ` +
      `[Compared to Kafka, SQS and Temporal](${url("/start/compare/")})`,
    "",
  );

  lines.push("## One step, one commit", "");
  lines.push(
    "When a service handles an event, it takes the event, changes state, emits the next " +
      "events, schedules what comes later and makes a retry harmless. In most stacks those " +
      "live in four systems, and each commits on its own. Queen keeps all five in one " +
      "replicated log, so one call writes them as one entry.",
    "",
    "```js",
    HOME_STEP,
    "```",
    "",
  );
  for (const r of roles) lines.push(`- **[${r.role}](${url(r.href)})**: ${r.primitive}`);
  lines.push(
    "",
    "If the lease has expired, if this receipt was already pushed by an earlier try, or if any " +
      "part is refused, nothing in the call is written. The outbox table, the idempotency table and the cron " +
      "job that exist only to stitch separate commits together are not needed. Atomicity " +
      "covers state inside Queen: a call your worker makes to another system is outside any " +
      `commit. [Transactions](${url("/concepts/transactions/")})`,
    "",
  );

  lines.push("## One partition per entity", "");
  lines.push(
    "Most brokers order messages per *shard*: a fixed number of partitions, chosen up front, " +
      "with your entities hashed onto them. Entities that share a shard wait for each other.",
    "",
    "In Queen the partition is the entity. You name it on push (`customer-123`), and it " +
      "exists from that message on. Order holds inside it, and there is no partition count " +
      "to choose.",
    "",
    "**A slow entity delays only itself.**",
    "",
    "```text",
    "customer A  ──► A1 ──► A2 ──► A3   in order",
    "customer B  ──► B1 ──► B2          waits for no one",
    "customer C  ──► C1 ──► C2 ──► C3   waits for no one",
    "```",
    "",
    "A hot partition stays sequential, by design: parallelism comes from many partitions, " +
      `not from splitting one. [Partitions](${url("/concepts/partitions/")})`,
    "",
    "With the one-entry commit, every entity can run as a small state machine: its partition " +
      "is the input, its KV entry is the state, each transaction is one transition, and the " +
      `workers stay stateless. [One state machine per entity](${url("/guides/state-machines/")})`,
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
    `There are [SDKs for JavaScript, Python, Go, Rust, C++ and PHP](${url("/start/clients/")}), ` +
      `[an operator CLI](${url("/reference/queenctl/")}), and a ` +
      `[plain HTTP API](${url("/reference/http/")}) that curl speaks.`,
    "",
  );

  lines.push("## What runs it", "");
  for (const item of differentiators) {
    lines.push(`### ${item.title}`, "", item.body, "", `[More](${url(item.href)})`, "");
  }

  lines.push("## The dashboard is already in there", "", HOME_DASHBOARD, "");
  lines.push(HOME_DASHBOARD_IMAGE, "");
  lines.push(`[Monitoring](${url("/operate/monitoring/")})`, "");

  lines.push("## Measured, with the conditions attached", "");
  lines.push(
    "Queen MQ 2.0 on three nodes, 16 vCPU each, every write replicated. These runs measure " +
      "push, pop and ack, not transactions. Every figure names its run, and 1.x results, " +
      "measured when Queen's storage was PostgreSQL, are not repeated here.",
    "",
  );
  for (const item of proof) {
    lines.push(`### ${item.figure} ${item.unit}`, "", item.body, "", `[Conditions](${url(item.href)})`, "");
  }

  lines.push("## The limits worth knowing first", "");
  lines.push(HOME_LIMITS, "");
  lines.push(`[Read the full list before you design around it](${url("/reference/limits/")})`, "");

  lines.push("## Start", "");
  lines.push(
    `- [Quickstart](${url("/start/quickstart/")}): run a node and commit your first step in five minutes.`,
    `- [Pick a client](${url("/start/clients/")}): six SDKs, curl, queenctl and the embedded Rust broker.`,
    `- [The model](${url("/concepts/")}): partitions, progress, state, time and identity, and the commit that joins them.`,
    `- [Run it](${url("/operate/")}): a node, a cluster of three or five, Kubernetes, tenants and monitoring.`,
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
