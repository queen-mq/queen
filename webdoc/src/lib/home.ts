/**
 * The landing page's words, in one place.
 *
 * `src/pages/index.astro` lays them out and `src/pages/index.md.ts` writes them
 * as markdown for agents and `llms-full.txt`. Both import this module, so the
 * page and its markdown twin cannot say different things; there is nothing to
 * transcribe twice. `scripts/check-markdown.mjs` still reads the built page
 * and checks that every element marked `data-md` reached `dist/index.md`.
 *
 * Voice: plain and short, the same text as the README's top half. Every number
 * with its conditions. No em dashes (scripts/check-prose.mjs reads this file).
 */

/** The image every copy-paste command on the page runs. One place to bump it. */
export const IMAGE = "ghcr.io/queen-mq/queen:2.7.0";

/**
 * The `<title>`. NimbusHead appends " | Queen MQ" because this differs from
 * the site title, so the brand stays out of the string. It leads with the
 * words a reader searches for ("message broker"). 44 characters: with the
 * suffix it stays under the 65 that scripts/check-seo.mjs allows.
 */
export const HOME_TITLE = "Message broker with one partition per entity";

/** One line describing the page, for the index and corpus rows that list it. */
export const HOME_SUMMARY =
  "The landing page: partitions for ordering instead of scaling, then a five-minute guide in " +
  "JavaScript, Python, Go, Rust, PHP and C++ (run a node, configure a queue, push to a partition, " +
  "consume, commit one step as one transaction, replay from a time and fetch by offset, run a job " +
  "once under a lock, aggregate with a stream, request/reply over an ephemeral queue), the " +
  "measured numbers with their conditions, and where to go next.";

export const hero = {
  eyebrow: "A new paradigm for message brokers",
  headline: "Partitions are for ordering.",
  /** Second line of the `<h1>`, drawn dimmer. */
  subline: "Not for scaling.",
  lead:
    "Queen is an open-source distributed message broker, able to sustain millions of messages " +
    "per second. Every customer, order or conversation gets its own ordered partition. Every " +
    "step a worker takes is saved as one commit, or not at all.",
  second: "One binary, no database, and Kafka clients work with it out of the box.",
  primary: { label: "Run it in five minutes", href: "#guide" },
  secondary: { label: "See the model", href: "/introduction/model/" },
  run: `docker run -p 6632:6632 ${IMAGE}`,
  runNote: "Then open localhost:6632: the dashboard is served by the same process.",
};

/**
 * The five-minute guide that follows the hero. The words are here; the code of
 * each step comes from the programs that run it (see `src/lib/home-guide.ts`),
 * one per language, picked with the guide's language selector.
 */
export const guideSection = {
  eyebrow: "Five-minute guide",
  title: "Queen in five minutes",
  lead: [
    "Queen was born at Smartness to run queues with a FIFO partition per entity, created on the " +
      "fly. In a year it grew into a distributed broker with transactions, a KV store, timers, " +
      "streams and in-memory queues.",
    "This is what you can do with it, written by its lead maintainer. Every snippet comes from a " +
      "program that runs all the steps against the node from step 1 and checks each one.",
  ],
  pick: "Language",
  programLabel: "The program behind these snippets",
};

/** One code block of a step: which region to show, and its caption. */
export interface GuideCode {
  id: string;
  caption: string;
}

export interface GuideStep {
  id: string;
  title: string;
  /** Paragraphs before the first code block. */
  body: string[];
  code: GuideCode[];
  /** Paragraphs shown after a code block, keyed by its index. */
  between?: Record<number, string>;
  links: { label: string; href: string }[];
}

export const guideRun = {
  title: "Run a node",
  body: [
    "One binary serves the HTTP API and the dashboard on one port. Open localhost:6632 and watch " +
      "every step below land.",
  ],
  installNote: "Then add the client:",
};

export const guideSteps: GuideStep[] = [
  {
    id: "configure",
    title: "Configure a queue",
    body: [
      "Optional: the first push creates a queue with the defaults. Configure it when you want " +
        "other settings.",
    ],
    code: [{ id: "configure", caption: "Configure" }],
    links: [{ label: "Every queue option", href: "/writing-applications/queues/" }],
  },
  {
    id: "push",
    title: "Push to a partition",
    body: [
      "In Kafka you choose a number of partitions and hash keys onto them, so a slow customer " +
        "holds up every customer that shares its partition. Here a partition is one entity: a " +
        "customer, an order, a conversation. The push that names it creates it, order holds " +
        "inside it, and one queue has held ten million.",
      "The transactionId makes a retry safe: the same id within the dedup window (an hour by " +
        "default) is not written twice.",
    ],
    code: [{ id: "push", caption: "Push" }],
    links: [
      { label: "Partitions", href: "/writing-applications/queues/" },
      { label: "Deduplication", href: "/writing-applications/push/" },
    ],
  },
  {
    id: "consume",
    title: "Consume",
    body: [
      "Without a consumer group, each message goes to one worker, and the broker sizes every pop " +
        "for you.",
    ],
    code: [
      { id: "consume", caption: "Consume" },
      { id: "consume-group", caption: "Consume with a group, every option set" },
    ],
    between: {
      0:
        "With a consumer group, every group gets every message and the workers of one group share " +
        "them. A new group starts at the tail, so this one asks for everything already in the " +
        "queue. Here every option is set by hand, and the handler acks.",
      1:
        "Ack failed and the message comes back, up to the queue's retryLimit; after the last retry " +
        "it goes to the dead-letter queue.",
    },
    links: [{ label: "Groups, leases and acks", href: "/writing-applications/pop/" }],
  },
  {
    id: "transaction",
    title: "One step, one commit",
    body: [
      "The ack, the order's new state, the next event and tomorrow's reminder commit as one log " +
        "entry, or none of them does. That one call replaces the outbox table, the idempotency " +
        "table and the cron job.",
      "Then open the dashboard: its KV page holds the order's new state, and its Timers page " +
        "holds tomorrow's reminder.",
    ],
    code: [{ id: "transaction", caption: "Ack, KV, push and timer in one transaction" }],
    links: [
      { label: "Transactions", href: "/writing-applications/transactions/" },
      { label: "KV", href: "/writing-applications/kv-locks-timers/" },
      { label: "Timers", href: "/writing-applications/kv-locks-timers/" },
    ],
  },
  {
    id: "replay",
    title: "Read it again",
    body: [
      "Consuming does not delete: the queue is a log, kept for its retention. A new group can " +
        "start from a point in time and read the same orders again, while the groups that already " +
        "read them stay where they are.",
    ],
    code: [
      { id: "replay", caption: "A new group, from a time" },
      { id: "fetch", caption: "Fetch a partition by offset" },
    ],
    between: {
      0:
        "Or read a partition by offset with fetch: no group, no lease, no ack. It suits an audit, " +
        "rebuilding one entity, or a sink that keeps its own position.",
    },
    links: [
      { label: "Where a new group starts", href: "/writing-applications/pop/" },
      { label: "Reads by offset", href: "/writing-applications/clients/" },
    ],
  },
  {
    id: "lock",
    title: "Run a job once",
    body: [
      "Some work no message triggers: a nightly report, a cleanup, a singleton. Take a lock for " +
        "it. A lock is a lease, and a slow holder can outlive it, so guard the transaction with " +
        "it: the push commits only while the lock is still yours.",
    ],
    code: [{ id: "lock", caption: "A guarded job" }],
    links: [{ label: "Locks and semaphores", href: "/writing-applications/kv-locks-timers/" }],
  },
  {
    id: "stream",
    title: "Aggregate with a stream",
    body: [
      "A stream reads a queue and writes its results to another. The partition is the key, so " +
        "each customer gets its own windows. Like a new group it starts at the tail: push a few " +
        "more orders and a result arrives a minute later.",
    ],
    code: [{ id: "stream", caption: "Revenue per customer per minute" }],
    links: [{ label: "Streams", href: "/writing-applications/streams/" }],
  },
  {
    id: "ephemeral",
    title: "Ask and answer",
    body: [
      "Ephemeral queues live in memory, outside the log, for request/reply. The worker answers on " +
        "whatever inbox a request names; the caller makes a private inbox and waits on it. There " +
        "is no transaction or replay here, and a crash loses what is in flight.",
    ],
    code: [
      { id: "worker", caption: "Worker" },
      { id: "caller", caption: "Caller" },
    ],
    links: [{ label: "Ephemeral queues", href: "/writing-applications/ephemeral/" }],
  },
];

export const guideClosing =
  "Push, consume, ack, replay, fetch, transactions, KV, timers, locks, streams and ephemeral " +
  "queues all combine. Build on them and Queen carries the logic you would otherwise write " +
  "around a broker.";

export const dashboardShot = {
  image: "dashboard-overview",
  caption: "The dashboard, served on the same port as the API.",
  alt:
    "The dashboard's overview of a broker under load: messages pushed and consumed per second, " +
    "queues, partitions and consumer groups, the backlog and its trend, the partitions drawn as a " +
    "sunflower, and tiles for throughput, lag and errors.",
};

export const proofSection = {
  eyebrow: "Benchmarks",
  title: "Numbers",
  lead:
    "Three GCP nodes, 24 vCPU each, local NVMe. A write is confirmed only after two nodes have " +
    "written it to disk (fsync). Kafka, Pulsar and Redpanda ran on the same machines.",
  link: { label: "Full details", href: "/benchmarks/" },
};

/** The numbers strip. Each figure, unit and body reaches dist/index.md (checked). */
export const proof: { figure: string; unit: string; body: string; href?: string }[] = [
  {
    figure: "3M msg/s",
    unit: "in and out of one queue",
    body: "From 10 to 10M partitions, p99 15-25 ms. Kafka and Redpanda could not keep up at 50,000 partitions, Pulsar at 100,000.",
    href: "/benchmarks/queen-vs-kafka/",
  },
  {
    figure: "1M msg/s",
    unit: "of 1 KB messages",
    body: "About 1 GB/s, p99 16-22 ms.",
    href: "/benchmarks/queen-vs-kafka/",
  },
  {
    figure: "5M msg/s",
    unit: "on one queue at most",
    body: "One queue of 1,000 partitions.",
    href: "/benchmarks/queen-vs-kafka/",
  },
  {
    figure: "100 GB",
    unit: "of lag drained in 32 s",
    body: "3.4 GB/s, 104.7M messages of 1 KB.",
  },
  {
    figure: "4 ms",
    unit: "p99 per transaction",
    body: "10 messages each, at 6,000 transactions per second.",
    href: "/benchmarks/transactions/",
  },
];

export const provenSection = {
  eyebrow: "Trust",
  title: "Proven",
};

export const proven: { title: string; body: string; href: string }[] = [
  { title: "In production", body: "At Smartness, where Queen is built.", href: "/introduction/about/" },
  {
    title: "257 Jepsen tests",
    body: "On 2.6, all passed. No acknowledged message lost, no lease held twice.",
    href: "/benchmarks/jepsen/",
  },
  { title: "Rust, Apache-2.0", body: "Written in Rust. Open source under Apache-2.0.", href: "https://github.com/queen-mq/queen" },
];

export const nextSection = {
  eyebrow: "Next",
  title: "Where to go from here",
  mcpNote: "Using a coding agent? Add the MCP server:",
  mcp: "claude mcp add --transport http queen https://queenmq.com/mcp",
};

/** The book, shown above the cards of the last section (public/book/, book/build.sh --publish). */
export const book = {
  title: "The little book of QueenMQ",
  body:
    "The whole story in one free PDF, written by the authors of QueenMQ: what it is and why we " +
    "built it, how to build with it, how to run it, and how it works inside.",
  href: "/book/",
};

export const start: { title: string; body: string; href: string }[] = [
  { title: "Write applications", body: "Every feature of the client, one page each, in six languages.", href: "/writing-applications/" },
  { title: "Pick a client", body: "Six SDKs, curl, queenctl and the broker embedded in Rust.", href: "/writing-applications/clients/" },
  { title: "The model", body: "Partitions, leases, transactions and a plain HTTP interface.", href: "/introduction/model/" },
  { title: "Patterns", body: "Work queue, pub/sub, request/reply, workflows, rate limits and more.", href: "/patterns/" },
  { title: "Kafka, PostgreSQL, S3", body: "Kafka protocol, PostgreSQL in and out, S3 export, all inside the broker.", href: "/integrations/" },
  { title: "Compare", body: "Against Kafka, RabbitMQ, SQS and Temporal, and when to pick them.", href: "/introduction/compare/" },
];

/** The last line of the page. */
export const closing = "Nothing happens halfway.";
