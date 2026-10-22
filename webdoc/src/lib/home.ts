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
export const IMAGE = "ghcr.io/queen-mq/queen:2.6.0";

/**
 * The `<title>`. NimbusHead appends " | Queen MQ" because this differs from
 * the site title, so the brand stays out of the string. It leads with the
 * words a reader searches for ("message broker"). 44 characters: with the
 * suffix it stays under the 65 that scripts/check-seo.mjs allows.
 */
export const HOME_TITLE = "Message broker with one partition per entity";

/** One line describing the page, for the index and corpus rows that list it. */
export const HOME_SUMMARY =
  "The landing page: partitions for ordering instead of scaling, one partition per entity, one " +
  "commit per step, a push and a consumer in code, what you can remove, what is included, the " +
  "measured numbers with their conditions, and how to try it.";

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
  primary: { label: "Run it in five minutes", href: "/start/quickstart/" },
  secondary: { label: "See the model", href: "/concepts/" },
  run: `docker run -p 6632:6632 ${IMAGE}`,
  runNote: "Then open localhost:6632: the dashboard is served by the same process.",
};

/** The old way against the Queen way. */
export const paradigm = {
  eyebrow: "A new paradigm",
  oldTitle: "The old way",
  old: [
    "In Kafka, Pulsar or Redpanda, partitions are how you scale. You pick a number, say 64, and " +
      "hash your keys onto it. Order is a side effect.",
    "So one slow customer blocks every other customer on the same partition. When you need more " +
      "partitions, keys move, and ordering is lost during the change.",
    "The work is spread out too. To handle one event you ack it on the broker, update the " +
      "database, write to an outbox, schedule a job and save an idempotency key. Four systems, " +
      "four commits. A crash in between leaves the step half done.",
  ],
  newTitle: "The Queen way",
};

export const shifts: { title: string; body: string; href: string }[] = [
  {
    title: "One partition per entity.",
    body:
      "Push to customer-123 and the partition exists. No count to choose. A partition uses about " +
      "1 KB of memory, so one queue can hold ten million. A slow customer only slows itself down.",
    href: "/concepts/partitions/",
  },
  {
    title: "One commit per step.",
    body:
      "In one call, a worker acks the event, saves state, pushes the next events and sets a " +
      "timer. Queen writes it all as one log entry. If any part fails, nothing is written. A " +
      "retry with the same id is a no-op.",
    href: "/concepts/transactions/",
  },
  {
    title: "The queue is a log.",
    body:
      "Messages stay after they are processed. You get leases, retries and a dead-letter queue, " +
      "plus replay, fan-out to many groups and seek by time.",
    href: "/concepts/consuming/",
  },
];

/**
 * A push, then a consumer whose every step is one transaction. The same code
 * as the README's "In code": edit them together. `.each()` hands the handler
 * one message, `autoAck(false)` leaves the ack to the transaction, and the ack
 * names its group so the line reads the same over HTTP.
 */
export const step = `// Push: the first message for a customer creates its partition
await queen.queue('orders').partition(customerId)
  .push([{ transactionId: \`order-\${orderId}\`, data: { orderId, customerId } }])

// Consume: each step is one commit
await queen.queue('orders').group('billing').autoAck(false).each()
  .consume(async (message) => {
    const { orderId, customerId } = message.data
    await queen.transaction()
      .ack(message, 'completed', { consumerGroup: 'billing' })
      .kv.put('orders', \`order-\${orderId}\`, { status: 'paid' })
      .queue('receipts').partition(customerId)
        .push([{ transactionId: \`receipt-\${orderId}\`, data: { orderId } }])
      .timer('reminders').key(\`order-\${orderId}\`).delay('24h')
        .payload({ orderId }).schedule()
      .commit()
  })`;

export const stepSection = {
  eyebrow: "In code",
  title: "One call, one log entry",
  lead:
    "The ack, the new state, the receipt and the reminder are one log entry. Over HTTP it is " +
    "POST /api/v1/transaction.",
  link: { label: "How a transaction commits", href: "/concepts/transactions/" },
};

/** The five roles of a step, in the order the transaction above performs them. */
export const roles: { role: string; primitive: string; href: string }[] = [
  { role: "Progress", primitive: "the ack, fenced by the lease", href: "/concepts/consuming/" },
  { role: "State", primitive: "a KV write", href: "/concepts/kv/" },
  { role: "Events", primitive: "a push into the customer's partition", href: "/concepts/partitions/" },
  { role: "Identity", primitive: "a transactionId the retry reuses", href: "/concepts/dedup/" },
  { role: "Time", primitive: "a timer that fires as a push", href: "/concepts/timers/" },
];

export const removeSection = {
  eyebrow: "Less to run",
  title: "What you can remove",
};

export const removals: { title: string; body: string }[] = [
  { title: "The outbox table", body: "The next event is pushed in the same commit." },
  { title: "The idempotency table", body: "A transactionId makes a retry a no-op." },
  { title: "The cron job", body: "Timers fire as messages in the entity's partition." },
  { title: "Partition planning", body: "Partitions create themselves." },
  { title: "Consumer rebalances", body: "Workers just take the next entity that has work." },
];

export const includedSection = {
  eyebrow: "In the same binary",
  title: "Also included",
};

export const included: { title: string; body: string; href: string }[] = [
  { title: "State and time", body: "KV store and timers, in the same commit.", href: "/concepts/kv/" },
  {
    title: "Streams and live queues",
    body: "Stream windows and in-memory queues for request/reply.",
    href: "/guides/streams/",
  },
  {
    title: "Kafka, PostgreSQL, S3",
    body: "Kafka protocol, PostgreSQL in and out, S3 export, all inside the broker.",
    href: "/guides/kafka/",
  },
  {
    title: "Any client",
    body: "HTTP API, SDKs for JS, Python, Go, Rust, C++ and PHP (with Laravel), an MCP server for coding agents.",
    href: "/start/clients/",
  },
  {
    title: "Ready to run",
    body: "Dashboard, Prometheus metrics, auth, encryption, multi-tenant proxy, Raft on 3 or 5 nodes.",
    href: "/operate/",
  },
];

export const dashboardShot = {
  image: "dashboard-overview",
  caption: "The dashboard, served on the same port.",
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
  { title: "In production", body: "At Smartness, where Queen is built.", href: "/start/about/" },
  {
    title: "257 Jepsen tests",
    body: "On 2.6, all passed. No acknowledged message lost, no lease held twice.",
    href: "/benchmarks/jepsen/",
  },
  { title: "Rust, Apache-2.0", body: "Written in Rust. Open source under Apache-2.0.", href: "https://github.com/queen-mq/queen" },
];

export const trySection = {
  eyebrow: "Start",
  title: "Try it",
  pushNote: "Push your first message:",
  push: `curl -X POST http://localhost:6632/api/v1/push -H 'content-type: application/json' -d '{
  "items": [{ "queue": "orders", "partition": "customer-123",
              "transactionId": "order-8891-created",
              "payload": { "orderId": 8891 } }]
}'`,
  after: "Open localhost:6632 to see it in the dashboard.",
  mcpNote: "Using a coding agent? Add the MCP server:",
  mcp: "claude mcp add --transport http queen https://queenmq.com/mcp",
};

export const start: { title: string; body: string; href: string }[] = [
  { title: "Quickstart", body: "Run a node and commit your first step with curl.", href: "/start/quickstart/" },
  { title: "Pick a client", body: "Six SDKs, curl, queenctl and the broker embedded in Rust.", href: "/start/clients/" },
  { title: "Examples", body: "Whole programs: a chat backend, a saga, webhooks, rate limits.", href: "/examples/" },
  { title: "Compare", body: "Against Kafka, RabbitMQ, SQS and Temporal, and when to pick them.", href: "/start/compare/" },
];

/** The last line of the page. */
export const closing = "Nothing happens halfway.";
