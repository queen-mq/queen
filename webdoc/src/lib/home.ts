/**
 * The landing page's words, in one place.
 *
 * `src/pages/index.astro` lays them out and `src/pages/index.md.ts` writes them
 * as markdown for agents and `llms-full.txt`. Both import this module, so the
 * page and its markdown twin cannot say different things; there is nothing to
 * transcribe twice. `scripts/check-markdown.mjs` still reads the built page
 * and checks that every element marked `data-md` reached `dist/index.md`.
 *
 * Voice: the docs brief. Specific over grand, every number with its
 * conditions, limits stated once. No em dashes (scripts/check-prose.mjs reads
 * this file).
 */

/** The image every copy-paste command on the page runs. One place to bump it. */
export const IMAGE = "ghcr.io/queen-mq/queen:latest";

/**
 * The `<title>`. NimbusHead appends " | Queen MQ" because this differs from
 * the site title, so the brand stays out of the string. It leads with the
 * words a reader searches for ("message broker"); the page itself says "event
 * broker" too, so both reach the index. 54 characters: with the suffix it is
 * the 65 that scripts/check-seo.mjs allows before results cut it.
 */
export const HOME_TITLE = "Transactional message broker with per-entity ordering";

/** One line describing the page, for the index and corpus rows that list it. */
export const HOME_SUMMARY =
  "The landing page: what Queen MQ is, the step that commits as one entry, one partition per " +
  "entity, the primitives that share the commit, the Kafka, PostgreSQL and S3 doors, the dashboard, " +
  "the measured numbers with their conditions, and how it runs.";

export const hero = {
  eyebrow: "Queen MQ 2.0 · open source, Apache 2.0",
  headline: "Nothing happens halfway.",
  /** Second line of the `<h1>`: what the product is, under the line that says why. */
  subline: "A transactional message broker, one ordered partition per entity.",
  lead:
    "Queen MQ is a transactional event broker. When a worker handles an event, the ack, the " +
    "state it changes, the events it emits and the timer it sets commit as one entry of a " +
    "replicated log, or not at all. On three nodes it carries 1M msg/s in and out of one queue " +
    "of 10M partitions.",
  second:
    "Every customer, order or conversation gets its own ordered partition, created by the first " +
    "message that names it. One binary with no database beside it: Kafka clients connect " +
    "unchanged, PostgreSQL tables stream in and out, and queues land in S3.",
  primary: { label: "Run it in five minutes", href: "/start/quickstart/" },
  secondary: { label: "See one step commit", href: "/concepts/" },
  run: `docker run -p 6632:6632 ${IMAGE}`,
  runNote: "Then open localhost:6632: the dashboard is served by the same process.",
};

/**
 * The anatomy of a step, the same seven lines as the Overview page
 * (src/content/docs/start/index.mdx): edit them together. The ack names its
 * consumer group so the line reads the same over HTTP (queen-mq 2.0 would take
 * it from the message), and the KV and timer keys are strings because the SDK
 * refuses anything else.
 */
export const step = `await queen.transaction()
  .ack(message, 'completed', { consumerGroup: 'billing' })                   // the event is done
  .kv.put('orders', \`order-\${orderId}\`, { status: 'paid' }, { ttl: '30d' })  // state
  .queue('receipts').partition(customerId)
    .push([{ transactionId: \`receipt-\${orderId}\`, data: receipt }])          // the next event, once
  .timer('reminders').key(\`order-\${orderId}\`).delay('24h')
    .payload({ orderId }).schedule()                                         // what comes later
  .commit()`;

export const stepSection = {
  eyebrow: "One step, one commit",
  title: "Handle an event in one call",
  lead:
    "Handling an event takes five things: take the event, change some state, emit what happens " +
    "next, schedule what comes later, and make sure a retry changes nothing. Most stacks spread " +
    "them over a broker, a database with an outbox table, a scheduler and an idempotency table. " +
    "That is four commits, and a crash between any two of them leaves the step half done. In " +
    "Queen it is one call and one log entry.",
  after:
    "If the worker's lease ran out, if an earlier try already pushed this receipt, or if any part " +
    "is refused, the broker writes nothing at all, so a retry is always safe. What stays outside " +
    "the commit is a call your worker makes to another system, which needs that system's own " +
    "idempotency key.",
  link: { label: "How a transaction commits", href: "/concepts/transactions/" },
};

/** The five roles of a step, in the order the code above performs them. */
export const roles: { role: string; primitive: string; href: string }[] = [
  { role: "Progress", primitive: "the ack, fenced by the lease", href: "/concepts/consuming/" },
  { role: "State", primitive: "a KV write with its lifetime", href: "/concepts/kv/" },
  { role: "Events", primitive: "a push into the customer's partition", href: "/concepts/partitions/" },
  { role: "Identity", primitive: "a transactionId the retry reuses", href: "/concepts/dedup/" },
  { role: "Time", primitive: "a timer that fires as a push", href: "/concepts/timers/" },
];

export const partitionSection = {
  eyebrow: "Partitions",
  title: "A partition for every customer, order and conversation",
  lead:
    "Most brokers hash your keys onto a partition count you chose up front, so one slow customer " +
    "holds up everyone who hashed next to it. In Queen the partition is the entity. Name it on " +
    "push and it exists, created in the same log entry as its first message. Order holds inside " +
    "it, a slow entity delays only itself, and a partition costs about 2 KB of the leader's memory.",
  after:
    "A busy partition stays sequential by design. Parallelism comes from the number of entities " +
    "with work to do, which is why a queue can hold millions of them.",
  link: { label: "Partitions and order", href: "/concepts/partitions/" },
};

/** The primitives, each one a role a step can play in the same commit. */
export const primitives: { title: string; body: string; href: string }[] = [
  {
    title: "Queues and partitions",
    body: "A partition per entity, created by its first message. Offsets without gaps, replay from any point in the retained history.",
    href: "/concepts/partitions/",
  },
  {
    title: "Consumer groups",
    body: "A cursor per group and partition. Leases keep each entity in order, and retries and the dead-letter queue are the broker's job.",
    href: "/concepts/consuming/",
  },
  {
    title: "Transactions",
    body: "Acks, pushes, KV writes and timers of one step in one call, written as one entry and fenced by the worker's lease.",
    href: "/concepts/transactions/",
  },
  {
    title: "KV",
    body: "Linearizable state beside your queues, with versions, compare-and-set and a lifetime on every write.",
    href: "/concepts/kv/",
  },
  {
    title: "Timers",
    body: "Schedule a message inside the step that wants it. It fires as an ordinary push, into the entity's own partition.",
    href: "/concepts/timers/",
  },
  {
    title: "Dedup and once",
    body: "A retried push with the same transactionId is stored once, and once makes a whole step run at most once.",
    href: "/concepts/dedup/",
  },
  {
    title: "Streams",
    body: "Windows and aggregates whose state commits with the ack of the events they counted, so a count never drifts.",
    href: "/guides/streams/",
  },
  {
    title: "Ephemeral queues",
    body: "In-memory partitions for presence and live updates, served by every node and handed over when a node stops.",
    href: "/guides/ephemeral/",
  },
  {
    title: "Tenants and API keys",
    body: "The proxy inside the binary adds API keys, a console, plans, quotas and metering, a tenant per cluster.",
    href: "/operate/tenants/",
  },
];

export const doorsSection = {
  eyebrow: "Integrations",
  title: "Kafka, PostgreSQL and S3, inside the broker",
  lead:
    "They run in the broker process and go through the same pipeline as everything else, so " +
    "there is no connector cluster to deploy and no second copy of your data to keep in step.",
  snippetTitle: "One call streams a table into a queue",
  snippet: `curl -X PUT localhost:6632/api/v1/connectors/orders-src \\
  -H 'content-type: application/json' -d '{
  "kind": "source",
  "connection": { "host": "pg", "database": "shop",
                  "user": "queen", "password": "secret" },
  "source": { "tables": [
    { "table": "public.orders", "queue": "orders" } ] }
}'`,
};

export const doors: { title: string; body: string; href: string }[] = [
  {
    title: "Kafka clients",
    body: "Point bootstrap.servers at Queen. Producers and consumer groups run unchanged on the same queues Queen's own clients read, and Kafka Streams and Kafka Connect have run against it as they are.",
    href: "/guides/kafka/",
  },
  {
    title: "PostgreSQL source",
    body: "Stream a table's committed changes into a queue through logical replication: a snapshot first, then every change in commit order, in a partition named after the row's key.",
    href: "/guides/postgres/",
  },
  {
    title: "PostgreSQL sink",
    body: "Write a queue into a table as appends, upserts, change events or your own SQL. The sink's progress commits in the same PostgreSQL transaction as the rows, so a crash never writes one twice.",
    href: "/guides/postgres/",
  },
  {
    title: "S3 sink",
    body: "Mirror queues into any S3-compatible bucket as JSONL or Parquet, in a Hive layout that DuckDB, Spark and ClickHouse read directly. A crash or a retried upload rewrites the same bytes under the same key, so every record lands once.",
    href: "/guides/s3/",
  },
  {
    title: "HTTP and six SDKs",
    body: "JSON over HTTP, so curl is a complete client. SDKs for JavaScript, Python, Go, Rust, C++ and PHP, with a Laravel queue driver and supervisor.",
    href: "/start/clients/",
  },
];

export const dashboardSection = {
  eyebrow: "Dashboard",
  title: "The dashboard is in the binary",
  lead:
    "Served on the port you already opened: queues and partitions, consumer groups and their lag, " +
    "dead letters with their errors, KV, timers and the state of the raft cluster. Light or dark, " +
    "like these pages.",
  link: { label: "What to watch in production", href: "/operate/monitoring/" },
  image: "dashboard-overview",
  alt:
    "The dashboard's overview of a broker under load: messages pushed and consumed per second, " +
    "queues, partitions and consumer groups, the backlog and its trend, the partitions drawn as a " +
    "sunflower, and tiles for throughput, lag and errors.",
};

export const proofSection = {
  eyebrow: "Benchmarks",
  title: "Measured, with the conditions attached",
  lead:
    "Queen 2.0 on three 16-vCPU nodes, every write fsynced on two of them, beside Kafka, Redpanda " +
    "and Pulsar on the same machines. Every number names its run, including the places where " +
    "another system does better.",
  link: { label: "All the benchmarks", href: "/benchmarks/" },
};

/** The numbers strip. Each figure and body reaches dist/index.md (checked). */
export const proof: { figure: string; unit: string; body: string; href: string }[] = [
  {
    figure: "1,000,000",
    unit: "msg/s pushed and consumed",
    body: "One queue on three nodes at every partition count from 200 to 10,000,000, e2e p99 103 to 163 ms, deduplication off (2.0.0-beta.2, 60 s runs).",
    href: "/benchmarks/partitions/",
  },
  {
    figure: "10,000,000",
    unit: "partitions in one queue",
    body: "Each one created by a push, none declared. 1,000,000 msg/s in and out on the same three nodes, e2e p99 122 ms over the last 30 s.",
    href: "/benchmarks/partitions/",
  },
  {
    figure: "4 ms",
    unit: "commit p99, ten messages a step",
    body: "A transaction that acks its inputs and pushes its outputs, at 9,000 msg/s on three nodes: e2e p99 19 ms, every input found exactly once in the output.",
    href: "/benchmarks/transactions/",
  },
  {
    figure: "153",
    unit: "Jepsen tests on the code of 2.1.0",
    body: "Five nodes under kill -9, pauses, network partitions, clock jumps and power loss: 152 valid, and the other valid when run again with the test nodes' clocks set. No acknowledged message lost, no lease held twice. Testing, not proof.",
    href: "/benchmarks/jepsen/",
  },
];

export const runSection = {
  eyebrow: "Operations",
  title: "One binary, on one node or five",
};

/** "How it runs". Each title and body reaches dist/index.md (checked). */
export const differentiators: { title: string; body: string; href: string }[] = [
  {
    title: "One binary",
    body: "A node keeps everything in a log on its own disk, with no database, ZooKeeper or sidecar beside it. The same process serves the dashboard, Prometheus metrics and, when you turn them on, the Kafka port and the API-key proxy.",
    href: "/operate/",
  },
  {
    title: "Three or five nodes",
    body: "Raft writes every entry to a majority before you get an answer. Any node takes any request, followers forward writes to the leader, and a leader that stops gracefully hands over first.",
    href: "/operate/cluster/",
  },
  {
    title: "A tenant in one raft group",
    body: "That is what lets a transaction be a single entry, with no coordinator and no two-phase commit. The price is one leader per tenant's writes, so you scale out with more raft groups and clusters.",
    href: "/internals/",
  },
  {
    title: "Recovery, rehearsed",
    body: "Every recovery procedure in these docs was run against a broken cluster, most of them under load: a lost disk, a lost majority, a poisoned entry, a rolling restart gone wrong.",
    href: "/operate/recovery/",
  },
];

/** The limits paragraph: the full list is /reference/limits/. */
export const limits =
  "Queen has real limits. Delivery is at-least-once: exactly-once holds for effects inside Queen, " +
  "not for a call your worker makes to another system. A busy partition is sequential by design. " +
  "A transaction is one call inside one tenant, with no interactive BEGIN and COMMIT. One leader " +
  "orders every write of a raft group. And there is no routing: no exchanges, bindings or header " +
  "matching.";

export const start: { title: string; body: string; href: string }[] = [
  { title: "Quickstart", body: "Run a node and commit your first step with curl.", href: "/start/quickstart/" },
  { title: "Pick a client", body: "Six SDKs, curl, queenctl and the broker embedded in Rust.", href: "/start/clients/" },
  { title: "Examples", body: "Whole programs: a chat backend, a saga, webhooks, rate limits.", href: "/examples/" },
  { title: "Compare", body: "Against Kafka, RabbitMQ, SQS and Temporal, and when to pick them.", href: "/start/compare/" },
];
