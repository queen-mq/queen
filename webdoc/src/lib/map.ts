/**
 * The words of the map page (/map/): every core of QueenMQ as a yellow box, every
 * option or feature of a core as a dot, and the goals ("I want to...") that walk
 * through a pattern's pieces in the order you use them.
 *
 * One source for two surfaces: the drawing with its side panel and goals
 * (src/components/QueenMap.astro), and the page's markdown twin
 * (src/lib/markdown-partials.ts).
 *
 * Nothing here is new: each entry restates its docs page and links to it. A
 * default is copied from that page's option table; the code is a step of the
 * programs that run (`code`, cut by src/lib/book-code.ts) or, for a feature those
 * programs do not cover yet, the page's own example (`snippet`).
 * `scripts/check-map.mjs` fails the build when a `docs` link points at a page or
 * a heading that does not exist.
 *
 * Text may hold `inline code` in backticks; the component renders it.
 */

export type CoreId = "push" | "queue" | "connectors" | "pop" | "stream" | "transaction" | "kv" | "timers" | "ephemeral";

export interface MapSetting {
  /** The option, call or variable, as you type it. */
  name: string;
  /** Its default, or what to give it. */
  value: string;
}

export interface MapEntry {
  /** The URL fragment, /map/#<id>. Unique across cores and dots. */
  id: string;
  /** The words on the map. */
  label: string;
  /** What it is, in one or two sentences. */
  line: string;
  /** The settings, each with its default. */
  settings?: MapSetting[];
  /** The heading of the settings' first column ("Setting" when absent). */
  settingsName?: string;
  /** The heading of the settings' second column ("Default" when absent). */
  settingsHead?: string;
  /** When to use it. */
  use: string;
  /** A step of the programs under examples/tutorials/ (see src/lib/book-code.ts). */
  code?: string;
  /** The docs page's own example, when no program step covers the feature yet. */
  snippet?: { lang: string; code: string };
  /** The docs section that says it in full. */
  docs: string;
  /** The release that brought it, when that is the coming one. */
  since?: string;
  /**
   * What other brokers call it, by broker ("Also called" for a pattern's name):
   * the find field matches these words, and the overlay lists them.
   */
  aka?: Record<string, string[]>;
}

export interface MapCore extends MapEntry {
  id: CoreId;
  items: MapEntry[];
}

export interface MapStep {
  /** The core or dot this step uses: lit, and numbered on the map. */
  id: string;
  /** What you do there, in one line. */
  text: string;
  /**
   * Number it on the transaction's "ack" line instead of on the dot: inside a
   * transaction the ack is that line, and Pop's list is under the overlay.
   */
  onAckLine?: true;
}

export interface MapGoal {
  id: string;
  label: string;
  /** One line above the steps. */
  intro: string;
  /** The pieces in the order you use them. */
  steps: MapStep[];
  /** The pattern or integration page. */
  href: string;
}

export const MAP_TITLE = "Everything QueenMQ does, on one map";

export const intro = {
  goalsLabel: "I want to",
};

const ROUTING_HEADERS = `await queen.queue('orders').partition('order-9137').push([{
  transactionId: 'order-9137-paid',
  data: { orderId: 9137, total: 120 },
  headers: { type: 'order.paid', traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' },
}])`;

const ROUTING_FILTERED = `await queen.queue('order-events')
  .group('billing')
  .where('data.kind', 'in', ['order.paid', 'order.refunded'])
  .consume(handler)`;

const ROUTING_READING = `await queen.queue('orders-it').config({
  from: 'orders',
  where: "data.country = 'IT'",
  partitionBy: 'same',
  leaseTime: 30,
  retryLimit: 5,
}).create()`;

const ROUTING_LINK = `await queen.transaction()
  .ack(message)
  .queue('manual-review').partition(message.partition)
  .link([message], { headers: { 'review-reason': 'amount' } })
  .commit()`;

const PG_SOURCE = `curl -s -X PUT localhost:6632/api/v1/connectors/orders-src \\
  -H 'content-type: application/json' -d '{
  "kind": "source",
  "connection": {"host": "pg", "database": "postgres", "user": "postgres",
                 "password": "secret", "sslMode": "disable"},
  "source": {"tables": [{"table": "public.orders", "queue": "orders"}]}
}'`;

const PG_SINK = `curl -s -X PUT localhost:6632/api/v1/connectors/orders-copy \\
  -H 'content-type: application/json' -d '{
  "kind": "sink",
  "connection": {"host": "pg", "database": "postgres", "user": "postgres",
                 "password": "secret", "sslMode": "disable"},
  "sink": {"queue": "orders", "table": "public.orders_copy", "mode": "cdc"}
}'`;

export const cores: MapCore[] = [
  {
    id: "push",
    label: "Push",
    aka: { Kafka: ["produce", "producer"], RabbitMQ: ["publish"], "Amazon SQS": ["SendMessage"], Redis: ["XADD"] },
    line:
      "Write messages. The first push that names a queue and a partition creates them: there is no partition " +
      "count to choose.",
    use: "Every producer starts here.",
    code: "push",
    docs: "/writing-applications/push/",
    items: [
      {
        id: "partition",
        label: "Partition",
        aka: { Kafka: ["message key", "partition key"], "Amazon SQS": ["FIFO queue", "message group ID"], "Azure Service Bus": ["session"], "Google Pub/Sub": ["ordering key"] },
        line:
          "A partition is one entity: a customer, an order, a conversation. Order holds inside it, and a slow " +
          "entity holds up no other. A push with no partition goes to `Default`.",
        settings: [{ name: ".partition(name)", value: "Default" }],
        use: "Name it after the entity whose events must stay in order, like `customer-42`.",
        code: "push",
        docs: "/writing-applications/push/",
      },
      {
        id: "dedup-id",
        label: "Dedup id",
        aka: { "Amazon SQS": ["message deduplication ID"], Kafka: ["idempotent producer"], "NATS JetStream": ["Nats-Msg-Id"], "Azure Service Bus": ["duplicate detection"] },
        line:
          "A message's `transactionId`. The same id in the same partition within the dedup window is not " +
          "written again: the push answers `duplicate`.",
        settings: [{ name: "dedupWindowSeconds", value: "3600 s, set on the queue; at most 7 days, 0 turns it off" }],
        use:
          "Derive it from the work (`receipt-${orderId}`), never a fresh UUID: then a retry after a timeout is " +
          "safe.",
        code: "push",
        docs: "/writing-applications/push/",
      },
      {
        id: "answers",
        label: "Per-message answer",
        aka: { RabbitMQ: ["publisher confirms"], Kafka: ["delivery report"] },
        line:
          "One push carries up to 10,000 messages, and the broker answers each one: `queued`, `duplicate` or " +
          "`error`. The HTTP status is 201 even when some are refused.",
        settings: [{ name: "messages per push", value: "up to 10,000" }],
        settingsHead: "Limit",
        use:
          "Read each message's status: retry only what was refused, and take a duplicate as an earlier attempt " +
          "that got through.",
        code: "push-callbacks",
        docs: "/writing-applications/push/#every-message-gets-its-own-answer",
      },
      {
        id: "buffered-push",
        label: "Buffered push",
        aka: { Kafka: ["linger.ms", "batch.size"] },
        line:
          "The client collects messages and sends them in batches: when a buffer holds `messageCount` " +
          "messages, or when `timeMillis` has passed.",
        settings: [
          { name: ".buffer({ messageCount, timeMillis })", value: "off" },
          { name: "bufferMaxMessages", value: "100,000 per client (JavaScript)" },
        ],
        use:
          "Thousands of small messages a second. A crash loses what is still in the buffer, so call " +
          "`flushAllBuffers()` before the process exits.",
        code: "push-buffer",
        docs: "/writing-applications/push/#buffering-for-many-small-messages",
      },
      {
        id: "binary-push",
        label: "Binary push",
        aka: { Kafka: ["compression.type"] },
        line:
          "A push travels as one binary body instead of JSON; with compression the client packs and " +
          "zstd-compresses each partition's batch. A framed pop reads the records as stored.",
        settings: [
          { name: "binaryPush", value: "false" },
          { name: "pushCompression", value: "false, needs binaryPush" },
          { name: "framedPop", value: "false" },
        ],
        use:
          "A high message rate, when the broker's CPU or the network is the limit. Otherwise leave them off: " +
          "JSON is easier to read when you debug.",
        code: "wire-forms",
        docs: "/writing-applications/push/#binary-push-compression-and-framed-pop",
      },
      {
        id: "headers",
        label: "Headers",
        aka: { Kafka: ["record headers"], RabbitMQ: ["message properties"], "Amazon SQS": ["message attributes"], "Google Pub/Sub": ["attributes"], "Azure Service Bus": ["application properties"] },
        line:
          "A small map of strings stored in clear beside the payload: the message's type, its trace context, " +
          "its source. A `where` reads them without reading the payload.",
        settings: [
          { name: "headers", value: "up to 64 per message, 8 KiB in all" },
          { name: "names", value: "a-z 0-9 . _ - (queen- is reserved)" },
        ],
        settingsHead: "Limit",
        use: "Route and trace on a header, even on an encrypted queue. No secret in a header: it is never encrypted.",
        snippet: { lang: "js", code: ROUTING_HEADERS },
        docs: "/writing-applications/headers/",
        since: "2.9",
      },
    ],
  },
  {
    id: "queue",
    label: "Queue",
    aka: { Kafka: ["topic"], "Google Pub/Sub": ["topic"], "NATS JetStream": ["stream"], Redis: ["stream"] },
    line:
      "A named set of partitions that share one configuration. A push creates it with the defaults; " +
      "configure it when you want others.",
    use:
      "Always configure a queue before its first push: the defaults surprise. A queue a push creates leases for " +
      "60 s, one from `create()` for 300 s, and retention is off, so it keeps everything.",
    code: "configure",
    docs: "/writing-applications/queues/",
    items: [
      {
        id: "lease",
        label: "Lease",
        aka: { "Amazon SQS": ["visibility timeout"], "Azure Service Bus": ["lock duration", "peek-lock"], "Google Pub/Sub": ["ack deadline"], "NATS JetStream": ["AckWait"], RabbitMQ: ["consumer timeout"] },
        line:
          "How long a worker holds the messages it took before they go to another worker. Long work renews " +
          "the lease while it runs.",
        settings: [
          { name: "leaseTime", value: "60 s (300 s from create())" },
          { name: ".renewLease(true, ms)", value: "off" },
        ],
        use: "Short enough that a crashed worker's messages move on quickly; renew it for slow but healthy work.",
        code: "consume-run",
        docs: "/writing-applications/pop/#leases-and-long-work",
      },
      {
        id: "delay",
        label: "Delay",
        aka: { "Amazon SQS": ["delay queue", "DelaySeconds"] },
        line: "Every new message waits this long after its push before anyone can take it.",
        settings: [{ name: "delayedProcessing", value: "0 s" }],
        use: "The same delay for every message of a queue. For one message at a time, use a timer.",
        code: "configure-jobs",
        docs: "/writing-applications/queues/#every-option",
      },
      {
        id: "burst-window",
        label: "Burst window",
        aka: { "Also called": ["debounce"] },
        line: "New messages wait in their partition this long, so a burst is handed over as one batch.",
        settings: [{ name: "windowBuffer", value: "0 s" }],
        use: "A guest sends three chat messages in a row, and the assistant answers all three at once.",
        code: "uc-chat",
        docs: "/patterns/ordering-batching/",
      },
      {
        id: "retries",
        label: "Retries",
        aka: { "Amazon SQS": ["maxReceiveCount", "redrive policy"], RabbitMQ: ["x-delivery-limit"], "Azure Service Bus": ["MaxDeliveryCount"], "Google Pub/Sub": ["max delivery attempts"], "NATS JetStream": ["MaxDeliver"] },
        line:
          "How many failed attempts a message gets, and how long it waits before each new one. Its partition " +
          "waits with it, for that group.",
        settings: [
          { name: "retryLimit", value: "3" },
          { name: "retryDelay", value: "1000 ms" },
        ],
        use: "Temporary failures heal by themselves; the limit stops a message that never succeeds from blocking its partition.",
        code: "configure-jobs",
        docs: "/writing-applications/retries/",
      },
      {
        id: "dlq",
        label: "Dead-letter queue",
        aka: { RabbitMQ: ["dead letter exchange", "DLX"], "Amazon SQS": ["dead-letter queue"], "Google Pub/Sub": ["dead-letter topic"], Kafka: ["dead letter topic"], "Also called": ["dead letter channel"] },
        line:
          "Where a message goes after its last retry, with its error, so its group moves on. With both settings " +
          "off, the message is dropped instead.",
        settings: [
          { name: "deadLetterQueue", value: "true" },
          { name: "dlqAfterMaxRetries", value: "true" },
        ],
        use: "Keep both on: a message that fails for good is parked, not lost, and you can replay it once the cause is fixed.",
        docs: "/writing-applications/retries/",
      },
      {
        id: "dedup-window",
        label: "Dedup window",
        aka: { "Amazon SQS": ["deduplication interval"], "Azure Service Bus": ["duplicate detection window"], "NATS JetStream": ["duplicate window"] },
        line:
          "How long a partition remembers `transactionId`s: a second push with the same id in that time writes " +
          "nothing and answers `duplicate`.",
        settings: [{ name: "dedupWindowSeconds", value: "3600 s; at most 7 days; 0 turns it off" }],
        use: "Cover the time in which your producers may retry a push.",
        code: "configure-jobs",
        docs: "/writing-applications/queues/#every-option",
      },
      {
        id: "retention",
        label: "Retention",
        aka: { Kafka: ["retention.ms"], "Amazon SQS": ["MessageRetentionPeriod"], "Google Pub/Sub": ["message retention duration"], "NATS JetStream": ["MaxAge"], RabbitMQ: ["max-age"] },
        line:
          "How long messages are kept. Consuming does not delete: a queue is a log, kept for its retention.",
        settings: [
          { name: "retentionEnabled", value: "false" },
          { name: "retentionSeconds", value: "0: older than this goes, consumed or not" },
          { name: "completedRetentionSeconds", value: "0: older than this goes, once every group acked it" },
          { name: "maxWaitTimeSeconds", value: "0: works without retentionEnabled" },
        ],
        use: "Keep the history long enough to read it again, and no longer than the disk allows.",
        code: "configure-jobs",
        docs: "/writing-applications/queues/#every-option",
      },
      {
        id: "encryption",
        label: "Encryption",
        aka: { "Amazon SQS": ["server-side encryption", "SSE"], "Also called": ["encryption at rest"] },
        line: "Payloads are encrypted on disk. Headers stay in clear, so routing still reads them.",
        settings: [
          { name: "encryptionEnabled", value: "false" },
          { name: "QUEEN_ENCRYPTION_KEY", value: "required on every node" },
        ],
        use: "Queues that carry personal data.",
        docs: "/writing-applications/queues/#every-option",
      },
      {
        id: "labels",
        label: "Labels",
        line:
          "`namespace` and `task` group queues in the dashboard and in a discovery pop, one pop over every queue " +
          "of a namespace or a task.",
        settings: [
          { name: ".namespace(name)", value: "from the queue's name" },
          { name: ".task(name)", value: "from the queue's name" },
        ],
        use: "Find a team's queues at a glance, and pop from all of them in one call.",
        code: "configure",
        docs: "/writing-applications/queues/#every-option",
      },
      {
        id: "reading-queue",
        label: "Reading queue",
        aka: { RabbitMQ: ["exchange", "binding", "topic exchange"], "Amazon SNS": ["filter policy"], "Azure Service Bus": ["auto-forwarding", "subscription filter"], "Also called": ["router", "content-based router"] },
        line:
          "A queue whose messages come from other queues, with a `where` and partitions of its own. It holds " +
          "links of about 20 bytes, not copies.",
        settings: [
          { name: "from", value: "none: a queue, or a list of them" },
          { name: "where", value: "every message" },
          { name: "partitionBy", value: "same" },
          { name: "start", value: "now" },
        ],
        use: "A reader that needs settings of its own, partitions of its own or several sources.",
        snippet: { lang: "js", code: ROUTING_READING },
        docs: "/writing-applications/routing/#reading-queues",
        since: "2.9",
      },
    ],
  },
  {
    id: "connectors",
    label: "Connectors",
    aka: { Kafka: ["Kafka Connect", "connector"], "Also called": ["CDC"] },
    line:
      "The broker copies data between QueenMQ and the systems around it: database tables into queues " +
      "(sources), queues into tables and buckets (sinks), and Kafka clients through the Kafka port. No other " +
      "process runs.",
    settings: [
      { name: "PUT /api/v1/connectors/:name", value: "one call per source or sink" },
      { name: "QUEEN_ENCRYPTION_KEY", value: "required: a database password is stored sealed with it" },
    ],
    settingsHead: "What it needs",
    use:
      "Get a table's changes out without Debezium and Kafka Connect, keep a table or a bucket in step with a " +
      "queue, keep your Kafka clients while you move.",
    docs: "/integrations/",
    items: [
      {
        id: "postgres-source",
        label: "PostgreSQL source",
        aka: { Kafka: ["Debezium", "source connector"], "Also called": ["CDC", "change data capture"] },
        line:
          "Reads committed changes through logical replication: every insert, update and delete becomes a " +
          "message, with exactly-once effects.",
        settings: [
          { name: "PostgreSQL", value: "17 or later" },
          { name: "wal_level", value: "logical" },
        ],
        settingsHead: "What it needs",
        use: "Keep a cache, a search index or another service in step with a table.",
        snippet: { lang: "bash", code: PG_SOURCE },
        docs: "/integrations/postgres/#the-source",
      },
      {
        id: "mysql-source",
        label: "MySQL source",
        aka: { Kafka: ["Debezium", "source connector"], "Also called": ["CDC", "change data capture"] },
        line:
          "Reads the binlog the way a replica does: every committed row change becomes a message, with " +
          "exactly-once effects.",
        settings: [
          { name: "MySQL", value: "8.0 or later, not MariaDB" },
          { name: "binlog_row_metadata", value: "FULL" },
          { name: "gtid_mode", value: "ON" },
        ],
        settingsHead: "What it needs",
        use: "The same as the PostgreSQL source, for a MySQL table.",
        docs: "/integrations/mysql/#the-source",
        since: "2.9",
      },
      {
        id: "postgres-sink",
        label: "PostgreSQL sink",
        aka: { Kafka: ["JDBC sink connector", "sink connector"] },
        line: "A consumer group the broker runs for you, writing every message into a table.",
        settings: [{ name: "PUT /api/v1/connectors/:name", value: "one call per sink" }],
        settingsHead: "How",
        use: "Replace the consumer you would write to keep a table in step with a queue.",
        snippet: { lang: "bash", code: PG_SINK },
        docs: "/integrations/postgres/#the-sink",
      },
      {
        id: "s3-sink",
        label: "S3 sink",
        aka: { Kafka: ["S3 sink connector"], AWS: ["Firehose"] },
        line:
          "Copies chosen queues into a bucket as JSONL or Parquet files that DuckDB, Spark or ClickHouse read. " +
          "Exactly once: a retried upload rewrites the same file.",
        settings: [
          { name: "QUEEN_S3_EMBEDDED", value: "off until set to true" },
          { name: "QUEEN_S3_QUEUES", value: "the queues to copy" },
          { name: "retentionSinkHold (on the queue)", value: "\"default\": retention waits for the sink, up to 7 days" },
        ],
        use: "History for analytics, audits and replays, on storage that costs less than the brokers' disks.",
        docs: "/integrations/s3/",
      },
      {
        id: "kafka",
        label: "Kafka port",
        aka: { "Also called": ["Kafka protocol", "Kafka API", "Kafka-compatible"] },
        line:
          "Kafka producers, consumers and admin tools talk to QueenMQ through the Kafka port: a topic is a " +
          "queue, a key is a partition, a Kafka consumer group is a QueenMQ group.",
        settings: [
          { name: "QUEEN_KAFKA_EMBEDDED", value: "off until set to true" },
          { name: "QUEEN_KAFKA_ADVERTISED_ADDR", value: "required, no default" },
        ],
        use: "Move from Kafka one service at a time: change `bootstrap.servers` and nothing else.",
        docs: "/integrations/kafka/",
      },
    ],
  },
  {
    id: "pop",
    label: "Pop",
    aka: { Kafka: ["poll", "consumer"], RabbitMQ: ["basic.consume", "basic.get"], "Amazon SQS": ["ReceiveMessage"], "Google Pub/Sub": ["pull"], Redis: ["XREADGROUP"], "Also called": ["competing consumers"] },
    line:
      "Take messages. Without a consumer group each message goes to one worker; with groups, every group " +
      "gets every message.",
    use: "Workers that compete for jobs, and services that each want every event.",
    code: "consume",
    docs: "/writing-applications/pop/",
    items: [
      {
        id: "consumer-groups",
        label: "Consumer groups",
        aka: { Kafka: ["consumer group"], RabbitMQ: ["fanout exchange"], "Amazon SNS": ["subscription"], "Google Pub/Sub": ["subscription"], "Azure Service Bus": ["topic subscription"], "NATS JetStream": ["durable consumer"], Redis: ["XGROUP"] },
        line:
          "Each group reads the whole queue at its own pace, and the workers of one group share it. A group " +
          "works a partition with one worker at a time.",
        settings: [{ name: ".group(name)", value: "none: queue mode, each message to one worker" }],
        use: "Billing, emails and analytics each want every order: one group each, and none slows the others.",
        code: "consume-group",
        docs: "/writing-applications/pop/#queue-mode-and-consumer-groups",
      },
      {
        id: "group-start",
        label: "Where a group starts",
        aka: { Kafka: ["auto.offset.reset", "earliest", "latest"], "NATS JetStream": ["DeliverPolicy"], "Google Pub/Sub": ["seek"] },
        line:
          "Fixed at the group's first pop: `new` reads what comes after it, `all` everything still kept, " +
          "`subscriptionFrom` from a time. A seek moves a group later.",
        settings: [
          { name: "subscriptionMode", value: "new" },
          { name: "subscriptionFrom", value: "none" },
        ],
        use:
          "A new projection wants the history, a bug fix replays the last day. A new group skips everything " +
          "pushed before its first pop unless it asks for `all`.",
        code: "replay",
        docs: "/writing-applications/pop/#where-a-new-group-starts",
      },
      {
        id: "batch",
        label: "Pop parameters",
        aka: { Kafka: ["max.poll.records"], "Amazon SQS": ["MaxNumberOfMessages", "WaitTimeSeconds", "long polling"], RabbitMQ: ["prefetch", "basic.qos"] },
        line:
          "By default the broker sizes every pop: 100 to 1,000 messages, from the partitions that have work. " +
          "A value you set yourself is never changed.",
        settings: [
          { name: ".batch(n)", value: "autopilot, 100 to 1,000" },
          { name: ".partitions(n)", value: "autopilot, 1 to 64" },
          { name: ".wait(bool)", value: "true: waits for messages, up to the timeout" },
          { name: ".timeoutMillis(ms)", value: "30000, at most 60 s" },
          { name: ".autopilot(false)", value: "one message from one partition per pop" },
        ],
        use: "Leave it to the broker, unless the handler needs a batch of a fixed size.",
        code: "consume-batch",
        docs: "/writing-applications/pop/#how-many-messages-a-pop-takes-autopilot",
      },
      {
        id: "one-or-batch",
        label: "One or a batch",
        aka: { "Spring Kafka": ["batch listener"] },
        line:
          "With `.each()` the handler gets one message at a time; without it, the whole batch. When it returns the " +
          "client acks, when it throws it acks `failed`; `.autoAck(false)` leaves the ack to the handler.",
        settings: [
          { name: ".each()", value: "off: the handler gets the batch" },
          { name: ".autoAck(bool)", value: "true" },
        ],
        use: "`each()` for one-at-a-time work, a batch for bulk writes, `autoAck(false)` when the handler acks in its own transaction.",
        code: "consume-batch",
        docs: "/writing-applications/pop/#one-message-at-a-time-or-a-whole-batch",
      },
      {
        id: "worker",
        label: "Running a worker",
        aka: { "Also called": ["consumer threads", "graceful shutdown"] },
        line:
          "`concurrency(n)` runs n handlers, each on its own partitions; `renewLease` keeps long work leased; " +
          "`limit` and `idleMillis` end the loop; a `signal` stops it at once and gives back what it had not started.",
        settings: [
          { name: ".concurrency(n)", value: "1" },
          { name: ".renewLease(true, ms)", value: "off" },
          { name: ".idleMillis(ms)", value: "none: run until stopped" },
          { name: "{ signal }", value: "none" },
        ],
        use: "A deployed worker: several handlers, renewal for long tasks, a clean stop on SIGTERM.",
        code: "consume-run",
        docs: "/writing-applications/pop/#running-a-consumer-for-real",
      },
      {
        id: "conflation",
        label: "Conflation",
        aka: { Kafka: ["log compaction", "compacted topic"], RabbitMQ: ["last value queue"], "Also called": ["LVQ"] },
        line:
          "A group with `.conflation()` gets only the newest message of each partition, and its cursor moves " +
          "past the older ones.",
        settings: [{ name: ".conflation()", value: "off; needs a group, set at its first pop" }],
        use:
          "Commands where only the latest counts, like \"recompute the price of A-1\": a worker that is behind " +
          "does the work once.",
        code: "conflation",
        docs: "/writing-applications/pop/#only-the-newest-message-conflation",
      },
      {
        id: "no-ack",
        label: "No ack",
        aka: { RabbitMQ: ["autoAck", "auto-ack"], Kafka: ["enable.auto.commit"], "Also called": ["at most once"] },
        line:
          "With `.commitOnDelivery()` the cursor moves as the messages are handed out: no lease, nothing to " +
          "ack. Delivery is then at most once.",
        settings: [{ name: ".commitOnDelivery()", value: "off; pop() only" }],
        use: "A live chart that can lose a reading now and then.",
        code: "commit-on-delivery",
        docs: "/writing-applications/pop/#faster-with-no-ack-commitondelivery",
      },
      {
        id: "filtered-group",
        label: "Filtered group",
        aka: { RabbitMQ: ["exchange", "routing key", "binding", "topic exchange", "headers exchange"], "Amazon SNS": ["filter policy"], "Azure Service Bus": ["subscription filter", "SQL filter"], "Google Pub/Sub": ["subscription filter"], "NATS JetStream": ["filter subject"], "Also called": ["router", "content-based router", "message filter"] },
        line:
          "A group with a `where` gets only the messages it matches, and its lag counts only those. The " +
          "`where` is set at the group's first pop.",
        settings: [{ name: ".where(field, op, value)", value: "every message" }],
        use: "\"Only some of these\", in one line in the consumer, with no queue to create.",
        snippet: { lang: "js", code: ROUTING_FILTERED },
        docs: "/writing-applications/routing/#filtered-consumer-groups",
        since: "2.9",
      },
      {
        id: "fetch",
        label: "Read by offset",
        aka: { Kafka: ["offset", "seek", "assign"], RabbitMQ: ["stream offset"], Redis: ["XRANGE"], "Also called": ["replay"] },
        line: "`fetch` reads a partition by offset: no group, no lease, no ack.",
        use: "An audit, rebuilding one entity, or a sink that keeps its own position.",
        code: "fetch",
        docs: "/writing-applications/pop/#read-it-again",
      },
      {
        id: "four-answers",
        label: "Ack: four answers",
        aka: { RabbitMQ: ["basic.ack", "basic.nack", "basic.reject", "requeue"], "Amazon SQS": ["DeleteMessage", "ChangeMessageVisibility"], "Azure Service Bus": ["complete", "abandon", "dead-letter"], "NATS JetStream": ["Ack", "Nak", "Term"], Kafka: ["commitSync"] },
        line:
          "`completed` moves the cursor. `failed` spends a retry. `retry` gives the message back at once, " +
          "spending none. `dlq` files it in the dead-letter queue now. `consume()` acks for you: `completed` " +
          "when the handler returns, `failed` when it throws.",
        settings: [
          { name: "completed", value: "done, the cursor moves past it" },
          { name: "failed", value: "comes back after retryDelay" },
          { name: "retry", value: "comes back at once" },
          { name: "dlq", value: "dead-lettered now" },
        ],
        settingsHead: "What happens",
        use:
          "`dlq` for input that can never work, like a payload that will not parse; `retry` for a worker that " +
          "is shutting down.",
        code: "consume-callbacks",
        docs: "/writing-applications/ack/#four-answers",
      },
      {
        id: "dead-letters",
        label: "Replay dead letters",
        aka: { "Amazon SQS": ["DLQ redrive"], RabbitMQ: ["shovel"] },
        line:
          "Read the dead-letter queue, newest first, with each letter's error. A replay puts a letter back at the " +
          "end of its partition, once, and takes it out of the dead-letter queue.",
        settings: [
          { name: ".dlq(group).limit(n).get()", value: "reads it, newest first" },
          { name: "admin.retryMessage(partitionId, transactionId)", value: "replays one letter" },
        ],
        settingsHead: "What it does",
        use: "Fix the cause, then send the parked messages through again, without anyone pushing them by hand.",
        code: "dlq-read",
        docs: "/writing-applications/retries/#replay-it",
      },
    ],
  },
  {
    id: "stream",
    label: "Stream",
    aka: { Kafka: ["Kafka Streams", "ksqlDB"], "Also called": ["stream processing", "Flink"] },
    line:
      "A stream reads a queue or the tenant feed, keeps its state in the broker, and commits its results " +
      "with the acks of its inputs in one transaction.",
    use:
      "Counting, summing and grouping over time in a normal process, with no separate stream processor. In " +
      "the JavaScript, Python, Go and Rust clients.",
    code: "stream",
    docs: "/writing-applications/streams/",
    items: [
      {
        id: "windows",
        label: "Windows",
        aka: { Kafka: ["tumbling window", "hopping window", "session window"], "Also called": ["windowed aggregation"] },
        line:
          "Four kinds: tumbling (fixed, one after the other), sliding (overlapping), session (closed after a " +
          "quiet gap) and cron (aligned to the clock).",
        settings: [
          { name: "windowTumbling", value: "{ seconds: 60 }" },
          { name: "windowSliding", value: "{ size: 600, slide: 60 }" },
          { name: "windowSession", value: "{ gap: 1800 }" },
          { name: "windowCron", value: "{ every: 'hour' }" },
        ],
        settingsHead: "Example",
        use: "Revenue per minute, logins in the last ten minutes, one window per visit, hourly reports.",
        code: "stream-session",
        docs: "/writing-applications/streams/#four-kinds-of-window",
      },
      {
        id: "event-time",
        label: "Event time",
        aka: { Kafka: ["TimestampExtractor"], "Also called": ["watermark", "late events"] },
        line:
          "With `eventTime`, a window holds the events that happened during it, not the ones that arrived; " +
          "`allowedLateness` says how late an event may still count.",
        settings: [
          { name: "eventTime", value: "the arrival time" },
          { name: "allowedLateness", value: "0" },
          { name: "onLate", value: "'drop'" },
        ],
        use: "A phone that was offline, a batch upload: count a sale in the hour it happened.",
        code: "stream-hourly",
        docs: "/writing-applications/streams/#when-it-happened-or-when-it-arrived",
      },
      {
        id: "shape",
        label: "Shape the data",
        aka: { Kafka: ["KStream", "mapValues", "aggregate"] },
        line:
          "`map`, `filter` and `flatMap` change, drop or split messages; `aggregate` folds a window into named " +
          "numbers (`count`, `sum`, `min`, `max`, `avg`), `reduce` is the general fold; `keyBy` groups by " +
          "something other than the partition.",
        use: "Most stream jobs are the same few steps: drop noise, reshape, group, fold.",
        code: "feed-window",
        docs: "/writing-applications/streams/#shaping-the-data",
      },
      {
        id: "gate",
        label: "Gate",
        aka: { "Also called": ["rate limit", "token bucket", "throttle"] },
        line:
          "`gate(fn)` decides, message by message, whether a message goes on now, with state kept per " +
          "partition. A refused message stops its partition and comes back, in order, when its lease runs out.",
        settings: [
          { name: "tokenBucketGate({ capacity, refillPerSec })", value: "ready-made" },
          { name: "slidingWindowGate", value: "ready-made" },
        ],
        settingsHead: "Gates",
        use: "An SMS provider that accepts five messages a second per customer.",
        code: "stream-gate",
        docs: "/writing-applications/streams/#rate-limits-with-a-gate",
      },
      {
        id: "outputs",
        label: "Outputs",
        aka: { Kafka: ["to()", "exactly-once", "EOS"] },
        line:
          "`.to(queue)` pushes each result in the same commit as the state and the acks. `.foreach(fn)` calls your " +
          "code and acks after it returns, so a crash calls it again. `.foreachBatch` hands results over in batches.",
        settings: [
          { name: ".to(queue)", value: "exactly once, in the commit" },
          { name: ".foreach(fn)", value: "at least once" },
          { name: ".foreachBatch(fn, { maxItems, maxWaitMillis })", value: "1000 items, 0 ms" },
        ],
        settingsHead: "Delivery",
        use: "Results that stay in QueenMQ go `to` a queue; a call to the outside world goes in `foreach`, with an idempotency key.",
        code: "feed-batch",
        docs: "/writing-applications/streams/#two-ways-to-end-a-stream",
      },
      {
        id: "feed",
        label: "Tenant feed",
        line:
          "`Stream.fromFeed` reads every push to the queues you name, in commit order, from the followers. Its " +
          "`where` runs in the broker, so a message that fails it never crosses the network.",
        settings: [
          { name: "queues", value: "every queue of the tenant" },
          { name: "from", value: "'now'" },
        ],
        use: "Route, filter or window across several queues at once.",
        code: "feed-route",
        docs: "/writing-applications/streams/#streams-from-the-tenant-feed",
      },
      {
        id: "patterns-joins",
        label: "Patterns and joins",
        aka: { Kafka: ["KStream join"], "Also called": ["CEP", "complex event processing"] },
        line:
          "Wait for one message after another (`pattern`), for several kinds (`join`), for n parts " +
          "(`gather`), or join a stream with a table (`joinTable`), each within a time.",
        settings: [{ name: "within", value: "'15m', '2d', ..." }],
        settingsHead: "Example",
        use: "An order not paid within 15 minutes; an order paid and shipped; a batch whose parts all arrived.",
        code: "feed-pattern",
        docs: "/writing-applications/streams/#patterns-and-joins",
      },
      {
        id: "run-stream",
        label: "Run a stream",
        aka: { Kafka: ["application.id", "state store"] },
        line:
          "`run({ queryId })` names the stream: its position, snapshots and lock. One process runs it at a time; " +
          "`shards` spreads it over several; `Stream.view` reads its state while it runs; `onPoison` says what a " +
          "message that keeps failing does.",
        settings: [
          { name: "queryId", value: "required" },
          { name: "exclusive", value: "true: one process at a time" },
          { name: "shards", value: "1" },
          { name: "onPoison, retries", value: "'stop', 3" },
          { name: "serveState", value: "true" },
        ],
        use: "A stream that restarts where it stopped, runs in one place, and can be read while it runs.",
        code: "feed-shards",
        docs: "/writing-applications/streams/#run-options",
      },
    ],
  },
  {
    id: "transaction",
    label: "Transaction",
    aka: { Kafka: ["transactions", "exactly-once"], "Also called": ["outbox", "transactional outbox"] },
    line:
      "One call that commits acks, pushes, KV writes and timers together: one entry of the log, all or " +
      "nothing.",
    settings: [
      { name: "KV operations", value: "at most 64, over 256 keys, one write per key" },
      { name: "Timer operations", value: "one per queue and timer key" },
      { name: "Request body", value: "at most 64 MiB" },
    ],
    settingsName: "Part",
    settingsHead: "Limit",
    use: "Every step of a workflow: ack the input, push the next events, save the state, schedule the reminder.",
    code: "transaction",
    docs: "/writing-applications/transactions/",
    items: [
      {
        id: "conditions",
        label: "Conditions",
        aka: { "Also called": ["compare-and-set", "CAS", "optimistic locking"] },
        line:
          "A transaction cannot read and then decide. Put a condition on a write instead: `expect` names the " +
          "version a key must have, and `required: true` rolls the whole call back when it misses.",
        settings: [
          { name: "expect", value: "none" },
          { name: "required", value: "false" },
        ],
        use: "Two workers read the same version and both decide to write: one wins, the other rolls back with nothing written.",
        code: "uc-order",
        docs: "/writing-applications/transactions/#conditions-instead-of-reads",
      },
      {
        id: "once",
        label: "once()",
        aka: { "Also called": ["idempotency key", "idempotent consumer", "inbox"] },
        line:
          "A marker written with `putIfAbsent` and `required: true`, keyed on the business fact. A second " +
          "delivery of the same fact rolls back whole, receipts and timers included.",
        settings: [{ name: ".once(ns, key, { ttl })", value: "JavaScript and Python; elsewhere putIfAbsent + required" }],
        settingsHead: "Where",
        use: "Send the receipt once per payment: key the marker on the `paymentId`.",
        docs: "/writing-applications/transactions/#conditions-instead-of-reads",
      },
      {
        id: "link",
        label: "Link",
        aka: { "Azure Service Bus": ["auto-forwarding"], "Also called": ["forward"] },
        line:
          "Forward the message you hold into another queue without its payload: about 20 bytes, written in " +
          "the same commit.",
        settings: [{ name: ".link([message], { headers })", value: "inside a transaction" }],
        settingsHead: "Where",
        use: "Relays and pipelines: a hop costs about 20 bytes, whatever the payload weighs.",
        snippet: { lang: "js", code: ROUTING_LINK },
        docs: "/writing-applications/routing/#link-forward-a-message-without-its-payload",
        since: "2.9",
      },
      {
        id: "rollback",
        label: "Rollback answers",
        aka: { Kafka: ["abortTransaction"], "Also called": ["abort"] },
        line:
          "A rolled-back call answers HTTP 200 with `success: false` and a reason: `rejected_ack`, " +
          "`kv_precondition`, `duplicate` or `reserved`. The SDKs return it; they do not throw.",
        settings: [
          { name: "success", value: "false on a rollback" },
          { name: "reason", value: "why it rolled back" },
        ],
        settingsHead: "In the answer",
        use: "Read `success` after every commit. `duplicate` means the step had already committed.",
        docs: "/writing-applications/transactions/#why-a-transaction-rolls-back",
      },
    ],
  },
  {
    id: "kv",
    label: "KV / Locks",
    aka: { Redis: ["GET", "SET"], "NATS JetStream": ["KV bucket"], "Also called": ["key-value store"] },
    line:
      "Keys and values in the same log as the messages, and the locks built on them. Every write names its " +
      "lifetime and gives the key a new version; a lock is a lease with one holder, a semaphore one with n.",
    use:
      "An entity's state, the markers that make a step safe to repeat, counters, and the work no message " +
      "triggers: a nightly job, a singleton, a pool of n. KV has no queries: the records you search stay in " +
      "your database.",
    code: "kv",
    docs: "/writing-applications/kv-locks-timers/",
    items: [
      {
        id: "get-put",
        label: "Get and put",
        aka: { Redis: ["GET", "SET", "EXPIRE"], "NATS JetStream": ["KV get", "KV put"] },
        line:
          "`get`, `getMany`, `put` and `delete`. A write names a TTL or `forever`; a read returns the value, " +
          "its version and when it expires.",
        settings: [{ name: "ttl", value: "every write names one, or forever" }],
        settingsHead: "Rule",
        use: "The state a step reads before it decides, written back in the step's transaction.",
        code: "kv",
        docs: "/writing-applications/kv-locks-timers/#kv",
      },
      {
        id: "put-if-absent",
        label: "Put if absent",
        aka: { Redis: ["SETNX", "SET NX"], "NATS JetStream": ["KV create"] },
        line: "`putIfAbsent` writes only when the key does not exist: of several callers, one wins.",
        use: "A claim: the first worker to arrive does the work, the others see who has it.",
        docs: "/writing-applications/kv-locks-timers/#kv",
      },
      {
        id: "versions",
        label: "Versions",
        aka: { "NATS JetStream": ["revision"], "Also called": ["ETag", "compare-and-set"] },
        line:
          "`expect` makes a write depend on the version you read; `check` asserts a version and writes " +
          "nothing. A missed condition answers `applied: false` with the current value.",
        settings: [
          { name: "expect", value: "none" },
          { name: "check", value: "asserts, writes nothing" },
        ],
        use: "Two workers read the same cart and both write it: the second is refused instead of overwriting what it never saw.",
        code: "kv",
        docs: "/writing-applications/kv-locks-timers/#kv",
      },
      {
        id: "counters",
        label: "Counters",
        aka: { Redis: ["INCR", "INCRBY"] },
        line: "`incr` adds to a counter with no read first. With `max` or `min`, an increment past the bound does not apply.",
        settings: [{ name: "incr(ns, key, by, { max, min, ttl })", value: "no bound" }],
        use: "A rate limit in one call: at most 100 calls a minute per customer.",
        code: "kv",
        docs: "/writing-applications/kv-locks-timers/#kv",
      },
      {
        id: "prefix",
        label: "Prefix pages",
        aka: { Redis: ["SCAN MATCH"], "Also called": ["list keys"] },
        line: "`getPrefix` reads one page of keys under a prefix, with a cursor for the next. Not inside a transaction.",
        use: "List the keys of one customer, or of one job.",
        docs: "/writing-applications/kv-locks-timers/#kv",
      },
      {
        id: "lock",
        label: "Lock",
        aka: { Redis: ["Redlock"], "Also called": ["distributed lock", "leader election", "mutex"] },
        line:
          "`queen.lock(name, { ttl })`: `acquire()` answers false while someone else holds it. The " +
          "JavaScript, Python, Go, Rust and C++ handles renew it in the background.",
        settings: [{ name: "ttl", value: "you choose; renewed every third of it" }],
        use: "One worker at a time for a nightly report.",
        code: "uc-singleton",
        docs: "/patterns/one-worker/",
      },
      {
        id: "semaphore",
        label: "Semaphore",
        aka: { "Also called": ["distributed semaphore", "concurrency limit"] },
        line: "`queen.semaphore(name, n)`: at most n holders at once. One handle is one permit.",
        use: "At most four jobs at a time on a shared resource, across every worker.",
        docs: "/writing-applications/kv-locks-timers/#locks-and-semaphores",
      },
      {
        id: "guard",
        label: "Guard",
        aka: { "Also called": ["fencing token"] },
        line:
          "`.guard(lock)` on a transaction: it commits only while the lock is still yours. A lock expires " +
          "without telling its holder; the guard stops a paused process from doing the work twice.",
        use: "Fence the work of a lock holder inside QueenMQ.",
        code: "lock",
        docs: "/writing-applications/kv-locks-timers/#locks-and-semaphores",
      },
    ],
  },
  {
    id: "timers",
    label: "Timers",
    aka: { "Azure Service Bus": ["scheduled messages"], "Also called": ["cron", "scheduler", "reminder"] },
    line:
      "A message scheduled for later, named by its queue and its key. When it fires, the broker pushes it " +
      "through the log like any push.",
    use: "Reminders, timeouts and follow-ups, with no cron job and no table to scan.",
    code: "uc-reminder",
    docs: "/writing-applications/kv-locks-timers/#timers",
    items: [
      {
        id: "schedule",
        label: "Schedule and move",
        aka: { "Azure Service Bus": ["ScheduledEnqueueTime"], RabbitMQ: ["delayed message exchange", "x-delay"], "Amazon SQS": ["message timer"], Pulsar: ["deliverAt"] },
        line:
          "Scheduling the same key again replaces the timer, so moving a reminder is the same call. A fire is " +
          "never early, and a little late: the leader checks every 50 ms.",
        settings: [
          { name: "timer(queue).key(k).delay(d).schedule()", value: "replaces a timer with the same key" },
          { name: ".headers({ ... })", value: "the message it fires carries them (2.9)" },
        ],
        settingsHead: "How",
        use: "Ask for a review 24 hours after delivery, and move it when the delivery was late.",
        code: "uc-reminder",
        docs: "/patterns/delayed-messages/",
      },
      {
        id: "cancel",
        label: "Cancel",
        aka: { "Azure Service Bus": ["CancelScheduledMessage"] },
        line: "A cancel answers `absent` when there is no timer, which may mean it already fired.",
        use: "The guest already wrote a review: no reminder. A handler that compensates checks the entity's state first.",
        code: "uc-reminder",
        docs: "/writing-applications/kv-locks-timers/#timers",
      },
      {
        id: "timer-partition",
        label: "Entity's partition",
        line: "Give a timer the entity's partition, and it arrives in order with the entity's other events.",
        settings: [{ name: ".partition(name)", value: "the entity's partition" }],
        settingsHead: "How",
        use: "A timeout that must stay in step with the events of the same order.",
        docs: "/writing-applications/kv-locks-timers/#timers",
      },
    ],
  },
  {
    id: "ephemeral",
    label: "Ephemeral queues",
    aka: { RabbitMQ: ["Direct reply-to", "transient queue"], NATS: ["request-reply"], "Also called": ["RPC"] },
    line:
      "Queues in memory, outside the log, for request and reply. Every node serves every queue. No " +
      "transactions and no replay, and a crash loses what is in flight, by design.",
    use: "A request that waits for its answer now (what is the price of A-1?): speed matters more than history.",
    code: "caller",
    docs: "/writing-applications/ephemeral/",
    items: [
      {
        id: "reply-inbox",
        label: "Reply inbox",
        aka: { RabbitMQ: ["Direct reply-to", "reply_to"], NATS: ["inbox"], "Also called": ["return address", "correlation ID"] },
        line: "The caller makes a private inbox, sends its name as `replyTo`, and waits on it with a timeout.",
        settings: [
          { name: "replyTo", value: "the caller's inbox" },
          { name: "timeout", value: "you choose" },
        ],
        settingsHead: "What to give",
        use: "A quote with a fallback: wait at most half a second, then show \"ask us\".",
        code: "uc-reply",
        docs: "/patterns/request-reply/",
      },
      {
        id: "ephemeral-worker",
        label: "Worker",
        aka: { "Also called": ["RPC server", "responder"] },
        line:
          "A worker pops requests and answers on whatever inbox each one names. Groups, leases and acks work " +
          "as on a regular queue.",
        use: "A pricing service, a lookup, any answer computed on demand.",
        code: "worker",
        docs: "/writing-applications/ephemeral/",
      },
      {
        id: "in-memory",
        label: "In memory",
        aka: { RabbitMQ: ["transient", "non-durable"], NATS: ["Core NATS"] },
        line:
          "A partition keeps its order while one node owns it. When that node restarts, the partition moves, " +
          "and a message pushed during the move can come out early. A crash loses that node's share.",
        use: "Anything that must survive a crash goes in a regular queue.",
        docs: "/writing-applications/ephemeral/",
      },
    ],
  },
];

export const goals: MapGoal[] = [
  {
    id: "goal-work",
    label: "Spread jobs over workers",
    intro: "Many workers share one stream of jobs, and each job is done by one of them.",
    steps: [
      { id: "partition", text: "Push each job into its user's partition: a partition is worked by one worker at a time." },
      { id: "pop", text: "Consume without a consumer group: each job goes to one worker." },
      { id: "worker", text: "Run a few handlers at once with `concurrency`; add workers to go faster." },
      { id: "lease", text: "A job not acked within its lease goes to another worker." },
      { id: "four-answers", text: "Ack each job when it is done; a failed one comes back." },
    ],
    href: "/patterns/work-queue/",
  },
  {
    id: "goal-pubsub",
    label: "Send one event to many services",
    intro: "One event, read by every service at its own pace.",
    steps: [
      { id: "push", text: "Push the event once." },
      { id: "consumer-groups", text: "Each service reads with its own consumer group, and every group gets every event." },
      { id: "group-start", text: "A group added later asks for `subscriptionMode('all')` to start from the first event." },
    ],
    href: "/patterns/publish-subscribe/",
  },
  {
    id: "goal-workflow",
    label: "Run a workflow step by step",
    intro: "Each step of the workflow is one transaction.",
    steps: [
      { id: "versions", text: "Read the order's state and its version." },
      { id: "transaction", text: "Open one transaction for the whole step." },
      { id: "four-answers", text: "Ack the event that started the step.", onAckLine: true },
      { id: "conditions", text: "Write the new state only if its version did not move." },
      { id: "push", text: "Push the event for the next step." },
      { id: "once", text: "Or gate the step on a key instead of a version." },
    ],
    href: "/patterns/workflows/",
  },
  {
    id: "goal-reply",
    label: "Ask and wait for an answer",
    intro: "A request with a deadline, answered through a private inbox in memory.",
    steps: [
      { id: "reply-inbox", text: "Make a private inbox, send it as `replyTo`, and wait on it with a timeout." },
      { id: "ephemeral-worker", text: "Any instance of the service takes the request and answers to that inbox." },
      { id: "in-memory", text: "Nothing goes to disk: a node that crashes loses the requests it held." },
    ],
    href: "/patterns/request-reply/",
  },
  {
    id: "goal-later",
    label: "Do something later",
    intro: "A timer is a message that waits, named by its queue and a key.",
    steps: [
      { id: "schedule", text: "Schedule a timer under a key; scheduling the key again moves it." },
      { id: "cancel", text: "Cancel the key when the reminder is no longer needed." },
      { id: "timers", text: "When it is due, the timer arrives in its queue as a normal message." },
      { id: "delay", text: "Or make every message of a queue wait a set time before anyone can take it." },
    ],
    href: "/patterns/delayed-messages/",
  },
  {
    id: "goal-once",
    label: "Run a job on one worker",
    intro: "Every replica runs the same scheduler; one of them does the job.",
    steps: [
      { id: "lock", text: "Take the lock: the worker that gets it does the job, the others skip." },
      { id: "transaction", text: "Write the job's result in one transaction." },
      { id: "guard", text: "Put `.guard(lock)` on it: the commit rolls back if the lock was lost meanwhile." },
    ],
    href: "/patterns/one-worker/",
  },
  {
    id: "goal-retry",
    label: "Retry, then park what fails",
    intro: "A call that can fail is tried again, then kept aside for a person.",
    steps: [
      { id: "retries", text: "Give the queue a `retryLimit` and a `retryDelay`." },
      { id: "four-answers", text: "Throw, or ack it as failed: the message comes back after the delay." },
      { id: "dlq", text: "After the last try it goes to the dead-letter queue." },
      { id: "dead-letters", text: "Read the dead letters, fix the cause, and send them back." },
    ],
    href: "/patterns/retries-dead-letters/",
  },
  {
    id: "goal-latest",
    label: "Keep only the newest update",
    intro: "For a value that changes, only the last one matters.",
    steps: [
      { id: "partition", text: "Push each thing's updates into its own partition: a hotel, a sensor." },
      { id: "conflation", text: "Read with `.conflation()`: the group gets only the newest message of each partition." },
    ],
    href: "/patterns/last-value/",
  },
  {
    id: "goal-burst",
    label: "Answer a burst at once",
    intro: "A guest sends three messages in a row; the assistant answers once.",
    steps: [
      { id: "partition", text: "Every conversation is a partition, so its messages stay in order." },
      { id: "burst-window", text: "Set `windowBuffer`: new messages wait until the first is that old, then go together." },
      { id: "one-or-batch", text: "Consume without `.each()`: the handler gets the whole burst as one batch." },
    ],
    href: "/patterns/ordering-batching/",
  },
  {
    id: "goal-rate",
    label: "Rate-limit each customer",
    intro: "Each customer gets so many messages a second, all of them in order.",
    steps: [
      { id: "partition", text: "Push each customer's messages into the customer's partition." },
      { id: "gate", text: "Run a stream with a token bucket gate: so many messages a second, per partition." },
      { id: "lease", text: "A message the gate refuses comes back when its lease ends, still in order." },
    ],
    href: "/patterns/rate-limiting/",
  },
  {
    id: "goal-window",
    label: "Count and sum over time",
    intro: "Events add up to one record per window: a trace, a session, a minute.",
    steps: [
      { id: "partition", text: "One partition per trace, session or visitor." },
      { id: "windows", text: "A stream closes a window after a gap of quiet, or every N seconds." },
      { id: "shape", text: "Add each window up: count, sum, the first and the last." },
      { id: "outputs", text: "Push one message per closed window with `.to(queue)`, once." },
      { id: "event-time", text: "Or window by the time in the event, not by when it arrived." },
    ],
    href: "/patterns/windowed-aggregation/",
  },
  {
    id: "goal-route",
    label: "Route by type",
    intro: "Each reader gets only the messages it wants, and each message is written once.",
    steps: [
      { id: "headers", text: "Push with headers, such as a `type`." },
      { id: "filtered-group", text: "A consumer group reads only what matches its `where`." },
      { id: "reading-queue", text: "Or a queue reads other queues through a `where`, as links of about 20 bytes." },
      { id: "link", text: "Or a transaction forwards the message it holds to another queue." },
    ],
    href: "/writing-applications/routing/",
  },
  {
    id: "goal-cdc",
    label: "Stream a table into a queue",
    intro: "A table's changes arrive as messages, from inside the broker.",
    steps: [
      { id: "postgres-source", text: "Stream a PostgreSQL table into a queue through logical replication." },
      { id: "mysql-source", text: "Or a MySQL table, from its binlog." },
    ],
    href: "/integrations/",
  },
  {
    id: "goal-kafka",
    label: "Keep my Kafka clients",
    intro: "Kafka clients use QueenMQ unchanged; move one service at a time.",
    steps: [
      { id: "kafka", text: "Turn on the Kafka port and point `bootstrap.servers` at it." },
      { id: "consumer-groups", text: "A topic is a queue and a Kafka group is a consumer group: both clients share the data." },
    ],
    href: "/integrations/kafka/",
  },
];

/** Every core and dot by id. */
export function entryById(): Map<string, { entry: MapEntry; core: MapCore }> {
  const all = new Map<string, { entry: MapEntry; core: MapCore }>();
  for (const core of cores) {
    for (const entry of [core, ...core.items]) {
      if (all.has(entry.id)) throw new Error(`[map] the id "${entry.id}" is used twice in src/lib/map.ts`);
      all.set(entry.id, { entry, core });
    }
  }
  for (const goal of goals) {
    const seen = new Set<string>();
    for (const { id } of goal.steps) {
      if (!all.has(id)) throw new Error(`[map] goal "${goal.id}" has a step on "${id}", which is not in src/lib/map.ts`);
      if (seen.has(id)) throw new Error(`[map] goal "${goal.id}" numbers "${id}" twice: one piece, one number`);
      seen.add(id);
    }
  }
  return all;
}
