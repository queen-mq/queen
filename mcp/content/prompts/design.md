Design this flow on Queen MQ 2: {{flow}}

If that is missing or cut to a word or two, design the flow the user described in this conversation, and if there is none, ask the user to describe it first. The code will be in the project's language.

Work in this order, and show the result before writing any code.

1. Entities. List the things that need their own ordered history: an order, a customer, a device. Each becomes a partition named by its id, inside one queue per kind of event. Name the field that is the partition key.
2. Steps. List every step a worker performs. Each step is one transaction: it acks what it took, pushes what it produces, writes the state it changes in KV, and sets or cancels the timers it needs. Write each step as one line: input queue, output queues, KV keys, timers.
3. Readers. For each queue, who reads it: competing workers in queue mode, or consumer groups that each see every message. For each consumer group, its subscription mode and why.
4. Waiting. Every "after N minutes", reminder, deadline or retry-later becomes a timer, not a sleep or a cron job.
5. Identity. What makes a delivery a duplicate (a payment id, a webhook id), and where that is enforced.
6. Singletons. Work that no message triggers (a cron job, a migration, a pool of N workers) takes a lock or a semaphore, with the guard on its transaction. Work on one entity needs none: its partition is already serial.
7. The table: queue | partition key | readers | the transaction of each step | timers | KV keys.

Then call `example` for the closest whole app (chat, webhooks, saga, rate limiter, exactly-once, Kafka bridge) and for each step's operations in the project's language, call `check` for that language, and only then write code. Ask `guide` about anything this list leaves open.
