This project talks to Kafka through {{client}}. Bring it to Queen MQ 2. Goal: {{goal}}.

There are two paths.

A. Keep the Kafka client. Queen speaks the Kafka wire protocol from inside the broker. Call `kafka_client` with the client's name and version and apply its mandatory config. Then read `guide(page: "guides/kafka")` for how to connect and how topics, partitions, consumer groups and offsets map onto Queen, and call `check` with language `kafka` for the traps.

B. Move to Queen's own SDK. Read `guide(page: "guides/kafka")` for the mapping, then rewrite each producer and consumer with `example` in the project's language. Model each entity as a partition named by its id instead of hashing keys into a fixed partition count, and turn each read-process-write loop into one transaction per step.

Say which path fits and why before you change code. Keep behaviour the same unless the user asks otherwise, and list every behaviour that changes.
