## Using this server

- Before writing Queen code, call `example` for each operation you need (push, consume, transaction and so on) in the project's language, and use its method names exactly. Never invent an SDK method. When no example exists, the HTTP API is the ground truth: ask `guide`.
- Before you finish writing or reviewing Queen code, call `check` with the language and go through every item.
- When Queen returns an error (an HTTP status, a `code`, a transaction `reason`, a Kafka error), call `explain_error` before you change code.
- For a Kafka client library pointed at Queen, call `kafka_client` with its name and version.
- To set up KV, timers, stream processing, ephemeral queues, the Kafka facade, the Postgres source or sink, or the S3 sink, call `setup` with the feature and the project's language.
- For concepts, limits and configuration, call `guide`. To design a new flow, follow the model above or use the `design` prompt.
