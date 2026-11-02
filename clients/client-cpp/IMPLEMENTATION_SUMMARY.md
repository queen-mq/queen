# Queen C++ Client - How It Is Built

A map of `queen_client.hpp` (2.7.1) for whoever changes it. The [README](README.md) is the user
guide.

## Shape

One header, C++17, in `namespace queen`. Its dependencies are cpp-httplib (HTTP and TLS),
nlohmann/json (vendored in `third_party/`, used only when the program has none) and, with
`QUEEN_CLIENT_ZSTD`, zstd. No thread pool, no other library. The public API follows the Go and Rust
clients: typed envelopes (`Message`, `PushResult`, `AckResult`, `TransactionResult`), JSON payloads.

## Layers, top to bottom

| Part | What it does |
| --- | --- |
| `Queen` | The client. Owns one `HttpClient` and one `BufferManager`; hands out builders; acks, renews, closes. |
| `QueueBuilder` | Push (direct or buffered), pop, consume, fetch, DLQ, configure. Holds the consume options. |
| `detail::pop_once` | THE pop: path, query (`pop_query`, autopilot via `util::pop_sizing`), the conflation check, the parse into `Message`s. `pop()` and every consume worker use it, so the two cannot drift. |
| `ConsumerManager` | The consume loop: `concurrency` std::threads, a shared limit, `Cancel`, idle, the per-message or per-batch settling rules (mirrors `client-rust/src/consumer.rs`), `LeaseRenewal` per batch, supervision. |
| `TransactionBuilder` | Acks, pushes (with partition), KV and timer riders, lock guards; `commit()` resends once a guard lost only to its own lock's renewal. |
| `KvBuilder`, `TimersBuilder`, `LocksBuilder`, `Lock` | The KV, timer and lock surfaces. Every operation body is minted in `namespace wire`, so standalone calls and transaction riders send the same bytes. `Lock` renews on a thread of its own. |
| `EphemeralBuilder`, `reads::`, `DLQBuilder`, `Admin` | Ephemeral queues, reads by offset, the dead-letter reader, the management routes. |
| `BufferManager`, `MessageBuffer` | Client-side push buffering: one flusher thread per queue/partition, a blocking bound, failed batches back at the front. |
| `HttpClient` | The transport: a keep-alive connection pool per backend, TLS by URL scheme, 429 handling, retry with backoff, failover, the binary push/transaction bodies and the framed pop. |
| `LoadBalancer` | Affinity (FNV-1a ring over `util::grouping_key`, as every SDK), round-robin, session; backend health. |

## Errors

`queen::Error` is the base. `HttpError` is a status of 400 or more, `NetworkError` no answer at all.
A broker verdict that is a value (an ack refused, a KV write not applied, a lost transaction
precondition) is returned, never thrown. Misuse throws `std::invalid_argument` before any request.

## Wire compatibility

The request bodies and paths are the ones 2.7.0 sent: `test_kv_timers`, `test_locks`,
`test_wire_forms`, `test_autopilot`, `test_conflation` and `test_consumer` pin them against
in-process stubs. Queue mode sends `consumerGroup: null` on acks, as before.

## Tests

`make run-unit` runs the broker-free suites; `./bin/test_client <url>` the live one. CI builds the
image in `test/runners/cpp/` and runs both against a broker.
