This PR adds `driver-queen`, a driver for [Queen MQ](https://queenmq.com). Queen is an Apache-2.0
transactional message broker that keeps its state in a raft-replicated log on local disk.

The driver:

- speaks Queen's own HTTP protocol: binary push, framed pop with the broker's pop autopilot, and
  batched asynchronous acks;
- has no client library of its own. It uses the async-http-client version benchmark-framework already
  pins (2.12.3), plus aircompressor for the optional client-side zstd;
- needs Queen 2.4 or later.

Queen also has a Kafka-compatible facade, which the stock `driver-kafka` can benchmark unchanged. This
driver measures the native protocol.

## How OMB maps onto Queen

|     OMB      |                                                     Queen                                                      |
|--------------|----------------------------------------------------------------------------------------------------------------|
| Topic        | A queue                                                                                                        |
| Partition    | A named partition of the queue, `"0"` .. `"P-1"`                                                               |
| Subscription | A consumer group                                                                                               |
| Producer     | A handle on a Kafka-style accumulator shared on the worker (`lingerMs`, `batchMaxBytes`, `maxInFlight`)        |
| Consumer     | A share of one pool of long-poll pops per worker, so thousands of topics need no thread or connection per consumer |

Every push is answered once a majority of the brokers have fsynced it, and every push is checked
against the queue's dedup window. The driver cannot turn either off.

## What is in it

- `driver-queen/`:
  - the driver;
  - three profiles, like the Kafka driver's: `queen.yaml` (the settings we benchmark with),
    `queen-throughput.yaml` (`popBatch` 10,000 for one or a few partitions) and
    `queen-many-topics.yaml` (`maxPops` 12,000, with the brokers on `QUEEN_QLOG_SHARDS=16`), plus
    `queen-local.yaml` for the Docker cluster;
  - a README that documents every key.
- `driver-queen/deploy/docker`: a three-node cluster on one machine.
- `driver-queen/deploy/ssd-deployment`: Terraform and Ansible for AWS. It uses the same instance types
  as `driver-kafka`'s ssd-deployment: 3 × i3en.6xlarge brokers and 4 × m5n.8xlarge clients.
- The root `pom.xml` and `benchmark-framework/pom.xml` list the new module, and the root README lists
  Queen MQ.

## Testing

- **Build.** `mvn --no-transfer-progress --batch-mode verify` on JDK 17: BUILD SUCCESS.
  - Master does not pass `verify` today, so this branch's first commit is the fix from the separate
    PR "Fix mvn verify on master: spotless and spotbugs in the files #443 added".
  - Once that PR is merged, this PR is a single commit.
- **Local, against `deploy/docker`.** Queen 2.4.0, with `verify: true` in the driver (it counts
  duplicates and gaps per partition):
  - `simple-workload.yaml`, shortened to 1 minute, at 10,000 msg/s: 1,222,984 messages produced,
    consumed and acked. None was delivered twice, and no partition had a gap.
  - 20 topics with `maxPops: 4`, so the subscriptions wait on the long-poll fetch that tells them
    which partitions got data: 242,954 messages over 20 subscriptions. None was delivered twice, and
    no partition had a gap.
- **GCP.** Three brokers (24 vCPU, local NVMe) and three benchmark clients, against a pre-release Queen
  build. We ran workloads from four of the five families in `workloads/` (many topics is next):
  - fixed rates on 100 and 10,000 partitions;
  - max rate;
  - 1M msg/s on 10 topics;
  - backlog.

  In line with the README's notice, this PR publishes no results.
- **AWS deployment.** Not run end to end. `terraform validate` and `ansible-playbook --syntax-check`
  pass, and the templates render as expected against a test inventory.
