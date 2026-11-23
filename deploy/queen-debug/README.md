# queen-debug

An MCP server that lets an AI agent debug a running QueenMQ cluster: which node
is sick, why a queue stopped, where a message went, and what to do about it.

It reads and never repairs. When a fix is needed, it gives the step and the
recovery guide's link, and a person runs it.

## Tools

| Start here | |
| --- | --- |
| `diagnose` | What is wrong with the cluster, worst first, with the next step |
| `why_stuck` | Why a queue or group is not moving: no reader, a held lease, nobody polling, failing messages |
| `get_message` | Where one message is, its status per group, last error, retries and trace |

| Cluster | |
| --- | --- |
| `node_detail` | One node: role, lag, disk, memory, version, restarts, exit code, events |
| `node_logs` | A node's log, including the run before its last crash |
| `cluster_metrics` | Commits, connections, CPU and memory over time |
| `write_probe` | Tests whether the cluster still commits. The only write, off by default |

| Queues and consumers | |
| --- | --- |
| `list_queues` | Every queue, those in trouble first |
| `queue_detail` | One queue's settings, counts and partitions |
| `queue_history` | Push, pop, ack, backlog and lag over time |
| `list_groups` | Every consumer group, the furthest behind first |
| `group_detail` | Lag, worker and lease for each partition |

| Messages and dead letters | |
| --- | --- |
| `list_messages` | By queue, partition, time and status |
| `fetch_messages` | A partition in order, from an offset or a time |
| `find_messages` | Search by transaction ID, payload field or header |
| `follow_trace` | One business flow across queues, for apps that record traces |
| `dlq_summary` | Why messages failed, grouped by error |
| `list_dlq` | The failed messages, with error and retry count |

| Application state (needs a quorum) | |
| --- | --- |
| `read_kv` | Namespaces, keys and values |
| `list_timers` | Timers waiting to fire |
| `list_locks` | Who holds each lock or semaphore |
| `stream_state` | Streams, and windows nobody is closing |
| `connector_status` | Postgres connector state and S3 sink lag |

Every other tool reads each node's own state, so it keeps working with no
leader. Its answer says which node replied and whether that node had a quorum.

Why reuse: queue, group and node verdicts come from the console's own rules,
imported from `app/src`, so the agent and the console cannot disagree.

## Install in Kubernetes

1. Edit the namespace and the label selector at the top of `install.yaml`.
2. Apply it, with an explicit context.

   ```bash
   kubectl --context <yours> apply -f deploy/queen-debug/install.yaml
   ```

3. Open it from your machine, and add it to your agent.

   ```bash
   kubectl --context <yours> -n queen-debug port-forward deploy/queen-debug 7700:7700
   ```

   ```bash
   claude mcp add --transport http queen-debug http://localhost:7700/mcp
   ```

The server finds the broker pods itself, including pods that are not ready.

## Run it on one machine

For Docker or VMs, over stdio. Pass every node, so it can compare them:

```bash
claude mcp add queen-debug -- docker run -i --rm --network host ghcr.io/queen-mq/queen-debug:2.10.0 --brokers http://127.0.0.1:6632
```

Without Kubernetes there are no pod states or logs: `node_logs` gives you the
command to run.

## Who can use it

There is no login. Kubernetes decides who gets in: whoever may port-forward in
the `queen-debug` namespace. To let a team in, bind the `queen-debug-user` Role
to its group. To remove someone, take them out of the group.

Why it is safe without a login:

- It listens only on the pod's loopback. A port-forward arrives there, and no
  other pod can reach it, so it needs no Service.
- It refuses anything from a browser, because while a forward is open it sits on
  your localhost, where any web page could try to call it.
- It can call only a fixed list of read routes, checked on every request.

**Keep it in its own namespace.** Port-forward rights are per namespace. Someone
let in beside the brokers could open a broker's port directly, and that port may
have no login and full write access.

Everyone let in sees the same thing, and the call log records the tool, queue
and time, not the person.

## What leaves the cluster

A tool's answer goes to your AI provider. So by default it holds ids, statuses,
times and error text with emails, ids and long numbers removed. Message bodies,
headers and KV values stay hidden.

| Setting | Flag | Default | What it does |
| --- | --- | --- | --- |
| `QUEEN_DEBUG_READ_MESSAGES` | `--read-messages` | `false` | Show bodies, headers and KV values |
| `QUEEN_DEBUG_MASK` | `--mask` | none | Fields hidden even then: `data.guest.email,headers.authorization`. `*` is any key |
| `QUEEN_DEBUG_ALLOW_PROBE` | `--allow-probe` | `false` | Turn on `write_probe` |
| `QUEEN_DEBUG_BROKERS` | `--brokers` | none | Broker addresses, instead of finding the pods |
| `QUEEN_DEBUG_NAMESPACE` | `--namespace` | its own | Namespace of the broker pods |
| `QUEEN_DEBUG_SELECTOR` | `--selector` | none | Label selector of the broker pods |
| `QUEEN_DEBUG_BROKER_PORT` | `--broker-port` | `6632` | Broker port on a pod |
| `QUEEN_DEBUG_TRANSPORT` | `--transport` | `stdio` | `stdio` or `http` |
| `QUEEN_DEBUG_LISTEN` | `--listen` | `127.0.0.1:7700` | HTTP address. Loopback only |
| `QUEEN_DEBUG_TOKEN` | none | none | A read-only token, for a broker with JWT on |

Masking hides what reaches the AI provider. It does not stop someone who can
also reach the broker directly.

## Limits

- A broker with JWT on: a read-only token can use every tool, but the broker
  keeps its raft routes for read-write tokens. Node detail then comes from
  `/health`: no CPU, memory or disk.
- It connects to port 6632, which serves one tenant: the default one. On a
  cluster with several tenants it does not see the others' queues.
- The broker serves its log search from followers only. When the one node left
  was the leader, `get_message` and `find_messages` read the message lists
  instead: newest first, for up to 10 seconds, and they say how far they got.
- The broker keeps no history of leader changes, so it can show the current
  leader and term, not when they changed.

## Tests

```bash
npm ci --prefix app && npm ci --prefix deploy/queen-debug
```

```bash
npm test --prefix deploy/queen-debug
```

The unit tests replay answers recorded from a real cluster. `npm run test:live`
starts a throwaway three-node cluster in Docker, stops nodes on purpose and
checks every tool through the failures.
