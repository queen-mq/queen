# Three Queen nodes with Docker Compose

A real three-node Raft cluster on one machine, to watch replication, a leader hand-off and a
rolling upgrade happen. The walkthrough with the output to expect is on the docs site at
[/operate/cluster/](https://queenmq.com/operate/cluster/).

```bash
cd deploy/compose/three-node
echo "QUEEN_RAFT_TOKEN=$(openssl rand -hex 32)" > .env
docker compose up -d --wait
for n in 1 2 3; do echo "queen-$n $(curl -s localhost:${n}6632/health | jq -r .raft.role)"; done
```

`--wait` returns once every node's healthcheck passes, which means each one knows the leader.
Node N answers on `localhost:N6632` (16632, 26632, 36632), API and dashboard alike, so the cluster
does not collide with a single node on 6632. The ports are bound to 127.0.0.1 because the broker
port has no authentication of its own, and the raft port (7400) is not published at all.

The images are published for linux/amd64 and linux/arm64, so the cluster runs natively on Apple
silicon as well.

## Stop the leader

```bash
docker compose stop queen-3          # whichever node said "leader"
docker compose up -d --wait queen-3  # start it again
```

On SIGTERM the leader hands its leadership to the most caught-up follower before it exits, so the
other two keep answering. Started again, it catches up from the new leader, and its `/health`
shows `applied` equal to `commit`.

## Rolling upgrade

Set `QUEEN_VERSION` in `.env` to the release you move to, then replace one node at a time; `--wait`
holds the loop until the new node is healthy:

```bash
for n in 1 2 3; do docker compose up -d --wait queen-$n; done
```

## Clean up

```bash
docker compose down -v               # removes the containers and the three volumes
```

Three containers on one host survive a process dying, not the host dying, so this file is for
trying the cluster out. For servers, read the production notes on the same docs page and the
[Kubernetes](https://queenmq.com/operate/kubernetes/) manifest.

## Kafka clients

`compose.kafka.yaml` turns on the Kafka facade on every node, as one Kafka cluster with three
brokers (node N listens on `127.0.0.1:N6092`). Kafka transactions are refused in this cluster mode;
the rest is described at [/guides/kafka/](https://queenmq.com/guides/kafka/).

```bash
docker compose -f compose.yaml -f compose.kafka.yaml up -d --wait
```
