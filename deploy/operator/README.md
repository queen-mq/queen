# The Queen operator

It runs Queen clusters on Kubernetes from `QueenCluster` objects, and does through the broker's
membership API what a StatefulSet cannot: change the voters, replace a node, grow the volumes, and
restart the pods one at a time, the leader last, each behind a write that commits. The docs page is
[/operate/operator/](https://queenmq.com/operate/operator/).

```bash
kubectl apply -f https://raw.githubusercontent.com/queen-mq/queen/master/deploy/operator/install.yaml

kubectl create namespace queen
kubectl -n queen create secret generic queen \
  --from-literal=QUEEN_RAFT_TOKEN="$(openssl rand -hex 32)" \
  --from-literal=QUEEN_ENCRYPTION_KEY="$(openssl rand -hex 32)"
kubectl apply -f https://raw.githubusercontent.com/queen-mq/queen/master/deploy/operator/examples/queencluster.yaml

kubectl -n queen get queencluster
```

```text
NAME    VERSION   VOTERS    COMMITS   PHASE   AGE
queen   2.2.0     [1,2,3]   true      Ready   40s
```

## What a change does

| You change | The operator |
|---|---|
| `spec.version`, or anything else in the pod | Restarts the pods one at a time: the ones that do not lead from the highest ordinal, the leader last. Before each stop every other voter must be Ready for `minReadySeconds`, live and caught up as the leader sees it, and a write must commit through a pod that stays. |
| `spec.replicas` up | Adds the pods, adds each new node as a learner once it answers, and promotes them in one change when they have caught up. |
| `spec.replicas` down | Removes the highest node from the membership, then its pod and its volume claim, and so on. A node that leads hands leadership away first. |
| `spec.storage.size` up | Asks every claim for the new size, restarts a pod whose file system needs it, then creates the StatefulSet again around its running pods, because a StatefulSet's volume template cannot be edited. |
| the annotation `queenmq.com/replace-pod: <pod>` | Replaces that pod's volume: the node leaves the membership, the claim is deleted, the empty pod joins as a learner and is promoted. The annotation is removed when it is a voter again. |

`kubectl -n queen get queencluster queen -o yaml` shows what it is doing in `status.message`, and
`kubectl -n queen describe queencluster queen` lists each step it took.

## What it leaves to you

Everything that can lose data: a forced recovery, an apply skip, two clusters where there was
one. It reports the state (`Blocked`, or `Degraded` with the reason) and stops. The procedures are
in [recovery](https://queenmq.com/operate/recovery/).

It also notices a voter that is behind the leader and does not move, which is what a volume wiped
or restored under a node's id looks like, and restarts nothing while there is one. Replacing that
pod is your decision: set the annotation.

## Not for a cluster applied by hand

The operator names its objects as the manifest in the docs does (`queen`, `queen-headless`) and
labels them differently (`app.kubernetes.io/name` and `app.kubernetes.io/instance`). A StatefulSet
keeps the selector it was created with, so a `QueenCluster` cannot take over a cluster applied from
that manifest, and one created under the same name in the same namespace would point that
cluster's Services at pods that do not exist. Give it a namespace or a name of its own.

## How it reaches the brokers

By default the operator calls each pod at its own DNS name, on the broker port, so it needs a
network path to the pods: with a NetworkPolicy on the cluster, let the `queen-system` namespace
in. `--broker-access=apiserver` goes through the API server's pod
proxy instead, which needs the `pods/proxy` permission (not in `install.yaml`) and no network path;
it is also how the operator runs outside a cluster, in development.

The broker port has no authentication of its own in this deployment. The operator does not
support a broker port that asks for one.

## Develop

```bash
make test          # go vet and the unit tests
make manifests     # the CRD and the role from the code, and install.yaml
make docker-build  # IMG=... to name the image
```

The module stands alone (`GOWORK=off`): it is not part of the repository's `go.work`, so the
broker image builds without it.

Every decision is in `internal/controller/plan.go`, a function of what was observed that changes
nothing, and `plan_test.go` walks each sequence step by step.
