# The Jepsen rig

One control VM and six test VMs on GCP: `jepsen-ctl` (e2-standard-8, 100 GB) and
`jepsen-n1` to `jepsen-n6` (e2-standard-2). Five nodes run every test; the
standby tests (W12) use all six (`--nodes n1,...,n6 --standby-nodes 3`).
The services the connector workloads need (PostgreSQL 17, an S3 server) run on
the control VM, which no fault touches.

## Order

```bash
# 1. on your machine: the VMs (prints their private IPs)
OWNER=<you> GCP_PROJECT=<project> bash test/jepsen/rig/create.sh

# 2. on your machine: the source to the controller (master, or the build under test)
git archive --format=tgz -o /tmp/src.tgz HEAD
gcloud compute scp /tmp/src.tgz jepsen-ctl:/tmp/src.tgz --zone=europe-west1-b

# 3. on the controller, as root: JDK, Leiningen, Rust, graphviz, gnuplot, the source, an SSH key
bash /root/src/test/jepsen/rig/ctl-setup.sh

# 4. on every node, as root, with the controller's public key and the IPs from step 1
CTL_KEY="$(cat /root/.ssh/id_ed25519.pub)" HOSTS="<ip> jepsen-ctl ctl
<ip> n1
..." bash node-setup.sh

# 5. on the controller: PostgreSQL 17 and RustFS (Docker), bound to the private address
bash /root/src/test/jepsen/rig/ctl-services.sh

# 6. on the controller: the brokers under test and the feed-stream runner bundle
bash /root/src/test/jepsen/rig/build.sh
SRC=/root/src OUT=/root/runners.tgz bash /root/src/test/jepsen/rig/build-runners.sh

# 6b. the build under test (2026-11-13): the routing branch's fix1 snapshot, built
#     linux/amd64 by its own session (md5 580a58e778c8b9fe2ada8ecf2608d6ae). From
#     your machine, copied as it is:
gcloud compute scp .claude/worktrees/routing/benchmark-queen/2026-11-12-ab-scheduler/bin/fix1/queen \
  jepsen-ctl:/root/bin/queen-fix1 --zone=europe-west1-b

# 7. a campaign (five nodes), then the standby one (six)
cd /root/src/test/jepsen
RUNS=/root/runs BIN=/root/bin/queen-fix1 ./run-campaign.sh matrix-pre-kyle-2026-11-13.txt
NODES=n1,n2,n3,n4,n5,n6 RUNS=/root/runs-standby BIN=/root/bin/queen-fix1 \
  ./run-campaign.sh matrix-standby-2026-11-13.txt
```

## Delete the rig when the campaign is done

```bash
gcloud compute instances delete jepsen-ctl jepsen-n1 jepsen-n2 jepsen-n3 jepsen-n4 jepsen-n5 jepsen-n6 \
  --project=<project> --zone=europe-west1-b --quiet
```
