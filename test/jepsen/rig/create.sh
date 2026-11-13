#!/bin/bash
# The Jepsen rig's VMs: jepsen-ctl (e2-standard-8, 100 GB) and jepsen-n1..n6
# (e2-standard-2), Ubuntu 24.04, subnet queen-bench-ew1, no service account,
# labels purpose=queen-jepsen owner=$OWNER. Prints the private IPs the node
# setup needs.
set -u
P=${GCP_PROJECT:?set GCP_PROJECT}; Z=${ZONE:-europe-west1-b}
COMMON="--project=$P --zone=$Z --subnet=queen-bench-ew1 --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
 --no-service-account --no-scopes --labels=purpose=queen-jepsen,owner=${OWNER:?set OWNER}"
gcloud compute instances create jepsen-ctl $COMMON --machine-type=e2-standard-8 --boot-disk-size=100GB 2>&1 &
gcloud compute instances create jepsen-n1 jepsen-n2 jepsen-n3 jepsen-n4 jepsen-n5 jepsen-n6 $COMMON \
  --machine-type=e2-standard-2 --boot-disk-size=40GB 2>&1 &
wait
gcloud compute instances list --project=$P --filter='labels.purpose=queen-jepsen' \
  --format='table(name,status,networkInterfaces[0].networkIP)'
