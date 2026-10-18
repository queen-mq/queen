#!/bin/bash
# The rig of the docs runs (2026-10-14/16): 3 brokers c4-standard-24-lssd (4 local NVMe), 3 loaders c4-highcpu-16,
# VPC queen-bench (kept), Ubuntu 24.04,
# no service account, labels purpose=queen-bench owner=$OWNER.
set -u
G=$(cd "$(dirname "$0")" && pwd)
P=${GCP_PROJECT:?set GCP_PROJECT}; Z=europe-west1-b
KEY=$(cat ~/.ssh/id_rsa.pub)
sed "s#__PUBKEY__#$KEY#" $G/startup.sh > $G/startup.filled.sh
COMMON="--project=$P --zone=$Z --subnet=queen-bench-ew1 --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
 --no-service-account --no-scopes --labels=purpose=queen-bench,owner=${OWNER:?set OWNER} --metadata-from-file=startup-script=$G/startup.filled.sh"
gcloud compute instances create queen-bench-1 queen-bench-2 queen-bench-3 $COMMON \
  --machine-type=c4-standard-24-lssd --boot-disk-size=30GB --boot-disk-type=hyperdisk-balanced 2>&1 &
gcloud compute instances create queen-load-1 queen-load-2 queen-load-3 $COMMON \
  --machine-type=c4-highcpu-16 --boot-disk-size=50GB --boot-disk-type=hyperdisk-balanced 2>&1 &
wait
gcloud compute instances list --project=$P --filter='labels.purpose=queen-bench' \
  --format='table(name,status,networkInterfaces[0].networkIP,networkInterfaces[0].accessConfigs[0].natIP)'
