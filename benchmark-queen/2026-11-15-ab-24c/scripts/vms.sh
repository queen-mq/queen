#!/usr/bin/env bash
# vms.sh (Mac): the 11-15 rig. Three brokers qab-b1/b2/b3 and three loaders qab-l1/l2/l3, all in one zone, label
# purpose=queen-ab-24c. Brokers: 24-vCPU Intel with local NVMe (c4-standard-24-lssd) in zones b, c, d; only if none
# has it, 22-vCPU (c3-standard-22-lssd). A try that creates only some of the three deletes them before the next.
# Loaders then take the same zone: c4-highcpu-24, else c4-highcpu-16, else c3-highcpu-22. Prints what worked.
set -u
P=smartness-stage
common=(--project=$P --subnet=queen-bench-ew1 --no-service-account --no-scopes --image-family=ubuntu-2404-lts-amd64
  --image-project=ubuntu-os-cloud --labels=purpose=queen-ab-24c,owner=aliceviola)
boot() { case $1 in c4-*) echo hyperdisk-balanced ;; *) echo pd-balanced ;; esac; }
ZONE=""; SHAPE=""
for shape in c4-standard-24-lssd c3-standard-22-lssd; do
  for z in europe-west1-b europe-west1-c europe-west1-d; do
    out=$(gcloud compute instances create qab-b1 qab-b2 qab-b3 "${common[@]}" --zone=$z --machine-type=$shape \
      --boot-disk-size=40GB --boot-disk-type=$(boot $shape) --format="value(name)" 2>&1)
    made=$(gcloud compute instances list --project=$P --filter="name~^qab-b[123]$ AND zone:$z" --format="value(name)" 2>/dev/null | wc -l | tr -d ' ')
    if [ "$made" = 3 ]; then ZONE=$z; SHAPE=$shape; break 2; fi
    echo "$shape in $z: $(echo "$out" | grep -o -m1 -E 'stockout|QUOTA[A-Z_]*|quota[^.]*|does not have enough resources|ZONE_RESOURCE_POOL_EXHAUSTED' || echo "$out" | tail -1)"
    if [ "$made" != 0 ]; then
      gcloud compute instances delete $(gcloud compute instances list --project=$P --filter="name~^qab-b[123]$ AND zone:$z" --format="value(name)") \
        --project=$P --zone=$z --quiet >/dev/null 2>&1
    fi
  done
done
[ -n "$ZONE" ] || { echo "NO BROKER SHAPE AVAILABLE"; exit 1; }
echo "brokers: $SHAPE in $ZONE"
for lshape in c4-highcpu-24 c4-highcpu-16 c3-highcpu-22; do
  out=$(gcloud compute instances create qab-l1 qab-l2 qab-l3 "${common[@]}" --zone=$ZONE --machine-type=$lshape \
      --boot-disk-size=50GB --boot-disk-type=$(boot $lshape) --format="value(name)" 2>&1)
  made=$(gcloud compute instances list --project=$P --filter="name~^qab-l[123]$ AND zone:$ZONE" --format="value(name)" 2>/dev/null | wc -l | tr -d ' ')
  if [ "$made" = 3 ]; then echo "loaders: $lshape in $ZONE"; break; fi
  echo "loaders $lshape in $ZONE: $(echo "$out" | tail -1)"
  if [ "$made" != 0 ]; then
    gcloud compute instances delete $(gcloud compute instances list --project=$P --filter="name~^qab-l[123]$ AND zone:$ZONE" --format="value(name)") \
      --project=$P --zone=$ZONE --quiet >/dev/null 2>&1
  fi
done
gcloud compute instances list --project=$P --filter="labels.purpose=queen-ab-24c" \
  --format="table(name,zone.basename(),machineType.basename(),networkInterfaces[0].networkIP,networkInterfaces[0].accessConfigs[0].natIP,status)"
