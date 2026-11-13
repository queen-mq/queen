#!/usr/bin/env bash
# keys.sh [vm...] (Mac): install the rig's key for root on the VMs (through IAP), in parallel. Default: the 8-vCPU rig.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
PUB=$(cat /private/tmp/claude-502/-Users-alice--claude-spares-spare-EeaKo8/7fba3cfb-3a78-403f-9f19-577659052673/scratchpad/qsched_key.pub)
VMS="${*:-qsched-n1 qsched-n2 qsched-n3 qsched-load}"
for vm in $VMS; do
  (gcloud compute ssh "$vm" --project=smartness-stage --zone=europe-west1-b --tunnel-through-iap --quiet \
    --command="sudo mkdir -p /root/.ssh && echo '$PUB' | sudo tee -a /root/.ssh/authorized_keys >/dev/null && sudo chmod 600 /root/.ssh/authorized_keys && echo $vm-ok" 2>&1 | tail -1) &
done
wait
