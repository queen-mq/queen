#!/bin/bash
# Hot-entity isolation sweep — Queen 1.1.0, anchor shape 2k/1000, pp=40.
# One hot entity at 1x (uniform baseline), 10x, 50x, 200x, 1000x a cold entity.
# Fresh stack per cell; broker AND loader sampled (SPEC §5.1).
set -uo pipefail
B=46.101.186.250
L=142.93.170.82
OUT=/private/tmp/claude-502/-Users-alice-Work-queen/c440cf99-b13b-4314-af90-4276616ebefd/scratchpad/campaign
mkdir -p "$OUT"

for F in 1 10 50 200 1000; do
  NAME="skew-f${F}"
  echo "################ CELL $NAME  $(date -u +%H:%M:%SZ) ################"

  ssh -o BatchMode=yes root@$B ". /root/arbenv.sh
    cd /root/cmbench/deploy
    docker compose -f docker-compose.queen.yml down -v >/dev/null 2>&1
    docker compose -f docker-compose.queen.yml up -d >/dev/null 2>&1
    for i in \$(seq 1 60); do
      [ \"\$(curl -s -o /dev/null -w '%{http_code}' http://localhost:6632/health)\" = '200' ] && break
      sleep 1
    done
    rm -f /root/samples/$NAME.csv
    setsid nohup /root/sampler.sh cmbench-queen cmbench-queen-pg > /root/samples/$NAME.csv 2>/dev/null </dev/null &
    echo \$! > /root/sampler.pid
    echo 'broker ready + sampling'"

  ssh -o BatchMode=yes root@$L "rm -f /root/cmbench/$NAME.loader.csv
    setsid nohup /root/cmbench/sampler.sh > /root/cmbench/$NAME.loader.csv 2>/dev/null </dev/null &
    echo \$! > /root/loadsampler.pid"

  ssh -o BatchMode=yes root@$L "ulimit -n 200000
    cd /root/cmbench && rm -rf results/$NAME && mkdir -p results/$NAME
    CMD=\"./cmbench -system queen -queen-url http://10.114.0.2:6632 -queen-pop-mode wildcard -queen-pop-partitions 40 -rate 2000 -properties 1000 -hot-props 1 -hot-factor $F -duration 180 -ramp 10 -drain 90 -logdir results/$NAME\"
    echo \"\$CMD\" > results/$NAME/invocation.txt
    \$CMD > results/$NAME/run.log 2>&1
    echo \"exit=\$?\" >> results/$NAME/invocation.txt
    tail -3 results/$NAME/invocation.txt
    sed -n '/hot-entity isolation/,/ratio/p;/correctness/,/VERDICT/p' results/$NAME/run.log"

  ssh -o BatchMode=yes root@$B "[ -f /root/sampler.pid ] && kill \$(cat /root/sampler.pid) 2>/dev/null; true"
  ssh -o BatchMode=yes root@$L "[ -f /root/loadsampler.pid ] && kill \$(cat /root/loadsampler.pid) 2>/dev/null; true"

  mkdir -p "$OUT/$NAME"
  scp -q root@$L:/root/cmbench/results/$NAME/result.json "$OUT/$NAME/" 2>/dev/null
  scp -q root@$L:/root/cmbench/results/$NAME/run.log "$OUT/$NAME/" 2>/dev/null
  scp -q root@$L:/root/cmbench/results/$NAME/invocation.txt "$OUT/$NAME/" 2>/dev/null
  scp -q root@$B:/root/samples/$NAME.csv "$OUT/$NAME/sampler-broker.csv" 2>/dev/null
  scp -q root@$L:/root/cmbench/$NAME.loader.csv "$OUT/$NAME/sampler-loader.csv" 2>/dev/null
  echo "collected -> $OUT/$NAME"
done
echo "################ CAMPAIGN DONE $(date -u +%H:%M:%SZ) ################"
