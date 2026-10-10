#!/usr/bin/env bash
# The short matrix: both builds with completed retention 60 s, then both with retention off. 1M msg/s, 500,000 partitions, 300 s.
cd "$(dirname "$0")"
bash run.sh a-210-completed queen-210 completed 300 111111 500000
bash run.sh b-rc7-completed queen-rc7 completed 300 111111 500000
bash run.sh c-rc7-off       queen-rc7 off       300 111111 500000
bash run.sh d-210-off       queen-210 off       300 111111 500000
python3 report.py ../runs/a-210-completed ../runs/b-rc7-completed ../runs/c-rc7-off ../runs/d-210-off > ../runs/report-first-four.txt 2>&1
echo CHAIN_DONE >> ../runs/report-first-four.txt
