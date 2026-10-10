#!/usr/bin/env bash
# The completed-retention pair once more, in the other order, to see how much one run differs from the next.
cd "$(dirname "$0")"
bash run.sh b2-rc7-completed queen-rc7 completed 300 111111 500000
bash run.sh a2-210-completed queen-210 completed 300 111111 500000
python3 report.py ../runs/b2-rc7-completed ../runs/a2-210-completed > ../runs/report-second-pair.txt 2>&1
for t in b2-rc7-completed a2-210-completed; do for n in 1 2 3; do echo "$t n$n warnings before the load: $(cat ../runs/$t/warn-before-n$n.txt 2>/dev/null), at the end: $(grep -o 'WARN_ERROR=[0-9]*' ../runs/$t/end-n$n.txt)"; done; done >> ../runs/report-second-pair.txt
echo CHAIN_DONE >> ../runs/report-second-pair.txt
