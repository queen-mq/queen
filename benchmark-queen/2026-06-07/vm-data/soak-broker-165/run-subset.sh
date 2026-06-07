#!/bin/bash
R=/root/bench-runs/run_test_v3.sh
DUR=${DUR:-240}
for img in 0.15.5 simd; do
  $R "bp10-$img"    1000 10  $DUR 1 50  1 50 100 1 "$img"
  $R "bp100-$img"   1000 100 $DUR 1 50  1 50 100 1 "$img"
  $R "hipart1-$img" 1    1   $DUR 5 100 2 50 100 1 "$img"
  $R "cg10-$img"    1000 10  $DUR 1 50  1 50 100 1 "$img" 10
done
echo "SUBSET-DONE"
