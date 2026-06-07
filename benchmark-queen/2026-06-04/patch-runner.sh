#!/bin/bash
# Patch the April harness (staged on 206) for the new-arch rerun:
#  - default queen image -> pushser
#  - inject the static-pop env (QUEEN_CONCURRENCY_MODE=static + QUEEN_POP_MAX_CONCURRENT=16)
#    so pop isn't Vegas-capped (the bug we diagnosed); everything else (PG config,
#    harness, cell matrix) stays exactly as April ran it.
#  - cells 900s -> 300s
set -e
cd /root/bench-runs

# 1. run_test_v3: default image -> pushser (run_master passes no image arg, so it uses the default)
sed -i 's|QUEEN_IMAGE_TAG="${11:-0.14.0.alpha.3}"|QUEEN_IMAGE_TAG="${11:-pushser}"|' run_test_v3.sh
# 2. run_test_v3: add static-pop env to the queen container
sed -i 's|-e SIDECAR_POOL_SIZE=250 \\|-e SIDECAR_POOL_SIZE=250 -e QUEEN_CONCURRENCY_MODE=static -e QUEEN_POP_MAX_CONCURRENT=16 \\|' run_test_v3.sh
# 3. masters: 300s cells
sed -i 's|^DURATION=900|DURATION=300|' run_master.sh run_cg_master.sh
# 4. cg_master: image -> pushser
sed -i 's|^IMAGE_TAG="0.14.0.alpha.3"|IMAGE_TAG="pushser"|' run_cg_master.sh

echo "=== verify run_test_v3 (image default + static-pop env) ==="
grep -n 'QUEEN_IMAGE_TAG=' run_test_v3.sh | head -1
grep -n 'QUEEN_CONCURRENCY_MODE\|QUEEN_POP_MAX_CONCURRENT' run_test_v3.sh
echo "=== verify masters (300s + pushser) ==="
grep -n '^DURATION=' run_master.sh run_cg_master.sh
grep -n '^IMAGE_TAG=' run_cg_master.sh
