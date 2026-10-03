# Jepsen archive, 2026-09-27 (P8 to P10)

Copied from the Jepsen control node `134.209.236.110` (nodes `n1`–`n5`, DigitalOcean) before the VMs were released. The harness itself is in git (`test/jepsen`). This folder holds what git does not: campaign results, Jepsen stores, matrices, scripts and the two tested binaries.

Extract any bundle with `zstd -dc <file>.tar.zst | tar -x`. Check integrity with `shasum -a 256 -c SHA256SUMS`.

## What is in git

| Path | Contents |
|---|---|
| `campaigns/<runs-dir>/` | `PROGRESS.txt`, `summary.txt` and `FINAL.txt` of every campaign, taken from `runs.tar.zst` |
| `bug-evidence.tar.zst` | The 13 logs named under "Evidence for each bug" below, same paths (5 MB) |
| `harness-and-tools.tar.zst` | As below (104 KB) |
| `SHA256SUMS` | Checksums of all five bundles |

The other four bundles (6.3 GB) are not in git. They live on Alice's Mac in
`~/Work/queen-bench-archive/2026-09-27-jepsen-archive/`, with this README and `SHA256SUMS`.

## Bundles

| File | Contents |
|---|---|
| `runs.tar.zst` | Every campaign folder: `runs` (early phases), `runs-p7`, `runs-p8`, `runs-p8f` (P8 final, 77 tests), `runs-p9` (early re-runs), `runs-p9full` (P9), `runs-p9b`, `runs-p9c`, `runs-p9d`, `runs-p9e`, `runs-p10s` (smoke), `runs-p10` (full pass 1), `runs-p10b` (full pass 2). Each holds `PROGRESS.txt` (one line per test), `summary.txt`, per-test `.log` and `clock-sync.log`; the P10 folders also have `FINAL.txt`. |
| `jepsen-p8-store.tar.zst` | The Jepsen store of every P8–P10 test: `history.edn`, `results.edn`, `jepsen.log`, and each node's `queen.log`. |
| `older-harness-p0-p7.tar.zst` | Older harness copies (`jepsen-queen`, `jepsen-queen-next`, `jepsen-p7`, `jepsen-w2`) with their stores (P0–P7). |
| `harness-and-tools.tar.zst` | `/root/jepsen-p8` without its store: all matrices (`p8-final.txt`, `p9*.txt`, `p10*.txt`) and `run-campaign.sh`. Also `elle-debug` (the Elle/bifurcan false-alarm investigation), repro scripts, build logs, `pack-archive.sh`. |
| `binaries.tar.zst` | `queen-nopg` (raft `ec9d88c4`, tested by P10) and `queen-kvfix6` (the pushed fixes, tested by P9e). |

## Results

| Campaign | Binary | Result |
|---|---|---|
| P8 final | `503bbdd2` | 68 of 77 valid. 2 real KV bugs, a claim-harness bug, Elle false alarms. |
| P9 | kvfix2 | First 52 valid, then `w5-clock` failed (pops stalled after a clock jump). |
| P9b | kvfix3 | `w2-clock` lease overlap (a forward jump of the leader's clock). |
| P9d | kvfix5 | 15 of 16 valid; `w3c-pause` lost an acknowledged write (stale leader). |
| P9e | kvfix6 (the pushed fixes) | 44 of 44 valid, then stopped to switch images. |
| P10 smoke | `queen-nopg`, Postgres removed | 6 of 6 valid |
| P10 full pass 1 | `queen-nopg` | 82 of 82 valid |
| P10 full pass 2 | `queen-nopg` | 82 of 82 valid |

## Evidence for each bug

| Bug | Where |
|---|---|
| Acknowledged write never committed (stale leader answered by index) | `runs-p9d/p9d-w3c-pause.log`; store `queen-claim-pause…/2026-09-26T04:34:12…` |
| Fractured read (a read saw half a batch) | `runs-p8f/p8-w4-pause.log` |
| G1c on the KV fallback path | `runs-p8f/p8-w4-leader-deaf.log` |
| Pops stalled after a leader clock excursion | `runs-p9full/p9-w5-clock.log`, `runs-p9c/p9c-w2-clock.log` |
| Lease ended early by a leader clock jump | `runs-p9b/p9b-w2-clock.log` |
| Claim harness lost ids on timeouts | `runs-p8f/p8-w3c-pause.log`, `runs-p8f/p8-w3c-membership.log` |
| Elle `cyclic-versions` false alarms | `runs-p8f/p8-w4-{restart,part,lz-kill,bridge,membership}.log`; analysis in `harness-and-tools` → `elle-debug` |
