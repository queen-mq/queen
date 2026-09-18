# Spike S4 — forwarding transport (PLAN_RAFT.md WP-0.6, §9.2, §12.5, D12)

What it answers: for the receiver → leader forwarding path on port 6634
(D12), what do **length-prefixed frames over a few persistent TCP
connections** cost against **HTTP/1.1 with binary bodies over a hyper
keep-alive pool**, in round-trip latency, CPU on both processes, bytes on the
wire per message and connection count — and what do the §9.2 mutual HMAC
handshake, a per-frame MAC and TLS add on top.

It is a transport spike only: no planner, no store, no Raft. The leader
process decodes a forwarded command exactly as far as the receiver pipeline
would (every field walked, request id extracted) and answers an outcome.

## What is on the wire

One command = one forwarded push batch, as §5.1 describes it:

```
req_id[16] | deadline_us u64 | tenant | queue | partition | count u16
          | count × ( dedup_hash[16] | plen u32 | payload[plen] )
```

With the defaults (`--batch 10 --payload 256`) that is 2800 B per command,
272 B of which is per message (256 payload + 16 dedup hash, D10). The outcome
is the compact answer of §5.4: `req_id | commit_index | count × (status,
offset)` = 116 B. Nothing is ever read from disk.

Transport (a), `--transport tcp` (`src/tcpx.rs`): frames
`len u32 | xxh3 u64 | type u8 | body` (§9.2), one reader task and one writer
task per socket, frames never read inside a `select!`, several commands in
flight on one socket multiplexed by request id. Mutual HMAC handshake
(`src/auth.rs`): both nonces, cluster_id, from/to node ids, and a session key
derived from the same transcript. `--mac 1` adds the per-frame MAC of D12
(HMAC-SHA256 truncated to 16 B over `type || body`). `--tls 1` puts rustls
(ring) under the frames instead.

Transport (b), `--transport http` (`src/httpx.rs`): `POST /f` with the same
bytes as an `application/octet-stream` body over a hyper-util legacy
keep-alive pool; the outcome is the response body. A pooled connection cannot
carry a per-connection handshake (the pool hands out whichever connection is
idle), so `--mac 1` MACs every request body instead and the leader verifies
it. `--tls 1` is the same pool over rustls.

Bytes are counted on the raw `TcpStream` (`src/net.rs`), i.e. below rustls and
below HTTP framing, so "bytes per message" includes TLS records and HTTP
headers. CPU is `getrusage(RUSAGE_SELF)` deltas over the measured window on
both processes; the leader reports its own counters to the receiver over the
same transport, so one process prints the whole row.

## Load model

The receiver is an **open-loop** pacer: command *i* is due at
`start + i / cmd_rate`, `cmd_rate = rate / batch`, and each command is sent
from its own task. Two latencies are reported:

- `rt_send_*` — from the actual send to the outcome. This is the transport
  round trip, the number to compare across configurations.
- `rt_due_*` — from the due time to the outcome. It also contains the pacer's
  own error: tokio's timer has ~1 ms granularity, so at 5000 cmd/s commands
  leave in bursts of ~5 every millisecond and `rt_due_p50` sits near 1 ms
  whatever the transport does. `pace_lag_max_us` reports that error directly.

## Build and run

```sh
cd test/raft/spikes/s4-transport
cargo build --release                     # standalone crate, NOT in server/Cargo.toml
cargo +1.88 check                          # MSRV C-3
```

One configuration (two processes, kill the leader by PID, never by port):

```sh
./target/release/s4 leader   --transport tcp --port 6734 --secret $SECRET --mac 0 --tls 0 &
LP=$!
./target/release/s4 receiver --transport tcp --addr 127.0.0.1:6734 --secret $SECRET \
    --mac 0 --tls 0 --rate 50000 --batch 10 --payload 256 --secs 60 --warmup 5 --conns 2
kill $LP
```

TLS: the leader mints a throwaway self-signed `localhost` certificate at
start and writes the DER to `--cert-out`; the receiver trusts exactly that
file with `--cert`. No key material is ever written to the repo.

The whole matrix (15 configurations × 60 s, ~17 min):

```sh
bash run.sh                       # -> results/laptop/results.jsonl + runlog.txt
SECS=10 bash run.sh               # shakedown
TAG=vm OUT=$PWD/results/vm bash run.sh

# interleaved: 4 rounds of 10 s per configuration, round-robin, so a noisy
# neighbour hits every configuration alike (this is how pass B was taken)
TAG=laptop-interleaved OUT=$PWD/results/laptop-interleaved \
    ROUNDS=4 SECS=10 WARMUP=3 bash run.sh
```

Never edit `run.sh` while it is running: bash re-reads a script from a byte
offset, and a mid-run edit killed pass A after its 13th configuration.

Unit tests (framing, the sequenced per-frame MAC and its replay/reflection/
length refusals, the mutual handshake and its refusals, fail-closed secrets):

```sh
cargo test --release      # 13 tests
```

Micro-benchmark of the per-command crypto (added 2026-09-18; answers "how much
of the HTTP MAC row is hex formatting" and "what does the sequenced MAC cost"):

```sh
./target/release/s4 bench --iters 20000 --rounds 5
```

Flags: `--rate` msgs/s, `--batch`, `--payload`, `--secs`, `--warmup`,
`--conns` (framed transport: persistent connections), `--pool`
(HTTP: `pool_max_idle_per_host`), `--threads` (tokio workers, default 4 on
both processes), `--mac`, `--tls`, `--secret` (**required**, ≥ 16 bytes: the
binary fails closed since 2026-09-18), `--pace timer|spin`.

`--pace spin` (2026-09-18) keeps the same due times but lands on them within
microseconds instead of tokio's ~1 ms timer granularity, so commands stop
arriving in bursts of five. It burns a core in the receiver — its `recv`
column is meaningless — and exists only to separate the framed writer's
coalescing from the harness's arrival shape (`RESULTS-vm.md` §9).

`run.sh` parts: `PART=matrix|sweep|all` (the original 15 configurations),
`PART=extra` (the four cells of `RESULTS-vm.md` §8) and `PART=pace` (§9).

## The VM run (WP-0.2/§13.6 host, 164.90.215.224) — DONE 2026-09-17

Laptop numbers are smoke (§0.3): macOS scheduling and the loopback stack are
not the deployment. The numbers to quote come from the Linux VM, and they are
in `RESULTS-vm.md`: two independent 60 s passes of all 15 configurations,
15:08–15:25Z (`results/vm/`, copy in `results/vm-a/`) and 15:26–15:43Z
(`results/vm-b/`). Do not re-run the matrix to re-read it. The commands that
produced it:

```sh
# 0. the VM must be idle: another phase-0 job must not be running
ssh root@164.90.215.224 'uptime; pgrep -a queen; pgrep -af raft; pgrep -af s4'

# 1. sources (no target/, no node_modules)
rsync -az --exclude target --exclude node_modules \
   /Users/alice/Work/queen/test/raft/spikes/s4-transport/ \
   root@164.90.215.224:/root/raft/wp-0.6/s4-transport/

# 2. build on the VM
ssh root@164.90.215.224 '. ~/.cargo/env && cd /root/raft/wp-0.6/s4-transport && cargo build --release'

# 3. the matrix (~17 min); nohup + poll, the ssh session must not own it
ssh root@164.90.215.224 '. ~/.cargo/env && cd /root/raft/wp-0.6/s4-transport && \
   nohup env TAG=vm SECS=60 PORT=6734 bash run.sh > /root/raft/wp-0.6/matrix.log 2>&1 &'
ssh root@164.90.215.224 'tail -5 /root/raft/wp-0.6/matrix.log; wc -l /root/raft/wp-0.6/s4-transport/results/vm/results.jsonl'

# 4. results back into the repo, and nothing left running
rsync -az root@164.90.215.224:/root/raft/wp-0.6/s4-transport/results/vm/ \
   /Users/alice/Work/queen/test/raft/spikes/s4-transport/results/vm/
ssh root@164.90.215.224 'pgrep -af s4 || echo clean'
```

A second, short VM session on **2026-09-18** (`run-vm-refute.sh`, ~9 minutes,
results in `results/vm-c/` and `results/vm-pace/`) added the four cells the
matrix never had and the pacer comparison, after an adversarial review of
`MEMO.md`. Phase 1 deliberately ran the **pass A binary unchanged**
(`/root/raft/wp-0.6/s4-transport/target/release/s4`, md5 `7ecbc517…`) so its
rows compare with the passes above.

Of the two things the VM run was meant to add:

1. **8 vCPU instead of 10 fast cores**, and Linux's loopback and scheduler —
   **done.** The CPU-per-message figures in `RESULTS-vm.md` are the ones that
   transfer to a pod, and they are repeatable to within 5 % across the two
   passes. One thing the VM could NOT settle: its guest CPU model masks
   `sha_ni`, so the per-frame MAC ran on software SHA-256 there too
   (`RESULTS-vm.md` §5, MEMO Deferred 1).
2. **A real RTT** — **not done.** Everything measured is loopback (≈60 µs
   round trip); pod-to-pod in a cell is 0.2–1 ms, which shifts p50 for BOTH
   transports by the same constant but changes how many connections HTTP/1.1
   needs (`connections ≈ offered command rate × RTT`). Approximating it with
   `tc qdisc add dev lo root netem delay 500us` (and
   `tc qdisc del dev lo root` afterwards) changes a VM-wide setting, so it
   needs Alice's approval first (§0.3); until then `RESULTS-vm.md` quotes the
   connection-count arithmetic rather than a measurement.

## Files

| file | what |
|---|---|
| `src/frame.rs` | §9.2 framing + optional per-frame MAC |
| `src/auth.rs` | §9.2 mutual HMAC handshake; HTTP per-request MAC |
| `src/wire.rs` | synthetic command/outcome bodies, leader stats |
| `src/net.rs` | byte-counting socket wrapper, TLS setup, rusage |
| `src/tcpx.rs` | transport (a): framed TCP |
| `src/httpx.rs` | transport (b): HTTP/1.1 over a hyper pool |
| `src/main.rs` | the `Transport` trait, leader and receiver modes, pacer, report |
| `run.sh` | the configuration matrix (`ROUNDS=n` interleaves it; `PART=extra\|pace`) |
| `run-vm-refute.sh` | the 2026-09-18 VM session: the missing cells on the pass A binary, then the pacer comparison |
| `refute.py` | every ratio the revised memo quotes, recomputed from the four result sets |
| `table.py` | results.jsonl -> the markdown tables (`--agg` medians the rounds) |
| `pairs.py` | within-round pairwise comparison, robust to a noisy host |
| `RESULTS-laptop.md` | measured numbers, laptop (smoke) |
| `RESULTS-vm.md` | measured numbers, Linux VM (the quotable ones); §8–§10 are the 2026-09-18 additions |
| `MEMO.md` | recommendation for D12 |
