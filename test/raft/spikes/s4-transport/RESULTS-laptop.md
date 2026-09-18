# S4 transport — laptop measurements (smoke, 2026-09-17)

> **These are smoke numbers and must never be quoted** (§0.3): macOS
> scheduling and its loopback stack are not the deployment. `RESULTS-vm.md`
> supersedes every figure here. Noted 2026-09-18 because PLAN_RAFT.md's D12
> rationale ("1.35 vs 2.77 µs, half the leader CPU") quotes *this* file; the
> VM measured 1.26 vs 2.15 = 0.59×, on 2 sockets against 15–49.

Host: `alice.local`, Apple M4, 10 cores, 24 GB, macOS 15.5, rustc 1.94.0,
release build (`opt-level=3`, thin LTO). Both processes on one host over
loopback, 4 tokio worker threads each. Command: `bash run.sh` (pass A) and
`ROUNDS=4 SECS=10 WARMUP=3 bash run.sh` (pass B), from
`test/raft/spikes/s4-transport`.

Per §0.3 these numbers are **smoke, not quotable**: the deployment is Linux,
this is loopback (RTT ≈ 40 µs against 0.2–1 ms pod to pod), and the laptop was
shared with the other phase-0 agents and with Alice's desktop for the whole
run. `results/*/runlog.txt` records the load average before and after every
single run; it moved between **3.9 and 52.7** on a 10-core machine. The
consequence is stated plainly below: p50, CPU per message and bytes per
message are usable and consistent across both passes; **the tails (p99 and
beyond) on this host measure the neighbours, not the transport.**

## What was measured

Offered load per configuration: 20 000 and 50 000 msg/s, batch 10 (so 2 000
and 5 000 forwarded commands/s), 256 B payload + 16 B dedup hash per message,
one request id per command. Open-loop pacer; `p50/p99` are measured from the
actual send to the outcome (`rt_send_*`). CPU is a `getrusage` delta over the
measured window on each process, so it is the work my two processes did, not
the machine's load. Bytes are counted on the raw socket, below TLS and below
HTTP framing.

Two passes, same binary:

- **pass A** (`results/laptop/`): one contiguous 60 s per configuration —
  what the WP asks for. 13 of 15 runs completed: I edited `run.sh` while bash
  was executing it, which killed the run after the 13th configuration (bash
  re-reads a script from a byte offset). The two missing connection-sweep
  rows were re-run afterwards, so the sweep rows come from a later, quieter
  window than the transport rows above them.
- **pass B** (`results/laptop-interleaved/`, per-run logs concatenated into
  `leader-all.log` / `receiver-all.log`): the same 15 configurations,
  4 rounds of 10 s each, round-robin (so 40 s measured per configuration
  spread over ~16 minutes). A load spike hits every configuration alike, so
  this is the pass to compare across transports. The table shows the median
  of the 4 rounds.

## Pass B — 15 configurations, median of 4 rounds of 10 s (the comparison table)

| config | offered msg/s | achieved msg/s | p50 us | p99 us | p99.9 us | max us | leader cores | recv cores | leader us/msg | recv us/msg | B/msg wire | conns open | conns opened | errors | rounds |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| tcp @20k/2conn | 20,000 | 19,999 | 99 | 426 | 2631 | 9724 | 0.046 | 0.100 | 2.33 | 4.98 | 294.2 | 2 | 2 | 0 | 4 |
| tcp+mac @20k/2conn | 20,000 | 20,000 | 124 | 4758 | 13401 | 22230 | 0.066 | 0.120 | 3.31 | 5.99 | 297.4 | 2 | 2 | 0 | 4 |
| tcp+tls @20k/2conn | 20,000 | 19,998 | 96 | 960 | 3442 | 17395 | 0.051 | 0.097 | 2.54 | 4.84 | 297.9 | 2 | 2 | 0 | 4 |
| http @20k/64conn | 20,000 | 19,998 | 94 | 474 | 1887 | 22945 | 0.065 | 0.124 | 3.23 | 6.17 | 309.7 | 35 | 35 | 0 | 4 |
| http+mac @20k/64conn | 20,000 | 20,001 | 108 | 373 | 2602 | 22978 | 0.092 | 0.145 | 4.61 | 7.28 | 314.9 | 20 | 20 | 0 | 4 |
| http+tls+mac @20k/64conn | 20,000 | 19,997 | 166 | 4468 | 9991 | 18637 | 0.109 | 0.173 | 5.44 | 8.69 | 319.4 | 37 | 37 | 0 | 4 |
| tcp @50k/2conn | 50,000 | 50,001 | 106 | 4378 | 9022 | 26334 | 0.068 | 0.160 | 1.35 | 3.20 | 294.2 | 2 | 2 | 0 | 4 |
| tcp+mac @50k/2conn | 50,000 | 49,998 | 156 | 790 | 6601 | 37526 | 0.119 | 0.211 | 2.37 | 4.23 | 297.4 | 2 | 2 | 0 | 4 |
| tcp+tls @50k/2conn | 50,000 | 49,988 | 97 | 274 | 1607 | 14813 | 0.069 | 0.133 | 1.37 | 2.67 | 296.9 | 2 | 2 | 0 | 4 |
| http @50k/64conn | 50,000 | 50,002 | 117 | 884 | 7120 | 99537 | 0.139 | 0.257 | 2.77 | 5.13 | 309.7 | 64 | 164 | 0 | 4 |
| http+mac @50k/64conn | 50,000 | 49,988 | 172 | 1607 | 13269 | 111038 | 0.227 | 0.344 | 4.54 | 6.89 | 314.9 | 61 | 156 | 0 | 4 |
| http+tls+mac @50k/64conn | 50,000 | 49,998 | 181 | 790 | 4657 | 101111 | 0.261 | 0.378 | 5.22 | 7.56 | 320.1 | 62 | 77 | 0 | 4 |
| tcp @50k/1conn | 50,000 | 49,999 | 96 | 277 | 2080 | 43561 | 0.040 | 0.124 | 0.80 | 2.49 | 294.2 | 1 | 1 | 0 | 4 |
| tcp @50k/4conn | 50,000 | 50,003 | 103 | 2318 | 8806 | 47630 | 0.100 | 0.187 | 2.00 | 3.75 | 294.2 | 4 | 4 | 0 | 4 |
| tcp @50k/8conn | 50,000 | 49,987 | 126 | 2410 | 6157 | 34885 | 0.107 | 0.190 | 2.15 | 3.81 | 294.2 | 8 | 8 | 0 | 4 |

`conns open` is the leader's live connection gauge at the end of the window;
`conns opened` is how many connections were opened in total, so the two differ
exactly when the HTTP pool churns.

## Pass B — within-round pairwise comparison (contention-robust)

Inside one round both configurations saw the same neighbours, so the ratio is
meaningful even where the absolute numbers are not. "rounds A lower" counts
how many of the 4 rounds put configuration A below B.


### offered 20,000 msg/s  (rounds compared pairwise)

| pair | metric | A/B median ratio | rounds A lower | n |
|---|---|---|---|---|
| framed TCP vs HTTP (no auth) | p50 | 0.93x | 3/4 | 4 |
| framed TCP vs HTTP (no auth) | leader CPU/msg | 0.68x | 3/4 | 4 |
| framed TCP vs HTTP (no auth) | recv CPU/msg | 0.77x | 3/4 | 4 |
| framed TCP vs HTTP (no auth) | bytes/msg | 0.95x | 4/4 | 4 |
| framed TCP vs HTTP (authenticated) | p50 | 1.27x | 1/4 | 4 |
| framed TCP vs HTTP (authenticated) | leader CPU/msg | 0.66x | 4/4 | 4 |
| framed TCP vs HTTP (authenticated) | recv CPU/msg | 0.75x | 3/4 | 4 |
| framed TCP vs HTTP (authenticated) | bytes/msg | 0.94x | 4/4 | 4 |
| framed TCP+TLS vs HTTPS+mac | p50 | 0.61x | 4/4 | 4 |
| framed TCP+TLS vs HTTPS+mac | leader CPU/msg | 0.47x | 4/4 | 4 |
| framed TCP+TLS vs HTTPS+mac | recv CPU/msg | 0.59x | 4/4 | 4 |
| framed TCP+TLS vs HTTPS+mac | bytes/msg | 0.93x | 4/4 | 4 |
| per-frame MAC cost (tcp+mac vs tcp) | p50 | 1.36x | 0/4 | 4 |
| per-frame MAC cost (tcp+mac vs tcp) | leader CPU/msg | 1.37x | 0/4 | 4 |
| per-frame MAC cost (tcp+mac vs tcp) | recv CPU/msg | 1.16x | 1/4 | 4 |
| per-frame MAC cost (tcp+mac vs tcp) | bytes/msg | 1.01x | 0/4 | 4 |
| TLS cost (tcp+tls vs tcp) | p50 | 1.01x | 2/4 | 4 |
| TLS cost (tcp+tls vs tcp) | leader CPU/msg | 1.10x | 2/4 | 4 |
| TLS cost (tcp+tls vs tcp) | recv CPU/msg | 0.96x | 2/4 | 4 |
| TLS cost (tcp+tls vs tcp) | bytes/msg | 1.01x | 0/4 | 4 |
| MAC vs TLS (tcp+mac vs tcp+tls) | p50 | 1.35x | 0/4 | 4 |
| MAC vs TLS (tcp+mac vs tcp+tls) | leader CPU/msg | 1.31x | 0/4 | 4 |
| MAC vs TLS (tcp+mac vs tcp+tls) | recv CPU/msg | 1.21x | 0/4 | 4 |
| MAC vs TLS (tcp+mac vs tcp+tls) | bytes/msg | 1.00x | 4/4 | 4 |

### offered 50,000 msg/s  (rounds compared pairwise)

| pair | metric | A/B median ratio | rounds A lower | n |
|---|---|---|---|---|
| framed TCP vs HTTP (no auth) | p50 | 0.84x | 4/4 | 4 |
| framed TCP vs HTTP (no auth) | leader CPU/msg | 0.49x | 4/4 | 4 |
| framed TCP vs HTTP (no auth) | recv CPU/msg | 0.61x | 4/4 | 4 |
| framed TCP vs HTTP (no auth) | bytes/msg | 0.95x | 4/4 | 4 |
| framed TCP vs HTTP (authenticated) | p50 | 1.01x | 2/4 | 4 |
| framed TCP vs HTTP (authenticated) | leader CPU/msg | 0.58x | 4/4 | 4 |
| framed TCP vs HTTP (authenticated) | recv CPU/msg | 0.69x | 4/4 | 4 |
| framed TCP vs HTTP (authenticated) | bytes/msg | 0.94x | 4/4 | 4 |
| framed TCP+TLS vs HTTPS+mac | p50 | 0.59x | 4/4 | 4 |
| framed TCP+TLS vs HTTPS+mac | leader CPU/msg | 0.29x | 4/4 | 4 |
| framed TCP+TLS vs HTTPS+mac | recv CPU/msg | 0.39x | 4/4 | 4 |
| framed TCP+TLS vs HTTPS+mac | bytes/msg | 0.93x | 4/4 | 4 |
| per-frame MAC cost (tcp+mac vs tcp) | p50 | 1.60x | 1/4 | 4 |
| per-frame MAC cost (tcp+mac vs tcp) | leader CPU/msg | 1.78x | 0/4 | 4 |
| per-frame MAC cost (tcp+mac vs tcp) | recv CPU/msg | 1.41x | 1/4 | 4 |
| per-frame MAC cost (tcp+mac vs tcp) | bytes/msg | 1.01x | 0/4 | 4 |
| TLS cost (tcp+tls vs tcp) | p50 | 0.93x | 2/4 | 4 |
| TLS cost (tcp+tls vs tcp) | leader CPU/msg | 1.02x | 2/4 | 4 |
| TLS cost (tcp+tls vs tcp) | recv CPU/msg | 0.83x | 2/4 | 4 |
| TLS cost (tcp+tls vs tcp) | bytes/msg | 1.01x | 0/4 | 4 |
| MAC vs TLS (tcp+mac vs tcp+tls) | p50 | 1.76x | 0/4 | 4 |
| MAC vs TLS (tcp+mac vs tcp+tls) | leader CPU/msg | 1.58x | 0/4 | 4 |
| MAC vs TLS (tcp+mac vs tcp+tls) | recv CPU/msg | 1.42x | 0/4 | 4 |
| MAC vs TLS (tcp+mac vs tcp+tls) | bytes/msg | 1.00x | 0/4 | 4 |

## Pass A — one contiguous 60 s per configuration

| config | offered msg/s | achieved msg/s | p50 us | p99 us | p99.9 us | max us | leader cores | recv cores | leader us/msg | recv us/msg | B/msg wire | conns open | conns opened | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| tcp @20k/2conn | 20,000 | 20,000 | 116 | 1603 | 6585 | 20974 | 0.056 | 0.117 | 2.81 | 5.87 | 294.2 | 2 | 2 | 0 |
| tcp+mac @20k/2conn | 20,000 | 20,000 | 155 | 2304 | 10468 | 45720 | 0.096 | 0.170 | 4.78 | 8.49 | 297.4 | 2 | 2 | 0 |
| tcp+tls @20k/2conn | 20,000 | 20,000 | 105 | 466 | 2336 | 14611 | 0.073 | 0.128 | 3.65 | 6.39 | 298.1 | 2 | 2 | 0 |
| http @20k/64conn | 20,000 | 19,999 | 135 | 4955 | 11080 | 20749 | 0.072 | 0.142 | 3.62 | 7.11 | 309.7 | 46 | 46 | 0 |
| http+mac @20k/64conn | 20,000 | 20,000 | 222 | 8926 | 23382 | 86524 | 0.128 | 0.226 | 6.41 | 11.32 | 314.9 | 64 | 241 | 0 |
| http+tls+mac @20k/64conn | 20,000 | 20,000 | 244 | 19352 | 111707 | 146669 | 0.142 | 0.232 | 7.11 | 11.59 | 320.2 | 64 | 1866 | 0 |
| tcp @50k/2conn | 50,000 | 50,001 | 136 | 7867 | 19080 | 34263 | 0.073 | 0.175 | 1.45 | 3.50 | 294.2 | 2 | 2 | 0 |
| tcp+mac @50k/2conn | 50,000 | 50,001 | 155 | 7853 | 17530 | 34394 | 0.126 | 0.221 | 2.52 | 4.41 | 297.4 | 2 | 2 | 0 |
| tcp+tls @50k/2conn | 50,000 | 49,999 | 110 | 2812 | 13212 | 29177 | 0.079 | 0.150 | 1.57 | 3.01 | 296.9 | 2 | 2 | 0 |
| http @50k/64conn | 50,000 | 50,000 | 126 | 2221 | 11366 | 30274 | 0.136 | 0.244 | 2.73 | 4.89 | 309.7 | 64 | 327 | 0 |
| http+mac @50k/64conn | 50,000 | 50,000 | 110 | 331 | 1021 | 7426 | 0.152 | 0.219 | 3.04 | 4.37 | 314.9 | 40 | 40 | 0 |
| http+tls+mac @50k/64conn | 50,000 | 49,997 | 137 | 1397 | 45682 | 108149 | 0.189 | 0.281 | 3.77 | 5.62 | 319.6 | 64 | 853 | 0 |
| tcp @50k/1conn | 50,000 | 50,000 | 97 | 459 | 3803 | 39505 | 0.036 | 0.122 | 0.71 | 2.45 | 294.2 | 1 | 1 | 0 |
| tcp @50k/1conn | 50,000 | 50,000 | 74 | 401 | 6913 | 17391 | 0.024 | 0.081 | 0.49 | 1.63 | 294.2 | 1 | 1 | 0 |
| tcp @50k/4conn | 50,000 | 50,001 | 100 | 327 | 988 | 7365 | 0.078 | 0.148 | 1.56 | 2.95 | 294.2 | 4 | 4 | 0 |
| tcp @50k/8conn | 50,000 | 49,999 | 110 | 544 | 1349 | 7434 | 0.109 | 0.190 | 2.17 | 3.81 | 294.2 | 8 | 8 | 0 |

## What the numbers say

1. **Both transports carried the offered rate.** Achieved = offered to within
   0.03% in every one of the 76 runs, and `cmds_bad` is 0 everywhere: no
   command was lost, mis-routed or answered with the wrong request id. 50 000
   msg/s = 5 000 forwarded commands/s = 13.6 MB/s of command bodies.

2. **Bytes on the wire per message** (272 B of that is the payload and the
   dedup hash, i.e. the bytes that must travel):

   | transport | B/msg | overhead over 272 B |
   |---|---|---|
   | framed TCP | 294.2 | +8.2% |
   | framed TCP + per-frame MAC | 297.4 | +9.3% |
   | framed TCP + TLS | 296.9–298.1 | +9.2–9.6% |
   | HTTP/1.1 | 309.7 | +13.9% |
   | HTTP/1.1 + MAC header | 314.9 | +15.8% |
   | HTTPS + MAC header | 319.4–320.1 | +17.4% |

   The framed figure is exact arithmetic, not an estimate: 2800 B of command
   body + 13 B of frame header + 116 B of outcome + 13 B = 2942 B per batch of
   10. HTTP adds ~155 B of request and response headers per command.

3. **CPU is where the transports actually differ.** At 50 000 msg/s the leader
   spent 1.35 µs of CPU per message on framed TCP and 2.77 µs on HTTP — 0.49×,
   in 4 rounds out of 4. With authentication on both sides (per-frame MAC vs
   MAC header) it is 2.37 vs 4.54 µs (0.58×, 4/4); with TLS on both sides,
   1.37 vs 5.22 µs (0.29×, 4/4). The receiver shows the same ordering.

4. **The per-frame MAC costs more than TLS on this host.** HMAC-SHA256 over
   the whole 2.8 KB body, twice per round trip on each side, is +78% leader CPU
   per message at 50k (4/4 rounds) and +60% on p50. TLS (rustls, ring,
   AES-256-GCM) is 1.02× the plaintext CPU — inside the noise — and +2.7 B per
   message. The reason is not that AES is magic: `sha2` 0.10.9 selects its
   hardware backend on aarch64 only with the `asm` feature
   (`sha2-0.10.9/src/sha256.rs:19`), so this laptop ran software SHA-256, while
   on x86-64 the same crate detects SHA-NI at runtime
   (`cpufeatures::new!(shani_cpuid, "sha", …)`, `src/sha256/x86.rs:100`).
   **The MAC/TLS ratio must therefore be re-measured on the VM** before it is
   used to decide D12; expect the MAC to look much better there.

5. **Connection count is the structural difference.** HTTP/1.1 carries one
   request per connection at a time, so the pool grows to the offered
   concurrency: 20–64 live connections against 2 for the framed transport, and
   it *churns* — in pass A the HTTPS configuration opened **1866** connections
   in 60 s to keep 64 alive, which puts a full TLS handshake on the forwarding
   path tens of times a second and is what the 100–147 ms maxima are made of.
   The framed transport opened exactly the 1, 2, 4 or 8 connections it was told
   to and kept them for the whole run.

6. **Fewer connections cost less CPU** on the framed transport, because the
   writer task coalesces whatever is queued into one `write_all`: at 50k the
   leader spent 0.80 µs/msg on 1 connection, 1.35 on 2, 2.00 on 4 and 2.15 on 8,
   with p50 flat (96–126 µs). The latency argument for more sockets does not
   appear at this rate; the CPU argument against them does.

7. **Handshake cost per connection** (connect + handshake, loopback, median of
   pass B): framed TCP with the mutual HMAC 306 µs, framed TCP + TLS 912 µs,
   HTTP first request 262 µs, HTTPS first request 564 µs. Paid once per
   connection for the framed transport; paid on the request path whenever the
   HTTP pool churns.

## Known biases and limits of this run

- **The host was shared.** Load average moved between 3.9 and 52.7 in pass A
  (min 4.1, max 14.4 in pass B), from the other phase-0 agents, OrbStack and a
  desktop session. Every p99/p99.9/max figure here is contaminated by that;
  the pairwise table is the honest read. On the quiet end, framed TCP p99 sat
  at 274–426 µs.
- **Loopback, not a network.** RTT ≈ 40 µs. A real pod-to-pod RTT (0.2–1 ms)
  adds a constant to both transports, but it multiplies HTTP's connection
  count (in-flight = command rate × RTT) while leaving the framed transport at
  the same 1–2 sockets.
- **The pacer has ~1 ms granularity** (tokio timer), so commands leave in
  bursts of 2 (20k) or 5 (50k) per millisecond; `rt_due_*` in the JSON carries
  that error, `rt_send_*` (used everywhere above) does not.
- **The framed path pays one copy the HTTP path does not**: the reader task
  copies each 2.8 KB frame body before handing it to the work task, where hyper
  passes `Bytes`. The CPU gap in the framed transport's favour is therefore
  slightly understated.
- **No Raft, no disk, no planner.** This is the transport alone. The real
  forward answers only after commit + apply (D7).
- After the measurements the MAC comparisons in `auth.rs`/`frame.rs` were
  changed from `!=` on the tag to hmac's constant-time `verify_slice` /
  `verify_truncated_left`. The HMAC computation is identical, so the CPU
  figures stand; `cargo test --release` (8 tests) passed on the changed code.
