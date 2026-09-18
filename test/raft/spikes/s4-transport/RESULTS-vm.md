# S4 transport — Linux VM measurements (the quotable ones, 2026-09-17)

Host: `queenpgless-01`, 164.90.215.224 (§13.6 / WP-0.2 host). Ubuntu 24.04,
Linux 6.8.0-124-generic, x86_64, 8 vCPU, 15 GB. QEMU guest
(`pc-i440fx-6.1`, CPU family 6 model 106) on an **INTEL(R) XEON(R) GOLD
6548N**, BIOS clock 2.0 GHz, 8 cores / 1 thread per core, no SMT exposed.
rustc 1.98.1 (recorded in `results/vm-b/host.txt`), release build
(`opt-level=3`, thin LTO). Both processes on the VM over loopback, 4 tokio
worker threads each. The machine was idle: `runlog.txt` records the load
average before and after every single run and it never left **0.03–1.83**
(against 3.9–52.7 on the laptop).

Per §0.3 these are the numbers to quote. The laptop pass
(`RESULTS-laptop.md`) stays as smoke.

**Two independent 60 s passes of the same 15 configurations ran back to
back**, which is what makes single 60 s runs usable here:

| pass | directory | when (UTC) | remote dir | note |
|---|---|---|---|---|
| A | `results/vm/` (and `results/vm-a/`) | 2026-09-17 15:08–15:25 | `/root/raft/wp-0.6/s4-transport` | queued by the coordinator, `TAG=vm SECS=60 PORT=6734 bash run.sh` |
| B | `results/vm-b/` | 2026-09-17 15:26–15:43 | `/root/raft/wp06-part2/s4-transport` | repeat on the same VM and binary; `build.log`, `test.log` (8 unit tests, rc=0) and `host.txt` are beside it |

`results/vm-a/` and `results/vm/` are the same pass: `results.jsonl` and
`runlog.txt` are byte-identical (`cmp`), but the *directories* are not —
`cert.der` exists only in `vm/` and `queue.log` only in `vm-a/`. (Corrected
2026-09-18: this file used to call `vm-a/` a byte-identical copy.)

Nothing was re-run for this write-up: all 30 rows were already in the repo and
all 15 configurations are present in both passes. **What was checked, and how**
(corrected 2026-09-18 — the earlier claim "`rc=0` for every one" was not the
check performed: `run.sh` takes `local RC=$?` after the pipeline
`"$BIN" receiver … | grep | sed >> results.jsonl`, so it records `sed`'s
status; a receiver that died mid-run would still have logged `rc=0`):

- one JSON row per configuration, 15 in each pass, same configuration set;
- `cmds_bad = 0` in every row (a row counts a mismatched request id as bad);
- `achieved_msgs_s` = offered to within 0.005 % in every row;
- `cmds_ok` = `achieved_cmds_s × secs` to within 1 % in every row, which is
  what actually rules out a receiver that stopped early.

The only other command run from here was the read-only CPU-feature check
quoted below. `pgrep -af raft` / `pgrep -af s4` on the VM: nothing left
running.

**A second VM session on 2026-09-18** added four configurations the matrix
never had plus a pacer comparison; see §8 and §9. They do not replace anything
above.

## What was measured

Offered load per configuration: 20 000 and 50 000 msg/s, batch 10 (2 000 and
5 000 forwarded commands/s), 256 B payload + 16 B dedup hash per message, one
request id per command, 60 s measured after 5 s of warm-up. Open-loop pacer.
`p50/p99` are from the actual send to the outcome (`rt_send_*`). CPU is a
`getrusage(RUSAGE_SELF)` delta over the measured window on each process.
Bytes are counted on the raw `TcpStream`, below rustls and below HTTP
framing. Crypto: rustls 0.23.45 with the ring 0.17.14 provider (TLS 1.3,
default suites); the per-frame MAC and the handshake use hmac 0.12.1 +
sha2 0.10.9 (HMAC-SHA256 truncated to 16 B).

Across both passes: **6 839 965 forwarded commands, 68 399 650 messages, 0
errors, 0 outcomes with a mismatched request id.** Achieved = offered to
within 0.006 % in all 30 runs.

## The 15 configurations — pass A / pass B

Every cell is `pass A / pass B`. Reproduced with
`python3 table.py results/vm/results.jsonl` and
`python3 table.py results/vm-b/results.jsonl`.

| config | offered msg/s | achieved | p50 µs | p99 µs | p99.9 µs | max µs | leader cores | recv cores | leader µs/msg | recv µs/msg | B/msg | conns open | conns opened | handshake µs | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| tcp @20k/2conn | 20,000 | 20,000 / 19,999 | 60 / 59 | 110 / 111 | 144 / 155 | 462 / 2202 | 0.044 / 0.045 | 0.080 / 0.082 | 2.22 / 2.27 | 4.02 / 4.10 | 294.2 / 294.2 | 2 / 2 | 2 / 2 | 285 / 311 | 0 / 0 |
| tcp+mac @20k/2conn | 20,000 | 20,000 / 20,000 | 89 / 89 | 144 / 147 | 175 / 187 | 1446 / 2003 | 0.068 / 0.069 | 0.114 / 0.116 | 3.38 / 3.43 | 5.68 / 5.80 | 297.4 / 297.4 | 2 / 2 | 2 / 2 | 329 / 305 | 0 / 0 |
| tcp+tls @20k/2conn | 20,000 | 20,000 / 20,000 | 63 / 63 | 114 / 116 | 148 / 159 | 1599 / 3140 | 0.050 / 0.051 | 0.089 / 0.089 | 2.48 / 2.53 | 4.47 / 4.47 | 298.2 / 298.2 | 2 / 2 | 2 / 2 | 820 / 798 | 0 / 0 |
| http @20k/64conn | 20,000 | 20,000 / 20,000 | 71 / 72 | 121 / 127 | 159 / 169 | 1182 / 1902 | 0.058 / 0.058 | 0.121 / 0.119 | 2.88 / 2.88 | 6.04 / 5.97 | 309.7 / 309.7 | 5 / 7 | 5 / 7 | 187 / 160 | 0 / 0 |
| http+mac @20k/64conn | 20,000 | 20,000 / 20,000 | 96 / 102 | 153 / 167 | 197 / 213 | 797 / 1369 | 0.088 / 0.089 | 0.147 / 0.143 | 4.42 / 4.44 | 7.35 / 7.14 | 314.9 / 314.9 | 5 / 8 | 5 / 8 | 188 / 189 | 0 / 0 |
| http+tls+mac @20k/64conn | 20,000 | 20,000 / 20,001 | 107 / 110 | 171 / 175 | 218 / 217 | 2073 / 832 | 0.098 / 0.098 | 0.157 / 0.156 | 4.90 / 4.91 | 7.84 / 7.81 | 319.3 / 319.3 | 7 / 7 | 7 / 7 | 410 / 362 | 0 / 0 |
| tcp @50k/2conn | 50,000 | 50,000 / 49,999 | 63 / 62 | 113 / 112 | 156 / 150 | 1239 / 770 | 0.064 / 0.062 | 0.135 / 0.128 | 1.27 / 1.25 | 2.70 / 2.56 | 294.2 / 294.2 | 2 / 2 | 2 / 2 | 310 / 289 | 0 / 0 |
| tcp+mac @50k/2conn | 50,000 | 50,000 / 50,000 | 125 / 125 | 203 / 199 | 261 / 242 | 2003 / 745 | 0.123 / 0.124 | 0.215 / 0.215 | 2.46 / 2.49 | 4.31 / 4.30 | 297.4 / 297.4 | 2 / 2 | 2 / 2 | 317 / 325 | 0 / 0 |
| tcp+tls @50k/2conn | 50,000 | 50,000 / 50,000 | 69 / 67 | 122 / 118 | 164 / 156 | 1720 / 639 | 0.067 / 0.070 | 0.136 / 0.141 | 1.34 / 1.40 | 2.71 / 2.82 | 297.0 / 297.1 | 2 / 2 | 2 / 2 | 796 / 852 | 0 / 0 |
| http @50k/64conn | 50,000 | 49,999 / 49,999 | 71 / 70 | 135 / 127 | 191 / 173 | 1282 / 8808 | 0.107 / 0.108 | 0.205 / 0.205 | 2.15 / 2.16 | 4.11 / 4.09 | 309.7 / 309.7 | 15 / 49 | 15 / 49 | 200 / 189 | 0 / 0 |
| http+mac @50k/64conn | 50,000 | 50,000 / 50,000 | 104 / 103 | 177 / 172 | 239 / 246 | 1486 / 1103 | 0.192 / 0.191 | 0.286 / 0.287 | 3.85 / 3.83 | 5.72 / 5.73 | 314.9 / 314.9 | 12 / 18 | 12 / 18 | 187 / 174 | 0 / 0 |
| http+tls+mac @50k/64conn | 50,000 | 50,000 / 50,000 | 112 / 113 | 188 / 199 | 262 / 269 | 1520 / 826 | 0.211 / 0.211 | 0.307 / 0.308 | 4.22 / 4.22 | 6.14 / 6.15 | 319.3 / 319.3 | 19 / 13 | 19 / 13 | 364 / 370 | 0 / 0 |
| tcp @50k/1conn | 50,000 | 50,000 / 50,000 | 71 / 72 | 130 / 131 | 177 / 183 | 1032 / 1816 | 0.032 / 0.031 | 0.114 / 0.112 | 0.63 / 0.62 | 2.28 / 2.24 | 294.2 / 294.2 | 1 / 1 | 1 / 1 | 497 / 454 | 0 / 0 |
| tcp @50k/4conn | 50,000 | 50,000 / 50,000 | 62 / 60 | 113 / 109 | 158 / 154 | 950 / 1883 | 0.081 / 0.083 | 0.145 / 0.145 | 1.63 / 1.67 | 2.89 / 2.91 | 294.2 / 294.2 | 4 / 4 | 4 / 4 | 206 / 232 | 0 / 0 |
| tcp @50k/8conn | 50,000 | 50,001 / 49,999 | 60 / 61 | 110 / 113 | 146 / 150 | 844 / 839 | 0.095 / 0.094 | 0.147 / 0.150 | 1.90 / 1.87 | 2.95 / 3.00 | 294.2 / 294.2 | 8 / 8 | 8 / 8 | 180 / 173 | 0 / 0 |

`@64conn` on the HTTP rows is `pool_max_idle_per_host`, the ceiling; `conns
open` is what the pool actually held at the end of the window.

**Repeatability, pass A against pass B** (the reason single 60 s runs are
enough on this host): `leader_cpu_us_per_msg` differs by **0.1–4.9 %** in all
15 configurations (median 1.6 %), `recv_cpu_us_per_msg` by 0.1–5.4 %, p50 by
0–6 µs, p99 by 0–14 µs, bytes/msg not at all. The two columns that do move
are `max` (0.6–8.8 ms, one-off scheduling) and the HTTP pool size (below).

Pairwise ratios: `python3 pairs.py results/vm/results.jsonl` (n = 1 per pair,
so read it as the ratio, not as a vote count).

## 1. Round trip

Loopback, idle host, so these are the transport's own service times with no
network in them:

| | framed TCP | +per-frame MAC | +TLS | HTTP/1.1 | +MAC | HTTPS+MAC |
|---|---|---|---|---|---|---|
| p50 @20k | 59–60 | 89 | 63 | 71–72 | 96–102 | 107–110 |
| p99 @20k | 110–111 | 144–147 | 114–116 | 121–127 | 153–167 | 171–175 |
| p50 @50k | 62–63 | 125 | 67–69 | 70–71 | 103–104 | 112–113 |
| p99 @50k | 112–113 | 199–203 | 118–122 | 127–135 | 172–177 | 188–199 |
| p99.9 @50k | 150–156 | 242–261 | 156–164 | 173–191 | 239–246 | 262–269 |

The tails are real here, unlike on the laptop: p99 is 1.6–1.9× p50 and p99.9
is 2.0–2.5× p50 in every configuration, and the worst single sample over 60 s
of any framed run was 3.1 ms. The laptop's p99 of 4378 µs for framed TCP at
50 k was measuring its neighbours, not the transport.

The pacer's own error is unchanged by the host: `rt_due_p50` sits at
673–805 µs and `pace_lag_max_us` at 1.9–3.6 ms in every row, because tokio's
timer has ~1 ms granularity and at 5 000 cmd/s commands leave in bursts of
about five. Nothing above uses `rt_due_*`.

## 2. CPU

Per process, at the offered rate, on 8 vCPU:

| configuration | leader cores | recv cores | leader µs/msg | leader µs/command |
|---|---|---|---|---|
| framed TCP @50k | 0.062–0.064 | 0.128–0.135 | 1.25–1.27 | 12.6 |
| framed TCP+MAC @50k | 0.123–0.124 | 0.215 | 2.46–2.49 | 24.7 |
| framed TCP+TLS @50k | 0.067–0.070 | 0.136–0.141 | 1.34–1.40 | 13.7 |
| HTTP/1.1 @50k | 0.107–0.108 | 0.205 | 2.15–2.16 | 21.5 |
| HTTP/1.1+MAC @50k | 0.191–0.192 | 0.286–0.287 | 3.83–3.85 | 38.4 |
| HTTPS+MAC @50k | 0.211 | 0.307–0.308 | 4.22 | 42.2 |

Framed against HTTP, leader CPU per message (mean of both passes):

| | no auth | authenticated | encrypted, as first written (tcp+tls vs https+**mac**) |
|---|---|---|---|
| @20k | 0.78× | 0.77× | ~~0.51×~~ |
| @50k | **0.59×** | **0.64×** | ~~0.32×~~ |

**The "encrypted" column above is wrong and is superseded by §8.** It divides
framed+TLS by HTTPS *plus a per-request HMAC*, because the matrix has no
HTTPS-without-MAC row: `run.sh` only ever ran `one http 1 1`. The missing cell
was measured on 2026-09-18 on the same binary; the like-for-like ratios are
**0.46× @50k and 0.61× @20k** (§8). The no-auth and authenticated columns are
like-for-like and stand — with the socket-count and arrival-shape caveats of
§6 and §9 attached.

The receiver column follows the same ordering (0.64–0.79× at 50k) but it is
an upper bound on transport cost, not a clean measurement: the harness's
receiver also clones a 2.8 KB template, arms a tokio timer, spawns a task and
sends a latency sample for **every** command. The leader column is the honest
one — it does nothing but read, decode, answer and account.

RSS stayed at 4.1–5.9 MB (leader) and 6.7–13.8 MB (receiver) in every run.

Neither process ever came near saturation: the busiest configuration used
0.21 of 8 cores on the leader. Everything above is a per-message cost at low
utilisation, not a capacity number.

## 3. Bytes per message

Identical to the laptop, as it must be — this is arithmetic, not a
measurement, and both passes agree to the last printed digit. 272 B of each
message has to travel (256 payload + 16 dedup hash, D10):

| transport | up B/msg | down B/msg | total | over 272 B |
|---|---|---|---|---|
| framed TCP | 281.3 | 12.9 | 294.2 | +8.2 % |
| framed TCP + per-frame MAC | 282.9 | 14.5 | 297.4 | +9.3 % |
| framed TCP + TLS | 283.3–283.5 | 13.7–14.7 | 297.0–298.2 | +9.2–9.6 % |
| HTTP/1.1 | 290.4 | 19.3 | 309.7 | +13.9 % |
| HTTP/1.1 + MAC header | 295.6 | 19.3 | 314.9 | +15.8 % |
| HTTPS + MAC header | 297.8 | 21.5 | 319.3 | +17.4 % |

Per command (batch of 10): framed is 2813 B up (2800 body + 13 B frame
header) and 129 B down (116 B outcome + 13 B); HTTP is 2904 up and 193 down,
i.e. **+155 B of headers per command**, 91 on the request and 64 on the
response. TLS adds 28–40 B per command over the framed baseline; the
per-frame MAC adds exactly 32 B (16 per direction).

## 4. Connections and handshake

The framed transport opened exactly the connections it was told to (1, 2, 4
or 8), kept them for the whole 60 s, and `conns opened` equals `conns open`
in every single row of both passes.

The HTTP pool grew with concurrency and did not churn on this host:
5–8 connections at 20 k, 12–49 at 50 k, with `opened == open` in all twelve
HTTP rows. **The laptop's churn did not reproduce**: there, one 60 s HTTPS
run opened 1866 connections to keep 64 alive and produced 100–147 ms maxima.
On the idle VM the worst HTTP maximum was 8.8 ms. That part of the laptop
write-up was a contended-macOS artefact and should not be quoted. What does
reproduce is the structural point: 12–49 sockets against 2 for the same work,
and the pool size is not stable even between two identical passes (15 against
49 for HTTP plain at 50 k, 19 against 13 for HTTPS+MAC) because it follows
whatever the burst concurrency happened to be.

Handshake, mean wall time of connect + handshake over the connections a run
opened:

| | µs per connection |
|---|---|
| framed TCP, mutual HMAC (§9.2) | 173–232 (8 and 4 conns) · 285–329 (2 conns) · 454–497 (1 conn, a single cold sample) |
| framed TCP + TLS, mutual HMAC over rustls | 796–852 |
| HTTP/1.1 first request | 160–200 |
| HTTPS first request | 362–410 |

TLS costs about +0.5 ms once per framed connection, which for a pod means
twice at start-up per peer pool. HTTPS costs +0.2 ms per connection the pool
opens, which is on the request path whenever the pool grows.

## 5. tcp+mac against tcp+tls on x86-64 — the question the laptop could not answer

The laptop found the per-frame HMAC-SHA256 much more expensive than TLS and
blamed aarch64: `sha2` 0.10.9 takes its hardware path on aarch64 only with the
`asm` feature, while on x86-64 it detects SHA-NI at run time. The memo asked
the VM to settle it. **The VM reproduces the same ordering, and the reason is
the same class of artefact, so the question is still open** (see Deferred):

```
$ ssh root@164.90.215.224 'grep -c sha_ni /proc/cpuinfo; lscpu | head -20'
0
... INTEL(R) XEON(R) GOLD 6548N, BIOS Model name: pc-i440fx-6.1 CPU @ 2.0GHz,
    CPU family 6, Model 106, 8 CPU(s)
```

The guest exposes `aes`, `vaes`, `pclmulqdq`, `avx2`, `avx512f`, `gfni`,
`vpclmulqdq` — and **not** `sha_ni`. The physical part is Emerald Rapids but
the QEMU CPU model the droplet presents is Icelake-Server-shaped, and it
masks the SHA extensions. `results/vm-b/host.txt` recorded `sha_ni_lines=0
aes_lines=8` at run time, so this was the state during both passes. So on
this host too, AES-GCM runs on hardware and SHA-256 runs in software.

Measured added cost over plain framed TCP, at 50 k (mean of both passes):

| | leader µs/msg | recv µs/msg | p50 µs | B/msg |
|---|---|---|---|---|
| framed TCP | 1.260 | 2.627 | 62.5 | 294.2 |
| + per-frame MAC | 2.474 (**+96 %**) | 4.304 (+64 %) | 125.0 (**+100 %**) | 297.4 |
| + TLS | 1.370 (**+9 %**) | 2.767 (+5 %) | 68.0 (**+9 %**) | 297.1 |

At 20 k: MAC +52 % leader CPU and +50 % p50; TLS +12 % and +6 %.

Per command the leader hashes or encrypts ~2918 B (it verifies the 2801 B
request and signs/encrypts the 117 B outcome):

- per-frame MAC: **+12.1 µs/command ⇒ 4.16 ns/B ⇒ 0.24 GB/s** — software
  HMAC-SHA256, exactly what a CPU without SHA-NI gives.
- TLS: **+1.10 µs/command ⇒ 0.38 ns/B ⇒ 2.65 GB/s** — hardware AES-GCM,
  consistent with the `aes`/`vaes`/`pclmulqdq` flags that *are* exposed.

That is an **11× difference in the added CPU**, and it is the whole of the
p50 difference too, because of where the work lands: on the framed transport
the MAC is computed inside the per-connection reader and writer tasks, which
are the serial part of that path. That shows up as the one place framed TCP
loses on latency: authenticated framed p50 at 50 k is **125 µs against
HTTP+MAC's 104 µs** (1.20×), even though its CPU per message is 0.64× — HTTP
spread the same HMAC work over 12–18 connections and hyper's per-connection
tasks. TLS, at 1.1 µs/command, does not move p50 measurably (68 vs 62 µs).

## 6. Connection-count sweep, framed TCP at 50 k (mean of both passes)

| conns | leader µs/msg | leader cores | recv µs/msg | p50 µs | p99 µs |
|---|---|---|---|---|---|
| 1 | 0.627 | 0.032 | 2.259 | 71.5 | 130.5 |
| 2 | 1.260 | 0.063 | 2.627 | 62.5 | 112.5 |
| 4 | 1.647 | 0.082 | 2.899 | 61.0 | 111.0 |
| 8 | 1.886 | 0.095 | 2.973 | 60.5 | 111.5 |

CPU per message doubles from 1 to 2 connections and then rises more slowly,
because the writer task coalesces whatever is queued into one `write_all`, so
fewer sockets means bigger writes. Latency moves the other way and then
stops: 1 connection costs +9 µs on p50 and +18 µs on p99 against 2, while 4
and 8 buy at most another 2 µs for 1.3–1.5× the CPU.

**Do not read a pool size out of this table** (corrected 2026-09-18; the memo
used to call 2 "the knee"). Two things this sweep cannot see:

- the +9 µs is loopback latency, on a path that D7 makes wait for commit *and*
  apply — S3 measured ~3 ms p50 for that, so the saving is ~0.3 % of the real
  round trip, bought with double the leader CPU per message;
- the sweep ran at a mean of **0.31 commands in flight** (5 000 cmd/s ×
  62 µs). With the commit + apply wait, the real depth is 15–35 per receiver,
  50–100× deeper, which is a different point on the coalescing curve.

What the table does establish is the mechanism: on the framed transport the
per-message CPU is a function of how many commands share a write, and §9 shows
how much of that is the harness's own arrival shape.

## 7. What changed against the laptop

| configuration @50k | p50 µs laptop → VM | leader µs/msg laptop → VM | live conns laptop → VM |
|---|---|---|---|
| framed TCP | 106 → 62 | 1.35 → 1.26 | 2 → 2 |
| framed TCP+MAC | 156 → 125 | 2.37 → 2.47 | 2 → 2 |
| framed TCP+TLS | 97 → 68 | 1.37 → 1.37 | 2 → 2 |
| HTTP/1.1 | 117 → 70 | 2.77 → 2.15 | 64 (164 opened) → 15–49 (no churn) |
| HTTPS+MAC | 181 → 112 | 5.22 → 4.22 | 62 (77 opened) → 13–19 (no churn) |

- **Latency**: 30–40 % lower on the VM, and the tails collapse (framed p99
  4378 → 113 µs). That is the quiet host, not the hardware: the 2.0 GHz Xeon
  is slower per core than the M4.
- **CPU per message**: within a few per cent for framed TCP and TLS, ~20 %
  cheaper for HTTP. Every ratio between transports held.
- **Bytes**: identical.
- **Every conclusion of the laptop write-up survives except the HTTP
  connection churn**, which was macOS under contention (§4).

> **The laptop column of this table is the one PLAN_RAFT.md D12 quotes**
> ("1.35 vs 2.77 µs, half the leader CPU"). §0.3 forbids quoting laptop
> numbers: the measured VM figures are 1.26 vs 2.15 = 0.59× on 2 sockets
> against 15–49 (0.88× at matched sockets, §6/§9). Flagged 2026-09-18;
> PLAN_RAFT.md is not this WP's file to edit.

## 8. The four cells the matrix never had (2026-09-18, `results/vm-c/`)

Added after the WP-0.6 refutation. Same VM, same **pass A binary**, unchanged
(`md5 7ecbc51709f9b37f0d18c7ef8a43ded6`, built 2026-09-17 15:08), 60 s each
after 5 s of warm-up, load average 0.00–0.90 throughout (`vm-c/runlog.txt`).
Driver: `run-vm-refute.sh` phase 1 (`PART=extra bash run.sh`).

| configuration | p50 µs | p99 µs | p99.9 µs | leader µs/msg | recv µs/msg | B/msg | conns open / opened |
|---|---|---|---|---|---|---|---|
| HTTPS, **no MAC**, @50k | 109 | 183 | 227 | **3.330** | 5.872 | 314.1 | 64 / **198** |
| HTTPS, **no MAC**, @20k | 111 | 189 | 245 | **4.626** | 8.677 | 314.1 | 5 / 5 |
| HTTP/1.1, pool capped at 2, @50k | 128 | 224 | 281 | **5.467** | 9.396 | 309.7 | 2 / **218 695** |
| framed TCP @50k/2conn — **control** | 76 | 130 | 167 | **1.419** | 3.278 | 294.2 | 2 / 2 |

`cmds_bad = 0` and achieved = offered in all four.

**The control is 12.6 % above pass A/B** (1.419 against 1.260 µs/msg; p50 76
against 62.5). The host was idle both days; the difference is the day, so the
ratios below correct the pass A/B framed rows by that factor rather than
comparing across days.

1. **The encrypted comparison, at last like for like.** Framed+TLS scaled to
   this day is 1.543 µs/msg against HTTPS-plain's 3.330: **0.46×** at 50 k
   (uncorrected, 0.41×). At 20 k: framed+TLS 2.505 → 2.821 against 4.626,
   **0.61×**. The memo's 0.32× / 0.51× compared TLS-only against TLS+HMAC and
   are withdrawn (§2).
   Note this also refutes the reviews' *arithmetic* estimate of ~0.54×: it
   assumed TLS costs the HTTP path what the MAC costs it (+0.38 µs/msg).
   Measured, TLS costs the HTTP path **+0.7 to +0.9 µs/msg** — 3.330 against a
   day-corrected HTTP-plain of 2.425 (pass A/B × 1.126) or against the 2.632
   of the same-week 40 s run in §9 — while on the framed path it costs
   **+0.11 µs/msg**. Six to eight times more, because framed encrypts one TLS
   record per coalesced write while HTTP encrypts per request.
2. **HTTP/1.1 cannot be held to a socket budget.** `pool_max_idle_per_host`
   caps *idle* sockets, not concurrent ones: with it at 2, hyper opened
   **218 695 connections in 60 s** (3 645/s), kept 2 idle, and cost **3.85×**
   the framed leader CPU (5.467 vs 1.419). So the socket-matched comparison
   can only be made in the other direction — framed raised to 8 sockets
   (1.886 µs/msg against HTTP's 2.154 on 15–49 sockets, **0.88×**, §6).
3. **The HTTPS pool churned where HTTPS+MAC had not**: 198 connections opened
   to hold 64 at 50 k, against 13–19 opened and held in the original HTTPS+MAC
   rows. Pool size and churn are not a property of the load here; they are a
   property of whatever hyper's pool happened to do. That instability is
   itself the argument of §4.

## 9. Does the framed CPU advantage survive a jitter-free pacer? (`results/vm-pace/`)

The framed writer coalesces everything queued into one `write_all`, and the
harness's tokio pacer releases commands in bursts of about five per
millisecond (§1), so part of the framed advantage is the harness's arrival
shape rather than the transport. `--pace spin` (added 2026-09-18) keeps the
same due times but lands on them within microseconds. Four 40 s runs, one
binary, same hour, framed on 2 sockets:

| pacer | due-time p50 | pace lag max | framed µs/msg | HTTP µs/msg | ratio | framed p50 | HTTP p50 |
|---|---|---|---|---|---|---|---|
| timer (as measured in passes A/B) | 718 / 777 µs | 2.0 / 2.1 ms | 1.416 | 2.632 | **0.54×** | 77 µs | 93 µs |
| spin (jitter-free) | 52 / 60 µs | 0.9 ms | 1.781 | 2.684 | **0.66×** | 39 µs | 47 µs |

- Removing the bursts costs the framed transport **+26 %** CPU per message and
  HTTP **+2 %** — which is the coalescing effect, measured, and it is the
  right size: at ~5 commands per write the framed path saves roughly four
  write syscalls out of five, and HTTP/1.1 cannot coalesce at any pool size
  because one connection carries one request at a time.
- The ordering and the recommendation survive: **0.66×**, on 2 sockets against
  8–12, with the same 5 % byte saving.
- Both p50s halve. Much of the "62 µs loopback p50" of §1 was commands waiting
  behind their own burst, not transport service time. p99.9 and max get worse
  under `spin` (294/2963 µs framed) because the busy-wait competes with the
  receiver's own workers — `recv µs/msg` rises to 24–25 and is meaningless in
  these rows. The leader column is unaffected by the receiver's pacer and is
  the one compared above.

## 10. Micro-benchmark: what the per-command crypto actually costs (`s4 bench`)

Run on the VM on 2026-09-18, 20 000 iterations × 5 rounds, minimum taken
(`./target/release/s4 bench --iters 20000 --rounds 5`, saved as
`results/vm-c/bench-vm.txt`). Another phase-0
job (S1) started on the VM at about this time, so treat these as bounds; the
minimum-of-rounds and the agreement with the end-to-end rows below are what
make them usable.

| operation, on a 2801 B body | ns |
|---|---|
| HMAC-SHA256 over the body | 9 023 |
| hex of a 16 B tag (16 × `format!`) | 492 |
| `String ==` on two 32-char hex tags | 2 |
| HMAC + constant-time verify (`verify_truncated_left`) | 9 094 |
| frame encode + sequenced MAC | 10 062 |
| frame encode, no MAC | 178 |

- **0.31 GB/s** for HMAC-SHA256, against the 0.24 GB/s derived end-to-end in
  §5 and the 2.65 GB/s of AES-GCM. Two independent measurements of the same
  software-SHA-256 fact.
- **The HTTP MAC is not a straw man in any way that matters.** Its hex
  formatting and `String ==` are 5.2 % of the MAC operation and ~1.3 % of that
  row's leader CPU; removing them moves the authenticated ratio 0.645 →
  0.653. The opposite bias — the HTTP path MACs only the request while the
  framed path MACs both directions — is of the same order and pushes the other
  way. Net effect on §2's authenticated column: under 2 %.
- **The sequenced MAC costs what it should**: 13 B more HMAC input per frame,
  9 884 ns against the 9 023 + framing of the unsequenced form, i.e. +0.4 %,
  an order of magnitude below the pass-to-pass spread.

## Known biases and limits of this run

- **Loopback, not a network.** Both processes on one VM; RTT ≈ 60 µs round
  trip, against 0.2–1 ms pod to pod. No NIC, no MTU segmentation, no
  congestion control, no interrupt or softirq cost. The byte counts transfer;
  the syscall-and-driver share of the CPU figures does not.
- **The guest has no SHA-NI** (§5). The MAC-vs-TLS ratio measured here is the
  no-SHA-NI case on both hosts tested so far.
- **Far from saturation** (leader ≤ 0.21 of 8 cores). Bounded per-connection
  queues (mpsc capacity 4096) were never exercised; no backpressure was
  observed because none was reached.
- **The receiver's CPU column includes harness work** (template clone, pacer
  timer, task spawn, latency sample per command).
- **One rate shape only**: batch 10, 256 B payload, one queue, one partition.
  Arrivals are *not* uniform: the pacer's ~1 ms timer granularity releases
  about five commands per millisecond, which is what the framed writer
  coalesces — §9 measures how much of the framed advantage that is worth. No
  large frames, no mixed sizes.
- **The code changed after these runs.** The 2026-09-18 refutation round fixed
  three defects in the spike (a deadline and waiter eviction in
  `tcpx.rs::round_trip`, a fail-closed secret check, a sequenced per-frame
  MAC). None of them is on the leader's hot path for these rows, and the rows
  above were all produced by the untouched binary; `results/vm-pace/` ran on
  the same code plus the `--pace` option. MEMO.md §Refutations lists them.
- **No Raft, no disk, no planner.** The real forward answers only after commit
  and apply (D7), which is where the latency of a forwarded write will
  actually come from.
- **`max` is a single sample per run** and moved by up to 10× between the two
  passes; p99.9 is the deepest column worth quoting.

## Deferred — what these rows cannot show

1. **SHA-NI, still unanswered.** The memo asked the VM to settle MAC vs TLS on
   x86-64; this VM cannot, because its hypervisor masks the SHA extensions.
   The honest statement is: *where SHA-NI is absent, TLS costs 11× less CPU
   than a per-frame HMAC-SHA256.* Scaled from the 0.24 GB/s measured here to
   the 1.5–2 GB/s SHA-NI is normally worth, the MAC's +1.21 µs/msg would fall
   to roughly +0.15–0.20 µs/msg, i.e. near TLS's +0.11. Settling it needs
   either a VM whose CPU model exposes `sha_ni`, or a micro-benchmark of
   `hmac`+`sha2` over 2.8 KB on such a CPU. Until then the design must not
   assume the flag is there: it is a cloud CPU-model decision, not ours.
2. **A real RTT.** Not approximated: `tc qdisc add dev lo root netem delay
   500us` changes a VM-wide setting and needs Alice's approval (§0.3), so the
   netem run did not happen. What the measured rows support is only the
   arithmetic: at 5 000 commands/s the *mean* number of commands in flight is
   `rate × round trip` = 0.31 at this loopback round trip, 1.3 at +200 µs,
   2.8 at +500 µs, 5.3 at +1 ms. The framed transport multiplexes all of them
   on its configured 1–2 sockets; HTTP/1.1 needs one connection per in-flight
   request, so that mean is its floor. Its *peak* is what actually sizes the
   pool, and here it was 40–140× the mean (15–49 sockets for a mean of 0.35)
   because arrivals are bursty — how that peak scales with RTT is not
   measured and should not be extrapolated from these rows.
3. **Head-of-line blocking behind a big frame.** One writer task per socket
   serialises frames; a 96 MiB entry (`QUEEN_RAFT_ENTRY_MAX_BYTES`) ahead of a
   heartbeat is exactly what §12.5 forbids. This spike only ever wrote 2.8 KB
   frames, so the separate-pools rule (Raft RPC / forwarding / snapshots) is
   asserted, not tested. §5 gives the first evidence that *any* per-frame work
   on the reader/writer task lands on the critical path.
4. **Failure behaviour.** No peer kill, no half-open connection, no deadline
   expiry, no reconnect with a reused request id. D13's hold and §9.2's
   per-call deadlines are untested here.
5. **Saturation and backpressure**, per the limits above.
6. **Certificates, and TLS peer authentication.** `rcgen` mints a throwaway
   self-signed cert and **both sides run `with_no_client_auth()`** over a DER
   shared through the filesystem, so these rows price TLS's *encryption* and
   nothing else: as configured, TLS here authenticates neither peer, and the
   §9.2 HMAC handshake inside it is not bound to the TLS channel (a relay that
   terminates both legs passes it through). What the product does — a
   self-signed CA at bootstrap pinned in IDENTITY, per-node leaf certs, peer
   certs required both ways, or RFC 5705 channel binding — is an open question
   that G0 did not answer.
7. **Cross-host, cross-AZ and multi-peer fan-out.** One receiver, one leader,
   one host. A leader with two followers plus forwarding from both is the
   shape §12.5 actually has to survive.

8. **The blast radius of multiplexing.** Every in-flight command shares the
   configured sockets, so one socket failure makes all of them retry (bounded
   by D6's recorded outcomes), where HTTP/1.1's pool would lose one request
   per dead connection. Not exercised: no peer kill, no half-open socket.
9. **HTTP/2 over the same rustls** was never measured; see MEMO.md Deferred 9.
