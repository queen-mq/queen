# S4 — transport memo (WP-0.6, input to D12 at gate G0)

Measured 2026-09-17: laptop smoke in `RESULTS-laptop.md`, **Linux VM in
`RESULTS-vm.md` (two independent 60 s passes of all 15 configurations on an
idle 8 vCPU x86-64 host — these are the quotable numbers, §0.3)**. Harness and
commands in `README.md`.

**Revised 2026-09-18 after an adversarial review** (two refuters, both
"refuted / major"). Their arithmetic was right: every figure in the first
memo reproduces from the result files. What they refuted was the *meaning* of
three of the four recommendations. Four more VM cells and one micro-benchmark
were run to settle them (`results/vm-c/`, `results/vm-pace/`, ~9 minutes of VM
time, nothing left running). Result:

- **Recommendations 1 and 3 stand** — framed TCP for forwarding and Raft
  traffic — with corrected numbers and the mechanism named.
- **Recommendation 2 is changed**: TLS is no longer preferred over the
  per-frame MAC. They are alternatives, as D12 already says, and the MAC must
  be respecified (the measured one is replayable). §Refutations R-B1.
- **Recommendation 4 is changed**: "two connections is the knee" is
  withdrawn; the sweep cannot size the pool.
- **D12's rationale in PLAN_RAFT.md quotes laptop numbers and must be
  corrected** (R-A6). That is not this memo's file to edit.

## Recommendation

1. **Keep D12's framed transport for forwarding and Raft traffic.**
   Length-prefixed frames on a dedicated port, a few persistent connections,
   one reader task and one writer task per socket, requests multiplexed by
   request id. On the VM at 50 000 msg/s, leader CPU per message against
   HTTP/1.1 carrying the same bodies, in three like-for-like pairs:

   | pair | framed | HTTP | ratio |
   |---|---|---|---|
   | plain | 1.26 | 2.15 | **0.59×** |
   | authenticated (per-frame MAC vs per-request MAC) | 2.47 | 3.84 | **0.64×** |
   | encrypted (TLS vs HTTPS, both without any MAC) | 1.54¹ | 3.33 | **0.46×** |

   ¹ the framed+TLS row (1.370) corrected by 1.126, the factor by which the
   same framed configuration re-measured 12.6 % slower on the day the HTTPS
   cell was run; uncorrected the ratio is 0.41×.

   plus **5 % fewer bytes** on the wire (294.2 vs 309.7 B/msg) on **2
   connections against 15–49**. The encrypted row is new (`results/vm-c/`,
   2026-09-18, same binary, same-day framed control): the first memo's
   **0.32× was wrong** — it divided framed+TLS by HTTPS+*MAC* and there was no
   HTTPS-without-MAC row to divide by. At 20 k the corrected encrypted ratio
   is 0.61× (was 0.51×).

   Two things that number depends on, both now measured rather than assumed:

   - **Socket count.** The framed writer coalesces whatever is queued into one
     `write_all`, so its CPU per message follows the socket count:
     0.63 / 1.26 / 1.65 / 1.89 µs/msg on 1 / 2 / 4 / 8. At **8 framed sockets
     against HTTP's 15–49 the ratio is 0.88×**, not 0.59×. The comparison
     cannot be run the other way: HTTP/1.1 *cannot be held* to a socket
     budget — capping the pool at 2 made hyper open **218 695 connections in
     60 s** and cost 3.85× framed (R-A2).
   - **Arrival shape.** The harness's tokio pacer releases commands in bursts
     of about five per millisecond, which is what the writer coalesces. With a
     jitter-free pacer (`--pace spin`, due-time error p50 777 → 60 µs) framed
     CPU per message rises **+26 %** and HTTP's by 2 %, so the plain ratio
     moves 0.54× → **0.66×** in the same hour on the same binary. The
     advantage shrinks; it does not disappear. Quote 0.59× only with "2
     sockets, bursty arrivals" attached.

2. **Take the mutual HMAC handshake on every connection. Treat TLS and a
   per-frame MAC as alternatives, not as a preference** — which is what D12
   already says. *(Changed: the first memo preferred TLS.)*
   - The cost gap is real but small in absolute terms: on both hosts measured
     (neither exposes SHA-NI) the per-frame HMAC-SHA256 costs +1.21 µs/msg
     over plain framed TCP against TLS's +0.11 — 11× — but the *difference*,
     1.10 µs/msg, is **0.055 of one core at 50 000 msg/s**: 0.7 % of this
     8-vCPU box. On latency it is +57 µs of p50 (125 against 68) on a path D7
     makes wait for commit *and* apply, which S3 measured at ~3 ms p50: ~2 %.
   - The latency argument survives one refutation (R-A3/R-B): a MAC that
     cannot be replayed needs a per-direction frame **sequence number**, and a
     sequence number must be assigned where frames are serialised — the writer
     task. rustls encrypts on that same task. So both options put per-frame
     crypto on the reader/writer tasks; what separates them is only the 11×,
     and only where SHA-NI is missing (Deferred 1).
   - Against that: the broker has **no server-side TLS today** (no
     `rustls::ServerConfig` anywhere in `server/src`; rustls appears only
     client-side in `pgtls.rs`, `db.rs`, `httpget.rs`; `rcgen` is not a
     dependency), and the certificate lifecycle — bootstrap CA, pinning in
     IDENTITY, rotation, expiry, clock skew at boot — is still undecided
     (Deferred 7, raised for G0 and not answered by it). The MAC needs only
     the `QUEEN_RAFT_SECRET` D12 already mandates.
   - **Whichever is chosen, two properties must both hold**, and the spike as
     measured had neither (R-B1, R-B2, now fixed in its code):
     (a) *the peer is authenticated* — the spike's TLS used
     `with_no_client_auth()` and a shared self-signed DER, and the HMAC
     handshake is not bound to the TLS channel, so a relay that terminates
     both legs passes the handshake through and then injects on its own
     channels. Either require client certificates or mix RFC 5705 exporter
     material into the handshake transcript;
     (b) *frames are not injectable, replayable or reorderable* — the
     measured MAC covered `type || body` only, with one key in both
     directions and no counter. `frame.rs` now MACs
     `dir || seq || len || type || body` with per-direction keys and a
     receiver that accepts only its expected next sequence number
     (+13 B of HMAC input, +0.4 %; three new tests).

3. **Do not use HTTP/1.1 for forwarding.** Not the bytes (+14–17 %) and not
   only the CPU: HTTP/1.1 has no multiplexing, so the socket count follows the
   offered concurrency and **is not yours to set**. On the VM: 12–49 live
   connections at 50 k against a fixed 2; not stable between two identical
   passes (15 then 49, and 64 with 198 opened in the new HTTPS row); and when
   capped to 2 idle it churned 218 695 connections in 60 s at 3.85× the framed
   CPU. On a real network the floor grows with `rate × RTT` while the framed
   transport stays at its configured sockets. **Withdrawn (2026-09-17):** the
   laptop's connection *churn* figure under load; on the idle VM
   `conns opened == conns open` in all twelve original HTTP rows. HTTP stays
   fine where master already uses it: the ephemeral relay in
   `server/src/peerclient.rs`, small bodies, one request in flight.
   **Not measured: HTTP/2 over the same rustls** (R-B: one connection,
   multiplexed, per-stream flow control, `h2` is not yet in
   `server/Cargo.lock`). It is the one alternative that could remove most of
   the hand-written transport surface, and this spike says nothing about it.
   Deferred 9.

4. **Size the pools in the transport WP, not from these rows.** *(Changed:
   "two is the knee" is withdrawn.)* One connection is the CPU-optimal
   default (0.63 vs 1.26 µs/msg, i.e. 0.032 vs 0.063 of a core at 50 k); the
   only measured reason to prefer two was +9 µs of p50 and +18 µs of p99 —
   on loopback, in a regime this spike never left. D7 makes the leader answer
   only after commit *and* apply (S3: ~3 ms p50), so the real mean depth is
   15–35 commands in flight per receiver against the **0.31** measured here.
   The two honest reasons to keep more than one socket are head-of-line
   blocking behind a big frame and failure blast radius, and both are
   unmeasured (Deferred 3, 4). Keep §12.5's separate pools (Raft RPC,
   forwarding, snapshots), start at 2, and re-run the sweep with a commit
   delay and mixed frame sizes before fixing it.

5. **Fix three defects in the code this memo offers for porting** — done in
   the spike on 2026-09-18, after the measurement runs, so no measured number
   moves (details in §Refutations):
   `tcpx.rs::round_trip` had **no deadline at all** and leaked its waiter when
   a connection died (every in-flight forward hung for ever); `main.rs`
   defaulted `--secret` to a literal and `auth.rs` accepted an empty one
   (pgless U19, the very defect D12 cites); the per-frame MAC was replayable.

## The numbers behind it (VM, mean of the two 60 s passes, offered 50 000 msg/s)

| configuration | p50 µs | p99 µs | leader µs/msg | recv µs/msg | B/msg | live conns |
|---|---|---|---|---|---|---|
| framed TCP | 62.5 | 112.5 | 1.26 | 2.63 | 294.2 | 2 |
| framed TCP + per-frame MAC | 125.0 | 201.0 | 2.47 | 4.30 | 297.4 | 2 |
| framed TCP + TLS | 68.0 | 120.0 | 1.37 | 2.77 | 297.1 | 2 |
| HTTP/1.1 | 70.5 | 131.0 | 2.15 | 4.10 | 309.7 | 15 / 49 |
| HTTP/1.1 + MAC header | 103.5 | 174.5 | 3.84 | 5.72 | 314.9 | 12 / 18 |
| HTTPS + MAC header | 112.5 | 193.5 | 4.22 | 6.15 | 319.3 | 19 / 13 |
| **HTTPS, no MAC** (2026-09-18) | 109 | 183 | **3.33** | 5.87 | 314.1 | 64 / 198 |
| **HTTP, pool capped at 2** (2026-09-18) | 128 | 224 | **5.47** | 9.40 | 309.7 | 2 / **218 695** |
| framed TCP, same-day control | 76 | 130 | **1.42** | 3.28 | 294.2 | 2 |

The last three rows ran on 2026-09-18 on the **pass A binary, unchanged**
(md5 `7ecbc517…`), and that day's framed control came out 12.6 % above pass
A/B, so the ratios in recommendation 1 correct the framed side by that factor.
At 20 000 msg/s the ordering is the same and the per-message CPU is roughly
double everywhere (fewer frames coalesce per write): framed TCP 2.24,
HTTP 2.88, framed+MAC 3.41, framed+TLS 2.51, HTTPS-no-MAC 4.63 µs/msg.

8 659 953 commands were forwarded across all four VM sessions with **0
mismatched request ids and `cmds_bad = 0` in all 38 rows**; achieved = offered
to within 0.005 %, and `cmds_ok` equals `achieved_cmds_s × secs` in every row.
Handshake per connection: framed+HMAC 0.17–0.33 ms, framed+TLS 0.80–0.85 ms,
HTTP first request 0.16–0.20 ms, HTTPS 0.36–0.41 ms.

Absolute latency here is loopback latency. In a cell the forward adds one
network RTT to a path that D7 already makes wait for commit + apply.

## Refutations (2026-09-18) — every finding and what was done

Verified first: all quoted figures reproduce from `results/vm/results.jsonl`
and `results/vm-b/results.jsonl` (`python3 refute.py`). Both refuters agreed
the arithmetic was sound; nothing below is an arithmetic correction.

| # | finding | verdict | resolution |
|---|---|---|---|
| R-A1 | "Encrypted 0.32×" is not like-for-like: no HTTPS-without-MAC row exists | **upheld** | **Measured it.** HTTPS plain = 3.33 µs/msg; corrected ratio **0.46×** @50k, 0.61× @20k. Note both reviews' estimate (~0.54×) was also wrong: TLS costs the HTTP path +0.7–0.9 µs/msg, not the +0.38 the MAC arithmetic implied, because HTTP encrypts per request while framed encrypts per coalesced write. |
| R-A2 | The CPU gap measures write coalescing, not framing; 0.88× at 8 sockets; the burst shape is a harness artefact | **upheld in part** | **Measured it** with a jitter-free pacer: framed +26 %, HTTP +2 %, ratio 0.54× → 0.66× (`results/vm-pace/`). Both numbers and the mechanism are now in recommendation 1. The other direction (hold HTTP to 2 sockets) is impossible: 218 695 connections in 60 s. |
| R-A3 / R-B (rec. 2) | TLS-over-MAC rests on an 11× that is 0.055 core, and on a p50 argument the memo's own porting note deletes | **upheld** | Recommendation 2 **changed** to "either". The p50 argument is re-grounded, not dropped: an anti-replay MAC needs a sequence number, which must live on the writer task — where rustls also encrypts. Absolute sizes now stated (0.055 core; ~2 % of a 3 ms commit path). |
| R-A4 | "Two connections is the knee" is a 9 µs loopback artefact | **upheld** | Recommendation 4 **changed**: 1 is CPU-optimal, 2 is an unmeasured hedge, the pool is sized in the transport WP under a commit delay. |
| R-A5 | Multiplexing's blast radius (one dead socket = N unknown outcomes) is never weighed | **upheld, with a bound** | Added to Deferred 4. It is bounded by D6: the receiver retries with the same request id and gets the recorded outcome, so a dead socket costs N *retries*, not N unknown writes — provided in-flight waiters fail fast, which they now do (the reader task clears the pending map; before, they hung for ever). |
| R-A6 | PLAN_RAFT.md D12 ratifies with laptop numbers ("1.35 vs 2.77 µs", "half the leader CPU") | **upheld** | Confirmed: those are `RESULTS-laptop.md` rows, which §0.3 forbids quoting. The VM says 1.26 vs 2.15 = 0.59× on 2 sockets against 15–49, 0.88× at matched sockets. **PLAN_RAFT.md is not this WP's file to edit** — flagged to the coordinator. |
| R-A7a | `results/vm-a/` is called a "byte-identical copy" of `results/vm/` | **upheld** | Corrected in `RESULTS-vm.md`: `results.jsonl` and `runlog.txt` are identical (`cmp`), the directories are not (`cert.der` only in `vm/`, `queue.log` only in `vm-a/`). |
| R-A7b | "rc=0 for every one" records `sed`'s status, not the receiver's | **upheld** | Confirmed in `run.sh` (`local RC=$?` after a pipeline). Claim replaced in `RESULTS-vm.md` by the checks that do hold: `cmds_bad = 0`, `achieved = offered`, `cmds_ok = achieved_cmds_s × secs`, one JSON row per configuration. |
| R-A7c | RAFT_STATUS.md still says WP-0.6 "in progress" / G0 "pending" while PLAN_RAFT.md says G0 RATIFIED | **upheld** | Confirmed (RAFT_STATUS.md:22 and :82). Not this task's file to edit; flagged to the coordinator. |
| R-B1 | TLS as measured has no peer authentication and no channel binding, so a MITM relays the HMAC handshake | **upheld — the most serious finding** | Confirmed in `net.rs:149–172` (`with_no_client_auth()` on both sides, a DER shared through the filesystem). Recommendation 2 now requires client certificates **or** RFC 5705 channel binding, and says D12's TLS branch cannot be ratified while its trust anchor is undecided. |
| R-B2 | The per-frame MAC covers `type \|\| body` only: replayable after the D6 window, reflectable, `len` unauthenticated | **upheld** | **Fixed in `frame.rs`**: per-direction keys derived from the session key, MAC over `dir \|\| seq \|\| len \|\| type \|\| body`, the receiver accepts only its expected sequence. Three new tests (replay, reflection, length). Cost: 13 B more HMAC input per frame = +0.4 % (VM bench: 9884 ns for a 2801 B frame), far below the 5 % pass-to-pass spread. |
| R-B3 | "fail closed" is claimed but not implemented: empty secret handshakes happily, `main.rs` defaults it | **upheld** | **Fixed**: `auth::check_secret` (≥16 B) on both sides of the handshake, `main.rs` refuses to start without `--secret`, one new test. This was pgless U19 reproduced inside the spike that cites U19. |
| R-B4 | The authenticated pair (0.64×) is a strawman: HTTP MACs one direction, and hex-formats through 16 `format!`s compared with `String ==` | **upheld in wording, negligible in size** | **Measured** (`s4 bench` on the VM): HMAC over 2801 B = 9023 ns, hex = 492 ns, `String ==` = 2 ns, so the hex path is 5.2 % of the HTTP MAC op and ~1.3 % of that row's CPU; removing it moves 0.645 → 0.653. The opposite bias (HTTP MACs one direction, framed two) is of the same order and pushes the other way. Net: under 2 %. `auth.rs` now also carries the binary, constant-time form as the one to port. |
| R-B5 | The sweep ran at 0.31 commands in flight; D7 implies 15–35 | **upheld** | Recommendation 4 changed accordingly; the re-run shape is specified in Deferred 2. |
| R-B6 | §9.2 puts `PayloadRead` on the forwarding pool, and `MAX_FRAME` (8 MiB) cannot carry a 96 MiB `QUEEN_RAFT_ENTRY_MAX_BYTES` entry | **upheld** | Not fixed here (raising the cap without a chunking rule turns `buf.resize(len)` into an allocation DoS). Documented at the top of `frame.rs` and in Deferred 3 as an open item the transport WP must close. |
| R-B7 | `tcpx.rs` is not portable "as is": no deadline, no reconnect, waiters leak on connection loss | **upheld** | **Fixed** the deadline (5 s default, waiter removed on expiry) and the leak (reader clears the pending map on exit). Reconnect is **not** implemented — it needs the D13 hold and the leader hint, which this spike has no notion of; Deferred 4. The receiver-side timer cannot move any leader CPU figure. |
| R-B8 | HTTP/2 over the same rustls was never measured, and it is the alternative that removes the hand-written surface | **upheld** | Not measured — a new dependency and a new transport, beyond this spike's window. Recorded as Deferred 9 with the reasons it may be worth a half-day before the transport WP starts. |

Changes to the spike's code, all made **after** the measurement runs and none
on the leader's hot path: `frame.rs` (sequenced MAC), `auth.rs` (fail-closed
secret, binary constant-time request MAC), `tcpx.rs` (deadline, waiter
eviction, writer-side encoding), `main.rs` (`--pace`, `bench`, no default
secret), `run.sh` (`PART=extra`, `PART=pace`), `run-vm-refute.sh`, `refute.py`.
`cargo test --release`: **13 tests, all green**, on the laptop and on the VM.
The three transports were re-run end to end after the changes (laptop, 5 s at
50 000 msg/s each, smoke only): plain, sequenced MAC and TLS all completed
with `cmds_bad = 0` and achieved = offered — 25 000 commands under the
sequenced MAC without one sequence mismatch — and both binaries now refuse to
start without a secret.

## Deferred — what this spike cannot decide

1. **SHA-NI, and with it the true size of the MAC/TLS gap.** Neither host has
   it: the VM's guest CPU model (QEMU `pc-i440fx-6.1`, family 6 model 106, on
   a Xeon Gold 6548N) masks the SHA extensions (`grep -c sha_ni /proc/cpuinfo`
   → 0), so AES-GCM runs at 2.65 GB/s and HMAC-SHA256 at 0.24–0.31 GB/s
   (end-to-end and micro-benchmark agree). Scaled to the 1.5–2 GB/s SHA-NI is
   worth, the MAC would cost ~+0.15–0.20 µs/msg, near TLS's +0.11 — i.e.
   **with SHA-NI the two are probably close; without it TLS is 11× cheaper.**
   Settling it needs a host whose `/proc/cpuinfo` shows `sha_ni`; `s4 bench`
   answers it in five seconds on one.
2. **A real RTT, and the in-flight depth D7 implies.** `tc qdisc add dev lo
   root netem delay 500us` changes a VM-wide setting and needs Alice's
   approval (§0.3). The pool-sizing re-run that recommendation 4 asks for is:
   hold each forward 3–7 ms before answering (commit + apply), 500 µs and 1 ms
   of netem, mixed frame sizes (2.8 KB outcomes interleaved with a multi-MB
   `PayloadRead`), sweep 1/2/4/8 sockets.
3. **Head-of-line blocking behind a big frame, and `MAX_FRAME`.** One writer
   task per socket serialises frames; this spike only ever wrote 2.8 KB. The
   §12.5 separate-pools rule is asserted, not tested, and `frame.rs`'s 8 MiB
   cap is not reconciled with `QUEEN_RAFT_ENTRY_MAX_BYTES` (96 MiB): either
   the cap rises with a negotiated bound on `buf.resize(len)`, or large
   entries are chunked. R-B6.
4. **Failure behaviour, reconnect and blast radius.** No peer kill, no
   half-open socket, no reconnect reusing a request id. With one multiplexed
   socket, one failure makes every in-flight command on it retry (bounded by
   D6's recorded outcomes); with HTTP/1.1's pool, one dead connection loses
   one request. Deliberate trade, untested either way.
5. **Backpressure.** Both sides ran far from saturation (leader ≤ 0.21 of 8
   cores). The bounded per-connection queues (mpsc 4096) were never exercised.
6. **Certificates and the trust anchor.** `rcgen` mints a throwaway
   self-signed cert and both sides run `with_no_client_auth()`. What the
   product does — self-signed CA at bootstrap, fingerprint pinned in IDENTITY,
   per-node leaf certs, peer cert required on both sides — is an open question
   that G0 did not answer (PLAN_RAFT.md §17 contains no certificate decision).
   **This blocks the TLS branch of D12, not the framed transport itself.**
7. **One host, one peer, one shape.** Loopback only (no NIC, no MTU
   segmentation, no congestion control), one receiver against one leader,
   batch 10 / 256 B / one partition. A leader with two followers plus
   forwarding from both is what §12.5 has to survive.
8. **Nothing about Raft itself.** No log, no commit, no snapshot streaming;
   the snapshot pool of §12.5 is S3's and phase 4's problem.
9. **HTTP/2 over rustls as a third configuration.** Multiplexes on one
   connection, has per-stream flow control (the answer to Deferred 3) and
   mature backpressure; pure Rust and cmake-free (hyper's `http2`, `h2` not
   yet in `server/Cargo.lock`, so a new dependency, C-2-compatible). If it
   landed within ~20–30 % of framed CPU it would remove most of the framing,
   multiplexing, backpressure and reconnect code D12 obliges us to write. One
   harness day; nothing in these rows decides it.

## Implementation notes for `rsm/net/`

- `src/frame.rs`, `src/auth.rs` and the reader/writer/work task shape in
  `src/tcpx.rs` are the starting point, **as revised on 2026-09-18** — not as
  measured. What changed and why is in §Refutations; what is still missing
  before a port is reconnect (Deferred 4) and the `MAX_FRAME` decision
  (Deferred 3).
- **Every RPC carries a deadline** (§0.3, I15). `round_trip` now takes one and
  removes its waiter when it expires; the command body already carries
  `deadline_us` (§9.2 "every request carries its remaining budget") and the
  product must *enforce* it, which the spike still does not.
- **Fail closed.** `auth::check_secret` refuses a secret shorter than 16 B on
  both sides, and the binary refuses to start without one. Do not copy
  master's mesh handshake (`server/src/mesh.rs`): it proves one direction
  only, over one nonce, with no cluster id and no peer ids, and an empty
  secret means *open mode* (pgless U19). Its `verify_slice` constant-time
  check is worth keeping.
- **The per-frame MAC is sequenced**: per-direction keys, MAC over
  `dir || seq || len || type || body`, the sequence not on the wire, the
  receiver accepting only its expected next value. This forces encoding onto
  the writer task (the sequencer) — accept that; rustls is there too.
- **If TLS is used, authenticate the peer**: client certificates on both
  sides, or mix `export_keying_material` (RFC 5705) into the handshake
  transcript. `with_no_client_auth()` plus a pinned DER is a spike shortcut,
  not a design.
- Keep the mesh's JSON payloads out: §5.1 says no serde formats on the wire.
- The 16-byte request id is the multiplexing key on the wire as well as the
  dedup key of D6; the reader resolves waiters by it, and a retry after a
  reconnect reuses it, which is what makes I6 reachable from the transport.
- Frames are read only by the per-connection reader with `read_exact`, never
  inside a `select!`.
- `rcgen` is a spike-only dependency; `hex` formatting and `String ==` on a
  MAC are not what the product should do (`auth::request_mac_bin` /
  `verify_request_mac` are the forms to copy).

## Outside this memo — for the coordinator and Alice

1. **D12's rationale in PLAN_RAFT.md quotes laptop numbers** ("1.35 vs
   2.77 µs", "half the leader CPU"). The VM numbers are 1.26 vs 2.15 = 0.59×
   on 2 sockets against 15–49, 0.88× at matched sockets, 0.46× encrypted.
2. **D12's TLS branch is not ratifiable yet**: its trust anchor (certificates)
   is Deferred 6, which G0 did not decide, and TLS without peer
   authentication or channel binding does not deliver §9.2's "nothing can be
   injected mid-stream".
3. **RAFT_STATUS.md** still shows WP-0.6 "in progress" and G0 "pending" while
   PLAN_RAFT.md records G0 RATIFIED citing these memos (§0.3 rule 5).
