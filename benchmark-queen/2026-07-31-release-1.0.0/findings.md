# 2026-07-31 — Verifica pre-release 1.0.0: il caso dei numeri di luglio

**Esito: nessuna regressione nella 1.0.0 — ma i baseline T1/T2 del 2026-07-23
appartengono a un'implementazione che non esiste piu'. Sono stati certificati su
un dedup a HashMap (O(1) a lookup, 64 B/hash) e su un pop wildcard pre-hotlist;
il giorno DOPO (24/07) entrambi sono stati riscritti (ring dedup 16 B/hash +
hotlist "smart pop"), e le shape T1/T2 non sono mai state ri-misurate sulla
riscrittura — fino a oggi. Il rig e' innocente: la shape del soak, unica baseline
certificata sul codice post-riscrittura, si riproduce oggi al 100% su entrambe
le build, e la 1.0.0 la esegue con un terzo degli ackErr della vecchia.**

## Rig

| ruolo | host | note |
|---|---|---|
| PG + broker | `queen-01` 139.59.133.1 (10.114.0.2) | 32c fisici (no HT), 62GB, fdatasync 98 µs, Xeon Gold 6548N |
| loader | `queen-02` 207.154.209.12 (10.114.0.3) | 48c/94GB (ricreata a meta' sessione; con 32c dava numeri identici) |

Link VPC 10 Gbit/s (iperf3), RTT 1,31 ms, MTU 1500, zero errori NIC. Ambiente
verificato sotto carico: **steal 0%**, IPC 1,06 a ~2,06 GHz, NUMA singolo.
PG 18 via `postgres:18`, boot = `setup-broker.sh` ricostruito del 24/07 (quello
del soak).

Build confrontate, compilate sulla stessa VM con lo stesso Dockerfile:
- **vecchia** = `615efdc` (24/07, "good performance on multitenant bench") — la build del soak 24h.
- **candidata** = `rustproxy` HEAD `21cfce0`, boot `version=1.0.0-alpha-01`.

## Atto I — la shape T1 non si riproduce, su NESSUNA delle due build

Shape T1 del 23/07 (1M msg/s, 100 partizioni, 850 consumer, push-batch 100,
pop-batch 1000, pp10, dedup 300 s, autoAck): baseline = 1M/1M sostenuti, p50
90-120 ms, PG 14,3 core / 61 backend, Queen 9,7 core.

Oggi (run da 120 s, tutte in `raw/`):

| run | build | dedup | push/s | pop/s | p50 | PG | Queen |
|---|---|---|--:|--:|--:|--:|--:|
| ab-wait | 1.0.0 | 300s | 631k | 357k | 979 ms | 0,7c | 15,8c |
| ab-old | 615efdc | 300s | **634k** | **365k** | 791 ms | 0,7c | 15,4c |
| ab-tuned (boot T3) | 1.0.0 | 300s | 707k | 121k | 289 ms | 0,5c | 23,6c |
| ab-nodedup | 1.0.0 | off | **1M pieni, shed 0** | 397k | 20,6 ms | 2,9c | 5,0c |
| ab-suppress (cache 256MB) | 1.0.0 | 300s | 595k | 325k | 758 ms | — | — |
| val (loader 48c) | 1.0.0 | 300s | 600k | 338k | — | — | — |
| val2/val3 (48c, dedup off, ±wait) | 1.0.0 | off | 1M | 397k/305k | — | — | — |
| val4 (commit_delay=0) | 1.0.0 | 300s | 603k | 328k | — | — | — |

Vecchia == candidata entro il 2% su tutto ⇒ **quello che manca rispetto a luglio
manca in tutte e due**. Escluso per misura: dimensione del loader (32c==48c),
pop-wait, cache dedup 256MB↔20GB, tuning push-lane T3, commit_delay, rete,
disco, steal/clock/NUMA.

`perf` sul broker con dedup acceso: **60,23% dei cicli in
`DedupCache::verified_for_push`** — lo scan di membership. Con dedup off il push
fa 1M/s pieni e il broker scende a 5 core.

## Atto II — l'archeologia: cosa girava DAVVERO il 23 luglio

```
7353ec0  23/07  Dedup at 1M: shard the cache lock, ...   <- fix7-era: T1/T2 certificati qui
1a09bd8  23/07  1M with dedup and tcp
c0c84cf  23/07 19:06  implementing smart pop             <- DOPO le run T1/T2 (13:50/14:02 UTC)
125beb6  24/07 09:21  ring dedup and hot list            <- LA RISCRITTURA
14bf888  24/07 12:44  pop priority-inversion fix + ...
615efdc  24/07 15:19  good performance on multitenant bench   <- build del soak
a7e5afa  ~29/07  removed auto vac truncate on hot tables <- fix del soak, committato DOPO 615efdc
```

Il dedup del 23/07 (`7353ec0`, dedup.rs:176): `set: HashMap<u128, u32>` —
membership **O(1)**, ~64 B/hash (i tre OOM del 23 vengono da qui). Il dedup dal
24/07 in poi (identico in `615efdc` e in HEAD): hot buffer a scan lineare +
blocchi ordinati da **4096 hash** con una binary search **per blocco**. Il
commento in testa a dedup.rs lo quantifica da solo: *"a 300 s / 1 M-msg/s window
is ~1.5 M hashes/partition ≈ 366 blocks, a few thousand comparisons per probed
hash"*. A 1M hash/s sono miliardi di probe al secondo: e' esattamente il 60% di
CPU misurato, e la ragione per cui il TODO del soak diceva gia' *"il ceiling 1M ×
24h resta raggiungibile con bloom sul ring dedup (finestra 300s)"*.

La riscrittura e' stata il trade giusto (16 B/hash, niente rehash multi-GB
sincronizzati, e' cio' che rende possibile il free-tier e il soak senza OOM), ma
il suo costo di probe cresce col contenuto-finestra per partizione:

- shape soak: 600k/s ÷ 200 part × 60 s = 180k hash/part ≈ 44 blocchi → economico ✓
- shape T1: 1M/s ÷ 100 part × 300 s = 3M hash/part ≈ 732 blocchi → insostenibile ✗
- shape T2: 900k/s ÷ 200 part × 300 s = 1,35M hash/part → insostenibile ✗

Anche il pop: le T1/T2 di luglio usavano il wildcard pre-hotlist; la hotlist
nasce la sera del 23. Sulla shape T1 (batch 1000, pp10, 100 part, 850 consumer)
il pop path attuale si ferma a ~400k/s con broker e PG scarichi (99% pop vuote
nonostante 62M di lag), mentre sulla shape soak (batch 500, pp1, 200 part)
consegna 600k/s senza sforzo. Ceiling da caratterizzare, ma anch'esso identico
fra vecchia e candidata.

## Atto III — la controprova: la shape del soak si riproduce, oggi, su entrambe

Shape esatta del 24h (600k/s, 200 part, 600 consumer, push-batch 100, pop-batch
500, manual-ack + ack-async, dedup 60 s, retention 300/3600 s), 300 s + ramp 60:

| | **soak luglio (24h, steady)** | **615efdc oggi (5 min)** | **1.0.0 oggi (5 min)** |
|---|--:|--:|--:|
| throughput | 600k/s piatti | 600k/s, shed 0 | 600k/s, shed 0 |
| p50 / p99 | 87,6 / ~236 ms | 67 / 782* ms | 68 / 782* ms |
| ackErr | 47.600 su 24h | **16.100** in 5 min | **5.200** in 5 min |
| PG CPU / Queen CPU | 6,1c / 14,4c | 3,6c / 10,2c | 3,4c / 10,6c |
| commit/s | 3.133 | 3.214 | 3.361 |
| fsync/s · latenza | 731 · 697 µs | 798 · 624 µs | 847 · 596 µs |
| backend attivi | 13 | 12 | 12 |

\* p99 a onde (170↔990 ms): e' il wobble da autovacuum-truncation dell'atto 1 del
soak di luglio, atteso su un run breve. La vecchia build NON ha il fix
`vacuum_truncate=off` nel SQL (arrivo' con `a7e5afa`, a caldo durante il soak);
la 1.0.0 ce l'ha in 041 — ed e' il motivo per cui fa **3× meno ackErr** della
vecchia a parita' di tutto. I contatori PG (commit/s, fsync, backend) coincidono
con luglio: stesso motore, stesso comportamento, rig equivalente.

## Atto IV — il fix: bloom generazionale davanti al ring (implementato, misurato)

Stesso giorno, su richiesta di Alice: implementato in `server/src/dedup.rs`
(invariante 4 nel doc di modulo) il **bloom front generazionale**: per ogni
Entry un ring temporale di split-block bloom (16 bit/hash, k=7, i 7 bit di probe
dentro UNA cache line), capienze a tier ×8 da `block_cap` a 1M hash. Un miss su
tutte le generazioni prova l'assenza senza toccare ne' hot ne' blocchi; un
"maybe" paga il path esatto di oggi, che resta l'unica autorita' per
LocalDuplicate. Le generazioni scadono con la stessa regola whole-unit dei
blocchi (watermark interamente pre-window), quindi nessun hash in-window perde
mai copertura: la soundness poggia sullo stesso argomento di prima. Protocollo
`p_verified` e firme pubbliche invariati; fusion.rs non toccato. 22/22 test
verdi (3 nuovi: superset end-to-end, expiry generazionale, accounting byte).

Run di validazione: **shape T1 identica a luglio** (1M msg/s, dedup 300 s,
autoAck), build `21cfce0` + bloom, 600 s:

| | **T1 luglio (HashMap O(1))** | **T1 oggi PRE-fix (ring)** | **T1 oggi col bloom** |
|---|--:|--:|--:|
| push achieved | 584,5M / 585,0M (99,91%) | ~600k/s, shed 32M+ | **584,1M / 585,0M (99,85%), shed 0** |
| p50 push server | — | 1.108 ms | **4,8-6,5 ms** |
| p50 client | 118 ms | 979 ms | **62,7 ms** |
| CPU verify (perf) | n/d (O(1)) | **60,23%** dei cicli | **3,7%** (+1,6 gen_maybe +1,1 append) |
| Queen CPU | 9,7c | 15,8c (a 600k!) | **9,5c (a 1M)** |
| cache dedup | 19,8 GB (64 B/hash, 3 OOM) | 4,8 GB | **5,2 GB piatti (18 B/hash), 0 suppression** |
| RSS broker | ~35 GB | ~10-11 GB | **~9 GB** |

Il push della shape T1 e' tornato ai numeri di luglio con un quarto della RAM
di allora. Il **pop resta al suo tetto (~320-430k/s, 99% pop vuote su lag
profondo)**: e' il secondo cantiere (claim path hotlist, per scelta esplicita
niente A/B in questa sessione), ed e' il motivo per cui il popped finale e'
193M e non 584M. Artefatti in `raw/t1bloom/` (run.out, bench.csv, loader.csv,
perf-broker-bloom.txt, broker-rates.log).

## Atto V — il pop: diagnosi strumentata e fix del claim (budget-aware)

Domanda di Alice: perche' il pop resta a ~400k? Risposta trovata col trace nativo
del broker (`QUEEN_HOTLIST_TRACE`), PGSS e perf — poi fixata e misurata.

**La causa: il claim economy della hotlist.** `hotlist_pop_attempt` reclamava
`max_parts × 8, clamp(16, 256)` partizioni per ogni pop (16 con pp1, 80 con
pp10!), tenendole INFLIGHT — fuori dal ready ring — per tutta la gamba SQL della
serve. Equazione verificata dal trace: 100 partizioni ÷ (16 × 7,7 ms di ciclo) =
810 serve/s teoriche, **806 misurate**, × 863 msg medi = il tetto ~700k intero.
A luglio il claim viveva nei row-lock SQL: una partizione per pop, ~100 serve
concorrenti → 1M. Scagionati con misure: SQL `log_pop_list` (0,86 ms mean PGSS,
p50 wall 3 ms), Vegas e pool (`vegas_wait=0 pool_wait=0` su ogni serve
tracciata), CPU broker (perf), loader (30/48 core), PG (3,3c).

**Il fix** (`take_batch` in hotlist.rs + call site in data.rs, patch in
`patches/hotlist-budget-claim.patch`): claim budget-aware — si smette di
staccare candidati quando la somma dei `batch_count` (la stessa stima che
`ready_est` gia' usa, polarizzata al ribasso per costruzione) copre il batch
richiesto; il cap `k` scende a `max_parts × 2, clamp(2, 64)` e serve solo da
tetto per i ring scarichi. Semantica di checkin/epoch/wheel intatta. 175/175
test verdi (nuovo: `take_batch_budget_stops_at_covered_backlog`).

**Risultato sulla T1 identica a luglio (pp10, 850 consumer, 10 min):**

| | pre-fix | **post-fix (bloom2)** | luglio |
|---|--:|--:|--:|
| popped totale | 193,4M | **499,9M (2,6×)** | 584,5M |
| pop/s regime | 320-430k | **790-984k** | ~1M |
| hotlist ready | 0 | **100** | n/a (pre-hotlist) |
| push | 584,1M shed 0 | 582,0M shed 0,5M | 584,5M |
| Queen CPU | — | 9,3c | 9,7c |

**Residuo (~10-15%) caratterizzato ma non chiuso**: 1200 consumer e pool 400
non spostano nulla → non e' client, non e' pool. Il pop ora *insegue* il push in
un equilibrio auto-regolato: quando il lag cala i batch si diluiscono (serve
parziali in multipli esatti di 100 = segmenti freschi), il pop rallenta, il lag
ricresce, i batch si ringrassano. Lag stazionario 5-20M contro ~40k di luglio.
Il filo da tirare e' il take parziale di `log_pop_v1` (perche' una partizione
con cursore arretrato serva a volte solo i segmenti freschi) — follow-up, non
release-blocking.

## Atto VI — mpw negativo, T2 ri-misurata, e la forma finale del gap

**`minPopWaitTime=50` NON ingaggia** (run `mpw50`): batch medio fermo a ~860
(PGSS: 192.447 serve, 165,5M msg), pop invariato. La guardia usa `ready_est`
DI ANELLO, e sotto push continuo i `batch_count` non si azzerano mai (epoch
sempre bumpato dai mark in gara) ⇒ la stima copre sempre il batch e l'attesa
non scatta. La leva TASK M e' cablata sul ring scarso, non sul candidato
convergiuto. Diagnosi raffinata dei parziali: il ready ring e' round-robin PER
SHARD (24 shard da 4-5 partizioni, popolazioni disuguali) ⇒ intervalli di
rivisita sbilanciati (parziali da ~300 = rivisita a ~30 ms).

**Verifica dedup-a-luglio** (domanda di Alice): confermata dal sampler di quel
giorno — broker RSS 21,1 GB stabili / picco 35,8 durante la T1 del 23/07 =
la cache HashMap 64 B/hash × 300 s × 1M/s; senza dedup un broker a quel carico
sta a ~5 GB (misurato). Piu' la campagna 2026-07-23-openloop/dedup/ (11 log).

**T2 (QOS1: ack espliciti + async, 900k, 200 part, pp1, dedup 300 s) coi fix:**

| | luglio | oggi (bloom2) |
|---|--:|--:|
| push | 526,4M/526,5M (99,97%) | **525,7M/526,5M (99,84%), shed 0** |
| pop | 526,3M (~900k/s) | 395,8M (~660-720k/s) |
| ackErr / ackAvg | 15,6k / 67 ms | 40k / 104 ms |
| p50 / p99 | 129 / 358 ms | 155 / 1073 ms |
| PG / Queen | — | 4,1c / 9,1c |

Push a parita' (il bloom regge anche 900k × 300 s: cache 5,3 GB, 0
suppression). Il pop insegue con gap ~25%, piu' largo del ~10% della T1:
nella lane QOS1 l'intervallo di rivisita di una partizione include l'**RTT
dell'ack** (Took in lease → wheel → promote all'ack): ackAvg 104 ms ⇒
intervallo ~130 ms ⇒ cap batch 500 / 0,13 s × 200 part ≈ 770k < 900k ⇒
ratchet. A luglio l'ackAvg 67 ms teneva l'intervallo ~resonante (~111 ms
richiesti). Stessa famiglia del gap T1: il throughput pop e' governato
dall'intervallo di rivisita per-partizione, e le shape di luglio erano in
risonanza con i loro parametri.

**Il pacchetto post-release che chiude entrambe le lane** ("controllo
dell'intervallo di rivisita"): (a) min-pop-wait per-CANDIDATO (gate su
batch_count del claimato, plumbing TASK M gia' in loco); (b) fairness
per-partizione del ring (cursor pesato sulle popolazioni degli shard);
(c) per QOS1, promote-su-ack piu' aggressivo / revisit bounded sotto il pad
attuale. Nessuno dei tre e' release-blocking: la 1.0.0 coi due fix consegna
push a parita' di luglio su T1 e T2 e pop a 2,6× (T1) / 660-720k (T2) senza
perdite, contro i 320-430k di ieri.

## Atto VII — il processo al pop, con le prove (e la correzione di Alice)

Domanda diretta di Alice: DOVE e PERCHE' il pop e' peggiorato. Con l'avvertenza
che i commit di luglio arrivavano DOPO i benchmark passati — quindi l'ordine dei
commit non dice quale codice produsse quali numeri. Verifica rifatta su
artefatti e A/B, non su git.

### QOS0 (T1): colpevole la hotlist — PROVATO, doppia via

1. **Flag A/B su binario identico** (`QUEEN_HOTLIST=0` = path wildcard SQL):
   hotlist ON 870-930k con lag-ratchet a 82M; hotlist OFF **1.011-1.055k, lag
   finale 29.500** (luglio: 3.300), popped==pushed (193,93M). Il pop legacy
   SUPERA il push quando serve: il margine di recupero esiste.
2. **Impronta PG** (immune ai giochi di commit): luglio T1 = 61 backend / 14,3c
   PG / LockManager nelle wait; oggi legacy = 37 be / 8,7c / 6,2k commit — la
   stessa classe (pop SQL parallelo); oggi hotlist = 12 be / 3,2c / 2,3k —
   un'altra architettura. Luglio girava pop-SQL-parallelo, qualunque cosa
   dicesse il working tree.

### QOS1 (T2): il codice e' INNOCENTE — il gap e' del rig. PROVATO.

La correzione di Alice ha ribaltato l'indagine: le date vere dicono che l'ack
registry (e9dd171, 12:21) era GIA' nel build certificato, e `bee0ed9` ("after
fist complete test", **16:21 = nove minuti dopo la fine della T2 delle 16:02**)
e' lo snapshot dichiarato del codice certificato. Ricompilato e misurato:

| | luglio T2 (cert) | bee0ed9 OGGI | binario 1.0.0 oggi |
|---|--:|--:|--:|
| pop/s | ~900k | **~660k** | 611-682k (ogni variante flag) |
| PG / backend / commit/s | 13,7c / 58 / 8.708 | **11,4c / 51 / 8.871** | 3-4c / 20 / 3,4k |
| ackAvg | 66,8 ms | 71-79 ms | 94-104 fusion / 49-61 senza |
| serve/s → lease-hold | 1.754 → **114 ms** | 1.320 → **151 ms** | ~1.330 → ~150 ms |

Il build certificato, con l'impronta PG di luglio riprodotta quasi alla cifra,
fa 660k su questo rig — come OGNI build di oggi (bee == hl0 == hl0+af0 ==
hotlist +/- 5%). ⇒ per la T2 il codice non ha perso niente: e' il RIG che non
riproduce i 900k. La fisica: il pop QOS1 e' una catena SERIALE per partizione
(serve → ricezione client → ack → commit) e il throughput e' 200 partizioni ÷
lease-hold. Luglio teneva la catena a 114 ms, questo rig a ~150. Coerente col
segnale gemello sulla T1 legacy: stessa 1M/s di throughput ma p50 client 477 ms
contro i 118 di luglio — ogni operazione qui costa di piu' a parita' di codice.
Candidati (non piu' verificabili, VM morte): CPU della bench VM di luglio (mai
registrata; qui Xeon 6548N a ~2,1 GHz), punto-release di postgres:18, RTT VPC.
Scagionati con misure: client-go (ciclo consumer 94 ms oggi vs 104 luglio — il
client di oggi e' PIU' veloce), goload (riproduce 1M/1M su T1), dedup/bloom e
claim-fix (A/B sullo stesso binario), ack fusion (65k ackErr vs 2,5k e +30 ms
di ackAvg su questa shape — danno reale ma non il gap: spenta, restano 667k).

### Sintesi finale del pop

- **T1: regressione di codice, nella hotlist.** Meta' ripagata (claim fix,
  2,6×), reference = il legacy path che sullo stesso binario fa 1M/1M.
- **T2: nessuna regressione di codice.** Il 900k era il rig di luglio; il
  numero riproducibile di QUESTO rig per quella shape e' ~660-680k con
  qualunque build mai committata. L'unico danno di codice reale sulla lane e'
  l'ack fusion (errori + latenza su questa shape): default da rivedere.

## Atto VIII — box unico da 60 core: ack fusion sfatata, T3 CERTIFICATA

VM singola `queen-04` (60c/117GB, stesso 6548N, fdatasync 114 µs) con broker, PG
e loader insieme. I numeri ASSOLUTI non sono confrontabili con la campagna a due
VM (contesa CPU, niente rete); gli A/B relativi si'.

### Ack fusion: l'ipotesi degli shard e' SMENTITA

Shape T2 (900k, ack espliciti async), tre bracci identici salvo il flag:

| braccio | popped | ackErr | ackAvg |
|---|--:|--:|--:|
| off (`ACKFUSION=0`) | 84,10M | 44.000 | 112,6 ms |
| **s4** (il default del rig) | **84,12M** | 54.000 | 122,6 ms |
| s24 (parita' push fusion) | **78,37M (−7%)** | **71.000 (+31%)** | 120,9 ms |

Alzare gli shard PEGGIORA, in modo monotono. Il motivo, letto al contrario nella
mia diagnosi precedente: piu' shard = buffer piu' frammentati = **meno righe fuse
per commit** = piu' transazioni e piu' fsync. Il trade-off e' coalescing contro
concorrenza e su questa shape vince il coalescing.

**Correzione a quanto scritto nell'Atto VII**: il divario "65.500 vs 2.500
ackErr" confrontava `hl0-t2` (hotlist OFF, fusion ON) con `hl0af0-t2` (hotlist
OFF, fusion OFF) — cioe' misurava la fusion CON LA HOTLIST GIA' SPENTA. Con la
hotlist accesa (il default di prodotto) la fusion e' quasi neutra: pop identico
allo 0,02%, +10k ackErr su 84M messaggi = +0,012%. **Nessun fix necessario:
lasciare il default.** Resta valido il guadagno documentato il 24/07 nella shape
tenant (12,7 cursori/commit): la fusion serve dove i commit dominano.

### T3 channel-manager: PASS, parita' con luglio

Testata byte-identica a quella del 23/07 (1000 property, 25k ev/s, ramp 15s,
push-shards 1000, pop-batch 100, pp10, dedup 300s, lease 60s), build con bloom +
claim fix:

| | luglio (cert) | oggi |
|---|--:|--:|
| eventi e2e | 25k/s | **25k/s tenuti** |
| messaggi consegnati | 88.503.408 | **86.925.062 (98,2%)** |
| **dups / gaps / order-violations** | 0 / 0 / 0 | **0 / 0 / 0 su tutti e 12 gli stream** |
| ackErr / shed | 0 / 0 | **0 / 0** |
| pushErr | 2.096 | 2.802 |
| PG / Queen | 19c / 7c | 10,1c / 5,7c |
| commit/s · DB | ~14k · 8GB | 11,2k · 7,1GB |
| e2e p50 / p99 | 1,5-1,8s / 8-12s | 5,5s / 16s (fine run 10,7s / 39s) |

`VERDICT: PASS (0 gaps below frontier, 0 order-violations, 0 dups)`. Throughput,
integrita' e ordine totale sono quelli certificati a luglio; il 98,2% di volume e'
il residuo in-flight al cutoff (1,96M). La latenza e' 3-4x perche' gli executor
del channel manager (sleep 10-30 ms per hop, 4 hop) girano sullo STESSO box del
broker: ogni hop accumula coda. Con loader separato tornerebbe nel range di
luglio — non c'e' segnale di regressione ne' sul broker ne' su PG (che qui usa
meta' dei core di luglio per lo stesso lavoro).

## Atto IX — prova finale su due VM pulite: T3 REPLICATA (anzi battuta), T2 no

Rig finale: bench `queen-04` 60c (broker+PG, **PG fresco prima di ogni run**),
loader `queen-05` 32c separato, VPC privato RTT 1,4 ms — la topologia di luglio.

### T3 channel manager — replica piena

| | luglio (cert 23/07) | **oggi (bloom + claim fix)** |
|---|--:|--:|
| producedA / producedB | 7.406.175 / 7.406.075 | **7.406.796 / 7.406.681** |
| messaggi ackati | 88.502.190 | **88.755.886 (+0,3%)** |
| dups / gaps / order-violations | 0 / 0 / 0 | **0 / 0 / 0** |
| ackErr / shed | 0 / 0 | **0 / 0** |
| pushErr | 2.096 | **1.458** |
| e2e p50 | 1,5-1,8 s | **1,58-1,65 s** |
| e2e p99 | 8-12 s | **2,3-2,5 s (4x meglio)** |
| PG / Queen | 19c / 7c | **15,1c / 8,2c** |
| commit/s · backend | ~14k · 58-61 | 20,2k · 60 |

`VERDICT: PASS`. Non solo replicata: volume, errori e p99 sono **migliori** di
luglio, con PG che usa 4 core in meno. E la predizione fatta sul box singolo
(latenza 3-4x = co-residenza degli executor, non regressione) e' confermata:
stessa run, solo loader separato, p50 da 5,5 s a 1,6 s.

### T2 900k ack espliciti — l'unica che non replica

`pushed 175.286.300` a 900k/s pieni, **shed 0, pushErr 125, ackErr 0**,
`popped 124.803.100` = **594k/s**, ackAvg 105 ms, PG 4,3c / 17 backend.

Push perfetto, integrita' perfetta (ackErr finalmente a ZERO: anche i 40-73k del
box singolo erano artefatto di co-residenza), ma il pop resta a 594k contro i
900k di luglio — in linea con i 660k misurati sul rig DO a due VM con OGNI build,
inclusa quella certificata (`bee0ed9`).

### La forma finale della campagna

| shape | dominata da | replica luglio? |
|---|---|---|
| soak 600k | throughput parallelo | **si'** (p50 71 vs 87,6 ms) |
| T3 channel manager | throughput parallelo + PG | **si'**, anzi meglio |
| T1 1M autoAck | throughput parallelo | **si'** sul path legacy (1M/1M); la hotlist e' una regressione di codice, meta' ripagata dal claim fix |
| T2 900k ack espliciti | **catena SERIALE per partizione** (serve → receive → ack → commit) | **no**: 594-660k su ogni rig e ogni build |

Tre shape su quattro tornano. L'unica che non torna e' l'unica il cui throughput
non dipende dal parallelismo ma dalla latenza di una catena seriale — e su quella
dimensione questo ferro e' ~30% piu' lento (lease-hold 151 ms vs 114 derivato,
fsync sotto carico 1005 vs 820 µs a parita' di commit/s).

## Verdetto per la release

1. **1.0.0 vs 615efdc: nessuna regressione**; sulla baseline valida la candidata
   e' uguale in throughput/latenza e migliore su ackErr (fix truncation).
2. **I numeri "1M autoAck / 900k ack" del 23/07 non sono un target valido** per
   questa verifica: appartengono al dedup HashMap ritirato il 24/07. Ri-guadagnarli
   sul ring dedup = follow-up bloom (gia' pianificato nel TODO del soak).
3. Le baseline confrontabili per la 1.0.0 sono: **soak** (riprodotta ✓), **T3
   channel-manager** (contenuto-finestra per partizione ~2k hash → il ring non e'
   un fattore) e le run **proxy** della campagna 07-29/30.
4. Ceiling pop shape-T1 (~400k con batch 1000/pp10/100 part) da caratterizzare
   post-release: non e' una regressione (identico sulla build del soak) ma e' il
   secondo motivo per cui "1M/1M" non torna oggi.

## File

- `raw/ab-*`, `raw/val*` — le run shape-T1 (goload + sampler 1 Hz broker/loader).
- `raw/soakcheck/`, `raw/soaknew/` — shape soak su 615efdc e 1.0.0.
- `raw/perf-broker-dedup.txt` — 60% in verified_for_push.
- `raw/perf-loader-json-gc.txt` — profilo goload (escluso come causa: 32c==48c).
- `raw/broker-rates.log` — rates/sizes del broker (vegas al pavimento, pop_empty 99%).
