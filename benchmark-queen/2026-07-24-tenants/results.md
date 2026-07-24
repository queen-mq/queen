# 2026-07-24 — Test "tanti ma piccoli": 1000 tenant × 10 code (sizing enterprise cloud)

VM: queen 165.245.251.124 (32c/62GB, disco fdatasync ~95µs), loader 139.59.156.151
(48c/94GB). Broker `queen-seg-rust:hl4` = ef6fb4f (hot-list + ack fusion ON di
default), PG18, boot memory-safe (SHBUF 16G, DEDUPMB 4G). Loader: nuovo
`goload -mode tenants` (goload/tenants.go): T tenant × Q code, 1 consumer
long-poll (25s) con ack espliciti per coda, traffico per-tenant ciclico
10↔2 msg/s ogni 120s con fasi sfalsate random; `-idle-only` spegne i producer.
Sampler 1 Hz su entrambe le VM. Raw in `raw/`.

## I tre scenari

| | SMALL | IDLE | BIG |
|---|---|---|---|
| Tenant × code | 10×10 = 100 | 1000×10 = 10.000 | 1000×10 = 10.000 |
| Consumer parcheggiati | 100 | 10.000 | 10.000 |
| Traffico aggregato | ~52 msg/s | **zero** | ~6.000 msg/s (10k↔2k ciclico) |
| **PG CPU (media)** | **0,53 core** | **0,55 core** | **12,1 core** (picco 13,8) |
| **Queen CPU (media)** | **0,07 core** | **0,99 core** | **4,2 core** (picco 4,8) |
| Queen RAM | ~0 | 1,13 GB | 1,54 GB |
| DB size | 23 MB | ~0 | 1,6 GB |
| e2e p50 / p99 | 6-7 / 9-11 ms | — | 334 / 610 ms |
| Errori | 0 su 35.174 | 400 popErr (solo storm conn iniziale) | **0 su 5,4M** msg (2 run) |
| Consegna | 100%, tail 0 | — | 100%, tail ~1,4k in-flight al cutoff |

Provisioning: 10.000 code configurate in **0,9-1,1 s** (le INSERT ordinate del
fix livelock reggono la creazione di massa senza un fremito).

## Risposte alle tue domande

**"Quanta VM mi serve all'inizio?" (10 tenant)** — Praticamente niente:
l'intero scenario costa **0,6 core totali** e latenze 6-7ms. Una VM 2c/4GB
(PG compreso) serve i primi dieci tenant con margine 3x. Il broker a basso
carico si comporta benissimo: fusion fire-on-idle = zero hold, e2e single-digit.

**Costo del "presidio" (idle)** — 10.000 code + 10.000 consumer parcheggiati
a traffico ZERO costano **~1,5 core totali** (PG 0,55 + Queen 1,0) e 1,1 GB.
Composizione misurata:
- PG: stats-refresh `log_refresh_all_stats_v1` = 182 ms ogni 10 s (**scala col
  numero di code** — c'è già il tunable STATS_INTERVAL_MS, per l'enterprise va
  alzato o reso incrementale), reseed floor hot-list ~330 scan/s da 0,2 ms
  (trascurabile), retention/autovacuum di fondo.
- Queen: sospetto principale il **wake-tick della hot-list che itera 10k code
  ogni 5 ms** (2M check/s) — fix banale (lista dirty invece di scan): porterebbe
  il floor di Queen da ~1 core a ~0. Segnato come follow-up.
- I 400 popErr sono solo lo storm di apertura di 10k connessioni al t=0.

**1000 tenant attivi (~6k msg/s)** — 12 core PG + 4,2 Queen ≈ **16-17 core
totali**: la VM 32c li serve con margine 2x, zero errori su 5,4M messaggi,
p99 sotto i 700ms. Estrapolando: **~500 msg/s per core PG** in questo regime.

## IL finding per il prodotto: nel "tanti ma piccoli" il costo è il pop magro

Attribuzione pg_stat_statements del BIG: il **pop path è ~2/3 del costo PG**
(7,1M list-pop da ~1,7ms complessivi per ~5,4M messaggi = **1,3 pop per
messaggio consegnato**, batch medio <1!). Col traffico sparso su 10k code il
wake sveglia il consumer a ogni goccia: ogni messaggio paga un pop intero.
Anche la push fusion non può bundlare (3,3 segmenti/commit vs 16+ nel caso
consolidato). L'ack fusion invece TIENE anche qui: 12,7 cursori/commit.

**La leva è già nel prodotto: windowBuffer.** Un default di 200-500 ms sulle
code dei tenant piccoli farebbe ingrassare i pop (da 1,3 pop/msg verso
~0,2-0,3), con un costo di latenza irrilevante per questo profilo d'uso.
Stima: capacità per core ×3-4 nel regime enterprise. Da A/B-are: stesso BIG
con windowBuffer 300 ms — un run da 10 minuti quando vuoi.

## Note operative

- Nessun OOM, nessun collasso, RSS piatte in tutti gli scenari.
- popErr di apertura: il client potrebbe rampare le connessioni (cosmetico).
- Il loader regge 10k long-poll + 1000 pacer con ~33k goroutine senza sforzo.

---

# Addendum (critiche di Alice): asse partizioni + indagine latenza

## Variante partition-heavy (forma SmartChat: poche code × molte partizioni)

1000 tenant × **1 coda × 100 partizioni** = 1.000 code, **100.000 partizioni
reali** (create e popolate: 2,51M msg, zero errori), 10 consumer wildcard per
coda (10k parcheggiati come prima).

| | queue-heavy (10k code × 1 part) | **partition-heavy (1k code × 100 part)** |
|---|---|---|
| **IDLE: PG** | 0,55 core | **1,16 core** |
| **IDLE: Queen** | 0,99 core | **0,44 core** |
| Queen RAM idle | 1,13 GB | 1,92 GB |
| **Carico ~6k msg/s: PG** | 12,1 core | **14,5 core** |
| **Carico: Queen** | 4,2 core | **6,2 core** |
| e2e p50 | 238 ms (vedi sotto) | **63-76 ms** |

Letture: (1) la critica era fondata — l'**idle PG scala con le partizioni**
(2,1x per 10x partizioni; driver: stats-refresh e autovacuum su tabelle
100k-righe), e sotto carico la forma SmartChat costa +20% PG / +47% Queen a
parità di rate; (2) l'**idle Queen scala con le CODE** (0,99→0,44 con 10x meno
code) — coerente col wake-tick O(#code); (3) il floor combinato resta
gestibile: ~1,6 core per la forma SmartChat con 100k partizioni. Proiezione
134k partizioni SmartChat: idle ~1,5-2 core PG. Il tunable STATS_INTERVAL_MS
e lo stats incrementale sono la leva se serve di meno.

## Indagine p50 334ms (critica 2) — stato

Fatti stabiliti con esperimenti mirati (tutti riproducibili, raw in raw/):
- NON è il pacer del loader (stamp al build; pushRTT misurato separatamente:
  p50 19-34ms), NON è la CPU del loader (3/48 core), NON è il backoff cap
  (334→243ms con cap 1000→150: non proporzionale), NON è la hot-list
  (identico con HOTLIST=0), NON è la wake in sé (probe manuale con 10k
  parcheggiati: push→delivery 18ms).
- **La latenza di scoperta scala ~linearmente col NUMERO DI CODE** in
  entrambi i path: ~5ms extra @100 code, ~35ms @1k, ~210ms @10k (~20µs/coda).
  È il motivo per cui SMALL faceva 6ms e la forma SmartChat 71ms.
- Bug distinto scoperto a margine (path legacy, idle): push sparso verso
  consumer lungo-parcheggiato = scoperta a ~60s ESATTI senza redelivery
  (attempt_count=1) — un pop fresco sulla stessa coda riceve in 18ms. Puzza
  di macchina del watermark empty-scan.
- Indagine con repro locale in corso (agente dedicato): sospetti principali
  un'iterazione periodica O(#code) condivisa (parked-gauge/replica 1Hz,
  metrics per-queue flush, o gemelli del wake-tick).

Implicazione per l'enterprise: il p50 multitenant NON è strutturale — a
parità di tutto la forma 1k-code sta a 71ms. Fixata l'iterazione O(#code),
l'attesa è p50 <100ms anche a 10k code. Il windowBuffer proposto resta valido
e NON si somma al bug (che va comunque fixato prima di qualunque SLO).

---

# Batteria FINALE (hl10: clear-su-ack drained-scoped + hook seek/delete + priority-inversion + NACK fix)

| | mattina (pre-fix) | **finale hl10** |
|---|---|---|
| SMALL e2e p50/p99 | 6-7 / 9-11 ms | **7,5 / 10 ms** (pushRTT 3,3) |
| IDLE 10k: PG / Queen | 0,55 / 0,99 core | 0,58 / 0,98 core (wake-tick clone = follow-up) |
| **BIG 10k code ~6k msg/s: e2e** | **334 / ~650 ms** | **59,6 / 148 ms** |
| BIG: lag ingresso | 1-2k | **0** |
| BIG: PG / Queen | 12,1 / 4,2 core | **9,3 / 4,25 core** (−23% PG) |
| **PARTS 100k partizioni: e2e** | 63-76 / 224 ms | **34,6 / 108 ms** |
| PARTS: PG / Queen | 14,5 / 6,2 core | **11,0 / 5,4 core** |
| PARTS-IDLE: PG / Queen | 1,16 / 0,44 core | 1,17 / 0,42 core |
| Errori (2 run carichi, 3,25M msg) | 0 | **0** |

Sizing enterprise aggiornato: 1000 tenant attivi ≈ **13,5 core totali** (~640 msg/s
per core PG, era ~500); latenze multitenant da ~1/3 di secondo a <150ms p99.
Catena dei fix di oggi: priority-inversion sui poll vuoti → NACK promote +
reseed wheel-reclaim → clear-su-ack (covered + batch_count + drained
claim-scoped) → hook seek/delete. Restano: wake-tick O(#code) clone (idle ~1
core a 10k code), windowBuffer come moltiplicatore ×3-4 del regime sparso.
