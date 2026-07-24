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
