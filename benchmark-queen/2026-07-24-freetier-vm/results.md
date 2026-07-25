# 2026-07-24 — Free-tier VM: regge 10 tenant? (2c/8GB shared, per-tenant)

VM `queen-free-01` @ **209.38.244.240** (DigitalOcean, **2 vCPU / 7.8 GiB / no swap**,
disco 157G, fdatasync ~130µs → SSD veloce), Ubuntu 24.04. Stack tutto in Docker
sulla stessa VM: **PG18** (tuned 8GB: shared_buffers 2G, work_mem 8M, pool 64,
`setup-broker-8gb.sh`) + broker **`queen-seg-rust:latest`** buildato dal working
tree `rustserverandstorage` (hotlist+ackfusion ON, dedup cache **256MB** non 20G,
`1.0.0-alpha-01`). Loader `goload -mode tenants` **co-locato** come processo host:
la CPU di broker/PG viene da `docker stats` (per-cgroup) quindi la CPU del loader
NON entra nei numeri server; a questi rate il loader è 0,05–0,30 core. Raw in `raw/`.

> Cosa misura: la **capacità risorse** del box condiviso sotto un workload a forma
> di tenant. NON misura l'enforcement di tenancy (quota JWT / isolamento) che sta
> sul branch `multitenant`. "Consumer" = code (1 consumer long-poll per coda).
> 1 core = 100% di un core; il tetto del box è **200%** (2 vCPU).

## TL;DR — sì, regge 10 tenant con margine ~8×

Alla forma cloud nominale (10 tenant × 10 code × 1 consumer, ~10 msg/s/tenant):
**~0,25 core server, e2e p50 12ms, 0 errori, 6,7 GB RAM liberi.** Su questo box
ci stanno molti più di 10 tenant così. I due tetti da conoscere per 10 tenant:
- **messaggi**: ~**480 msg/s aggregati** sostenibili (~48/tenant, ~5× il picco
  nominale) — limite = commit PG, **tunabile ×3-4 con windowBuffer**;
- **consumer parcheggiati**: ~**5.000 comodi**, 10.000 al limite (wake-tick).

## 1. Baseline nominale — 10 tenant × 10 code, 100↔20 msg/s ciclici

| | valore |
|---|---|
| code / consumer parcheggiati | 100 / 100 |
| provisioning 100 code | 0,9 s |
| push=pop=ack | 10.558 msg, **0 persi, 0 errori**, tail 0 |
| e2e p50 / p99 | **12 / 30 ms** (pushRTT p50 5ms) |
| CPU broker / PG / loader | 0,08 / 0,18 / 0,05 → **~0,25 core server** |
| RAM broker / PG | **19 MB** / 0,6 GB — 6,7 GB liberi |

Verdetto: **regge liscio, ~8× di margine sulla CPU, RAM appena sfiorata.** Le
latenze sono ~2× la VM enterprise di stamattina (p50 6-7ms) — atteso su 2 core
condivisi. Zero code in coda, zero perdite.

## 2. Tetto messaggi (10 tenant, forma a 100 code) — commit-bound, non HW-bound

Rampa a rate costante (10 tenant × 10 code):

| target | push reale | lag | e2e p99 | broker/PG/loader | esito |
|---|---|---|---|---|---|
| 500/s | ~455/s | 1–6 (stabile) | ~105 ms | 0,36 / 1,05 / 0,30 | **sostenibile** |
| 1000/s | **~490/s** (non sale) | 2.900→drena | **10–52 s** | ~1,7 core | saturo |
| 2000/s | **~480/s** (non sale) | spike | **11–52 s** | ~1,7 core | saturo |

Il box **plateau a ~480–510 msg/s aggregati** e non sale oltre, **fermandosi a
~1,7 di 2 core con 6,5 GB RAM liberi** → NON è CPU né RAM.

**Diagnosi (raw/diag.out):**
- disco fdatasync **~7.500 ops/s (~130µs)** → non è il disco;
- wait event sotto carico = quasi solo **`LWLock:WALWrite` / `LWLock:WALInsert`**
  (backend serializzati sul lock WAL, `CPU:run` basso) → i backend **dormono su
  un lock**, ecco perché non riempiono i 2 core;
- commit PG cap a ~1.900–2.300/s con **~4 commit per messaggio consegnato** (il
  "pop magro": batch <1) → ~1.900/4 ≈ **480 msg/s**. Torna.
- **`synchronous_commit=off`** (applicato per davvero via `ALTER DATABASE` +
  restart broker, verificato `off`): **nessun salto** (~500/s, commit 1.900→2.300)
  → il limite è il **lock WALInsert + il NUMERO di commit**, non l'fsync/flush.

**Leva vera = ridurre i commit/msg → `windowBuffer` (batching pop lato broker),**
la stessa leva ×3-4 del report enterprise di stamattina. Il tetto è **morbido/di
prodotto, non un muro hardware.** `synchronous_commit=off` da solo NON serve qui.

Nota forma: il tetto dipende dalla forma. 100 code = batch grassi = ~480/s; più
code a pari rate = pop più magri = tetto più basso (coerente col report enterprise).

## 3. Tetto consumer parcheggiati (10 tenant, idle, 1 consumer/coda)

| consumer | provisioning | broker CPU | broker RAM | PG CPU | errori |
|---|---|---|---|---|---|
| 100 | 0,9 s | ~0,04 core | 15 MB | ~0 | 0 |
| 1.000 | 1,5 s | ~0,15 core | 102 MB | ~0,05 | 0 |
| **5.000** | 8,6 s | **~0,6 core** | 498 MB | ~0,1 | **0** |
| 10.000 | 21,6 s | **1,0–1,8 core** | ~1,0 GB | spiky | **5.445 popErr** (storm conn) |

- **~5.000 parcheggiati = comodi** (0,6 core, 0 errori).
- **10.000 = al limite**: il broker brucia 1–1,8 core solo per *tenere* l'esercito
  (wake-tick O(#code)), ~1 GB RAM, provisioning 22s, e **5.445 pop error** durante
  lo storm di apertura di 10k connessioni su 2 core (enterprise ne faceva 400).
- **Floor idle scala col NUMERO DI CODE**: con 10.000 code in DB e **zero consumer
  connessi** il broker idla comunque a **1,15 core** a scandirle. È il wake-tick
  O(#code) noto e **fixabile** (dirty-list → floor ~0), stesso follow-up del report
  di stamattina.

## Bottom line per il piano free (2c/8GB shared)

- **10 tenant nominali: banali** — 0,25 core, margine ~8×. Il box ne reggerebbe
  parecchi di più a quel profilo (CPU-wise ~50-60 tenant nominali, ma prima
  toccano i due tetti sotto a seconda della forma).
- **Per 10 tenant hai spazio fino a ~480 msg/s aggregati E ~5.000 consumer
  parcheggiati**, comodi, con margine. (msg e consumer testati separatamente; un
  caso combinato pesante starebbe più in basso.)
- **Due limiti morbidi da sapere**: (1) throughput msg = commit-bound (~480/s a
  questa forma; **windowBuffer** lo alza ×3-4); (2) costo idle per #code =
  wake-tick (~1 core / 10k code; il fix dirty-list lo azzera). Nessuno dei due è
  RAM o disco — la RAM non è mai il vincolo qui.

Stack lasciato UP e pulito: dashboard `http://209.38.244.240:6632`, API `:6682`.
