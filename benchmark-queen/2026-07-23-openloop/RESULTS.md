# 2026-07-23 — Open-loop campaign: la caccia al massimo sostenibile a push-batch 100

VM: bench queen-01 139.59.151.52/10.114.0.2 (32c, fdatasync 68µs — miglior disco
delle tre giornate), loader 138.68.87.245/10.114.0.3 (32c al mattino, **48c dal
pomeriggio**). Broker queen-seg-rust:fix (= e3ce7ff: log engine + fix convoy +
fix cache), PG18 + wal_compression=lz4 + track_wal_io_timing=on. Loader nuovo:
goload `-mode openloop` (offerta imposta, latenze dall'istante schedulato,
shed contato — mai bloccare il pacer).

## 🎯 Risultato finale — CERTIFICATO (loader 48c)

**1.000.000 msg/s per lato, push-batch 100, open-loop, 300 secondi:**
300.049.600 offerti → 299.990.900 consegnati (99,998%) → 299.984.600 consumati;
shed=0, pushErr=0, popErr=0, **lag finale 6.300 msg**; **p50 81ms / p99 358ms /
p999 594ms**; retention attiva (sweep 5s), DB stabile 15GB; broker ~6/32 core.
= **2M msg/s combinati su un singolo Postgres.**
Config: commit_delay 500/8 (indifferente vs default), Vegas adattivo (il floor
pop NON serve), pop-batch 1000, pop-partitions 10, 850 consumer, 100 partizioni,
payload 256B, dedup off, idle-conns 8192.

## La giornata in tabella (leve provate)

| Test | Leva | Verdetto |
|---|---|---|
| T1 | `-idle-conns` ≥ worker | **+9,7%** (TIME_WAIT 26k→0; msg/commit +18,5%). SEMPRE. |
| T2 | sweep open-loop pop500 | ginocchio ~950k; 900k×300s sostenibili (loader 32c) |
| T3 | commit_delay 500/1000µs | **vicolo cieco**: group commit già saturo (~4,6 commit/fsync); 1000µs peggiora. WAL scagionato coi numeri veri: fsync ~1300/s×300µs (<10% busy), ~50MB/s |
| prof 90s | perf + thread + locks + vegas | broker scagionato (24 thread al 22-27%, perf piatto), log_consumers scagionato (lock ~0) |
| floor pop 48 | Vegas pop 18→48 | **+2%** — ammissione scagionata; push regge 1M, pop no |
| pp5 | pop-partitions 10→5 | nessun effetto — claim-parallelism scagionato |
| ldrcheck | CPU loader live | **COLPEVOLE: goload 2968%/3200 (93%), idle 4%** — parse JSON risposte pop su 32c |
| 48c | upgrade loader | **1M/side certificato** (sopra) — p99 crolla da ~2s a 358ms |

Lezione di metodo (terza volta in tre giorni): il collo apparente era lo
strumento — connessioni, sampler stale, CPU di parse. Ogni scagionamento è
arrivato da una misura, non da un'opinione.

## Contenuto directory

- t1/ — idle-conns A/B (goload.out, loader/bench csv)
- t2/ — sweep open-loop (loader_*.log, mon, commit, waits)
- t3/ — commit_delay (results.md + raw/)
- t4-partial/ — sweep pop1000 interrotto (step_950000.log tracking pulito a 950k; 1M fallito col loader 32c)
- final-cert/ — prof1m/popfloor1m/pp5/ldrcheck.log + profiling.tgz (perf, thread, waits, locks, vegas). I run 90s+300s della certificazione sono riportati sopra (stdout diretto).

## Aperti

Soak multi-ora a 1M; dedup-ON a 1M (tassa era ~zero a 738k); densità cache
dedup (~64B/hash reali → ring-only ~16-20B, 4x finestra/GB); multitenant 2000
code; aggiornare doc 17 (numeri e metodo open-loop); ramp producer per azzerare
i deadlock warm-up (cosmetico); backoff esponenziale in goload (gentilezza
negli incidenti).
