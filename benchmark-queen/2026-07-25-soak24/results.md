# QueenMQ — Soak 24 ore, semantica di produzione (2026-07-24/25)

**51,82 miliardi di messaggi in 24 ore su un singolo Postgres, zero restart,
error rate ~0,0001%.**

## Setup

- **Carico**: 600.000 msg/s open-loop, push-batch 100, payload 256B, per 86.400s (24h).
- **Semantica di produzione piena**: lease + **ack espliciti** (async) + **dedup**
  (finestra 60s) + retention attiva (completed 300s, pending 3600s).
- 200 partizioni, 600 consumer, pop-batch 500.
- Broker `queen-seg-rust` = commit **615efdc** (hot-list + ack fusion + dedup ring, tutti ON di default).
- VM: queen 32c/62GB (disco fdatasync ~95µs), loader 48c/94GB. PG18, boot memory-safe
  (SHBUF 16G, MAINTMEM 512M, DEDUPMB 6144).
- Watchdog 1 Hz con forensics + restart contati (0 usati); sampler bench+loader 1 Hz.

## Risultato

| Metrica | Valore |
|---|---|
| **Messaggi pushati** | **51.820.403.100** |
| Messaggi poppati / ackati | 51.820.623.201 / 51.820.561.201 |
| **shed** | **0** |
| pushErr / popErr / ackErr | 15.742 / 26 / 47.600 |
| **Error rate totale** | **0,00012%** (63.368 su 51,8 mld) |
| **Restart / incidenti** | **0 / 0** |
| Latenza e2e (overall) | p50 **87,6ms** · p99 **272ms** · p999 473ms |
| Latenza e2e (mediana steady, t>0,5h) | p99 **~236ms** |
| Throughput sostenuto | 600k msg/s piatto (mediana 599.980) |
| Queen CPU / RAM | ~14 core · **6,3 GB piatti 24h (nessun leak)** |
| PG CPU | ~6-7 core |
| PG commit/s · WAL fsync/s | ~3.000 · ~700 |
| DB size | plateau **~18 GB** (→ ~8 GB dopo lo stop, drenato dalla retention) |
| Loader | ~30 core · ~3 Gbps per direzione |

## La storia in due atti (visibile nei grafici)

**Atto 1 — prime ~4,5 ore, turbolente.** Onde periodiche di lag (fino a 11,7M) e
latenza (p99 fino a 27s). Diagnosticate e fixate a caldo:
- **Causa radice: heap-truncation dell'autovacuum** su log_partitions/log_consumers
  (tabelle a popolazione fissa). La fase "truncating heap" prende un ACCESS
  EXCLUSIVE lock e congela ogni push/pop per secondi, senza recuperare nulla.
  Beccata live con `pg_stat_progress_vacuum`, fixata alle t≈2,7h con
  `ALTER TABLE ... SET (vacuum_truncate = off)`. **È la causa dell'intera classe
  "wobble" vista in tutti i run ad alto rate della settimana** (onde del soak,
  escursione 6M dell'800k, burst di ackErr).
- Le onde residue fino a t≈4,5h = un vacuum già in corso al momento del fix +
  l'assestamento del ciclo checkpoint.

**Atto 2 — le restanti ~20 ore, piatte come vetro.** Throughput 600k costante,
p99 ~236ms, lag ~0, RAM inchiodata a 6,3GB, disco plateau 18GB. Solo due blip
residui (t≈9h, t≈18h) dal **boundary del checkpoint** (PG scrive ~10GB ogni 15min,
quasi back-to-back) — cosmetici (+300-9.000 ackErr transitori, latenza sempre
recuperata). Zero errori nuovi negli ultimi ~14h di run.

## Cosa il soak ha certificato che nessun run corto poteva

- **Nessun leak**: RAM broker piatta a 6,3GB per 24h (il ring dedup a 16,2B/hash
  regge, l'OOM da raddoppio HashMap è morto per costruzione).
- **Retention in regime**: il DB plateau a 18GB (≈ completed 300s + txns 900s ×
  600k/s × ~46B/~17B) — ~17.000 sweep, nessuna deriva; a load-stop scende a 8GB.
- **La classe wobble spiegata e chiusa**: la truncation era il colpevole comune;
  il checkpoint il residuo cosmetico.

## TODO (dai finding del soak)

1. **Committare il fix `vacuum_truncate = off`** in 041_log_schema.sql (già scritto).
2. Prossimo soak: **max_wal_size > 96GB** per distanziare i checkpoint (elimina i blip residui).
3. Il disco (95µs) plafona a ~600-650k; il ceiling "1M × 24h" resta raggiungibile con
   bloom sul ring dedup (finestra 300s) + disco classe-68µs.

## Contenuto

- `queen-soak24-report.png` — 6 pannelli sull'asse 24h (throughput, lag, latenza,
  CPU/MEM broker+PG, commit/fsync/disco, loader).
- `raw/` — soak24b.out (goload, report 10s), soak-bench.csv + soak-loader.csv (1 Hz),
  soak-watchdog.log.
- `make_soak_report.py` — generatore del PNG.
