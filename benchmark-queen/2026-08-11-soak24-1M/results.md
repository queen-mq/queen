# QueenMQ — Soak 24 ore a 1M msg/s (2026-08-10/11)

**86.369.975.300 messaggi in 24 ore su un singolo Postgres, un milione al secondo
per direzione, semantica di produzione piena, zero restart, memoria del broker
piatta a 4,1 GB.**

## Setup

- **Carico**: 1.000.000 msg/s open-loop, push-batch 100, payload 256B, per 86.400 s (24h), rampa 60 s.
- **Semantica di produzione piena**: lease + **ack espliciti** (async, 256 in volo) + **dedup**
  (finestra 60 s) + retention attiva (completed 300 s, pending 3600 s).
- 1 coda (`segbench`), 200 partizioni, 1 consumer group, 600 consumer, pop-batch 1000.
- **Broker `queen` 1.0.0** (commit `074de2e`). L'immagine è stata costruita dagli stessi sorgenti
  prima del bump di versione, quindi si autodichiara `1.0.0-beta.4` negli screenshot: il commit
  1.0.0 non tocca `server/src`.
- PG 18.4 (`postgres:18`): shared_buffers 16 GB, maintenance_work_mem 512 MB, max_wal_size 96 GB,
  synchronous_commit on, commit_delay 200, wal_compression on.

| ruolo | host | ferro |
|---|---|---|
| broker + PG | `queen-01` 68.183.72.48 (10.114.0.2) | 32 vCPU Xeon Gold 6548N (no HT), 62 GiB, 387 GB ext4, `fdatasync` ~70 µs |
| loader | `queen-load-01` 164.90.219.77 (10.114.0.4) | 16 vCPU / 31 GiB |
| loader | `queen-load-02` 138.68.109.22 (10.114.0.5) | 16 vCPU / 31 GiB |
| loader | `queen-load-03` 159.89.106.160 (10.114.0.6) | 16 vCPU / 31 GiB |

VPC misurata a 10,1 Gbit/s per direzione (iperf3, 8 stream). Tre loader e non uno perche' a 1M
msg/s la generazione e il consumo del carico costano ~20 µcore per operazione-messaggio, cioe'
~40 core solo per il banco: una singola VM da 48 core si ferma a ~950k sul lato pop.

## Risultato

Somma delle righe `[final]` dei tre loader (`raw/loader-0*/g.out`):

| Metrica | Valore |
|---|---|
| Messaggi offerti | 86.370.017.100 |
| **Messaggi accettati (push)** | **86.369.975.300** |
| **Scartati dal cap in volo del loader (shed)** | **0** |
| Messaggi poppati | 86.369.532.700 |
| Messaggi ackati | 86.369.517.700 |
| Richieste di push fallite | 4 |
| Richieste di pop fallite | 0 |
| Ack falliti (messaggi) | 600 |
| **Restart / incidenti** | **0 / 0** |
| Latenza e2e sull'intero run | p50 88,6-100,9 ms · p99 284,7-313,3 ms · p99.9 383,0-419,8 ms |
| Ack round trip medio | 84,0-95,4 ms |

Throughput medio accettato: 86.369.975.300 su 86.400 s = **999.652 msg/s**, rampa inclusa.

### Stabilita'

Dai contatori del broker (`raw/bench/metrics.csv`), throughput per ora:

| ora | push/s | pop/s | ora | push/s | pop/s |
|---|--:|--:|---|--:|--:|
| h01 | 999.856 | 999.861 | h13 | 999.957 | 999.959 |
| h02 | 1.000.015 | 1.000.014 | h14 | 999.979 | 999.978 |
| h03 | 1.000.171 | 1.000.173 | h15 | 1.000.006 | 1.000.006 |
| h04 | 999.798 | 999.783 | h16 | 999.968 | 999.959 |
| h05 | 1.000.205 | 1.000.199 | h17 | 1.000.114 | 1.000.124 |
| h06 | 999.817 | 999.822 | h18 | 1.000.005 | 1.000.003 |
| h07 | 1.000.194 | 1.000.200 | h19 | 1.000.132 | 1.000.122 |
| h08 | 999.925 | 999.931 | h20 | 999.821 | 999.828 |
| h09 | 999.888 | 999.881 | h21 | 1.000.050 | 1.000.044 |
| h10 | 1.000.197 | 1.000.198 | h22 | 999.930 | 999.934 |
| h11 | 999.787 | 999.782 | h23 | 1.000.015 | 1.000.006 |
| h12 | 1.000.064 | 1.000.076 | | | |

Ventitre' ore consecutive dentro una banda di **±0,02%**. Su 1.401 finestre da un minuto, una
sola cade fuori dal ±5%, ed e' il minuto della rampa.

### Latenza, distribuzione sui 24h

8.643 report da 30 s (tre loader):

| percentile riportato | mediana | p90 | p99 | massimo |
|---|--:|--:|--:|--:|
| p50 | 92,7 ms | 106,0 ms | 117,2 ms | **160,8 ms** |
| p99 | 288,8 ms | 325,6 ms | 358,4 ms | **593,9 ms** |
| p99.9 | 383,0 ms | 432,1 ms | 489,5 ms | **856,1 ms** |

Nessun intervallo da 30 s ha superato il secondo, in nessuno dei tre loader, in 24 ore.

### Risorse

Sampler 1 Hz sull'host del bench, 86.417 campioni (`raw/bench/bench.csv`):

| Risorsa | Valore |
|---|---|
| **Memoria residente del broker** | **media 4,10 GB, picco 4,37 GB** |
| CPU broker | media 10,92 core di 32, picco 12,90 |
| CPU PostgreSQL | media 11,54 core, picco 13,88 |
| Backend attivi | 43 |
| Commit PostgreSQL | 8.056/s |
| WAL | 83 MB/s, **6,8 TB** sul run |
| fsync WAL | 598/s, latenza media 714 µs |
| Dimensione del DB | banda 29,6-31,4 GB, finale 31,2 GB |

La curva del DB e' piatta dalla mezz'ora in poi: 29,8 / 29,6 / 30,2 / 30,0 / 29,9 / 31,4 / 30,4 /
31,2 GB ai passi di tre ore. E' la retention a regime — il plateau lo fissano la finestra e il
tasso, non la durata del run.

## Note di lettura

- `popped` sta sotto `pushed` di 442.600 messaggi (0,0005%): e' il residuo in volo al taglio dei
  86.400 s, non perdita. Il conto del broker (pagina Analytics: 85B ingested / delivered / acked,
  **ack failures 0**) coincide con quello dei loader.
- Le 4 richieste di push fallite valgono 400 messaggi; sommate ai 600 ack falliti fanno **1.000
  eventi su 86,37 miliardi**.
- 41.800 messaggi offerti non sono mai stati accettati (0,000048%), con shed a zero.

## Artefatti

| File | Contenuto |
|---|---|
| `raw/loader-0*/g.out` | stdout dei tre loader: 2.880 righe da 30 s ciascuno piu' l'aggregato finale |
| `raw/loader-0*/loader.csv` | sampler 1 Hz per loader: CPU, RSS, rx/tx di rete |
| `raw/bench/bench.csv` | sampler 1 Hz sull'host: CPU e memoria di broker e PostgreSQL, commit, WAL, dimensione del DB, wait event |
| `raw/bench/metrics.csv` | contatori cumulativi del broker ogni 10 s piu' i quantili rtt per operazione |
| `raw/bench/broker.log` | righe di boot, rate e cicli di manutenzione del broker |
| `raw/bench/restarts.count` | contatore dei riavvii del watchdog (0) |
