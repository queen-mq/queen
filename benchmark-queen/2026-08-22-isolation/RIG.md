# Rig — campagna hot-partition isolation (2026-08-22)

## Le macchine

| ruolo | ssh | VPC | ferro |
|---|---|---|---|
| broker `queen-01` | `root@46.101.186.250` | `10.114.0.2` | 16 vCPU Xeon Platinum 8358, 31 GiB, 193 GB ext4 |
| loader `queen-02` | `root@142.93.170.82` | `10.114.0.3` | 16 vCPU Xeon Platinum 8358, 31 GiB |

**Stessa CPU (8358) della campagna 2026-08-03/04**, che girava pero' a 32 vCPU:
i confronti con la fair-matrix sono a **meta' dei core**, e vanno dichiarati.

`pg_test_fsync` su queen-01: **fdatasync 90 µs/op** (open_datasync 94, fsync 117).
Classe del rig di luglio (~95 µs), NON del rig 1.0.4 del 20/08 (131 µs).

## Software

- broker: `ghcr.io/queen-mq/queen:1.1.0`, digest `sha256:42df7a659e1e61dd3f427897337b4bc29874a79f40d187c334146353a2488c4a` (creata 2026-08-21)
- PG del broker: `postgres:17` (17.11) via `docker-compose.queen.yml`
- docker 29.7.2, compose v5.5.0, Ubuntu 24.04.4

### Config cambiata rispetto al repo

1. `deploy/postgres.conf` diceva *"sized for an 8-core / 16 GB broker VM"*. Scalato
   per 16c/31GiB: `shared_buffers 4GB→8GB`, `effective_cache_size 10GB→20GB`.
   **Nient'altro toccato**: il resto resta byte-identico fra Queen e pgmq (SPEC §5.1).
   Originale conservato in `/root/cmbench/deploy/postgres.conf.orig-8c`.
2. `deploy/queen-tuned.env` **NON usato**: pinnava `QUEEN_IMAGE=queen:trace8`
   (build locale del 2026-08-03) e tuning derivato su 8c/16GB. Sostituito da
   `/root/arbenv.sh`: solo immagine + `CM_CPUS=16 CM_MEM=28g KAFKA_HEAP_OPTS=-Xmx8g`.
   1.1.0 con env pulito **non emette warning di knob deprecati** al boot.

## Baseline ancora validata — Queen 1.1.0, forma 2k/1000

```
./cmbench -system queen -queen-url http://10.114.0.2:6632 \
  -queen-pop-mode wildcard -queen-pop-partitions 40 \
  -rate 2000 -properties 1000 -duration 180 -ramp 10 -drain 90
```

| | valore |
|---|---|
| correttezza | **gaps 0 · order violations 0 · dups 0 · PASS** (exit 0) |
| e2e flow A | p50 **85,0** · p95 101,1 · p99 120,2 ms |
| e2e flow B | p50 77,9 · p95 92,7 · p99 120,2 ms |
| consegne | 2.111.958 |
| costo | **10,96 core di 16 (68%)** — queen 2,18 · **PG 8,78** |
| disco | 26 MB/s in scrittura |
| lane / consumer | 4.000 / 96 |

Riferimento storico della stessa forma (runbook): 311,7 ms su trace9-Vegas,
~1,1 s su arbiter9 — ma su **32 core**. Qui p50 85 ms su **16**. Promettente,
NON un A/B pulito: cambiano core, PG, build e tuning insieme.

## Pavimento a vuoto

Con 4.000 lane provisionate e **traffico zero**: **0,29 core** (queen 0,06 + PG 0,22).
La stima di 5,7 core estrapolata dalla matrice di agosto era **sbagliata**: i due
punti usati per il fit (2k/48 consumer e 12k/96 consumer) differiscono anche nel
numero di consumer, quindi non erano un fit sul solo rate.

Il disco a vuoto legge 12,8 MB/s nei 2 minuti dopo il carico: e' coda di
checkpoint/autovacuum, **non** stato stazionario. Serve una finestra idle lunga
per separarli — e' esattamente cio' che deve misurare la campagna cella/TCO.

## Trappole nuove (pagate oggi)

1. **`-queen-pop-partitions` e' load-bearing per la CORRETTEZZA, non solo per la
   latenza.** A pp=8 su 4.000 lane: `gaps 203` (drain 60 s) e `gaps 54` (drain
   90 s), con messaggi ancora non consegnati dopo 90 s di carico zero — le lane
   in coda alla rotazione non vengono mai visitate. A **pp=40** (regola
   documentata `pp ≈ lane/25`): **gaps 0**. Il default dell'esempio nel
   RIG-RUNBOOK (pp=8) **non vale per questa forma**.
2. `in-flight at cutoff` **non** implica perdita: la run PASS ne ha 48. Il
   segnale da guardare e' `gaps`.
3. `push_err` non nullo (9-16) anche su run PASS con `gaps 0`: il client conta
   l'errore ma il messaggio atterra lo stesso. Da capire, non bloccante.
4. Le run di oggi **non** hanno il sampler sul loader: SPEC §5.1 invalida una run
   senza la prova che il loader stesse sotto il 70%. Da mettere negli script di
   campagna prima delle run che finiscono nel report.
