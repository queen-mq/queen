# 2026-08-20 — "tanti ma piccoli" RERUN su 1.0.4 (confronto con 2026-07-24)

Ripetizione del test di densità enterprise di [2026-07-24](../2026-07-24-tenants/results.md)
sulla release **1.0.4** (scheduler claim-row, stats refresh dimagrito, pending gate
sui pop pinned/discovery, endpoint depth).

**Rig.** queen 46.101.193.166 (32c/62GB, Ubuntu 24.04, PG **18.6 nativo**, broker
`ghcr.io/queen-mq/queen:1.0.4` in docker con `--network host`), loader
209.38.206.19 (**8c**/15GB, stessa VPC, RTT 1ms). Loader `goload -mode tenants`
byte-identico a quello di luglio (README della campagna 2026-07-29 lo certifica),
quindi il confronto è sullo stesso codice di carico.

**Le due colonne sono calcolate con lo STESSO summarizer** (`tools/summarize.py`,
scarta il primo 20% come warm-up) girato anche sui CSV grezzi di luglio: i valori
"luglio" qui sotto sono ricalcolati, non copiati dalla prosa. Dove differiscono
dal report di luglio lo dico esplicitamente.

## ⚠️ I due caveat che vanno letti prima dei numeri

1. **Il disco è più lento di quello di luglio.** `pg_test_fsync`: **fdatasync
   131 µs/op** contro i ~95 µs del rig di luglio (+38%). Irrilevante per IDLE
   (traffico zero, quasi nessun commit), pesa qualche ms su SMALL, e **domina
   BIG**, che è commit-bound: il profilo delle wait lo dimostra da solo —
   `WALWrite` 56% qui contro 31% a luglio. BIG **non è un A/B pulito**, è una
   misura su hardware diverso.
2. **Non si sta misurando solo la 1.0.4.** Luglio girava `ef6fb4f`
   (pre-1.0.0). Fra i due punti c'è un mese di lavoro: arbitro admission v5,
   riscrittura di `log_pop_list_v1`, hot-list windowed reseed, KV+timers, oltre
   alla 1.0.4. Attribuire *tutto* il delta alla 1.0.4 sarebbe falso; dove si può
   attribuire (stats refresh, gate) lo dico nella sezione per-scenario.

## I tre scenari

| | SMALL 07-24 | **SMALL 08-20** | IDLE 07-24 | **IDLE 08-20** | BIG 07-24 | **BIG 08-20** |
|---|---|---|---|---|---|---|
| Tenant × code | 10×10 = 100 | 100 | 1000×10 = 10.000 | 10.000 | 10.000 | 10.000 |
| Consumer parcheggiati | 100 | 100 | 10.000 | 10.000 | 10.000 | 10.000 |
| Traffico | ~52 msg/s | **~70 msg/s** | zero | zero | ~6.000 msg/s | ~5.900 msg/s |
| **PG CPU** (core) | 0,53 | **0,13** | 0,54 | **0,17** | 10,07 | **11,64** |
| **Queen CPU** (core) | 0,07 | **0,05** | 0,96 | **0,64** | 3,58 | **3,76** |
| **TOTALE** (core) | 0,59 | **0,19** | 1,50 | **0,81** | 13,65 | **15,40** |
| Queen RAM | 0,04 GB | 0,04 GB | 1,12 GB | 1,30 GB | 1,55 GB | 1,74 GB |
| commit/s | 309 | 247 | 1.706 | 1.611 | 10.315 | 10.815 |
| e2e p50 / p99 | 6-7 / 9-11 ms | **8 / 16 ms** | — | — | 334 / 610 ms | **33-40 / 104-134 ms** |
| Errori | 0 | **0 su 18.470** | 400 popErr (storm iniziale) | **0** | 0 su 5,4M | **0 su 3,6M** |

(I valori BIG di luglio ricalcolati — 10,07 / 3,58 / 13,65 — sono più bassi di
quelli pubblicati allora — 12,1 / 4,2 / ~16 — perché il report di luglio citava
finestre diverse. Il confronto qui è comunque omogeneo: stesso script su
entrambi i CSV.)

## SMALL — il costo per i primi dieci tenant crolla

**0,59 → 0,19 core totali (3,1x), con il 35% di traffico IN PIÙ** (~70 msg/s
contro 52). PG da 0,53 a 0,13 core.

L'attribuzione dice che non resta quasi niente di manutenzione:

| quota | query |
|---|---|
| 56% | `log_pop_list_v1` (payload) |
| 26% | `log_push_multi_v1` (payload) |
| 12% | `log_ack_multi_v1` (payload) |
| 1% | `log_refresh_all_stats_v1` (31 chiamate, 5,5 ms l'una) |
| 1% | `log_retention_step_v1` + `log_txns_purge_step_v1` |
| 0% | **claim row dello scheduler: 612 chiamate, 0,017 ms l'una, 11 ms in totale** |

**Il payload è il 94% del tempo DB.** Lo scheduler claim-row della 1.0.4 — la
cosa che si poteva temere costasse — è gratis: 11 ms su 300 s.

Latenza p50 8 ms contro 6-7 ms di luglio: è il disco (il path è commit-bound),
non il codice. La riga "una VM 2c/4GB serve i primi dieci tenant con margine 3x"
del report di luglio diventa **margine ~10x**.

## IDLE — il presidio dimezza, ma il colpevole è cambiato

**1,50 → 0,81 core (1,85x).** PG 0,54 → 0,17 (3,2x), Queen 0,96 → 0,64 (1,5x).

Il comportamento dei consumer parcheggiati è esattamente quello di design:
**711.006 pop vuoti in 1800 s = 395/s su 10.000 consumer**, cioè ogni consumer
parcheggia per l'intero timeout di long-poll (25 s) e ripolla. Nessuna tempesta
di re-poll, zero errori (a luglio: 400 popErr nello storm di apertura).

Ma l'attribuzione ribalta la diagnosi di luglio:

| quota | query | note |
|---|---|---|
| **29%** | `log_refresh_all_stats_v1` | 180 chiamate, **121,6 ms** l'una (luglio: 182 ms) |
| **18%** | `INSERT INTO queen.queue_lag_metrics` | **260.000 insert** |
| 10% | lookup config per pop (`delayed_processing`/`window_buffer`) | 600.000 |
| 9% | `log_hotlist_reseed_v1` | 544.487 |
| **8%** | `INSERT INTO queen.queue_parked_replica` | **260.000 insert** |
| 6% | `consumer_groups_metadata` EXISTS | 320.000 |
| 6% | `lease_time` per coda | 300.647 |
| 4% | *il mio sampler* (misura, non prodotto) | 1.806 |

### Finding 1 — il refresh scala con le CODE, e la 1.0.4 ha ottimizzato un ALTRO asse

182 → 121,6 ms per chiamata è solo 1,5x, contro il **7x misurato in produzione**
(1.067 → 148 ms). Non è una contraddizione: la 1.0.4 ha tolto i termini che
scalano con **partizioni e segmenti** (in prod: 54k partizioni, 1,36M segmenti,
1 GB di heap scansionato). Qui ci sono 10.000 code con **una partizione l'una e
quasi nessun segmento**: quei termini erano già a zero. Quello che resta è
O(code) — il rollup `per_queue`, l'upsert di 10.000 righe in `queen.stats`, gli
aggregati namespace/task/system.

**Conseguenza da tenere a mente per l'enterprise:** a 10k code il refresh costa
121 ms ogni 10 s (~1,2% di un core, irrilevante). A **100k code** sarebbero ~1,2 s
per ciclo su una cadenza di 10 s: di nuovo contro il muro. L'asse "tante code"
non è stato risolto dalla 1.0.4, solo quello "tante partizioni".

### Finding 2 — NUOVO: a traffico zero il DB scrive 520.000 righe di metriche

`queue_lag_metrics` (260.000 insert, 261.428 righe, **75 MB**) e
`queue_parked_replica` (260.000 insert, 260.000 righe, **47 MB**) in **30 minuti
di traffico ZERO**: una riga per coda al minuto per tabella, indipendentemente
dal fatto che la coda abbia fatto qualcosa.

Sono il **26% del tempo DB** dello scenario e **122 dei 147 MB** di crescita del
database. Estrapolato: **~244 MB/ora, ~5,7 GB/giorno di sole metriche** con 10k
code ferme.

Questa è oggi la leva di densità più grossa e più economica: gating dell'insert
sulle code che hanno avuto attività nella finestra. Non era visibile a luglio
perché il refresh a 182 ms la copriva.

### Finding 3 — il floor del broker non è più il wake-tick

Luglio sospettava "il wake-tick della hot-list che itera 10k code ogni 5 ms
(2M check/s)" e ipotizzava che una dirty-list avrebbe portato Queen da ~1 core a
~0. Oggi Queen sta a 0,64 core e il profilo per-thread (letto da
`/proc/<pid>/task/*/stat`) mostra il costo **spalmato uniformemente su tutti i 32
worker tokio**, nessun thread caldo isolato: è il lavoro async dei 10.000
long-poll parcheggiati (timer, wakeup, connessioni HTTP), non un loop
patologico. Una dirty-list non è più la leva che era.

## BIG — stessa portata, latenza 10x migliore, su un disco più lento

3.598.023 messaggi spinti, **0 errori**, ~5.900 msg/s.

- **e2e p50 334 → 33-40 ms, p99 610 → 104-134 ms.** È il risultato più vistoso
  del rerun, e **non è attribuibile alla 1.0.4**: è il lavoro sul path di
  consegna fra luglio e oggi (arbitro admission v5, riscrittura di
  `log_pop_list_v1`, hot-list). Va nella colonna "il prodotto è migliorato", non
  in quella "la 1.0.4 ha fatto".
- PG 10,07 → 11,64 core a parità di portata: **peggio del 15%**, e la spiegazione
  è il disco. Il profilo delle wait è cambiato da ClientRead-dominato a
  **WALWrite 56%**: questa run è disk-bound dove quella di luglio era
  client-bound. Per commit: 0,98 → 1,08 mcore·s, +10%, coerente con +38% di
  fdatasync.
- L'economia del "pop magro" di luglio **regge**: `log_pop_list_v1` è l'**84%**
  del costo PG, 3.591.973 chiamate per 3.595.357 messaggi = **1,0 pop per
  messaggio consegnato** (luglio: 1,3). Col traffico sparso su 10k code ogni
  messaggio continua a pagare un pop intero. È ancora *il* finding di prodotto
  per la forma "tanti ma piccoli".

## Cosa portarsi a casa

1. **Il floor a basso carico è crollato**: 0,59 → 0,19 core per 100 code con
   traffico. La storia "10 tenant su una 2c/4GB" ora ha ~10x di margine.
2. **Il presidio di 10k code costa 0,81 core invece di 1,50**, ma per metà è
   ancora broker, e la parte PG è ormai fatta di *scritture di metriche per code
   ferme* (finding 2) più il refresh O(code) (finding 1).
3. **Le due leve prossime, in ordine di rapporto valore/rischio:**
   - gate degli insert `queue_lag_metrics` / `queue_parked_replica` sulle code
     realmente attive nella finestra — 26% del tempo DB idle e 5,9 GB/giorno;
   - refresh incrementale sull'asse CODE (dirty-set per queue_id), che è ciò che
     `PLAN_STATS_REFRESH.md` Tier 3 descrive: oggi non urgente a 10k, obbligatorio
     a 100k.
4. **Lo scheduler claim-row della 1.0.4 non costa niente** (0,017 ms per claim) e
   in compenso rende le cadenze vere: è la premessa perché i due punti sopra si
   possano misurare senza l'aritmetica `interval/repliche`.

## Riproduzione

`tools/` contiene tutto: `setup-queen.sh` (PG18 + docker + profilo di tuning),
`bench-sampler.sh` (1 Hz, stesso contratto CSV di luglio, adattato a PG nativo),
`run.sh` (reset contatori → sampler → goload → attribuzione), `reset.sh`,
`summarize.py`. Loader: `goload -mode tenants` da
`../2026-07-29-vm-campaign/goload/` (build.sh linux).

```sh
bash tools/run.sh small 300  -tenants 10   -queues-per-tenant 10
bash tools/run.sh idle  1800 -tenants 1000 -queues-per-tenant 10 -idle-only
bash tools/run.sh big   600  -tenants 1000 -queues-per-tenant 10
```

**Due difetti del rig trovati e corretti prima di misurare**, nella tradizione
della campagna di luglio: (1) le righe `trust` aggiunte in coda a `pg_hba.conf`
non si applicavano mai perché la regola `scram-sha-256` di default le precede —
risolto con `.pgpass`; (2) `pkill -f bench-sampler.sh` faceva match sulla riga di
comando dell'ssh stesso e uccideva la shell che stava per avviare il sampler,
lasciando goload girare **non monitorato** senza alcun errore visibile — risolto
con un pid-file. La prima run di SMALL è stata buttata per questo.

## Aggiornamento 2026-08-20 pomeriggio — 100k code: CRASH + conferma dell'estrapolazione

La previsione del Finding 1 e' stata **verificata e confermata**: a 100.000 code
`log_refresh_all_stats_v1` costa **1.384 ms per chiamata** (previsti ~1,2 s), 36% del
tempo DB, 14% di una cadenza da 10 s.

Ma la run **ha fatto crashare il broker** — `Exited (139)`, stack overflow su un worker
tokio, sotto saturazione del pool DB (160/160, 10.352 richieste in coda, solo 3.584
consumer su 100.000 parcheggiati). Dettagli, repro e stato pre-crash: **[CRASH-100k.md](CRASH-100k.md)**.

Conseguenza sui numeri: **le risorse della run 100k NON sono utilizzabili** per la
densita' (descrivono un sistema in fallimento). Il tetto pratico di una cella con
`DB_POOL_SIZE=160` sta ben sotto 100k code-con-consumer; a 10k la stessa forma regge
senza un errore.

## Extra 2026-08-20 sera — req/s GREZZI: push-batch 1 (niente batching client)

Domanda: quante RICHIESTE/s regge queen se ogni messaggio e' una push HTTP a se'?
`goload -mode max -push-batch 1 -producers 300`, 120 s, coda da 100 partizioni:

| | valore |
|---|--:|
| **push req/s (1 msg = 1 richiesta HTTP)** | **~29.500 sostenuti** (28,2-30,5k) |
| messaggi totali | 3.537.863 pushed / 3.536.543 popped, **0 errori** |
| PG / broker CPU | 12,2 / 4,4 core (WALWrite 72% delle wait) |
| commit/s | 10.043 |
| chiamate SP push | 604.999 → **5,85 messaggi per chiamata SP** |

Il numero chiave e' l'ultimo: con `push-batch 1` il broker ha comunque servito
29,5k richieste/s con SOLE ~5k chiamate SP/s — la **push fusion cross-request**
(fusion.rs) coalizza le richieste concorrenti nella stessa transazione, e' il
batching fatto server-side. E' il motivo per cui il client non-batchante non
paga 1 commit per messaggio: 29,5k req/s → 10k commit/s totali.

Caveat: il loader (8 core) era al ~65% — questo e' un PAVIMENTO del broker, non
il tetto (la VM broker aveva 15 core liberi). Disco 131 µs, WALWrite-bound: su
disco classe-luglio (95 µs) il numero sale.
