# 2026-08-20 — Gate: throughput e latenza multi-hop (queen 1.0.4)

Prima misura di [Gate](https://github.com/queen-mq/gate) sotto carico. Gate impagina
l'egress attraverso un grafo dichiarato: il lavoro entra da un nodo **entry**, viene
**relayed** hop per hop (ogni relay e' UNA transazione queen che porta ack+push
insieme) e viene consumato da un **terminal**. Ogni hop puo' portare budget.

**Rig**: Gate (`ghcr.io/queen-mq/gate:latest`, build 2026-08-20) accanto al broker
1.0.4 sulla stessa VM 32c/62GB (come in prod: i pod gate stanno accanto ai pod
queen), PG 18.6 nativo. Loader `gateload` (in `gateload/`) da una VM 8c nella stessa
VPC. Raw in `raw/`.

## Il numero che conta: ~310 ms per hop

Confronto pulito r3 vs r4 — **stesso cap, stesso push rate, backlog ZERO in
entrambi**, quindi la latenza misura il percorso e non la profondita' di coda:

| shape | push | admitted | backlog | **e2e p50** | p90 | **p99** |
|---|--:|--:|--:|--:|--:|--:|
| **1 hop** (entry→term), cap 2000/10s | 150/s | 144/s | **0** | **320 ms** | 544 ms | **628 ms** |
| **2 hop** (entry→mid→term), cap 2000/10s | 150/s | 144/s | 10 | **632 ms** | 977 ms | **2.018 ms** |

**Ogni hop costa ~310 ms al p50 e circa raddoppia il p99.** La latenza e' lineare nel
numero di hop, non esponenziale, ed e' esattamente cio' che ci si aspetta da un relay
guidato dal lease: un item aspetta in media mezzo ciclo per hop.

Con budget larghissimo (nessuna pacing pressure) la forma non cambia:

| shape | push | admitted | backlog | e2e p50 | p90 | p99 |
|---|--:|--:|--:|--:|--:|--:|
| 1 hop, cap 500k/10s | 600/s | 539/s | 5.286 | 342 ms | 3.907 ms | 11.963 ms |
| 2 hop, cap 500k/10s | 250/s | 240/s | **0** | 396 ms | 836 ms | 2.548 ms |

Il p90/p99 della riga 1-hop e' peggiore **perche' quella run era leggermente in
overload** (600/s spinti contro 539/s ammessi → 5.286 di backlog). E' la prova, dentro
la stessa tabella, che i percentili alti misurano la coda e non il percorso.

## Throughput: ogni hop costa ~2,4x

Push **illimitato**, cioe' il ceiling di Gate:

| shape | pushed | **admitted** | backlog a fine run |
|---|--:|--:|--:|
| 1 hop | 767.884 (7.679/s) | **893/s** | 678.584 |
| 2 hop | 925.637 (9.256/s) | **367/s** | 888.937 |
| 1 hop, cap 2000/10s (=200/s) | 886.125 | **158/s** | 870.333 |

Tre letture:

1. **Il push accetta 7.700-9.200/s**, un ordine di grandezza sopra quello che la
   pipeline di relay muove. Se si spinge a line rate il backlog si forma subito — che
   e' il comportamento voluto (Gate *trattiene* il lavoro), ma va detto perche' rende
   inutilizzabile qualsiasi latenza misurata in quel regime.
2. **Il secondo hop dimezza abbondantemente la portata** (893 → 367/s, 2,4x). Il
   README di Gate dice "the severe path is short. One hop" per ragioni di correttezza
   (il certificato dell'upstream e' al piu' vecchio di una coda); questi numeri danno
   la stessa raccomandazione per ragioni di costo.
3. **Il pacing tiene**: cap dichiarato 200/s, ammessi 158/s. Sotto il cap, mai sopra,
   che e' il verso giusto per un rate limiter (una finestra rolling non deve mai
   superare il tetto del fornitore).

## Gate e' quasi gratis; il costo e' il broker sotto

Durante le run di ceiling, con ~900 msg/s ammessi e ~8.000/s spinti:

| | CPU | RSS |
|---|--:|--:|
| **container Gate** | **4,2-8,7% di UN core** | **117-235 MB** |
| PostgreSQL | **6,0-8,3 core** | — |
| broker queen | 0,52-0,58 core | — |

**Non si dimensiona Gate: si dimensiona il broker dietro.** Le 767k-925k push per run
da 100 s sono cio' che brucia PG; Gate stesso e' rumore. Chi pianifica capacita' per
un deployment Gate deve guardare il ceiling del broker, non quello di Gate.

## Tre regole di dichiarazione che Gate impone (e ha ragione)

Trovate sbagliando, tutte validate a declare-time con un 422 esplicito invece che
accettate in silenzio:

1. **Un terminal DEVE dichiarare un budget.** `"[budgets] node term declares no budget
   and has no out-edge: it would admit everything straight to a consumer, which is a
   queue with extra steps"`. Non esiste il grafo "senza limite": per misurare il
   ceiling serve un cap altissimo, non l'assenza di cap.
2. **`batch >= cap x lease / period`**, per nodo: `"batch 250 is below the 5000000
   items a lease of 5s allows: the batch would limit, not the budget"`. Un batch
   troppo piccolo renderebbe il batch il limitatore vero e la dichiarazione una
   bugia.
3. La regola 2 vale **anche sui nodi interni**: il primo tentativo di grafo a 2 hop e'
   stato rifiutato sul nodo `mid` (cap 3000/10s con lease 5s → servono 1500, batch era
   1000).

## Riproduzione

```sh
cd gateload && GOWORK=off GOOS=linux GOARCH=amd64 go build -o gateload-linux-amd64 .
# ceiling (push illimitato)
./gateload -gate http://HOST:8788 -hops 1 -cap 500000 -period 10 -lease 1 \
           -pace-batch 50000 -duration 100 -pushers 32 -consumers 8
# rate-matched (l'unico regime in cui la latenza significa qualcosa)
./gateload -gate http://HOST:8788 -hops 2 -cap 2000 -period 10 -lease 5 \
           -pace-batch 2000 -mid-cap 3000 -rate 150 -duration 100
```

`gate.sh` / `gate3.sh` sono i driver delle due passate.

**Avvertenza metodologica, la piu' importante di questo report**: con push illimitato
il p50 misurato e' 13-45 SECONDI. Non e' la latenza di Gate, e' la profondita' del
backlog che Gate sta trattenendo apposta. Qualsiasi numero di latenza va letto insieme
al backlog di fine run, e solo le righe con backlog ~0 descrivono il percorso.

## Aggiornamento sera — trovato e rimosso il collo di bottiglia: il pre-check bogus-ack

L'attribuzione pg_stat_statements della run di ceiling ha mostrato che il **68% del
tempo DB** era UNA query: il pre-check "bogus ack" che il broker eseguiva prima di ogni
`POST /api/v1/transaction` — per OGNI hash ackato, una scansione dell'INTERA storia
`log_txns` della partizione (144 ms a chiamata a 24k righe). Ogni relay di Gate e'
ack+push su quella rotta, quindi ogni relay lo pagava; due hop, due volte.

Il fix (working tree, da rilasciare): il pre-check e' eliminato e il contratto si e'
spostato dentro `log_transaction_wire_v1` — `log_ack_by_hash_v1` calcola GIA'
`unresolvedHashes` (hash che non risolvono ne' nello span ackabile ne' sotto il
cursore) nella sua unica passata; il wire ora RAISEa `QTXN` su quella lista, il
rollback e' atomico per costruzione (plpgsql), e il broker mappa gia' il prefisso
QTXN su `ack_rejected`. I duplicati sotto-cursore (relay ritentati) restano tollerati.
Verificato: `transactionRollback` + 4 test e2e di transazione verdi, 711 test server
verdi, probe ASSENTE da pg_stat_statements.

### Prima / dopo

| | 1.0.4 | ackfix | delta |
|---|--:|--:|--:|
| ceiling 1 hop | 893/s | **1.384/s** | +55% |
| ceiling 2 hop | 367/s | **1.238/s** | **3,4x** |
| penalita' per hop (portata) | 2,4x | **1,12x** | — |
| 1 hop @600/s: p50 / p99 | 342 / 11.963 ms (backlog 5.286) | **174 / 568 ms (backlog 0)** | il relay ora regge 600/s |
| 2 hop @250/s: p50 / p99 | 396 / 2.548 ms | 354 / 2.532 ms | ~= |
| 1 hop paced @150/s: p50 / p99 | 320 / 628 ms | 299 / 595 ms | ~= |
| 2 hop paced @150/s: p50 / p99 | 632 / 2.018 ms | 573 / 2.031 ms | ~= |

Le latenze pacate sono invariati per design: li' domina la cadenza del lease (~180 ms
per hop con lease 1 s, ~275 ms con lease 5 s), non il DB. Quello che e' sparito e' il
collasso sotto carico: a 600/s il p99 e' passato da 12 s a 568 ms perche' il relay non
resta piu' indietro.

**Il residuo**: il costo del wire e' ora la scansione `occ` DENTRO `log_ack_by_hash_v1`
— O(righe log_txns sopravvissute della partizione), pagata una volta per leg. In
steady-state e' limitata dalla finestra txns + purge; sotto ingest estremo su poche
partizioni cresce. Oltre ~1,4k/s per hop la risposta e' rendere indicizzabili gli hash
(cambio di design dello storage, non una patch).

## Il ceiling del BROKER sulla transaction wire (txnload) — ovvero: 1,4k/s e' di Gate, non di queen

Domanda di Alice: "non possiamo chiamare transaction piu' di ~1,5k/s su questa VM?"
Risposta misurata: **no — il broker fa 23-34k item/s in forma relay sulla stessa VM.**
Il loader `txnload/` emula ESATTAMENTE l'hop di Gate (pop batch da A → UNA
transaction con push su B + ack di A) a concorrenza reale.

| configurazione | item relayed/s | txn/s | txn p50 | PG core | collo |
|---|--:|--:|--:|--:|---|
| Gate pipeline (post-fix) | 1.384 | ~25 | — | 2,8 | il relay di Gate: seriale, lease-paced |
| txnload NAIVE (fan-out su 16 partizioni per txn) | 1.654 | 33 | 434 ms | **1,1** | **convoy di lock, macchina al 95% IDLE** |
| **txnload lane-pinned (16 lane disgiunte)** | **23.451 medio, 34.428 picco** | **603** | **22 ms** | 14,5 | CPU PG + scan occ crescente |

Tre lezioni, tutte e tre da documentazione:

1. **Le lane sono l'unita' di parallelismo anche sulla transaction wire.** Una
   transaction che tocca N partizioni prende N row lock in ordine canonico: col
   fan-out su tutte le partizioni ogni transaction serializza contro tutte le
   altre — 64 worker, 1 corsia effettiva, PG a 1 core e throughput in caduta.
   Partizioni disgiunte per worker: 16 lane → 23k item/s. E' la stessa lezione
   della crossbench ("il claim loop seriale per partizione"), applicata alla wire.
2. **Il numero di Gate e' il pipeline di Gate.** Durante il suo ceiling la VM era
   al ~90% idle: il relay di Gate e' un loop seriale per edge, cadenzato dal
   lease. Con la stessa forma a 16 lane il broker fa 17x tanto. Se Gate vuole
   piu' portata, la leva e' la concorrenza dei suoi relay (per lane/partizione),
   non il broker.
3. **L'accumulo di log_txns e' il decadimento residuo**: 34k→13k item/s in 60 s
   perche' 1,4M push in un minuto su 32 partizioni accumulano ~44k righe txns a
   partizione che la scan `occ` dell'ack leg ripaga a ogni chiamata, e la purge
   non puo' toccarle prima della finestra (>=15 min). A regime la finestra limita
   l'accumulo; sotto burst estremi il costo cresce linearmente fino al pareggio
   con la purge. Il fix strutturale resta lo storage hash indicizzabile.

Riconciliazione con "queen regge 1M msg/s": quel numero e' push/pop batchati su
centinaia di lane. La wire qui fa 34k item/s con SOLE 16 lane a batch 50 — scala
con le lane esattamente come il resto del prodotto.
