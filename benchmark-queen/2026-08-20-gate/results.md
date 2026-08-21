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

---

# Round 2 (sera) — Gate 0.1.5 "relay parallelo" contro broker 1.0.5

**Rig identico al Round 1** (Gate accanto a broker e PG 18.6 sulla stessa VM 32c/62GB,
gateload dalla VM 8c), DB azzerati a inizio campagna. Gate 0.1.5 = `eefdce6` "relay:
one runner per source admitted partition": il relay non e' piu' UN loop seriale per
destinazione ma un runner per OGNI partizione admitted della sorgente. Broker =
`ghcr.io/queen-mq/queen:1.0.5` (l'ackfix rilasciato). Nota di build: la CI docker di
gate ha compilato e FALLITO il push (`permission_denied: write_package`: il package
ghcr e' nato da un push manuale e non e' collegato al repo — da sistemare nei package
settings); l'immagine 0.1.5 e' stata buildata e pushata da locale, digest `42c1f391`.

## La headline: il relay e' raddoppiato, ma l'e2e non lo mostra

Stessi comandi del Round 1:

| e2e [final] | 1.0.4 seriale | ackfix seriale | 0.1.5 parallelo |
|---|--:|--:|--:|
| 1 hop ceiling | 893/s | 1.384/s | 1.226/s |
| 2 hop ceiling | 367/s | 1.238/s | 1.203/s |

Piatto. Ma il numero [final] di gateload misura il SUO lato consumer, non Gate. La
novita' metodologica di questo round: l'endpoint depth (1.0.4+) decompone il backlog
residuo per stadio, e le rate per stadio raccontano un'altra storia (run 1-hop):

| stadio | rate |
|---|--:|
| admission (entry.push → admitted) | ~6.100/s |
| **relay (cio' che 0.1.5 ha cambiato)** | **~2.775/s — 2x il seriale** |
| gli 8 consumer di gateload | ~1.226/s ← era QUESTO il tetto dell'e2e |

Sul 2-hop le DUE leg girano in parallelo a ~2.65k/s CIASCUNA con la coda mid quasi
vuota: la penalita' per hop sulla portata del relay (2,4x in 1.0.4, 1,12x post-ackfix)
e' sostanzialmente sparita.

E no, non basta "aggiungere consumer": con 32 consumer il terminal si svuota ma il
relay DIMEZZA (1.231/s) e PG triplica (8,9 core) — la pressione di pop contende sulle
stesse partizioni del relay. Su questo rig l'e2e single-target si ferma a ~1,2k/s
comunque si configuri il loader; e' il sistema a essere contention-bound.

## Perche' si ferma li': il contatore del nodo E' una partizione sola

Dai depth: `term.push` ha UNA partizione (`default`); `term.admitted` ne ha 16. E' il
design del budget: il conteggio esatto vuole una corsia sola, quindi ogni item che
entra in un nodo passa da quella partizione — e aggiornarne la riga e' un lock. Sedici
runner convergono su un lock solo: wait profile `tuple` al 96-100% in OGNI run di
ceiling. Il commit stesso lo dichiara ("what bounds it is the destination's single
push partition, which IS the node's counter").

Scaling con le partizioni della sorgente (stato pulito): parts=1 → relay 764/s (un
runner = il controllo); parts=16 → 2.775/s (3,6x, meglio del "flat dopo 4" misurato
dal commit su laptop). Oltre, il collo e' la corsia di destinazione, non i runner.

txnload (Round 1) fa 23-34k item/s con la STESSA forma perche' le sue 16 lane sono
disgiunte ANCHE a destinazione: nessuna riga condivisa. Gate paga il conteggio esatto
con un imbuto per nodo. Il broker non c'entra.

## La prova: target multipli scalano

Se il collo e' il contatore per-NODO, N nodi = N contatori = N corsie. Misurato con
10 grafi 1-hop indipendenti in parallelo (reset completo, 120 s):

| | single-target | 10 target (6 pusher/grafo) | 10 target (16 pusher/grafo) |
|---|--:|--:|--:|
| e2e aggregato | 1.226/s | **4.004/s** | **4.636/s** |
| stato a fine run | backlog 400-600k | **TUTTO drenato** (entry.push=0, admitted~0) | entry.push=0 ovunque; residui piccoli (admitted ~51k tot) |
| p50 / p99 e2e | (backlog-driven) | 393-531 / 1.308-5.146 ms | 647-755 / 1.675-7.476 ms |
| PG core (mean/peak) | 2,4 / 3,1 | 13,8 / 19,2 | 9,8 / 18,5 |
| container Gate | 4-8% di un core | 26% di un core | 36% di un core |

Il run a 6 pusher/grafo NON ha saturato Gate: ogni coda e' finita vuota, cioe' il
limite era il push del loader (4,2k/s), non la pipeline. Wait profile diversificato
(tuple 75%, transactionid 13%, WALWrite 10%): commit paralleli veri, appare la WAL.

Il rerun a 16 pusher/grafo (160 goroutine di push) ha alzato il feed solo del 28%
(4,2k → 5,4k/s: i pusher sono latency-bound contro il broker sotto carico) e Gate ha
continuato a mangiarsi tutto: entry.push a ZERO su tutti e dieci i grafi, relay
aggregato ~4,9k/s con residui admitted di ~1 s di lavoro, consumer a 4.636/s. PG a 9,8
core medi su 32: il tetto multi-target di Gate su questa VM NON e' stato raggiunto —
per trovarlo serve un loader piu' grosso (o un secondo), non questa VM da 8 core.
Quello che e' dimostrato: 1.226 → 4.636/s e2e (3,8x) aggiungendo target, con margine.

**Lettura di prodotto**: il ~2,8k/s single-target e' il tetto PER PROVIDER, e un rate
limiter esiste per pacare i provider ben sotto quel numero. Il numero commerciale e'
l'aggregato su molti target contati indipendentemente — e scala.

## Il costo delle forme cappate (da portare al team Gate)

La forma cap 200/s brucia **6,5 core PG per ammettere 172-181/s**. Attribuzione
pg_stat_statements su run dedicata:

- `log_transaction_wire_v1`: 4.869 chiamate, **media 1.639 ms** — quasi tutto attesa
  su tuple lock (convoy dei mini-batch paced dei 16 runner), non CPU;
- `log_streams_cycle_v1`: 3.453 chiamate × 94 ms = **~3,2 core** — la macchina a stati
  di Gate cavalca lo streams engine del broker anche quando ammette 172/s;
- `log_txns_purge_step_v1`: ~1 core a ripulire i 5k/s di push dietro il cap.

## Latenze paced: PEGGIORATE, ma il verdetto e' sporco

| paced | ackfix p50/p99 | 0.1.5 p50/p99 (stato sporco) |
|---|--:|--:|
| 1 hop @150/s | 299 / 595 ms | 561 / 1.226 ms |
| 2 hop @150/s | 573 / 2.031 ms | 1.104 / 2.985 ms |
| 1 hop @600/s | 174 / 568 ms | 388 / 2.300 ms (a 493/s: il loader non ha retto 600) |

~2x peggio, MA misurate su un DB che portava ~4M righe di detriti dei run di ceiling
(lo stesso accumulo ha fatto collassare il primo tentativo di sweep: PG a 12,8 core di
tuple wait e push rate in caduta run dopo run — da qui i reset per-run del Round 2).
I run paced-150 a stato pulito non sono stati completati (sweep interrotto): il
"2x paced" resta NON verificato. Indizio contro la regressione: nei 10 grafi a stato
pulito il p50 a ~400/s per grafo era 393-531 ms con backlog ~0 sotto 14 core di
carico aggregato. Da rimisurare pulito prima di parlarne come regressione.

## Cosa direbbe questo round al team Gate

1. **Shardare il contatore** (N partizioni su `.push` con budget a fette cap/N, o
   contatori sharded sommati al roll della finestra): il relay diventa pinned
   end-to-end = la forma txnload = ~20k+/s su questa VM anche single-target.
2. In alternativa: **contare con l'aritmetica dei watermark** (la depth route da' gia'
   le somme per partizione): ammessi-nella-finestra = delta dei watermark, e gli item
   non devono piu' convergere fisicamente su una corsia.
3. Guardare `log_streams_cycle_v1` nelle forme cappate: 3,2 core di stato per 172/s
   ammessi e' il primo costo fisso che un cliente vede.

Driver: `gatec.sh` (ceiling cons32), `gatef.sh` (sweep a stato pulito, interrotto),
`gatem.sh` (multi-target). Raw in `out/`.
