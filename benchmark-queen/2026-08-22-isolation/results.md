# Hot-entity isolation — Queen 1.1.0 (2026-08-22)

**Una entita' rumorosa su 1.000. La domanda non e' quanto soffre lei, ma quanto
fanno soffrire i vicini.** Fino a **200x** — una sola entita' che si prende un
sesto del flusso — i vicini restano **esattamente sul basale**: p99 120,2 ms
contro 120,2 ms. Il dolore resta tutto sull'entita' calda.

> **Revisione 2 (07:20Z).** La prima passata dava l'isolamento rotto fra 50x e
> 200x. Era **falso**: era il generatore di carico, non Queen. Il produttore
> ordina per chiave su 64 pusher seriali (`prop % 64`), quindi l'entita' calda
> e le ~15 fredde con lo stesso resto finivano nella **stessa coda FIFO del
> client**, prima del broker. Rifatta la cella con un pusher per proprieta'
> (`-push-shards 1000`): `shed` da 293 a **0** e p99 freddo da 1.617 a **120,2 ms**.
> La sezione §2 qui sotto e' riscritta; i numeri vecchi restano in
> `raw/skew-f200/` come cella INVALIDA.

Rig, immagine e trappole: [RIG.md](RIG.md). Definizione delle celle:
[crossbench/SPEC.md §10](../crossbench/SPEC.md). Grezzi in [raw/](raw/).

## La forma

Ancora 2k/1000 (`-rate 2000 -properties 1000 -queen-pop-partitions 40`,
180 s + ramp 10 + drain 90), stack Queen **1.1.0** rifatto da zero per ogni cella.
**Una** entita' calda (`-hot-props 1`), che riceve `F` volte la quota di una
fredda. Il rate offerto NON cambia fra le celle: la skew ridistribuisce gli
stessi 2.000 ev/s, altrimenti si misurerebbe il sovraccarico, non l'isolamento.

Campagna completa in **19 minuti** (06:43:07Z → 07:02:06Z).

## Il risultato

| F | quota calda (cfg → consegnata) | shed | correttezza | **FREDDI** p50 / p95 / **p99** | CALDA p50 / p95 / p99 | core |
|---:|---|---:|:--|---|---|---:|
| 1 (basale) | — | 0 | PASS | 77,9 / 101,1 / **120,2** ms | — | 10,78 |
| 10 | 1,0% → 1,0% | 0 | PASS | 77,9 / 101,1 / **110,2** ms | 85,0 / 155,9 / 202,1 ms | 10,82 |
| 50 | 4,8% → 4,7% | 0 | FAIL (warm-up, §4) | 77,9 / 101,1 / **120,2** ms | 131,1 / 220,4 / 262,1 ms | 10,54 |
| **200** (1000 shard) | 16,7% → **16,6%** | **0** | PASS | 77,9 / 92,7 / **120,2** ms | 170,0 / 285,9 / 370,7 ms | — |
| ~~200~~ (64 shard) | 16,7% → 16,1% | 293 | INVALIDA | ~~1.617,1~~ | ~~6.468,5~~ | 10,74 |
| ~~1000~~ (entrambe) | 50,0% → 30-34% | 85-98k | INVALIDA | — | — | — |

Latenze e2e CO-corrette dall'istante *schedulato* del produttore, cohort caldo e
freddo misurati **dentro la stessa run** (SPEC §10.3: la variabilita' run-to-run
di questo banco copre p50 340→3.234 ms, un delta fra run sarebbe rumore).
Core = media sulla finestra attiva, broker + PG. Loader fra **7,5% e 10,0%** di
CPU in ogni cella: tutte ammissibili sotto il cancello del 70% (SPEC §5.1).

## 1. L'isolamento tiene fino a 50x, e tiene *completamente*

A 10x e 50x il p99 dei vicini e' **110,2 e 120,2 ms** contro i **120,2 ms** del
basale uniforme: identico entro il rumore. Nel frattempo l'entita' calda paga da
sola — 202 ms a 10x, 262 ms a 50x. A 50x una singola entita' assorbe il 4,7%
dell'intero carico di consegne e i 999 vicini **non se ne accorgono**.

Questo e' il risultato che il partizionamento per entita' deve produrre, ed e'
misurato, non asserito.

## 2. L'isolamento tiene fino a 200x — e il tetto e' il banco, non Queen

Nelle quattro celle valide il p99 dei vicini non si muove:

| F | quota consegnata alla calda | p99 freddi | vs basale |
|---:|---:|---:|---:|
| 1 | — | 120,2 ms | — |
| 10 | 1,0% | 110,2 ms | 0,92x |
| 50 | 4,7% | 120,2 ms | 1,00x |
| **200** | **16,6%** | **120,2 ms** | **1,00x** |

Una sola entita' su mille si prende **un sesto di tutte le consegne** e i 999
vicini non se ne accorgono: stesso p99 del carico uniforme. La calda paga da
sola e in modo ordinato (170 ms p50, 370 ms p99 a 200x).

### Perche' 1000x non e' misurabile con questo banco

A 1000x l'entita' calda dovrebbe ricevere 500,3 ev/s **su una sola chiave**. Il
contratto di ingresso (SPEC §1) e' publish singoli, e l'ordine per chiave impone
un pusher seriale per chiave: a 3,36 ms di RTT p50 un pusher seriale fa ~300/s.
Con 64 shard `shed`=84.649, con 1000 shard `shed`=97.530 — **il banco non riesce
a offrire il carico**, quindi la cella non misura Queen.

Non e' un limite di sharding, e' aritmetica: 500 round-trip ordinati al secondo
richiedono 2 ms di RTT. Per superare 200x servirebbe pipelining dell'ingresso
(invii sovrapposti con completamento ordinato), che e' un cambio del contratto
di §1 e va deciso, non improvvisato.

## 3. Il tetto di ingresso di UNA chiave ordinata: ~300 ev/s

A 1000x la calda ha ricevuto il 50,0% del flusso offerto ma ne ha consegnato il
29,9-34,0%: l'ammanco e' `shed` **lato produttore**, non backlog lato broker.

Il numero utile che ne esce e' del banco, non del broker: **un publisher ordinato
per chiave sostiene ~300 ev/s su quella chiave** a questo RTT. E' un vincolo che
vale per qualunque produttore che voglia ordine per chiave senza pipelining, ed e'
un dato da mettere sul tavolo quando qualcuno progetta un'entita' molto calda.

Conseguenza pratica: `hot_saturated` calcolato sul *cap* di batch e' inutile e
non va pubblicato come tale — l'indicatore onesto di saturazione e' l'ammanco fra
quota configurata e quota consegnata, che e' una misura e non un modello.

## 4. La cella F=50 e' FAIL, e non c'entra la skew

`gaps 30, order violations 0, dups 0`. Localizzati: sono **5 proprieta' (1 calda,
4 fredde) che hanno perso il messaggio `seq 0`**, cioe' il messaggio di *warm-up*.
Ogni messaggio perso all'hop 1 blocca 6 stream di flusso A: 5 × 6 = 30.

Il warm-up gira **prima** dei produttori ed e' uniforme in tutte e cinque le
celle — quando e' successo, la skew non era nemmeno attiva. Il log lo dice:

```
skew-f1      warmup: 1000 properties x 2 flows in 2.169s,   drained=true  (12000/12000)
skew-f10     warmup: ...                        in 2.169s,   drained=true  (12000/12000)
skew-f50     warmup: ...                        in 2m2.263s, drained=FALSE (11970/12000)
skew-f200    warmup: ...                        in 2.491s,   drained=true  (12000/12000)
skew-f1000   warmup: ...                        in 2.248s,   drained=true  (12000/12000)
```

### Cosa faceva il broker: 100 secondi di silenzio a consumer parcheggiati

Dalla telemetria del broker (`raw/broker-rates.log`), finestra 06:50:22–06:52:02:

```
push_s=0  pop_s=0  ack_s=0  pop_empty_pct=100.0  parked=96  ring_depth=0  pool_waiting=0
```

**Tutti e 96 i consumer parcheggiati, ogni pop vuoto, l'anello a zero** — mentre
in Postgres c'erano 5 messaggi non consegnati. Il broker era convinto che non ci
fosse niente da servire. Alle 06:52:12 riparte da solo e la finestra tarata
riprende a 12.000 consegne/s senza altri interventi.

La ripresa arriva ~120 s dopo il push, e la configurazione al boot dice
`hotlist_reseed_window_ms=120000`. **Lettura (inferenza, non misura diretta):**
la registrazione di quelle 5 lane nella hot-list si e' persa alla creazione, i
consumer parcheggiati non sono mai stati svegliati, e il reseed a finestra le ha
ripescate al suo giro. La rete di sicurezza ha funzionato; la finestra di
esposizione e' fino a **due minuti** sul primo messaggio di una lane nuova.

Da confrontare con i follow-up gia' noti in `PLAN_HOTLIST_FOLLOWUP.md` prima di
aprirlo come difetto nuovo. In produzione si vedrebbe cosi': una partizione
appena creata non consegna il suo primo messaggio per un massimo di 120 s, poi si
sblocca da sola — cioe' il tipo di sintomo che si archivia come "rete lenta".

## 5. Dentro il broker non si muove NIENTE

Telemetria del broker mediata sulla finestra di carico, da F=1 a F=1000:

| | F=1 | F=10 | F=50 | F=200 | F=1000 |
|---|---:|---:|---:|---:|---:|
| `ready_age` p50 / p95 | 2 / 8 ms | 2 / 8 ms | 2 / 8 ms | 2 / 9 ms | 2 / 6 ms |
| `pop_empty_pct` | 0,0 | 0,0 | 0,0 | 0,0 | 0,0 |
| `pool_waiting` | 0 | 0 | 0 | 0 | 0 |
| `cycle_ms` | 1,5 | 1,2 | 1,3 | 1,4 | 1,3 |
| popRTT consumer p50 / p95 | 8,2 / 21,2 | — | — | 8,9 / 21,2 | 8,9 / 21,2 |
| barrier terminali p50 | 32,8 ms | — | — | 32,8 ms | 32,8 ms |

**Piatta ovunque, anche nelle celle invalide.** E' questo che ha smascherato il
difetto del banco: l'eta' di ingresso all'hop 1 saliva a 1.923 ms mentre ogni
numero *dentro* il broker restava identico. Il tempo si spendeva prima del broker.

## 6. La skew non costa CPU

10,78 / 10,82 / 10,54 / 10,74 / 9,58 core al variare di F. **Concentrare meta' del
carico su una sola entita' non costa un core in piu'**: costa coda. La cella a
1000x costa perfino *meno* (9,58) perche' 999 lane fredde ricevono poco traffico
e il lavoro si concentra dove il batching rende. PG resta ~80% del costo totale
in ogni cella (7,78–8,64 core su 9,58–10,82).

## Limiti di questa campagna

- **Una sola entita' calda.** Un *tenant* rumoroso e' molte entita' insieme:
  e' la campagna shared-cell, non questa.
- **Una replica per cella.** Con la varianza nota di questo banco, i delta
  cross-run (§2) vanno riletti con almeno 3 ripetizioni prima di finire su una
  pagina pubblica. I confronti caldo/freddo *dentro* la cella non hanno questo
  problema ed e' per questo che sono la misura primaria.
- **Nessun confronto cross-broker.** Kafka, RabbitMQ e pgmq girano lo stesso
  identico codice di carico e non sono ancora stati passati su queste celle: e'
  il prossimo passo, ed e' la meta' che rende la pagina un confronto.
- **Il p99 freddo non e' scomposto per entita'.** Gli stream log contengono
  `prop,seq` ma non timestamp, quindi non si puo' dire offline *quali* vicini
  hanno preso 1,6 s. Per rispondere serve un istogramma per entita' nel recorder.
- Il ginocchio fra 50x e 200x non e' campionato (§2).
