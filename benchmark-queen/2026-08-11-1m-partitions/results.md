# QueenMQ — 1.000.000 di partizioni ordinate (2026-08-11)

**Un milione di lane FIFO in un solo PostgreSQL, create durante il run a mille al secondo mentre
il broker serve il carico. A 200k msg/s con lease e ack espliciti e retention attiva: p50 27,5 ms
e p99 115 ms a spazio completo, zero errori di push, pop o ack su 722 milioni di messaggi, e un
plateau del database misurato per un'ora. Il milione di partizioni pesa 315 MB di righe e 5 GB di
RSS del broker.**

> Questo referto sostituisce una stesura precedente scritta a run in corso. Quella conteneva due
> errori materiali, corretti qui e segnalati esplicitamente nella sezione
> [Correzioni alla stesura di metà giornata](#correzioni-alla-stesura-di-meta-giornata), perche'
> uno dei due capovolgeva la diagnosi del difetto principale.

## Rig

| ruolo | host | ferro |
|---|---|---|
| broker + PG | `queen-01` 68.183.72.48 (10.114.0.2) | 32 vCPU Xeon Gold 6548N, 62 GiB, 387 GB ext4, `fdatasync` ~70 µs |
| loader | `queen-load-01` 164.90.219.77 (10.114.0.4) | 16 vCPU / 31 GiB |

Un solo loader per scelta, non per risparmio: ogni processo `goload` ruota la propria finestra
attiva in modo indipendente, quindi tre loader darebbero tre finestre sovrapposte e l'insieme
attivo non sarebbe piu' mille partizioni al secondo ma un numero che non controlliamo.

**Broker `queen` 1.0.0** (immagine costruita dagli stessi sorgenti prima del bump di versione,
quindi si autodichiara `1.0.0-beta.4`; il commit 1.0.0 non tocca `server/src`). PostgreSQL 18.4
co-locato.

## La forma del carico

Uno spazio di **1.000.000 di partizioni** su una coda sola (`partbench`), **create dal primo push
che le nomina**, non preallocate. Il loader tiene **1.000 partizioni attive al secondo** e ruota
la finestra, quindi copre lo spazio in 1.000 secondi. E' la forma "un milione di entita' di cui
poche attive per volta": a 200k msg/s ogni lane attiva vede 200 messaggi al secondo e ogni lane
dello spazio ne vede 0,2.

Il supporto e' stato aggiunto a `goload` per questa campagna (`-active-partitions`,
`-active-policy`); il default resta il round robin storico su tutto lo spazio.

256B di payload, push batch 100, pop batch 1000, `max-inflight` 20.000, open loop, lease con ack
espliciti asincroni (256 in volo), retention completed a 300 s e pending a 3600 s.

## I quattro bracci

Una variabile per passo. Le finestre di dedup sono **60 secondi** dove attiva.

| braccio | tasso | `RETENTION_PARALLELISM` | dedup | esito |
|---|--:|--:|---|---|
| `raw-baseline-p4` | 100k/s | 4 | 60 s, cache 6 GB | retention al **69%** del fabbisogno, ciclo cresciuto fino a 402 s |
| `raw-p16-dedup6g` | 200k/s | 16 | 60 s, cache 6 GB | **collasso** a t≈20 min, cache satura |
| `raw-p16-dedup16g` | 200k/s | 16 | 60 s, cache 16 GB | **fermato**: cache in crescita lineare, stessa traiettoria |
| **`raw-final-dedupoff`** | **200k/s** | **16** | **spenta** | **PASS**, plateau misurato |

## Il risultato: 200k msg/s su un milione di partizioni

`raw-final-dedupoff`, 11:05:07 → 12:05:30 UTC, riga `[final]`:

```
offered=722.270.400  achieved=722.265.600  shed=0  pushErr=0
popped=722.263.300   popErr=0
acked=722.261.300    ackErr=0   ackAvg=25,54 ms
lag finale 2.300     overall p50=30,34  p99=168,96  p999=292,86 ms
```

**722 milioni di messaggi, zero errori di ogni tipo, zero shed.**

### La latenza si legge in due fasi

Sono qualitativamente diverse e mediarle insieme nasconde il risultato migliore.

| fase | report da 30 s | p50 medio | p99 medio | p99 peggiore | p99.9 medio |
|---|--:|--:|--:|--:|--:|
| mentre crea il milione (1.000/s) | 34 | 40,5 ms | 214,1 ms | 346,1 ms | 339,8 ms |
| **a spazio completo (1M in piedi)** | **86** | **27,5 ms** | **115,3 ms** | **209,9 ms** | **189,9 ms** |

Quasi tutta la coda del run sta nella fase di creazione: nascere costa a una partizione una riga
e un lock, e farlo mille volte al secondo sotto 200k msg/s di traffico e' il momento piu' caro
della vita di questo sistema. A spazio fermo il p99 dimezza.

### Il plateau, misurato

Dalla copertura completa (11:22) alla fine, 43 minuti a un milione di partizioni in piedi:

```
          11:56     11:59     12:01     12:04
segments  3085 MB   3085 MB   3085 MB   3085 MB     fermo
txns      5301 MB   5302 MB   5302 MB   5302 MB     +1 MB in nove minuti
DB        9,37 GB   9,40 GB   9,43 GB   9,47 GB     +0,011 GB/min
```

E la causa fisica del plateau, dai cicli di retention: **4.002 righe/s di media sugli ultimi otto
cicli**, contro un fabbisogno di 4.000 (2.000 segmenti + 2.000 righe di `log_txns` al secondo, a
200k msg/s con push batch 100). Cancellazione uguale a inserimento, quindi tabelle ferme.

### Quanto pesa un milione di partizioni

| oggetto | dimensione |
|---|--:|
| `log_partitions` (1M righe) | **315 MB** |
| `log_segments` (finestra retention) | 3,01 GB |
| `log_txns` (sidecar, pavimento 900 s) | 5,18 GB |
| **database totale** | **9,47 GB** |
| **RSS del broker** | **5,03 GB** |

Le lane costano 315 MB di righe; il resto scala col **tasso**, non con la cardinalita'. I 5 GB di
RSS del broker sono la cifra di sizing che conta: **circa 5 KB di stato per partizione**, e con la
dedup spenta non cresce oltre.

### Risorse

Sampler a 1 Hz sull'host, medie sul run:

| risorsa | valore |
|---|---|
| CPU PostgreSQL | 10,15 core di 32 |
| CPU broker | 3,84 core |
| RSS broker | 5,03 GB (picco = finale, nessuna deriva) |
| backend attivi | 28 |
| commit/s | ~16.500 |
| `fsync` | 247 µs |
| pool | mai sopra 208/300 |
| restart / incidenti | **0 / 0** |

L'anello della hotlist e' rimasto sotto le poche migliaia di voci per tutto il run: contiene solo
le partizioni **che hanno dati**, quindi il costo del serve path segue il lavoro e non lo spazio.
E' la ragione per cui push e pop sono indifferenti al conteggio delle partizioni, ed e' la
proprieta' che questa campagna esiste per dimostrare.

## I tre difetti trovati, tutti della stessa famiglia

Il costo di tre strutture segue lo **spazio delle partizioni** invece del **lavoro nella
finestra**. A 200 partizioni non si vede, a un milione decide tutto.

### 1. La dedup trattiene ogni hash, non quelli in finestra

Il difetto principale, e quello che ha ucciso due bracci.

Misurato su `raw-p16-dedup16g`, **dopo** che lo spazio era completo (quindi nessuna struttura
nuova da allocare): la cache cresce di **235 MB al minuto, perfettamente lineare**. A 200k msg/s
sono 12 milioni di messaggi al minuto, cioe' **19,6 byte per messaggio**: la dimensione di un
hash. La cache sta trattenendo ogni hash mai pushato.

Il modello di sizing documentato in `server/src/config.rs:269-271` dice:

```
needed_mb ≈ 16 × msg_rate × dedup_window_seconds / 1e6
```

Con finestra 60 s a 200k msg/s: **192 MB**. Le abbiamo dato 6.144 e poi 16.384 MB — **32 e 85
volte** il fabbisogno dichiarato — e li ha riempiti entrambi. Non e' sizing insufficiente: e' una
violazione del modello di due ordini di grandezza.

**La causa**: il recupero e' a granularita' di blocco, e un blocco si libera intero quando il suo
watermark e' tutto fuori finestra. Con un milione di partizioni ogni lane vede 0,2 msg/s: un
blocco da 4096 hash impiega **quasi sei ore** a riempirsi, quindi non si sigilla mai, quindi non
scade mai. Gli hot buffer di ~940.000 partizioni inattive restano allocati a tenere hash che non
possono piu' essere duplicati di niente.

**La forma del fix**: scadenza per partizione. Con mille partizioni attive al secondo e una
finestra di 60 s, in qualunque istante solo ~60.000 partizioni hanno qualcosa di probabile; le
altre non hanno motivo di tenere una entry.

**Il modo di fallire, quando la cache si satura** (`raw-p16-dedup6g`, t≈20 min): le partizioni
soppresse pagano una probe SQL a finestra piena — che resta **autorevole, quindi la correttezza
tiene, nessun duplicato viene ammesso** — ma il costo si sposta su PostgreSQL, il pool satura e da
li' e' un ginocchio: 2.673 msg/s consegnati contro 200k offerti, p50 a 19 secondi.

### 2. La work list della retention e' O(partizioni)

`WORK_LIST_SQL` restituisce una riga per partizione senza filtro di eleggibilita', e la fase 2
(purga di `log_txns`) chiama la stored procedure su **ognuna**, incondizionatamente. A un milione
di partizioni sono due milioni di chiamate per ciclo, pagate anche dalle centinaia di migliaia che
non hanno nulla da cancellare.

Misurato a `RETENTION_PARALLELISM=4` (`raw-baseline-p4`), durata del ciclo contro il conteggio:

| partizioni | ciclo | tasso |
|--:|--:|--:|
| ~400k | 108 s | 378 seg/s |
| ~600k | 149 s | 726 seg/s |
| ~800k | 204 s | 731 seg/s |
| ~1M | 296 s | 689 seg/s |
| ~1M | **402 s** | in perdita |

Fabbisogno a 100k msg/s: 1.000 seg/s. La retention seriale a quattro worker si ferma al **69%**, e
il database cresce senza plateau.

**Il tampone che funziona oggi**: `RETENTION_PARALLELISM=16` porta il ciclo a ~195 s a un milione
di partizioni e la cancellazione a 4.002 righe/s, che a 200k msg/s e' esattamente il fabbisogno.
E' un fattore costante contro un termine lineare: a 1M basta, a 4M servirebbe N=64.

**La forma del fix**: l'eleggibilita' nella work list, cosi' il costo del ciclo segue il lavoro e
smette di dipendere dal conteggio.

### 3. Il refresh delle stats e' O(partizioni)

`stats: refresh elapsed_ms=6815` con 825.706 partizioni. Non ha fatto danno in questo run, ma e'
la terza struttura della stessa famiglia e va nella stessa lista.

## Correzioni alla stesura di meta' giornata

La versione precedente di questo file, scritta con il run ancora in corso, conteneva due errori
che cambiavano le conclusioni.

**La finestra di dedup non era 3600 s ma 60.** La configura `goload` al t=0 con `-dedup-window 60`
(la stessa disciplina del soak: nessun flip a meta' run). Sulla finestra sbagliata il calcolo
dava "servivano 11,5 GB, gliene abbiamo dati 6, quindi il degrado e' corretto". Con la finestra
vera ne servivano 192 MB e la cache ne ha riempiti 16.384: la conclusione si capovolge, da regola
di sizing rispettata a difetto di due ordini di grandezza.

**Il braccio `raw-p16-dedup16g` non e' un PASS.** Era marcato "PASS, ancora in corso"; e' stato
fermato perche' la cache cresceva di 235 MB/min **a spazio gia' completo**, sulla stessa
traiettoria del braccio che era collassato prima. Il PASS appartiene al quarto braccio, che nella
stesura precedente non esisteva ancora.

## Cosa questo run stabilisce

- **Un milione di lane ordinate in una coda sola su un PostgreSQL**, create a mille al secondo
  sotto carico, senza preallocazione.
- **200k msg/s con lease e ack espliciti** e retention attiva, 722 milioni di messaggi, zero
  errori.
- **Il serve path e' indifferente alla cardinalita'**: p50 27,5 ms a un milione di partizioni, con
  broker e PostgreSQL sotto la meta' della macchina.
- **Il costo per partizione**: 315 byte di riga e ~5 KB di RSS del broker.

## Cosa non stabilisce

- **Non e' un run da 24 ore.** Sessanta minuti in tutto, di cui 43 a spazio completo. Il plateau
  e' misurato su quella finestra: tabelle ferme e cancellazione pari all'inserimento. Il soak da
  24 ore e' un'altra misura, su 200 partizioni e con la dedup accesa, e le due non si sommano.
- **La dedup era spenta.** E' la scelta che ha reso il run possibile e va dichiarata. Nel campo la
  dedup generica non e' un default (Kafka e Pulsar hanno idempotenza del producer, SQS FIFO ha una
  finestra fissa di cinque minuti), quindi il confronto esterno regge; il confronto con il **nostro
  stesso** soak di ieri no, perche' quello ce l'aveva.
- **Un solo tasso e una sola forma.** 200k msg/s, mille partizioni attive al secondo, policy
  `rotate` (finestra contigua, caso migliore per la localita' dell'indice). La policy `scatter`
  esiste nel loader e non e' stata provata.
- **Payload comprimibile.** Corpo a carattere ripetuto, come tutta questa famiglia di misure.
- **Niente alta disponibilita', niente fallimenti, niente proxy.**

## Artefatti

| file | contenuto |
|---|---|
| `raw-*/g.out` | stdout del loader: configurazione, report da 30 s, riga `[final]` dove il run e' chiuso |
| `raw-*/bench.csv` | sampler 1 Hz: CPU e memoria di broker e PostgreSQL, commit, WAL, dimensione del DB, wait event |
| `raw-*/partitions.csv` | conteggio partizioni e byte per tabella, ogni 60 s |
| `raw-*/broker.log` | boot, blocchi `rates` e `sizes`, cicli di retention, refresh delle stats |
| `raw-final-dedupoff/metrics.csv` | contatori cumulativi del broker ogni 10 s |
