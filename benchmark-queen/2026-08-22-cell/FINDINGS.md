# Queen Cloud — numeri finali della giornata (2026-08-22)

Due banchi: **VM** (16 vCPU DigitalOcean, cella completa 1 PG + 2 broker +
HAProxy + proxy) e **Mac nativo** (PG Homebrew + broker + proxy, 4 thread tokio).
La VM da' i numeri assoluti, il Mac le *forme* — e le due cose concordano dove
si sovrappongono.

## 1. Cosa costa una cella

| asse | effetto |
|---|---|
| **tasso di messaggi** | **e' il tetto.** Ogni collasso misurato oggi e' un collasso di rate |
| cardinalita' (partizioni) | **gratis** — da 10.000 a 1.000.000 nessun effetto |
| distribuzione (uniforme / concentrata / rotante) | nessuno oltre il rumore |
| **ampiezza della pop** (`pop-partitions`) | **leva enorme, lato client** |

### Cardinalita' gratis — misurato pulito

`pop-partitions` **fissato a 100** su tutti i rung, cosi' l'unica variabile e' la
cardinalita':

| partizioni | msg/s | p99 |
|---:|---:|---:|
| 10.000 | 2.500 | 107 ms |
| 100.000 | 2.500 | 148 ms |
| 1.000.000 | 2.500 | 123 ms |
| 10.000 | 5.000 | 41.157 ms |
| 1.000.000 | 5.000 | 17.170 ms |

A 2.500 msg/s un milione di partizioni si comporta come diecimila. A 5.000
crollano entrambe. **La cardinalita' non sposta il tetto di rate.**

### La coda dormiente e' gratis, anche se ruota

1.000 msg/s, insieme attivo ~100 partizioni:

| partizioni totali | insieme caldo fisso | insieme caldo **rotante** |
|---:|---:|---:|
| 10.000 | 5,47 ms | — |
| 100.000 | 5,34 ms | 2,13 ms |
| 1.000.000 | 1,96 ms | **1,50 ms** |

Un milione di partizioni con l'insieme attivo che scorre di continuo su tutte:
**1,50 ms**. E' la forma "un partizione per utente Amazon italiano, mille che
cliccano adesso" — e non costa nulla.

### La leva piu' grande della giornata

Stesse 1.000.000 di partizioni, stessi 5.000 msg/s, cambia **solo** l'ampiezza
della pop:

| `pop-partitions` | p99 |
|---:|---:|
| 100 | **17.170 ms** |
| 10.000 | **102 ms** |

**168x da un'impostazione del client.** Non e' un artefatto del banco: e' l'SDK
che decide se una cella e' sana o morta. Da capire e da tarare nel client, non
da lasciare al default.

## 2. Numeri della cella (VM, 16 vCPU — gli unici assoluti affidabili)

| piano | forma | tenant/cella | p99 all'ultimo rung dentro SLO |
|---|---|---:|---:|
| free | 2 code x 100 part, 5 msg/s | **>= 600** | 100,9 ms |
| dev | 10 code x 100 part, 25 msg/s | **100-200** | 105,0 ms a 100 |
| pro | 20 code x 500 part, 50 msg/s | **>= 30** | 34,0 ms a 30 |

free e pro sono **limiti inferiori**: sono finiti i tenant provisionati, non la
cella. Costo cella misurato: 5,54 core a 1.000 tenant, 10,21 a 5.000, con
**Postgres al ~60%** in ogni rung.

Il bilanciatore va a **hash sul tenant**: round-robin per richiesta costava
1.044 ms di p99 contro 38,7 ms con affinita' (HAProxy, 5.000 tenant). Con
HAProxy + hash la prima r5000 completamente pulita: **0 persi, 0 duplicati,
0 extra, 0 cross-tenant su 1.017.083 messaggi**.

Failover di un broker sotto carico pieno: **0 messaggi persi**, 13 s di
rilevazione, danno confinato ai tenant che stavano sul nodo morto.

## 3. Cosa questo dice dei piani

- **Vendere partizioni a piene mani.** Sono gratis e sono il differenziatore. Il
  cap serve solo come guardia anti-DoS e puo' stare ordini di grandezza sopra
  qualunque applicazione onesta (la produzione Smartpricing gira 53.259
  partizioni per **tre** prodotti).
- **Misurare i msg/s.** E' l'unico asse che consuma la cella.
- **`partitions per queue` non e' una manopola del tenant**: e' un moltiplicatore
  di costo (2,8x fra 100 e 500 di ampiezza a pari carico) e va deciso dal
  prodotto.
- **Il limite vero e' della CELLA, non del tenant**: piani generosi + un budget
  di partizioni per cella rispettato dal placement.

## 4. Cosa NON e' stato misurato (e non va spacciato)

- **Termine storage del TCO: assente.** Nessuna run ha visto bloat, autovacuum a
  regime o crescita disco. E' di solito il costo che sorprende.
- **Partizioni dormienti VUOTE.** Un milione di partizioni *senza storia* e'
  gratis da servire; un milione con dati vecchi e' un altro esperimento (soak con
  coda seminata).
- **Manutenzione a lungo termine.** 60 s non vedono un ciclo di stats refresh; il
  muro noto sta fra 5 e 20 M di segmenti.
- **Rumore.** Il rig locale mostra **1,5x di spread p99 su configurazione
  identica** a 60 s. Sotto quel fattore non c'e' segnale. Una replica per rung
  ovunque.
- **Il Mac non sincronizza davvero**: `wal_sync_method=open_datasync` su macOS
  non forza la cache del disco (`fsync_writethrough` costa 3.989 us contro i
  57 us dichiarati). I numeri locali di throughput non sono confrontabili con la
  VM; le *forme* si'.

## 5. Difetti di prodotto trovati oggi

1. **Il broker e il proxy fissano `0.0.0.0`** — nessuna configurazione di bind.
   "Il broker e' raggiungibile solo dal proxy" non e' imponibile dal processo.
   Su VM (il piano Queen Cloud) resta appeso alle regole di firewall dell'host.
2. **Le riconsegne contano sulla quota messaggi del tenant**: una cella in
   affanno strozza il proprio recupero, e chi e' stato colpito viene rallentato
   dalla propria quota mentre cerca di riprendersi.
3. **Il default di Docker, 1.024 file descriptor**, uccide una cella verso i 900
   tenant — una long-poll parcheggiata per tenant. Si presenta come timeout del
   broker e 502, mai come qualcosa che nomini gli fd.
4. **Chiedere esattamente `max_partitions_per_queue` fa scattare la quota**: la
   partizione di default conta, quindi l'allowance effettiva e' N-1.
5. **`req/s` e `msg/s` dei piani si contraddicono**: `free` dava 20 msg/s con
   5 req/s, inusabile per un client non batched dato che un messaggio e'
   push+pop+ack.
6. **Il mesh non ha telemetria**: nessun contatore di invii/ricezioni/scarti. Se
   un giorno i risvegli si perdessero davvero, oggi non ci sarebbe alcun numero
   che lo mostri.
