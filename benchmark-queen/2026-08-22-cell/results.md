# Densita' di una cella Queen Cloud — 1 PG, 2 broker, 1 proxy (2026-08-22)

**Una cella su una VM da 16 vCPU regge 3.000 tenant dentro un p99 di 200 ms,
spendendo 8,54 core. A 5.000 si rompe — ma non per CPU, non per il database e
non per il rate limiter: si rompe perche' il giro di consegna sulle lane
impiega ~25 secondi a servire una lane gia' pronta.**

Montaggio della cella e trappole: [README.md](README.md). Grezzi in `raw/`.

## La forma

Mix SaaS realistico, tre profili concorrenti sulla stessa cella:

| profilo | quota | piano | rate | perche' |
|---|---:|---|---:|---|
| idle | 90% | `free` | 0,2 msg/s | la coda lunga che non paga e costa comunque code, pop parcheggiate e manutenzione |
| active | 9% | `pro` | 5 msg/s | tenant paganti ordinari |
| noisy | 1% | `pro` | 50 msg/s (batch 10) | il vicino rumoroso |

Ogni tenant tiene **un consumer in long-poll parcheggiato**. SLO dichiarato
prima di misurare: **la cella e' piena quando il p99 e2e supera 200 ms**.
`429 = 0` in ogni rung: se il limitatore di piano avesse morso avremmo misurato
il piano, non la cella.

## Il risultato

| tenant | idle p99 | active p99 | noisy p99 | verdetto SLO | **core cella** | RSS |
|---:|---:|---:|---:|:--|---:|---:|
| 1.000 | 9,8 ms | 13,0 ms | 20,1 ms | PASS | 5,54 | 3,96 GB |
| **3.000** | 16,8 ms | 43,3 ms | **43,8 ms** | **PASS** | **8,54** | 4,97 GB |
| 5.000 | 27,5 ms | 929,8 ms | 536,6 ms | FAIL | 10,21 | 6,42 GB |

A 3.000 tenant il peggior p99 e' **43,8 ms contro un budget di 200**: quattro
volte di margine. Fra 3.000 e 5.000 il ginocchio non e' campionato.

### Ripartizione del costo

| tenant | PG | broker A | broker B | proxy | lb | TOTALE | quota PG |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1.000 | 3,50 | 0,75 | 0,77 | 0,32 | 0,20 | 5,54 | 63% |
| 3.000 | 5,24 | 1,07 | 1,12 | 0,67 | 0,44 | 8,54 | 61% |
| 5.000 | 6,04 | 1,28 | 1,30 | 0,99 | 0,60 | 10,21 | 59% |

Tre cose che si leggono da sole:

1. **Postgres e' ~60% della cella**, stabile a ogni rung. Proxy (0,32→0,99) e
   bilanciatore (0,20→0,60) sono spiccioli. Chi vuole abbassare il costo di una
   cella lavora su Postgres, non sul broker.
2. **Il costo e' fortemente sublineare**: 5x i tenant per 1,84x la CPU. La
   maggioranza della flotta e' idle, e un tenant idle in CPU non costa quasi
   nulla — costa code e connessioni parcheggiate.
3. **I due broker si dividono il carico in parti uguali** (0,75/0,77 →
   1,07/1,12 → 1,28/1,30). La forma a due repliche dietro un bilanciatore
   funziona come in produzione.

## Cosa si rompe davvero a 5.000

Non e' nessuna delle risposte comode. Dalla telemetria del broker nella finestra
r5000:

```
pool="4/160"   pool_waiting=0   (0 warning di pool saturo)
oldest_wait_ms = 0,9 – 19,8     adm: nessuna corsia in attesa
pop_empty_pct  = 0 – 23,6%      parked = 1.075 – 2.478 per broker
ready_age_p95  = 22.503 – 31.471 ms
```

- **CPU**: la cella sta al **64%** di una VM da 16 vCPU. Non e' satura.
- **Database**: pool a 4 connessioni su 160. Non e' il collo.
- **Rate limiter**: zero 429. Non e' il piano.
- **Generatore di carico**: media **6,1%** CPU, picco 10,9%, load1 picco 2,7 su
  16 vCPU — largamente sotto il cancello del 70% (SPEC §5.1). **Non e' il banco.**
  Verificato apposta: nella campagna isolation la stessa forma di degrado *era*
  il banco, quindi qui e' stato misurato invece che assunto.

Resta `ready_age_p95` fra **22 e 31 secondi**. Il broker *sa* che il lavoro e'
pronto — lo misura — e poi passano venticinque secondi prima che qualcuno lo
reclami. Con 5.000 code e ~5.000 consumer parcheggiati il giro di visita delle
lane non ce la fa, e il p99 esplode mentre la macchina e' inattiva per un terzo.

La firma dell'errore lo conferma: `firstMissing=[8811 8812 ...]`. Manca la
**coda** della sequenza, non la testa: ogni tenant ha ricevuto un prefisso
contiguo e ha perso tutto il resto. Non e' perdita, e' backlog non drenato —
anche con 60 s di drain.

**Il limite della densita' di cella e' la latenza di rotazione sulle lane, non
una risorsa.** E' la stessa legge gia' misurata nella crossbench (la latenza e'
la cardinalita' delle lane) e qui si vede sul numero che interessa vendere.

## TCO

Una VM CPU-Optimized da 16 vCPU / 32 GiB costa **336 $/mese**.

- **3.000 tenant dentro SLO → 0,112 $ per tenant al mese** di sola compute.
- Ma a quel punto la cella usa **8,54 core su 16: il 53%**. Poiche' il tetto e'
  la rotazione e non la CPU, **il 47% della macchina si paga e non si puo'
  riempire**. Non e' un margine di sicurezza, e' spreco strutturale.

Da cui la leva vera: **ogni miglioramento del giro di consegna si converte
direttamente in tenant per euro**, mentre comprare piu' core no. Se la rotazione
reggesse a CPU costante, la stessa VM arriverebbe vicino ai 5.000 tenant — cioe'
**0,067 $/tenant/mese**, un terzo in meno, senza toccare l'hardware.

## Difetto di configurazione trovato montando la cella

Al primo giro **130 code su 900 non si sono create** (125 timeout + 5 `502
bad_gateway: upstream unreachable`) e goload si e' rifiutato di misurare. Causa:
il limite di file descriptor **di default di Docker, 1024**, sul container del
proxy (`Too many open files (os error 24)`; nginx logga `RLIMIT_NOFILE
1024:524288`).

Una cella tiene **una long-poll parcheggiata per tenant**: il budget di fd scala
col numero di tenant, non col traffico, e ~900 tenant e' esattamente dove
finiscono 1024 descrittori. **Si presenta come timeout del broker ed errori di
gateway, mai come qualcosa che nomini i file descriptor.** Se Queen Cloud fa
girare il proxy senza un `nofile` esplicito, lo incontrera' verso il migliaio di
tenant per quanto inattivi siano.

Stesso guasto, secondo sintomo: tre tenant idle a r300 con `firstMissing=[1]`.
Il warm-up a 900 tenant aveva colpito lo stesso muro, quindi quei gruppi non si
erano mai stabiliti e il loro primo messaggio e' stato saltato dal subscription
mode `new`.

## Limiti

- **Ginocchio non campionato** fra 3.000 e 5.000: manca r4000.
- **Una replica per rung.** I numeri di costo sono medie su ~190 s di finestra
  attiva, ma non c'e' ripetizione.
- **Nessuna misura di storage.** Un'ora non vede bloat, autovacuum a regime, ne'
  la crescita del disco: il termine storage del TCO qui **non c'e'**, e va preso
  da una run lunga separata.
- **`shared-queue`**: una coda per tenant. Un tenant reale ne ha piu' d'una, e la
  campagna tenant di luglio/agosto usava 10 code per tenant — questi numeri non
  sono confrontabili con quelli.
- Il tetto misurato vale **per questo mix**. Alzare la quota di tenant attivi
  sposta tutto.

---

## Appendice — perche' due broker costano latenza (e non e' il mesh)

Ipotesi in campo (Alice, 2026-08-22): *fra i broker a volte si perde il risveglio
del mesh, e questo alza la latenza*. Testata, perche' e' falsificabile con una
variabile sola.

Tre configurazioni, stesso r5000, stesso drain da 60 s, loader campionato e
sempre sotto il cancello del 70%:

| config | idle p95 / p99 | active p95 / p99 | noisy p95 / p99 |
|---|---:|---:|---:|
| **A** 2 broker, mesh **ON** | 23,9 / 43,8 | 593,9 / 1044,5 | 110,1 / 708,6 |
| **B** **1 broker** | **13,8 / 19,8** | **12,9** / 839,7 | **12,0 / 50,4** |
| **C** 2 broker, mesh **OFF** | 22,7 / 40,2 | 58,1 / 1368,1 | 45,3 / 1138,7 |

**Il mesh non e' il meccanismo.** Se il problema fossero i risvegli persi,
spegnere il mesh (C) dovrebbe peggiorare le cose in modo drastico — nessun
risveglio, non "qualcuno perso". Invece C e' indistinguibile da A sull'idle
(22,7 contro 23,9 di p95) ed e' perfino *migliore* su active e noisy al p95.
Mesh acceso e mesh spento si equivalgono.

**Quello che cambia e' il NUMERO di broker.** Un broker da 12-14 ms di p95 su
tutti e tre i profili; due broker danno 45-594 ms, con o senza mesh.

Spiegazione compatibile con i dati: ogni broker gira **la propria**
manutenzione e la propria rotazione su tutte le 5.000 code dello stesso
Postgres. Quel lavoro per-coda si paga due volte e le due rotazioni si contendono
le stesse lane. Un consumer parcheggiato su B aspetta che la rotazione **di B**
arrivi sulla lane: un risveglio mandato da A non gli fa saltare la fila. E'
coerente con la legge gia' misurata altrove (la latenza e' la cardinalita' delle
lane) e spiega perche' il mesh non sposti nulla.

**Cautela:** una replica per configurazione, e il p99 di questo banco balla
(839 / 1044 / 1368 su run equivalenti). Il segnale solido e' il **p95**:
12-13 ms contro 45-594 ms sta molto fuori da quel rumore. Il confronto
mesh-ON/mesh-OFF in particolare va ripetuto 3 volte prima di pubblicarlo.

**Il mesh non ha telemetria.** Non esiste un contatore di invii, ricezioni o
scarti in `obs.rs` ne' nella riga `rates`. Se un giorno i risvegli si perdessero
davvero, oggi non ci sarebbe **nessun numero** che lo mostri. Da aggiungere a
prescindere da questo risultato.

### Conseguenza sulla cella e sul prezzo

A un broker la cella arriva **vicino ai 5.000 tenant dentro SLO** (solo active
p99 sfora) contro i 3.000 con due. Sono **~0,067 $/tenant/mese** invece di
0,112: **-40% di COGS**.

Ma due repliche in produzione ci sono per **disponibilita'**, non per portata
(`helm_v1/broker/values.yaml`: `replicas: 2`). Quindi la scelta va fatta con gli
occhi aperti: **l'HA dentro la cella oggi costa circa il 40% della densita'**.
Non e' un argomento per toglierla — e' un argomento per farla costare meno,
perche' quel 40% si converte direttamente in margine.

---

## Appendice 2 — era il BILANCIATORE, non il mesh e non i due broker

Domanda di Alice: *ma davanti ai due broker c'e' un nginx, giusto?* Si', ed e'
stato un mio errore di setup, non un dato su Queen.

nginx con `upstream` di default bilancia **per RICHIESTA** (round-robin). Con
5.000 consumer in long-poll, le pop successive dello stesso consumer atterrano
alternativamente sui due broker: nessuno dei due costruisce localita' sulle
lane, ognuno deve riscoprirle. Un Service Kubernetes **non** fa cosi' (bilancia
per CONNESSIONE, L4), quindi il banco stava producendo un rimbalzo che la
produzione non ha.

Rifatta la stessa cella con **affinita' per tenant** (hash sull'header
`x-queen-tenant` che il proxy inietta gia'), r5000, mesh acceso, 60 s di drain:

| bilanciatore | idle p99 | active p99 | noisy p99 | correttezza |
|---|---:|---:|---:|:--|
| nginx round-robin | 43,8 ms | 1.044,5 ms | 708,6 ms | dup |
| un solo broker | 19,8 ms | 839,7 ms | 50,4 ms | FAIL |
| nginx hash-tenant | 74,2 ms | 52,5 ms | 50,9 ms | 10 dup |
| **HAProxy hash-tenant** | **27,8 ms** | **38,7 ms** | **27,8 ms** | **0 / 0 / 0** |

**Tutta la penalita' dei due broker era il round-robin.** Con l'affinita', due
broker vanno *meglio* di uno (active p99 38,7 contro 839,7), e la cella regge
**5.000 tenant dentro SLO** invece di 3.000.

HAProxy batte nginx anche a parita' di algoritmo, su tutti e tre i profili, e
soprattutto sulla correttezza: **0 mancanti, 0 duplicati, 0 extra, 0 cross-tenant
su 1.017.083 messaggi**. E' la prima r5000 completamente pulita della campagna.
Avevo previsto che HAProxy non avrebbe cambiato i numeri: previsione sbagliata.

### Cosa va corretto nei numeri di sopra

- La densita' della cella e' **>=5.000 tenant**, non 3.000. Il tetto vero non e'
  stato ancora trovato: a 5.000 con HAProxy il peggior p99 e' 38,7 ms su un
  budget di 200.
- **0,067 $/tenant/mese** (5.000 su una VM da 336 $), non 0,112. Per tenant
  pagante: **0,67 $/mese** al mix 90/9/1.
- La frase "l'HA dentro la cella costa il 40% della densita'" e' **falsa**. Non
  costava l'HA: costava il round-robin.
- `ready_age_p95` a 22-31 s resta il sintomo osservato **sotto round-robin**. Non
  e' stato rimisurato sotto affinita' e non va citato come proprieta' del broker.

### La regola che resta, e che vale per il prodotto

**La mappatura tenant -> broker deve essere deterministica e stabile.** Con N
macchine proxy davanti a M celle, se ogni proxy sceglie il broker per conto suo
in round-robin si ricade esattamente nei 1.044 ms. L'hash consistente sull'id di
tenant lo garantisce, sia che viva nel bilanciatore sia — meglio — nel proxy,
che il tenant lo conosce gia' e si risparmierebbe un hop e un componente per
cella.

---

## Appendice 3 — failover di un broker sotto carico (HAProxy, 5.000 tenant)

`docker kill cell-broker-a` a meta' run, carico pieno, hash consistente per
tenant. Con due server l'hash consistente sposta **tutti** i tenant del morto sul
superstite in una volta: e' anche la prova del thundering herd.

| | |
|---|---|
| rilevazione → rimozione dal pool | **13 s** (kill 09:14:59Z, DOWN 09:15:12Z) |
| **messaggi persi** | **0 su tutti e tre i profili** |
| duplicati | 1 / 38 / 100 — al massimo lo 0,017% di 576.270 |
| idle p99 | 23,2 ms (pulito: 27,8) — **invariato** |
| noisy p99 | 41,2 ms (pulito: 27,8) — **quasi invariato** |
| **active p99** | 38,7 → **708,6 ms** — qui sta tutto il costo |
| 429 su noisy | **1.259** (run pulita: 0) |

**Zero perdita attraverso la morte di un broker sotto carico pieno.** I duplicati
sono at-least-once dopo la scadenza dei lease del nodo morto: goload stampa FAIL
perche' il suo cancello e' 0 duplicati, ma una riconsegna dopo la caduta di un
nodo e' la semantica che funziona, non un difetto. Da non leggere male.

### Due cose da sistemare

1. **Il rate limit di piano strozza il recupero.** I 1.259 × 429 compaiono solo
   nella run di failover: quando i tenant di A si spostano su B, la raffica di
   recupero supera il loro stesso limite di messaggi e viene throttolata. I
   tenant piu' colpiti dalla perdita del nodo vengono poi **rallentati dalla
   propria quota** mentre recuperano — cioe' il tempo di recupero dipende dal
   piano. Serve burst headroom o un'esenzione di recupero.
2. **La rilevazione e' larga.** 13 s contro i ~6 s teorici di `inter 2s fall 3`.
   Con `inter 1s fall 2` si sta sui 2-3 s. Siccome il costo e' tutto dentro
   quella finestra, dimezzarla dimezza all'incirca il picco su active.

### Nota sullo strumento

Le righe HAProxy `CH-- 400` in coda al log sono delle 09:18:02, cioe' lo
smontaggio della run, non il failover: il mio grep ha preso le righe di richiesta
che contengono "broker-b" invece degli annunci di cambio stato. La transizione
buona e' quella campionata dalle stats, ed e' quella riportata sopra.

---

## Appendice 4 — il tetto di contesa (mix commerciale 500/300/200)

Mix rivisto da Alice: **50% free / 30% dev / 20% pro**, cioe' meta' dei tenant
paga. 5.000 tenant, **10 partizioni per coda**. Il rate offerto e' stato
rampato a popolazione FISSA, perche' la scala per numero di tenant misurava
"quanti tenant stanno dentro al 4,4% del loro diritto" — una risposta che dava
per assunto proprio il rapporto di sovravendita che doveva misurare.

Diritto venduto a questa popolazione: 2500×20 + 1500×40 + 1000×200 =
**310.000 msg/s**. Ogni rung riporta la frazione di quel diritto che offre.

| rung | offerti | contesa | free p99 | dev p99 | pro p99 | persi | dup |
|---|---:|---:|---:|---:|---:|---:|---:|
| L025 | 3.375/s | **1,09%** | 41 ms | 54 ms | 100 ms | 0 | 0 |
| L030 | 4.050/s | 1,31% | 643 ms | 276 ms | 109 ms | 0 | 0 |
| L035 | 4.725/s | 1,52% | 56 ms | 112 ms | 202 ms | 0 | 0 |
| L040 | 5.400/s | 1,74% | 53 ms | 151 ms | 1.044 ms | 0 | 0 |
| L050 | 6.750/s | 2,18% | 19.792 ms | 16.450 ms | 14.877 ms | 0 | 0 |
| L075 | 10.125/s | 3,27% | 19.792 ms | 14.090 ms | 15.532 ms | 0 | 6.873 |
| L100 | 13.500/s | 4,35% | 19.530 ms | 19.268 ms | 19.530 ms | 0 | 13.143 |

### Cosa si legge

1. **Zero messaggi persi a ogni rung**, anche quattro volte oltre il ginocchio.
   La cella degrada in latenza e poi in riconsegna, **mai in perdita di dati**.
2. **La CPU non e' il vincolo.** Fra L025 e L050 il costo passa da **10,14 a
   11,35 core** (+12%) mentre il p99 va da 100 ms a 19.800 ms. Un terzo della
   macchina resta inattivo mentre la cella e' inservibile. **Dimensionare a
   core-per-tenant misura la risorsa sbagliata**: dare 32 core a questa cella
   non sposterebbe il ginocchio.
3. **A regime la cella fa ~333 msg/s per core** (3.375 su 10,14) dentro SLO.
4. **L'ordine di rottura**: prima la latenza, poi la riconsegna (dup a L075),
   la perdita mai.
5. **Sotto contesa cede prima il tier FREE.** A L025 il peggiore e' `pro`
   (100 ms) e `free` il migliore (41 ms); un rung dopo si inverte. Le lane rade
   di un tenant free aspettano il giro di rotazione, e quando il giro rallenta
   pagano loro. Commercialmente e' al contrario: chi si sta facendo un'idea del
   prodotto ha l'esperienza peggiore proprio quando la cella e' carica.

### Il ginocchio NON e' stato inchiodato

L030 (643 ms) e' peggio di L035 (56 ms) a carico **maggiore**: non monotono. E'
la stessa varianza run-to-run gia' vista nella campagna isolation (p50 340 →
3.234 ms su celle identiche), ed e' esattamente il motivo per cui SPEC §10.3
chiede tre ripetizioni prima di pubblicare un delta cross-run. Difendibile oggi:
**sana all'1,09%, marginale intorno all'1,5%, distrutta al 2,18%**. Per dire di
piu' servono 3 run per rung.

### Le partizioni costano

A parita' di tenant e su DB pulito:

| forma | partizioni | carico pulito | p99 |
|---|---:|---:|---:|
| vecchio mix, 1 partizione/coda | 5.000 | 5.650 msg/s | 27,8 ms |
| nuovo mix, 10 partizioni/coda | 50.000 | 3.375 msg/s | ~100 ms |

**Dieci partizioni per coda dimezzano circa il tetto di portata**, a parita' di
numero di tenant. Il parallelismo d'ordinamento dentro un tenant non e' gratis.

### Le partizioni residue avvelenano tutto

Le prime run del mix commerciale sono state falsate da **89.086 partizioni**
rimaste in DB dalla run a 10.000 tenant: configure in timeout (131/2500 falliti),
duplicati a decine di migliaia, e **548.402 × 429** sui pro. Dopo `DROP DATABASE
queen` le stesse identiche configurazioni danno configure puliti in 2-3 s, zero
429 e zero duplicati fino a L050.

Il meccanismo dei 429 va ricordato perche' e' un difetto di prodotto, non del
banco: **le riconsegne contano sulla quota messaggi del tenant**, quindi una
cella in affanno strozza il proprio recupero, e i tenant piu' colpiti vengono
rallentati dalla loro stessa quota mentre cercano di riprendersi. Stessa firma,
in piccolo, nella prova di failover.

---

## Appendice 5 — tabella di capacita' per piano (cella 16 core)

Obiettivo corretto da Alice: non "5.000 tenant stanno in 16 core?" ma **per una
macchina di N core, quanti tenant di ciascun piano ci stanno?** — cosi' i conti
si possono fare.

Piani come specificati (la forma dell'applicazione fa parte del piano, non e' un
dettaglio implementativo da semplificare):

| piano | code | partizioni/coda | partizioni tot | msg/s e2e |
|---|---:|---:|---:|---:|
| free | 2 | 100 | 200 | 5 |
| dev | 10 | 100 | 1.000 | 25 |
| pro | 20 | 500 | 10.000 | 50 |

Cella: 1 PG + 2 broker + HAProxy + proxy, **non capped su box da 16 vCPU**, tutti
i container sotto un'unica slice systemd. SLO: **p99 peggiore fra TUTTE le code
del tenant ≤ 200 ms** (chi ha la coda 7 lenta non si consola con le altre sei).

| piano | tenant | consumer | partizioni | scan/pop | msg/s | p99 peggiore | SLO |
|---|---:|---:|---:|---:|---:|---:|:--|
| free | 100 | 200 | 20.000 | 100 | 500 | 3,7 ms | OK |
| free | 250 | 500 | 50.000 | 100 | 1.250 | 11,5 ms | OK |
| free | 400 | 800 | 80.000 | 100 | 2.000 | 42,2 ms | OK |
| **free** | **600** | 1.200 | 120.000 | 100 | 3.000 | **100,9 ms** | OK ← finiti i tenant |
| dev | 25 | 250 | 25.000 | 100 | 625 | 7,4 ms | OK |
| dev | 50 | 500 | 50.000 | 100 | 1.250 | 13,9 ms | OK |
| **dev** | **100** | 1.000 | 100.000 | 100 | 2.500 | **105,0 ms** | OK |
| dev | 200 | 2.000 | 200.000 | 100 | 5.000 | 44.302 ms | **OVER** |
| pro | 5 | 100 | 50.000 | 500 | 250 | 7,1 ms | OK |
| pro | 10 | 200 | 100.000 | 500 | 500 | 9,2 ms | OK |
| pro | 20 | 400 | 200.000 | 500 | 1.000 | 12,3 ms | OK |
| **pro** | **30** | 600 | 300.000 | 500 | 1.500 | **34,0 ms** | OK ← finiti i tenant |
| *prova forma* | 10 | 200 | 100.000 | 500 | **2.500** | **292,9 ms** | **OVER** |

**Tetti: free ≥600, dev fra 100 e 200, pro ≥30.** Due su tre sono limiti
INFERIORI: free e pro non si sono rotti, sono finiti i tenant provisionati.

### Le tre leggi che ne escono

1. **Il tasso di messaggi e' il costo primario.** Ogni riga OVER e' una riga ad
   alto rate. 300.000 partizioni a 1.500 msg/s fanno 34 ms; 200.000 a 5.000 msg/s
   fanno 44 secondi.
2. **Le partizioni per CELLA sono quasi gratis.** 300.000 partizioni a 34 ms.
   Cade anche la paura dello stack overflow a 100k della 1.0.4: 100k, 200k e
   300k girano puliti su questa forma.
3. **Le partizioni per CODA costano** — e' l'ampiezza di scansione per pop.
   Stesse 100.000 partizioni e stessi 2.500 msg/s: scansione da 100 → 105 ms,
   scansione da 500 → 293 ms. **2,8x per la sola geometria.**

Osservazione di Alice che ha inquadrato il punto: *se il carico e' sparso su piu'
partizioni per coda, la pop soffre di piu'*. Esatto — il rate per partizione e'
identico (0,025 msg/s) nei due casi, ma ogni consumer pro deve spazzare 500
partizioni invece di 100 per raccogliere i suoi messaggi.

### Correzione a quanto detto prima in questo documento

Nelle appendici precedenti avevo scritto che **le partizioni** erano il vincolo
(«dieci partizioni per coda dimezzano il tetto di portata»). Era dedotto da forme
in cui partizioni e rate salivano INSIEME, quindi non li distingueva. Queste
scale li separano per la prima volta: **il rate domina, la cardinalita' e'
economica, e cio' che costa davvero e' l'ampiezza di scansione per pop.**

### Cosa manca

- **La colonna dei costi.** `sampler.sh` legge i cgroup nel percorso docker di
  default, ma `--cgroup-parent=queencell.slice` sposta i container: tutti i CSV
  della campagna sono **vuoti**. La slice espone `cpu.stat` (`usage_usec`), che
  e' anche la misura giusta — e' il budget della cella. Da rifare su 4 rung.
- **I tetti veri di free e pro** (servono piu' tenant provisionati).
- **8 core e 2 core**: la tabella per macchina e' ferma al primo dei tre passi.
- Una replica per rung, con la varianza nota di questo banco.
