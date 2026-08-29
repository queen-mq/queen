# Horizon vs Queen PHP vs Queen Rust — report tecnico

**Stato del documento:** report completo e auditabile, aggiornato al 29 agosto
2026.

**Classificazione dei risultati Laravel:** diagnostic.

**Classificazione dei fault test:** diagnostic_smoke.

**Protocollo congelato:** [protocol.md](protocol.md).

Il benchmark è stato progettato ed eseguito dal team Queen. Protocollo
predefinito, conservazione dei run, pairing e gate fail-closed sono misure
anti-bias; rendono l'analisi riproducibile e auditabile, non una certificazione
indipendente.

## 1. Obiettivo e conclusione

La campagna risponde a quattro domande distinte:

1. il client Laravel Queen, nella configurazione corretta, è competitivo con
   Horizon?
2. quanto costa il solo orchestratore PHP o Rust?
3. quanto costa il perimetro consumer+backend realmente misurato?
4. worker crash, volume elevato e funzionamento prolungato conservano
   correttezza e operatività?

La risposta supportata dagli artifact è:

- il vecchio prefetch 1 limitava fortemente Queen; con **prefetch 4 e ACK batch
  1**, Queen PHP e Queen Rust superano Horizon di circa il 14% nel profilo
  fixed primario sul commit post-guard misurato 3655cd2a;
- Queen Rust conserva throughput analogo a Queen PHP ma riduce il PSS del
  control plane a circa il 4,0% di Horizon;
- nel profilo primario Queen Rust usa circa il 40% in meno di CPU nello stack
  misurato, ma circa il 39% in più di memoria rispetto alla
  topologia Horizon+Redis;
- nel workload CPU-bound il throughput e la CPU dello stack misurato
  convergono, mentre il vantaggio dell'orchestratore Rust resta;
- a un worker Queen è circa il 2% sopra Horizon; a otto worker le mediane sono
  più basse ma i CI paired attraversano 1;
- 15 fault lane p4/a1 post-guard su 15 completano il recovery strict; p4/a4 conserva
  at-least-once in 10/10 lane ma strict passa solo 3/10 per sette duplicati;
- p16/a16 pre-guard, con floor runtime nominale sotto la lease, conserva
  at-least-once in 10/10 lane ma strict passa solo 1/10, con 17 duplicati;
- il soak da circa 1M msg/s dimostra la capacità broker-native di Queen, ma non
  è un benchmark Laravel e non è numericamente confrontabile con i job/s.

La proposta è **orchestratore Rust**, ma con shipping default conservativo
**p1/a1**. P4/a1 resta un profilo prestazionale short-job soltanto se
`retry_after > prefetch × worst-case runtime + margine`, oppure esiste lease
renewal. Le 64 partizioni e pop fusion off sono la baseline verificata, non
parametri selezionati da un confronto dedicato. Queen PHP resta un fallback;
ogni ACK batch >1 resta opt-in idempotente. La conclusione è una readiness
recommendation condizionata: non autorizza ancora un claim generale di
sostituzione production-ready di Horizon. Il perimetro verifica performance e
recovery, non la feature parity di dashboard, metriche, failed-job store,
pause/resume o altri controlli operativi Horizon.

Il claim prestazionale non si trasferisce a p1/a1: nel legacy Queen PHP/Rust
misurano 118,15/121,39 job/s contro 261,85 Horizon, circa -55%. Rust è
raccomandato come control plane; performance equivalente o superiore richiede
p4 con lease adeguata oppure una futura lease renewal.

## 2. Sistemi confrontati

| Lane | Backend di coda | Orchestratore | Processo worker |
| --- | --- | --- | --- |
| Horizon | Redis | master e supervisor Horizon in PHP | artisan horizon:work |
| Queen PHP | broker Queen + PostgreSQL | comando Laravel Queen in PHP | artisan queue:work queen |
| Queen Rust | broker Queen + PostgreSQL | queen-supervisor Rust | artisan queue:work queen |

La fixture usa PHP 8.3.33, Laravel 12.68.0 e Horizon 5.48.3. Il client Queen è
costruito dal checkout, riportato come dev-main; il supervisor Rust è costruito
dallo stesso checkout. Tutte le lane eseguono lo stesso BenchmarkJob
serializzato e scrivono sullo stesso tipo di sink JSONL esterno al backend di
coda. La descrizione completa della fixture è nel
[README del benchmark](../../README.md).

Il confronto espone intenzionalmente due viste:

- **control plane:** CPU e PSS dei processi di orchestrazione, esclusi worker e
  backend;
- **stack misurato:** cgroup applicazione consumer (supervisor e worker) più
  backend completo, cioè Redis per Horizon e broker+PostgreSQL per Queen;
  producer, observer e sampler dell'harness sono esclusi dall'aggregato.

Confrontare il solo broker Queen con Redis, omettendo PostgreSQL, sarebbe
fuorviante. Analogamente, il PSS dell'orchestratore non può essere usato come
memoria del perimetro misurato, che a sua volta non include producer e
observer.

## 3. Provenienza

### 3.1 Freeze e storia del codice

Il protocollo è stato congelato prima dell'ispezione dei risultati:

| Passaggio | Commit | Significato |
| --- | --- | --- |
| Freeze | f2c39fd98987c2628c7d9a8bb6cd57c4ecbb16cc | protocollo e regole di selezione |
| Hot path | 92c2cbe33c6c80c6133957573a69182ca02bb9dc | trim dei percorsi worker Laravel |
| Harness | 5b47a7d579abd9258cb073492b2b229a7f95f377 | metodologia supervisor resa fail-closed |
| Observer | 2a4b107a0690bdfd68b7cfeb840b3987d773b5e4 | raccolta risultati scalabile e streaming |
| Aggressive confirm/fault | 327849a1edbae2b1c5f17c402062dbf5dc5c4237 | conferme e recovery p4/a4+p16/a16 |
| Lease guard post-benchmark | c405902736d527a1a03fceda380e68f89c7df814 | fail-fast su finestre prefetch/lease non sicure |
| Harness lease budget | 3655cd2a6302e296722bab207a704e38726805f7 | applicazione del guard, smoke, fixed e fault post-guard misurati |
| Hardening tooling | 774c24136a55cd5bf4ec9f518ef41af3b20e975f | chiarimento dell'override negativo; successivo alle image misurate |

Si tratta di un freeze locale timestamped: il mtime di `protocol.md`
(02:45:19 locali) precede l'avvio dello screening (02:45:33), ma il documento
non è una preregistrazione firmata o un artifact immutabile di terza parte.

Le worktree delle campagne selezionate e confermatorie registrate nei metadata
sono clean; gli screening one-shot dirty sono esclusi. Il freeze
registrava immagini preliminari app
sha256:adaac94a4f2ce503db1c27ca26697a37872f6c8fa57831352afa1025d1d1be3e
e broker
sha256:472ac63907a8168702b037053258fd0f01e0c04180d214b429a4f3d79b3dad7a.
Queste non vanno confuse con le immagini effettivamente misurate, catturate
nei metadata di ogni campagna:

| Campagne misurate | Commit clean | App image | Broker image |
| --- | --- | --- | --- |
| fixed/auto storici pre-guard, legacy, CPU, select p4/p16 | 5b47a7d579abd9258cb073492b2b229a7f95f377 | sha256:39f291705a9acf7408daa4d06818e766c865f77dbebcdd451883f1c60394b918 | sha256:c239df18950ee0598d1517a9f9989a29dd8aa2ed664b33ead1f2f3feebd4187a |
| stress a1, scaling w1/w8, discovery a4, fault a1 | 2a4b107a0690bdfd68b7cfeb840b3987d773b5e4 | sha256:9386d3716853a6f9d09c9e455ce9c95326a01516655747fa785566543b7ccf6c | sha256:c239df18950ee0598d1517a9f9989a29dd8aa2ed664b33ead1f2f3feebd4187a |
| fixed/auto p16/a16 e fault p4/a4+p16/a16 | 327849a1edbae2b1c5f17c402062dbf5dc5c4237 | sha256:b631f6f6ff90202611ac337fecf10d88e557509ea545f9965df023aa0c0cc21d | sha256:c239df18950ee0598d1517a9f9989a29dd8aa2ed664b33ead1f2f3feebd4187a |
| fixed primaria, fault p4/a1 e smoke post-guard misurati | 3655cd2a6302e296722bab207a704e38726805f7 | sha256:0e82c6cc8930410001d2a65de5c73abe1f39811f0dd3e78476f8e09071aa8958 | sha256:522bdc090da5c0845990444194d25c916abe3ce8a34bc653093e1aba87e801fd |
| auto p4/a1 post-guard | 774c24136a55cd5bf4ec9f518ef41af3b20e975f | sha256:0e82c6cc8930410001d2a65de5c73abe1f39811f0dd3e78476f8e09071aa8958 | sha256:522bdc090da5c0845990444194d25c916abe3ce8a34bc653093e1aba87e801fd |

La tabella usa gli image ID completi registrati nei
[metadata stress](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/metadata.json),
negli [images fault a1 post-guard](fault-p4-a1-guarded-r01/images.json) e negli
[images fault a4](fault-p4-a4-r01/images.json), oltre agli
[images fault p16/a16](fault-p16-a16-valid-r01/images.json). I manifest risolti delle
singole immagini base sono nei
[containers.json](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/queen-rust/fixed/r01/containers.json).
Non è presente un'attestazione SBOM che leghi crittograficamente commit e
immagini: la relazione è quella registrata dall'harness.

I commit c4059027/3655cd2a sono avanzamenti successivi alle campagne storiche:
la fixed primaria e il fault p4/a1 sono stati ripetuti sull'image post-guard
misurata, mentre auto, stress,
scaling e profili aggressivi restano sulle image indicate nelle rispettive
righe. Questo separa evidenza sperimentale, mitigazione derivata dal pilot e
verifica post-fix.

Il branch tracked termina ora a 774c2413, worktree clean; quel commit chiarisce
la semantica dell'override nel tooling e non è incorporato nell'app image
sha256:0e82c6cc…8958. Fixed e fault hanno metadata 3655cd2a; l'auto post-guard è
stata orchestrata successivamente con metadata 774c2413, riusando la stessa
image applicativa misurata.

La colonna “Broker image” riporta il digest Queen registrato dal metadata di
campagna, non tutti i backend della lane. Un audit separato dei
`containers.json` della campagna post-guard 3655cd2a e della fixed pre-guard
risolve Redis a
`sha256:02419de7eddf55aa5bcf49efb74e88fa8d931b4d77c07eff8a6b2144472b6952`
e PostgreSQL a
`sha256:38471f330eb885e04de130b768d6db4e10469e2311879c7e5c699f6d2d8a1c74`.
`campaign-stats` non usa questi digest come gate automatico: la provenienza va
letta per campagna, non come un unico digest implicito per tutti i 99 run.

### 3.2 Host locale

Tutte le campagne Laravel considerate sono state eseguite sullo stesso host:

| Campo | Valore |
| --- | --- |
| Host | macOS 26.5, arm64 |
| Runtime | Docker Desktop / Engine 29.7.2 |
| VM container | LinuxKit 7.0.12, aarch64 |
| CPU esposte | 10 |
| Memoria esposta | 8.214.851.584 byte |
| cgroup | v2 |
| Modalità | una lane misurata alla volta |

Il protocollo registra assenza di container estranei al freeze e inibizione
dello sleep macOS. I fault metadata confermano che non erano presenti
container preesistenti nei cinque round p4/a1.

Docker Desktop condivide il kernel tra i container, quindi i timestamp
monotonic sono confrontabili all'interno di un run. Non equivale però a Linux
nativo per scheduler, I/O o qualità dei contatori cgroup: per questo la
qualificazione resta diagnostic.

### 3.3 Provenienza del soak

Il [soak 24 h](../../../2026-08-11-soak24-1M/results.md) appartiene a un'altra
campagna:

- broker Queen 1.0.0, commit 074de2e; l'immagine si autodichiara beta.4 perché
  il bump di versione non modificava server/src;
- broker e PostgreSQL su queen-01: 32 vCPU Xeon Gold 6548N, 62 GiB, disco
  ext4 387 GB, fdatasync circa 70 µs;
- tre loader separati, ciascuno 16 vCPU e 31 GiB;
- VPC misurata a 10,1 Gbit/s per direzione.

Il report del soak non registra un digest OCI. Non va quindi attribuito alle
immagini della campagna Laravel.

## 4. Metodologia controllata e misure anti-bias

### 4.1 Regole predefinite

Il [protocollo](protocol.md) è stato congelato prima di ispezionare lo
screening. Stabiliva:

- fixed come confronto primario;
- stessi worker bounds e stesso job per tutte le lane;
- selezione del più piccolo setting entro il 5% del massimo valido;
- ACK batch maggiore di 1 classificato separatamente come aggressivo;
- correttezza, code, sampler, OOM e restart come gate, non come note;
- nessuna eliminazione post-hoc degli outlier;
- almeno tre repliche per cella principale, cinque dove possibile;
- stress high-volume e test auto sul profilo selezionato;
- fault test qualificato come diagnostico, senza claim exactly-once.

L'harness ruota l'ordine delle lane, ricrea backend e volumi per ogni campione
e conserva ogni run. Build e dependency install non si sovrappongono alla
misura.

La discovery iniziale era una griglia one-shot Queen Rust: un run per cella,
prefetch strict 1/4/8/16/32/64 e full-batch 4/8/16/32/64, con anchor iniziale
e finale. Tutte le celle in [screen](screen/01-anchor-start/20260829T004533Z-f2c39fd989/metadata.json)
e nel subset [screen-clean](screen-clean/02-p4-a1/20260829T005411Z-f2c39fd989/metadata.json)
registrano `metadata.git.dirty=true` e sono escluse dalle conferme e dai 99
run selezionati. L'anchor completion-span scende da
[133,28](screen/01-anchor-start/20260829T004533Z-f2c39fd989/report.json) a
[112,22 job/s](screen/12-anchor-end/20260829T005149Z-f2c39fd989/report.json),
pari a -15,8%, quindi la griglia non è evidenza quantitativa confermatoria.
Per la selezione iniziale, dopo il commit clean sono stati replicati soltanto
p4/a1 e p16/a1; p16/a16 è
stato confermato cross-engine ma non selezionato da una griglia clean
replicata. Il follow-up pianificato su partizioni 1/4/16/64 e pop fusion
off/on non è stato eseguito. P4/a1 è perciò la baseline prestazionale più
piccola fra i candidati replicati, non l'optimum globale né lo shipping default
safety dimostrato sullo SHA finale.

Il protocollo prescriveva 512 warm-up per la discovery. `select-p4`,
`select-p16` e il fixed safe storico sul commit 5b47a7d5 non registrano
`warmup_jobs` perché quella versione del runner non lo esponeva; il fixed
post-guard 3655cd2a
primario registra esplicitamente warm-up 0, mentre la conferma fixed aggressiva
registra 512. Ciò non rompe il pairing interno alle singole campagne, ma è una
deviazione dal protocollo e vieta un confronto causale safe↔aggressivo.

### 4.2 Budget e perimetro

| Componente | Budget per lane |
| --- | --- |
| Applicazione/supervisor/worker | 4 CPU, 1 GiB, pids 512 |
| Backend Horizon | Redis: 2 CPU, 2 GiB |
| Backend Queen | broker: 1 CPU, 1 GiB; PostgreSQL: 1 CPU, 1 GiB |
| Campionamento | 0,5 s nelle campagne riportate |

Il budget backend aggregato è quindi 2 CPU/2 GiB per entrambe le topologie.
Il produttore gira fuori dai cgroup misurati. Il result sink è un volume
comune, separato da Redis e PostgreSQL, per non accreditare a un backend il
costo di osservazione.

“Fuori dai cgroup misurati” non significa senza perturbazione: il producer ha
una quota di 4 CPU e condivide la stessa VM. Applicazione 4 CPU + backend 2 +
producer 4 saturano nominalmente le 10 CPU esposte, prima del sampler; nella
fixed post-guard 3655cd2a il dispatch Queen dura circa 1,8× quello Horizon. La contabilità
esclude correttamente il producer dai rapporti di risorsa, ma non può escludere
contention durante la finestra di dispatch.

Questa parità di budget non rende equivalenti le semantiche: Redis è volatile
nella fixture Horizon, mentre Queen usa PostgreSQL con synchronous_commit on.
Inoltre il Compose locale monta /var/lib/postgresql/data su tmpfs da 768 MiB:
synchronous_commit governa il commit nel database, ma qui non misura la
persistenza su storage fisico. I rapporti descrivono CPU e memoria della
topologia scelta, non un confronto a durabilità o I/O production equivalenti.

### 4.3 Gate di validità

Un run è eleggibile solo se:

- il set di job completati è esattamente quello atteso;
- missing, duplicati, failed e job inattesi sono zero;
- la coda è stabile a zero, incluse ready, reserved e delayed quando esposte;
- sampler e copertura PSS sono validi;
- non vi sono eventi OOM nel sampler;
- configurazione, workload, risorse e sampling coincidono con la coppia
  baseline/candidato.

Il campaign aggregator associa profile+rXX e confronta una canonical key che
comprende workload, worker bounds, dispatch mode/batch, knob Queen, transport,
campionamento e limiti risolti. Le differenze attese di engine, connessione e
topologia backend sono consentite; ogni altra differenza sopprime il rapporto.

Nei sei gruppi selezionati fixed/auto post-guard, legacy, CPU e select p4/p16
risultano 48/48 run validi, 336.000/336.000 job,
48/48 code quiescenti, nessun missing, duplicato o failure, nessun errore
sampler e nessun OOM. Lo stress a1 aggiunge 9/9 run e 450.000 job; scaling
w1/w8 aggiunge 18/18 run e 54.000 job; la discovery a4 aggiunge 6/6 run e
300.000 job; le conferme fixed/auto p16/a16 aggiungono 18/18 run e 108.000
job. Il totale selezionato è quindi **99 run e 1.248.000 job misurati**, tutti
validi e quiescenti. Fixed e auto post-guard sostituiscono uno-a-uno le vecchie
campagne pre-guard (24 run/204.000 job), mantenute come corroborazione
ridondante e non sommate alla matrice. Il lordo di tutti gli artifact
prestazionali riusciti e preservati, incluse conferme ridondanti e smoke, è
**132 lane e 1.602.360 job**: corpus 99 + fixed storica 15 + auto storica 9 +
stress-v2 3 + post-streaming smoke 3 + post-lease smoke 3. Esclude lo smoke iniziale e il post-freeze smoke (6
lane/360 job), gli screening dirty, l'observer failure e i fault. È una
selezione di artifact riusciti esplicitamente enumerata; serve alla trasparenza
di provenienza e non aumenta n.

Il conteggio dei job misura volume e correttezza, non aumenta artificialmente
la numerosità statistica: l'unità indipendente resta il run, n=3 o n=5 per
cella confermatoria.

Il corpus selezionato di 99 run esclude i warm-up, la prima esecuzione stress
fallita, `screen/` e `screen-clean/` dirty, post-freeze smoke, la conferma
stress-v2 a una ripetizione per engine e altri
screening/rerun ridondanti non inclusi nella matrice. I fault test sono
conteggiati a parte: 35 lane e 840 job unici, at-least-once 35/35 e strict
19/35, con 24 duplicati osservati sotto p4/a4 o p16/a16. Le due lane del test
negativo p16/a16 con lease insufficiente sono preservate ma escluse da questi
totali. I 15 fault p4/a1 post-guard sostituiscono uno-a-uno i 15 round
pre-guard, mantenuti come corroborazione ridondante. L'inventario fault lordo
preservato è quindi 50 lane/1.200 job unici, 50/50 at-least-once, 34/50 strict
e 24 duplicati; il pilot negativo da due lane/48 job resta separato.

Il campaign aggregator non usa RestartCount come gate automatico. Un audit
separato degli artifact containers.json preservati trova comunque
RestartCount=0 e OOMKilled=false in tutti i 99 run. “Zero restart” è quindi
un risultato verificato sugli artifact container, non una proprietà
fail-closed dell'aggregatore.

La fixed primaria è stata ripetuta sul commit post-guard misurato 3655cd2a con timeout 120 s,
prefetch 4 e retry_after 481 s, quindi supera il guard `>480 s`: 15/15 run
validi e quiescenti. La fixed precedente con retry_after 180 s resta
corroborazione pre-guard; le campagne p16 richiederebbero oggi >1.920 s e
restano evidenza storica sulle image dichiarate. Lo smoke n=1 per engine non è
usato per inferenza; la campagna post-guard n=5 è il risultato prestazionale
primario. Anche il fault p4/a1 è stato ripetuto post-guard con retry_after 41
s, ma timeout/retry e worker count differiscono dal profilo prestazionale a
481 s: chiude la safety dei knob, non il suo tempo di recovery esatto.

### 4.4 Statistica

Gli aggregati assoluti riportano n, mediana, quartili R-7 e IQR. I confronti
sono rapporti candidato/Horizon per la stessa ripetizione. Il CI 95% è un
percentile bootstrap deterministico della mediana dei rapporti paired:

| Parametro | Valore |
| --- | --- |
| Seed globale | 20260829 |
| Resample | 10.000 |
| Seed per cella | primi 64 bit di SHA-256(seed, profile, engine, metrica) |
| Rapporto | candidato / Horizon |

Per throughput un rapporto maggiore di 1 è favorevole; per latenza, CPU e
memoria è favorevole un rapporto minore di 1. Con n=3 il bootstrap può
riprodurre soprattutto i valori osservati e non giustifica inferenze ampie.
Gli intervalli quantificano la variabilità dei rapporti paired osservati su
questo host, non una garanzia o inferenza di popolazione e non un test di
significatività. Non è applicata correzione per confronti multipli.

### 4.5 Definizioni operative

- **Completion-span throughput:** job completati divisi per l'intervallo fra
  prima e ultima completion; è la metrica primaria del drain.
- **Headline throughput:** dispatch-to-last-completion; include la finestra del
  produttore.
- **E2E p95/p99:** timestamp monotonic di enqueue e completion nel sink comune;
  include arrival shape e durata del dispatch dello scenario e non isola la
  latenza broker/service. I confronti paired mantengono uguali dispatch mode e
  batch; dispatch throughput e headline throughput sono guardrail obbligatori
  per interpretare la metrica.
- **CPU orchestratore:** CPU dei processi supervisor/master identificati.
- **PSS orchestratore:** picco PSS di quei processi; non include worker.
- **CPU/RAM stack:** cgroup applicazione consumer più backend completo, usando
  delta CPU cgroup e picco memory.current; producer, observer e sampler sono
  esclusi.
- **Worker-second:** integrale del numero di worker osservato nel profilo auto.

## 5. Matrice delle campagne

| Campagna | Engine | Profilo | Run/engine | Job/run | Forma lavoro | Dispatch | Queen |
| --- | --- | --- | ---: | ---: | --- | --- | --- |
| [fixed post-guard 3655](confirm-fixed-safe-head/20260829T035034Z-3655cd2a63/campaign-stats.json) | 3 | fixed 4 | 5 | 10.000 | sleep 10 ms, warm-up 0 | single | p4/a1, retry 481 s, 64, fusion off |
| [legacy](confirm-fixed-legacy/20260829T014350Z-5b47a7d579/campaign-stats.json) | 3 | fixed 4 | 3 | 6.000 | sleep 10 ms | single | p1/a1, 64, fusion off |
| [auto p4/a1 post-guard](confirm-auto-safe-head/20260829T042032Z-774c24136a/campaign-stats.json) | 3 | auto 1…4 | 3 | 6.000 | sleep 10 ms | single | p4/a1, retry 481 s, 64, fusion off |
| [fixed aggressivo](confirm-fixed-aggressive/20260829T031618Z-327849a1ed/campaign-stats.json) | 3 | fixed 4 | 3 | 6.000 | sleep 10 ms, warm-up 512 | single | p16/a16, 64, fusion off |
| [auto aggressivo](confirm-auto-aggressive/20260829T032231Z-327849a1ed/campaign-stats.json) | 3 | auto 1…4 | 3 | 6.000 | sleep 10 ms | single | p16/a16, 64, fusion off |
| [CPU safe](confirm-cpu-safe/20260829T020132Z-5b47a7d579/campaign-stats.json) | 3 | fixed 4 | 3 | 6.000 | 25.000 iterazioni | single | p4/a1, 64, fusion off |
| [select p4](select-p4/20260829T012603Z-5b47a7d579/campaign-stats.json) | Rust | fixed 4 | 3 | 4.000 | sleep 10 ms | single | p4/a1 |
| [select p16](select-p16/20260829T012742Z-5b47a7d579/campaign-stats.json) | Rust | fixed 4 | 3 | 4.000 | sleep 10 ms | single | p16/a1 |
| [scale w1](scale-w1-safe/20260829T024935Z-2a4b107a06/campaign-stats.json) | 3 | fixed 1 | 3 | 3.000 | sleep 10 ms, warm-up 256 | single | p4/a1 |
| [scale w8](scale-w8-safe/20260829T030004Z-2a4b107a06/campaign-stats.json) | 3 | fixed 8 | 3 | 3.000 | sleep 10 ms, warm-up 256 | single | p4/a1 |
| [stress 50k](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/campaign-stats.json) | 3 | fixed 4 | 3 | 50.000 | zero-work | bulk 100 | p4/a1, 64, fusion off |
| [discovery ACK4](discovery-ack4-stress/20260829T030358Z-2a4b107a06/campaign-stats.json) | PHP/Rust | fixed 4 | 3 | 50.000 | zero-work | bulk 100 | p4/a4, 64, fusion off |

## 6. Risultati fixed primari

Il confronto primario è la campagna
[confirm-fixed-safe-head](confirm-fixed-safe-head/20260829T035034Z-3655cd2a63/campaign-stats.json):
commit clean 3655cd2a, app image sha256:0e82c6cc…8958, quattro worker,
10.000 job per run, sleep 10 ms, warm-up 0, dispatch single, prefetch 4, ACK
batch 1, timeout 120 s e retry_after 481 s. Sono validi e quiescenti tutti i
15/15 run, cioè cinque ripetizioni per engine e 150.000/150.000 job unici.
L'audit dei 40 container snapshot trova zero restart/OOM; app, backend e stack
riportano zero throttling in tutti i run.

Mediana [Q1–Q3]:

| Metrica | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion job/s | 253,38 [252,37–253,51] | 289,17 [288,40–290,01] | 289,13 [285,03–292,61] |
| Headline job/s | 243,66 [242,84–243,67] | 288,98 [288,19–289,81] | 288,94 [284,84–292,39] |
| Dispatch job/s | 6.322,41 [6.306,81–6.332,39] | 3.568,46 [3.548,47–3.573,32] | 3.530,46 [3.525,64–3.545,22] |
| E2E p95 | 37.547 ms [37.530–37.706] | 30.403 ms [30.300–30.438] | 30.067 ms [30.015–30.355] |
| E2E p99 | 39.078 ms [39.010–39.216] | 31.499 ms [31.398–31.541] | 31.208 ms [31.179–31.605] |
| CPU orchestratore | 0,14447 s [0,14172–0,14547] | 0,03201 s [0,03095–0,03247] | 0,01409 s [0,01379–0,01426] |
| PSS orchestratore | 71,05 MiB | 35,15 MiB | 2,87 MiB |
| CPU worker | 21,461 s | 5,376 s | 5,195 s |
| CPU backend | 5,607 s | 11,061 s | 11,132 s |
| CPU stack | 27,191 s [27,135–27,370] | 16,563 s [16,471–16,637] | 16,423 s [16,339–16,439] |
| RAM stack | 217,66 MiB | 338,23 MiB | 303,63 MiB |

La CPU worker Queen più bassa non va letta isolatamente: Queen sposta più
lavoro nel broker+PostgreSQL, come mostra la CPU backend. La CPU stack è la
vista corretta per il costo operativo complessivo.

Rapporti paired candidato/Horizon, mediana [bootstrap CI 95%]:

| Metrica | Queen PHP | Queen Rust |
| --- | ---: | ---: |
| Completion-span | 1,14276 [1,13453–1,15147] | 1,14049 [1,12052–1,16322] |
| Headline | 1,18675 [1,18023–1,19656] | 1,18577 [1,16440–1,20805] |
| Dispatch | 0,56429 [0,55205–0,58451] | 0,56074 [0,55610–0,57749] |
| E2E p95 | 0,80964 [0,80357–0,81408] | 0,80370 [0,79740–0,82212] |
| E2E p99 | 0,80349 [0,80131–0,81448] | 0,80254 [0,79371–0,81016] |
| CPU orchestratore | 0,21838 [0,20800–0,22691] | 0,09874 [0,09022–0,10109] |
| PSS orchestratore | 0,49480 [0,49477–0,49671] | 0,04045 [0,04043–0,04051] |
| CPU stack | 0,60573 [0,59332–0,61312] | 0,60062 [0,59159–0,60399] |
| RAM stack | 1,55398 [1,52163–1,56203] | 1,39299 [1,38927–1,42009] |

Interpretazione per Queen Rust:

- throughput +14,05%, con intervallo paired interamente sopra 1;
- p95 -19,63%;
- CPU orchestratore -90,13% e PSS -95,95%;
- CPU stack -39,94%;
- RAM stack +39,30%.

Il vantaggio Rust è dunque molto forte nel control plane, ma la memoria
PostgreSQL resta una voce reale del deployment.

La riduzione CPU orchestratore del 90,13% va letta anche in assoluto:
0,14447→0,01409 s su un drain di circa 39,5 s, cioè circa 130 ms risparmiati.
La percentuale è grande su una base piccola; PSS del control plane e CPU dello
stack consumer+backend sono più materiali per il dimensionamento. Il dispatch
Horizon è circa 1,8× più rapido, mentre headline e completion favoriscono
Queen: poiché il produttore è fuori dai cgroup, dispatch e headline restano
guardrail, non una spiegazione causale del risparmio misurato.

I rapporti completion Queen sono maggiori di 1 in tutti i 5/5 paired run su
questo host. Con soli cinque segni concordi, il sign test esatto bilaterale è
p=0,0625: l'intervallo bootstrap resta descrittivo dei rapporti osservati e non
viene presentato come significatività statistica di popolazione.

## 7. Ricerca della configurazione

### 7.1 Legacy p1/a1 contro p4/a1

| Engine | Legacy p1/a1, job/s | P4/a1 pre-guard, job/s | p4/p1 |
| --- | ---: | ---: | ---: |
| Horizon | 261,85 | 261,46 | 0,9985 |
| Queen PHP | 118,15 | 290,87 | 2,4619 |
| Queen Rust | 121,39 | 290,64 | 2,3942 |

Nel legacy paired contro Horizon:

| Candidato | Completion ratio, CI 95% | p95 ratio, CI 95% |
| --- | ---: | ---: |
| Queen PHP | 0,4512 [0,4265–0,4648] | 2,1774 [2,0908–2,2811] |
| Queen Rust | 0,4628 [0,4429–0,4643] | 2,1110 [2,0903–2,2120] |

Il dato spiega l'impressione iniziale che Queen fosse più lenta: quella
conclusione era corretta per p1/a1, ma non per il client ottimizzato.
Nel legacy p1/a1 anche il costo dello stack peggiora: Rust/Horizon è 1,5759 per
CPU e 1,5117 per RAM (PHP 1,6097 e 1,6303). P1 è quindi il default safety, non
un profilo già competitivo per throughput o costo consumer+backend.

Il confronto p1→p4 usa campagne separate con 6.000/10.000 job e n=3/n=5, quindi
non ha paired CI e non prova da solo causalità. Il controllo Horizon varia solo
-0,15%, mentre le due implementazioni Queen crescono del 139–146%: è una
evidenza direzionale forte e coerente con il prefetch come collo di bottiglia.
Entrambe le colonne della tabella usano commit 5b47a7d5 e precedono il guard:
servono a localizzare storicamente il collo di bottiglia, non sono il risultato
primario. La campagna post-guard 3655cd2a con lease ammessa conferma separatamente p4/a1 a
289,17/289,13 job/s PHP/Rust contro 253,38 Horizon.

### 7.2 Prefetch 4 contro 16

I due screening seguenti includono solo Queen Rust e sono campagne separate,
entrambe con n=3 e 4.000 job:

| Metrica | p4/a1 | p16/a1 | p16/p4 |
| --- | ---: | ---: | ---: |
| Completion job/s | 292,026 | 290,901 | -0,39% |
| Headline job/s | 285,008 | 283,235 | -0,62% |
| E2E p95 | 12.343 ms | 12.314 ms | -0,23% |
| E2E p99 | 12.764 ms | 12.816 ms | +0,41% |
| CPU orchestratore | 0,00817 s | 0,00806 s | -1,32% |
| PSS orchestratore | 2,87 MiB | 2,86 MiB | -0,10% |
| CPU worker | 2,251 s | 2,659 s | **+18,13%** |
| CPU stack | 6,399 s | 6,503 s | +1,63% |
| RAM stack | 287,83 MiB | 289,82 MiB | +0,69% |

P16 non aumenta il throughput e costa più CPU worker. In applicazione della
regola predefinita del minimo setting entro il 5% del massimo, p4 è il profilo
prestazionale più piccolo fra i candidati replicati. Non è però il default
safety universale: quello resta p1. ACK batch resta 1, quindi non è stato
scambiato un vantaggio apparente con una finestra di ACK differito più ampia.

### 7.3 Scaling fixed a 1 e 8 worker

Le campagne w1 e w8 condividono commit 2a4b107a, app image
sha256:9386d3716853a6f9d09c9e455ce9c95326a01516655747fa785566543b7ccf6c,
3.000 job da 10 ms, warm-up 256, p4/a1 e n=3. Tutti i 18 run sono validi,
quiescenti e configuration-consistent.

Mediane assolute:

| Worker | Engine | Completion job/s | Headline job/s | E2E p95 | E2E p99 | CPU stack | RAM stack |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Horizon | 61,25 | 60,49 | 46.757 ms | 48.631 ms | 11,040 s | 111,41 MiB |
| 1 | Queen PHP | 62,58 | 62,25 | 45.309 ms | 46.960 ms | 12,887 s | 222,18 MiB |
| 1 | Queen Rust | 62,43 | 62,07 | 45.389 ms | 47.124 ms | 13,018 s | 193,27 MiB |
| 8 | Horizon | 546,08 | 508,32 | 5.153 ms | 5.357 ms | 5,125 s | 319,45 MiB |
| 8 | Queen PHP | 503,02 | 494,91 | 4.769 ms | 4.921 ms | 3,928 s | 447,76 MiB |
| 8 | Queen Rust | 513,37 | 463,85 | 4.610 ms | 4.864 ms | 4,242 s | 440,03 MiB |

Rapporti paired candidato/Horizon:

| Worker | Metrica | Queen PHP, mediana [CI 95%] | Queen Rust, mediana [CI 95%] |
| ---: | --- | ---: | ---: |
| 1 | Completion | 1,0216 [1,0167–1,0299] | 1,0192 [1,0112–1,0213] |
| 1 | E2E p95 | 0,9690 [0,9689–0,9706] | 0,9701 [0,9693–0,9830] |
| 1 | CPU stack | 1,1636 [1,1489–1,1856] | 1,1737 [1,1692–1,2111] |
| 1 | RAM stack | 1,9968 [1,9944–1,9968] | 1,7312 [1,7259–1,7389] |
| 8 | Completion | 0,9210 [0,9063–1,0662] | 0,9400 [0,8565–1,0648] |
| 8 | E2E p95 | 0,9055 [0,8012–0,9515] | 0,8946 [0,7985–0,9993] |
| 8 | CPU stack | 0,7690 [0,7429–0,8218] | 0,8304 [0,7615–0,9887] |
| 8 | RAM stack | 1,4017 [1,3858–1,4132] | 1,3775 [1,3109–1,3823] |

A un worker Queen è circa il 2% più veloce, con CI interamente sopra 1, ma
spende 16–17% in più di CPU stack e 73–100% in più di RAM stack. A otto worker
le mediane completion favoriscono Horizon del 6–8%, ma i CI attraversano 1:
n=3 e la variabilità Queen impediscono una conclusione di throughput. Le p95 e
la CPU stack favoriscono invece Queen nei tre paired run, mentre la RAM resta
38–40% più alta.

Il rapporto descrittivo w8/w1 è 8,92× Horizon, 8,04× Queen PHP e 8,22× Queen
Rust. È un rapporto fra campagne separate, senza pairing né bootstrap; non va
interpretato come efficienza lineare garantita.

Il w4 primario non è una cella matched: usa 10.000 job, warm-up 0, n=5, commit
3655cd2a e app image sha256:0e82c6cc…8958. Non viene inserito artificialmente
fra w1 e w8 per costruire una curva 1→4→8. Serve un nuovo w4 da 3.000 job sul
commit e immagine delle due campagne scaling.

### 7.4 Conferma fixed aggressiva p16/a16

La campagna
[confirm-fixed-aggressive](confirm-fixed-aggressive/20260829T031618Z-327849a1ed/campaign-stats.json)
usa commit 327849a1, app image sha256:b631f6f6…cc21d, quattro worker,
6.000 job da 10 ms, warm-up 512, dispatch single, prefetch 16 e ACK batch 16.
Tutti i 9/9 run sono validi, quiescenti e configuration-consistent; nessun
container ha restart/OOM e il throttling CPU mediano app/backend è zero.

Mediane assolute:

| Metrica | Horizon | Queen PHP p16/a16 | Queen Rust p16/a16 |
| --- | ---: | ---: | ---: |
| Completion job/s | 255,96 | 295,91 | 293,85 |
| Q1–Q3 completion | 255,94–259,43 | 289,64–297,79 | 285,49–298,87 |
| Headline job/s | 252,12 | 290,82 | 291,69 |
| E2E p95 | 21.669 ms | 18.437 ms | 18.179 ms |
| E2E p99 | 22.565 ms | 18.878 ms | 18.711 ms |
| CPU orchestratore | 0,05843 s | 0,02704 s | 0,01093 s |
| PSS orchestratore | 65,23 MiB | 35,15 MiB | 2,87 MiB |
| CPU worker | 12,278 s | 2,945 s | 2,828 s |
| CPU backend | 3,262 s | 3,795 s | 3,843 s |
| CPU stack | 15,600 s | 6,813 s | 6,685 s |
| RAM stack | 209,34 MiB | 319,98 MiB | 289,79 MiB |

Rapporti paired candidato/Horizon, mediana [bootstrap CI 95%]:

| Metrica | Queen PHP p16/a16 | Queen Rust p16/a16 |
| --- | ---: | ---: |
| Completion | 1,1563 [1,0778–1,1708] | 1,1482 [1,0827–1,1559] |
| Headline | 1,1536 [1,0870–1,1764] | 1,1571 [1,0870–1,1640] |
| E2E p95 | 0,8508 [0,8318–0,9016] | 0,8509 [0,8389–0,9002] |
| E2E p99 | 0,8364 [0,8198–0,8921] | 0,8301 [0,8290–0,8806] |
| CPU orchestratore | 0,4632 [0,4357–0,4693] | 0,1831 [0,1778–0,2210] |
| PSS orchestratore | 0,5388 [0,5388–0,5403] | 0,0440 [0,0438–0,0441] |
| CPU stack | 0,4370 [0,4227–0,4670] | 0,4416 [0,4249–0,4434] |
| RAM stack | 1,5303 [1,5212–1,5381] | 1,3838 [1,3686–1,3843] |

Nel fixed aggressivo Queen è 14,8–15,6% sopra Horizon, riduce p95 di circa il
15% e CPU stack di circa il 56%; RAM stack resta 38–53% più alta. Rust e PHP
hanno throughput simile, mentre Rust conserva il vantaggio del control plane.

Non è un A/B causale contro il fixed safe p4/a1: cambiano commit, app image,
warm-up, job count e n. P16/a16 varia contemporaneamente prefetch e ACK batch;
il precedente screening p16/a1 non mostrava un vantaggio su p4/a1, ma ciò non
isola quantitativamente l'effetto ACK16 fra queste campagne.

Il fault diretto p16/a16 successivo usa gli stessi knob prefetch/ACK, ma non
l'intero preset steady-state: due worker, job da 500 ms e retry_after 12 s
contro quattro worker, 10 ms e retry_after 180 s. Passa at-least-once 10/10 ma
strict solo 1/10. Insieme alle sette failure strict p4/a4 rende prudente
classificare ogni ACK batching come opt-in; i workload diversi impediscono un
confronto causale dei tassi di duplicazione.

## 8. Autoscaling

La campagna
[auto p4/a1 post-guard](confirm-auto-safe-head/20260829T042032Z-774c24136a/campaign-stats.json)
usa metadata clean 774c2413, app image sha256:0e82c6cc…8958, strategy size,
min 1, max 4, cooldown 3 s, max shift 1, retry_after 481 s e 6.000 job da 10
ms. I 9/9 run sono validi, quiescenti e configuration-consistent; 24 snapshot
container hanno zero restart/OOM e app/backend/stack zero throttling in ogni
run. Gli algoritmi rispettano gli stessi limiti ma non sono semanticamente
identici: Horizon tende ad allocare il massimo a una coda attiva; Queen calcola
la richiesta da backlog e target.

### 8.1 Prestazioni

| Metrica mediana | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion job/s | 243,75 | 234,13 | 235,69 |
| Headline job/s | 228,61 | 233,93 | 235,46 |
| E2E p95 | 24.173 ms | 23.362 ms | 22.932 ms |
| E2E p99 | 25.082 ms | 23.795 ms | 23.703 ms |
| CPU orchestratore | 0,13549 s | 0,04332 s | 0,02010 s |
| PSS orchestratore | 76,93 MiB | 40,35 MiB | 3,04 MiB |
| CPU worker | 13,231 s | 4,300 s | 4,183 s |
| CPU backend | 3,520 s | 7,639 s | 7,738 s |
| CPU stack | 16,915 s | 12,004 s | 12,007 s |
| RAM stack | 208,00 MiB | 323,81 MiB | 295,33 MiB |

Rapporti paired candidato/Horizon, mediana [bootstrap CI 95%]:

| Metrica | Queen PHP | Queen Rust |
| --- | ---: | ---: |
| Completion | 0,9500 [0,9430–0,9605] | 0,9669 [0,9360–0,9714] |
| Headline | 1,0106 [1,0035–1,0233] | 1,0300 [0,9961–1,0334] |
| E2E p95 | 0,9720 [0,9665–0,9768] | 0,9487 [0,9460–0,9849] |
| E2E p99 | 0,9571 [0,9486–0,9733] | 0,9450 [0,9413–0,9758] |
| CPU orchestratore | 0,3197 [0,3196–0,3374] | 0,1486 [0,1419–0,1518] |
| PSS orchestratore | 0,5245 [0,5245–0,5254] | 0,0396 [0,0396–0,0396] |
| CPU stack | 0,7124 [0,7071–0,7375] | 0,7009 [0,6813–0,7575] |
| RAM stack | 1,5579 [1,5419–1,5857] | 1,4209 [1,3946–1,4620] |

Queen PHP/Rust hanno completion-span inferiore del 5,0%/3,3%, ma headline
mediano superiore dell'1,1%/3,0% e p95 inferiore del 2,8%/5,1%. Rust riduce
CPU stack del 29,9% e PSS orchestratore del 96,0%, con RAM stack +42,1%.
Completion è sotto Horizon in tutti i 3/3 paired run per entrambe le lane; il
sign test bilaterale con n=3 vale p=0,25. Il completion span parte dalla prima
completion e risponde in modo diverso alla rampa: vanno pubblicati insieme a
headline e dispatch guardrail, senza claim di significatività.

### 8.2 Dinamica dei worker

Mediane sui tre run, da campioni ogni 0,5 s:

| Engine | Primo worker rilevato | Tempo al picco | Primo downscale | Worker-second | Picco/finale | Ritorno al minimo post-completion |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Horizon | 1,654 s | 4,653 s | 30,635 s | 127,27 | 0/4/1 | non disponibile |
| Queen PHP | 0,141 s | 8,644 s | 27,135 s | 111,53 | 1/4/1 | 7,497 s |
| Queen Rust | 0,130 s | 8,631 s | 27,152 s | 111,53 | 1/4/1 | 7,663 s |

Queen raggiunge il massimo circa 4 s dopo Horizon ma usa circa il 12,4% in meno
di worker-second. Per Horizon il sampler vede initial_workers=0 e
final_workers=1, perciò non produce un ritorno allo stesso minimo iniziale:
non va trasformato artificialmente in zero secondi. Questi sono tempi da
sampling, non eventi emessi dal controller; l'incertezza è almeno dell'ordine
dell'intervallo di 0,5 s.

### 8.3 Conferma auto aggressiva p16/a16

La campagna
[confirm-auto-aggressive](confirm-auto-aggressive/20260829T032231Z-327849a1ed/campaign-stats.json)
usa gli stessi limiti auto 1…4, cooldown 3 s e max shift 1, ma porta Queen a
prefetch 16 e ACK batch 16. I 9/9 run sono validi, quiescenti e coerenti:
54.000/54.000 job unici completati, zero missing, duplicati o failed. Il
controllo separato dei 24 container registra RestartCount=0 e OOMKilled=false;
le mediane di throttling CPU applicazione/backend sono zero.

| Metrica mediana | Horizon | Queen PHP p16/a16 | Queen Rust p16/a16 |
| --- | ---: | ---: | ---: |
| Completion job/s | 245,22 | 240,47 | 244,96 |
| Headline job/s | 230,06 | 240,26 | 244,72 |
| E2E p95 | 24.031 ms | 22.961 ms | 22.352 ms |
| E2E p99 | 24.923 ms | 23.285 ms | 22.768 ms |
| CPU orchestratore | 0,13558 s | 0,04885 s | 0,02173 s |
| PSS orchestratore | 76,93 MiB | 40,35 MiB | 3,03 MiB |
| CPU worker | 13,158 s | 3,349 s | 3,261 s |
| CPU stack | 16,787 s | 7,712 s | 7,556 s |
| RAM stack | 207,82 MiB | 315,91 MiB | 285,26 MiB |

| Rapporto paired candidato/Horizon | Queen PHP p16/a16 | Queen Rust p16/a16 |
| --- | ---: | ---: |
| Completion | 0,9836 [0,9574–0,9941] | 0,9990 [0,9963–1,0067] |
| Headline | 1,0473 [1,0201–1,0587] | 1,0637 [1,0615–1,0719] |
| E2E p95 | 0,9532 [0,9322–0,9781] | 0,9301 [0,9270–0,9335] |
| E2E p99 | 0,9320 [0,9112–0,9554] | 0,9113 [0,9096–0,9201] |
| CPU orchestratore | 0,3593 [0,3561–0,3685] | 0,1587 [0,1551–0,1603] |
| PSS orchestratore | 0,5245 [0,5244–0,5245] | 0,0394 [0,0394–0,0395] |
| CPU stack | 0,4578 [0,4568–0,4759] | 0,4516 [0,4501–0,4570] |
| RAM stack | 1,5206 [1,5197–1,5206] | 1,3726 [1,3695–1,3727] |

Sul completion span PHP è 1,6% sotto Horizon, con CI interamente sotto 1;
Rust è sostanzialmente in parità e il CI attraversa 1. Headline e latenza
favoriscono invece entrambe le lane Queen: +4,7%/-4,7% p95 per PHP e
+6,4%/-7,0% p95 per Rust. Rust riduce CPU dell'orchestratore dell'84,1%, PSS
del 96,1% e CPU stack del 54,8%, ma la RAM stack cresce del 37,3%; per PHP la
CPU stack scende del 54,2% e la RAM cresce del 52,1%.

| Engine | Primo worker rilevato | Tempo al picco | Primo downscale | Worker-second | Iniziale/picco/finale | Ritorno al minimo post-completion |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Horizon | 1,644 s | 4,645 s | 30,653 s | 127,28 | 0/4/1 | non disponibile |
| Queen PHP | 0,127 s | 8,628 s | 27,135 s | 111,53 | 1/4/1 | 8,151 s |
| Queen Rust | 0,138 s | 8,641 s | 28,652 s | 112,03 | 1/4/1 | 10,125 s |

Queen usa il 12,4% (PHP) e il 12,0% (Rust) di worker-second in meno. Anche qui
il dato deriva da polling ogni 0,5 s: lo zero iniziale di Horizon è un artefatto
della prima osservazione, non l'assenza di un worker, e il ritorno nullo non
indica mancato scale-down.

Il confronto con l'auto safe p4/a1 è solo direzionale: campagna, commit e app
image sono diversi e i run non sono interleaved. Non attribuiamo quindi a
prefetch/ACK16 la differenza rispetto al safe. I run steady-state puliti non
provano da soli la fault safety di p16/a16; la valutazione crash con gli stessi
knob e workload diverso è trattata nella sezione 12.

## 9. Workload CPU-bound

La campagna usa quattro worker, 6.000 job, sleep 0 e 25.000 iterazioni CPU.

| Metrica mediana | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion job/s | 359,49 | 365,02 | 364,05 |
| E2E p95 | 14.681 ms | 13.778 ms | 14.047 ms |
| CPU orchestratore | 0,02998 s | 0,01808 s | 0,00755 s |
| PSS orchestratore | 65,04 MiB | 35,15 MiB | 2,86 MiB |
| CPU stack | 65,902 s | 67,237 s | 67,160 s |
| RAM stack | 209,48 MiB | 325,03 MiB | 292,64 MiB |

| Rapporto candidato/Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: |
| Completion, CI 95% | 1,01249 [0,92743–1,01539] | 1,01173 [1,00948–1,01634] |
| E2E p95, CI 95% | 0,94140 [0,92086–0,94362] | 0,96204 [0,95041–0,96648] |
| CPU orchestratore, CI 95% | 0,59828 [0,56988–0,60304] | 0,25737 [0,24906–0,26705] |
| CPU stack, CI 95% | 1,02027 [1,01985–1,11210] | 1,01909 [1,01313–1,02093] |
| RAM stack, CI 95% | 1,55282 [1,53852–1,65555] | 1,39700 [1,38888–1,40284] |

Il CI throughput PHP è ampio e attraversa 1, a causa della variabilità di uno
dei tre paired run. Per Rust l'effetto è piccolo ma coerente nei campioni. In
entrambi i casi la CPU stack è circa il 2% più alta: quando il job applicativo
domina, il vantaggio di Queen non va raccontato come risparmio CPU end-to-end.
Il motivo operativo per Rust resta il control plane molto più leggero.

## 10. Stress 50k confermato

### 10.1 Configurazione e gate

La campagna
[stress-50k-safe-confirm](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/campaign-stats.json)
usa:

- tre engine, tre ripetizioni ciascuno;
- fixed 4 worker, 50.000 job per run;
- sleep 0, CPU iterations 0, nessun warm-up;
- dispatch Laravel bulk, chunk 100;
- Queen p4/a1, 64 partizioni, pop fusion off;
- sample 0,5 s, post-drain 3 s, timeout 1.200 s.

Tutti i 9/9 run hanno 50.000/50.000 job unici, max attempt 1, zero missing,
duplicati, failed e inattesi. Tutte le code finali hanno
size=ready=reserved=delayed=0; sampler integrity_errors=0, OOM events=0,
copertura PSS orchestrator/worker=1,0; container restart=0 e OOMKilled=false.

### 10.2 Mediane assolute

| Metrica | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion job/s | 841,97 | 2.042,17 | 2.007,13 |
| Headline job/s | 820,33 | 2.039,59 | 2.004,57 |
| Dispatch job/s | 6.998,07 | 4.508,05 | 4.602,23 |
| E2E p95 | 49.799 ms | 11.571 ms | 11.990 ms |
| E2E p99 | 51.632 ms | 12.690 ms | 13.085 ms |
| CPU orchestratore | 0,27398 s | 0,01704 s | 0,00813 s |
| PSS orchestratore | 71,05 MiB | 35,15 MiB | 2,86 MiB |
| RSS orchestratore | 96,01 MiB | 49,98 MiB | 4,06 MiB |
| CPU worker | 26,174 s | 11,286 s | 11,266 s |
| CPU applicazione | 26,486 s | 11,303 s | 11,278 s |
| RAM applicazione, max sampled | 205,41 MiB | 186,58 MiB | 155,59 MiB |
| CPU backend | 8,491 s | 26,619 s | 27,077 s |
| RAM backend, max sampled | 130,03 MiB | 281,52 MiB | 276,77 MiB |
| CPU stack | 34,977 s | 37,923 s | 38,355 s |
| RAM stack | 320,55 MiB | 467,98 MiB | 432,24 MiB |

Il produttore Horizon pubblica più rapidamente, ma il drain è più lento. Poiché
il produttore è fuori dai cgroup misurati, dispatch throughput va letto come
guardrail e non come risparmio di risorse della lane.

### 10.3 Rapporti paired

Mediana [bootstrap CI 95%], tre coppie eleggibili su tre:

| Metrica | Queen PHP / Horizon | Queen Rust / Horizon |
| --- | ---: | ---: |
| Completion-span | 2,42604 [2,17922–2,50006] | 2,38075 [2,18877–2,49219] |
| Headline | 2,48836 [2,23958–2,56267] | 2,44057 [2,24956–2,55609] |
| Dispatch | 0,64464 [0,59967–0,69889] | 0,64479 [0,61564–0,71625] |
| E2E p95 | 0,23236 [0,21667–0,25704] | 0,23837 [0,22473–0,26468] |
| E2E p99 | 0,24578 [0,23365–0,27522] | 0,25082 [0,23749–0,28160] |
| CPU orchestratore | 0,05993 [0,05732–0,06431] | 0,02770 [0,02762–0,02969] |
| PSS orchestratore | 0,49475 [0,49475–0,49480] | 0,04030 [0,04029–0,04034] |
| RSS orchestratore | 0,52051 [0,51936–0,52057] | 0,04231 [0,04221–0,04235] |
| CPU worker | 0,43118 [0,40898–0,43209] | 0,43110 [0,40342–0,43163] |
| CPU applicazione | 0,42677 [0,40552–0,42813] | 0,42646 [0,39979–0,42742] |
| RAM applicazione | 0,90834 [0,90646–0,90963] | 0,75738 [0,75694–0,75845] |
| CPU backend | 3,13514 [3,13326–3,16522] | 3,18054 [3,05576–3,26476] |
| RAM backend | 2,16795 [2,10209–2,18914] | 2,12846 [2,11569–2,15929] |
| CPU stack | 1,07508 [1,05415–1,08424] | 1,06473 [1,05337–1,10599] |
| RAM stack | 1,45921 [1,43830–1,46917] | 1,34860 [1,33866–1,36406] |

### 10.4 Throttling e cgroup peak

I guardrail cgroup mostrano che lo stress satura il budget backend Queen:

| Guardrail, mediana n=3 | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Backend cpu_nr_throttled | 0 | 224 | 231 |
| Backend cpu_throttled_seconds, aggregato | 0,000 s | 26,999 s | 25,860 s |
| App cpu_nr_throttled | 0 | 0 | 0 |
| App cpu_throttled_seconds | 0,000 s | 0,000 s | 0,000 s |
| App memory_peak_reported | 205,55 MiB | 186,60 MiB | 155,64 MiB |
| Backend memory_peak_reported | 130,48 MiB | 308,17 MiB | 303,40 MiB |

Il backend Queen comprende due cgroup, broker e PostgreSQL, ciascuno limitato a
1 CPU; il tempo throttled riportato è aggregato. Questo rende il confronto
coerente con il budget comune di 2 CPU ma non rappresenta la capacità
unconstrained. Nei profili fixed sleep-bound, legacy e auto il throttling
mediano è zero. Nel CPU-bound si osserva solo throttling applicativo ridotto:
mediana 5 eventi/0,031 s per Horizon e 1 evento/0,006 s o 0,004 s per Queen
PHP/Rust.

Lo stress mostra due fatti contemporaneamente:

- Queen completa gli stessi 50.000 job con throughput circa 2,4× e riduce il
  p95 di circa il 76%;
- il backend Queen spende circa 3,1–3,2 volte la CPU Redis, portando la CPU
  stack 6–8% sopra Horizon e la RAM 35–46% sopra.

Il workload non contiene sleep né lavoro CPU. È quindi uno stress del percorso
queue/client/result sink e non rappresenta la produttività di un job business.
Proprio per questo mette in evidenza sia il minor overhead worker Queen sia il
costo del backend PostgreSQL.

### 10.5 Discovery p4/a4: throughput e semantica

La campagna
[discovery-ack4-stress](discovery-ack4-stress/20260829T030358Z-2a4b107a06/campaign-stats.json)
mantiene lo stesso workload Queen zero-work da 50.000 job, p4, bulk 100 e
commit 2a4b107a, ma porta ACK batch da 1 a 4. Non include Horizon. Tutti i 6/6
run sono corretti, quiescenti e configuration-consistent.

| Metrica mediana n=3 | Queen PHP p4/a4 | Queen Rust p4/a4 |
| --- | ---: | ---: |
| Completion job/s | 7.306,88 | 6.989,41 |
| Q1–Q3 completion | 6.257,14–7.341,95 | 6.975,94–7.162,24 |
| Headline job/s | 7.272,61 | 6.958,31 |
| E2E p95 | 34,64 ms | 30,98 ms |
| E2E p99 | 94,72 ms | 98,81 ms |
| CPU orchestratore | 0,00813 s | 0,00382 s |
| PSS orchestratore | 35,15 MiB | 2,86 MiB |
| CPU stack | 15,510 s | 15,492 s |
| RAM stack | 424,72 MiB | 393,13 MiB |

La p95 PHP è molto dispersa: 32,28 ms, 34,64 ms e 3.277,43 ms nei tre run.
Con n=3 la mediana non riassume da sola questa instabilità.

Contro le mediane p4/a1 della campagna stress separata, completion è 3,58× per
PHP e 3,48× per Rust. Il confronto è discovery/non paired: non ha bootstrap CI
e non mescola i rapporti paired prodotti dentro ciascuna campagna. La
completion è inoltre registrata prima dell'ACK; il batching differito rende
particolarmente necessario verificare la coda finale e il comportamento al
crash.

I fault p4/a4 successivi mostrano il trade-off: at-least-once passa in 10/10
lane ma sette lane osservano un duplicato e strict passa solo 3/10. Per questo
a4 non sostituisce la baseline ACK prudente a1. Le conferme fixed e auto p16/a16
successive chiudono il gate steady-state su job da 10 ms; il fault p16/a16
matched sui knob chiude at-least-once 10/10 ma strict solo 1/10 con job da 500
ms. La discovery zero-work non è quindi un nuovo default.

## 11. Failure iniziale del gate di osservazione/completamento

### 11.1 Evidenza preservata

La prima esecuzione stress,
[stress-50k-safe](stress-50k-safe/20260829T020655Z-5b47a7d579/horizon/fixed/r01/summary.json),
era sul commit 5b47a7d5 e sull'app image sha256:39f291…b918. Si arrestò durante
la prima lane Horizon:

| Segnale | Valore |
| --- | ---: |
| Job attesi | 50.000 |
| Completion uniche osservate | 28.617 |
| Missing nel summary parziale | 21.383 |
| Duplicati / failure | 0 / 0 |
| Queue size | 21.921 |
| Ready / reserved / delayed | 21.918 / 3 / 0 |
| Result-check | file di 0 byte |

Fonti:
[summary parziale](stress-50k-safe/20260829T020655Z-5b47a7d579/horizon/fixed/r01/summary.json),
[queue state](stress-50k-safe/20260829T020655Z-5b47a7d579/horizon/fixed/r01/queue-state.final.json),
[result-check](stress-50k-safe/20260829T020655Z-5b47a7d579/horizon/fixed/r01/result-check.json).

La coda non era vuota: i 21.383 job non ancora osservati non possono essere
classificati come persi. Non risultano OOM di container o errori del sampler.
Lo stderr e l'exit reason del vecchio observer non furono conservati, quindi
l'artifact prova il fallimento del gate di osservazione/completamento, non la
causa precisa.

### 11.2 Mitigazione e conferma

Il commit 2a4b107a, “test: scale benchmark result collection”, sostituisce la
rilettura/materializzazione completa dei risultati con snapshot JSONL
incrementali e aggregati compatti. Il
[test streaming](../../app/tests/streaming_results.php) verifica 50.000 record
con memory_limit 64 MiB, oltre a snapshot incrementali, percentili, conteggi e
rifiuto di JSON malformato.

La sequenza di verifica è:

1. failure preservata del vecchio gate di osservazione/completamento;
2. modifica streaming e regression test;
3. [stress 50k v2 singolo](stress-50k-safe-v2/20260829T022644Z-2a4b107a06/campaign-stats.json)
   completato;
4. stress 50k n=3, 9/9 run validi e quiescenti.

La relazione è cronologica e la mitigazione elimina una modalità nota di
crescita della memoria. In assenza dello stderr originale, non viene dichiarata
come root cause forense certa e, soprattutto, non viene imputata alla queue.

## 12. Fault injection e recovery

### 12.1 Disegno p4/a1 post-guard

Ogni round usa commit clean 3655cd2a, app image sha256:0e82c6cc…8958, un
backend fresco e, per ciascun engine:

- fixed 2 worker, 24 job da 2.000 ms;
- dispatch single, tries 2, timeout worker 10 s, retry_after 41 s;
- Queen prefetch 4 e ACK batch 1;
- selezione di un solo child worker non-master già qualificato;
- SIGKILL dopo 100 ms con backlog ancora presente;
- attesa respawn, retry, set esatto delle completion e coda stabile a zero;
- ispezione container per restart e OOM.

Il floor nominale del workload è 4×2=8 s e la product guard richiede
retry_after >4×timeout10=40 s: 41 s è dunque una configurazione accettata dal
guard misurato. Non è però l'intero preset della fixed primaria, che usa
quattro worker, job da 10 ms, timeout 120 s e retry_after 481 s. Il fault
osserva redelivery intorno a 40–45 s; la configurazione primaria può invece
attendere quasi otto minuti. Sono matched p4/a1 e la regola di lease, non il
tempo di recovery esatto del profilo prestazionale.

Tutti i target hanno target_proved_work_before_kill=true e
target_is_container_init=false. Il timestamp del kill e le completion usano il
clock monotonic della VM Docker; il tempo alla prima completion retry è
correttamente espresso come intervallo lower/upper, non come punto.

### 12.2 P4/a1 post-guard: tutti i round

| Rep | Engine | Full pool dopo kill | Bound prima completion retry | Completion a tentativo >1 |
| --- | --- | ---: | ---: | ---: |
| r01 | Horizon | 85,4 ms | 42.704,9–42.770,5 ms | 1 |
| r01 | Queen PHP | 3.261,6 ms | 40.465,7–40.524,5 ms | 3 |
| r01 | Queen Rust | 2.887,2 ms | 43.125,7–43.192,6 ms | 4 |
| r02 | Horizon | 84,8 ms | 41.958,5–42.021,0 ms | 1 |
| r02 | Queen PHP | 1.543,8 ms | 44.571,2–44.638,1 ms | 2 |
| r02 | Queen Rust | 1.540,3 ms | 40.494,6–40.553,9 ms | 3 |
| r03 | Horizon | 88,6 ms | 41.930,7–41.992,6 ms | 1 |
| r03 | Queen PHP | 3.108,4 ms | 43.278,8–43.345,0 ms | 4 |
| r03 | Queen Rust | 3.003,7 ms | 40.430,4–40.492,2 ms | 3 |
| r04 | Horizon | 923,3 ms | 43.210,9–43.273,6 ms | 1 |
| r04 | Queen PHP | 1.614,3 ms | 40.609,2–40.673,7 ms | 3 |
| r04 | Queen Rust | 2.964,8 ms | 41.121,6–41.195,9 ms | 2 |
| r05 | Horizon | 86,7 ms | 42.275,6–42.339,2 ms | 1 |
| r05 | Queen PHP | 1.495,5 ms | 40.506,0–40.569,8 ms | 3 |
| r05 | Queen Rust | 2.932,1 ms | 40.464,0–40.525,3 ms | 4 |

Riassunto descrittivo sui cinque round:

| Engine | Full pool mediana (range) | Bound retry mediano | Retry completion totali |
| --- | ---: | ---: | ---: |
| Horizon | 86,7 ms (84,8–923,3) | 42.275,6–42.339,2 ms | 5 |
| Queen PHP | 1.614,3 ms (1.495,5–3.261,6) | 40.609,2–40.673,7 ms | 15 |
| Queen Rust | 2.932,1 ms (1.540,3–3.003,7) | 40.494,6–40.553,9 ms | 16 |

I tempi di respawn Queen restano entro 3,3 s e il timeout previsto era 30 s;
Horizon ricostituisce il pool più rapidamente in questi run. Il numero di
completion a tentativo maggiore di uno non è un duplicato finale: il set unico
resta esatto. È compatibile con una finestra in-flight più ampia a p4, ma il
test non isola causalmente il fattore.

### 12.3 Gate di affidabilità p4/a1 post-guard

In tutte le 15 lane:

| Gate | Esito |
| --- | ---: |
| at_least_once_pass | 15/15 |
| strict_observation_pass | 15/15 |
| Worker respawned | 15/15 |
| Retry in-flight osservato | 15/15 |
| Job unici | 24/24 per lane; 360/360 totali |
| Record / max attempt | 360 / 2 |
| Missing / duplicati / inattesi | 0 / 0 / 0 |
| Failure signal nei log | 0 |
| Queue finale | size/ready/reserved/delayed = 0 |
| Probe error / timeout queue | 0 / 0 |
| OOM / restart container | 0 / 0 |

Artifact principali:
[r01](fault-p4-a1-guarded-r01/report.json),
[r02](fault-p4-a1-guarded-r02/report.json),
[r03](fault-p4-a1-guarded-r03/report.json),
[r04](fault-p4-a1-guarded-r04/report.json),
[r05](fault-p4-a1-guarded-r05/report.json).
Ogni directory conserva inoltre timeline, identità del target, process tree,
eventi, queue state, container state e log. I cinque round storici
`fault-p4-r01…r05` restano preservati come corroborazione pre-guard, ma sono
esclusi uno-a-uno dalle 35 lane fault selezionate.

### 12.4 P4/a4: at-least-once passa, strict no

Cinque round successivi eseguono lo stesso fault per Queen PHP e Queen Rust,
con prefetch 4 e ACK batch 4. Usano commit
327849a1edbae2b1c5f17c402062dbf5dc5c4237, worktree clean e app image
sha256:b631f6f6ff90202611ac337fecf10d88e557509ea545f9965df023aa0c0cc21d.

| Rep | Engine | At-least-once | Strict | Duplicati | Job unici | Missing/inattesi | OOM/restart |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| r01 | Queen PHP | pass | fail | 1 | 24/24 | 0/0 | 0/0 |
| r01 | Queen Rust | pass | pass | 0 | 24/24 | 0/0 | 0/0 |
| r02 | Queen PHP | pass | fail | 1 | 24/24 | 0/0 | 0/0 |
| r02 | Queen Rust | pass | pass | 0 | 24/24 | 0/0 | 0/0 |
| r03 | Queen PHP | pass | fail | 1 | 24/24 | 0/0 | 0/0 |
| r03 | Queen Rust | pass | fail | 1 | 24/24 | 0/0 | 0/0 |
| r04 | Queen PHP | pass | fail | 1 | 24/24 | 0/0 | 0/0 |
| r04 | Queen Rust | pass | fail | 1 | 24/24 | 0/0 | 0/0 |
| r05 | Queen PHP | pass | pass | 0 | 24/24 | 0/0 | 0/0 |
| r05 | Queen Rust | pass | fail | 1 | 24/24 | 0/0 | 0/0 |

Tutte le 10/10 lane osservano respawn, retry in-flight, max attempt 2, set
esatto dei 24 job, failure signal 0 e coda finale quiescente a zero.
At-least-once passa 10/10; strict passa 1/5 PHP e 2/5 Rust, cioè 3/10
complessivo. I sette duplicati sono distribuiti su sette run distinti.

Artifact:
[a4 r01](fault-p4-a4-r01/report.json),
[a4 r02](fault-p4-a4-r02/report.json),
[a4 r03](fault-p4-a4-r03/report.json),
[a4 r04](fault-p4-a4-r04/report.json),
[a4 r05](fault-p4-a4-r05/report.json).

Il risultato rende misurabile il trade-off: ACK4 aumenta molto il throughput
zero-work, ma amplia la finestra in cui un crash ridelivera lavoro già
completato nell'applicazione e non ancora confermato al broker. È utilizzabile
solo come opt-in per job ed effetti esterni idempotenti, con i duplicati
trattati come comportamento atteso della semantica at-least-once.

### 12.5 P16/a16 matched sui knob: at-least-once 10/10, strict 1/10

La campagna matched sui knob
[p16/a16 r01](fault-p16-a16-valid-r01/report.json)…[r05](fault-p16-a16-valid-r05/report.json)
usa prefetch 16/ACK batch 16, due worker e 24 job da 500 ms; le conferme
steady-state usano invece quattro worker, job da 10 ms e retry_after 180 s.
Il floor nominale del batch è circa 16×0,5=8 s, sotto retry_after 12 s. Il
guard attuale usa però timeout 10 s e richiederebbe retry_after >160 s: questi
artifact pre-guard, nonostante il suffisso `valid`, oggi verrebbero rifiutati.

| Engine | Lane at-least-once | Lane strict | Run con duplicati | Record / unici | Duplicati | Respawn mediano (range) | Bound retry lower mediano |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Queen PHP | 5/5 | 0/5 | 5/5 | 129 / 120 | 9 | 2.910 ms (1.588–2.948) | 11.496 ms |
| Queen Rust | 5/5 | 1/5 | 4/5 | 128 / 120 | 8 | 3.035 ms (1.626–3.113) | 11.565 ms |
| Totale | 10/10 | 1/10 | 9/10 | 257 / 240 | 17 | — | — |

Tutte le lane osservano respawn, retry in-flight, max attempt 2, 24/24 job
unici, zero missing/inattesi/failure signal, coda finale quiescente e zero
OOM/restart. I knob conservano quindi at-least-once nel workload nominale
osservato, ma la strict observation fallisce in 9/10 lane per duplicati.
Questo è un fault diretto p16/a16; non consente però un confronto quantitativo
del tasso di duplicazione con p4/a4, che usa job da 2 s anziché 500 ms.

### 12.6 Pilot negativo: lease budget p16/a16 insufficiente

Il pilot iniziale
[fault-p16-a16-r01](fault-p16-a16-r01/report.json) riutilizzava job da 2 s e
retry_after 12 s: 16×2=32 s supera il lease. È stato fermato dopo le due lane
del primo round e viene escluso dai totali fault principali.

| Engine | Unici / record | Duplicati | Failure signal | Queue finale size/reserved | At-least-once / strict |
| --- | ---: | ---: | ---: | ---: | ---: |
| Queen PHP | 24 / 39 | 15 | 763 | 16 / 16 | fail / fail |
| Queen Rust | 24 / 29 | 5 | 993 | 24 / 24 | fail / fail |

Entrambe le lane completano i 24 ID unici e non hanno OOM/restart, ma dopo 120
s la coda non è quiescente; perciò il gate at-least-once fail-closed resta
falso. Il pilot è riportato integralmente come test negativo di configurazione
e dimostra la necessità di validare `retry_after` rispetto all'intero batch
prefetched; non misura l'affidabilità della campagna `valid-r`, riportata
separatamente e comunque pre-guard.

### 12.7 Mitigazione post-benchmark c4059027

Il commit post-benchmark `c4059027` introduce due guard fail-fast:

- la [configurazione condivisa dei supervisor](../../../../clients/client-laravel/src/Laravel/Supervisor/SupervisorConfiguration.php)
  rifiuta `retry_after <= prefetch × timeout`;
- il [fault harness](../../scripts/fault-recovery.sh) rifiuta almeno
  `prefetch × sleep_ms >= retry_after`, salvo override esplicito
  `--allow-lease-risk` per protocolli negativi, e registra l'override nei
  metadata. L'override non bypassa la product guard nell'image post-guard: un
  test negativo end-to-end richiede un'image pre-guard, altrimenti resta
  dry-run. Il commit documentale 774c2413 chiarisce questo limite, ma non è
  incluso nell'image misurata.

Il secondo controllo è soltanto un lower bound: CPU work e overhead framework
richiedono margine ulteriore. Due regression test coprono il guard; la suite
post-fix passa **342 test / 1.349 assertion**. La suite è stata eseguita
localmente con PHP 8.5.9, mentre la fixture benchmark usa PHP 8.3.33: è una
regressione del guard, non una prova cross-versione del runtime. Questa
mitigazione è successiva agli artifact pre-guard e non ne migliora
retroattivamente i risultati; la nuova fixed primaria, invece, usa l'image
post-guard 3655cd2a che include il guard, come il nuovo fault p4/a1.

Lo [smoke post-guard](post-lease-guard-smoke/20260829T034840Z-3655cd2a63/campaign-stats.json)
usa p4/a1, timeout 120 s e retry_after 481 s: 3/3 lane (Horizon, PHP, Rust)
sono valide e quiescenti. Con 60 job, warm-up 10 e una sola ripetizione per
engine è un controllo di compatibilità, non evidenza prestazionale; le
completion 251,73/246,02/248,11 job/s non vengono confrontate inferenzialmente.

La successiva
[fixed post-guard 3655](confirm-fixed-safe-head/20260829T035034Z-3655cd2a63/campaign-stats.json)
usa la stessa lease ammessa con 10.000 job e n=5: 15/15 run validi e
quiescenti. Costituisce la conferma prestazionale primaria della compatibilità
guard+profilo. I cinque
[fault round p4/a1 post-guard](fault-p4-a1-guarded-r01/report.json) chiudono
anche recovery strict 15/15 con retry_after 41 s; timeout/retry e worker count
differiscono dalla fixed primaria, quindi non ne misurano il recovery quasi
otto minuti della lease 481 s.

### 12.8 Limiti del fault test

- Il failed-job store durabile è disabilitato: durable_failure_count è null e
  i failure signal sono derivati dai log. Non va scritto “zero failed job
  durabili”.
- Il target deve avere una completion precedente, backlog e job lunghi; manca
  un evento start del job corrente. È una forte qualificazione, non la prova al
  nanosecondo che SIGKILL cada dentro user code.
- La completion è scritta alla fine di handle, prima dell'ACK.
- La fixture non ha un ledger esterno per verificare l'idempotenza degli
  effetti business.
- Un crash per engine/profilo per backend fresco, ripetuto cinque volte, non
  stima una probabilità di failure.
- Non sono stati iniettati crash di broker, PostgreSQL o Redis, né fault rete,
  disk-full o failover.

Il risultato corretto da comunicare è: p4/a1 post-guard supera recovery
at-least-once e strict in 15/15 smoke; p4/a4 supera at-least-once in 10/10 ma strict soltanto
in 3/10; p16/a16 col floor runtime nominale sotto lease supera at-least-once
in 10/10 ma strict in 1/10. Totale principale: 35/35 lane at-least-once,
19/35 strict, 840 job unici
e 24 duplicati osservati. Le due lane del pilot con lease insufficiente sono
separate. Nessun profilo prova exactly-once.

## 13. Soak Queen broker-native da 1M msg/s

### 13.1 Setup

Il test separato usa per 86.400 s:

- target open-loop 1.000.000 msg/s, rampa 60 s;
- push batch 100, payload 256 byte;
- lease, ACK espliciti async con 256 in volo, deduplica 60 s e retention;
- una coda, 200 partizioni, un consumer group, 600 consumer;
- pop batch 1.000;
- PostgreSQL 18.4 con synchronous_commit on;
- tre loader Go via HTTP, non Laravel.

### 13.2 Risultato

| Metrica | Valore |
| --- | ---: |
| Messaggi offerti | 86.370.017.100 |
| Push accettati | 86.369.975.300 |
| Gap offerti−accettati | 41.800 |
| Throughput accettato medio, rampa inclusa | 999.652 msg/s |
| Messaggi poppati | 86.369.532.700 |
| Messaggi ACK | 86.369.517.700 |
| Shed | 0 |
| Richieste push fallite | 4 |
| Richieste pop fallite | 0 |
| Ack falliti, messaggi | 600 |
| Restart watchdog / contatore incidenti dichiarato | 0 / 0 |

Il divario pushed−popped è 442.600 e popped−acked è 15.000 al cut-off. Sono
compatibili con stato in-flight, ma senza drain finale e ledger per ID non
provano conservation end-to-end e non autorizzano un claim zero-loss. Dei
41.800 messaggi offerti ma non accettati, solo 400 sono spiegati esplicitamente
dalle quattro richieste push batch fallite; il resto non è riconciliato
nell'artifact. I 600 ACK falliti sono un ulteriore contatore operativo. Questi
segnali non vanno nascosti dietro “zero incidenti” né sommati senza distinguere
le rispettive semantiche.

Stabilità e latenza:

| Metrica | Valore |
| --- | ---: |
| Ore consecutive nella banda ±0,02% | 23 |
| Finestre da 1 min fuori ±5% | 1/1.401, il minuto di rampa |
| p50 mediano sui report 30 s | 92,7 ms |
| p99 mediano sui report 30 s | 288,8 ms |
| p99 massimo | 593,9 ms |
| Intervalli 30 s oltre 1 s | 0 |

Risorse del nodo broker+PostgreSQL:

| Risorsa | Valore |
| --- | ---: |
| Broker RSS | media 4,10 GB, picco 4,37 GB |
| CPU broker | media 10,92 core, picco 12,90 |
| CPU PostgreSQL | media 11,54 core, picco 13,88 |
| Backend attivi | 43 |
| Commit PostgreSQL | 8.056/s |
| WAL | 83 MB/s, 6,8 TB totali |
| Database finale | 31,2 GB |

Il database raggiunge un plateau di circa 29,6–31,4 GB grazie alla retention.
Il WAL da 6,8 TB è un costo operativo importante da dimensionare, anche se il
DB residente resta piatto.

Fonti:
[risultati](../../../2026-08-11-soak24-1M/results.md),
[metriche broker](../../../2026-08-11-soak24-1M/raw/bench/metrics.csv),
[sampler host](../../../2026-08-11-soak24-1M/raw/bench/bench.csv),
[loader 01](../../../2026-08-11-soak24-1M/raw/loader-01/g.out),
[loader 02](../../../2026-08-11-soak24-1M/raw/loader-02/g.out),
[loader 03](../../../2026-08-11-soak24-1M/raw/loader-03/g.out).

### 13.3 Separazione obbligatoria dal benchmark Laravel

Il soak misura chiamate broker native batchate, su hardware dedicato e con 600
consumer. Il benchmark Laravel misura job serializzati, queue:work, client PHP,
orchestrazione e sink di completion su Docker Desktop. Cambiano:

- percorso software e protocollo;
- batching e concorrenza;
- host e budget;
- commit/versione;
- unità operativa: messaggio broker contro job Laravel.

Il soak dimostra che, in quello specifico setup, il broker ha headroom ben
oltre le centinaia di job/s osservate in Laravel. Non dimostra l'assenza
generale di limiti, né che l'integrazione Laravel possa processare 1M job/s, e
non autorizza un rapporto numerico 1.000.000/290. È inoltre storico, su commit
e infrastruttura diversi: 999.652 è accepted throughput broker-native, non un
capacity claim per il client Laravel o per l'attuale SHA.

## 14. Valutazione complessiva

| Criterio | Horizon | Queen PHP | Queen Rust |
| --- | --- | --- | --- |
| Fixed throughput post-guard 3655 | baseline | +14,28% | +14,05% |
| Fixed p95 post-guard 3655 | baseline | -19,04% | -19,63% |
| Costo orchestratore | più alto | circa metà PSS | nettamente più basso |
| CPU stack misurato fixed post-guard | baseline | -39,43% | -39,94% |
| RAM stack misurato fixed post-guard | baseline | +55,40% | +39,30% |
| Fixed w1 completion | baseline | +2,2% | +1,9% |
| Fixed w8 completion mediana | baseline | -7,9%, CI attraversa 1 | -6,0%, CI attraversa 1 |
| Auto completion-span post-guard | migliore | -5,0% | -3,3% |
| Auto worker-second post-guard | baseline | -12,4% | -12,4% |
| Legacy p1/a1 completion | baseline | -54,9% | -53,7% |
| Fixed p16/a16 completion | baseline | +15,6% | +14,8% |
| Auto p16/a16 completion | baseline | -1,6% | -0,1%, CI attraversa 1 |
| Fault strict p4/a1 post-guard | 5/5 | 5/5 | 5/5 |
| Fault strict p4/a4 | non applicabile | 1/5 | 2/5 |
| Fault strict p16/a16 | non applicabile | 0/5 | 1/5 |
| ACK4 | non applicabile | opt-in idempotente | opt-in idempotente |
| ACK16 | non applicabile | opt-in idempotente | opt-in idempotente |
| Portabilità | ecosistema Horizon | solo PHP | richiede binario Rust |
| Ruolo orchestratore | baseline di controllo | fallback | **predefinito proposto** |
| Shipping queue knobs | non applicabile | **p1/a1** | **p1/a1** |
| Profilo performance | non applicabile | p4/a1 condizionato alla lease | p4/a1 condizionato alla lease |

Rust è la scelta naturale se l'obiettivo originario è eliminare il costo
dell'orchestratore Horizon: il vantaggio PSS è di un ordine di grandezza
rispetto a Queen PHP, senza sacrificare throughput nel profilo p4 misurato; il
default p1 resta invece circa 54% sotto Horizon. La topologia Queen richiede
però più RAM nel perimetro misurato di Horizon+Redis; il dimensionamento
production deve usare questo dato consumer+backend, non il solo processo Rust,
aggiungendo poi producer/observer e gli altri costi esclusi.

## 15. Minacce alla validità e limiti

1. **Host diagnostico.** Docker Desktop su macOS non sostituisce Linux nativo,
   soprattutto per cgroup, I/O e scheduler.
2. **Campione ridotto.** n=3 o n=5; i CI bootstrap non sono population CI e non
   correggono molte metriche.
3. **Topologie diverse.** Redis volatile contro broker+PostgreSQL
   synchronous_commit on, ma PostgreSQL locale usa tmpfs. Parità di budget non
   significa parità di durabilità o I/O storage.
4. **Ordine temporale.** L'ordine engine è ruotato e ogni backend è fresco, ma
   l'esecuzione resta sequenziale sullo stesso host e può subire drift.
5. **Produttore escluso.** Il dispatch non è incluso nei cgroup della lane.
   Producer, applicazione e backend condividono però la VM: le quote nominali
   sommano a 10 CPU, oltre al sampler, e il dispatch Queen dura circa 1,8×
   Horizon nella fixed post-guard 3655cd2a. Restano possibili contention e
   perturbazione.
6. **Sink comune.** È necessario per la comparabilità, ma nel zero-work può
   diventare una quota rilevante del costo.
7. **PSS parziale.** PSS descrive il control plane; memory.current descrive lo
   stack. Nessuna delle due va sostituita all'altra.
8. **Un'unica coda.** L'auto test non copre distribuzione multi-queue,
   priorità, starvation o strategie time warmed.
9. **Fault circoscritto.** Nessun backend crash, failover, fault rete,
   corruzione, disk-full o side-effect ledger.
10. **Failed store disabilitato.** Le failure nei fault sono log-derived.
11. **Soak separato.** Versione, protocollo, host e unità di lavoro differenti;
    nessun confronto numerico con Laravel e nessun soak Laravel prolungato
    completato.
12. **Scaling non completamente matched.** W1 e w8 sono inclusi e coerenti fra
    loro; il w4 primario usa commit, immagine, job count e n diversi.
13. **Profili aggressivi non isolati.** La discovery p4/a4 è zero-work; le
    conferme p16/a16 variano insieme prefetch e ACK. Il fault p16/a16 matched
    sui knob usa 500 ms, quello p4/a4 usa 2 s: entrambi osservano duplicati ma
    non consentono un confronto causale dei tassi.
14. **Partizioni/fusion non selezionate.** 64 partizioni e fusion off sono
    sempre state la baseline, non il vincitore di un follow-up comparativo.
15. **Feature parity fuori perimetro.** I test coprono performance e recovery
    del percorso misurato, non dashboard, metriche, failed-job persistence,
    pause/resume o altri controlli operativi Horizon. Il sink valida il set di
    ID, non ordering o priorità; Redis single-queue e Queen a 64 partizioni
    possono divergere con più worker.
16. **Freeze e warm-up.** Il protocollo è un freeze locale, non una
    preregistrazione firmata; select p4/p16 e fixed safe storico non registrano
    il warm-up 512 prescritto, mentre il fixed post-guard registra warm-up 0. Il
    pairing interno resta valido, ma l'aderenza non è integrale.
17. **Lease del prefetch.** Queen riserva il batch prima dell'esecuzione locale;
    ACK1 non impedisce che un job buffered scada prima di iniziare. Ogni
    prefetch >1 richiede runtime bounded, margine e guard/renewal espliciti.
18. **Container estranei.** Il runner performance conserva gli snapshot finali
    ma non un inventario/fail-fast di container preesistenti per ogni lane; il
    fault harness lo fa. L'assenza registrata al freeze non prova assenza per
    ogni run performance.

## 16. Raccomandazione production-ready

### 16.1 Default tecnico

- Queen prefetch **1** come shipping default conservativo;
- ACK batch **1**;
- 64 partizioni e pop fusion off come baseline testata, non ottimizzata;
- supervisor **Rust**;
- PHP supervisor disponibile come fallback e strumento di bootstrap.

P1/a1 rinuncia al throughput osservato di p4, ma evita per default una coda
locale di job già leased. P4/a1 è il profilo prestazionale verificato soltanto
per job brevi e bounded quando
`retry_after > prefetch × worst-case runtime + margine`, oppure con lease
renewal. ACK1 evita la finestra aggiuntiva dell'ACK differito, non il rischio di
scadenza dei job prefetched prima dell'avvio. Ogni ack_batch maggiore di 1,
inclusi p4/a4 e p16/a16, resta opt-in per job idempotenti con osservabilità dei
duplicati.

Quindi la raccomandazione di Rust riguarda il control plane, non implica che
Queen p1/a1 sia performance-equivalent a Horizon: nel legacy è circa 54–55%
più lenta. La fixed post-guard 3655cd2a dimostra che p4 con lease accettata può superare
Horizon per job brevi da 10 ms; per generalizzare insieme performance e safety
servono runtime bounded e margine verificato, oppure lease renewal.

### 16.2 Rollout

1. Iniziare con worker fixed o limiti auto conservativi in un canary.
2. Osservare queue age, ready/reserved/delayed, worker count, retry, lease
   expiry, CPU backend, WAL e memoria consumer+backend.
3. Applicare e monitorare
   `retry_after > prefetch × worst-case runtime + margine`, includendo overhead,
   code e jitter; se il runtime non è bounded, usare p1 o lease renewal.
4. Rendere i job idempotenti e registrare gli effetti esterni, perché la
   semantica resta at-least-once.
5. Se si abilita ack_batch > 1, etichettarlo come profilo aggressivo e
   includere duplicate count e fault regression matched nei gate.
6. Conservare alert su respawn, OOM, restart, queue non quiescente e crescita
   dei delayed/reserved.
7. Confrontare costi infrastrutturali completi: il risparmio control-plane Rust
   non annulla RAM e WAL di PostgreSQL.

### 16.3 Gate prima della GA

- cella fixed w4 matched a w1/w8 per completare la curva;
- ripetizione su Linux arm64 e amd64 nativi;
- soak Laravel di molte ore con job sleep-bound, CPU-bound e misti;
- fault ripetuti e concorrenti su worker;
- restart/crash di broker, PostgreSQL e Redis;
- latenza rete, partition, failover e disk pressure;
- failed-job store durabile e side-effect ledger idempotente;
- workload multi-queue con ordering, priorità e autoscaling;
- se p16/a16 resta disponibile, fault matched ripetuti e un ledger degli
  effetti esterni, oltre alle conferme fixed/auto già completate;
- screening dichiarato di partizioni e pop fusion;
- upgrade/rollback di client, broker e supervisor;
- budget e SLO formalizzati per throughput, p95/p99, recovery e backlog;
- snapshot e fail-fast per container estranei in ogni lane performance.

## 17. Stato dello scaling

W1 e w8 sono completi: 18/18 run validi, quiescenti e comparabili entro ciascun
worker count. I rapporti Queen/Horizon e i CI paired sono riportati in 7.3.

La curva end-to-end non è ancora omogenea perché manca w4 sullo stesso commit
2a4b107a, app image sha256:9386…ccf6c, 3.000 job, warm-up 256 e n=3. Il w4
primario post-guard 3655cd2a rimane l'evidenza cross-engine più robusta, ma usa commit,
immagine, 10.000 job, warm-up 0 e n=5 differenti; non viene usato per un
calcolo causale di speedup insieme a w1/w8.

## 18. Indice degli artifact

### Campagne aggregate

- [Fixed post-guard 3655 JSON](confirm-fixed-safe-head/20260829T035034Z-3655cd2a63/campaign-stats.json)
  e [Markdown](confirm-fixed-safe-head/20260829T035034Z-3655cd2a63/campaign-stats.md)
- [Fixed safe storico pre-guard JSON](confirm-fixed-safe/20260829T012952Z-5b47a7d579/campaign-stats.json)
  e [Markdown](confirm-fixed-safe/20260829T012952Z-5b47a7d579/campaign-stats.md),
  corroborazione ridondante esclusa dai 99
- [Legacy JSON](confirm-fixed-legacy/20260829T014350Z-5b47a7d579/campaign-stats.json)
  e [Markdown](confirm-fixed-legacy/20260829T014350Z-5b47a7d579/campaign-stats.md)
- [Auto p4/a1 post-guard JSON](confirm-auto-safe-head/20260829T042032Z-774c24136a/campaign-stats.json)
  e [Markdown](confirm-auto-safe-head/20260829T042032Z-774c24136a/campaign-stats.md)
- [Auto storico pre-guard JSON](confirm-auto-safe/20260829T015317Z-5b47a7d579/campaign-stats.json),
  corroborazione ridondante esclusa dai 99
- [CPU JSON](confirm-cpu-safe/20260829T020132Z-5b47a7d579/campaign-stats.json)
  e [Markdown](confirm-cpu-safe/20260829T020132Z-5b47a7d579/campaign-stats.md)
- [Fixed aggressivo JSON](confirm-fixed-aggressive/20260829T031618Z-327849a1ed/campaign-stats.json)
  e [Markdown](confirm-fixed-aggressive/20260829T031618Z-327849a1ed/campaign-stats.md)
- [Auto aggressivo JSON](confirm-auto-aggressive/20260829T032231Z-327849a1ed/campaign-stats.json)
  e [Markdown](confirm-auto-aggressive/20260829T032231Z-327849a1ed/campaign-stats.md)
- [Select p4 JSON](select-p4/20260829T012603Z-5b47a7d579/campaign-stats.json)
  e [p16 JSON](select-p16/20260829T012742Z-5b47a7d579/campaign-stats.json)
- [Scaling w1 JSON](scale-w1-safe/20260829T024935Z-2a4b107a06/campaign-stats.json)
  e [Markdown](scale-w1-safe/20260829T024935Z-2a4b107a06/campaign-stats.md)
- [Scaling w8 JSON](scale-w8-safe/20260829T030004Z-2a4b107a06/campaign-stats.json)
  e [Markdown](scale-w8-safe/20260829T030004Z-2a4b107a06/campaign-stats.md)
- [Stress 50k JSON](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/campaign-stats.json)
  e [Markdown](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/campaign-stats.md)
- [Stress-v2 singola ripetizione JSON](stress-50k-safe-v2/20260829T022644Z-2a4b107a06/campaign-stats.json),
  preservata nel lordo ma esclusa dai 99
- [Post-streaming smoke](post-streaming-smoke/20260829T022544Z-2a4b107a06/report.md),
  preservato nel lordo ma escluso dai 99
- [Discovery ACK4 JSON](discovery-ack4-stress/20260829T030358Z-2a4b107a06/campaign-stats.json)
  e [Markdown](discovery-ack4-stress/20260829T030358Z-2a4b107a06/campaign-stats.md)
- [Smoke post-guard JSON](post-lease-guard-smoke/20260829T034840Z-3655cd2a63/campaign-stats.json)
  e [Markdown](post-lease-guard-smoke/20260829T034840Z-3655cd2a63/campaign-stats.md)
- [Grid dirty: anchor iniziale](screen/01-anchor-start/20260829T004533Z-f2c39fd989/report.json)
  e [anchor finale](screen/12-anchor-end/20260829T005149Z-f2c39fd989/report.json)
- [Subset dirty screen-clean](screen-clean/02-p4-a1/20260829T005411Z-f2c39fd989/report.json)

### Correttezza e recovery

- [Fault p4/a1 post-guard r01](fault-p4-a1-guarded-r01/report.json)
- [Fault p4/a1 post-guard r02](fault-p4-a1-guarded-r02/report.json)
- [Fault p4/a1 post-guard r03](fault-p4-a1-guarded-r03/report.json)
- [Fault p4/a1 post-guard r04](fault-p4-a1-guarded-r04/report.json)
- [Fault p4/a1 post-guard r05](fault-p4-a1-guarded-r05/report.json)
- [Fault p4/a1 storico pre-guard r01](fault-p4-r01/report.json), con r02…r05
  preservati come corroborazione ridondante
- [Fault ACK4 r01](fault-p4-a4-r01/report.json)
- [Fault ACK4 r02](fault-p4-a4-r02/report.json)
- [Fault ACK4 r03](fault-p4-a4-r03/report.json)
- [Fault ACK4 r04](fault-p4-a4-r04/report.json)
- [Fault ACK4 r05](fault-p4-a4-r05/report.json)
- [Fault p16/a16 r01](fault-p16-a16-valid-r01/report.json)
- [Fault p16/a16 r02](fault-p16-a16-valid-r02/report.json)
- [Fault p16/a16 r03](fault-p16-a16-valid-r03/report.json)
- [Fault p16/a16 r04](fault-p16-a16-valid-r04/report.json)
- [Fault p16/a16 r05](fault-p16-a16-valid-r05/report.json)
- [Pilot p16/a16 lease insufficiente](fault-p16-a16-r01/report.json)
- [Prima failure 50k](stress-50k-safe/20260829T020655Z-5b47a7d579/horizon/fixed/r01/summary.json)
- [Regression test observer](../../app/tests/streaming_results.php)
- [Regression test lease guard](../../../../clients/client-laravel/tests/LaravelSupervisorProductionTest.php)

### Broker-native

- [Report soak 24 h / 1M](../../../2026-08-11-soak24-1M/results.md)
- [Sampler raw soak](../../../2026-08-11-soak24-1M/raw/bench/bench.csv)
- [Contatori broker soak](../../../2026-08-11-soak24-1M/raw/bench/metrics.csv)
