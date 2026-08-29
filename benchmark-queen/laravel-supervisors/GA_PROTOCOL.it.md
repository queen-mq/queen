# Protocollo congelato — qualificazione GA dei supervisor Laravel Queen

Congelato il 29 agosto 2026 alle 05:40:35 UTC (07:40:35 Europe/Rome), prima
dell'esecuzione e dell'ispezione dei risultati della nuova campagna GA.

- Commit al freeze del protocollo: `ec85800f7088c430ec29545305395e48ea9896f8`.
- Branch: `feat/laravel-horizon-replacement`.
- La campagna precedente è evidenza esplorativa e ha determinato i rischi da
  verificare; non sarà ricontata nel nuovo corpus confermatorio.
- Il commit del prodotto, gli image ID e l'hash del protocollo saranno fissati
  nei metadata prima della prima lane.

## 1. Obiettivo e classificazione

Verificare se il client Laravel Queen e i supervisor PHP/Rust soddisfano i gate
necessari a una release candidate che possa sostituire il control plane di
Horizon nel perimetro dichiarato. Le domande sono separate:

1. correttezza delle lease e conservazione degli effetti del workload;
2. lifecycle dei failed job e comportamento multi-coda;
3. throughput, latenza, CPU, memoria e autoscaling a concorrenza matched;
4. stabilità nel tempo e recovery da fault di worker, backend, rete e storage;
5. ripetibilità su Linux `amd64` e `arm64`.

I run su Docker Desktop sono `diagnostic`. I run su VM Linux hosted sono
`diagnostic_native`: sono kernel Linux nativi ma non hardware dedicato. Solo un
host Linux dedicato, qualificato e senza workload estranei può produrre una
campagna `publishable_candidate`.

Questo protocollo è progettato dal team Queen e non costituisce certificazione
indipendente. Tutti i risultati, inclusi fallimenti e pilot, saranno conservati.

## 2. Configurazioni congelate

### 2.1 Lane

- Horizon: Laravel Horizon con Redis.
- Queen PHP: Laravel `queue:work queen`, orchestratore PHP.
- Queen Rust: Laravel `queue:work queen`, orchestratore Rust.

Ogni confronto paired usa lo stesso checkout, workload, numero di worker,
budget cgroup e ordine di dispatch. Una lane alla volta; backend e volumi
freschi per ogni campione. Il producer resta fuori dai cgroup misurati ma la sua
contesa host viene dichiarata.

### 2.2 Profili Queen

- prudente: prefetch 1, ACK batch 1;
- prestazionale: prefetch 4, ACK batch 1;
- renewal: prefetch 4, ACK batch 1, helper di rinnovo abilitato;
- ACK batch maggiore di 1: solo discovery/fault idempotente, mai candidato GA
  generale.

Il profilo renewal deve fallire prima di consumare altri job se non può più
rinnovare in sicurezza. Una failure dopo un effetto esterno conserva semantica
at-least-once e può richiedere idempotenza: non viene chiamata exactly-once.

## 3. Gate ambientali e provenienza

Prima di ogni lane l'harness deve salvare e verificare:

- commit/dirty state, branch, image ID e manifest risolti;
- kernel, architettura, CPU model/count, RAM, Docker/Compose, cgroup version;
- stato termico quando esposto;
- inventario completo dei container e assenza di container estranei;
- limiti CPU/RAM/PID risolti e backend atteso;
- clock e spazio libero iniziale/finale.

Una lane viene esclusa automaticamente se cambia checkout/image/configurazione,
se compaiono container estranei, se mancano campioni, se ci sono OOM/restart non
iniettati, se il backend non torna healthy o se la coda non diventa quiescente.
L'override di un gate produce soltanto evidenza diagnostica esplicitamente
marcata.

## 4. Correttezza e ledger

Il workload di affidabilità usa un ledger esterno alla queue con chiave
idempotente `run_id + job_id`. Deve distinguere:

- dispatch attesi e accettati;
- tentativi di esecuzione;
- effetti applicativi tentati, creati e già presenti;
- completion osservate;
- ACK/queue quiescence e failed job.

Gate strict steady-state:

- set job atteso completo;
- un effetto creato per ogni job e nessun effetto estraneo;
- zero missing, payload/checksum mismatch e failure;
- queue ready/reserved/delayed/processing a zero per la finestra di settle;
- zero OOM, restart e sampler error.

Nei fault test vengono pubblicati separatamente:

- `at_least_once`: tutti gli effetti attesi presenti e coda quiescente;
- `strict_execution`: nessun tentativo o completion duplicato;
- `idempotent_effect`: esattamente un effetto creato per chiave nonostante
  eventuali retry.

Il ledger prova soltanto il side effect della fixture; non generalizza a
database o API dell'applicazione.

## 5. Feature parity operativa

### 5.1 Failed job

Per ciascun engine verificare con backend fresco:

1. job terminalmente fallito visibile nel repository Laravel;
2. snapshot Queen DLQ coerente quando applicabile;
3. `queue:failed`, `queue:retry`, `queue:forget`, flush e prune;
4. retry che ripubblica una sola unità di lavoro e pulisce gli indici soltanto
   dopo successo;
5. failure della pulizia DLQ che non elimina prematuramente il record Laravel.

### 5.2 Multi-coda

Usare tre code (`high`, `default`, `low`) con distribuzione congelata
60%/30%/10% e almeno un worker raggiungibile per coda. Verificare set globale,
set per coda, assenza di starvation, processo allocato per coda attiva, drain e
ritorno ai minimi. Ordering e priorità sono riportati come proprietà distinte:
algoritmi Horizon e Queen non sono assunti semanticamente identici.

## 6. Campagne prestazionali

### 6.1 Scaling matched

Profilo fixed, job da 10 ms, dispatch single, p4/a1, concorrenza
`1, 2, 4, 8`; 10.000 job e cinque ripetizioni per cella su host da almeno 10
CPU. Su host da quattro CPU il punto 8 è diagnostico e viene escluso dalla
curva primaria. L'ordine engine ruota per ripetizione.

Profilo auto `1..4`, stesso workload, 6.000 job e cinque ripetizioni. Metriche:
completion job/s, E2E p50/p95/p99, CPU/PSS control plane, CPU/RAM stack,
worker-second, tempo di scale-up, drain e ritorno al minimo.

Il profilo renewal viene confrontato paired con il profilo p4/a1 conservativo
per quantificare costo del helper; non si attribuisce causalità da campagne non
paired.

### 6.2 Partizioni e pop fusion

Discovery Queen Rust one-shot con partizioni `1, 4, 16, 64` e fusion `off/on`,
4 worker, p4/a1, 20.000 job zero-work e anchor ripetuto alla fine. Selezionare la
configurazione più piccola entro il 5% del massimo valido. Confermare candidato
e baseline con cinque ripetizioni; nessun outlier viene rimosso.

## 7. Soak

Eseguire prima uno soak diagnostico locale di almeno un'ora e poi uno soak
Linux dedicato di 24 ore per engine, una lane alla volta. Workload misto e
deterministico: job brevi sleep-bound, job CPU-bound, job lunghi entro timeout,
burst e periodi idle; seed e mix nei metadata.

Campionare almeno ogni cinque secondi: queue depth, throughput, latenze,
worker count, CPU, RSS/PSS/cgroup memory, throttling, connection errors, OOM e
restart. Gate: ledger conservato, coda drenata, nessun restart/OOM inatteso,
nessun failure terminale inatteso e nessuna crescita di memoria non bounded.
La pendenza RAM viene riportata con intervallo e grafico, non ridotta al solo
picco.

## 8. Fault injection

Ogni scenario usa almeno cinque backend freschi per engine e conserva timeline,
log, container state, ledger e queue state:

- SIGKILL del worker durante user code;
- SIGKILL del supervisor/master;
- stop/restart del broker o Redis con persistenza dichiarata;
- stop/restart PostgreSQL;
- isolamento rete app↔backend e broker↔PostgreSQL, quindi ripristino;
- latenza e perdita di pacchetti controllate, quando `tc/netem` è disponibile;
- storage pieno su volume usa-e-getta, seguito da recovery o fail-closed.

Il test di storage non riusa mai il backend danneggiato per una lane successiva.
Redis volatile e PostgreSQL durable non vengono chiamati equivalenti: i test di
recovery usano configurazioni persistenti dichiarate oppure riportano la perdita
attesa come differenza di topologia.

## 9. Statistica e reporting

- Tutte le ripetizioni vengono riportate; niente eliminazione post-hoc.
- Mediana, quartili, range e valori per-run.
- Rapporti paired candidato/Horizon soltanto per workload e host identici.
- Bootstrap paired deterministico 95% descrittivo e sign test esatto; nessuno è
  presentato come confidenza di popolazione con `n=5`.
- Nessun rapporto numerico tra architetture o host differenti.
- Screening, smoke, failure harness e conferme restano corpus separati.
- Raw artifact, log e report ricevono manifest SHA-256; i conteggi del corpus
  dichiarano inclusioni ed esclusioni.

## 10. Decisione GA

La release candidate non supera il gate generale se manca uno dei seguenti:

- lease renewal sicuro oppure default p1 per workload non bounded;
- failed-job lifecycle e multi-coda E2E;
- soak Linux 24h con ledger;
- fault worker/backend/rete/storage con recovery documentato;
- almeno una campagna Linux amd64 su host dedicato;
- nessuna regressione nelle suite PHP/Rust e negli artifact gate.

Linux arm64 è gate di supporto per pubblicare binari e claim arm64, non blocca
una GA esplicitamente limitata ad amd64. Dashboard e metriche operative restano
un workstream di feature parity separato e devono essere dichiarati prima di
presentare Queen come sostituto completo dell'esperienza Horizon.
