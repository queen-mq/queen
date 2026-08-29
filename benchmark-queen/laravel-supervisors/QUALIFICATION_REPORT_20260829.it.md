# Qualificazione tecnica Horizon vs Queen PHP vs Queen Rust

**Data:** 29 agosto 2026

**Branch prodotto e benchmark:** `feat/laravel-horizon-replacement`

**Commit misurato:** `a79db4de9bc9d59cdc6facf42a475bbb7a3dd7a1`

**Branch dashboard e report:** `feat/laravel-supervisor-dashboard`

**Classificazione:** performance `diagnostic`; feature/fault
`diagnostic_smoke`.

## 1. Conclusione tecnica

L'implementazione corretta non usa Queen come sostituto di `queue:work`: i
worker rimangono processi Laravel standard. Queen PHP o Queen Rust sono il
control plane che crea, dimensiona, drena, riavvia e distribuisce quei worker
fra le code, con configurazione condivisa e limiti equivalenti a un supervisor
Horizon.

I risultati supportano queste conclusioni nel perimetro testato:

1. Queen Rust elimina quasi tutta la memoria residente del master PHP: 2,9 MiB
   di PSS mediano contro 65,0 MiB Horizon sul commit finale.
2. P4/a1 rimuove il collo di bottiglia osservato con il vecchio prefetch 1.
   Nella conferma finale Queen Rust è +4,6% in completion e -11,2% in p95;
   nella conferma storica più lunga era +14,05% e -19,63%. Queste campagne
   non includevano il helper renewal ora obbligatorio per p>1 e restano quindi
   diagnostiche, non un claim prestazionale production.
3. Il risparmio del control plane non equivale a un risparmio dello stack:
   broker Queen più PostgreSQL consumano più RAM della topologia Redis usata
   dalla fixture Horizon.
4. Il recovery da SIGKILL conserva gli effetti idempotenti e dimostra
   semantica at-least-once, non exactly-once.
5. Failed job, multi-coda, renewal, controllo e distribuzione precompilata sono
   ora coperti nel client Laravel. Il pannello completa la prima control
   surface applicativa, limitata al master locale.

Queen Rust è quindi la scelta consigliata; Queen PHP resta fallback e
implementazione di riferimento. La qualificazione GA generale resta
condizionata ai gate Linux nativi e ai fault/soak estesi elencati nella sezione
12.

## 2. Perimetro confrontato

| Lane | Backend | Orchestratore | Worker |
| --- | --- | --- | --- |
| Horizon | Redis | master/supervisor Horizon PHP | `artisan horizon:work` |
| Queen PHP | Queen + PostgreSQL | master Laravel PHP | `artisan queue:work queen` |
| Queen Rust | Queen + PostgreSQL | `queen-supervisor` Rust | `artisan queue:work queen` |

Il benchmark usa la stessa applicazione Laravel, lo stesso job serializzato,
lo stesso sink JSONL e gli stessi limiti worker. Le metriche mantengono due
perimetri distinti:

- **control plane:** processi master/orchestratore, esclusi worker e backend;
- **stack misurato:** container applicativo consumer più backend completo,
  Redis per Horizon e broker+PostgreSQL per Queen.

Producer, observer e sampler non sono attribuiti allo stack, pur condividendo
l'host. Redis e Queen+PostgreSQL non hanno topologia o durabilità equivalenti;
il confronto rappresenta il costo delle configurazioni dichiarate, non un
microbenchmark broker contro broker.

## 3. Metodo e protezioni anti-bias

Il [protocollo GA](GA_PROTOCOL.it.md) è stato congelato prima della nuova
campagna; il [relativo emendamento](GA_PROTOCOL_AMENDMENT_PANEL.it.md) ha
registrato i gate del pannello prima dell'implementazione. Il protocollo è
progettato dal team Queen e non è una certificazione indipendente.

La procedura applicata comprende:

- una lane alla volta, backend e volumi freschi per ogni campione;
- workload, worker, cgroup e configurazione matched;
- rotazione dell'ordine Horizon/Queen PHP/Queen Rust;
- set esatto dei job e quiescenza della coda come gate preliminare;
- nessuna eliminazione post-hoc degli outlier;
- mediane, quartili, valori per-run e bootstrap paired deterministico;
- snapshot dei container e watcher continuo degli eventi Docker;
- fallimento chiuso su checkout dirty, image/config mismatch, container
  estraneo o sostituito, restart/OOM, sampler incompleto e coda non vuota;
- artifact raw conservati anche quando un gate fallisce.

Il fail-closed è stato verificato concretamente. La prima campagna finale aveva
9/9 run funzionalmente validi, ma una modifica locale non pertinente
(`benchmark-queen/.DS_Store`) rendeva `git.dirty=true`: tutte le statistiche
paired sono state soppresse. Dopo aver preservato quella modifica fuori dalla
worktree, la campagna è stata ripetuta sullo stesso commit clean ed è risultata
comparabile. Non è stato usato un override.

Gli intervalli bootstrap descrivono soltanto la variazione dei run su questo
host. Con n=3 non sono garanzie di popolazione e non eliminano bias di host,
topologia o workload.

## 4. Ambiente della conferma finale

| Campo | Valore |
| --- | --- |
| Host | macOS 26.5, arm64 |
| Runtime | Docker Desktop / Engine 29.7.2 |
| Kernel container | LinuxKit 7.0.12, aarch64 |
| Risorse esposte | 10 CPU, 8.214.851.584 byte RAM |
| cgroup | v2 |
| Profilo | fixed, 4 worker, p4/a1 |
| Lease renewal | disabilitato; profilo oggi ammesso solo come evidenza diagnostica |
| Workload | 2.000 job per lane, sleep 10 ms |
| Ripetizioni | 3 per motore, 9 totali |
| Validità | 9/9 corrette, quiescenti, isolate |
| Qualificazione | `diagnostic` |

Docker Desktop non è un host Linux nativo dedicato. I numeri sono utili per
diagnosi, regressione e scelta progettuale, ma non sono ancora un claim
publishable multipiattaforma.

## 5. Risultati finali sul commit a79db4de

Valori mediani; aggregati completi nell'[artifact della campagna
clean](results/final-a79db4de-fixed-safe-clean/20260829T075448Z-a79db4de9b/campaign-stats.md).

| Metrica | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion job/s | 294,30 | 300,08 | 307,67 |
| E2E p95 ms | 6.019,54 | 5.478,43 | 5.266,49 |
| PSS orchestratore MiB | 65,0 | 35,1 | 2,9 |
| CPU stack s | 3,750 | 4,148 | 3,938 |
| RAM stack MiB | 198,1 | 295,1 | 265,3 |

Rapporti candidate/Horizon, paired per ripetizione:

| Candidato | Throughput | E2E p95 | PSS orch. | CPU stack | RAM stack |
| --- | --- | --- | --- | --- | --- |
| Queen PHP | 1,018×, CI 0,937–1,041 | 0,924×, CI 0,874–0,933 | 0,541× | 1,061×, CI 1,018–1,110 | 1,488× |
| Queen Rust | 1,046×, CI 1,044–1,064 | 0,888×, CI 0,850–0,898 | 0,044× | 1,031×, CI 0,950–1,060 | 1,341× |

Lettura corretta:

- Queen Rust completa il 4,6% in più di job/s e riduce la p95 dell'11,2% nel
  campione finale;
- il PSS del control plane Rust scende del 95,6%;
- la CPU stack è compatibile con la parità perché l'intervallo attraversa 1;
- la RAM stack cresce del 34,1%, soprattutto per il diverso backend;
- Queen PHP riduce il PSS master del 45,9%, ma la sua CPU stack è maggiore nel
  campione finale e il throughput non è conclusivo.

Lo [smoke breve](results/final-a79db4de-smoke/20260829T074910Z-a79db4de9b/report.md)
da 60 job è conservato, ma non viene usato per la decisione: startup e drain
dominano una finestra così corta.

## 6. Evidenza storica di ottimizzazione

La campagna notturna ha isolato il principale errore di configurazione: p1/a1
limitava Queen a circa 118–121 job/s contro 262 Horizon. La conferma p4/a1 sul
commit `3655cd2a`, 4 worker, 10.000 job da 10 ms e 5 run per motore, ha
registrato 15/15 run validi e 150.000/150.000 job unici:

| Metrica mediana | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion job/s | 253,38 | 289,17 | 289,13 |
| E2E p95 ms | 37.547 | 30.403 | 30.067 |
| PSS orchestratore MiB | 71,05 | 35,15 | 2,87 |
| CPU stack s | 27,19 | 16,56 | 16,42 |
| RAM stack MiB | 217,66 | 338,23 | 303,63 |

Per Queen Rust i rapporti erano +14,05% completion, -19,63% p95, -95,95%
PSS orchestratore, -39,94% CPU stack e +39,30% RAM stack. Questo corpus usa
un commit precedente ed è riportato separatamente; non viene combinato con la
conferma a79db4de.

Il corpus storico selezionato contiene 99 run e 1.248.000 job validi e
quiescenti. Ha inoltre osservato:

- auto p4/a1: Queen Rust -3,3% job/s, -29,9% CPU stack e -96,0% PSS master;
- CPU-bound: throughput sostanzialmente in parità;
- stress 50k zero-work: Queen Rust circa 2,4× Horizon;
- ACK batch 4/16: throughput maggiore, ma duplicazioni superiori nei fault;
- a 8 worker il vantaggio non è confermato: i CI paired attraversano 1.

Questi scenari orientano la configurazione ma non autorizzano a dichiarare un
singolo rapporto universale.

## 7. Correttezza, failed job e multi-coda

Il [test feature parity finale](results/final-a79db4de-feature-parity/report.md)
ha usato backend freschi e ha verificato:

| Motore | Multi-coda | Failed lifecycle | Worker identity | Container gate |
| --- | --- | --- | --- | --- |
| Horizon | pass | n/a | pass | pass |
| Queen PHP | pass | pass | pass | pass |
| Queen Rust | pass | pass | pass | pass |

Per ogni motore sono stati completati esattamente 24/24 job su due code e lo
stato finale è vuoto. PID e Linux start ticks escludono una sostituzione worker
non osservata. Per Queen, il test crea un job terminalmente fallito, verifica
sia il repository Laravel sia il broker DLQ, esegue `queue:retry`, osserva il
successo e richiede entrambi gli store vuoti.

Il provider failed-job sincronizzato usa un fence di retry casuale one-shot e
un lock distribuito che copre validazione, pulizia DLQ, publish transazionale e
rimozione del vecchio record. In una topologia multi-host quel lock deve essere
realmente condiviso; un file lock locale non è sufficiente.

## 8. Fault e semantica di consegna

Nel [fault finale](results/final-a79db4de-fault-recovery/report.md) un worker
viene ucciso durante user code:

| Motore | Respawn | Unici | Missing | Effetti | Retry | Coda zero | At-least-once | Strict |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| Horizon | 997,5 ms | 24/24 | 0 | 24 | sì | sì | pass | fail |
| Queen PHP | 1.509,5 ms | 24/24 | 0 | 24 | sì | sì | pass | fail |
| Queen Rust | 1.442,8 ms | 24/24 | 0 | 24 | sì | sì | pass | fail |

Il fallimento strict è atteso: un tentativo interrotto viene ripetuto. Il
ledger SQLite della fixture conserva un solo effetto per `(run_id, job_id)`, ma
non rende atomici ACK e side effect di un database o servizio esterno.
L'applicazione deve essere idempotente per tollerare l'at-least-once.

I fault storici mostrano perché ACK batch resta 1 per il profilo generale:
p4/a1 post-guard ha osservato strict 15/15 in quello specifico timing, contro
3/10 per p4/a4 e 1/10 per p16/a16. Non è una garanzia exactly-once: il fault
finale durante user code fallisce strict per tutti i motori anche con a1.
Tutte le lane conservano invece at-least-once.

## 9. Lease e profili supportati

Il profilo senza helper è sicuro soltanto con prefetch 1 e:

```text
retry_after > worst_case_runtime_del_singolo_job + margine
```

P1/a1 è la scelta conservativa senza helper quando il runtime del singolo job
resta bounded sotto `retry_after`. Ogni `prefetch > 1` richiede invece renewal:
Laravel può trattenere il tail già leased durante maintenance mode,
`queue:pause` o un listener `Looping` per una durata non limitabile con la sola
formula timeout/rest. Il profilo prestazionale production da qualificare è
quindi p4/a1 con helper; i numeri p4/a1 della sezione 5 misurano il potenziale
del batching senza quel costo e devono essere ripetuti con renewal.

Il rinnovo considera riuscita solo una risposta con `renewed > 0`, valida
l'eventuale scadenza RFC 3339 e, se non può più garantire la lease, ferma e
fencea il worker prima che inizi altro lavoro. Non trasforma comunque la
consegna in exactly-once.

Gli smoke conservati includono due job PHP da 20 s completati con renewal,
nonché un drain Rust durante il secondo job da 10,5 s di una lease prefetched:
2/2 completion, zero duplicati, coda vuota e ledger passato. Nel fault renewal
con broker indisponibile il helper fencea il worker; dopo il ripristino il job
viene eseguito al secondo tentativo, con un solo effetto idempotente e coda
vuota. È evidenza di fail-safe/at-least-once, non di exactly-once.

## 10. Distribuzione con il client Laravel

Il crate Rust rimane nella root perché ha toolchain, lockfile e lifecycle di
build propri; la **distribuzione** è però coordinata dal pacchetto Laravel:

```console
php artisan queen:supervisor-install
vendor/bin/queen-supervisor --php php --artisan artisan
```

Composer installa il launcher, non esegue download automatici. Il comando
esplicito:

1. rileva OS e architettura senza fallback cross-platform;
2. scarica manifest e archivio versionati via HTTPS, oppure usa mirror/offline;
3. richiede versione, target, filename e SHA-256 esatti;
4. rifiuta symlink, file non regolari, input oversized e archive entry
   inattese;
5. richiede che il parent immediato della base esista già come directory reale
   fidata, lo fissa per device/inode e crea soltanto la foglia finale della base;
6. pubblica atomicamente sotto
   `storage/queen-supervisor-bin/<version>/<os>-<arch>`, mantenendo il cwd
   fissato e usando path relativi per lock, file temporanei e publish;
7. fissa e ricontrolla base, directory di versione e target prima di eseguire
   `./<temp-binary> --version`, quindi scrive una receipt locale;
8. il launcher ricontrolla receipt e hash a ogni avvio, fissa nuovamente base,
   versione e target e usa `pcntl_exec('./queen-supervisor')`, senza lasciare
   un processo PHP residente. I path CLI relativi sono prima risolti rispetto
   alla directory applicativa originale.

Manifest e receipt includono il commit sorgente. Un deploy può fornire
`--manifest-sha256` (o la variabile equivalente) con il digest ottenuto dopo
verifica Sigstore: il confronto avviene prima di selezione, estrazione o
esecuzione dell'archivio.

La pipeline usa Rust 1.88 e dipendenze locked, costruisce binario e archive due
volte e richiede identità byte-a-byte. Pubblica checksum, manifest canonico,
bundle Sigstore e attestazioni di provenance. La GitHub Release nasce come
draft; nomi e digest remoti di tutti gli asset vengono confrontati con `dist/`
prima della pubblicazione. Il job preparatorio risolve il
tag in un commit SHA immutabile, tutti i checkout successivi usano quello SHA
e il job di pubblicazione rilegge il tag remoto e fallisce se è stato spostato.
Rustfmt, test, Clippy, la suite Laravel, Composer validate/audit e il vero
installer Linux amd64 sono gate della release. L'installer non verifica un
digest indipendente o la firma del manifest per default: senza un pin esplicito
si affida a HTTPS e ai controlli della release, oppure a un file/mirror già
fidato. Valida quindi schema, versione, commit e target e usa lo SHA-256
contenuto nel manifest per verificare l'archivio. Sigstore resta un passaggio
esplicito del mirror/deploy ad alta assurance; il digest risultante può essere
imposto all'installer. Base uguale alla root, traversal e symlink nelle
directory dinamiche sono rifiutati. Il parent immediato deve già esistere ed
essere reale; se `storage/` è un symlink, il path di installazione deve puntare
al target reale. Un principal distinto che possa rinominare un ancestor
scrivibile può causare un fail/DoS, ma il cwd pin impedisce di deviare gli exec;
un processo con lo stesso UID effettivo del supervisor è nel trust boundary.

| OS | amd64/x64 | arm64 | Stato |
| --- | --- | --- | --- |
| Linux | musl statico, runner nativo | musl statico, runner nativo | preview pipeline |
| macOS 12+ | binario nativo predisposto | binario nativo predisposto | preview pipeline |
| Windows | — | — | rifiutato esplicitamente |

Al momento del report non esiste ancora un tag/release pubblico
`supervisor/v*`, quindi nessuno dei quattro asset è già distribuito. Il
workflow è predisposto per doppia build/archive, smoke del binario e test del
vero installer Laravel su Linux amd64. Linux arm64 e i due target macOS
usano runner nativi ma non hanno ancora test E2E Laravel/process-tree né firma
Developer ID/notarizzazione. La qualifica nativa e la verifica della provenance
di ciascun asset restano gate di release.

Il repository deve inoltre abilitare GitHub immutable releases e un ruleset
che impedisca modifica/cancellazione dei tag `supervisor/v*`. Il workflow
riduce la finestra creando una draft, confrontando i digest e ricontrollando il
tag immediatamente prima della pubblicazione, ma le policy server-side sono la
garanzia che impedisce mutazioni successive. La pubblicazione del package
Composer deve seguire soltanto lo smoke dei veri URL appena pubblicati; oggi
questa coordinazione resta procedurale.

Windows necessita di un backend dedicato:

- Job Objects per possedere e terminare l'intero albero dei worker;
- console control events o un protocollo equivalente per pause/drain;
- locking e atomicità con semantiche Windows;
- fencing dell'istanza e test dei crash senza segnali/process group Unix;
- runner CI x64/arm64 e suite E2E native.

L'ordine consigliato è Windows x64 prima di ARM64. Il primo incremento deve
astrarre il backend dei processi Rust, assegnare ogni worker a un Job Object
con kill-on-close, sostituire segnali e process group con controllo cooperativo
o console events, usare locking Windows e DACL private, rifiutare reparse point
e aggiungere naming/launcher `.exe` senza `pcntl_exec`. Solo dopo crash, drain,
takeover e install E2E su runner Windows nativi il target può entrare nel
manifest. Windows ARM64 segue quando toolchain e runtime PHP/Laravel sono stati
qualificati nativamente.

Pubblicare oggi un `.exe` ottenuto per cross-compilazione darebbe una falsa
promessa operativa. WSL può usare l'artefatto Linux, ma non equivale a supporto
Windows nativo.

## 11. Pannello di supervisione

Il pannello Laravel copre il control plane locale, mentre la dashboard broker
Queen continua a coprire code, analytics e DLQ. La prima versione espone:

- live/stale, engine, instance ID, PID e heartbeat;
- stato globale paused/running e pool con worker attivi o draining;
- configurazione risolta non sensibile e profondità code quando disponibili;
- riepilogo failed job senza payload, exception body, token o header;
- POST `pause`, `continue`, `terminate`, protette da autorizzazione, CSRF e
  expected `instance_id`; l'accettazione usa POST/Redirect/GET con risposta 303
  e indica soltanto che il comando fenced è pending.

È disabilitata di default e deny-by-default in production finché
l'applicazione non definisce il Gate `viewQueenDashboard`. Usa asset locali,
escaping Blade, Content Security Policy e `no-store`; backend o status non
disponibili diventano stati espliciti, non errori che rivelano dettagli.

Il pannello legge lo state directory del singolo master. Non offre una vista
multi-host e non rende due supervisor active-active: ciò richiederebbe
heartbeat centralizzati e leadership distribuita fenced.

Lo status `v1` include una allowlist della configurazione effettivamente
caricata dalla generazione in esecuzione, senza URL, token o header. Dashboard
e CLI usano da quello stesso snapshot pool, profondità, `control_ttl` e
`heartbeat_timeout`: una modifica o una config cache ricostruita prima del
riavvio non può cambiare retroattivamente la validità del master corrente. I
due motori limitano a 256 i pool pubblicabili e a 1 MiB configurazione e status;
telemetry, PID e code hanno cardinalità e dimensioni bounded. I worker in drain
restano inclusi nel `process_limit` finché non sono realmente usciti.
Un errore di pubblicazione dello status è fail-fast: il master drena i worker
ed esce non-zero. Dopo un kill forzato mantiene inoltre il lock di generazione
finché ogni child non è osservato terminato, evitando takeover e capacità
duplicata durante un kill fallito o ritardato.

Il riepilogo failed job è intenzionalmente read-only e bounded: non espone né
restituisce payload o exception e non esegue `COUNT(*)` a ogni refresh. Il
driver file deve comunque decodificare il documento Laravel bounded prima di
scartare quei campi. Retry, forget,
flush e prune rimangono nei comandi Laravel, mentre ispezione e operazioni DLQ
globali restano nella dashboard broker. L'etichetta di policy dell'indice è
inferita dalla connessione configurata e non dichiara l'esistenza live della
corrispondente riga DLQ. Di conseguenza il gate congelato
«accesso al lifecycle» è soddisfatto solo in parte dalla prima versione: lo
stato dell'indice è visibile e il percorso operativo è documentato, ma non ci
sono ancora azioni lifecycle web dirette.

## 12. Stato readiness e gate residui

| Area | Stato | Nota |
| --- | --- | --- |
| Supervisor PHP/Rust | pronto RC nel perimetro Unix | singolo master |
| Autoscaling/multi-coda | funzionale | scaling esteso ancora da replicare su Linux |
| Failed lifecycle | funzionale | lock condiviso obbligatorio multi-host |
| Lease renewal | implementato, unit/E2E smoke | obbligatorio con p>1; p1 senza helper solo se il job è bounded |
| Dashboard Laravel | implementata sul branch dedicato | locale, non multi-host |
| Control/status protocol | generation-fenced e bounded | rolling incompatibile fail-closed |
| Lifecycle failed job nel pannello | parziale | metadata bounded; mutazioni via Artisan/broker |
| Release Linux/macOS x64/arm64 | target preview predisposti | nessun asset pubblico; gate nativi e firma macOS |
| Windows nativo | non supportato | backend processi da implementare |
| Performance publishable | non ancora | host attuale Docker Desktop |
| At-least-once worker fault | verificato | non exactly-once |
| Fault backend/rete/storage | incompleto | gate GA |
| Soak Laravel 24 h | incompleto | gate GA |

Prima di un claim GA generale:

1. ripetere fixed/auto/scaling su host Linux dedicati amd64 e arm64;
2. eseguire soak Laravel 24 h per motore con ledger e analisi della pendenza
   RAM;
3. iniettare fault broker, PostgreSQL, rete, latenza/perdita e storage pieno;
4. completare la matrice 1/2/4/8 worker, tre code, partizioni 1/4/16/64 e pop
   fusion off/on;
5. eseguire smoke nativi e verifica provenance per ogni artefatto di release;
6. documentare l'idempotenza richiesta e mantenere ACK batch 1 come default;
7. per Windows, completare prima il backend nativo e la suite E2E.
8. decidere se completare le azioni failed-job web oppure registrare una
   revisione esplicita del gate del pannello prima della GA.
9. aggiungere firma/notarizzazione macOS, abilitare immutable releases/ruleset
   tag e automatizzare il rilascio GitHub-then-smoke-then-Composer.

## 13. Verifica software finale

La verifica sul branch dashboard, oltre ai test di scenario descritti sopra,
ha prodotto:

| Verifica | Esito |
| --- | --- |
| PHPUnit completo, PHP 8.5.9 | 473 test, 1.992 assertion, pass |
| PHPUnit dashboard focalizzato | 21 test, 117 assertion, pass |
| PHPUnit supervisor focalizzato | 108 test, 474 assertion, pass |
| PHPUnit installer focalizzato | 21 test, 146 assertion, pass |
| Rust `cargo test --locked` | 53 test, pass |
| Harness benchmark Python | 48 test, pass |
| Tooling release Python | 5 test, pass |
| Composer validate e audit | valido, nessun advisory |
| PHP lint (103 file), Rustfmt, Clippy `-D warnings` | pass |
| Ruff, ShellCheck, Bash syntax, Actionlint | pass |
| Lingua di test/fixture modificati | inglese, pass |

Questa matrice verifica codice, contratti, parser e workflow statico; non
sostituisce l'esecuzione della pipeline sui quattro runner target né i gate
Linux nativi e Windows indicati sopra. La policy aggiunta a
`CONTRIBUTING.md` richiede in inglese nomi, commenti, fixture, output e messaggi
di assertion dei test; i report di qualificazione restano invece in italiano.

## 14. Il claim “1 milione di messaggi/s”

Il [soak broker-native storico](../2026-08-11-soak24-1M/results.md) ha misurato
circa 999.652 push accettati/s medi per 24 ore con loader Go, 200 partizioni,
600 consumer, host dedicati e rete 10 Gbit/s. È compatibile con la capacità
broker dichiarata, ma non attraversa serialization Laravel, PHP worker,
orchestrazione o side effect dell'applicazione.

Non è quindi in conflitto con i 250–310 job Laravel/s di questi workload, e
non deve essere presentato come 1 milione di job Laravel/s o come prova
exactly-once. I due benchmark rispondono a domande diverse.

## 15. Artifact principali

- [Protocollo GA](GA_PROTOCOL.it.md)
- [Emendamento dashboard](GA_PROTOCOL_AMENDMENT_PANEL.it.md)
- [Conferma finale clean](results/final-a79db4de-fixed-safe-clean/20260829T075448Z-a79db4de9b/campaign-stats.md)
- [Esempio fail-closed dirty](results/final-a79db4de-fixed-safe/20260829T075042Z-a79db4de9b/campaign-stats.md)
- [Feature parity finale](results/final-a79db4de-feature-parity/report.md)
- [Fault finale](results/final-a79db4de-fault-recovery/report.md)
- [Graceful drain Rust con renewal](results/final-rust-graceful-drain/gate.json)
- [Renewal PHP, runtime oltre lease](results/lease-renewal-success-php/result.json)
- [Fault renewal Rust e retry](results/lease-renewal-fault-retry-rust-v2/ledger-analysis.json)
- [Sintesi storica notturna](results/nightly-20260828/report-sintetico.it.md)
- [Report storico notturno](results/nightly-20260828/report-tecnico.it.md)
- [README e riproduzione](README.md)

Gli artifact sotto `results/` sono intenzionalmente esclusi dal repository Git
perché voluminosi, ma sono preservati nella workspace. I due report di
qualificazione e i protocolli sono invece versionati.
