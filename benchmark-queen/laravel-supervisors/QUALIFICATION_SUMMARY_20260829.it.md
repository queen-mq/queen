# Horizon vs Queen PHP vs Queen Rust — sintesi di qualificazione

**Data:** 29 agosto 2026

**Classificazione:** risultati prestazionali `diagnostic`, eseguiti su Docker
Desktop arm64; test funzionali e di fault `diagnostic_smoke`.

**Codice misurato:** `a79db4de9bc9d59cdc6facf42a475bbb7a3dd7a1`.

## Esito

Queen Rust è il candidato consigliato per sostituire il control plane di
Horizon. Queen PHP resta il motore di riferimento e il fallback operativo. Sul
commit finale misurato, con quattro worker, 2.000 job da 10 ms, prefetch 4,
ACK batch 1 e renewal disabilitato, tutte le 9 lane sono risultate corrette,
quiescenti e isolate. Questo profilo è ora classificato solo diagnostic:

| Mediana, 3 run | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion job/s | 294,30 | 300,08 | 307,67 |
| E2E p95 | 6.019,54 ms | 5.478,43 ms | 5.266,49 ms |
| PSS orchestratore | 65,0 MiB | 35,1 MiB | 2,9 MiB |
| CPU stack misurato | 3,750 s | 4,148 s | 3,938 s |
| RAM stack misurato | 198,1 MiB | 295,1 MiB | 265,3 MiB |

Nei rapporti paired Queen Rust misura **+4,6% throughput**, **-11,2% p95** e
**-95,6% PSS del solo orchestratore** rispetto a Horizon. La CPU dello stack è
inconclusiva con tre coppie (rapporto 1,031; bootstrap descrittivo 95%
0,950–1,060). La RAM dell'intera topologia misurata è **+34,1%**: Horizon usa
Redis, Queen include broker e PostgreSQL. Il vantaggio di memoria riguarda
quindi il control plane, non automaticamente tutto lo stack.

La campagna storica più lunga sul commit `3655cd2a`, cinque run e 10.000 job
per lane, aveva misurato Queen Rust a +14,05% throughput, -19,63% p95,
-95,95% PSS dell'orchestratore e -39,94% CPU stack, ma +39,30% RAM stack. È
evidenza di ottimizzazione utile, non viene fusa con la conferma sul commit
finale.

## Affidabilità e operatività

- Multi-coda: 24/24 job per motore, set esatto, code vuote e identità dei
  worker stabile; tutte le tre lane superano il gate.
- Failed job Queen: record Laravel e snapshot DLQ presenti, retry manuale
  riuscito, entrambi gli indici puliti; Queen PHP e Rust superano il gate.
- SIGKILL worker: Horizon, Queen PHP e Queen Rust conservano 24/24 effetti,
  osservano il retry e drenano la coda. Il gate at-least-once passa 3/3; il
  gate strict fallisce come previsto perché il tentativo interrotto viene
  rieseguito.
- Lease: una coda Laravel può restare in pausa per tempo non bounded mentre
  conserva il tail prefetched. Perciò p4/a1 richiede sempre renewal; p1/a1 può
  restare senza helper solo se il singolo job termina entro la lease.
- La dashboard Laravel aggiunge stato live/stale, pool e worker, riepilogo
  failed-job bounded e comandi fenced `pause`, `continue`, `terminate`. Le
  mutazioni dei failed job restano nei comandi Artisan e le operazioni DLQ
  globali nella dashboard broker: il relativo gate del pannello è quindi
  parziale. La vista è locale al singolo master, non active-active multi-host.
- Il protocollo di stato pubblica la configurazione effettiva della generazione
  attiva: dashboard e CLI non reinterpretano una config Laravel eventualmente
  cambiata dopo l'avvio. Heartbeat e TTL dei comandi sono generation-specific;
  pool, processi, documenti e telemetry hanno limiti fail-closed condivisi dai
  due motori.

## Distribuzione

Il pacchetto Laravel distribuisce launcher e installer esplicito; il binario
Rust non viene scaricato durante Composer. Release e client sono versionati
insieme. L'installer seleziona l'artefatto esatto, accetta il manifest da HTTPS
o da una sorgente locale fidata, ne valida schema/versione/target, verifica lo
SHA-256 dell'archivio, la versione eseguibile e la receipt locale. La pipeline
di release costruisce e impacchetta due volte con confronto byte-a-byte e
pubblica bundle Sigstore e provenance. Checkout e build sono fissati al commit
immutabile preparato e il tag viene ricontrollato prima della pubblicazione. La
firma del manifest è verificata dal deploy o dal mirror ad alta assurance, non
automaticamente dall'installer; questi possono però imporre al client lo
SHA-256 del manifest già verificato prima di estrarre o eseguire il binario. La
release viene preparata come draft e pubblicata solo dopo verifica di elenco e
digest di tutti gli asset. L'installer richiede che il parent immediato della
base esista già e sia una directory reale fidata, lo fissa prima di creare la
sola foglia finale e poi fissa base, versione e target per device/inode. Il
launcher ripete il pin di base, versione e target e usa path relativi per
l'eseguibile: una rinomina di un ancestor fra check ed exec non può deviare il
binario. Root filesystem, traversal e symlink nelle directory dinamiche sono
rifiutati; se `storage/` è un symlink va configurato il suo target reale. Lo
stesso UID effettivo del supervisor resta parte esplicita del perimetro fidato.

| Piattaforma | x64 / amd64 | ARM64 |
| --- | --- | --- |
| Linux | target predisposto, musl statico nativo | target predisposto, musl statico nativo |
| macOS | preview nativa, firma Apple pending | preview nativa, firma Apple pending |
| Windows | non ancora supportata | non ancora supportata |

Al momento del report non esiste ancora un tag/release pubblico
`supervisor/v*`: la matrice descrive ciò che la pipeline è pronta a generare,
non asset già distribuiti. La GA richiede smoke/installer nativi per ogni
target; macOS richiede inoltre firma Developer ID e notarizzazione.
Prima della prima release vanno abilitate anche immutable releases e una
protezione server-side dei tag `supervisor/v*`; la successiva pubblicazione
Composer deve essere subordinata allo smoke degli URL reali.

Windows richiede backend nativi per Job Objects, console control events,
locking e fencing dei processi. Una semplice cross-compilazione non renderebbe
sicuri pause, drain e shutdown; la release rifiuta esplicitamente Windows.
Il percorso previsto è Windows x64 per primo, con test E2E nativi, e ARM64 dopo
la qualifica della disponibilità PHP/Laravel sul target.

## Decisione

Usare Queen Rust come orchestratore predefinito e Queen PHP come fallback.
P1/a1 è il profilo conservativo senza helper; il profilo prestazionale di
produzione è p4/a1 con renewal. La campagna p4/a1 qui riportata non includeva
il helper e resta quindi diagnostica: deve essere ripetuta con renewal prima
di un claim prestazionale GA. Non pubblicare ancora un claim GA
universale: restano da eseguire le campagne publishable su Linux nativo
amd64/arm64, soak Laravel 24 h e fault broker/rete/PostgreSQL/storage. Il claim
storico di circa 1 milione di messaggi/s riguarda il broker nativo con loader
Go, 200 partizioni e 600 consumer, non job Laravel/s.

Verifica software finale: suite PHP completa 473 test/1.992 assertion, suite
Rust 53 test, harness benchmark 48 test e tooling release 5 test; Composer
validate/audit, lint dei 103 file PHP, Rustfmt, Clippy, Ruff, ShellCheck e
Actionlint verdi. Nomi, commenti, fixture, output e messaggi di assertion dei
test modificati sono in inglese; la regola è ora esplicita in `CONTRIBUTING.md`.

Il dettaglio metodologico, i limiti e gli artifact sono nel
[report tecnico](QUALIFICATION_REPORT_20260829.it.md).
