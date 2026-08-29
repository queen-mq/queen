# Horizon vs Queen PHP vs Queen Rust — sintesi decisionale

**Stato:** report auditabile, 29 agosto 2026. Risultati Laravel `diagnostic`;
fault test `diagnostic_smoke`.

Fonti: [protocollo locale congelato](protocol.md) e
[report tecnico completo](report-tecnico.it.md).

## Decisione

Queen può sostituire il control plane di Horizon nel percorso testato, ma la
configurazione conta quanto l'implementazione:

- **Queen Rust è l'orchestratore consigliato**; Queen PHP resta il fallback di
  portabilità.
- Lo **shipping default resta p1/a1** (prefetch 1, ACK batch 1). È prudente ma
  non performance-equivalent: nel legacy Queen PHP/Rust misurano
  118,15/121,39 job/s contro 261,85 Horizon, circa -55%.
- **p4/a1 è il profilo prestazionale short-job**: sul commit post-guard
  misurato 3655cd2a supera Horizon di circa il 14%, ma richiede runtime bounded
  e `retry_after > prefetch × worst-case runtime + margine`.
- Ogni ACK batch >1 resta opt-in per job ed effetti esterni idempotenti: sotto
  SIGKILL aumenta sensibilmente la duplicazione osservata.

Non è ancora un claim generale di piena sostituibilità production-ready: i
test coprono performance e recovery, non feature parity con dashboard,
metriche, failed-job store, pause/resume, ordering e priorità Horizon.

## Risultato primario post-guard

La [fixed primaria](confirm-fixed-safe-head/20260829T035034Z-3655cd2a63/campaign-stats.md)
usa quattro worker, 10.000 job da 10 ms, p4/a1, timeout 120 s e retry_after
481 s. Commit clean 3655cd2a, 15/15 run validi e quiescenti, 150.000/150.000
job unici, zero missing/duplicati/failure, OOM, restart, throttling o errori del
sampler.

Mediane; lo “stack misurato” è applicazione consumer supervisor+worker più
backend completo. Producer, observer e sampler sono esclusi.

| Engine | Completion job/s | E2E p95 | CPU orch. | PSS orch. | CPU stack | RAM stack |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Horizon | 253,38 | 37.547 ms | 0,1445 s | 71,05 MiB | 27,19 s | 217,66 MiB |
| Queen PHP | 289,17 | 30.403 ms | 0,0320 s | 35,15 MiB | 16,56 s | 338,23 MiB |
| Queen Rust | 289,13 | 30.067 ms | 0,0141 s | 2,87 MiB | 16,42 s | 303,63 MiB |

Rapporti paired candidato/Horizon, mediana con bootstrap descrittivo 95%:

| Metrica | Queen PHP | Queen Rust |
| --- | ---: | ---: |
| Completion | 1,1428 [1,1345–1,1515] | 1,1405 [1,1205–1,1632] |
| E2E p95 | 0,8096 [0,8036–0,8141] | 0,8037 [0,7974–0,8221] |
| CPU orchestratore | 0,2184 [0,2080–0,2269] | 0,0987 [0,0902–0,1011] |
| PSS orchestratore | 0,4948 [0,4948–0,4967] | 0,0405 [0,0404–0,0405] |
| CPU stack | 0,6057 [0,5933–0,6131] | 0,6006 [0,5916–0,6040] |
| RAM stack | 1,5540 [1,5216–1,5620] | 1,3930 [1,3893–1,4201] |

Queen Rust è quindi +14,05% in completion, -19,63% in p95, -90,13% in CPU e
-95,95% in PSS dell'orchestratore, -39,94% in CPU stack e +39,30% in RAM
stack. Il risparmio CPU del solo orchestratore è però appena 0,1445→0,0141 s,
circa 130 ms su un drain di 39,5 s: PSS e CPU consumer+backend sono più
materiali.

Le due lane Queen superano Horizon in tutti i 5/5 paired run. Con cinque segni
concordi il sign test esatto bilaterale è p=0,0625: il bootstrap descrive i
rapporti osservati su questo host, non significatività o confidenza di
popolazione.

La p95 è enqueue→completion e incorpora arrival shape e dispatch, non è
latenza broker isolata. Headline favorisce Queen (~1,186×), mentre il dispatch
Horizon è ~1,8× più rapido; il producer è fuori dai cgroup contabili ma
condivide la VM e può introdurre contention.

## Altri scenari

| Scenario | Horizon | Queen PHP | Queen Rust | Lettura |
| --- | ---: | ---: | ---: | --- |
| Auto p4/a1 post-guard, job/s | 243,75 | 234,13 | 235,69 | PHP -5,0%; Rust -3,3%; 9/9 validi |
| Auto p16/a16, job/s | 245,22 | 240,47 | 244,96 | Rust in parità; ACK16 opt-in |
| CPU-bound, job/s | 359,49 | 365,02 | 364,05 | sostanziale parità; CPU stack ~+2% |
| Stress 50k zero-work, job/s | 841,97 | 2.042,17 | 2.007,13 | Queen ~2,4×; backend Queen throttled |
| Fixed p16/a16, job/s | 255,96 | 295,91 | 293,85 | +15,6/+14,8%; non A/B causale vs p4 |

Nell'auto p4/a1 post-guard il picco a quattro worker viene rilevato a 4,653 s
Horizon, 8,644 s PHP e 8,631 s Rust; worker-second 127,27/111,53/111,53
(-12,4% Queen). Rust usa -29,9% CPU stack e -96,0% PSS orchestratore, con RAM
stack +42,1%. Gli algoritmi non sono semanticamente identici. A un worker Queen è circa +2%; a otto worker
le mediane sono 6–8% sotto Horizon ma i CI n=3 attraversano 1. W1/w8 non sono
una curva matched con il w4 primario.

La discovery ACK4 zero-work arriva a 7.306,88 job/s PHP e 6.989,41 Rust,
~3,58×/~3,48× rispetto allo stress a1, ma è un confronto cross-campaign non
paired e non trasferibile a job applicativi reali.

## Recovery e affidabilità

Il [fault p4/a1 post-guard](fault-p4-a1-guarded-r01/report.md) usa due worker,
24 job da 2 s, timeout 10 s e retry_after 41 s: il guard richiede >4×10=40 s.
Nei cinque round per engine:

| Profilo | Lane ALO | Lane strict | Unici | Duplicati | Respawn mediano H/PHP/Rust |
| --- | ---: | ---: | ---: | ---: | --- |
| p4/a1 post-guard | 15/15 | 15/15 | 360/360 | 0 | 86,7 / 1.614 / 2.932 ms |
| p4/a4 | 10/10 | 3/10 | 240/240 | 7 | solo Queen |
| p16/a16 pre-guard | 10/10 | 1/10 | 240/240 | 17 | solo Queen |

Totale fault selezionato: **35/35 at-least-once, 19/35 strict, 840 job unici,
24 duplicati**; queue finale e gate OOM/restart passano 35/35. I vecchi 15
round p4/a1 pre-guard restano corroborazione e sono sostituiti uno-a-uno. Il
pilot p16/a16 con lease insufficiente è preservato fuori totale: entrambe le
lane restarono non quiescenti, rendendo visibile il bisogno del guard.

Il retry in-flight p4/a1 post-guard compare intorno a 42,3/40,6/40,5 s per
Horizon/PHP/Rust. La fixed primaria usa invece timeout 120/retry 481 s: può
attendere quasi otto minuti. Il fault valida gli stessi knob e la regola di
lease, non il tempo di recovery esatto del preset prestazionale.

I test dimostrano at-least-once nel perimetro osservato, non exactly-once: la
completion è scritta prima dell'ACK, il failed-job store è disabilitato e manca
un ledger degli effetti business.

## Volume e claim “1 milione/s”

La prima prova stress 50k fallì il gate di osservazione/completamento: 28.617
completion raccolte e 21.921 elementi ancora in coda, senza OOM registrato. Il
commit 2a4b107a rese
incrementale la raccolta JSONL; il [rerun n=3](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/campaign-stats.md)
passò 9/9. L'artifact iniziale non prova perdita della queue.

Il [soak storico broker-native](../../../2026-08-11-soak24-1M/results.md)
misura **999.652 push accettati/s** medi per 24 h e zero restart, ma usa loader
Go HTTP, 200 partizioni, 600 consumer e host dedicati: non attraversa Laravel.
Non prova 1M job/s, exactly-once o conservation end-to-end. Al cut-off:

- offered−accepted = 41.800; solo 400 sono spiegati da quattro push request
  fallite;
- pushed−popped = 442.600 e popped−acked = 15.000, compatibili con in-flight
  ma senza drain/ledger;
- 600 ACK-message falliti; shed=0, restart watchdog=0, contatore incidenti
  dichiarato=0.

## Metodo, corpus e provenienza

Il test è stato progettato dal team Queen. Protocollo locale timestamped prima
dello screening, pairing, conservazione dei run e gate fail-closed sono misure
anti-bias, non certificazione indipendente. La griglia iniziale era one-shot e
dirty; anche `screen-clean/` risulta dirty. È esclusa, come l'anchor drift
133,28→112,22 job/s. Sul commit clean furono replicati p4/a1 e p16/a1: p4 è la
baseline prestazionale più piccola fra candidati replicati, non optimum
globale. Partizioni/fusion non sono state confrontate.

Il corpus selezionato contiene **99 run/1.248.000 job**, tutti validi e
quiescenti, con n=3 o n=5 per cella. Le campagne storiche sostituite, smoke e
rerun sono preservati ma esclusi; l'inventario lordo è enumerato soltanto nel
report tecnico. Fault, pilot negativo e failure del gate sono separati.

La primaria performance e il fault p4/a1 usano commit clean 3655cd2a; l'auto
post-guard ha metadata clean 774c2413. Tutti usano app
`sha256:0e82c6cc…8958`, broker `sha256:522bdc09…801fd`, macOS 26.5 arm64,
Docker Desktop 29.7.2, 10 CPU/8,21 GB. Il commit 774c2413 chiarisce
l'override negativo ma non è incorporato nell'image applicativa. Provenienza completa,
digest Redis/PostgreSQL e campagne storiche sono nel report tecnico;
`campaign-stats` non gatea automaticamente i digest backend.

## Limiti e prossimi gate

- Docker Desktop/macOS, n ridotto e topologie non equivalenti: Redis volatile
  contro Queen+PostgreSQL `synchronous_commit on`, ma data dir locale su tmpfs.
- PSS riguarda solo il control plane; RAM stack include consumer+backend ma non
  producer/observer. Le quote nominali producer+app+backend saturano 10 CPU.
- Il runner performance non fa inventario/fail-fast di container estranei per
  ogni lane. Warm-up non aderisce integralmente al freeze.
- Nessun crash backend, network fault, failover, disk-full, Linux nativo o
  soak Laravel prolungato; niente ledger effetti esterni.
- Una sola coda: multi-queue, ordering, priority e starvation non verificati.

Prima della GA: ripetere su Linux arm64/amd64; fare soak Laravel e fault
backend/rete/storage; aggiungere ledger e failed store; completare scaling w4
matched, multi-queue e partizioni/fusion; introdurre lease renewal o mantenere
p1 quando il runtime non è bounded; aggiungere snapshot/fail-fast dei container
estranei a ogni lane performance.
