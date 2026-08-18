# Stato implementazione KV + TIMERS

Fermato il 2026-08-17 su richiesta. Branch `kvtimer`. Riferimento: `PLAN_KV_TIMERS.md`.

Questo file e' un passaggio di consegne, non un documento di progetto: si cancella quando il
lavoro e' chiuso.

## Dove siamo

| fase del piano | stato |
|---|---|
| F1 schema (`024_kv.sql`, `025_log_timers.sql`, `PROCEDURES`) | **fatto e verificato** |
| F2 KV (`kv_apply_v1`, rotte, pool, auth) | **fatto e verificato** |
| F3 timer (`025` completa, `sweeper.rs`, metriche) | **fatto e verificato** |
| F4 wire (step 0/0b in `005_log_ack.sql`, demux in `data.rs`) | **fatto e verificato** |
| F5 quote, degradazione, kill switch | fatto, **non verificato dalla suite** |
| F8 P1-P2 proxy | fatto, **non verificato dalla suite** |
| F6 client (js, py, go, rust, php, cpp, http) | fatto, **non verificato dalla suite** |
| F7 webdoc: internals, generatori, reference, concetti | fatto, `check:brief` verde |
| F7 webdoc: **i tre full example** | **NON FATTO** (fermato deliberatamente) |
| **La test run completa (`test/run.sh`)** | **MAI ESEGUITA** |

`server/sql/procedures/026_kv_sweeper.sql` e' un file che il piano non prevedeva: e' nato in
verifica, perche' `sweeper.rs` chiamava `kv_expire_step_v1` e `kv_usage_step_v1` che non
esistevano. Il broker degradava con due WARN, quindi il guasto era invisibile a ogni test, ma con
`vacuum_truncate = off` la heap non si ritira mai: `queen.kv` cresceva senza fine e la quota era
inerte. Implementa §7.5.

## Stato dell'albero

- ultimo commit: `4ce6091c` (merge in `kvtimer`)
- **188 file non committati**, fra modificati e nuovi
- `cargo build` **verde**
- `webdoc check:brief` **verde**: 29,6 KB su 30 KB (era 30001 byte, il taglio di `limits.mdx` ha
  liberato margine)
- otto pagine webdoc nuove presenti: `internals/{kv,timers,sweeper}.mdx`, `use/{kv,timers}.mdx`,
  `deploy/state.mdx`, `reference/http/{kv,timers}.mdx`

## Cosa fare per prima cosa domani

1. **La test run completa**, che non e' mai partita. E' il punto cieco piu' grande: sette client e
   F5 e F8 sono stati scritti ma nessuno li ha visti girare insieme.
   `test/run.sh` matrice intera, piu' il parity gate a flag OFF, che e' il criterio che dice se il
   percorso caldo ha retto.
2. I tre full example (§11.4 piu' quello nuovo su kv+timers richiesto: il candidato scelto e' il
   **rate limiter inverso**, perche' e' l'unico scenario in cui le due feature sono una cosa sola
   invece di due accostate).
3. Committare, perche' 188 file scoperti sono troppi per restare cosi'.

Il workflow fermato si riprende dalla cache degli agenti gia' completati:

```bash
# 14 dei ~20 agenti hanno gia' un risultato in cache e non rigirano
```

`Workflow({scriptPath: ".../kvtimer-impl-clients-docs-wf_b979c8c4-158.js", resumeFromRunId: "wf_b979c8c4-158"})`

## Due decisioni che aspettano Alice

**1. `delayMs` o `delaySeconds` sul wire dei timer (§20.6).** Aperta dalla ratifica di
`ttlSeconds`. L'implementazione e' andata avanti con **`delayMs`**, che e' la proposta del piano,
con la regola dichiarata *le durate che possono essere sotto il secondo sono in millisecondi,
quelle che non possono sono in secondi*. E' una porta a senso unico: dopo che sette client lo
mandano non e' piu' rinominabile. Se si cambia idea, si cambia **prima** del rilascio dei client.

**2. Il ramo `duplicate` del fuoco e' irraggiungibile per costruzione.** §6.2 ratifica
`p_verified = v_last`, quindi `log_push_one_v1` salta la sonda di dedup
(`003_log_push.sql:129-135`) e `025` chiude l'altra via alzando `QFIRE`. Misurato sul rig: un
timer il cui `txn` e' gia' nel log **viene appeso una seconda volta** (`messages_in_segments`
1 → 3).

La conseguenza vera, che va scritta in documentazione perche' contraddice quello che si e' detto
a lungo in progettazione: **il `txn` fisso NON e' la rete del fuoco. La rete e' delete piu' push
nella stessa transazione.**

Le due uscite, e vanno pesate: o `duplicate` esce da §4.1, §12 e §14.2 come ramo irraggiungibile
in v1, oppure il fuoco paga la sonda che §6.2 ha rifiutato per non finire dentro il serializzatore
di push condiviso con i produttori normali. La prima costa una riga di documentazione, la seconda
costa prestazioni sul percorso di push.

## Note operative

- Restano due container Postgres accesi che sembrano rig di test degli agenti, `rrl-a4` (4 ore) e
  `queen-apps-pg` (21 minuti): nessuno dei due pubblica porte sull'host e nessuno dei due ha un
  progetto compose. Non li ho toccati per non spegnere qualcosa che non e' mio. Da verificare e
  rimuovere.
- `test/compose/docker-compose.single.yml` ora accende kv e timers nello stack di test
  (`QUEEN_TEST_KV`), con `QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS=1000` come manopola di harness: senza,
  il primo timer su un broker altrimenti fermo aspetta il backoff di inattivita', misurato 17 s
  per uno chiesto a 500 ms. Il tetto di produzione resta dov'e'.
- Gotcha che continua a valere: l'SQL e' `include_str!`-embedded, quindi dopo ogni edit a un file
  `.sql` serve `cargo build` prima che il broker lo veda. Un test contro un binario vecchio mente.
