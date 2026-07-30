# PLAN_WEBDOC — la nuova documentazione di Queen MQ su Nimbus

**Rev 0.2 — 2026-07-30.** Information architecture + setup tecnico per `webdoc/`, la nuova
documentazione (e sito) di Queen MQ, costruita con [Nimbus](https://nimbus-docs.com)
(`@cloudflare/nimbus-docs` 0.8.2, Astro 7).

Rev 0.2 recepisce le decisioni di Alice (§9, tutte chiuse) e registra ciò che è stato
effettivamente costruito (§12). Differenze rispetto a rev 0.1:

- **niente sezione `cloud/`**: il multi-tenant è documentato come "costruisciti il tuo cloud
  con proxy+broker OSS" sotto `selfhost/multi-tenant/`;
- **streams non è marcato experimental**;
- **`priority`, `retryDelay`, `ttl`, `maxSize` non vengono menzionate da nessuna parte** (in
  rimozione dal codice), quindi cade la tabella "accettate ma non applicate" per quelle quattro;
- **versione pubblicata: 1.0.0**, con i client allineati;
- **deploy: Cloudflare Pages** (build statica, `dist/`);
- lingua del sito: **solo inglese**.

Base della ricognizione: branch `rustproxy` @ `7809814` + working tree (30-07-2026), 12 agenti
in parallelo su broker, engine, semantica, deploy, 6 client, proxy cloud, benchmark, docs
esistenti e harness di test, più due passaggi critici (completezza + fact-check avversariale).

---

## 1. Il punto di partenza (perché è una riscrittura, non un porting)

| Corpo documentale | Stato | Verdetto |
|---|---|---|
| `docs/` (23 pagine HTML → queenmq.com) | descrive il broker **C++/libqueen** ritirato, engine a righe `queen.messages` | riscrivere; archiviare i benchmark |
| `developer/01–14` | pre-porting Rust, riferiscono l'albero `lib/` cancellato | non portare |
| `developer/15–19` | attuali (log engine, ack semantics, hotlist, perf luglio) | fonti primarie per il tier "internals" |
| `README.md`, `DEVELOPING.md`, `CHANGELOG.md` | badge e mappa repo C++, changelog fermo a 0.16.0 | da riallineare (fuori scope webdoc, ma va fatto) |
| `server/README.md` | sembra autorevole, ha 5 errori verificati | non usare come fonte senza controllo |
| `app/README.md`, `test/README.md`, `queen_proxy/CONTRACTS.md`, `queen_proxy/console/README.md` | accurati e attuali | fonti primarie |

Fatti che definiscono il perimetro di verità del nuovo sito:

- `server/server.json` dice **`1.0.0-alpha-01`**, engine loggato `segments-rust`; il sito
  pubblica **1.0.0** (decisione §9.6). Il broker C++ 0.16.x è **ritirato**: nessuna sua misura, nessun suo
  confronto (Kafka/RabbitMQ/pgmq) può essere presentato come attuale.
- Il broker Rust registra **54 route** (`server/src/main.rs`) su `/api/v1/*`, `/internal/api/*`,
  `/streams/v1/*` più `/health`, `/status`, `/metrics`, `/metrics/prometheus`.
- Il tool di migration (`/api/v1/migration/*`) è stato **rimosso** in `7809814`; la RCE che
  shell-outava i parametri di connessione è stata chiusa in `b8efe84`. Backup = `pg_dump` a mano.
- `helm/`, `helm_queen/`, `upgrade.sh`, `upgrade_queen.sh` sono **gitignored e non tracciati**:
  script interni GKE Smartpricing, non artefatti di prodotto.
- La dashboard `app/` al momento della ricognizione bloccava il boot su `GET /auth/me` (route
  che il broker non registra); la dipendenza viene risolta a parte, quindi la documentazione la
  descrive normalmente (decisione §9.1).
- Gli SDK esistenti sono JavaScript, Python, Go, PHP/Laravel, C++ e la CLI. Un client **Rust**
  uscirà prima della pubblicazione del sito: `use/clients/rust.mdx` è pronto come bozza e nessuna
  pagina afferma che non esista.
- Non esiste **nessuno spec OpenAPI/Swagger**: la vecchia documentazione è arrivata a
  documentare due route inesistenti proprio per questo.
- Licenza: **Apache 2.0**.

---

## 2. I cinque pilastri → come li realizza lo schema

| Pilastro | Realizzazione |
|---|---|
| 1. Livelli a complessità crescente | 3 tier espliciti (`use` / `selfhost` / `internals`) resi come sezioni top-level Nimbus con `sidebar.scope: "section"` + tab in header, così ogni tier ha la sua rail |
| 2. Semplice ma completa | ogni pagina = una domanda del lettore (recipe Nimbus); la completezza vive in `reference/`, **generata dal codice**, non a mano |
| 3. Benchmark Queen Rust | sezione top-level `benchmarks/` con regola dura: nessun numero senza artefatto archiviato che ne riporti la config; l'era C++ va in `benchmarks/archive` |
| 4. API dei client + esempi verificati | `reference/sdk/<lang>` per l'API, `use/examples/*` per gli esempi, tutti gli snippet **estratti da file che la suite esegue** (pipeline §7) |
| 5. Cosa è, perché, forze **e debolezze** | `start/what-is-queen`, `start/why`, e — pagina di pari dignità — `start/limits` (limiti, non-obiettivi, cosa non è implementato) |

---

## 3. Albero delle pagine

Radice contenuti: `webdoc/src/content/docs/`. 7 sezioni top-level = 7 tab.
Legenda stato: `stable` · `beta` · `preview` · `internal`.

### 3.1 `start/` — Inizia qui

```
start/index.mdx                What is Queen MQ                    [overview]
start/why.mdx                  Perché Queen esiste                 [concept]
start/limits.mdx               Limiti, debolezze e non-obiettivi    [concept]
start/quickstart.mdx           Quickstart: docker → primo messaggio [quickstart]
start/compare.mdx              Queen vs Kafka / RabbitMQ / SQS / pgmq [concept]
```

- `why.mdx` porta gli 8 punti di forza dichiarati: partizioni FIFO dinamiche senza HOL, client e
  broker stateless (nessun rebalancing), Postgres come storage, performance, nessun bloat,
  client HTTP senza protocolli custom, multitenancy + webapp, semantica ibrida Kafka/RabbitMQ.
- `compare.mdx` resta **architetturale**: numeri comparativi solo se misurati sull'engine attuale
  (oggi: nessuno). Le SVG `assets/queen-vs-kafka.svg` / `queen-vs-rabbitmq.svg` sono arte
  posizionale dell'era C++ → riverificare o non usare.
- `limits.mdx` è la pagina che il vecchio sito non aveva. Contenuto verificato:
  no ordering globale (l'ordine è per partizione), no priority queueing, parallelismo dentro un
  gruppo limitato alla granularità di partizione, `log_dlq` mai purgato, `maxWaitTimeSeconds`
  cancella segmenti (perdita di dati per design), `/configure` è un replace totale, un solo
  Postgres come dominio di guasto, nessun listener TLS sul broker, niente purge dello stato degli
  stream.

### 3.2 `use/` — Usare Queen (tier 1)

```
use/index.mdx                  overview + percorsi
use/concepts/index.mdx         Il modello in una pagina
use/concepts/queues-partitions.mdx     Code, partizioni, namespace, task
use/concepts/producing.mdx             Push, transactionId, deduplicazione
use/concepts/consuming.mdx             Consumer group, subscription mode, pop, batch, long-poll
use/concepts/ack.mdx                   Ack è un commit di offset            ← pagina cardine
use/concepts/leases-retries-dlq.mdx    Lease, retry, dead-letter queue
use/concepts/transactions.mdx          Transazioni: ack + push atomici
use/concepts/replay.mdx                Replay e riposizionamento di un gruppo
use/concepts/retention.mdx             Retention e ciclo di vita dei dati
use/concepts/guarantees.mdx            Garanzie: ordine, durabilità, consegna
use/clients/index.mdx          Scegli l'SDK (matrice di capacità)
use/clients/javascript.mdx     ·  python.mdx  ·  go.mdx  ·  php-laravel.mdx  ·  cpp.mdx
use/clients/http.mdx           Usare Queen da qualunque linguaggio (HTTP puro)
use/examples/index.mdx         Cosa significa "esempio verificato"
use/examples/producer-consumer.mdx     Primo produttore e consumatore
use/examples/fanout.mdx                Fan-out con consumer group
use/examples/pipeline.mdx              Pipeline multi-stadio transazionale
use/examples/dedup.mdx                 Deduplicazione idempotente
use/examples/replay.mdx                Rileggere la storia di una coda
use/examples/dlq.mdx                   Gestire i fallimenti e la DLQ
use/errors.mdx                 Errori, 429/403, backpressure e retry
use/streams.mdx                Streams, finestre e aggregazioni
```

Note vincolanti dalla ricognizione:

- `ack.mdx` è **una sola** pagina concettuale (la ricognizione proponeva quattro pagine ack
  sovrapposte da quattro aree diverse: accorpate). Il contratto di wire va in `reference/http/ack`.
- `consuming.mdx` deve dire che un gruppo ha **al massimo un batch in lease per (partizione,
  gruppo)**: una coda a partizione singola non si consuma in parallelo dentro lo stesso gruppo.
- `guarantees.mdx`: at-least-once con lease; `autoAck=true` lato server è at-most-once; dedup
  esatta per partizione su `transactionId` in finestra (default 3600s, attiva); una transazione è
  un solo commit PG ma **non** rende exactly-once una pipeline se il transactionId non è
  deterministico.
- streams e `.gate()` restano `experimental` con i test rossi dichiarati in pagina (window
  arithmetic over-emette in JS e Python; `TestGateTokenBucketBasic` fallisce in modo
  deterministico in Go).

### 3.3 `reference/` — Reference (trasversale ai tier, lookup)

```
reference/index.mdx
reference/http/index.mdx       Convenzioni: 204 senza body, forma degli errori, access level
reference/http/routes.mdx      Tabella completa delle route          ← GENERATA
reference/http/push.mdx  ·  pop.mdx  ·  ack.mdx  ·  transaction.mdx  ·  lease.mdx
reference/http/queues.mdx  ·  consumer-groups.mdx  ·  messages-dlq.mdx  ·  traces.mdx
reference/http/status-metrics.mdx  ·  analytics.mdx  ·  system.mdx
reference/sdk/javascript.mdx  ·  python.mdx  ·  go.mdx  ·  php.mdx  ·  cpp.mdx
reference/queenctl.mdx         Comandi e flag di queenctl
reference/config.mdx           Ogni variabile d'ambiente del broker  ← GENERATA
reference/queue-options.mdx    Opzioni di /configure + "accettate ma non applicate"
reference/prometheus.mdx       Famiglie di metriche                  ← GENERATA
reference/errors.mdx           Codici di errore e status HTTP
reference/defaults.mdx         Default effettivi (i casi a due valori, risolti)
reference/compatibility.mdx    Versioni broker ↔ client
```

Vincoli:

- `reference/http/index.mdx` deve dichiarare in testa che **ogni 204 è senza body** (scelta
  deliberata: annunciare content-length su un body eliso avvelenava le connessioni undici) e che
  molte route di management restituiscono verbatim il JSON di una stored procedure.
- `reference/queue-options.mdx` elenca **solo le opzioni che `/configure` applica davvero**, e
  dice che `/configure` è un replace totale. `priority`, `retryDelay`, `ttl` e `maxSize` non
  vengono menzionate da nessuna parte: sono in rimozione dal codice.
- `reference/defaults.mdx` chiude i default a due valori: `leaseTime` 60s su coda creata dal push
  vs 300s dopo `/configure`; `QUEEN_HOTLIST` attivo di default (il doc-comment del modulo dice il
  contrario); `RETENTION_INTERVAL` 5000ms (non 300000); `POP_DEFAULT_TIMEOUT_MS` 30000 (non 2000).
- Flag da **non** documentare come knob stabili (il codice li marca come esperimenti):
  `QUEEN_V2_FUSION_MIN_FRAMES`, `QUEEN_V2_FUSION_MIN_WAIT_MS`, `QUEEN_V2_BUNDLE_MAX`,
  `QUEEN_V2_FUSION_MAX_INFLIGHT`. `QUEEN_V2_FUSION_FRAMES` è letto e loggato ma **inerte**.

### 3.4 `selfhost/` — Ospitare Queen (tier 2)

```
selfhost/index.mdx             overview
selfhost/deploy.mdx            Da zero al primo messaggio (Docker)   [how-to]
selfhost/postgres.mdx          Requisiti, privilegi, tuning, sizing
selfhost/configuration.mdx     Come funziona la configurazione
selfhost/ha.mdx                Alta disponibilità: 2–3 broker su un Postgres
selfhost/security/index.mdx    Confini di fiducia
selfhost/security/jwt.mdx      JWT: algoritmi, JWKS, IdP esterno, access level
selfhost/security/postgres-tls.mdx     TLS verso Postgres
selfhost/security/encryption.mdx       Cifratura at-rest dei payload
selfhost/observability.mdx     Log, blocchi rates/sizes, /metrics, Prometheus
selfhost/probes-restarts.mdx   Health, probe, SIGTERM, restart rolling e lease
selfhost/durability.mdx        Sopravvivere a un'outage di Postgres (spool, maintenance)
selfhost/retention.mdx         Retention, eviction e crescita del disco
selfhost/backup.mdx            Backup e restore (pg_dump)
selfhost/kubernetes.mdx        Kubernetes: stato e cosa serve
selfhost/dashboard.mdx         La dashboard integrata
selfhost/upgrade.mdx           Aggiornamenti, schema bootstrap, da 0.16 (C++) a 1.0 (Rust)
```

Pagine che la ricognizione iniziale non aveva previsto e che il critico ha imposto:

- `security/jwt.mdx`: il broker supporta HS256/384/512, RS256/384/512, EdDSA, `JWT_ALGORITHM=auto`,
  chiave da PEM statico **o da endpoint JWKS** con cache per `kid` e refresh su kid sconosciuto,
  più remapping dei claim di ruolo. ~15 env var mai documentate. `WriteOnly` non è gerarchico.
- `security/postgres-tls.mdx`: `PG_USE_SSL`, `PG_SSL_REJECT_UNAUTHORIZED`, con la modalità
  encrypt-only che serve al Postgres gestito, e il caveat sul certificato non verificato.
- `postgres.mdx`: serve `CREATE SCHEMA`, **nessuna extension**, `QUEEN_APPLY_SCHEMA=0` per girare
  con un ruolo a basso privilegio su schema pre-applicato, connessione diretta necessaria
  (advisory lock di sessione), e le tabelle `queen_streams.*` sono `GRANT TO PUBLIC`.
- `probes-restarts.mdx`: `/health` fa un round-trip reale al DB e risponde 503 se Postgres non
  c'è — quindi usarlo come **liveness** probe annulla lo spool su disco. Da dire una volta e forte.
- `ha.mdx`: la mesh è **TCP framed** (non UDP), l'handshake HELLO è HMAC ma il nonce non è
  tracciato (replay possibile) e i frame post-handshake sono JSON non autenticato, incluso
  `MAINTENANCE_MODE_SET`. **Firewallare la porta mesh è un requisito, non un consiglio.**
- `observability.mdx`: `/metrics/prometheus` **non è una superficie per-tenant** (le serie
  per-queue sommano attraverso i tenant, per scelta esplicita nel codice).
- `kubernetes.mdx`: oggi non si spedisce nessun chart. Vedi decisione §9.3.
- `dashboard.mdx`: oggi la SPA richiede `/auth/me` del proxy. Vedi decisione §9.1.

### 3.5 `selfhost/multi-tenant/` — costruirsi il proprio servizio multi-tenant

Non una sezione "cloud" commerciale: proxy e broker sono Apache 2.0, quindi questa parte spiega
come mettere in piedi lo stesso sistema in casa.

```
selfhost/multi-tenant/index.mdx           Architettura: tenant → cluster → cell
selfhost/multi-tenant/proxy.mdx           Deployare queen_proxy (pxdb, TLS, cache, config)
selfhost/multi-tenant/tenants.mdx         Modello dati e provisioning
selfhost/multi-tenant/auth.mdx            API key, sessioni utente, OAuth, matrice di autorizzazione
selfhost/multi-tenant/quotas.mdx          Quote, rate limit, shadow mode, contratto 429
selfhost/multi-tenant/endpoints.mdx       Cosa può chiamare un tenant            ← GENERATA
selfhost/multi-tenant/metering.mdx        Pipeline di metering
selfhost/multi-tenant/isolation.mdx       Garanzie di isolamento e bordi noti
selfhost/multi-tenant/broker-tenancy.mdx  QUEEN_TENANCY_HEADER e x-queen-tenant
```

- `endpoints.mdx` è generata da `queen_proxy/src/routes.rs` (7 classi di route): riconcilia le
  route del broker con ciò che un tenant può chiamare, e trasforma i due "isolation gap"
  (`/api/v1/status` non scopato, `/streams/v1/*` che fissa il tenant di default) da falle
  spaventose nella ragione per cui il proxy esiste.
- `quotas.mdx`: `QUEEN_PROXY_ENFORCE` è `false` di default (shadow mode) **ma** i cap di
  dimensione (413) e i due push-block (quota storage, quota mensile messaggi) sono duri e scattano
  comunque. Le quote mensili dei piani seeded sono `NULL`: non pubblicarle come attive.
- `broker-tenancy.mdx`: `x-queen-tenant` è header non autenticato per design — il confine di
  fiducia è il proxy e il cell secret. Quindi la pagina è operativa, non client-facing.
- `tenants.mdx` dice che il piazzamento delle cell è manuale e che il draining non esiste
  (`cells.status`/`capacity_slots` non sono letti da nessun codice).

### 3.6 `internals/` — Come funziona (tier 3)

```
internals/index.mdx           Architettura: un binario Rust e un Postgres
internals/storage-model.mdx   Segmenti, offset, le tabelle log_*
internals/life-of-a-push.mdx  Raggruppamento, fusion, un commit per N segmenti
internals/life-of-a-pop.mdx   Candidati, claim, lease, visibilità
internals/hotlist.mdx         La hot-list wildcard e la sua ruota
internals/dedup.mdx           Probe-before-allocate, lock shardati, suppression cache
internals/ack-internals.mdx   Registry, ack fusion, sidecar degli hash
internals/flow-control.mdx    Il limitatore adattivo (Vegas)
internals/retention.mdx       Come gira la retention
internals/stats.mdx           Contatori e lag
internals/mesh.mdx            Wakeup e coordinamento fra broker
internals/tenancy.mdx         Scoping nativo per tenant                [internal]
internals/schema.mdx          Tabelle e stored procedure
internals/legacy.mdx          Il rows engine ancora applicato al boot ma morto
internals/contributing/index.mdx        Build da sorgente e loop di sviluppo
internals/contributing/testing.mdx      L'harness test/run.sh
internals/contributing/docs.mdx         La pipeline degli snippet verificati
internals/contributing/release.mdx      Versioni e distribuzione
```

Vincoli di verità:

- niente "hierarchical timing wheel": il codice dice esplicitamente che è uno stand-in
  correctness-first con un min-heap per sotto-ring;
- `internals/legacy.mdx` spiega perché le procedure 001–018 del rows engine sono ancora applicate
  al boot pur non avendo **zero** call site, e perché `queen.stats` continua a dire
  `engine: segments` per compatibilità di telemetria;
- `db::pop_wildcard` esiste ma non è mai chiamata: non presentarla come implementazione del pop.

### 3.7 `benchmarks/` — Benchmark

```
benchmarks/index.mdx           Cosa pubblichiamo e come misuriamo
benchmarks/soak-24h.mdx        24 ore, 51,8 miliardi di messaggi        ← headline
benchmarks/peak.mdx            Picco: 1M msg/s per lato
benchmarks/ordered-pipeline.mdx  Pipeline ordinata: 25k ev/s, ordine totale certificato
benchmarks/multitenant-cell.mdx  Una cell da 2 core sotto enforcement
benchmarks/reproduce.mdx       Come rieseguirli
benchmarks/archive.mdx         Era C++ (0.12–0.16): non comparabile     [internal]
```

Regola dura, da §8: **nessun numero senza un artefatto archiviato in `benchmark-queen/` che ne
riporti la configurazione.** Conseguenze già note:

- headline = soak 24h (`2026-07-25-soak24`, commit `615efdc`): 51 820 403 100 messaggi,
  ~600k msg/s per lato **con ack espliciti** (`manualAck=true ackAsync=true`), error rate
  0,00012%, 0 restart, RSS broker piatto a 6,3 GB, VM 32 core / 62 GB;
- il picco 1M msg/s per lato (`2026-07-23-3test-report` T1) va pubblicato **solo** con le sue
  condizioni attaccate: `autoAck`, dedup **off**, 99,914% consegnato (~500k messaggi mai accettati);
- la pipeline ordinata T3 è il risultato più forte per la tesi FIFO: 4 stadi, 1000 partizioni,
  25k ev/s per 600s, 88 503 408 messaggi verificati, 0 duplicati, 0 gap, 0 violazioni d'ordine,
  con `dedupWindow=300s` effettivamente echeggiato dal loader;
- il titolo openloop "2M msg/s combinati" **non si pubblica**: non ha stdout archiviato e la sua
  config dice dedup off;
- la storia cloud (`>2400 msg/s` su cell 2 core, 12 tenant per un'ora, 0 consegne cross-tenant) va
  pubblicata dichiarando `dedupWindowSeconds: 0` e `retry429Attempts: 1`, e menzionando gli 818
  HTTP 502 + 4392 connection-refused presenti nel raw e assenti dal README;
- il `~480 msg/s` free-tier è **superato** dai 2400 della campagna 29-07 — e nel frattempo è
  citato in `queen_proxy/migrations/002_functions.sql:391` per giustificare il rate del piano pro:
  va corretto alla fonte, non solo nei doc;
- le cifre storage-v2 che circolano (6.2x storage, 5.2x consume, 16x retention) vengono da una
  directory **non presente nel working tree** e da un driver JS su laptop: non pubblicabili.

---

## 4. Come i tier diventano navigazione Nimbus

- `sidebar.scope: "section"` + tab in header: ogni tier ha la propria rail, la navigazione
  fra tier avviene dalle tab. È il pattern che Nimbus consiglia per siti grandi.
- `sidebar.items` dichiarato esplicitamente in `astro.config.ts` con `autogenerate: { directory }`
  per fissare l'ordine delle 7 sezioni ed etichette/icone; l'ordine interno con `sidebar.order`.
- `indexDisplay: "overview-leaf"` + `overviewLabel: "Panoramica"` così ogni sezione ha una landing
  leggibile invece di un header cliccabile.
- `defaultCollapsed: true` sui gruppi profondi (`reference/http`, `use/concepts`).

Badge di stato in sidebar (`sidebar.badge`) e in pagina: `beta` per tutto `cloud/`,
`experimental` per `use/experimental/*`, `internal` per `internals/tenancy` e
`benchmarks/archive`.

---

## 5. Setup tecnico

```bash
npx @cloudflare/create-nimbus-docs@0.6.3 webdoc --deploy other --content empty --package-manager pnpm --no-git --yes
```

`--deploy other` perché il target è Cloudflare **Pages** (build statica in `dist/`), non Workers:
non serve `wrangler.jsonc`. `--content empty` perché il contenuto è tutto nostro.

Nimbus è **pre-1.0 e dichiara che la superficie cambia fra minor**: `@cloudflare/nimbus-docs` è
pinnato **esatto a 0.8.2** in `webdoc/package.json`, e l'upgrade si fa leggendo il changelog.

`webdoc/astro.config.ts`:

```ts
defineNimbusConfig({
  site: "https://queenmq.com",
  title: "Queen MQ",
  description: "Ordered FIFO partitions and consumer groups on the Postgres you already run.",
  github: "https://github.com/queen-mq/queen",
  editPattern: "https://github.com/queen-mq/queen/edit/master/webdoc/{path}",
  sidebar: { scope: "section", defaultCollapsed: true, overviewLabel: "Panoramica",
             indexDisplay: "overview-leaf", items: [ /* le 7 sezioni */ ] },
})
```

Frontmatter custom da dichiarare in `webdoc/src/content.config.ts` (lo schema base è strict:
una chiave non dichiarata fa fallire la build — che è esattamente ciò che vogliamo):

| campo | valori | a cosa serve |
|---|---|---|
| `type` | `overview` `quickstart` `tutorial` `how-to` `concept` `reference` `example` `troubleshooting` `changelog` | il contratto di forma della recipe Nimbus |
| `tier` | `use` `operate` `cloud` `internals` | il livello, per filtri e coerenza |
| `status` | `stable` `beta` `experimental` `internal` `deprecated` | badge e banner automatici |
| `sourceOfTruth` | `string[]` — path di codice | ogni pagina dichiara il codice che la governa |
| `verifiedBy` | `string[]` — path di test/esempi | ciò che rende "verificato" un esempio |
| `lastVerified` | data | l'unica data che pubblichiamo |
| `generated` | boolean | la pagina è prodotta da uno script: non editarla a mano |

Lint da attivare (`nimbus-docs lint`): `frontmatter-shape` e `internal-link` come `error`
(i due portanti), più `description-required`, `code-block-lang`, `heading-hierarchy`,
`single-h1`, `no-self-host-url`.

Componenti: `Tabs`/`CodeGroup` per i 5 linguaggi, `Steps` per quickstart e how-to, `Aside` per i
callout di onestà, `Badge` per lo stato, `LinkCard`/`CardGrid` per gli hub, `PackageManagers` per
le install, `FileTree` per gli alberi. Da registry: `mermaid` per i diagrammi di `internals/`
(i plugin remark **non** funzionano: il markdown gira su Sätteri).

Diagrammi già esistenti e riutilizzabili: `assets/queen-partitions.svg`, `queen-lease-dlq.svg`,
`queen-replay.svg`, `queen-transaction.svg`, `queen-failover.svg`. Da riverificare contro il log
engine prima di ripubblicarli: `queen-vs-kafka.svg`, `queen-vs-rabbitmq.svg`.

---

## 6. Anti-drift: quattro pagine generate dal codice

La vecchia documentazione ha derivato fino a documentare route inesistenti perché era scritta a
mano. Quattro script in `webdoc/scripts/` producono partial in
`webdoc/src/content/partials/generated/`, e la CI fallisce se rigenerando cambia qualcosa:

| script | fonte | output |
|---|---|---|
| `gen-routes.mjs` | `server/src/main.rs` + `auth::route_access_level` | route, metodo, access level, tenant-scoped sì/no |
| `gen-config.mjs` | `server/src/config.rs` | ogni env var con default e gruppo |
| `gen-metrics.mjs` | `server/src/metrics.rs` | famiglie Prometheus e label |
| `gen-proxy-routes.mjs` | `queen_proxy/src/routes.rs` | le 7 classi di route del cloud |

---

## 7. Pipeline degli "esempi verificati" (pilastro 4)

Regola: **uno snippet pubblicato è una regione estratta da un file che la suite esegue.**

1. Nei file di test/esempio si marcano le regioni:
   `// docs:start(quickstart-push)` … `// docs:end`.
2. `webdoc/scripts/gen-snippets.mjs` le estrae in
   `webdoc/src/content/partials/snippets/<nome>.<lang>.mdx`, con in testa il path del file
   sorgente e la suite che lo esegue.
3. Le pagine li includono con `<Render file="snippets/..." />` — niente copia-incolla.
4. La CI rigenera e fallisce sul diff; `use/examples/index.mdx` spiega il patto al lettore.

Stato reale della suite (misurato oggi, branch `rustproxy`, broker ricostruito):

- verde: broker Rust 169–172 unit test, `queen_proxy` 170, C++ 37 + 14 asserzioni proxy su 3
  lane, mesh, tenancy 45/0, JS unit 91/91, Go `./streams/...` 13/13;
- rosso noto: finestre di streaming in JS (3 test) e Python (2, rotanti),
  `TestGateTokenBucketBasic` in Go (deterministico), `TestLoad_BenchSmoke` nella CLI;
- **da riparare prima di scrivere "verificato" da qualche parte**: `test/run.sh` può riportare un
  falso verde (`--abort-on-container-exit --exit-code-from runner` + l'handler SIGTERM del client
  JS che chiude con `process.exit(0)`), e la regex di parità tenancy cattura la durata di pytest.

Quindi: la parola "verificato" entra nel sito **dopo** il fix del gate della CI, e le pagine
streams/gate nascono `experimental` con i fallimenti dichiarati.

---

## 8. Regole editoriali (le "regole di verità")

1. Nessun numero senza artefatto archiviato che riporti la config che lo ha prodotto.
2. Ogni pagina dichiara il codice che la governa (`sourceOfTruth`); se il codice cambia, la pagina
   è in debito.
3. Tre stati pubblici — `stable`, `beta`, `experimental` — e nulla di flaggato-per-esperimento
   viene presentato come knob di prodotto.
4. Le cose accettate e non applicate si elencano, non si tacciono (tabella dedicata).
5. Nulla dell'era C++ è presentato come attuale; l'archivio esiste ma è etichettato.
6. "Esempio verificato" ha una definizione operativa, non retorica (§7).
7. I comportamenti che perdono dati si dicono nel titolo della sezione, non in nota.

---

## 9. Decisioni — chiuse

1. **Dashboard nel self-host.** Documentata normalmente: la dipendenza da `/auth/me` viene
   risolta a parte. `selfhost/dashboard.mdx` descrive cosa mostra e come si ricostruisce da
   `app/`, senza descrivere un boot gate di autenticazione.
2. **Deploy del sito.** **Cloudflare Pages**, dove il sito già sta. Build statica, output
   `dist/`, nessun `wrangler.jsonc` (lo scaffold è stato fatto con `--deploy other`). I redirect
   dalle URL indicizzate sono in `webdoc/public/_redirects`.
3. **Kubernetes.** Nessun chart Helm committato: `selfhost/kubernetes.mdx` pubblica un
   **manifest di riferimento completo** scritto per il broker Rust (comando e immagine reali,
   porta mesh TCP, secret ref per `QUEEN_SYNC_SECRET`, probe **non** accoppiate al DB) e dichiara
   che oggi non esiste un chart ufficiale. Le directory `helm/` e `helm_queen/` non vengono
   citate: sono gitignored e mai committate.
4. **Streams.** Pubblicato come funzionalità normale, senza etichetta `experimental`. Le pagine
   non rivendicano verifica per-test dell'aritmetica delle finestre e non pubblicano liste di
   test rossi.
5. **Multi-tenant, non "cloud".** Nessuna sezione `cloud/`: proxy e broker sono Apache 2.0, e
   `selfhost/multi-tenant/` spiega come costruirsi il proprio servizio multi-tenant in casa
   (architettura tenant → cluster → cell, proxy, quote, metering, isolamento). La sezione
   commerciale nascerà quando nascerà il cloud as a service.
6. **Numerazione.** `1.0.0`, broker e SDK allineati. `reference/compatibility.mdx` lo dice, e il
   contratto HTTP è la superficie di compatibilità.
7. **Lingua.** Sito **solo inglese**. Questo piano resta in italiano.
8. **Brand.** `assets/generate-brand.py` ha ora `webdoc/public/` come quinta superficie
   (favicon SVG theme-adaptive, favicon-32, apple-touch-icon, mark 64px, line-mark). Nessun mark
   nuovo inventato. Nell'header il mark è dipinto come mask su `currentColor`, così segue il tema
   e il toggle con un solo asset.

**Aperto, non bloccante:** un client Rust uscirà prima della pubblicazione del sito.
`use/clients/rust.mdx` esiste come bozza (`draft: true`) da riempire; nessuna pagina afferma che
un SDK Rust non esista.

---

## 10. Fasi

**Fase 0 — scaffold** (mezza giornata): `create-nimbus-docs` in `webdoc/`, config, frontmatter
custom, lint, sezioni vuote con landing, i 5 script generatori, CI di lint+build.

**Fase 1 — sito pubblicabile, ~30 pagine**: tutto `start/`, `use/concepts/*`, `use/clients/index`
+ javascript + python + http, `use/examples/*` (i primi tre), `reference/http/*` generata +
push/pop/ack, `reference/config` generata, `selfhost/{deploy,postgres,configuration,ha,observability,retention,upgrade}`,
`benchmarks/{index,soak-24h,ordered-pipeline}`, `internals/{index,storage-model,life-of-a-push,life-of-a-pop}`.
Più i redirect delle 21 URL indicizzate.

**Fase 2 — completezza**: il resto di `reference/` (SDK per linguaggio, queenctl, prometheus,
errors, defaults), il resto di `selfhost/` (security/*, probes, durability, backup, kubernetes,
dashboard), il resto di `internals/`, `benchmarks/{peak,multitenant-cell,reproduce,archive}`.

**Fase 3 — cloud e sperimentale**: tutto `cloud/`, `use/experimental/*`, la pipeline snippet
estesa a Go/PHP/C++, e `internals/contributing/*`.

---

## 11. Redirect dalle URL indicizzate

21 URL sono indicizzate su queenmq.com (`docs/sitemap.xml`) e vanno rimappate:

| vecchia | nuova |
|---|---|
| `/` | `/` |
| `/quickstart` | `/start/quickstart` |
| `/concepts` | `/use/concepts` |
| `/architecture` | `/internals` |
| `/clients` | `/use/clients` |
| `/http-api` | `/reference/http` |
| `/cli` | `/reference/queenctl` |
| `/server` | `/selfhost` |
| `/dashboard` | `/selfhost/dashboard` |
| `/sizing` | `/selfhost/postgres` |
| `/benchmarks` | `/benchmarks` |
| `/benchmarks-0.16-lag`, `-matrix`, `-soak`, `/benchmarks-2026-04` | `/benchmarks/archive` |
| `/use-cases` | `/use/examples` |
| `/use-case-pipeline` | `/use/examples/pipeline` |
| `/use-case-dedup` | `/use/examples/dedup` |
| `/use-case-replay` | `/use/examples/replay` |
| `/use-case-aggregations` | `/use/experimental/streams` |
| `/use-case-rate-limiter` | `/use/experimental/gate` |
| `/visual-tour` | `/start` |

Nota: `clients/client-cli/cmd/docs.go` costruisce link `queenmq.com/<topic>.html` hardcoded —
va aggiornato insieme al sito.
