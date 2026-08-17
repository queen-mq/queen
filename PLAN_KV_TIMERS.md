# PLAN_KV_TIMERS — messaggi schedulati e stato transazionale sul motore log

Rev 1.0 del 2026-08-17. Branch `kvtimer`, base `f1aad78dc684452505bb644f5e0f9eecde1c9861`. Ogni riferimento `file:riga` qui sotto risolve contro quello sha (`git show f1aad78:percorso`). I file SQL sono nominati con la numerazione VERA di oggi (`server/sql/procedures/001..023`), non con quella pre-rinumerazione.

Status: non iniziato. Fase 0 (contratti) da chiudere prima di scrivere una riga di SQL.

Fondato su cinque documenti di design (API KV, sweeper e timer, wire e ordine di lock, quote e osservabilita', superfici dei client) e su tre revisioni avversariali. La §19 mappa ogni difetto BLOCCANTE e SERIO sollevato dai revisori sulla sezione che lo chiude, oppure lo dichiara rischio accettato. Nessuno e' ignorato in silenzio.

---

## 0. Goal e non-goal

**Goal TIMER.** Un messaggio schedulato entra nel log al momento della consegna e non prima. Fino ad allora vive in una tabella di staging `queen.log_timers` chiavata per nome, dove puo' essere cancellato o riprogrammato. Al fuoco, la riga sparisce e il messaggio compare nella coda di destinazione dentro **una sola** transazione Postgres, quindi non esiste mai uno stato "consegnato a meta'" e la domanda "e' partito?" ha una sola autorita', il log.

**Goal KV.** Uno store chiave/valore transazionale `queen.kv`, chiavato `(tenant, namespace, key)`, con scadenza obbligatoria, utilizzabile **dentro** la stessa transazione di push e ack. Il valore del prodotto non e' il KV: e' che il marcatore di idempotenza, l'effetto e l'avanzamento del cursore committano insieme. Un KV a lato non puo' darlo, per nessun prezzo.

**Perche' un solo piano.** Le due feature condividono quattro cose, e sono quelle che costano: **un solo componente di fondo** (lo sweeper fa fuoco ai timer e pota le chiavi scadute), **un solo ordine di lock** esteso da due spazi a sei, **una sola superficie di quota e di tenancy** (una riga per tenant per tutto lo stato), **una sola migrazione della webdoc** e degli esempi. Spedirle separate significherebbe scrivere due volte il loop di fondo, due volte la dimostrazione di assenza di deadlock e due volte la storia delle quote. Sono pero' **indipendenti sul percorso di rilascio**: le fasi 2 e 3 (§16) sono scollegate, e ciascuna delle due feature puo' restare spenta mentre l'altra e' accesa.

**Non-goal v1.** Ognuno chiude una porta che il lettore aprirebbe da solo.

- `[T]` Nessuna ricorrenza, nessun cron (la parte difficile e' il recupero delle finestre perse, ed e' un prodotto diverso; qui non esiste nemmeno il concetto di finestra persa).
- `[T]` Nessun timer nel log (§1.1 dimostra perche' e' impossibile, non solo scomodo).
- `[T]` Il fuoco non passa dalla fusion (§1.8).
- `[T]` Nessun `list` globale sui timer di un tenant: la coda e' obbligatoria (`§6.2`).
- `[KV]` Nessuna query per valore, nessun indice sul valore, nessun predicato.
- `[KV]` Nessun elenco senza prefisso: un namespace non e' una tabella da enumerare.
- `[KV]` Nessuna CAS arbitraria oltre `expect` sulla versione, nessun `merge`, nessun `deletePrefix`, nessun `expire` (tutti additivi, motivazioni in §5.6).
- `[KV]` Tetto sul valore a 64 KiB, altrimenti diventa un blob store che compete col log nello stesso Postgres.
- `[KV]` **Nessun rider KV sul pop in v1.** Tagliato, con motivazione in §18.1. Il costo e' un round trip in piu' per lo stato compattato per partizione, dichiarato.
- `[KV]` **`getPrefix` non entra nel transaction wire**, mai, in nessuna fase (§5.5).
- `[both]` Nessuna partizione creata per richiesta, per chiave KV o per timer. La manutenzione di Queen scala con le PARTIZIONI, ed e' l'unica dimensione che questo piano non tocca.
- `[both]` Nessun `LISTEN/NOTIFY` di Postgres (§7.4), nessun frame mesh nuovo in v1 (§18.3).
- `[both]` Nessun watch, subscribe o long poll sulle chiavi: sarebbe la stessa richiesta trasformata in una connessione parcheggiata, e il parcheggio e' gia' una risorsa contesa.

**Deployment stance.** `QUEEN_KV_ENABLED=false` e `QUEEN_TIMERS_ENABLED=false`, **indipendenti**, entrambi spenti di default. Con il flag a `false` le rotte **non si registrano affatto** in `main.rs` (404 dal fallback JSON), il ramo KV del wire viene rifiutato con SQLSTATE di classe 42 (configurazione, permanente, non ritentato) e lo sweeper **non viene spawnato**. Le tabelle si creano comunque a ogni boot: da vuote costano zero, e un modello always-virgin non tollera due schemi possibili. Chi accende e' l'operatore di cella, mai un default. Un solo motore, nessun fork.

**Versione minima di Postgres: 14.** `GENERATED ALWAYS AS ... STORED` richiede 12, `starts_with()` richiede 11, e nessuno dei due ha un precedente nello schema attuale. `schema.rs` deve verificarla al boot e morire con un messaggio che nomina la versione trovata, altrimenti il fallimento e' un `apply` che gira a vuoto cinque volte dentro il retry sui deadlock (`schema.rs:88-103`) prima di uscire con un errore che non dice niente.

---

## 1. Architettura (decisa)

```
  SCHEDULE                      SWEEP (due-driven, leaderless)                LOG
  ────────                      ─────────────────────────────                ───
  POST /timers  ┐                claim(SKIP LOCKED, N righe)
  wire step 0b  ┼──> queen.log_timers ──> pack fuori transazione ──> fire ──> log_segments
  (upsert)      ┘     (staging, keyed by NAMES)      (zstd+xxh3)     |        log_txns
                                                                     └─ DELETE + push
                                                                        UNA transazione
  queen.kv  <── wire step 0 (kv accanto a pushes e acks) ──> stessa transazione, stesso commit
      ^
      └── sweeper, stessa passata, sotto-cadenza lenta: pota expires_at <= cutoff
```

**1.1 I timer non possono vivere nel log, e non e' una preferenza.** Il pop e' una scansione **contigua** di offset da `committed+1`: un frame futuro in mezzo o blocca il cursore per tutti, o viene saltato e perso. Una riga dentro un blob zstd solido non e' cancellabile: `queen.log_segments.blob` e' opaco a SQL e ha `STORAGE EXTERNAL` proprio perche' il broker ha gia' compresso (`001_log_schema.sql:85-86`, con il commento "ZERO secondary indexes: the PK is also the pop path"). E un frame futuro pendente pinnerebbe il suo segmento contro `log_start` per tutta la durata dell'attesa, cioe' un timer a 90 giorni terrebbe in vita 90 giorni di segmenti. Staging, quindi, e il log vede il messaggio solo al fuoco.

**1.2 Chiavate per NOME, non per id, e la ragione seconda e' piu' forte della prima.** La ragione ovvia: al momento dello schedule la coda di destinazione puo' non esistere, il provisioning e' pigro e avviene al fuoco dentro il ramo missing di `log_push_one_v1` (`003_log_push.sql:96-126`). Il precedente in casa e' `queen.hotlist_repairs`, PK `(tenant_id, queue_name, consumer_group)` (`001_log_schema.sql:186-195`), keyed by names per lo stesso motivo. La ragione **decisiva** e' un'altra: nessuna delle due tabelle nuove nomina un `partition_id`, quindi nessuna aggiunge una gamba di veto a `queen.log_partition_dead_v1` e quella scansione `O(partizioni)` non diventa piu' cara. C'e' anche un controllo meccanico gratuito: `log_partition_dead_v1` e' `LANGUAGE sql`, quindi risolve le tabelle **alla creazione**; vivendo in `006_log_maintenance.sql` e le tabelle nuove in `024`/`025`, aggiungere quella gamba **fa morire il boot**. Il numero di file e' la verifica di compilazione dell'invariante, e va scritto negli header di 024 e 025 con queste parole.

**1.3 Il fuoco e' in due fasi, e non si tiene mai una transazione aperta mentre si comprime.** Fase (a): `log_timers_claim_v1` prende N righe con `FOR UPDATE SKIP LOCKED`, mette `claimed_until` e un `claim_token`, e committa. Fase (b): il broker decomprime, raggruppa per `(tenant, queue, partition)`, impacchetta con `pack_frames + zstd + xxh3`, tutto **fuori** da qualunque transazione. Fase (c): `log_timers_fire_v1`, una sola chiamata, una sola transazione, che verifica i token, provisiona, pre-blocca le partizioni in ordine di `id` e pusha, cancellando i timer che ha appena consegnato.

**1.4 DELETE, non "mark done".** Con push e delete atomici il fuoco e' gia' exactly-once e non esiste nessuno stato da riconciliare dopo un crash. "E' stato mandato?" risponde il log, cercando il `txn` fisso del timer nella coda di destinazione. La tabella resta limitata dal lavoro PENDENTE, non dallo storico. Il prezzo, che va dichiarato e non nascosto, e' in §4.4: dopo il fuoco non esiste una lapide, quindi una cancel tardiva risponde `absent` e il chiamante deve sapere che `absent` significa "non e' piu' pendente", mai "non e' stato consegnato".

**1.5 Il KV generalizza `queen_streams.state`, non lo riscrive.** Meta' del meccanismo esiste: `queen_streams.state` e' `(query_id, partition_id, key) -> value JSONB` (`002_streams_schema.sql:58-65`), le `state_ops` di upsert e delete vivono dentro `log_streams_cycle_v1` (`007_log_streams.sql`), e `streams_state_get_v1` (`009_streams_state_get_v1.sql`) supporta gia' le letture per chiavi esplicite **e** per prefisso, con la precedenza dei filtri documentata. Quello che si generalizza e' lo **scoping**: via `partition_id`, dentro `namespace`. Il prezzo del taglio di `partition_id` e' esatto e va scritto nell'header di 024: lo streams model ottiene la serializzazione gratis perche' l'operatore tiene una lease esclusiva sulla partizione; `queen.kv` non ha nessuna lease, due worker possono colpire la stessa chiave insieme, ed **e' per questo** che `putIfAbsent` e `incr` sono primitive obbligatorie e non comodita'. Si eredita anche un difetto da non ripetere: `009_streams_state_get_v1.sql` usa `key LIKE prefix || '%'` senza escaping, e la cosa passa inosservata solo perche' `query_id` e `partition_id` restringono gia' a poche pagine di indice. Il KV usa `starts_with()`, che non conosce metacaratteri (§5.4).

**1.6 `expires_at` e' load-bearing, non un accessorio.** I due casi d'uso primari sono il marcatore `done:<txId>`, che deve vivere esattamente quanto la finestra di riconsegna, e il limitatore `quota:<cliente>:<ora>`, che deve chiudere la finestra. Entrambi vogliono un TTL. Una tabella senza retention naturale il cui TTL e' opzionale cresce in silenzio, e la crescita diventa una passivita' del prodotto pagata dall'operatore. Quindi la scadenza e' **obbligatoria sul wire**: ogni scrittura porta esattamente una fra `ttlMs` e `forever: true`, zero o due dichiarazioni sono un errore. E' una porta a senso unico presa nel verso giusto: un campo obbligatorio si puo' rendere opzionale dopo, mai il contrario. `NULL` in colonna significa "per sempre", e per sempre e' un opt-in esplicito, greppabile e verificabile in audit.

**1.7 Un solo componente di fondo, due orologi.** L'asimmetria e' di principio, non di comodo: il ritardo di un fuoco e' latenza di prodotto e si vede, il ritardo di una potatura KV non si vede affatto perche' il predicato `kv_live_v1` nasconde la riga scaduta gia' alla prima lettura, e costa solo dimensione di tabella. Quindi il fuoco e' due-driven su `min(visible_at)` e la potatura ha la sua sotto-cadenza fissa. Il precedente di forma e' `PARTITION_SWEEP_EVERY` in `retention.rs:92-99`, cioe' "questa fase e' O(spazio), non O(lavoro), quindi ha il suo orologio lento".

**1.8 Percorso separato dalla fusion, con un test che lo pinna.** La fusion del push (`fusion.rs`, `push_log_multi`) autocommitta su un client e il suo primo lock e' il pre-lock set-based sulle partizioni, la stessa forma di `003_log_push.sql:325-330`. Infilarci `queen.log_timers` metterebbe i due spazi di lock in ordine opposto sul percorso piu' caldo del prodotto. Il fuoco e' una SP sua che riusa `queen.log_push_one_v1` come unico code path dell'allocatore, esattamente come fa il wire (`005_log_ack.sql:996` in poi), e un test di modulo asserisce che il testo SQL del fuoco contenga `log_timers_fire_v1` e **non** contenga `log_push_multi_v1`.

**1.9 Lo sweeper e' leaderless, la retention no, e la differenza e' voluta.** `retention.rs:76` prende un advisory lock di **sessione** (`CLEANUP_LOCK_ID = 737_001`, verificato) e una sola replica lavora per ciclo; `stats.rs:31` usa `737_002`. Lo sweeper deve essere l'opposto su entrambe le dimensioni, due-driven e senza leader, perche' tutte le repliche devono drenare in parallelo condividendo il lavoro via `SKIP LOCKED`. **Non prende nessun advisory lock**, quindi non consuma un numero nuovo e non puo' chiudere cicli con lo spazio advisory (§2.1). Il numero `737_003` resta libero e va lasciato prenotato per `PLAN_S3_ARCHIVE.md`, che lo aveva gia' chiamato.

**1.10 Gli shard spalmano, non partizionano.** `shard = hashtextextended(chiave, 0) & 63`, colonna GENERATED STORED, modulo **fisso a 64 per sempre**: il modello always-virgin vale per lo SCHEMA, non per i DATI, quindi cambiarlo dopo ri-sharderebbe in silenzio le righe gia' scritte. Lo shard serve a due cose e solo a quelle: far partire ogni broker da una testa diversa dell'indice (`start_shard` derivato dal `server_id` e ruotato per ciclo) e dare una sveglia a costo fisso (64 seek invece di una scansione). **Non partiziona la proprieta'.** Ogni broker scandisce tutti i 64 shard e il coordinamento e' interamente `FOR UPDATE ... SKIP LOCKED`. La ragione e' una modalita' di guasto, non l'eleganza: qualunque ownership orfana gli shard di un broker morto, e un timer orfano non parte mai, che e' il guasto peggiore che questa feature possa avere. `QUEEN_SWEEPER_SHARDS` **non esiste**, e nessun commento nello schema deve suggerire il contrario.

---

## 2. Ordine di lock (deciso)

E' il gate del piano. Vale la stessa disciplina con cui l'ordine a due spazi e' scritto oggi in `005_log_ack.sql:884-897`, esteso a sei.

### 2.1 Inventario COMPLETO degli spazi

Una dimostrazione su quattro spazi e' finta se ne esistono sei. L'inventario reale, ricavato leggendo ogni `FOR UPDATE`, ogni `UPDATE` e ogni advisory lock in 003, 004, 005, 006, 007 e 020:

| # | spazio | granularita' | chi lo prende |
|---|---|---|---|
| 1 | `queen.kv` | riga `(tenant, ns, key)` | wire step 0, `kv_apply_v1` standalone, potatura sweeper (SKIP LOCKED) |
| 2 | `queen.log_timers` | riga `(tenant, queue, timer_key)` | wire step 0b, `log_timers_apply_v1` standalone, claim sweeper (SKIP LOCKED), fuoco |
| 3 | **ADV** advisory xact | chiave hash | ciclo streams (bloccante), pop (try, mai bloccante) |
| 4 | `queen.queues` | riga piu' attesa sull'xid dell'unique index | provisioning pigro: `003_log_push.sql:96-126`, `005_log_ack.sql:943-971` |
| 5 | `queen.log_partitions` | riga | push, pre-lock del wire (`005_log_ack.sql:973-984`), push multi (`003_log_push.sql:325-330`), streams sink, retention, cleanup (SKIP LOCKED) |
| 6 | `queen.log_consumers` | riga `(partition_id, group)` | pop (SKIP LOCKED), ogni ack, streams, cleanup per CASCADE |
| L | foglie | insert-only o scrittore unico | `log_segments`, `log_txns`, `log_dlq`, `retention_history`, `kv_usage`, `kv_quota` |

**Ordine totale dichiarato:**

```
queen.kv -> queen.log_timers -> ADV -> queen.queues -> queen.log_partitions -> queen.log_consumers -> foglie
```

Gli spazi 3 e 4 non erano nel brief ma **esistono gia' oggi**, e vanno nominati: senza, la prima persona che aggiunge un advisory lock allo sweeper crea un ciclo che il documento dichiarava impossibile. Lo sweeper in v1 non prende nessun advisory lock (§1.9), e la regola per chi lo aggiungera' e' scritta nell'header: chi prende un advisory lock **bloccante** deve prenderlo prima di toccare kv o timers, oppure non prenderlo affatto.

**Ordine intra-spazio**, necessario per ogni spazio in cui un attore prende piu' di una riga:

| spazio | ordine | dove |
|---|---|---|
| `queen.kv` | `(namespace, key)` crescente, `COLLATE "C"` esplicito | nuovo, §2.3 |
| `queen.log_timers` | `(queue, timer_key)` crescente, `COLLATE "C"` esplicito (il tenant e' costante dentro una chiamata) | nuovo, §2.3 |
| `queen.queues` | `name` crescente (`ORDER BY 2`) | esistente, `005_log_ack.sql:958` |
| `queen.log_partitions` | `id` crescente | esistente, `003_log_push.sql:329`, `005_log_ack.sql:983` |
| `queen.log_consumers` | `(partition_id, consumer_group)` crescente | esistente, `005_log_ack.sql` blocco ack |

### 2.2 La regola in una riga, e la distinzione che la rende falsificabile

> **Nessun attore puo' ACQUISIRE un lock su `queen.kv` o `queen.log_timers` dopo aver acquisito un lock su `queen.queues`, `queen.log_partitions` o `queen.log_consumers`.**

La parola **acquisire** e' load-bearing e va scritta in grassetto nell'header. Ri-toccare in scrittura una riga che **questa stessa transazione tiene gia'** non e' un'acquisizione, e' un no-op sul grafo delle attese. Il precedente e' esplicito in casa: il pre-lock multi di `003_log_push.sql:132-134` dice che ri-bloccare una riga gia' tenuta e' un no-op. Senza questa distinzione la regola e' infalsificabile e la prima persona che la applica alla lettera riscrive il fuoco, che nel suo passo finale cancella righe timer **che ha gia' bloccato al passo iniziale** (§2.4, C1 e C2).

Se la regola e' vera per ogni attore, e ogni attore rispetta l'ordine intra-spazio, il grafo delle attese e' aciclico per il teorema dell'ordinamento delle risorse. Le due condizioni sono entrambe necessarie: la seconda copre gli attori che prendono piu' righe dello stesso spazio, dove la prima non dice nulla.

### 2.3 Attore per attore

| # | attore | sequenza degli spazi | attende? | sottosequenza crescente? |
|---|---|---|---|---|
| 1 | transaction wire | KV(0) → T(0b) → Q(1) → P(2) → C(4) | si', ovunque | si', prefisso esatto dell'ordine totale |
| 2 | push semplice, push multi, flush fusion | Q → P | si' | si' |
| 3 | ack, flush ack-registry | C | si' | si', singleton; gia' dichiarato in `005_log_ack.sql` |
| 4 | pop e varianti | (ADV try) → C con SKIP LOCKED | **no** | si', e non puo' essere un arco del grafo: non attende mai |
| 5 | claim sweeper | T con SKIP LOCKED | **no** | si', singleton |
| 6 | fuoco sweeper | T(verifica, SKIP LOCKED) → Q → P → T(delete di righe gia' tenute) | attende su Q e P, **mai** su T | si', per la regola di §2.2 |
| 7 | potatura KV sweeper | KV con SKIP LOCKED | **no** | si', singleton |
| 8 | rollup uso sweeper | letture su KV senza row lock → foglia | si' su una foglia a scrittore unico | si' |
| 9 | retention | P, una riga | si' | si', singleton |
| 10 | cleanup partizioni | P (molte, id crescente, SKIP LOCKED) → C per CASCADE | non attende su P | si' |
| 11 | ciclo streams | ADV → Q → P → C | si' | si', e non tocca ne' KV ne' T |
| 12 | KV standalone (POST, PUT, DELETE) | KV | si' | si', singleton |
| 13 | timers standalone (POST, DELETE) | T | si' | si', singleton, e **non tocca `queen.queues`**: e' il senso del keyed-by-names |
| 14 | DLQ da ack | C → foglia | si' | si' |
| 15 | DLQ da timer velenoso | T → Q → P → foglia | si' | si', e **non tocca C**, per questo non riusa `log_dlq_head_v1` (§6.2) |
| 16 | stats, hotlist reseed | letture, foglia | non applicabile | si' |

Gli unici attori che toccano piu' di uno spazio sono 1, 2, 6, 10, 11, 14 e 15. Nessuna coppia `(X, Y)` compare in ordine invertito in due sequenze diverse: ogni sequenza si legge da sinistra a destra sullo stesso ordine totale. Gli attori 4, 5 e 7 non attendono mai, quindi non possono nemmeno essere un arco del grafo, qualunque ordine visitino.

**Il caso che sembra un ciclo e non lo e'.** Il wire tiene una riga KV mentre attende una partizione tenuta da un flush fusion. Il flush fusion non chiede mai KV: la catena `wire → fusion` termina quando fusion committa. Una catena non e' un ciclo. E' pero' il costo dichiarato in §18.2.

**Il caso multi-tenant che sembra sbagliato e non lo e'.** Lo sweeper ordina `(tenant_id, queue, timer_key)` perche' il suo batch e' multi-tenant; il wire ordina `(queue, timer_key)` a tenant fissato. Ristretti alle righe che i due possono contendersi, che per definizione hanno lo stesso `tenant_id`, i due ordini coincidono. Va scritto, perche' e' il tipo di asimmetria che il prossimo lettore "corregge".

### 2.4 I cicli TROVATI e la correzione di ciascuno

Cinque sono cicli veri che l'implementazione ovvia introduce. Due sono difetti non ciclici che vanno chiusi qui perche' nascono dallo stesso ragionamento.

**C1. Il fuoco che pusha prima di cancellare, in due transazioni.** L'implementazione naturale riusa il percorso fusion per ottenere i base offset e poi cancella i timer in una chiamata separata: sequenza `P → T`, contro il `T → P` del wire. Ciclo, sul percorso piu' caldo del prodotto. **Correzione decisa:** una sola SP `queen.log_timers_fire_v1`, **un solo corpo canonico** (§6.2), che nel suo passo 2 blocca le righe timer con `FOR UPDATE SKIP LOCKED`, e poi ai passi 3, 4 e 5 fa provisioning, pre-lock ascendente delle partizioni e push, e cancella per ultimo righe **gia' tenute dal passo 2**. Nessuna acquisizione di T dopo P: la delete finale e' un no-op sul grafo, per §2.2. Il broker comprime fuori e fa **una** chiamata.

**C2. Il pre-lock esplicito prima della DELETE: CANCELLATO.** Un documento di design prescriveva `PERFORM 1 FROM queen.log_timers t WHERE t.id = ANY(v_claimed) ORDER BY ... FOR UPDATE` prima della delete. Va cancellato per due motivi indipendenti. Primo: `queen.log_timers` **non ha una colonna `id`**, la PK e' `(tenant_id, queue, timer_key)`, quindi lo statement non compila e il boot muore. Secondo, ed e' quello che conta: `FOR UPDATE` senza `SKIP LOCKED` renderebbe il fuoco un **waiter** nello spazio T, contraddicendo l'invariante che lo stesso header dichiara quattro paragrafi sopra. Il guasto concreto: una cancel dentro un transaction wire tiene la riga per tutta la durata del bundle (provisioning, pre-lock, push, ack, fsync), e il fuoco, che prima saltava la riga e buttava un segmento, ora **aspetta**, tenendo il lease di 199 altri timer, uno slot `Lane::Maint` e una connessione. Con `PARALLELISM=1` un singolo cancel lento ferma il fuoco di tutto il broker. La correzione risolveva un bug che non esiste, perche' claim e delete nel corpo canonico sono nella **stessa** transazione e la delete non acquisisce nulla di nuovo. Da scrivere nell'header di 025 con questa forza: **il fuoco non prende MAI un lock che aspetta nello spazio `queen.log_timers`.**

**C3. La collation dell'ORDER BY.** Verificato: `grep -rn COLLATE server/sql/` restituisce **zero occorrenze** in tutto lo schema. Non esiste nessun precedente, quindi nessuna certezza puo' essere assunta. Ordinare con `ORDER BY op->>'ns', op->>'key'` usa la collation di **default del database**; due pod con `lc_collate` diversi, o due minor di ICU diverse, che e' il caso reale nei rolling upgrade di immagini base, ordinano `saga:A_1` e `saga:A-1` in ordine opposto: due bundle sovrapposti prendono le righe in ordine invertito e producono `40P01` solo su alcune installazioni e solo con certe chiavi, la peggior forma di deadlock. **Correzione in tre pezzi, tutti obbligatori:**
1. `COLLATE "C"` su **tutte e quattro** le colonne di nome: `queen.kv.namespace`, `queen.kv.key`, `queen.log_timers.queue`, `queen.log_timers.timer_key`. Un documento di design lo dichiarava presente su `log_timers` mentre la DDL non lo aveva: qui e' presente su entrambe.
2. Ogni `ORDER BY` su un'estrazione JSONB pinna la collation esplicitamente, perche' `op->>'ns'` **non** eredita la collation della colonna: `ORDER BY (o.value->>'ns') COLLATE "C", (o.value->>'key') COLLATE "C"`.
3. **Il broker non pre-ordina mai.** L'ordinamento e' una proprieta' della SP, in un solo posto. Se un giorno il Rust ordinasse "per aiutare", basterebbe una libc diversa fra due pod per riaprire il ciclo.

Piu' una verifica che va fatta sul rig e non assunta, ed e' un criterio di uscita della Fase 1: **confermare che il confronto fra una colonna `COLLATE "C"` e un parametro `text` non sollevi `42P22 could not determine which collation to use`**. Le due derivazioni implicite sono in conflitto in alcune forme. Se lo fa, ogni confronto va scritto `k.key >= p_prefix COLLATE "C"`, incluso dentro `kv_prefix_end_v1`. Zero precedente in casa significa zero certezza: si prova, non si assume.

**C4. Un lock nel rider KV sul pop.** Un `FOR UPDATE` o un `FOR SHARE` nel ramo KV di `log_pop_list_v1` darebbe `C → KV`, perche' a quel punto il pop tiene gia' le righe consumer, contro il `KV → C` del wire: ciclo sul percorso di lettura piu' caldo. **In v1 il rider e' tagliato** (§18.1), quindi il ciclo non e' raggiungibile. Quando il rider tornera' (Fase 9), il divieto va scritto nell'header di `log_pop_list_v1` come divieto e non come scelta: solo MVCC, mai un row lock. Corollario non ovvio che vale **gia' oggi**: una lettura MVCC prende comunque `ACCESS SHARE` di tabella, che confligge con `ACCESS EXCLUSIVE`, quindi `vacuum_truncate = off` su `queen.kv` non e' solo una difesa dal wobble, e' **parte dell'ordine di lock**.

**C5. Un `partition_id` dentro `log_timers` o `kv`.** Se una delle due tabelle lo portasse, il cleanup partizioni avrebbe bisogno di una gamba di veto e diventerebbe `P → T`, contro il `T → P` del wire e del fuoco. **Correzione:** keyed by names, con il controllo di compilazione descritto in §1.2.

**C6. Il provisioning pigro dallo step 0b.** Se `log_timers_apply_v1` provvisionasse `queen.queues` per validare la coda di destinazione, il wire avrebbe **due** statement che scrivono `queen.queues` nella stessa transazione, con insiemi diversi: quello dei timer allo step 0b e quello dei push allo step 1 (`005_log_ack.sql:952-971`). Due transazioni con insiemi incrociati si attendono a vicenda sull'xid dell'unique index `(tenant_id, name)`, e l'`ORDER BY 2` di ciascuno statement non lo previene perche' sono statement **diversi**. **Correzione:** i timer non scrivono mai `queen.queues`. Nessuna validazione della coda allo schedule; la coda nasce al fuoco.

**D7. La quota scritta nel wire: NON un ciclo, una cliff.** Un `UPDATE queen.kv_usage ... WHERE tenant_id = v_tenant` allo step 0 sarebbe **una riga sola per tenant tenuta per tutta la durata del bundle**: ogni transaction wire di quel tenant si serializzerebbe su di essa. Non deadlocka, e' peggio, e' un tetto di throughput pari a uno su durata del bundle, cioe' circa 80-100 bundle al secondo con la durabilita' piena misurata su questo stack. **Vietato nell'header:** il percorso di scrittura legge la quota dalla cache in `AppState` e non emette nessun SQL (§9).

**D8. `ALTER COLUMN ... SET STORAGE`: cancellato da entrambe le DDL.** Non e' fra le forme a lock ridotto: prende `ACCESS EXCLUSIVE`, incondizionatamente, **a ogni boot**, perche' `schema.rs` riapplica sempre tutto. Ed e' un no-op: `EXTENDED` e' gia' il default sia per `jsonb` sia per `bytea`. Prende il lock e basta. Su `queen.log_timers` il danno e' concreto: durante un rolling restart, ogni transaction wire in volo che porta un array `timers` si ferma allo step 0b, cioe' **prima** del pre-lock delle partizioni, e trascina con se' i push e gli ack di quel bundle. E' esattamente lo scenario che il commento `vacuum_truncate = off` della stessa DDL dichiara di voler impedire, autoinflitto tre righe piu' sotto. **Correzione:** cancellare entrambi gli statement, tenere solo il commento che spiega perche' `EXTENDED` e' il valore giusto. Se un giorno servisse davvero un valore diverso, e' una guardia `DO $$` su `pg_attribute.attstorage`, nello stile di `020_log_partition_counters.sql`.

*Collaterale fuori scope, da segnalare e non correggere qui:* `001_log_schema.sql:86` ha oggi la stessa forma non guardata su `queen.log_segments.blob`, quindi ogni riavvio prende un ACCESS EXCLUSIVE breve sulla tabella piu' calda del motore. E' preesistente, la finestra e' minuscola (catalogo, nessuna riscrittura), ma e' il precedente da **non** copiare.

### 2.5 L'ottimizzazione ovvia che va rifiutata

Contro noi stessi: lo step 0 e' la posizione **peggiore** per il tempo di tenuta. Un lock su `queen.kv` preso allo step 0 resta preso durante il provisioning, il pre-lock delle partizioni, tutti i push con i loro blob e tutti gli ack, fino alla fsync. Metterlo in fondo ridurrebbe la tenuta di quasi tutta la durata del bundle, e l'atomicita' non cambierebbe: un `required` perso in fondo fa comunque `RAISE` e i push rollbackano lo stesso.

Due ragioni per lo step 0, e una dimostrazione che la via di mezzo e' insensata.

1. **Il fallimento e' il percorso comune.** Il marcatore di idempotenza perde a ogni riconsegna legittima. Allo step 0 quel fallimento costa una `INSERT ... ON CONFLICT` e un `RAISE`, prima che venga preso un solo lock di partizione. In fondo, costa l'intero bundle scritto e poi buttato: WAL, allocatore, blob. Il caso d'uso numero uno del prodotto diventerebbe il piu' caro.
2. **I timer non hanno scelta.** Il fuoco fa `T → P`. Se il wire mettesse i timer in fondo, il ciclo con il fuoco sarebbe immediato. E tenere kv e timers su due lati opposti significa due regole invece di una: la prossima persona ne applica una sola.

**La via di mezzo e' UNSOUND, ed e' la parte importante.** L'idea attraente e' splittare: le op con precondizione allo step 0 perche' devono abortire presto, quelle incondizionate in fondo perche' non possono abortire niente. Costruzione del ciclo: T1 prende `kv:a` allo step 0, prende P allo step 2, chiede `kv:b` allo step 5; T2 prende `kv:b` allo step 0 e chiede P allo step 2, tenuta da T1. T1 attende `kv:b`, T2 attende P. Ciclo. Lo split introduce archi `P → KV` e `C → KV` **dentro un singolo protocollo**, e viola la regola di §2.2 nella forma piu' difficile da vedere in review, perche' entrambi i frammenti presi da soli sembrano corretti. **O tutte allo step 0, o tutte in fondo, mai divise.** Scegliamo lo step 0, e il costo di tenuta e' il rischio accettato §18.2.

Mitigazione concreta e gratuita: **il broker non manda al wire i bundle di solo KV.** Un `transaction()` che contiene esclusivamente op kv viene instradato su `kv_apply_v1` diretta, che e' una transazione corta. Una riga in `handlers/data.rs`, e toglie dal percorso caldo la categoria di chiamata piu' probabile fra quelle che non hanno alcun bisogno di starci.
