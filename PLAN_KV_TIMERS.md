# PLAN_KV_TIMERS: messaggi schedulati e stato transazionale sul motore log

Rev 1.1 del 2026-08-18: **§20.4 e' ratificata con l'esito rovesciato** rispetto alla proposta, i due flag di boot non esistono piu', e §16 (l'ordine di accensione) e' riscritto da capo perche' non si spedisce piu' niente al buio. Le sezioni toccate sono §0, §7.1, §9.4, §9.5, §11.2, §11.3, §12.1, §13.2, §15, §16, §17.1, §17.3, §18.6, §19.2 e §20.

Rev 1.0 del 2026-08-17. Branch `kvtimer`, base `f1aad78dc684452505bb644f5e0f9eecde1c9861`. Ogni riferimento `file:riga` qui sotto risolve contro quello sha (`git show f1aad78:percorso`). I file SQL sono nominati con la numerazione VERA di oggi (`server/sql/procedures/001..023`), non con quella pre-rinumerazione.

Status: non iniziato. Fase 0 (contratti) da chiudere prima di scrivere una riga di SQL.

Fondato su cinque documenti di design (API KV, sweeper e timer, wire e ordine di lock, quote e osservabilita', superfici dei client) e su tre revisioni avversariali. La §19 mappa ogni difetto BLOCCANTE e SERIO sollevato dai revisori sulla sezione che lo chiude, oppure lo dichiara rischio accettato. Nessuno e' ignorato in silenzio.

---

## 0. Goal e non-goal

**Goal TIMER.** Un messaggio schedulato entra nel log al momento della consegna e non prima. Fino ad allora vive in una tabella di staging `queen.log_timers` chiavata per nome, dove puo' essere cancellato o riprogrammato. Al fuoco, la riga sparisce e il messaggio compare nella coda di destinazione dentro **una sola** transazione Postgres, quindi non esiste mai uno stato "consegnato a meta'" e la domanda "e' partito?" ha una sola autorita', il log.

**Goal KV.** Uno store chiave/valore transazionale `queen.kv`, chiavato `(tenant, namespace, key)`, con scadenza obbligatoria, utilizzabile **dentro** la stessa transazione di push e ack. Il valore del prodotto non e' il KV: e' che il marcatore di idempotenza, l'effetto e l'avanzamento del cursore committano insieme. Un KV a lato non puo' darlo, per nessun prezzo.

**Perche' un solo piano.** Le due feature condividono quattro cose, e sono quelle che costano: **un solo componente di fondo** (lo sweeper fa fuoco ai timer e pota le chiavi scadute), **un solo ordine di lock** esteso da due spazi a sei, **una sola superficie di quota e di tenancy** (una riga per tenant per tutto lo stato), **una sola migrazione della webdoc** e degli esempi. Spedirle separate significherebbe scrivere due volte il loop di fondo, due volte la dimostrazione di assenza di deadlock e due volte la storia delle quote. Sono pero' **indipendenti nell'ordine di sviluppo**: le fasi 2 e 3 (§16) sono scollegate, e un cliente puo' volere solo i timer. Non lo sono piu' nel **rilascio**: non ci sono due interruttori di boot da spedire spenti, quindi quando il binario atterra atterrano tutte e due (§0). L'unica indipendenza che sopravvive a runtime e' quella dei kill switch, che sono tre e distinti (§12.1).

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

**Deployment stance: non esistono flag di boot, perche' kv e timer sono IL DEFAULT.** Decisione di Alice, 2026-08-18 (§20.4, ratificata con l'esito rovesciato rispetto alla proposta). `QUEEN_KV_ENABLED` e `QUEEN_TIMERS_ENABLED` **non esistono piu'**, e non sono stati girati a `true`: sono stati **tolti**. La ragione e' che non esiste `QUEEN_PUSH_ENABLED` ne' `QUEEN_POP_ENABLED`, e l'esistenza stessa di un flag di boot **e'** l'affermazione che la superficie sia opzionale, cioe' che un client possa legittimamente trovare una cella senza, e che ogni SDK, pagina e cruscotto debba portarsi dietro il caveat "se abilitato". Quindi: le rotte si registrano sempre, il ramo kv del wire e' sempre accettato, lo sweeper si spawna sempre. Le tabelle si creano a ogni boot come prima, ma ora perche' vengono usate, non perche' da vuote costano zero. Un solo motore, nessun fork.

**Cosa resta, e non va confuso con un gate.** I kill switch di **runtime** di `server/src/switches.rs` (`kv_enabled`, `timers_schedule_enabled`, `timers_fire_enabled`, in `queen.system_state`) restano tutti, e **nascono accesi**. Un gate si accende per provare una cosa: si legge una volta al boot, decide se la superficie **esiste**, cambiarlo e' un rollout, e ogni cella della flotta puo' legittimamente rispondere in modo diverso. Un kill switch si spegne per fermare una cosa che sta gia' girando e sta facendo male: si legge a ogni chiamata, la superficie esiste comunque, scatta subito su una cella che un operatore ha in mano alle tre di notte, e ci si aspetta che venga **riacceso**. E' la stessa classe di `maintenance_mode` e `pop_maintenance_mode`. Conseguenza sugli status, che vale la pena scrivere qui perche' e' la piu' visibile: **niente risponde piu' 404 per essere spento**; una superficie in pausa risponde 503 con `Retry-After` sulle rotte e un rifiuto **permanente** dentro il wire (§9.5, §12.1). L'unica manopola che ancora ferma un pezzo e' `QUEEN_SWEEPER=false`, che non e' un gate ma lo spegnimento del **mietitore** di superfici che continuano comunque a girare (§7.1).

**L'interlock di boot resta, ed e' fatale, ma e' keyed sulla sola modalita' di tenancy:** `QUEEN_TENANCY_HEADER=1` pretende `QUEEN_KV_TRUSTED_PROXY=1` (§9.4, §13.2). La cosa insicura non e' il KV, e' un'identita' di tenant opaca che nessuno valida; il KV l'ha solo resa **visibile**, essendo la prima superficie del prodotto indirizzabile puramente per NOME. Con il flag KV sparito, quel requisito diventa **incondizionato** per chi gira con l'header, che e' la forma onesta che aveva gia'.

**E niente si spedisce piu' al buio.** Dal momento in cui il binario atterra, kv e timer sono vivi su ogni cella che lo esegue. E' un cambio di strategia di rilascio, non un dettaglio: §16 lo scrive per esteso, insieme a cosa lo sostituisce come rete di sicurezza.

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

**1.6 `expires_at` e' load-bearing, non un accessorio.** I due casi d'uso primari sono il marcatore `done:<txId>`, che deve vivere esattamente quanto la finestra di riconsegna, e il limitatore `quota:<cliente>:<ora>`, che deve chiudere la finestra. Entrambi vogliono un TTL. Una tabella senza retention naturale il cui TTL e' opzionale cresce in silenzio, e la crescita diventa una passivita' del prodotto pagata dall'operatore. Quindi la scadenza e' **obbligatoria sul wire**: ogni scrittura porta esattamente una fra `ttlSeconds` e `forever: true`, zero o due dichiarazioni sono un errore. E' una porta a senso unico presa nel verso giusto: un campo obbligatorio si puo' rendere opzionale dopo, mai il contrario. `NULL` in colonna significa "per sempre", e per sempre e' un opt-in esplicito, greppabile e verificabile in audit.

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
| 11 | ciclo streams | ADV → Q → P → C | si' | si', e non tocca ne' KV ne' T **per decisione**, §18.9: e' l'unico attore che prende un advisory BLOCCANTE, e l'ordine totale mette KV e T prima di ADV |
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

---

## 3. Schema (file nuovi, nessuna migrazione)

### 3.1 I due file e l'edit a `schema.rs`

Due file nuovi, `server/sql/procedures/024_kv.sql` e `server/sql/procedures/025_log_timers.sql`, aggiunti in coda alla lista `PROCEDURES` di `server/src/schema.rs`, subito dopo la riga `("023_prometheus.sql", ...)` che oggi chiude la lista (verificato: e' l'ultima):

```rust
    ("024_kv.sql", include_str!("../sql/procedures/024_kv.sql")),
    ("025_log_timers.sql", include_str!("../sql/procedures/025_log_timers.sql")),
```

**GOTCHA che costa mezza giornata a chi non lo sa:** l'SQL e' `include_str!`-embedded, quindi un edit a un file `.sql` non ha nessun effetto finche' non si rilancia `cargo build`. Vale per ogni ciclo di test su questi due file.

L'ordine e' load-bearing solo dove un corpo `LANGUAGE sql` risolve le tabelle alla creazione. `005_log_ack.sql` si applica **prima** di 024 e 025, e chiamera' `queen.kv_apply_v1` e `queen.log_timers_apply_v1`: funziona **solo** perche' il corpo di `log_transaction_wire_v1` e' `plpgsql`, che risolve a runtime. Con `LANGUAGE sql` il boot morirebbe alla creazione. Va scritto nel commento accanto alla chiamata, perche' e' l'unica cosa che tiene in piedi l'ordine dei file.

### 3.2 `queen.kv` e le tabelle di quota e uso

Una sola definizione autoritativa, qui, per **tutte** le tabelle di quota e misura delle due feature. I documenti di design ne avevano quattro forme incompatibili e il modello di deploy non perdona: `CREATE TABLE IF NOT EXISTS` e' un **no-op silenzioso sulla forma**, quindi una cella che ha gia' bootato con la versione a tre colonne conserva la tabella vecchia e la prima lettura di una colonna nuova fallisce con `42703` **in produzione, sul percorso di schedule**, classificata configurazione e quindi non ritentata. Always-virgin vale per lo SCHEMA e per le FUNZIONI, non per i DATI di una tabella di configurazione scritta da un operatore, che non si puo' droppare.

**Regola decisa, con precedente in casa:** ogni tabella di configurazione o di misura si scrive `CREATE TABLE IF NOT EXISTS` **seguito da** una serie di `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, una per colonna, esattamente come `019_worker_metrics.sql:95-119` fa con dodici colonne di fila su `queen.queue_lag_metrics`. Costa dodici righe e rende la tabella estendibile per sempre.

```sql
-- ============================================================================
-- queen.kv: stato chiave/valore transazionale, usabile DENTRO una transazione.
--
-- Identita' = (tenant, namespace, key) e NIENT'ALTRO. Deliberatamente nessun
-- partition_id, a differenza di queen_streams.state (002_streams_schema.sql:58-65):
-- una chiave KV e' un'identita' di BUSINESS e il chiamante non sa, e non deve
-- avere bisogno di sapere, su quale partizione cade.
--
-- Il prezzo di togliere partition_id e' esatto ed e' scritto qui perche' nessuno
-- debba riscoprirlo: il modello streams ottiene la serializzazione gratis perche'
-- l'operatore tiene una lease esclusiva sulla partizione. Questa tabella non ha
-- nessuna lease. Due worker POSSONO colpire la stessa chiave insieme. Per questo
-- put/expect, putIfAbsent e incr sono primitive obbligatorie e non comodita', e
-- per questo un read-modify-write fatto dal CHIAMANTE su due round trip e'
-- unsound a meno che la chiave derivi dalla chiave di partizione.
--
-- Proprieta' acquisita gratis, e ragione per tenerla chiavata per NOMI: niente
-- qui referenzia una partizione, quindi questa tabella NON aggiunge una gamba a
-- queen.log_partition_dead_v1 (006_log_maintenance.sql) e quella scansione
-- O(partizioni) non diventa piu' cara. Stessa proprieta' e stessa ragione di
-- queen.hotlist_repairs (001_log_schema.sql:186-195). E c'e' un controllo di
-- COMPILAZIONE gratuito: log_partition_dead_v1 e' LANGUAGE sql, quindi risolve
-- le tabelle alla creazione; stando in 006 e questa tabella in 024, aggiungere
-- quella gamba FA MORIRE IL BOOT. Il numero di file e' la verifica dell'invariante.
--
-- I namespace NON sono registrati da nessuna parte: come le code
-- (003_log_push.sql:96-126) un namespace esiste se e solo se esiste una riga.
-- Namespace sconosciuto = risultato vuoto, mai un errore. Il charset e' validato
-- (queen.kv_check_names_v1) cosi' le classi di typo comuni non coniano in
-- silenzio un namespace fantasma da cui si legge vuoto per sempre.
--
-- INVARIANTE DI MANUTENZIONE: nessun percorso di questa feature crea una
-- partizione, per richiesta o per chiave. La manutenzione di fondo di Queen
-- scala con le PARTIZIONI.
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen.kv (
    -- Stesso default di queen.hotlist_repairs (001_log_schema.sql:187): un
    -- chiamante pre-tenancy atterra nel tenant di default, comportamento
    -- byte-identico.
    tenant_id  UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    -- COLLATE "C" su ENTRAMBE le colonne di nome, e regge due cose distinte:
    --   1. ORDINE DI LOCK. E' il fondamento dell'ordine intra-spazio di
    --      queen.kv (§2.3 del piano). Sotto la collation di default due pod con
    --      lc_collate o ICU diversi ordinano due chiavi in ordine opposto e due
    --      bundle sovrapposti deadlockano, su alcune installazioni soltanto.
    --   2. Sargabilita' del prefisso e stabilita' della paginazione: l'ordine e'
    --      quello dei byte, quindi identico fra macchine e stabile a un upgrade
    --      di libc/ICU. Un cursore il cui significato dipende dal locale del
    --      sistema operativo e' una corruzione in attesa di una minor version.
    -- Conseguenza da documentare: i risultati per prefisso tornano in ordine di
    -- BYTE, quindi le chiavi non ASCII non sono in ordine alfabetico di locale.
    -- ATTENZIONE: un'estrazione da JSONB NON eredita questa collation. Ogni
    -- ORDER BY su op->>'key' deve scrivere COLLATE "C" esplicitamente.
    namespace  TEXT COLLATE "C" NOT NULL,
    key        TEXT COLLATE "C" NOT NULL,
    value      JSONB NOT NULL,        -- 'null'::jsonb e' un VALORE legale
    -- Token opaco e UNICO. NON e' monotono e NON e' un conteggio di scritture:
    -- con CACHE ogni backend preleva un blocco, quindi il backend A puo' emettere
    -- 91005 dopo che B ha emesso 92000, in tempo reale, per la stessa chiave.
    -- I chiamanti fanno SOLO uguaglianza, mai aritmetica e mai confronto d'ordine.
    -- Viene da una sequenza e mai da version+1, cosi' una chiave scaduta, potata
    -- e ricreata non puo' riemettere una versione che un vecchio detentore porta
    -- ancora (l'ABA di un contatore per lineage). I buchi sono attesi.
    version    BIGINT NOT NULL,
    -- LOAD-BEARING, e OBBLIGATORIO sul wire (queen.kv_apply_v1). NULL = per
    -- sempre, ed e' un opt-in esplicito e greppabile, mai un default.
    expires_at TIMESTAMPTZ,
    -- Spalma-contesa, NON una partizione di proprieta': ogni broker scandisce
    -- ogni shard (header di server/src/sweeper.rs). GENERATED, cosi' un update
    -- non puo' mai spostarlo. Il modulo e' FISSO A 64 PER SEMPRE: il modello di
    -- deploy e' always-virgin per lo SCHEMA, non per i DATI, quindi cambiarlo
    -- ri-sharderebbe in silenzio le righe gia' scritte. Non esiste nessuna env
    -- che lo cambi e non deve esistere.
    -- NOTA: hashtextextended non e' garantito stabile fra major di PostgreSQL.
    -- STORED lo congela alla scrittura, quindi va bene; ma nessuna QUERY deve
    -- mai ricalcolarlo per confronto, o su una major diversa otterrebbe uno
    -- shard diverso da quello memorizzato.
    shard      SMALLINT NOT NULL
               GENERATED ALWAYS AS ((hashtextextended(key, 0) & 63)::smallint) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, namespace, key)
);

-- CACHE alto per una ragione sola e va scritta: un nextval per scrittura sul
-- percorso caldo produrrebbe un WAL record per scrittura. NON e' una difesa
-- contro il canale laterale della sequenza globale (§13.3): con un pool le
-- connessioni sono assegnate a tenant arbitrari, quindi il blocco di un backend
-- viene consumato da tenant interlacciati e il gap resta informativo.
CREATE SEQUENCE IF NOT EXISTS queen.kv_version_seq AS BIGINT START 1 CACHE 1000;

-- Controllo della churn, copiato verbatim da queen.log_partitions
-- (001_log_schema.sql:47-68, verificato): stessa forma di problema, i contatori
-- sono UPDATE-heavy e le righe con TTL sono DELETE-heavy, quindi fra una passata
-- e l'altra le tuple morte superano quelle vive e scale_factor 0 tiene
-- l'autovacuum a rifirare ogni naptime.
--
-- vacuum_truncate = off per la ragione scritta per esteso a 001_log_schema.sql:54-63
-- (2026-07-24, colto dal vivo con pg_stat_progress_vacuum in fase "truncating
-- heap"): la troncatura heap prende un ACCESS EXCLUSIVE, e QUESTA tabella e'
-- esattamente la forma che lo provoca, perche' si svuota e si riempie in stato
-- stazionario, che e' tutto il senso di expires_at. Quel lock e' stata la causa
-- radice dell'intera classe "wobble". Non riabilitarlo mai.
ALTER TABLE queen.kv SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 500,
    autovacuum_vacuum_cost_delay = 0,
    vacuum_truncate = off);

-- fillfactor: PER ANALOGIA con 001_log_schema.sql:39-46, NON misurato. Lo stato
-- stazionario e' UPDATE in place di colonne non indicizzate (value, version,
-- updated_at) su righe che devono restare HOT (i contatori). Va sostituito con
-- un numero MISURATO dopo il primo soak, nello stile dei blocchi datati di 001.
ALTER TABLE queen.kv SET (fillfactor = 70);

-- NESSUN "ALTER COLUMN value SET STORAGE EXTENDED" qui. EXTENDED e' gia' il
-- default per jsonb, quindi lo statement non farebbe nulla eccetto prendere un
-- ACCESS EXCLUSIVE su questa tabella A OGNI BOOT (schema.rs riapplica sempre
-- tutto). Il valore giusto e' EXTENDED, e questo commento e' il posto dove
-- dirlo: un blob di log e' EXTERNAL perche' zstd ha gia' girato, un valore KV e'
-- JSON grezzo che la compressione TOAST accorcia davvero, e al tetto di 64 KiB
-- un datum compresso inline e' un buffer dove un chunk fuori linea e' tre.

-- L'UNICO indice secondario, con un solo lettore: lo sweeper. Parziale, cosi'
-- ogni chiave scritta con "forever" qui non costa niente. shard in testa perche'
-- lo sweeper filtra per shard e poi fa range scan su expires_at.
-- Deliberatamente NON creati: un indice su updated_at (queen_streams.state ne
-- porta uno e nessun lettore lo interroga) renderebbe non-HOT OGNI update di
-- contatore; un indice su value non ha nessuna query da servire, perche' il
-- confine dichiarato e' chiavi e prefisso.
CREATE INDEX IF NOT EXISTS idx_kv_shard_expires
    ON queen.kv (shard, expires_at) WHERE expires_at IS NOT NULL;

-- ---------------------------------------------------------------------------
-- CONFIGURAZIONE di quota, per tenant. UNA riga per tenant per TUTTO lo stato
-- delle due feature: due tabelle di quota divergerebbero. Separata dalla misura
-- qui sotto di proposito: la configurazione la scrive un operatore, la misura la
-- sovrascrive lo sweeper, e mescolarle rende "chi ha scritto questo" senza
-- risposta.
--
-- NULL = illimitato SOLO quando la tenancy e' spenta. Con QUEEN_TENANCY_HEADER
-- attivo, l'ASSENZA della riga e' un DINIEGO, non un permesso (§9.2): il tenant
-- non e' validato contro niente (server/src/tenant.rs:16-19), quindi un default
-- fail-open significa spazio chiavi illimitato per un id inventato.
--
-- ADD COLUMN IF NOT EXISTS, non solo CREATE: precedente 019_worker_metrics.sql:95-119.
-- CREATE TABLE IF NOT EXISTS e' un no-op SILENZIOSO sulla forma, quindi senza
-- questi ALTER una cella che ha gia' bootato con una versione precedente scopre
-- la colonna mancante con un 42703 in produzione, sul percorso di schedule.
CREATE TABLE IF NOT EXISTS queen.kv_quota (
    tenant_id  UUID PRIMARY KEY,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS enabled            BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS max_rows           BIGINT;
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS max_bytes          BIGINT;
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS max_timers         BIGINT;
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS max_timer_horizon_s BIGINT;
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS max_reads_per_sec  INTEGER;
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS max_writes_per_sec INTEGER;

-- MISURA, scritta dallo sweeper su sotto-cadenza lenta e cachata in AppState da
-- ogni broker. PK per (tenant_id) e non per (tenant_id, shard): il modello
-- deciso e' "ogni broker puo' misurare qualunque shard" (§1.10), quindi una PK
-- per shard produrrebbe contesa write-write fra N broker sulle stesse righe e
-- N volte il costo di scansione. Last-writer-wins su computed_at.
-- L'enforcement e' MORBIDO e IN RITARDO per costruzione: lo sforo e' limitato in
-- §9.3, e la formula pubblicata e' quella vera, non quella ottimistica.
CREATE TABLE IF NOT EXISTS queen.kv_usage (
    tenant_id   UUID PRIMARY KEY,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE queen.kv_usage ADD COLUMN IF NOT EXISTS kv_rows      BIGINT NOT NULL DEFAULT 0;
ALTER TABLE queen.kv_usage ADD COLUMN IF NOT EXISTS kv_bytes     BIGINT NOT NULL DEFAULT 0;
ALTER TABLE queen.kv_usage ADD COLUMN IF NOT EXISTS timer_rows   BIGINT NOT NULL DEFAULT 0;
ALTER TABLE queen.kv_usage ADD COLUMN IF NOT EXISTS timer_bytes  BIGINT NOT NULL DEFAULT 0;
ALTER TABLE queen.kv_usage ADD COLUMN IF NOT EXISTS timer_oldest TIMESTAMPTZ;

-- cost_delay 0 SOLO su queen.kv e queen.log_timers, mai sulle tabelle di
-- configurazione e misura: autovacuum_max_workers e' un budget GLOBALE (default
-- 3), e sette tabelle con scale_factor 0, threshold 500 e I/O non throttlato si
-- contenderebbero i worker con log_partitions, log_consumers e log_segments, da
-- cui dipende il motore. Queste due sono minuscole e non hanno nessun bisogno di
-- essere aggressive.
ALTER TABLE queen.kv_quota SET (autovacuum_vacuum_scale_factor = 0,
                                autovacuum_vacuum_threshold = 500,
                                vacuum_truncate = off);
ALTER TABLE queen.kv_usage SET (autovacuum_vacuum_scale_factor = 0,
                                autovacuum_vacuum_threshold = 500,
                                vacuum_truncate = off);

-- Precedente e caveat nello stesso posto: 010_log_admin.sql concede
-- queen.hotlist_repairs allo stesso modo, motivandolo con "a non-owner caller
-- needs the table rights too". Ogni SP qui e' SECURITY INVOKER (non esiste un
-- solo SECURITY DEFINER in tutto lo schema), quindi il GRANT serve.
-- CONSEGUENZA, scritta qui perche' non venga trovata in un audit: l'isolamento
-- per tenant su questa tabella e' ESATTAMENTE una clausola WHERE dentro le SP.
-- Non c'e' RLS. Qualunque lettore SQL diretto lo scavalca, e NESSUNA rotta HTTP
-- puo' mai costruire una query contro queen.kv fuori da queste SP. C'e' un test
-- che lo verifica meccanicamente (§15).
--
-- Sulla sequenza si concede USAGE e NON SELECT: con SELECT chiunque potrebbe
-- leggere last_value e trasformare il canale laterale di §13.3 in un contatore
-- globale di scritture.
GRANT SELECT, INSERT, UPDATE, DELETE ON queen.kv TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON queen.kv_quota TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON queen.kv_usage TO PUBLIC;
GRANT USAGE ON SEQUENCE queen.kv_version_seq TO PUBLIC;
```

### 3.3 `queen.log_timers`

```sql
CREATE TABLE IF NOT EXISTS queen.log_timers (
    tenant_id     UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    -- COLLATE "C" per la stessa ragione di queen.kv, e la prima delle due e'
    -- l'ordine di lock, non la sargabilita'.
    queue         TEXT COLLATE "C" NOT NULL,
    timer_key     TEXT COLLATE "C" NOT NULL,
    partition     TEXT NOT NULL DEFAULT 'Default',
    deliver_at    TIMESTAMPTZ NOT NULL,
    -- transactionId FISSO del frame futuro. E' la rete SECONDARIA contro il
    -- doppio invio; la garanzia PRIMARIA e' che delete e push condividono una
    -- transazione. La correttezza non dipende MAI dalla finestra di dedup.
    txn           TEXT NOT NULL,
    -- Id stabile, coniato dal broker (util::uuidv7_bytes) allo SCHEDULE, cosi'
    -- l'API di schedule puo' rispondere "questo e' l'id che vedrai" e un
    -- ri-fuoco dopo un tentativo rollbackato consegna lo stesso id.
    -- NON e' un campo di input: vedi l'header di log_timers_apply_v1.
    message_id    UUID NOT NULL,
    payload       BYTEA NOT NULL,
    payload_zstd  BOOLEAN NOT NULL DEFAULT FALSE,
    -- La cifratura avviene allo SCHEDULE, come sul push, quindi il payload non
    -- sta in chiaro qui. CONSEGUENZA da dichiarare: una coda la cui cifratura
    -- viene abilitata DOPO che un timer e' stato schedulato consegna quel frame
    -- in chiaro. Il flag viaggia col frame.
    encrypted     BOOLEAN NOT NULL DEFAULT FALSE,
    -- Il sub AUTENTICATO di chi ha schedulato. NON e' un campo dell'op JSON:
    -- arriva a log_timers_apply_v1 come ARGOMENTO SEPARATO, dal middleware
    -- (server/src/auth.rs:31-36: "it is the ONLY source of that value, a
    -- client-supplied producerSub is never honored"). Un producerSub dentro
    -- un'op e' un RAISE 22023, mai un campo ignorato in silenzio.
    producer_sub  TEXT,
    -- SOLO fallimenti permanenti. Un errore transiente NON consuma budget, o
    -- cinque minuti di indisponibilita' del database manderebbero in DLQ ogni
    -- timer del sistema, cioe' un guasto di infrastruttura diventerebbe perdita
    -- di prodotto.
    attempts      INT NOT NULL DEFAULT 0,
    last_error    TEXT,
    -- Lease del fuoco. "claim_token IS NOT NULL AND claimed_until > now()" e'
    -- l'UNICA definizione di "in mano a qualcuno". Una riga che sta solo
    -- facendo BACKOFF dopo un fallimento ha claimed_until nel futuro e
    -- claim_token NULL, e resta cancellabile. Due colonne perche' collassarle
    -- renderebbe un timer velenoso incancellabile per tutto il backoff, cioe'
    -- l'utente non potrebbe rimuovere la cosa rotta.
    claimed_until TIMESTAMPTZ,
    claim_token   UUID,
    shard         SMALLINT NOT NULL
                  GENERATED ALWAYS AS ((hashtextextended(timer_key, 0) & 63)::smallint) STORED,
    -- L'unica chiave di scansione: "non visibile allo sweeper prima di questo
    -- istante". GENERATED e INDICIZZATA apposta, e il prezzo e' dichiarato:
    -- l'UPDATE della claim tocca una colonna indicizzata, quindi non e' mai HOT.
    -- E' comprato deliberatamente. Con un indice (shard, deliver_at) la claim
    -- resterebbe HOT, ma ogni timer velenoso in backoff starebbe in TESTA
    -- all'indice e verrebbe riletto da ogni passata di ogni broker per tutto il
    -- backoff: il costo crescerebbe col numero di timer ROTTI, che e' l'unica
    -- direzione che non deve degradare. I tassi dei timer sono tassi di control
    -- plane, ordini di grandezza sotto i tassi dei messaggi.
    -- CASE e non GREATEST+COALESCE con un literal '-infinity', cosi' l'espressione
    -- e' inequivocabilmente immutabile e il check della colonna generata non la
    -- rifiuta.
    visible_at    TIMESTAMPTZ NOT NULL
                  GENERATED ALWAYS AS (
                      CASE WHEN claimed_until IS NULL OR claimed_until < deliver_at
                           THEN deliver_at ELSE claimed_until END) STORED,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, queue, timer_key)
);

-- L'UNICO indice secondario, con un solo lettore: lo sweeper. shard in testa
-- perche' la sonda di scadenza e il calcolo della sveglia sono entrambi un
-- LATERAL di 64 seek a una riga; visible_at come seconda perche' la claim e' un
-- range scan in ordine di scadenza per shard con ZERO righe filtrate, che e' il
-- senso di avere visible_at generata invece di un predicato a runtime.
-- Deliberatamente NON creati: un indice su deliver_at (nessun lettore, lo
-- sweeper ordina per visible_at), uno su (tenant_id, queue) (le colonne di testa
-- della PK servono gia' peek e list), uno su claim_token (il fuoco indirizza per PK).
CREATE INDEX IF NOT EXISTS idx_log_timers_visible
    ON queen.log_timers (shard, visible_at);

ALTER TABLE queen.log_timers SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 500,
    autovacuum_vacuum_cost_delay = 0,
    vacuum_truncate = off);
ALTER TABLE queen.log_timers SET (fillfactor = 70);   -- per analogia, NON misurato

-- NESSUN "ALTER COLUMN payload SET STORAGE": vedi il commento gemello in 024.
-- Prenderebbe un ACCESS EXCLUSIVE a ogni boot su una tabella che lo step 0b del
-- wire scrive, cioe' fermerebbe ogni bundle con timer PRIMA del pre-lock delle
-- partizioni, trascinandosi dietro i suoi push e i suoi ack. E EXTENDED e' gia'
-- il default per bytea.

GRANT SELECT, INSERT, UPDATE, DELETE ON queen.log_timers TO PUBLIC;
```

`queen.log_timers` **legge** `queen.kv_quota` e scrive in `queen.kv_usage`, che sono create da 024. Non ne crea nessuna: una sola tabella di quota per tenant per tutto lo stato.

### 3.4 Invarianti e budget di indici

| invariante | come e' garantita | come si rompe |
|---|---|---|
| zero indici secondari resta vero per `queen.log_segments` | non tocchiamo 001 | qualcuno "estende" la regola alle tabelle di catalogo e conclude che sono senza indici |
| esattamente **un** indice secondario per tabella nuova, con **un** lettore | scritto sopra, con l'elenco di quelli deliberatamente non creati | un indice su `updated_at` renderebbe non-HOT ogni update di contatore |
| nessuna partizione creata per richiesta, chiave o timer | nessuna delle due tabelle nomina `partition_id` | vedi C5 |
| l'isolamento e' una clausola WHERE dentro le SP | `GRANT` a PUBLIC, nessuna RLS, e il test grep di §15 | una rotta HTTP che costruisce SQL contro `queen.kv` |
| un conflitto non puo' mai essere cross-tenant | il target di `ON CONFLICT` e' la PK **completa** | un indice unico su `(namespace, key)` "per efficienza" |
| il modulo shard e' 64 per sempre | colonna GENERATED, nessuna env | una env che sembri cambiarlo ri-sharderebbe in silenzio |

Le validazioni di forma sono `RAISE`, non `CHECK`: lo stile della casa ha un solo `CHECK` in tutto lo schema, e uno schema always-virgin non ha una migrazione per cambiare un vincolo di tabella.

---

## 4. Semantica dei timer (contratto dichiarato)

### 4.1 Le operazioni

| operazione | comportamento | esito quando non si applica |
|---|---|---|
| `schedule` | upsert su `(tenant, queue, timer_key)` | `too_late` se la riga e' gia' claimed |
| `reschedule` | **la stessa identica upsert**, quindi il retry dopo un crash del client e' sicuro per costruzione. `attempts` torna a 0 e `last_error` a NULL: un timer riprogrammato e' un timer nuovo sotto un nome vecchio, e un payload appena corretto non deve ereditare il budget consumato da quello che stava avvelenando | `too_late` se claimed |
| `cancel` | `DELETE` | `absent` se non esiste (idempotente, `ok:true`), `too_late` se claimed |
| `cancel` su timer in **backoff** | **riesce**: durante il backoff `claim_token` e' NULL apposta | non applicabile |
| `peek` | una chiave, con payload | `found:false` |
| `list` | keyset sulla PK, **coda obbligatoria** | nessun elenco per tenant: sarebbe una scansione che l'utente finale del cliente potrebbe innescare |
| `fire` | delete e push in una transazione | `stale` (token non corrispondente), `duplicate` (il txn e' gia' nel log). **`duplicate` e' IRRAGGIUNGIBILE in v1**: con `p_verified = v_last` la sonda di dedup non gira e il ramo non puo' essere restituito a questo chiamante (§6.2, decisione aperta §20.7) |

Tassonomia chiusa di `status`: `scheduled | rescheduled | cancelled | absent | too_late`. Chiusa perche' un client che deve distinguere scrive uno `switch`, non legge una frase.

### 4.2 Le semantiche dichiarate una volta sola

- **`deliverAt` e' "non prima di", mai "esattamente a".** Il pavimento misurato su questo stack e' single hop p50 circa 10 ms con fsync circa 4 ms, piu' un ciclo di sweep. Un timer sano sta sotto la decina di millisecondi sopra `QUEEN_SWEEPER_MIN_SLEEP_MS`; sopra `QUEEN_SWEEPER_MAX_SLEEP_MS` (1 s) c'e' un problema di sveglia, non di carico. Va scritto in documentazione con questi numeri, e `queen_timers_delivery_lateness_milliseconds` e' la metrica che li misura.
- **L'ordine di un timer nella sua chiave si decide al FUOCO, non allo schedule.** Due timer sulla stessa `(queue, partition)` che maturano nello stesso batch entrano nel log nell'ordine in cui la claim li ha restituiti, cioe' `ORDER BY visible_at`, che e' l'ordine di scadenza e non quello di schedulazione.
- **Un `deliverAt` nel passato e' legale** e fa fuoco al primo ciclo.
- **Solo durate relative sul wire** (`delayMs`), mai istanti assoluti. Il `deliver_at` si calcola in Postgres come `v_now + make_interval(...)`. Un solo orologio, e nessuna skew fra broker entra da nessuna parte: `visible_at` e' generata, il confronto e' server-side, il ritardo torna al broker gia' calcolato come `r_late_ms`, il sonno come `nextInMs` relativo. L'unico orologio del broker che partecipa e' quello di `tokio::time::sleep`, e sbagliarlo costa latenza, non correttezza.
- **`producerSub`, `messageId` e `tenant` non sono campi di input.** Presenti in un'op, sono un `RAISE 22023`. Ignorarli in silenzio li renderebbe un vettore: un tenant che posta `{"producerSub":"billing-service"}` otterrebbe, un secondo dopo, un frame nel log con provenienza attestata dal broker e falsificata dal client, e `producer_sub` e' l'unico campo non ripudiabile di un frame. Il rifiuto lo fa scoprire al primo test invece che in un audit.

### 4.3 Perche' `too_late` invece di lasciar vincere il chiamante

Una cancel o un reschedule che arrivano su un timer gia' claimed rispondono `too_late` con **HTTP 200**, non falliscono in silenzio e non falliscono rumorosamente: e' un verdetto. Il broker che detiene il claim ha gia' decompresso e impacchettato quel payload ed e' sul punto di committarlo; concedere la cancel renderebbe "e' partito?" senza risposta, e concedere il reschedule farebbe consegnare il vecchio payload dopo che il client crede di averlo sostituito. La finestra e' limitata dal lease, al massimo `QUEEN_SWEEPER_LEASE_MS` (30 s), ed e' anche il tempo massimo per cui una cancel puo' rispondere `too_late` dopo la morte di un broker. Il rimedio per il client e' una chiave nuova, oppure aspettare la consegna e agire sul messaggio.

### 4.4 Il contratto su `absent`, che e' il punto dove un utente si fa male

Avendo scelto DELETE e non "mark done" (§1.4), **non esiste una lapide**. Passata la finestra del lease, un timer consegnato non ha piu' una riga, e una cancel risponde `absent`. In grassetto, in documentazione e negli header:

> **`absent` significa "non e' piu' pendente". Puo' essere gia' stato consegnato. L'autorita' e' il log: cerca il `txn` del timer nella coda di destinazione.**

Due conseguenze operative, entrambe obbligatorie:
1. La risposta di `cancel` restituisce comunque il `txn` atteso, che il client conosce, cosi' la verifica e' possibile senza una seconda API.
2. Ogni esempio che cancella un timer di compensazione nel bundle di chiusura di una saga **deve mostrare il consumatore della coda di compensazione che verifica lo stato KV della saga prima di compensare**. Senza, il caso "il timer e' partito 5 ms prima della cancel" produce lo scioglimento di una prenotazione gia' spedita, e la cancel ha risposto `absent` con `ok:true`. E' la forma corretta, e' l'unica, e nessuno la scrivera' da solo: va negli esempi eseguiti, non in una nota.

Nota simmetrica: una cancel su un timer di un **altro tenant** risponde anch'essa `absent`. Non rivelare e' giusto; dire `ok:true` no. **`absent` porta `ok:false`**, con la stessa lezione gia' pagata in casa sul delete di una coda, dove `deleted:false` con 200 leggeva come successo per ogni client che si fidava del campo.

### 4.5 Il timer velenoso

`attempts` cresce **solo** sui fallimenti permanenti e di configurazione. Tassonomia per SQLSTATE, con un solo classificatore in tutto il broker (§7.6): `40001` e `40P01` piu' le classi `08`, `53`, `57`, `58` e l'assenza di SQLSTATE sono transienti (backoff corto fisso, budget non consumato); la classe `42` e' configurazione (budget consumato, WARN **non** campionato perche' e' azionabile da un operatore, e nomina coda e timer key); tutto il resto e' permanente (budget consumato, WARN campionato). Backoff esponenziale `min(backoff_min * 2^attempts, backoff_max)` calcolato dal broker. Superato `QUEEN_SWEEPER_MAX_ATTEMPTS` (default 5), il timer va nella DLQ della coda di destinazione (§6.2, `log_timers_dlq_v1`), con `consumer_group = '__timer__'` e `offset = -1`.

Il replay di una riga DLQ con `consumer_group LIKE '\_\_%\_\_'` **e' rifiutato esplicitamente**, con un errore che nomina il motivo: quel frame non ha mai avuto un gruppo, il suo offset non e' una posizione, e ripubblicarlo in un gruppo fantasma non ha una semantica definita.

---

## 5. Semantica del KV (contratto dichiarato)

Sette nomi, cinque code path: `get`, `getMany`, `getPrefix`, `put`, `putIfAbsent` (alias), `delete`, `incr`.

### 5.1 La regola trasversale: la scadenza e' obbligatoria

Ogni `put`, `putIfAbsent` e `incr` porta **esattamente una** fra `ttlSeconds` (intero maggiore di zero) e `forever: true`. Zero o due dichiarazioni sono `kv_expiry_not_specified` (SQLSTATE 22023, HTTP 400). La regola vive in `kv_apply_v1`, quindi tutti e sette i client la ereditano senza scrivere una riga, compresi quelli che nessuno riprogettera' e il broker embedded, che non passa dagli handler HTTP. **Non esiste un campo `expiresAt` in ingresso**: la forma comoda `until: <data>` resta negli SDK e viene convertita in delta al momento dell'invio.

Un `put` senza `ttlSeconds` **non eredita la scadenza precedente**: non e' esprimibile. Un put che ereditasse in silenzio il TTL e' il modo piu' rapido per rendere immortale un marcatore.

### 5.2 La regola che decide tutto il resto

> Il read-modify-write fra due chiamate e' sicuro **solo** quando la chiave KV deriva dalla chiave di partizione. Allora le corsie serializzano e la chiave non ha altri scrittori dentro quel consumer group (due gruppi diversi sulla stessa partizione corrono comunque). Quando non deriva, servono le atomiche.

La difesa e' meccanica, non documentale: l'handle di stato degli SDK conia da solo la chiave come `@p/<queue>/<partition>/<group>/<nome>`, quindi la derivazione e' imposta dall'API. E la frase da mettere in documentazione: *`expect` e' il modo di rendere falsificabile la tua ipotesi di serializzazione invece che silenziosa. Se credi che la corsia ti serializzi, mettilo lo stesso: se non fallisce mai non e' costato niente, e il giorno in cui fallisce hai appena scoperto che due consumer stanno servendo la stessa partizione, e l'hai scoperto con un verdetto invece che con un totale sbagliato.*

Gerarchia, da dichiarare in questo ordine: **la transazione di ack e' il recinto primario, `expect` e' l'asserzione secondaria.** Se la scrittura di stato condivide la transazione con l'ack, una lease scaduta fa sollevare l'ack e annulla la scrittura, cosa che la CAS **non** puo' fare, perche' un `expect` su una versione ancora corrispondente riesce anche da uno zombie.

### 5.3 `put`, `putIfAbsent` e il bug piu' grave riparato

| `expect` | statement | semantica |
|---|---|---|
| assente | `INSERT ... ON CONFLICT DO UPDATE` incondizionata | upsert, sostituisce valore **e** scadenza |
| `0` | `INSERT ... ON CONFLICT DO UPDATE ... WHERE NOT queen.kv_live_v1(k.expires_at, p_now)` | "non deve esistere", e vince anche contro una riga scaduta non ancora potata (resurrezione) |
| `N > 0` | **UPDATE puro**, mai il ramo INSERT | lock ottimistico |

La riga `N > 0` e' la riparazione decisiva: nella forma ingenua, `expect:N>0` su chiave assente cade nel ramo INSERT (il ramo di conflitto non viene mai valutato) e **crea** la riga, cioe' in una saga fa partire il comando di compensazione che `expect` doveva impedire. Un `expect` che vale zero righe **non deve mai creare niente**.

Perche' `expect:0` non e' una race: in `ON CONFLICT DO UPDATE` Postgres prende il **row lock prima** di valutare la `WHERE`. Due `putIfAbsent` concorrenti si serializzano, il secondo rivaluta contro la riga nuova e non applica. Esattamente uno vince. Va scritto nell'header, insieme al costo: anche una condizionale **fallita** tiene quel row lock fino al commit, ed e' cio' che alimenta il rischio accettato §18.2.

`putIfAbsent` e' un **alias** che si desugara in `put` con `expect:0` all'ingresso di `kv_apply_v1`, un solo code path. Esiste sotto il proprio nome perche' e' il nome della cosa e perche' `applied` che risponde "ho vinto io?" e' la domanda piu' frequente rivolta a questa API. Se il chiamante passa `putIfAbsent` **e** un `expect` diverso da 0, `RAISE 22023`.

**`expect` esplicitamente `undefined` o null e' un errore client-side, non un downgrade silenzioso a upsert.** Se l'utente ha scritto la parola `expect`, ha dichiarato l'intenzione di fencare, e un valore assente e' un bug del suo codice.

Ritorno uniforme di ogni scrittura, **con valore e versione correnti anche quando non si applica**, perche' chi perde non deve fare un secondo round trip: e' l'intero punto del marcatore di idempotenza. Tassonomia chiusa di `reason`: `exists`, `absent`, `version`, `limit`, `type`.

Nota di correttezza sulla versione restituita a chi perde: per `expect:N>0` e per `delete` con `expect`, le righe che non soddisfano il predicato **non vengono bloccate**, quindi una SELECT discriminante separata girerebbe su uno snapshot successivo e potrebbe restituire una versione gia' vecchia, producendo un ciclo CAS che non converge su una chiave contesa (e non c'e' backoff lato server). La discriminazione si fa **nello stesso statement**, con una CTE `upd` e un `LEFT JOIN` sulla riga corrente. E in documentazione: **la `version` restituita a chi perde e' advisory, mai un token di fencing da riusare alla cieca.**

### 5.4 `incr`, e le tre riparazioni obbligatorie

`incr` non ha `expect`: e' l'uscita di sicurezza **dalla** CAS, e metterci una precondizione reintrodurrebbe il ciclo che `incr` esiste per eliminare. Il valore e' `numeric`, quindi nessun overflow lato server; gli SDK tipizzati espongono int64 e falliscono esplicitamente se il valore non ci sta (in JavaScript va aggiunto un `Number.isSafeInteger` sul ritorno che solleva invece di consegnare un numero sbagliato, perche' oltre 2^53 `JSON.parse` perde precisione **in silenzio**).

**Riparazione 1, il valore risultante non e' una variabile plpgsql.** La forma ingenua scrive `AND (p_max IS NULL OR v_next <= p_max)` dove `v_next` e' il valore risultante. `v_next` dipende dalla riga in conflitto, quindi e' conoscibile **solo dentro** lo statement, e come variabile resta `NULL`. Il guasto e' silenzioso e fail-closed: `NULL <= p_max` e' `NULL`, la `WHERE` non e' vera, zero righe aggiornate, `applied:false, reason:'limit'`. Un rate limiter ammette la prima richiesta (ramo INSERT) e **rifiuta tutte le successive per sempre**, con un `reason` che dice "quota superata" mentre il contatore e' a 1, e siccome `applied` **e'** la decisione di ammissione, il cliente e' bloccato al 100% con il messaggio sbagliato. **Correzione:** un helper `queen.kv_num_v1(value, expires_at, now)` che restituisce il numerico effettivo (zero se scaduta), e la `WHERE` scrive `queen.kv_num_v1(k.value, k.expires_at, p_now) + p_delta <= p_max`, inline, tre volte.

**Riparazione 2, il ramo INSERT non applica nessuna guardia.** Il primo `incr` di una finestra passa da `VALUES (..., to_jsonb(p_delta), ...)`, che non ha `WHERE`. Con `max = 5` e `delta = 10` la prima chiamata restituisce `applied:true` e il contatore vale 10: la quota e' sfondata al primo colpo, e la scena si ripete **a ogni rotazione di finestra**, cioe' esattamente nel momento in cui un limiter viene attaccato. **Correzione:** `p_delta` si valida contro `p_min` e `p_max` nella passata di validazione, che e' un confronto puro e non ha bisogno della riga; un `delta` fuori dai limiti e' `applied:false, reason:'limit'` prima di qualunque scrittura.

**Riparazione 3, la guardia di tipo contraddice la regola di scadenza.** `WHERE jsonb_typeof(k.value) = 'number'` viene valutata sulla riga **vecchia**, incondizionatamente, mentre il `SET` tratta gia' la riga scaduta come zero. Guasto: qualcuno fa `put('quota', 'acme:...', {count: 0})` per "inizializzare", la chiave scade, e prima che lo sweeper la poti ogni `incr` restituisce `reason:'type'`, cioe' **tutte le richieste del cliente vengono rifiutate** fino alla potatura, con un motivo che nessun client gestisce come "riprova". Contraddice direttamente la regola di §5.7. **Correzione:** `WHERE NOT queen.kv_live_v1(k.expires_at, p_now) OR jsonb_typeof(k.value) = 'number'`.

**Il TTL di `incr` e' CREATE-ONLY.** Una riga viva conserva la sua scadenza: se `incr` la prorogasse, un limitatore a finestra fissa su un cliente sempre attivo non chiuderebbe mai la finestra, cioe' smetterebbe di limitare esattamente sotto carico. Tiene anche l'UPDATE HOT, perche' `expires_at` e' l'unica colonna indicizzata. Una riga scaduta conta come zero e **riparte con una finestra nuova**, ed e' cio' che rende il limitatore una chiamata sola.

**`max` e `min` non saturano e non troncano.** Se lo sforo avverrebbe, non si applica e si torna `applied:false, reason:'limit'` con il valore corrente. Senza, il limitatore confronta lato client **dopo** aver incrementato, quindi la richiesta che sfonda il tetto ha gia' consumato budget e non e' annullabile. Con `max`, **`applied` E' la decisione di ammissione**.

### 5.5 Letture, e il confine sul costo

| op | campi | ritorno |
|---|---|---|
| `get` | `ns`, `key` | `{found, key, value, version, expiresAt, updatedAt}` |
| `getMany` | `ns`, `keys[]` | `{rows, missing}` |
| `getPrefix` | `ns`, `prefix`, `after?`, `limit?`, `keysOnly?` | `{rows, truncated, nextAfter}` |

`missing` esplicito su `getMany`: l'assenza deve essere un **dato**, non un buco che il client calcola per differenza. `found` e' separato dal valore perche' `'null'::jsonb` e' un valore legale, e `{found:true, value:null}` e `{found:false}` sono cose diverse che nessun SDK deve collassare. Le letture multiple restituiscono **righe, mai una mappa chiave-valore**: la forma del ritorno rende inesprimibile la confusione, senza bisogno di un divieto.

Il predicato di prefisso e' **sempre** questa coppia, mai un `LIKE`:

```sql
AND k.key >= p_prefix
AND (v_end IS NULL OR k.key < v_end)     -- solo driver dell'indice
AND starts_with(k.key, p_prefix)         -- la semantica, esatta
```

`starts_with()` non conosce metacaratteri: elimina alla radice il buco che `009_streams_state_get_v1.sql` ha oggi (`LIKE prefix || '%'` senza escaping) e toglie `%` e `_` dal modello mentale dell'utente invece di chiedergli di ricordarli. Il range e' solo il driver dell'indice; `starts_with` e' la correttezza, e resta vero anche quando il bound e' `NULL`.

**Dove `getPrefix` puo' vivere, deciso:**
- **Vietata nel transaction wire** (`RAISE 22023`). Lavoro di lettura non limitato a priori dal chiamante, dentro la transazione che tiene lo spazio di lock piu' esterno e, a valle, quelli di partizione. `get` e `getMany` sono ammesse perche' il chiamante ne fissa il costo. Il confine e' sul **costo**, non sul tipo di operazione.
- **Vietata nel rider del pop**, che in v1 non esiste affatto (§18.1) e che quando esistera' accettera' solo la forma a chiavi esplicite, per lo stesso criterio.
- **Ammessa solo su `POST /api/v1/kv`**, mai come query string. Il motivo e' di privacy operativa e non e' negoziabile con una regola di logging: `?prefix=quota:acme:` passa dai log d'accesso del broker, da quelli del proxy, dal campione del meter, dal tracing per request id e da qualunque ingress davanti. Una mitigazione che vive in un solo componente su quattro non e' una mitigazione. La POST e' gia' dichiarata la superficie completa; le rotte a path sono zucchero per i tre casi che si scrivono a mano.

Tetti: `limit` di default 100, **clampato** a `QUEEN_KV_PREFIX_LIMIT` (1000) e mai rifiutato, con `truncated` che dice la verita' (un 400 su un limit troppo alto e' un errore che l'utente non sa risolvere senza leggere la config del server). Piu' un tetto **in byte**, `QUEEN_KV_MAX_READ_BYTES` (4 MiB), applicato all'aggregato dentro la SP: un tetto sul numero di chiavi non e' un tetto sui byte, e 1000 chiavi da 64 KiB sono 64 MB. La risorsa vera e' il byte. `after` e' un cursore keyset esclusivo, non un offset, stabile sotto `COLLATE "C"` e allineato all'ordine dell'indice, quindi senza nodo di Sort. Costruzione della pagina con `LIMIT p_limit + 1` nella **subquery**, cosi' la riga in piu' decide `truncated` senza una seconda query e il tetto e' reale.

`getPrefix` senza prefisso, o con prefisso vuoto: `400 kv_prefix_required`. E' il confine dichiarato. Un namespace non e' una tabella da enumerare.

**Ogni pagina di `getPrefix` e' il suo snapshot in READ COMMITTED. Non e' un'istantanea del namespace**: con `after` puo' non vedere una chiave inserita dietro il cursore. Va bene per compattare stato, non per un conteggio esatto.

### 5.6 Cosa NON esiste, e resta additivo

`merge` (un read-modify-write su tutto il datum dentro Postgres, amplificatore di WAL nello stesso WAL del log), `deletePrefix` (esisteva per potare le saghe chiuse, e con il TTL obbligatorio quel lavoro lo fa lo sweeper: la sua unica giustificazione e' sparita), `expire` (prorogare senza riscrivere e' coperto da un `put` con il valore che il detentore gia' possiede, perche' chi rinnova un lock e' il proprietario del valore), CAS sul valore, predicati, query per valore, indici secondari, elenco senza prefisso, tipi contenitore, watch e subscribe, registro dei namespace, cronologia versioni, transazioni multi-chiamata lato client. Tutte aggiunte additive, quindi rimandabili senza rompere niente.

### 5.7 La scadenza in una regola

> **Una chiave scaduta non viene MAI restituita e non conta MAI come esistente, anche se lo sweeper non l'ha ancora potata.**

Lo sweeper cancella in ritardo per costruzione: la verita' e' nel predicato, non nella presenza fisica della riga. Il precedente in casa sullo stesso tema e' in `006_log_maintenance.sql`, dove una riga sopravvissuta alla finestra risolve come sconosciuta e il lettore tollera il ritardo del purger. Il predicato non e' mai copiato a mano: e' `queen.kv_live_v1(expires_at, p_now)` nei cinque punti in cui compare (letture, `put expect:0` per negazione, `put expect:N` attraverso `kv_ver_v1`, `delete` con `expect`, `incr`).

Corollari da dichiarare in documentazione, perche' sono i punti dove un utente si fa male:
- **Un solo istante per chiamata.** `p_now` e' un parametro, derivato da `now()` (transaction timestamp) al confine. Una `getMany` non puo' vedere una chiave viva e la successiva morta per pochi microsecondi. Stessa disciplina del "un ciclo, un `now()`" di `006_log_maintenance.sql` e `retention.rs`.
- Lo sweeper cancella con `expires_at <= cutoff`, i lettori accettano con `expires_at > now()`: il confine coincide, una riga esattamente a `now()` e' morta per entrambi.
- **Un lock che scade non viene revocato.** Il vecchio detentore continua a lavorare, semplicemente non ha piu' la riga. Nessun sistema con TTL puo' fare altrimenti. La difesa e' il fencing: il detentore porta la sua `version` come `expect` in ogni scrittura successiva, cosi' le scritture di un detentore scaduto falliscono con `reason:"version"` invece di sovrascrivere quelle del nuovo. Limita il danno, non lo elimina, e va venduto per quello che e': **`putIfAbsent` piu' TTL non e' un lock distribuito.**
- `put expect:0` che resuscita una lineage scaduta azzera anche `created_at`. E' corretto come intento (e' una lineage nuova) e va detto accanto alla colonna, perche' `created_at` e' l'unico campo del datum che non torna mai al chiamante, quindi l'unico su cui un errore non si nota.

---

## 6. Superficie SQL

Convenzioni rispettate: suffisso `_v1`, parametri `p_`, locali `v_`, ritorno letto dal broker come `(...)::text`, `GRANT EXECUTE` in coda con la lista tipi **completa** (una lista sbagliata fa fallire l'apply, e l'apply fallito abbatte il processo). Ogni SP porta in testa il blocco di ordine di lock di §2.

### 6.1 KV: `024_kv.sql`

**Helper puri**, tutti `IMMUTABLE PARALLEL SAFE` e inlinabili, nessuna tabella toccata, quindi la loro posizione nella sequenza e' libera. Scritti una volta perche' il rischio vero e' che il settimo punto d'uso li riscriva qualcun altro in modo diverso.

| helper | firma | scopo |
|---|---|---|
| `kv_live_v1` | `(expires TIMESTAMPTZ, now TIMESTAMPTZ) -> BOOLEAN` | `expires IS NULL OR expires > now`. `now` e' un PARAMETRO, mai una chiamata: una chiamata alla SP usa un solo istante per ogni op del batch |
| `kv_ver_v1` | `(version BIGINT, expires, now) -> BIGINT` | versione effettiva sotto la regola di scadenza; `version(assente) = 0` e' una funzione totale, ed e' quella definizione che rende `expect:0` uguale a "non deve esistere" senza un flag magico |
| `kv_num_v1` | `(value JSONB, expires, now) -> NUMERIC` | valore numerico effettivo, zero se scaduta. Esiste per §5.4 riparazione 1 |
| `kv_prefix_end_v1` | `(prefix TEXT) -> TEXT` | bound superiore del range di byte. `NULL` = nessun bound calcolabile, il chiamante lascia cadere il bound e si affida a `starts_with`. La guardia sui surrogati D800-DFFF non e' decorazione: `chr()` rifiuta un surrogato in UTF8, quindi un prefisso che finisce a U+D7FF solleverebbe senza |
| `kv_check_names_v1` | `(ns TEXT, key TEXT)` | charset namespace `^[a-z0-9][a-z0-9._-]{0,63}$`, chiave non vuota, massimo `QUEEN_KV_MAX_KEY_BYTES` byte, nessun NUL. `RAISE`, non `CHECK` |

**`queen.kv_apply_v1(p_ops JSONB, p_tenant UUID, p_now TIMESTAMPTZ, p_in_wire BOOLEAN) RETURNS JSONB`**

Quattro argomenti, e `p_in_wire` e' un **flag, non una seconda SP**: le differenze fra le due superfici sono due (`getPrefix` vietata, cap 64 invece di 256), e due SP sarebbero due implementazioni che divergono. Stessa logica che rende `putIfAbsent` un alias e non un secondo code path.

Contratto del corpo, i sei punti che contano:

1. **VALIDATE-THEN-APPLY, quindi NESSUN savepoint.** Ogni rifiuto recuperabile (forma, dimensione, chiave duplicata nel batch, scadenza mancante, `getPrefix` nel wire, `delta` fuori dai limiti) si decide in una passata **prima** della prima scrittura. Non si copia il `BEGIN/EXCEPTION` per elemento del ciclo streams: li' gli elementi sono unita' indipendenti, qui la transazione e' all-or-nothing per definizione e il rollback per elemento non serve a niente. E il costo sarebbe reale: a decine di op per bundle sarebbero decine di subxid, e oltre 64 subxid vivi per transazione top-level la subxid cache va in overflow e **ogni snapshot della macchina** diventa suboverflowed.
2. **Ordine di applicazione crescente `(namespace, key) COLLATE "C"`, NON ordine di input**, con i risultati riportati in ordine di input per ordinale (§2.3, §6.4).
3. **Una chiave al massimo una volta per chiamata**, verificato in validazione. E' cio' che rende **totale** l'ordine intra-spazio, non una comodita': va marcata load-bearing nell'header, perche' il documento dice anche che vietarla e' rilassabile dopo, e rilassarla senza toccare la dimostrazione riaprirebbe il ciclo.
4. **Budget per CHIAMATA, non per operazione.** `QUEEN_KV_MAX_OPS_PER_CALL` conta le op, ma da solo non limita niente: 63 `getMany` da 256 chiavi leggono 16 128 righe, e con valori vicini al tetto sono fino a circa 1 GiB di detoast **prima** che venga preso il primo lock di partizione e **mentre** si tengono i row lock su `queen.kv`. Quindi si applicano tre budget insieme: numero di op, **somma** delle chiavi di tutte le op contro `QUEEN_KV_MAX_KEYS_PER_CALL`, e `QUEEN_KV_MAX_READ_BYTES` sull'aggregato letto, applicato **anche quando `p_in_wire`** e non solo sulle rotte HTTP.
5. **Perdita di precondizione uguale verdetto, non eccezione.** `applied:false` non fa rollback di niente. L'escalation e' **opt-in per elemento** con `"required": true`, e diventa un `RAISE` con `ERRCODE = 'check_violation'` (23514, classe 23 uguale permanente per il classificatore, che e' corretto: ritentare alla cieca una gara persa la perde di nuovo per sempre). Il `DETAIL` porta il JSON (`index`, `op`, `ns`, `key`, `reason`, `version`, **piu' `value` del vincitore**, limitato a 4 KiB), il broker legge `db_error.detail()` e **non fa mai parsing del messaggio**. Il `MESSAGE` resta **opaco**: nomi di namespace e di chiave finiscono nei log condivisi e negli aggregatori di errori, e i nomi stanno solo nel DETAIL.
   Il `value` nel DETAIL non e' un lusso: senza, chi perde deve fare un `kv.get` in piu' proprio sul percorso piu' frequente del prodotto, e l'intero punto di `putIfAbsent` e' che il perdente sappia gia' cosa ha fatto il vincitore.
6. **`p_tenant` e' un argomento, mai un campo delle op.** Un campo `tenant`, `tenantId` o `_tenant` dentro un'op e' un `RAISE 22023`.

### 6.2 Timer: `025_log_timers.sql`

**`queen.log_timers_apply_v1(p_ops JSONB, p_tenant UUID, p_producer_sub TEXT, p_now TIMESTAMPTZ) RETURNS JSONB`**

Un solo code path per schedule, reschedule e cancel. `p_producer_sub` e' un **argomento separato**, mai letto dalle op: e' la stessa disciplina che `auth.rs:31-36` dichiara per il push (unica sorgente, un valore fornito dal client non e' mai onorato), e senza di essa il timer sarebbe l'unico modo in tutto il prodotto di falsificare la provenienza di un frame. **`messageId` non e' un campo di input**: lo conia l'handler con `util::uuidv7_bytes` e lo passa dentro l'op **dopo** aver riscritto l'array, oppure lo genera la SP. Decidiamo per la prima, perche' l'API di schedule deve poter rispondere "questo e' l'id che vedrai" e l'handler e' l'unico posto che conosce il chiamante.

Schedule e reschedule sono **la stessa upsert**:

```sql
INSERT INTO queen.log_timers AS t (...) VALUES (...)
ON CONFLICT (tenant_id, queue, timer_key) DO UPDATE SET
       partition = EXCLUDED.partition, deliver_at = EXCLUDED.deliver_at,
       txn = EXCLUDED.txn, message_id = EXCLUDED.message_id,
       payload = EXCLUDED.payload, payload_zstd = EXCLUDED.payload_zstd,
       encrypted = EXCLUDED.encrypted, producer_sub = EXCLUDED.producer_sub,
       attempts = 0, last_error = NULL,          -- un timer nuovo sotto un nome vecchio
       claimed_until = NULL, claim_token = NULL,
       updated_at = p_now
WHERE t.claim_token IS NULL OR t.claimed_until <= p_now   -- "e' fuori dalle tue mani"
RETURNING (xmax = 0) AS inserted;
```

Zero righe significa che la riga esiste ed e' claimed, cioe' `too_late`. `xmax = 0` e' il discriminatore standard fra "il ramo di conflitto non ha girato" e il contrario, ed e' l'unica cosa che distingue `scheduled` da `rescheduled` senza una seconda sonda sotto lo stesso lock.

La cancel e' un `DELETE` con la stessa guardia sul claim; zero righe si disambiguano con una sonda `EXISTS` fra `absent` (`ok:false`, ma non e' un errore, §4.4) e `too_late`.

**`queen.log_timers_due_v1(p_shards SMALLINT[], p_now TIMESTAMPTZ, p_cap INT) RETURNS JSONB`**

Ritorna `{"nextInMs", "due", "dueCapped", "lateMs", "now"}`, **tutto relativo all'orologio del server**, cosi' il broker non fa mai aritmetica sui timestamp. `nextInMs` a `NULL` significa tabella vuota. Due statement, entrambi LATERAL per shard, perche' `shard = ANY(p_shards) ORDER BY visible_at` **non** e' index-ordered (un `ScalarArrayOpExpr` sulla chiave di testa non produce output ordinato sulla seconda, e il planner mette un Sort). `due` e' **cappato**: `LIMIT p_cap + 1` con `dueCapped` che dice se ha toccato il tetto, e `p_cap` scende da 10000 a **2000**, perche' questa sonda gira a ogni ciclo e un conteggio esatto sarebbe `O(dovuti)` proprio nel guasto che la sonda esiste per rilevare. `lateMs` e' un'approssimazione a 64 seek e va detto: sottostima se una riga con `deliver_at` antico ha `visible_at` spinto avanti da un backoff, che e' precisamente il caso in cui il ritardo e' voluto.

**`queen.log_timers_claim_v1(p_shards, p_now, p_lease_ms, p_max_rows, p_per_tenant) RETURNS TABLE(...)`**

`DROP FUNCTION IF EXISTS` scritto dal giorno uno, perche' il tipo di ritorno fa parte della firma e `CREATE OR REPLACE` rifiuta di cambiarlo: la meta' di idempotenza al boot della convenzione DROP+CREATE. Ritorna colonne `TEXT` per tenant e uuid (il broker non ha il crate uuid), piu' `r_late_ms` calcolato dal server.

Corpo, con due proprieta' che non erano nel design e sono obbligatorie:

- **Equita' fra tenant.** La forma ingenua e' `WHERE shard = s AND visible_at <= now ORDER BY visible_at LIMIT v_per`, cioe' rigorosamente il piu' vecchio per primo e **cieca al tenant**. Siccome lo shard e' hash del `timer_key` e non contiene il tenant, i timer di un tenant si spalmano su tutti i 64 shard: un tenant che schedula 200 000 timer tutti dovuti alle 09:00 riempie **ogni** claim batch di **ogni** broker per i minuti che servono a drenarlo, e il singolo promemoria di un altro tenant delle 09:00:01 arriva minuti dopo, con `queen_timers_oldest_late_seconds` in allarme per tutta la cella e nessuna metrica che nomini il colpevole. Un budget per tenant applicato **dopo** la claim non funziona: scartare righe gia' claimate le rimette in testa all'indice (`visible_at` non e' cambiato) e le fa riselezionare al ciclo dopo, cioe' un livelock che brucia l'intero batch a ogni giro. **Correzione decisa:** round robin sui tenant dentro la claim, `LATERAL` su `(SELECT DISTINCT tenant_id ... LIMIT k)` con `LIMIT p_per_tenant` dentro, la stessa forma gia' usata per gli shard. `QUEEN_SWEEPER_FIRE_RATE_PER_TENANT` diventa allora applicabile, perche' agisce sulla selezione e non sullo scarto.
- **Il pavimento per shard.** `v_per := p_max_rows / n_shards` con divisione intera da `3` con i default (200/64), e da `1` se un operatore abbassa il batch. Con `v_per = 1` un solo shard in ritardo non puo' mai recuperare. Si scrive `v_per := GREATEST(ceil(p_max_rows::numeric / n_shards)::int, 8)`, e il comportamento di `LIMIT` dentro un `LATERAL` con `FOR UPDATE SKIP LOCKED` va **scritto nell'header**, perche' la correttezza del drenaggio multi-broker ci si appoggia interamente.

`FOR UPDATE SKIP LOCKED` vive **dentro** il LATERAL: lo sweeper non aspetta MAI su una riga timer, ed e' cio' che lo tiene fuori da ogni ciclo di deadlock.

**`queen.log_timers_fire_v1(...) RETURNS JSONB`, il corpo canonico, uno solo**

E' la funzione piu' delicata delle due feature. Due documenti di design ne davano due corpi contraddittori; qui ce n'e' uno, e va **letto da qualcun altro prima del merge**.

1. **Guardie di allineamento**, nello stile delle guardie di `003_log_push.sql`: `array_length` uguale per tutti gli array segmento, `octet_length(hash[i]) = count[i]*16`, e il numero di chiavi con `seg_of = i` **esattamente** `count[i]`. Senza quest'ultima, una mappa frame-timer sbagliata cancellerebbe il timer sbagliato invece di fallire.
2. **Verifica del claim, con lock.** `FOR UPDATE SKIP LOCKED` su ogni riga nominata, confrontando `claim_token` **e** `claimed_until > v_now`, cosi' la definizione di lease e' una sola in tutto il codice. Un segmento e' **live** se e solo se tutte le sue righe sono state bloccate con token corrispondente: all-or-nothing per segmento, perche' il blob e' gia' impacchettato e un timer perso rende quel blob sbagliato.
3. **Rilascio simmetrico delle righe non usate.** Un segmento `stale` viene buttato dal broker, ma le sue **altre** righe questa transazione le ha bloccate con successo e conservano il claim: `claim_token` valido, `claimed_until` nel futuro, quindi `visible_at` nel futuro, quindi **invisibili alla claim successiva per tutto il lease**. Guasto concreto: 200 timer che maturano insieme sulla stessa `(tenant, queue, partition)`, un solo cancel concorrente su uno di essi, e tutti e 200 spariscono per 30 secondi, `lateMs` sale a scalini e nessuna metrica nomina la causa. **Correzione obbligatoria:** nel ramo `stale`, prima del commit, `UPDATE ... SET claimed_until = v_now, claim_token = NULL` sulle righe che questa transazione ha bloccato. Identico nel ramo `duplicate`. Scritto una volta, usato due.
4. **Provisioning gated-on-missing** dei segmenti live, la forma di `003_log_push.sql:96-126`, con `ORDER BY` sulla chiave unica per non fare convoy su `queen.queues`.
5. **UN pre-lock set-based** sulle partizioni dei segmenti live, ordine crescente `log_partitions.id`, byte per byte la forma di `003_log_push.sql:325-330` e `005_log_ack.sql:973-984`. Obbligatorio: senza, un fuoco multi-segmento prenderebbe i lock in ordine arbitrario e potrebbe deadlockare con un bundle di fusion.
6. **Push per segmento** riusando `queen.log_push_one_v1` con `pid` e `window` gia' risolti, cosi' non li ri-risolve (e' la regressione CPU di nested lookup che il rework seg v2 aveva eliminato).
7. **DELETE dei timer consegnati**, righe **gia' tenute** dal passo 2: nessuna acquisizione nuova nello spazio T, quindi nessuna violazione della regola di §2.2.
8. **Raggruppamento per `(tenant_id, queue, partition)`, mai per `(queue, partition)`.** `log_push_multi_v1` prende gia' un array `p_tenants` per riga. Un raggruppamento senza tenant fonderebbe i timer di due tenant che usano lo stesso nome di coda in un unico segmento: **e' il buco di isolamento piu' grave possibile in questa feature**, e' facilissimo da scrivere e non produce nessun errore. C'e' un test di regressione a due tenant con nomi di coda e `timer_key` identici, ed e' criterio di merge, non raccomandazione (§15).

**`p_verified` del fuoco: `v_last`, non `-1`.** Questa e' una correzione che cambia il costo del percorso caldo. Passare `-1` significa "nessuna cache del broker, sonda tutta la finestra", che e' sempre **corretto** ma non e' economico: con `-1` la sonda di dedup scandisce **tutta la finestra `log_txns` ritenuta** della partizione di destinazione, srotolando gli hash riga per riga, ed e' esattamente il lavoro che il bloom front-dedup ha portato dal 60% al 3,7% di verify nella baseline 1.0.0. Peggio, la sonda avviene **dopo** il `FOR UPDATE` sulla riga di `log_partitions`, quindi il costo non e' isolato nella corsia Maint: e' dentro il serializzatore di push della partizione di destinazione, condiviso con i produttori normali. Scenario: una coda con `dedupWindowSeconds = 3600` e 50k msg/ora, uno sweep che fa fuoco a 20 segmenti verso 20 partizioni, e ogni push scandisce un'ora di `log_txns` tenendo il lock. Il sintomo e' "il push e' peggiorato" e la telemetria del fuoco non lo spiega, perche' il tempo e' contabilizzato al push. **Decisione:** il fuoco passa `p_verified = v_last`, cioe' salta la sonda di finestra. La garanzia vera resta delete piu' push nella stessa transazione, che e' quello che il piano dichiara ovunque. **Il `txn` fisso NON e' una rete secondaria, e non va piu' descritto come tale**: il paragrafo qui sotto lo dimostra con la misura, e la vecchia formulazione ("su una coda con `dedupWindowSeconds = 0` la rete non c'e' comunque") sottostimava il fatto, perche' con `p_verified = v_last` non c'e' su **nessuna** coda.

**Conseguenza di `p_verified = v_last`, emersa in verifica: il ramo `duplicate` del fuoco e' irraggiungibile per costruzione.** La sonda di dedup di `log_push_one_v1` gira solo dentro `IF v_window > 0` (`003_log_push.sql:129`) e solo se `v_last > v_from`, con `v_from := GREATEST(COALESCE(p_verified, -1), v_txns_start - 1)` (`:148-149`). Passando `v_last` la span e' vuota, la sonda **non gira**, e l'allocatore non puo' rispondere `duplicate` a **questo** chiamante. L'altra via che ci arriverebbe, due segmenti live sulla stessa `(tenant, queue, partition)` nella stessa chiamata, e' chiusa a monte da un `RAISE` (punto 8), quindi non ci si arriva nemmeno per errore del packer. Misurato sul rig: un timer il cui `txn` e' gia' nel log viene **appeso una seconda volta**, e `messages_in_segments` va da 1 a 3, cioe' un messaggio pre-esistente piu' due fuochi (branch `kvtimer`: `server/tests/timers_fault_injection.rs:374-395`).

**La conseguenza vera, da scrivere ovunque il `txn` fisso venga chiamato "rete": il `txn` fisso non e' la rete del fuoco. La rete e' `DELETE` piu' push nella stessa transazione.** E' quell'atomicita', e solo quella, a dare l'exactly-once del fuoco; il `txn` fisso resta l'**identita'** del messaggio, cioe' cio' che rende rispondibile "e' stato mandato?" cercandolo nella coda di destinazione (§1.4, §4.4), e resta una rete di dedup solo per chi pusha **senza** `p_verified` prefissato, cioe' per i produttori normali. La differenza fra identita' e rete e' load-bearing: chi le confonde conclude che riprogrammare o ripubblicare lo stesso `txn` sia innocuo, e non lo e'.

Il ramo resta **implementato per intero** e non va tolto dal codice (`025_log_timers.sql:1104-1117`): `duplicate` e' un ritorno legale dell'allocatore condiviso, e un ramo che assumesse in silenzio "queued" cancellerebbe timer che non ha mai consegnato. Ma resta **non esercitabile end to end**, quindi la contabilita' di righe di quel ramo non e' coperta da nessun test, mentre §4.1, §12 e §14.2 lo elencano ancora nella tassonomia dichiarata. Le due uscite hanno costi diversi e la scelta non e' del piano: **§20.7**.

**`queen.log_timers_fail_v1(...)`** sposta `claimed_until` di un backoff e **azzera `claim_token`**, cosi' la riga e' in backoff e non in mano a nessuno, e una cancel durante il backoff riesce. `p_count_attempt` a `false` per i transienti. Ritorna l'elenco degli `exhausted`, che **non** vengono cancellati qui: la DLQ ha bisogno dello snapshot del payload in JSONB e solo il broker sa decomprimere.

**`queen.log_timers_dlq_v1(...)`** scrive direttamente in `queen.log_dlq`. Non riusa `log_dlq_head_v1` perche' quella pretende una lease valida su `(partition, group)` e avanza il cursore di un consumer: un timer velenoso non ha lease, non ha gruppo e non ha nessun cursore da avanzare. Vantaggio non secondario: cosi' il fuoco non tocca mai `queen.log_consumers` e la sua sequenza resta `T → Q → P`, senza l'ultimo spazio. Tre note: risolve o provisiona la partizione **sotto il tenant del timer** (`queen.log_dlq` non ha colonna tenant, lo scoping passa solo dal `partition_id`); il provisioning **non e' opzionale**, perche' `get_dlq_messages_v1` fa INNER JOIN su `log_partitions` e `queen.queues`, quindi una riga DLQ su una partizione inesistente e' archiviata e introvabile, il che e' peggio che non archiviarla; e la DELETE dei timer e' guardata da `attempts >= p_min_attempts AND claim_token IS NULL`, che chiude la corsa con un reschedule atterrato fra `fail_v1` e qui (un reschedule azzera `attempts`, quindi la delete non trova nulla e la riga vive).

Conseguenza dichiarata: una riga DLQ **pinna la sua partizione** contro il partition cleanup. E' voluto, il payload deve restare raggiungibile, e la retention non purga mai la DLQ.

**`queen.log_timers_peek_v1` / `_list_v1` / `_usage_step_v1`.** `peek` restituisce il payload solo per una chiave singola, mai su `list`. `list` e' keyset sulla PK con la coda obbligatoria e limit clampato. `usage_step_v1` e' la fase lenta di §7.5.

### 6.3 L'innesto nel wire, e la byte-identita' quando gli array sono assenti

**La firma non cambia.** `queen.log_transaction_wire_v1(p JSONB)` resta `CREATE OR REPLACE`, nessun `DROP FUNCTION`, e il wrapper Rust non cambia di un byte. Il tenant continua a viaggiare come `_tenant` dentro il payload (`005_log_ack.sql:925`), che e' esattamente il meccanismo che rende gratuita l'aggiunta di due array.

Lo skip vive in `DECLARE` piu' due `IF`, mai dentro una query esistente:

```sql
DECLARE
    v_now      TIMESTAMPTZ := now();   -- un solo istante per chiamata
    v_kv       JSONB := p->'kv';
    v_timers   JSONB := p->'timers';
    v_kv_n     INT := CASE WHEN jsonb_typeof(v_kv) = 'array'
                           THEN jsonb_array_length(v_kv) ELSE 0 END;
    v_timer_n  INT := CASE WHEN jsonb_typeof(v_timers) = 'array'
                           THEN jsonb_array_length(v_timers) ELSE 0 END;
```

`jsonb_typeof` e non `jsonb_array_length(COALESCE(...))` non e' pedanteria: `jsonb_array_length` **solleva** su un tipo non array, quindi `"kv": null`, che e' cio' che ogni serializzatore produce per un campo opzionale assente, farebbe fallire una transazione che oggi funziona.

Gli step 0 e 0b stanno **subito dopo la guardia di tenancy sugli ack** (`005_log_ack.sql:930-941`) e **prima** del provisioning (`:943-971`) e del pre-lock (`:973-984`). Non e' un'ottimizzazione: e' l'unico posto in cui possono stare senza invertire l'ordine dichiarato nel punto piu' caldo del prodotto (§2.5).

Tre proprieta' di costo zero per chi non usa la feature, tutte verificabili:

| voce | costo |
|---|---|
| `p->'kv'`, `p->'timers'` | due lookup binari sull'indice delle chiavi di un oggetto JSONB gia' detoastato dal primo `p->>'_tenant'` |
| due `jsonb_typeof` e due `IF` non presi | operatori scalari e salti |
| `v_now := now()` | lettura di `xactStartTimestamp`, gia' in memoria, nessuna syscall (a differenza di `clock_timestamp()`, che resta quello del push e non va confuso) |
| statement SQL aggiunti | **zero** |
| piani preparati aggiunti | **zero**: plpgsql pianifica pigramente alla prima esecuzione, quindi in un broker che non usa il KV gli statement dentro `kv_apply_v1` non vengono mai pianificati |
| nodi aggiunti a piani esistenti | **zero** |
| lock aggiunti | **zero**: nessun `ACCESS SHARE` su `queen.kv`, la tabella non compare in nessun piano eseguito |

La regola che lo garantisce meccanicamente, da scrivere nell'header: **il lavoro kv e timers vive in statement separati, dietro un `IF`, e non entra mai in uno statement esistente.** La tentazione da rifiutare esplicitamente e' fondere il conteggio nella query di provisioning o fare `FROM jsonb_array_elements(COALESCE(p->'kv','[]'))` in un `UNION` con i push: entrambe aggiungono un Function Scan e un nested loop al piano di uno statement che **si esegue sempre**, anche quando l'array e' vuoto.

Il ritorno si compone condizionalmente, e JSONB normalizza l'ordine delle chiavi, quindi il testo restituito e' byte-identico a oggi quando gli array sono assenti.

**Il caso che rompe, dichiarato.** Un broker nuovo contro un database vecchio con `QUEEN_APPLY_SCHEMA=0`: il corpo `plpgsql` risolve `queen.kv_apply_v1` a runtime, quindi un bundle **senza** kv funziona e uno **con** kv fallisce con `42883 undefined_function`, classe 42, configurazione, permanente, non ritentato. E' il comportamento giusto ed e' l'unico prezzo del modello always-virgin qui.

### 6.4 Il contratto di allineamento degli indici

I risultati sono index-allineati al proprio array di input, costruiti come `JSONB[]` pre-riempito con `array_fill` e scritti per ordinale, con la **guardia di conteggio prima di `to_jsonb`**. Senza quella guardia, un ordinale non riempito diventa un `null` JSON silenzioso in posizione, che e' precisamente la classe di disallineamento che le guardie di `003_log_push.sql:372-375` e il controllo gemello in 005 fanno fallire rumorosamente. La mappa fra questo spazio di indici e quello **piatto** dell'HTTP e' in §8.2, ed e' li' che la guardia va estesa.

---

## 7. Lo sweeper (`server/src/sweeper.rs`)

### 7.1 Forma, registrazione, e quando NON esiste

Modulo nuovo, non dentro `retention.rs`: quel ciclo e' a cadenza fissa e leader-gated (`retention.rs:76`, advisory di sessione `737_001`), e infilarci lo sweeper significherebbe o serializzarlo dietro le sue fasi, o rendere leaderless anche la retention. Stessa forma pero': `spawn`, `run_loop`, `step`, `step_result`, `Sampler` per gli errori.

Registrazione in tre punti, come ogni loop di fondo: la lista `mod` di `main.rs`, la lista gemella di `lib.rs`, e lo spawn accanto a `retention::spawn` e `stats::spawn`. In `embedded/boot.rs` **non si aggiunge una quarta copia a mano**: `sweeper::spawn` restituisce un `JoinHandle<()>` e `boot.rs` fa `tasks.push(...)`. Una riga, zero duplicazione, e i commenti "KEEP IN SYNC" restano onorati.

**Il task si spawna sempre, e questo e' il prezzo diretto della decisione di §0.** La forma precedente era `if !kv_enabled && !timers_enabled { return; }`, e l'argomento reggeva finche' i flag esistevano: un'installazione che non avrebbe mai usato ne' kv ne' timer avrebbe pagato per sempre una `log_timers_due_v1` al secondo per broker (LATERAL a 64 seek piu' una count), una `kv_expire_step_v1` al secondo, uno slot `Lane::Maint` e una connessione per ciclo, su due tabelle vuote; e su un free tier a 2 core con ceiling misurato attorno a 480 msg/s e' rumore misurabile che nessun cliente ha chiesto. **Quell'argomento e' morto con i flag, e non perche' il costo sia sparito:** perche' non esiste piu' un'installazione che "non ha" le due superfici, e una superficie con una scadenza, o una scadenza senza il suo mietitore, accumulano e basta. La difesa per chi non le usera' mai non e' quindi piu' l'assenza del task: e' **il backoff a tabella vuota** qui sotto, che e' una rete piu' sottile e va trattata come tale, con il suo perf gate a tabella vuota su cella 2-core come criterio di uscita di F3 (§15).

**`QUEEN_SWEEPER=false` resta, ed e' l'unica manopola che ferma il task.** Non e' un gate e non va descritta come tale in nessuna pagina: spegne il **mietitore**, non le superfici. Una cella che lo spegne continua ad accettare chiavi e timer, non pota mai una chiave scaduta e non fa partire mai un timer. E' la combinazione che accumula in silenzio, quindi il boot la dichiara con un WARN una volta, e §17.1 la mette in tabella accanto ai kill switch proprio per non farla scambiare per uno di loro.

E anche acceso serve un **backoff sulla tabella vuota**: dopo K cicli con `nextInMs` a `NULL` e zero righe potate, il sonno sale progressivamente fino a 30 s, azzerato dal `hint()` locale. Il tetto di 1 s e' una rete per la latenza di consegna, e non ha senso quando non c'e' niente da consegnare.

### 7.2 Il ciclo

```
loop {
    // A. sonda: una call, una transazione read-only, un now() del server
    let due = db::timers_due(&c, &shards).await;

    // B. drenaggio: claim e fire nella STESSA iterazione, mai 5000 righe in anticipo
    while due.due > 0 && drained < cfg.cycle_max_rows && lease_budget_ok() {
        let claims = db::timers_claim(...).await?;
        if claims.is_empty() { break; }
        for batch in group_and_pack(claims, cfg) {
            match db::timers_fire(&c, &batch).await { ... }
        }
    }

    // C. sotto-cadenze, ognuna col suo orologio, mai sul percorso due-driven
    if kv_gate.elapsed()   { drive_step(kv_expire_step_v1) }
    if usage_gate.elapsed() { usage_rollup() }

    // D. sonno due-driven
    tokio::select! { _ = sleep(sleep_ms) => {}, _ = wake.notified() => {} }
}
```

**Claim e fire nella stessa iterazione, con un tetto sul lavoro in volo.** La forma ingenua drena `CYCLE_MAX_ROWS = 5000` in 25 giri; se i fire sono lenti, cosa che succede proprio sotto carico per contesa di partizione con i push, i claim del primo giro scadono prima che il venticinquesimo fire committi. Un altro broker li riclaima, il nostro fuoco trova token diversi, tutti `stale`, il packing e' buttato, e sotto carico sostenuto due broker si rubano il lavoro a vicenda mentre `lateMs` sale e `fired` resta basso: **livelock osservabile solo come "lo sweeper e' lento"**. Il tetto e' `lease_budget_ok()`, cioe' si smette di claimare quando il lavoro stimato in volo supera meta' di `QUEEN_SWEEPER_LEASE_MS`.

**Il tetto della transazione di fuoco e' in BYTE, non in segmenti**: `QUEEN_SWEEPER_MAX_FIRE_BYTES` (8 MiB). Superato, il batch si spezza in piu' chiamate, ognuna una transazione. La lezione di `retention.rs` e' che il costo dello step e' per RIGA e un batch piu' grande non assorbe piu' lavoro, stalla solo i push; qui il costo e' per BYTE di WAL, quindi il tetto e' in byte.

**Backoff con jitter sulla claim vuota.** Quando `nextInMs <= 0` ma la claim non prende niente perche' un altro broker sta drenando, il pavimento di 5 ms e' uno **spin**: cinque broker, un burst, uno drena e quattro girano a 200 sonde al secondo ciascuno, ognuna con 64 seek piu' la count, circa 800 query al secondo aggiunte esattamente mentre il fuoco scrive WAL e i push competono per la stessa fsync. La claim vuota produce un backoff con jitter fra 25 e 200 ms; il pavimento serve solo quando c'e' lavoro effettivamente disponibile.

Regole ereditate, una per una: **slot prima della connessione, sempre** (`admission::lane_slot(Lane::Maint)` e poi `pool.get()`, invertire deadlocka); corsia `Maint`, mai `Push`, perche' le corsie sono state validate a quattro e questa feature non le ritara; **il ciclo che fallisce viene loggato e ingoiato**, con la ERROR rate-limitata da un `obs::Sampler`; **log solo quando ha lavorato**, altrimenti DEBUG, perche' un cluster fermo non deve scrivere una riga al secondo; **timeout e cancellazione** via `db::resolve_query_timeout` su ogni chiamata, perche' un fuoco incastrato dietro un lock di partizione che lascia il backend a girare terrebbe **lock su timer** aperti a tempo indefinito, e quelli bloccano le cancel degli utenti; **zero unwrap**, perche' `panic = "abort"` significa che un panic nel task abbatte il broker.

### 7.3 Parallelismo

`QUEEN_SWEEPER_PARALLELISM` default **1**, clampato a 8, con lo stesso `const _: () = assert!(MAX >= 2)` di `retention.rs`. Default 1 perche' il fuoco e' una transazione **scrivente** che tiene lock di partizione, non una delete di manutenzione: due fuochi concorrenti sulle stesse partizioni si contendono lo stesso serializzatore dei push. Si alza solo se `lateMs` cresce sotto carico e il profilo dice che il collo e' il round trip e non il commit.

### 7.4 La sveglia: locale e basta, in v1

**Niente `LISTEN/NOTIFY` di Postgres.** Verificato: `LISTEN` e `pg_notify` non compaiono da nessuna parte in `server/src` ne' in `server/sql`. Tre ragioni per non introdurlo qui, e la seconda da sola basterebbe: servirebbe una connessione dedicata **fuori dal pool** tenuta aperta per sempre per broker, che deadpool non ha e che sarebbe infrastruttura nuova per una sveglia best-effort; la coda di notifica di Postgres e' un'area condivisa da 8 GB e quando si riempie il `NOTIFY` **fa fallire la transazione che sta committando**, cioe' un guasto della sveglia diventerebbe un guasto dello schedule dell'utente, e la sveglia deve poter essere persa, mai fare danno; e il canale cross-broker esiste gia' ed e' dichiaratamente best-effort.

**In v1 la sveglia e' solo locale e in-process.** Una struct su `AppState` con un `AtomicI64` del `deliver_at` piu' vicino committato localmente e un `Notify`; l'handler di `POST /api/v1/timers` e quello di `/api/v1/transaction` chiamano `hint()` **dopo il commit**, mai prima (una sveglia per una transazione che poi rollbacka costa un ciclo sprecato e, peggio, insegna al loop che esiste lavoro che non esiste). Costo: un CAS. E l'anti-storm e' gratis: schedulare un milione di timer per la settimana prossima produce esattamente **una** sveglia, perche' il `hint` si applica solo quando il nuovo minimo e' anticipato.

**Il frame mesh `T_TIMER_DUE` e' tagliato dalla v1** (§18.3). La rete che rende la mesh non necessaria e' il tetto di sonno: `QUEEN_SWEEPER_MAX_SLEEP_MS` (1 s) e' anche la finestra di recupero massima per un timer schedulato da un altro broker. Un cluster multi-broker senza mesh funziona lo stesso, con al massimo un secondo di ritardo, e `deliverAt` e' "non prima di". Va scritto nell'header, o qualcuno leghera' la correttezza della feature alla mesh.

### 7.5 Il rollup d'uso, e perche' la forma ingenua e' un difetto

Tre fatti che vanno composti: `queen.kv` ha **un solo** indice secondario ed e' **parziale** (`WHERE expires_at IS NOT NULL`), quindi non esiste nessun access path per "tutte le righe di un tenant"; un rollup con `sum(pg_column_size(...))` per tenant e' quindi un **seq scan** della tabella; e `vacuum_truncate = off` significa che la heap **non si ritrae mai**.

Guasto composto: un tenant schedula un milione di timer per una campagna, che maturano e spariscono in un'ora. `queen.log_timers` torna a zero righe vive ma conserva per sempre le pagine del picco, e da quel momento il rollup scandisce quella heap ogni cinque minuti, su una connessione e uno slot `Lane::Maint`, per contare zero. Il costo di manutenzione e' diventato funzione del **picco storico**, non del lavoro pendente: e' la stessa forma del "la manutenzione scala con le partizioni" che il rework seg v2 ha eliminato, reintrodotta su un'altra dimensione.

E la misura stessa e' una biforcazione con **entrambi i rami difettosi**: `sum(pg_column_size(k.*))` su un whole-row var o appiattisce il datum composito, e allora **detoasta ogni valore KV della tabella ogni cinque minuti**, oppure non lo appiattisce, e allora conta il puntatore esterno da 18 byte al posto del valore, cioe' **sottostima sistematicamente proprio le righe grandi** che la quota esiste per limitare, con un errore di ordini di grandezza e non del venti per cento dichiarato.

**Correzione decisa, tre pezzi:**
1. `pg_column_size(k.value)` sulla **colonna**, mai sul whole-row: non detoasta e conta la dimensione esterna.
2. Il conteggio e' **incrementale**: `kv_rows` e `kv_bytes` si aggiornano con il delta delle righe che lo sweeper stesso cancella, piu' le insert contate in-process dal broker. Il full scan resta ma diventa **raro** (orario), ed e' **guardato dalla dimensione della tabella**: sopra una soglia si degrada a campionamento e la gauge si etichetta stimata.
3. Il rollup prende un `SKIP LOCKED` per shard, cosi' due broker non ripetono lo stesso lavoro, e la riga e' last-writer-wins su `computed_at` (§3.2 spiega perche' la PK e' per tenant e non per shard).

La cadenza lenta e' `QUEEN_KV_USAGE_EVERY_MS` (300 000), la stessa classe di costo di `PARTITION_SWEEP_EVERY`. **Non deve mai spostarsi sul percorso due-driven.**

### 7.6 Un solo classificatore SQLSTATE in tutto il broker

Oggi `classify_push_error` e' privata in `file_buffer.rs:123-139` e ha due esiti (verificato: `40001`, `40P01` e le classi `08`, `53`, `57`, `58` piu' l'assenza di SQLSTATE sono transienti, tutto il resto e' permanente). Si **estrae** in `db.rs` come `classify_sql -> SqlClass { Transient, Config, Permanent }` e `file_buffer.rs` la chiama. La classe 42 si scorpora da `Permanent` come `Config` perche' "il nome della coda di destinazione e' malformato" non e' lo stesso evento operativo di "questo payload viola un vincolo": un operatore puo' riparare un `Config`, nessuno puo' riparare un `Permanent`, e il percorso timer deve loggarli in modo diverso.

**Isolamento del veleno.** Il fuoco e' una transazione per molti segmenti: un solo segmento avvelenato fa fallire tutto il batch, e senza contromisura quel batch fallisce per sempre e i segmenti sani non arrivano mai in fondo. Su errore **permanente** con piu' di un segmento, il broker rilancia il batch **un segmento per chiamata**: quelli sani committano, il velenoso si prende il suo `attempts`. Costa un giro in piu' solo sul percorso di guasto. E' il guasto piu' insidioso di tutta la feature, perche' il sintomo (`lateMs` che sale) non nomina il colpevole. Knob `QUEEN_SWEEPER_ISOLATE_ON_PERMANENT`, default `true`.

Lo **snapshot per la DLQ** e' il payload decompresso che il broker ha gia' in mano dalla claim, parsato come JSON; se non e' JSON valido si scrive `{"_raw_b64":"..."}` invece di far fallire la DLQ: la DLQ e' l'ultima spiaggia e non puo' avere un ramo che perde il messaggio.

---

## 8. Broker: handler, rotte, e il livello HTTP verso wire

### 8.1 Rotte

Registrate in `main.rs` nel blocco `.route(...)` accanto al data path, non al management plane, rispettando la regola matchit statico-prima-di-parametrico che il file ripete quattro volte (quindi `/api/v1/kv` prima di `/api/v1/kv/:ns`).

| metodo | path | note |
|---|---|---|
| POST | `/api/v1/kv` | batch, `ns` obbligatorio in ogni op. **La superficie completa**, e l'unica che accetta `getPrefix` e `incr` |
| GET | `/api/v1/kv/:ns/*key` | catch-all, cosi' `order/9f1/items` si scrive naturale; header `ETag: "<version>"` |
| PUT | `/api/v1/kv/:ns/*key` | `{"value", "ttlSeconds"|"forever", "expect"?}` |
| DELETE | `/api/v1/kv/:ns/*key` | `{"expect"?}` opzionale |
| POST | `/api/v1/timers` | schedule e reschedule, batch |
| DELETE | `/api/v1/timers/:queue/*timerKey` | **cancel, rotta e classe SEPARATE**, §8.4 |
| GET | `/api/v1/timers/:queue/*timerKey` | peek |
| GET | `/api/v1/timers/:queue` | list, keyset |

Decisioni di forma: **una sola regola sugli status code**, lo status descrive l'esito della **chiamata**, non del predicato di business, quindi chiave assente, gara persa e delete a vuoto sono 200 con un campo esplicito. Il costo si dichiara (curl non si comporta "da REST" su una chiave mancante e chi fa scripting deve guardare il body); il beneficio e' che nessun SDK, proxy, dashboard o retry policy tratta l'esito piu' frequente del prodotto come un errore. Il precedente in casa e' verificato: il delete di una coda tiene 200 su `deleted:false` proprio perche' delete-before-create e' un idioma degli SDK. **`ETag` si', `If-Match` no**: l'header di risposta e' gratis e informativo, la scrittura condizionale passa **solo** da `expect` nel body, un solo modo di esprimere una precondizione, e va detto che l'ETag risparmia banda e **non** il round trip al DB. **Nessun segmento letterale sotto `/api/v1/kv/:ns/`**, o renderebbe irraggiungibile qualunque chiave che si chiami come lui, ed e' anche il motivo per cui `incr` esiste solo via POST batch. **Nessuna query string con prefissi** (§5.5).

**Livelli d'accesso in `auth.rs::route_access_level`, scritti esplicitamente.** Verificato: la funzione termina con un fallthrough a `ReadWrite`, e le rotte non nominate ci cadono dentro. Senza le regole, un token read-only prende 403 su `GET /api/v1/kv/:ns/*key`, e un token produce-only prende 403 su `POST /api/v1/timers`, che e' un'operazione di produzione. E la correzione fatta di fretta (un `starts_with` messo **fuori** dal blocco `if m == "GET"`) regala scrittura ai token read-only. Le regole vanno dentro il blocco giusto, nella stessa patch che ribatte `ACCESS_FINGERPRINT` per `gen-routes`:

| rotta | livello |
|---|---|
| `GET /api/v1/kv*`, `GET /api/v1/timers*` | `ReadOnly` (dentro il blocco `if m == "GET"`) |
| `POST|PUT|DELETE /api/v1/kv*` | `ReadWrite` |
| `POST /api/v1/timers` | `WriteOnly` (coerente con Produce) |
| `DELETE /api/v1/timers/*` | `ReadWrite` |

### 8.2 Il livello che manca in tutti i design: il demux HTTP verso wire

Verificato in `server/src/handlers/data.rs:3602-3685`: il body del transaction wire e' `{"operations":[...]}`, un array **piatto**, e l'handler fa il demux per campo `type` costruendo `groups` e `ack_groups` con un contatore `flat` che indicizza le echo, chiudendo con `_ => { any_unknown = true; flat += 1; }` e un 400 `"segments transaction supports only push and ack operations"`. Il wire JSONB a quattro array **non e' cio' che il client manda**. Nessun documento di design descriveva questo livello, e la prova e' che `handlers/data.rs` non compariva in nessuna lista di file toccati.

Serve, ed e' lavoro reale:

1. **Gli array `kv` e `timers` sono campi TOP-LEVEL della request, mai elementi di `operations`** (§6.3, §10.4), quindi **non** nascono due rami nuovi nel `match ty`: il `match` resta quello di oggi, e un'op che arriva comunque come `{"type":"kv"}` dentro `operations` continua a cadere nel `_ =>` e a prendere il 400 nominativo, che qui e' il fallimento migliore disponibile. Lo spazio piatto di `results[]` si estende **solo in coda**, con un layout append-only: `[0, ops_flat)` sono le `operations` esattamente come oggi, `[ops_flat, ops_flat + kv_n)` e' l'array `kv`, `[ops_flat + kv_n, + timers_n)` e' l'array `timers`. Un push o un ack **non cambia mai indice** perche' un rider e' presente, e un bundle che non porta nessuno dei due array produce un `results[]` della lunghezza e del contenuto di oggi. Non c'e' nessuna regola di interleaving da sbagliare, ed e' precisamente per questo che il layout e' questo invece di un ordine di richiesta fra tre array che JSON comunque non ordina.
   **Perche' la forma inline e' rifiutata, scritto qui perche' nessuno la reintroduca.** Allargare `Operation` con una gamba `kv` fa scartare a `encoding/json` **entrambi** i campi Go che portano la stessa chiave JSON allo stesso livello, senza errore e senza warning (§10.4). Il body partirebbe con **zero** op KV, il broker committerebbe una transazione senza gate, e il `putIfAbsent` per cui il bundle esisteva non sarebbe mai esistito: un difetto silenzioso in un client tipizzato, cioe' la classe che questo piano rifiuta ovunque. La forma top-level non lo ammette perche' i due array non condividono nessun livello con `operations`. Il ragionamento in esteso vive in §10.4 e §6.3, che sono le sezioni autoritative sulla forma di wire, ed e' quello che l'implementazione ha seguito (branch `kvtimer`, non la base f1aad78: `server/src/handlers/data.rs:3565-3589`).
2. **La mappa `flat index → (array, ordinale)`**, che con il layout append-only del punto 1 e' **due sole basi** (`kv_base`, `timers_base`) e non un vettore parallelo, e lo **scatter inverso** dei risultati dallo spazio per-array allo spazio piatto di `results[]`, che e' il contratto che i client leggono oggi.
3. **La guardia di allineamento portata sulla mappa.** La protezione contro un ordinale non riempito (`003_log_push.sql:372-375`) non e' esprimibile fra due spazi di indici diversi: va riscritta come "ogni ordinale piatto ha esattamente un risultato", e deve fallire rumorosamente.
4. `failedIndex` nella traduzione dell'errore `required` deve essere nello spazio **piatto**, altrimenti il client indicizza l'operazione sbagliata.
5. La riscrittura delle op prima di serializzarle verso la SP: `producerSub` e `messageId` iniettati dal server, `tenant` rifiutato (§4.2, §6.2).
6. L'instradamento dei bundle di **solo KV** fuori dal wire, su `kv_apply_v1` diretta (§2.5).

**Effetto collaterale desiderabile e da documentare:** un client nuovo contro un broker vecchio prende un **400 pulito e nominativo** su un'op sconosciuta, che e' il miglior fallimento possibile e va scritto in `reference/compatibility.mdx`.

### 8.3 `txn_fail_body` deve crescere, e tocca tutti i sette client

Oggi il corpo di fallimento e' `{transactionId, success:false, error, results:[]}` (`handlers/data.rs:3679-3685` e `:4060-4066`). Il verdetto di precondizione richiede `reason`, `failedIndex`, `kvReason`, `version` e `value`. Quel corpo e' condiviso da **ogni** fallimento di transazione, quindi e' un cambiamento di wire che tocca tutti e sette i client e le loro retry policy, non un dettaglio KV, e va contato come tale nelle fasi. Il client non puo' fare string matching sul messaggio: e' vietato ovunque in questa base di codice.

La traduzione al confine: un `required` perso e' l'esito atteso di ogni riconsegna legittima, quindi il broker lo traduce in **HTTP 200** `{"ok":false,"reason":"kv_precondition","failedIndex":0,"kvReason":"exists","version":90101,"value":{...}}`. La transazione in SQL abortisce davvero, il `RAISE` e' necessario, ma non inquina le metriche d'errore ne' le retry policy.

### 8.4 Le tre difese che questo endpoint deve avere e nessun altro ha

E' **il primo endpoint del prodotto il cui tasso di chiamata lo decidono gli utenti finali del cliente e non il volume dei messaggi.** Ogni altra rotta ha un limitatore naturale a monte; qui il limitatore e' il traffico web di qualcun altro.

1. **Pool di connessioni dedicato**, `QUEEN_KV_POOL_SIZE = clamp(pool_size / 10, 4, 32)`, **derivato dal pool e non fissato**, con lo stesso precedente esplicito di `admission_floor` in `config.rs` ("derived from the pool rather than hardcoded"). Con `DB_POOL_SIZE = 160` il pool KV e' 16, esattamente la riserva di admission che il prodotto gia' considera accettabile tenere ferma. Il pool **e' esso stesso il semaforo**, e la proprieta' che nessun'altra difesa da' e' questa: con letture da circa 1 ms la capacita' e' circa 16 000/s, molto sopra ogni rate limit, quindi non e' il limitatore in condizioni normali; diventa il limitatore quando il **DB rallenta**, perche' a 100 ms per lettura la capacita' crolla a 160/s e il resto prende 503 invece di rubare connessioni al percorso messaggi.
2. **Nessuna corsia dell'arbitro, ne' per le letture ne' per le SCRITTURE.** Questa e' una correzione rispetto al design: mettere le scritture KV su `Lane::Push` significa che 30 tenant, ciascuno dentro il proprio limite di 100 scritture al secondo, consumano 3000 slot `Lane::Push` al secondo su uno stack il cui ceiling misurato commit-bound sul free tier e' circa 480 msg/s. Nessun tenant ha violato niente, il percorso messaggi e' affamato, e il pesatore non sa distinguere i due tipi di lavoro perche' sono la stessa corsia. Le scritture KV standalone usano lo **stesso pool dedicato**, e la contropressione e' il pool. Le scritture KV che viaggiano **dentro** push e ack ereditano lo slot gia' preso dall'handler e non ne prendono un altro: quello e' un bundle di messaggi, ed e' giusto che stia nella corsia dei messaggi.
3. **Token bucket per tenant nel broker**, valutato **prima** di `pool.get()`, con 429 e `Retry-After`. Nel broker e non solo nel proxy perche' self-hosted e dedicated senza proxy sono deployment reali, e una difesa che esiste solo nel proxy non e' una difesa del prodotto. Default 200 letture al secondo con burst 400 (una lettura su PK costa 0,3-1 ms di backend, quindi 200/s sono circa il 20% di un core, e la regola difesa e' che **le letture KV non devono poter consumare piu' del 10% circa del tempo CPU del backend che serve il log**), 100 scritture al secondo (ogni scrittura e' un commit durabile, fsync circa 4 ms). Piu' un **tetto aggregato di cella** oltre i limiti per tenant, dimensionato come frazione dichiarata della capacita' del percorso messaggi, perche' N tenant conformi possono sommare a una cella non conforme.
4. **`resolve_query_timeout` obbligatorio**, non facoltativo: un `getPrefix` lento senza cancel server-side lascia il backend a girare e manda la connessione in quarantena, e su un pool da 16 tre quarantene sono il 19% della capacita'.

Sul **dedicato** i limiti si abbassano ma **non si tolgono**. Ogni altro limite di piano ha `NULL` uguale illimitato sul dedicato, perche' protegge i vicini e sul dedicato non ci sono vicini; questo protegge il tenant **da se stesso**, perche' il concorrente non e' un altro tenant, e' il suo stesso percorso messaggi sullo stesso Postgres. Va scritto accanto alla colonna, o il primo che la legge lo "corregge" a `NULL`.

### 8.5 Cosa NON si puo' fare, esplicitamente

- **Non si cacha il valore KV. Mai, con nessun TTL.** Il caso d'uso numero uno e' il marcatore di idempotenza: una lettura stantia dice "non c'e'" e il chiamante fa l'effetto esterno due volte. Una cache con TTL trasformerebbe il prodotto transazionale in eventualmente coerente **proprio nella primitiva che esiste per non esserlo**, e non esiste un TTL sicuro perche' non esiste invalidazione (la scrittura puo' avvenire su un altro broker e il broadcast mesh non e' sincrono con la lettura).
- **Non si servono le letture KV da una replica**, il giorno che ci saranno repliche: il lag e' la race.
- **Si cacha invece**: la misura d'uso e la quota (stantie per costruzione e gia' dichiarate morbide), il verdetto del token bucket (non e' una cache di dati), e il **single-flight**, che e' l'unico meccanismo di amplificazione **sicuro** e per fortuna e' proprio quello adatto al caso: due `GET` in volo per lo stesso `(tenant, ns, key)` condividono una query, non c'e' finestra di staleness oltre la durata della query stessa, e la risposta e' consistente con un istante reale del database. Su una chiave calda con 5000 lettori al secondo e una query da 1 ms riduce le query a circa 1000/s. Metrica dedicata, perche' e' anche la spia che un cliente ha messo il KV sul suo percorso web.
- **Non si espone il KV senza autenticazione**, nemmeno con una chiave pubblica di sola lettura: diventerebbe una CDN chiave/valore con Postgres al posto della CDN.
- **Non si fa watch, subscribe o long poll sulle chiavi.**

---

## 9. Quote, tenancy, e la quota storage del proxy

### 9.1 Correzione di premessa: la quota storage NON e' un no-op

Il brief e due documenti di design dicono che la quota storage del proxy "oggi e' ancora un no-op", e su quella premessa poggiava la decisione di accendere o spegnere la feature di default. **E' falso**, e va corretto in tutte le sezioni di rischio prima che qualcuno legga il gate di rilascio come gia' insoddisfacibile e lo ignori. Verificato in questa sessione:

- `proxy/src/registry.rs:423-434` decide `over_storage` chiamando `decide_over_storage` (`:441-449`), che ha **isteresi**: blocca sopra `max`, rilascia solo sotto `STORAGE_RELEASE_PERCENT` del cap, e un cluster gia' sopra alla prima passata blocca subito.
- `proxy/src/gateway.rs:120-127` risponde `err_403(CODE_STORAGE_QUOTA, "storage quota exceeded; pushes blocked")` su ogni `RouteClass::Produce`, ed e' un gate **hard**, applicato anche in shadow mode, con il commento che lo dichiara deliberato.

Quello che manca non e' l'enforcement, e' **la misura**: `registry.rs:377-381` somma esclusivamente `retainedBytes` per coda. `queen.kv` e `queen.log_timers` **non hanno una coda**, quindi oggi sarebbero byte invisibili a una quota che esiste ed e' viva. E' l'unico posto del prodotto dove un tenant potrebbe occupare disco senza che nessuna quota lo veda. E' il rischio vero, e' piu' piccolo di quello dichiarato, e la §16 lo trasforma in un criterio di uscita di fase.

### 9.2 Dove va ogni limite, e la regola che lo decide

Tre classi, tre punti di applicazione, e la classe determina il punto.

| classe | esempi | dove | perche' li' |
|---|---|---|---|
| **Forma** | dimensioni, conteggi, charset, TTL obbligatorio, orizzonte | **SQL, dentro la SP**, piu' una guardia di body al confine HTTP | sette client piu' il broker embedded, che non passa dagli handler. Una regola scritta in SQL vale per tutti e non ha una versione per linguaggio da tenere allineata |
| **Occupazione** | righe KV, byte KV, timer pendenti | **broker, in RAM**, da misura piu' delta locale | §9.3: una quota esatta e' strutturalmente incompatibile con l'ordine di lock scelto |
| **Tasso** | richieste al secondo per tenant | **proxy quando c'e', broker sempre** | il proxy e' opzionale per costruzione |

Limiti di forma, con le derivazioni che chiudono un bypass:

- `QUEEN_KV_MAX_VALUE_BYTES` 65536, applicato **in due posti che misurano cose diverse**: il confine HTTP misura i byte grezzi del body, la SP misura `octet_length(value::text)`, cioe' il testo canonico di JSONB, normalmente piu' corto. Va documentato, o e' una sorpresa al primo valore vicino al tetto. Errore SQLSTATE 22001 (classe 22, permanente per il classificatore), HTTP 413.
- `QUEEN_KV_MAX_KEY_BYTES` 512: la tupla dell'indice PK e' `uuid(16) + namespace + key`, e il tetto btree e' circa 2704 byte, quindi con 64+512+16 si sta a un quinto. Senza il tetto, il guasto e' un `54000` al primo insert, cioe' runtime invece che validazione.
- `QUEEN_TIMERS_MAX_PAYLOAD_BYTES = min(1 MiB, plan.max_payload_bytes)`, **derivato, non indipendente**. Un timer diventa un messaggio: se il suo tetto non e' quello del messaggio, il timer e' una porta di servizio per superare il `max_payload_bytes` del piano. E' un buco che si apre da solo se il limite viene scritto come costante propria.
- `QUEEN_TIMERS_MAX_HORIZON_S` **finito di default** (7 776 000, novanta giorni), non zero uguale illimitato. Con orizzonte infinito la quota righe diventa permanente invece che ciclica: il tenant riempie `max_timers` e non lo libera mai. Con orizzonte finito il caso peggiore della tabella e' calcolabile, `righe <= tasso_schedule * orizzonte`. Novanta giorni coprono retry, scadenza trial e follow-up; oltre serve un cron, che la v1 non fa comunque.

### 9.3 Perche' la quota di occupazione e' morbida, e qual e' lo sforo VERO

Una quota esatta richiede un contatore per tenant aggiornato da ogni scrittura. Quel contatore e' **una riga per tenant che ogni scrittura KV di quel tenant deve bloccare**, e vivrebbe nello **spazio di lock piu' esterno dei sei**, tenuto per tutta la durata del bundle. Serializzerebbe tutte le scritture di un tenant su una riga sola, davanti al percorso messaggi. Non e' costosa: e' l'anti-pattern esatto che l'ordine di lock esiste per impedire (§2.4 D7).

**Lo sforo dichiarato nei design era sbagliato di un ordine di grandezza, e va corretto.** La formula proposta era "tasso di scrittura per 30 s", dove i 30 s sono ogni quanto un broker **rilegge** la misura. Ma la misura viene **scritta** ogni 300 s. Il bound vero e' `tasso * (300 s + 30 s + durata del rollup)`, piu' i 10 s della pompa del proxy per la quota storage. Con i default dei piani: un tenant free a 20 scritture al secondo sfora di 6600 righe contro una quota di 10 000 (66%) e di 6600 timer contro una quota di 1000 (**660%**), stabilmente, senza mai vedere un 403. "Morbida" non descrive questo, descrive un limite che non esiste.

**Correzione decisa: il delta locale in-process e' L'ENFORCER, la misura serve solo al rilascio.**

- Ogni broker tiene in `AppState` la misura letta piu' il **proprio** delta di righe e byte scritti dall'ultimo rinfresco. Rifiuta a `quota - misura` senza aspettare la misura nuova, quindi il blocco e' immediato per lo scrittore che sfora ed e' limitato solo dal fan-out fra broker.
- Il **rilascio** guarda solo la misura vera, con la stessa isteresi di `decide_over_storage`: si esce dal blocco solo quando la misura e' sotto banda. Blocco veloce, rilascio lento. Il contrario sarebbe un tenant che oscilla dentro e fuori.
- La formula pubblicata in documentazione e' quella vera, e la cadenza del rollup scende a quella del rinfresco per i tenant sopra l'80%.

Il percorso di scrittura paga un **lookup in hashmap, zero SQL**. Va scritto nell'header del wire come divieto (§2.4 D7).

### 9.4 Il tenant non e' validato, e questo cambia i default

`server/src/tenant.rs:16-19` e' esplicito: con il flag acceso e l'header presente e sintatticamente valido, quel tenant e' **opaco e non validato contro niente**, la fiducia e' la rete di cella. Su questo si vorrebbero appoggiare un token bucket per tenant in RAM, una riga di quota dove **`NULL` uguale illimitato**, e un rollup che crea righe per tenant.

Scenario, su un deployment che i design stessi dichiarano reale (self-hosted o dedicated senza proxy): il client ruota l'header a ogni richiesta. Ogni richiesta ottiene un bucket nuovo, quindi il rate limit e' annullato; un lookup di quota che non trova riga, quindi **illimitato**; uno spazio chiavi nuovo; e una entry permanente nelle mappe in RAM del broker e nella tabella di misura. Disco illimitato, RAM illimitata, e ogni gauge che dice che nessuno e' sopra quota. La classe di attacco e' gia' stata capita in casa: `handlers/mod.rs` documenta che i **negativi non vengono mai cachati** proprio perche' un pid forgiato non deve poter far crescere la mappa.

**Correzioni decise, quattro:**
1. Con `QUEEN_TENANCY_HEADER` attivo, **l'assenza di una riga in `queen.kv_quota` e' un diniego** (403 `feature_gated`), non un permesso. Scritto nel commento del DDL, non solo nel piano. Con la tenancy spenta, la feature funziona senza configurare nulla: `QUEEN_KV_REQUIRE_GRANT` e' **derivato** dal flag di tenancy, cosi' l'utente self-hosted non deve capire perche' dovrebbe configurare qualcosa.
2. Le mappe per tenant in RAM (token bucket, cache quota) sono **size-capped**, e il cap **nega**, non sfratta.
3. Il rollup non crea righe per un tenant che non ha ne' quota ne' code.
4. Il boot **rifiuta di partire** con `QUEEN_TENANCY_HEADER=1` senza `QUEEN_KV_TRUSTED_PROXY=1`, e **senza nessun'altra condizione**. L'interlock era keyed su `QUEEN_TENANCY_HEADER=1 && QUEEN_KV_ENABLED=1`; con il flag KV sparito (§0) resta la sola gamba di tenancy, e il requisito diventa **incondizionato** per chi gira con l'header. E' la forma onesta che aveva gia': scegliere `QUEEN_TENANCY_HEADER=1` **e'** l'affermazione che davanti c'e' un proxy che scrive l'header e strippa quello del client, e un deployment che non puo' affermarlo non e' un deployment a cui manca una feature, e' un deployment con una porta aperta. **Fatale, e non c'e' una terza uscita da offrire:** non esiste piu' un flag che possa spegnere il KV per rendere sicura la combinazione, e non deve esistere, perche' "il motore manca su alcune celle" e' peggio di un boot che ti dice quale env mettere. Il precedente e' `env_bool`, che su un valore non riconosciuto chiama `obs::fatal()`: per un interruttore di sicurezza e' il comportamento giusto. Se un operatore deve davvero togliere il KV da una cella viva, quello e' il kill switch di **runtime**, che e' un altro strumento per un'altra situazione e che comunque non renderebbe sicura un'identita' di tenant non validata.

### 9.5 Cosa succede al superamento

**Una regola sola, e discrimina cio' che il client deve decidere:**

> **429 uguale riprova piu' tardi e funzionera'. 403 uguale riprova quanto vuoi, non funzionera' finche' non cambi qualcosa. 503 uguale non e' colpa tua, e' la cella.**

| condizione | status | code |
|---|---|---|
| forma sbagliata (charset, TTL mancante, ops, prefisso vuoto) | 400 | `kv_bad_request` piu' `reason` |
| valore o payload oltre il tetto | 413 | `payload_too_large` |
| **occupazione superata** | **403** | `kv_quota_exceeded`, `timers_quota_exceeded` |
| orizzonte oltre il piano | 403 | `timer_horizon_exceeded` |
| feature non nel piano, o nessuna riga di quota con tenancy attiva | 403 | `feature_gated` |
| tasso superato | 429 piu' `Retry-After` | `rate_limited` |
| pool KV esaurito o DB lento | 503 piu' `Retry-After` | `kv_unavailable` |
| feature messa in pausa dal kill switch di runtime | 503 piu' `Retry-After` sulle rotte, rifiuto **permanente** dentro il wire | `kv_disabled` |

**Non c'e' nessuna riga 404 in questa tabella, e l'assenza e' la decisione di §0.** C'era, e diceva "feature spenta da env, la superficie non esiste su questa cella". Su ogni cella che gira questo binario la superficie esiste, quindi non c'e' nessuna condizione di configurazione per cui una rotta kv o timers possa rispondere "questo endpoint non esiste qui". Il 404 resta solo cio' che il fallback JSON dice a un path che non e' una rotta, e l'unico 404 rilevante per questa feature e' quello del **proxy** su una cella cloud che non ha ancora F8 P1 (§9.8).

**Contro il 507:** e' WebDAV, nessun client HTTP lo tratta specialmente, e il proxy ha **gia'** `403 storage_quota_exceeded` con la nota che i client ci fanno `switch` sopra e lo trattano come terminale. Un terzo status per lo stesso concetto sarebbero due dialetti di esaurimento risorsa da tenere allineati in sette client per sempre. **Contro il 429 sull'occupazione:** `Retry-After` su una quota righe sarebbe una bugia, nessun ritardo la risolve, e farebbe ritentare il client in loop esattamente quando il tenant e' gia' oltre.

**Le letture e le DELETE sono sempre permesse**, anche sopra quota, altrimenti un tenant pieno non puo' liberarsi. Vale anche, e soprattutto, per i timer: vedi §9.6.

### 9.6 La cancel dei timer non e' mai bloccabile

Un design proponeva di classificare i timer come **variante di Produce** nel proxy, cosi' che i blocchi push valessero automaticamente. La meta' e' giusta e la meta' e' un guasto: `POST /api/v1/timers` porta `cancel` **nello stesso array** dello schedule, quindi un tenant sopra quota (o sotto `over_storage`) prenderebbe 403 anche sulle cancel. Nel frattempo il fuoco **non si spegne mai automaticamente** (§12), quindi il tenant continua a produrre messaggi che non puo' fermare, fino all'orizzonte o a un intervento d'operatore. Il blocco produrrebbe l'opposto del suo scopo.

**Decisione:** la cancel ha una rotta e una classe sue, `DELETE /api/v1/timers/:queue/*timerKey`, classificata come lettura o management nel proxy e **mai bloccabile**. Il batch POST rifiuta esplicitamente un batch misto quando il cluster e' bloccato, invece di bloccarne meta' in silenzio. Schedule e cancel sono la stessa SP ma **non la stessa decisione di autorizzazione**, e va scritto.

### 9.7 Metering

Il fuoco di un timer produce un messaggio che **il metering del proxy non vedra' mai**: avviene nello sweeper, dentro il broker, e non attraversa il gateway. Il timer e' l'unico modo, in tutto il prodotto, di produrre un messaggio senza passare dal proxy. Non e' aggiustabile lato proxy, perche' il meter conta cio' che passa dal campione del gateway.

**Decisione:** si fattura la promessa, non la consegna. `POST /api/v1/timers` conta **un messaggio per ogni op `schedule`** del batch, non uno per chiamata: con un cap di 256 op per chiamata, contare per chiamata sarebbe un sotto-conteggio fino a 256 volte. Il reschedule conta un altro messaggio, perche' e' la stessa upsert e allo schedule e' indistinguibile. La **cancel conta zero** e non rimborsa. Il fuoco conta zero, perche' e' gia' stato fatturato. E' difendibile in una riga verso il cliente ("un timer e' un messaggio programmato, si conta quando lo programmi") e va ratificato prima di accendere i timer su qualsiasi piano a consumo, non dopo il primo conguaglio (§20).

### 9.8 Cosa serve al proxy, in ordine di dipendenza

`proxy/src/routes.rs:1-8` dichiara: *"This file is the enforcement spec. Owned by the orchestrator, agents must not edit it; report desired changes instead."* Le modifiche qui sotto si **richiedono**, non si fanno, e vanno trattate come una **dipendenza esterna con una data**.

Il fatto che rende spedibile tutto il resto: `routes.rs:158` classifica come `Blocked` qualunque path sotto `/api/` che `classify` non riconosce, e il gateway risponde 404. Quindi il giorno in cui il broker espone `/api/v1/kv` e `/api/v1/timers`, **ogni cella cloud le nega gia'**, fail-closed, senza alcun intervento.

- **P1, gate di feature.** `plans.features` e' JSONB deliberatamente aperto, quindi una feature gated e' un cambio di dati e non una migrazione. `Feature::Kv` e `Feature::Timers`, `classify` che mappa le due famiglie su `Gated(...)` distinguendo `GET` dalle scritture e **isolando la cancel** (§9.6), due bool nella cache con la regola "chiave mancante uguale false". Circa quindici righe, zero modelli di errore nuovi.
- **P2, la misura, che e' il pezzo che manca davvero.** Il broker espone quattro campi top-level sulla stessa risposta che il reconciler gia' interroga, e il proxy li somma in `total_bytes` accanto ai `retainedBytes` per coda. **I campi si leggono dalla misura cachata, mai con un aggregato al volo**: il reconciler interroga ogni 10 s per cella, e la forma ovvia metterebbe un `count(*)` su `queen.kv` ogni 10 secondi. Campi assenti: **zero con un `warn!` una volta per cluster**, non "misura non trovata", perche' quel flag resta vero grazie alle code e una cella vecchia produrrebbe una **sottostima silenziosa** invece di un'astensione. Meglio zero rumoroso che sottostima muta.
- **P3, limiti numerici di piano.** Migrazione versionata nel proxy (che, a differenza del broker, ha migrazioni), stessa convenzione `NULL` uguale illimitato e `CHECK` delle colonne vicine, override per cluster gratis.
- **P4, metering** (§9.7).

**La v1 spedibile e' self-hosted e dedicated senza proxy.** E' anche il posto giusto dove far atterrare una feature con quota morbida: l'operatore e' il cliente stesso. La cella condivisa dipende da P1-P4 e non ha una data finche' l'orchestrator non ne da' una.

### 9.9 Una richiesta al proxy che questa feature rende load-bearing

`proxy/src/gateway.rs:261-274` rimuove gli header hop-by-hop, `HOST` e `AUTHORIZATION`, e poi **inserisce** l'header di tenant solo `if st.cfg.send_tenant_header`. Non c'e' nessuna rimozione incondizionata. Con il flag spento davanti a un broker con tenancy accesa, il tenant lo sceglie il client. E' preesistente, ma il KV e' la prima feature in cui questo significa "leggo e scrivo stato altrui **conoscendone solo il nome**", invece di "indirizzo una coda su cui l'ack ha comunque un gate di ownership". Il KV non ha nessun id opaco e quindi nessun gate equivalente: come dice l'ordine di lock, il gate e' la clausola WHERE, e la WHERE prende il tenant dall'header.

**Richiesta:** una riga, `parts.headers.remove(TENANT_HEADER)` **prima** dell'`if`, incondizionata. Va nella stessa patch, perche' e' questa feature a renderla load-bearing.

---

## 10. Matrice dei sette client

Sette client reali: `client-cli`, `client-cpp`, `client-go`, `client-js`, `client-laravel`, `client-py`, `client-rust`. Un solo documento di design ne progettava uno, e la tabella cross-SDK a due colonne estendeva a Go conclusioni valide solo per JavaScript. Realisticamente sono **cinque completi, uno parziale e uno da decidere**, e va detto cosi'.

### 10.1 Cosa il taglio del rider pop compra ai client

La ragione principale per cui il rider e' fuori dalla v1 (§18.1) e' proprio qui. `client-go` dichiara `type MessageHandler func(ctx, msg) error` e `BatchMessageHandler func(ctx, msgs) error` come **tipi pubblici**: aggiungere un parametro di stato e' una modifica incompatibile, e ogni consumatore esistente smette di compilare. Lo stesso vale per Rust (il bound della closure) e per C++. In JavaScript aggiungere un secondo argomento e' retrocompatibile al cento per cento, ed e' da li' che veniva la conclusione sbagliata. Senza rider, la superficie KV e' **puramente additiva in tutti e sette**.

### 10.2 La matrice

| client | API KV | API timer | tx builder | cambio di wire subito | gate di parita' | costo |
|---|---|---|---|---|---|---|
| `client-js` | 7 op, `once`, `listAll` | builder con terminale `schedule()`/`cancel()` | `tx.kv.*`, `tx.once`, `tx.timer` | **si'**: `commit()` deve RITORNARE su `{success:false, reason:'kv_precondition'}` invece di sollevare | integrazione piu' unit contro plan server | 4,5 gg |
| HTTP (nessun SDK) | tutte le rotte | tutte le rotte | corpo grezzo | no | script eseguito in CI | 0,5 gg |
| `client-py` | idem JS | idem | idem | stesso ramo di precondizione | integrazione | 3 gg |
| `client-go` | idem, piu' `KVGetAs[T]` come **unica** funzione generica del package | idem, con `Expiry` a valore zero non valido | array top-level nella request, `Operation` **non** cambia | **si'**: percorso raw in `http_client` (vedi sotto) | integrazione | 5 gg |
| `client-rust` | idem, tipizzato | idem | idem | ramo di precondizione tipizzato | integrazione | 4 gg |
| `client-laravel` (php) | 7 op, niente `once` in v1 | schedule e cancel | scritture in tx | ramo di precondizione | integrazione ridotta | 2,5 gg |
| `client-cpp` | get, put, putIfAbsent, delete, incr | schedule, cancel | scritture in tx | ramo di precondizione | integrazione ridotta | 3 gg |
| `client-cli` | **decisione aperta** (§20.5) | idem | non applicabile | dipende | dipende | 1,5 gg se si' |

**Ordine consigliato:** HTTP e JS insieme (l'HTTP e' l'unico che mostra il wire e valida la forma JSON contro un broker vero senza SDK in mezzo, e conviene scriverlo **per primo** come specifica eseguibile della SP), poi Python e Go, poi Rust, poi PHP e C++, poi la CLI se decisa. Circa il 55% del lavoro JS si puo' fare **senza broker** contro un plan server scriptato, e conviene farlo prima che la SP esista, per pinnare il contratto.

### 10.3 Le tre cose che i client rendono obbligatorie lato broker

1. **`txn_fail_body` con `reason`, `failedIndex`, `kvReason`, `version`, `value`** (§8.3). Senza, ogni client dovrebbe fare string matching sul messaggio d'errore, vietato ovunque in questa base di codice, e l'idioma centrale della feature non e' scrivibile.
2. **La regola sugli status code** (§8.1): senza il 200 su `applied:false`, l'esito piu' frequente del prodotto finirebbe nella retry policy e nelle metriche d'errore di sette client.
3. **Il `value` del vincitore nel DETAIL del `RAISE`** (§6.1 punto 5): senza, chi perde fa un round trip in piu' sul percorso piu' caldo.

### 10.4 Insidie per linguaggio, quelle che mordono

- **Go: percorso raw obbligatorio, ed e' il gate della feature nel client.** `attemptOnce` chiude con `json.Unmarshal` in `map[string]interface{}`, quindi **ogni numero e' un `float64`**: oggi non esiste un percorso in cui un intero a 64 bit sopravviva, e `version` e' un BIGINT mentre `incr` gira su `numeric`. Si sposta l'unmarshal in fondo (`attemptOnceRaw` che ritorna `[]byte`, `parseBody` che riproduce **esattamente** la semantica attuale, `GetRaw`/`PostRaw` usati solo da kv e timers, `json.Decoder` con `UseNumber()` nei soli decoder KV). **Non si mette `UseNumber()` in `parseBody`**: il parsing dei messaggi fa `msgMap["retryCount"].(float64)` e un `UseNumber` globale rompe quelle asserzioni in silenzio, con il campo che va a zero e nessun errore.
- **Go: `Operation` non si allarga.** Due campi Go con la stessa chiave JSON allo stesso livello vengono **scartati entrambi** da `encoding/json`, senza errore e senza warning: il body partirebbe con zero op KV, il broker committerebbe una transazione senza gate, e il `putIfAbsent` non sarebbe mai esistito. Quindi gli array kv e timers sono **campi top-level della request**, non elementi di `operations`, e la mappa verso lo spazio piatto la fa il server (§8.2).
- **JavaScript: le durate.** La convenzione dichiarata del client e' "numero con l'unita' nel nome" e non esiste nessun parser di durate. `{ ttl: '24h' }` sarebbe la prima durata stringata dell'SDK. Decisione: **`ttlSeconds` canonico piu' `ttl` come alias**, con il parser confinato in un solo file e **non** esportato dal barrel, cosi' non diventa un'utilita' generale.
- **JavaScript: gli oggetti sono sempre truthy.** `if (await kv.delete(ns, key))` e' sempre vero, per tutte e cinque le scritture. Nessuna difesa strutturale senza tipi: documentazione in grassetto, esempi, e una regola eslint nel repo di esempio.
- **JavaScript: `pop()` ingoia ogni errore e ritorna `[]`** per contratto dichiarato. E' il motivo per cui il rider, quando arrivera', dovra' avere il suo terminale non ingoiante e non appendersi a `pop()`. E c'e' un metodo di costruzione dei parametri di pop **morto** nel file: chi aggiungera' i parametri kv guardando il nome del metodo li mettera' nel posto sbagliato, la pop funzionera' e il rider non arrivera' mai, con sintomo "cold start" invece di "bug". Va cancellato nella stessa patch.
- **Tutti: `cleanupTestData`.** Obbligatorio, non cosmetico: senza purga, un test `putIfAbsent` e' verde alla prima esecuzione e rosso per sempre dopo, e un test `incr` accumula fra le run. Serve una delete dei namespace e delle code di test, ciascuna nel suo try/catch come le esistenti, perche' lo schema puo' non esistere. E negli esempi eseguiti in CI **`forever` e' vietato**: un test che sbaglia lascerebbe stato immortale in un database di test condiviso.

### 10.5 Lo streams SDK, che e' una superficie a se'

Quattro dei sette client hanno **anche** uno streams SDK: `client-js` (`client-v2/streams/`), `client-go` (`streams/`), `client-rust` (`src/streams/`), `client-py` (`queen/streams/`). La matrice di §10.2 progetta la superficie **piana** di tutti e sette e non li nomina. E' una lacuna, perche' per quei quattro la domanda "come uso kv e timer dentro un operatore" ha una risposta non ovvia, e chi non la trova scritta se ne inventera' una.

**La regola d'ordine, da scrivere prima di ogni altra cosa: dentro uno stream la primitiva di stato resta `state_ops`, non il KV.** Le `state_ops` di `log_streams_cycle_v1` (`007_log_streams.sql`) committano stato, sink e ack **nella stessa transazione**, e quell'atomicita' e' gratis. Il KV e' piu' nuovo e piu' visibile, quindi verra' scelto per default se la documentazione non dice il contrario, e chi lo sceglie perde senza accorgersene l'unica garanzia che lo streaming gli dava gia'. Va in `use/streams.mdx` **prima** che in `use/kv.mdx`: e' la ragione precisa per cui §11.3 elenca quella pagina fra quelle che diventano incomplete.

Dentro uno stream il KV serve percio' a una cosa sola, ed e' quella che `state_ops` non puo' fare: **stato che attraversa partizioni o query.** `queen_streams.state` e' `(query_id, partition_id, key)` (`002_streams_schema.sql:58-65`), quindi un marcatore su un id di business che cade su partizioni diverse, o una quota condivisa fra tutte le partizioni di una query, non sono esprimibili.

**Gli operatori hanno gia' un contesto, e questo ribalta il costo rispetto al consumatore piano.** Dove `MessageHandler` di Go non ha contesto e da li' §10.1 ricava il taglio del rider, gli operatori uno un contesto ce l'hanno gia':

| client | operatori con contesto | estendibile senza rompere firme |
|---|---|---|
| `client-go` | `MapFn`, `FilterFn`, `FlatMapFn`, `ForeachFn` prendono `ctx EmitCtx`; `GateFn` prende `ctx GateContext` (`streams/operators/operators.go:44-66`, `:243`) | **tutti** |
| `client-rust` | `ForeachFn = Arc<dyn Fn(Value, EmitCtx)>` (`src/streams/ops.rs:123`), `GateFn = Arc<dyn Fn(&Record, &mut GateCtx<'_>) -> bool>` (`:114`); `MapFn`, `FilterFn`, `FlatMapFn` prendono solo `&Record` (`:108-110`) | **`foreach` e i gate** |
| `client-js` | dinamico | tutti |
| `client-py` | dinamico | tutti |

Il limite di Rust **non morde**, e la ragione va scritta perche' e' di disegno e non di linguaggio: gli operatori che hanno bisogno del KV sono esattamente quelli che il contesto ce l'hanno gia'. `foreach` e' dove stanno gli effetti, i gate sono dove sta il rate limiting. Dare uno store con effetti collaterali a una `map` dichiarata pura sarebbe un difetto, non una feature mancante: **`map`, `filter` e `flatMap` non ricevono `kv` in nessun linguaggio**, compresi i due in cui sarebbe tecnicamente possibile. La simmetria fra i quattro client e' quindi una scelta, non un minimo comune denominatore, e come tale va scritta.

**Cosa si aggiunge in v1:** `kv` su `EmitCtx` e su `GateCtx`, le sette operazioni, **dichiarato NON atomico con il ciclo**. Sono chiamate ordinarie fatte da dentro un operatore, con le stesse garanzie del KV standalone e nessuna in piu'. La frase esatta va nella docstring della firma e non solo nella pagina: *una scrittura KV da un operatore non fa parte della transazione del ciclo. Se il ciclo fallisce e riparte, la scrittura e' gia' avvenuta.* Chi ha bisogno di atomicita' con l'ack usa `state_ops`, che e' il motivo per cui la regola d'ordine sta in cima a questa sezione.

---

## 11. Documentazione e webdoc

### 11.1 Il vincolo che morde per primo, con il numero vero

`webdoc/dist/llms-brief.txt` misura **30001 byte** contro un budget hard di **30720** (`check-brief.mjs:29`, `BUDGET_BYTES = 30 * 1024`), quindi il margine reale e' **719 byte**, non i circa 1985 che il piano di documentazione riportava. Un `digest:` nuovo costa circa 970 byte. **Due digest nuovi non ci stanno, e nemmeno uno.**

E il conto va peggio, perche' il piano richiede anche di **aggiornare** i digest di almeno tre pagine con contenuto nuovo (due superfici in piu', i non-goal nuovi), incrementi che nessuno aveva contato.

**Decisione:** prima si taglia, poi si scrive. Il digest di `reference/limits.mdx` e' il piu' grande del sito a circa 1800 byte e va comunque riscritto per i non-goal nuovi: si riscrive **piu' corto** di almeno 500 byte. Il margine si **rimisura dopo la fase A** (che tocca solo pagine senza digest) e solo allora si decide se i due digest nuovi entrano o se ne entra uno solo. `pnpm verify` e' una catena di **cinque** gate (`gen:check`, `build`, `check:markdown`, `check:brief`, `lint:docs`, `check:prose`), non quattro, e quello che mordera' per sorpresa e' `check:brief`.

### 11.2 Pagine nuove

| percorso | tier | contenuto in una riga |
|---|---|---|
| `use/timers.mdx` | concetto, blocca il rilascio | perche' un timer non puo' vivere nel log, cosa lo rende cancellabile, le tre semantiche (`deliverAt` non prima di, ordine deciso al fuoco, `too_late` e' un verdetto), e il contrasto con `delayedProcessing` |
| `use/kv.mdx` | concetto, blocca | sette operazioni, scadenza obbligatoria, l'unica cosa che un KV a lato non puo' dare, dove il read-modify-write resta sicuro, e la disambiguazione da `queen_streams.state` |
| `reference/http/kv.mdx` | reference, blocca | le cinque rotte, la tassonomia chiusa di `reason`, e la regola sugli status code |
| `reference/http/timers.mdx` | reference, blocca | schedule e cancel, la tassonomia chiusa di `status`, **il contratto su `absent`** (§4.4), la nota di metering |
| `internals/timers.mdx` | internals, blocca | `queen.log_timers` colonna per colonna, keyed by names con la ragione forte, `visible_at` col suo prezzo dichiarato, le due colonne di lease |
| `internals/kv.mdx` | internals, blocca | `queen.kv` colonna per colonna, `COLLATE "C"` load-bearing, la `version` da sequenza e **non monotona**, `kv_live_v1` come unica definizione di esistenza |
| `internals/sweeper.mdx` | internals, blocca | un componente due orologi, leaderless al contrario della retention, `SKIP LOCKED` ovunque, shard che spalmano e non partizionano, la sveglia locale col tetto di sonno come rete |
| `deploy/state.mdx` | operatore, blocca la **cella condivisa** | i **tre** livelli di interruttore e cosa risponde ognuno (503 dal kill switch di runtime, 403 dalla quota, 403 dal piano), **perche' non c'e' un quarto livello di boot** con la distinzione gate contro kill switch di §12.1, le tre difese dell'endpoint, l'ordine di rilascio, e la condizione verificabile per la cella condivisa |

Nel farlo si correggono le collisioni di `order` preesistenti in `deploy/` e in `internals/`, e si rinumerano le due pagine di `use/` che devono stare accanto a `streams`.

**Nessuna pagina in `benchmarks/`.** Quattro numeri di questo piano sono dichiarati non misurati (§18.6). La regola del sito e' che ogni numero porta le sue condizioni: una pagina di benchmark senza campagna sul rig sarebbe la prima pagina disonesta del sito. Le due pagine internals **devono dire** che quei numeri non sono misurati, con la stessa formula dei blocchi datati di `001_log_schema.sql`.

**`status: preview` ha perso l'alibi che lo giustificava.** La regola era "preview finche' gli interruttori sono `false` di default", cioe' finche' la superficie descritta era spenta quasi ovunque, ed era l'unico modo onesto di pubblicare una superficie spenta. Con i flag via (§0) la superficie e' viva su ogni cella che gira il binario: o le pagine escono senza `preview`, oppure `preview` va motivato con l'altra ragione, l'unica ancora vera, cioe' che la forma puo' ancora cambiare prima del GA (§20.6 e §20.7 sono aperte e toccano il wire dei timer e una tassonomia pubblica). Va deciso esplicitamente, non ereditato dalla riga precedente. **E la conseguenza pesa piu' del campo:** F7 smette di essere una fase che insegue il rilascio e diventa bloccante per esso, perche' non si spedisce piu' una superficie viva e non documentata.

### 11.3 Pagine esistenti: prima quelle che diventano FALSE

| pagina | cosa cambia |
|---|---|
| `internals/schema.mdx`, sezione sull'ordine di lock globale | **L'edit piu' load-bearing del sito.** Oggi elenca tre regole e conclude che due spazi non possono formare un ciclo. Con sei spazi la frase e' incompleta e la conclusione non e' piu' dimostrata dal testo. Va riscritta con l'ordine di §2.1, la regola in una riga di §2.2 **con la distinzione fra acquisire e ri-toccare**, e il residuo onesto: un fuoco che attende un lock di partizione tenendo lock timer fa **aspettare** una cancel, non la fa fallire |
| `internals/schema.mdx`, tabelle e procedure | le due tabelle nuove, le due di quota e uso, e le due sottosezioni di procedura |
| `internals/storage-model.mdx` | la rivendicazione "zero indici secondari" va **scoped**, non cancellata: resta vera per `log_segments`, e le due tabelle nuove ne portano uno ciascuna, con il lettore e con l'elenco di quelli deliberatamente non creati |
| `use/index.mdx` | "la superficie e' quattro operazioni" diventa falsa, nella prosa **e** nel digest |
| `use/model.mdx` | il bundle non e' piu' solo push e ack |
| `reference/http/transaction.mdx` | i due array, gli step 0 e 0b, `required` come escalation opt-in, la forma `200 {"ok":false,...}`, e la sezione "non e' exactly-once end to end" che resta vera per gli effetti esterni e va completata nominando l'unico caso in cui **lo e'** |
| `reference/errors.mdx` | il `required` perso e' un `RAISE` 23514 tradotto in **HTTP 200**: e' l'unica eccezione alla regola che la pagina enuncia, e va scritta li' o la pagina e' sbagliata. Piu' i codici nuovi, e **una sola** risposta per superficie in pausa: 503 con `Retry-After` sulle rotte, rifiuto permanente dentro il wire. La distinzione fra 404 da env e 503 da runtime **non esiste piu'**, perche' il livello env non esiste piu' (§0), e la pagina non deve reintrodurla nemmeno come nota storica |
| `reference/multi-tenant/quotas.mdx` | `retainedBytes` non vede le due tabelle nuove: o si ridefinisce, o la pagina deve dire che le esclude. Piu' le righe di piano e **la formula dello sforo vera** di §9.3 |
| `reference/limits.mdx` | i confini nuovi, il contrasto con `queen_streams.state` che non ha scadenza, e il **taglio del digest** |
| `reference/queue-options.mdx` | paragrafo obbligatorio che distingue `delayedProcessing` (ritardo di visibilita' per coda, per ogni messaggio, non cancellabile) da un timer (per messaggio, cancellabile, riprogrammabile). Senza, meta' dei lettori usa quello sbagliato |
| `reference/http/index.mdx` | correttezza vera, non cosmesi: `putIfAbsent` e `delete` sono idempotenti, `put` lo e' solo con `expect`, **`incr` non lo e'** e un retry cieco conta due volte a meno che la transazione che lo conteneva sia fallita |

Poi quelle che diventano **incomplete** e bloccano comunque: `use/streams.mdx` (**tre** cose, non due: le due chiamate "state" con la regola d'ordine di §10.5 che mette `state_ops` per primo, la scelta SDK di chiamare l'handle `kv` e non `state`, e la conseguenza di §18.9 sul pacing, che non e' esprimibile dentro un operatore e va scritta prima che qualcuno ci provi), `reference/index.mdx`, `reference/defaults.mdx`, `reference/config.mdx`, `reference/prometheus.mdx` (le due gauge d'allarme e la regola di cardinalita'), `internals/index.mdx`, `internals/flow-control.mdx` (le letture e le scritture KV non prendono nessuna corsia e hanno il loro pool; il fuoco usa `Maint`), `internals/life-of-a-push.mdx`, `internals/dedup.mdx` (il `txn` fisso come rete **secondaria**, piu' la nota che il fuoco **non** sonda la finestra, §6.2), `internals/retention.mdx`, `internals/stats.mdx`, `deploy/operations.mdx` (con l'`<Aside type="danger">` sulla maintenance mode, §12), `deploy/postgres.mdx`, `deploy/ha.mdx`.

E `internals/contributing/docs.mdx`, che porta gia' sei affermazioni stantie confermate, piu' due che questo piano aggiunge: i gate sono cinque e non quattro, e il margine del brief e' quello vero.

Non bloccanti: le sei pagine SDK, `reference/queenctl.mdx` (che **deve dire esplicitamente** se la CLI non riceve kv e timers, o l'omissione si legge come dimenticanza), `reference/compatibility.mdx` (client nuovo contro broker vecchio uguale 400 nominativo, §8.2), `reference/engine.mdx` e `reference/embed.mdx`, il resto di `multi-tenant/`, `start/compare.mdx` (i messaggi schedulati e uno stato transazionale sono esattamente dove Queen si stacca).

### 11.4 Full example: due bloccanti, non sei

Il piano di documentazione originale chiedeva sei full example per sette linguaggi, cioe' **34 programmi nuovi** con asserzioni deterministiche, alcune delle quali richiedono di far scadere una lease apposta o di vincere una gara con lo sweeper. Per confronto, `examples/apps` contiene oggi tre applicazioni, di cui una in quattro linguaggi soltanto. Piu' che triplicare quella superficie non e' una fase, e' un progetto.

**Decisione: due bloccanti, in tre linguaggi, il resto additivo dopo il GA.**

| esempio | proprieta' che DEVE misurare | linguaggi v1 |
|---|---|---|
| `exactly-once` | N ordini, riconsegna forzata, esattamente N addebiti; il secondo passaggio vede `ran === false`; un handler che fallisce **prima** del commit non lascia marcatore e non blocca il retry | js, py, http |
| `saga` | gate piu' stato piu' timer piu' push piu' ack in un bundle; una riconsegna forzata produce **un solo** push, **un solo** timer, **una sola** riga di stato; il timer cancellato non arriva mai; **e il consumatore della compensazione verifica lo stato prima di compensare** (§4.4) | js, py, go |

Additivi dopo il GA: `quota-gate` (che duplica concettualmente un limiter gia' sul sito e va disambiguato, non aggiunto in fretta), `reminders`, `request-reply` (il piu' difficile da rendere deterministico), `business-dedup`.

Struttura invariata rispetto a quella esistente: frontmatter con `verifiedBy` che elenca ogni programma, tre o quattro paragrafi di prosa senza heading (la war story, la correzione strutturale, cosa costruisce e **cosa misura**, il confine onesto), un solo `<Tabs>` di `<Render file="snippets/...">`, niente codice scritto a mano nella pagina, e `## Run it`. Piu' una riga per esempio in `MUST_CARRY` di `check-markdown.mjs`, o la pagina non e' asserita.

Due regole da portare su ogni programma: la war story nomina un guasto di prodotto reale, mai una feature; e **ogni programma asserisce un CONTEGGIO, con una deadline, mai aspettando il silenzio**.

Infrastruttura: suffisso univoco per esecuzione sul nome di coda (gia' documentato, per lo stato di partizione stantio) **piu' un secondo suffisso sul namespace KV**, per la stessa ragione. E `.github/workflows/docs.yml` non copre `examples/**`, quindi un cambio di programma non fa scattare il drift check in CI: con programmi nuovi va aggiunto al path filter, non e' piu' una scelta fra due opzioni.

### 11.5 Generatori

| generatore | obbligatorio | cosa fare | trappola |
|---|---|---|---|
| `gen-routes.mjs` | **si'** | la funzione di livello d'accesso finisce con un fallthrough a read-write, quindi i `GET` kv verrebbero **pubblicati come read-write**: le regole di §8.1 vanno scritte prima del fallthrough. Piu' un gruppo nuovo, o le rotte finiscono in `Ungrouped` | il fingerprint dell'access va **ribattuto** dopo aver toccato `auth.rs`, o il gen fallisce |
| `gen-config.mjs` | **si'** | un gruppo nuovo per `QUEEN_KV_*`, `QUEEN_TIMERS_*`, `QUEEN_SWEEPER*`, o circa venticinque variabili atterrano sotto `Other` | il parser vede **solo** i call site di `env_*`: ogni valore **derivato** (pool KV, `REQUIRE_GRANT`, payload dei timer) va aggiunto a mano **con la derivazione scritta**, o la tabella pubblica un default falso |
| `gen-metrics.mjs` | gratis se si segue il pattern | nessun edit se le famiglie sono dichiarate come le altre | una famiglia con help non parsabile **non viene scartata: viene pubblicata con la cella vuota**, quindi il guasto e' silenzioso |
| `gen-openapi.mjs` | **si', strutturalmente** | non e' un edit del generatore, e' un vincolo **sul codice Rust**: un handler che costruisce JSON per concatenazione esce come schema opaco. Se si vuole uno schema derivato, gli handler kv e timers devono avere struct serde vere per il body e un `Query<T>` per i parametri | il catch-all `*key` va dichiarato come path param o il gen fallisce |
| `gen-snippets.mjs` | nessun edit | `examples/apps` e' gia' fra le sorgenti: bastano i marker | id duplicato o regione non chiusa e' un errore hard, e la cartella dei partial viene cancellata e riscritta a ogni run |
| `gen-proxy-routes.mjs` | si', ma **si chiede** | rispecchia `routes.rs`, che non si tocca (§9.8); due fingerprint da ribattere | finche' non e' fatto, la fase 0 e' comunque spedibile e le pagine devono dire che le rotte sono bloccate dal proxy per costruzione |

Regole editoriali che valgono per ogni pagina nuova: niente em dash nella prosa (fallimento hard di `check-prose`; i commenti nel sorgente sono fuori scope, quindi gli header SQL possono tenerle), H1 solo dal `title`, `description` obbligatoria, i fatti sono **generati** e la prosa e' scritta a mano (nessuna tabella di env var, metriche o rotte scritta a mano), ogni fence ha un linguaggio, nessun prompt `$` nelle fence shell, componenti solo fra quelli gia' registrati, frontmatter strict. E **niente futuro e niente roadmap**: le cose non in v1 sono non-goal dichiarati in `reference/limits.mdx`, oppure non esistono. In particolare nessuna pagina puo' parlare del rider pop, di `merge`, di `deletePrefix` o delle ricorrenze come "in arrivo".

---

## 12. Modalita' di guasto (decisioni esplicite)

Ogni riga ha un comportamento gia' scelto, non un elenco di opzioni, e dove serve una soglia la soglia c'e'.

| guasto | comportamento | segnale, con soglia |
|---|---|---|
| il broker muore dopo la claim, prima del fuoco | le righe restano claimed; alla scadenza del lease `visible_at` rientra nella finestra e un altro broker le prende. Nessun duplicato: il fuoco non era mai committato. Ritardo massimo `QUEEN_SWEEPER_LEASE_MS` (30 s) | `queen_timers_oldest_late_seconds` |
| il broker muore dopo il commit del fuoco, prima di saperlo | niente da riparare e nessuno stato "fired" da riconciliare: e' l'intero motivo di DELETE invece di mark-done. "E' stato mandato?" risponde il log | nessuno |
| fuoco fallito in modo transiente (40001, 40P01, classe 08/53/57/58, nessun SQLSTATE) | nulla e' committato; `fail_v1` con `count_attempt = false` sposta `claimed_until` di un backoff corto. Se anche la fail fallisce, il lease scade e la riga torna disponibile. **Il budget di DLQ non si consuma mai per colpa dell'infrastruttura** | `queen_timers_fire_failures_total{class="transient"}` |
| coda di destinazione inesistente al fuoco | provisioning pigro dentro `log_push_one_v1`, esattamente come un push normale. **Non e' un errore** | nessuno |
| partizione cancellata dal cleanup fra schedule e fuoco | il timer conserva **i nomi**, quindi il fuoco ri-provisiona per nome. E' una delle ragioni del keyed-by-names, non un effetto collaterale | nessuno |
| coda cancellata mentre il fuoco e' in volo | il push fallisce su FK o con un errore nominato: classe 23 o P0001, quindi permanente, `attempts++`, e dopo N in DLQ. La DLQ **ri-provisiona coda e partizione** sotto il tenant del timer, o la riga sarebbe archiviata e introvabile | `queen_timers_dlq_total` |
| cancel su un timer claimed | `{"ok":false,"status":"too_late"}` con HTTP 200. Verdetto, non errore, e limitato dal lease | nessuno |
| cancel su un timer in **backoff** | **riesce**: `claim_token` e' NULL apposta durante il backoff, o un timer velenoso sarebbe incancellabile per minuti | nessuno |
| cancel su un timer **gia' consegnato** | `absent`, `ok:false`, con il `txn` atteso nella risposta. **Non significa "non e' stato consegnato"** (§4.4) | nessuno |
| reschedule su un timer claimed | stessa risposta `too_late`, **una regola sola**. Il rimedio e' una chiave nuova, oppure agire sul messaggio alla consegna | nessuno |
| due broker claimano la stessa riga | impossibile: `FOR UPDATE SKIP LOCKED` piu' il row lock del PK. Chi perde non aspetta, prende le righe dopo. E' anche il motivo per cui non serve nessun leader | `queen_sweeper_skip_locked_total` |
| fuoco che riparte con un claim scaduto | il confronto e' su `claim_token` **e** su `claimed_until > now()`: se un altro broker ha riclaimato, il segmento e' `stale` e viene buttato, **e le righe che questa transazione aveva bloccato vengono rilasciate** (§6.2 punto 3). Il token e' l'unica cosa che rende sicuro il lease | `queen_timers_fired_total{result="stale"}` |
| push che risponde `duplicate` | il `txn` fisso e' gia' nel log: quel timer e' gia' stato consegnato. Si cancellano i timer duplicati, non si pusha, e il resto del gruppo torna claimabile per essere ri-impacchettato senza di loro. Il gruppo avanza a ogni passata. **In v1 questo guasto non si osserva mai**: `p_verified = v_last` salta la sonda, quindi il timer con un `txn` gia' nel log viene **appeso una seconda volta** invece di essere riconosciuto (§6.2, §20.7). La riga descrive il ramo implementato, non un comportamento raggiungibile | `{result="duplicate"}`, **sempre zero in v1** |
| un timer velenoso dentro un batch da 200 | errore permanente su tutta la transazione; con `ISOLATE_ON_PERMANENT` il batch viene rilanciato un segmento per chiamata, 199 committano e uno prende `attempts++`. Senza isolamento quel batch fallirebbe **per sempre** e i 199 sani non partirebbero mai: e' il guasto piu' insidioso, perche' il sintomo non nomina il colpevole | `queen_timers_poisoned_total` |
| burst: un milione di timer che maturano insieme | nessun crollo, ma il ritardo cresce: la portata e' `claim_batch * cicli/s * broker * parallelismo` e `lateMs` sale linearmente finche' la coda non e' drenata. Con l'equita' per tenant di §6.2, un tenant non affama gli altri. Onesto e da scrivere in doc: **non e' uno scheduler ad alta frequenza, e' un temporizzatore durabile** | `fire_lag p95`, warn oltre 5 s |
| sweeper fermo (`QUEEN_SWEEPER=false`, fuoco in pausa dal kill switch, o tutte le repliche giu') | nessuna perdita: `deliverAt` e' "non prima di", e al ritorno si riparte dal piu' vecchio. Sotto `panic = "abort"` il caso "il task e' morto e il broker serve" non esiste, perche' un panic abbatte il processo e k8s riavvia | `queen_timers_due_backlog`, warn a 60 s continui, page a 300 s |
| **maintenance mode attiva** | i push vengono deviati sullo spool, ma **un fuoco non puo' andare sullo spool**: il fuoco e' DELETE piu' push nella **stessa** transazione, ed e' quell'atomicita' a dargli l'exactly-once. Quindi in maintenance mode **il fuoco si sospende**: i timer restano in tabella, il lag esplode, **niente si perde e niente si duplica**. Va scritto nella pagina della maintenance mode con un avviso di pericolo, o qualcuno lo scoprira' durante una manutenzione e lo leggera' come perdita di messaggi | `fire_lag` |
| sweeper indietro sulla potatura KV | le letture restano **perfettamente corrette** (la scadenza e' un predicato, non la presenza fisica) mentre la tabella cresce. E' un guasto di liveness che si traveste da correttezza, quello che nessuno nota fino al disco pieno | `queen_kv_expired_not_pruned`, **cappata** a N con report `>= N`, allarme sopra 50 000 righe **oppure** `kv_expiry_lag_seconds > 600` |
| dead tuple ratio sulle due tabelle | `scale_factor 0` e `threshold 500` non bastano: e' l'indicatore che precede il rallentamento della cella, e su queste due e' l'**unico**, perche' `vacuum_truncate = off` toglie il sintomo acuto e lascia solo quello lento | ratio > 5 da `pg_stat_user_tables`, che il prodotto legge gia' |
| KV oltre quota | 403 sulle scritture, **letture e delete sempre permesse** | `kv_quota_ratio > 0.8`, che con una quota morbida e' gia' tardi |
| KV oltre il tetto di valore | 413 al confine HTTP, 22001 nella SP, e i due misurano cose diverse (§9.2) | nessuno |
| tempesta di sveglie | non raggiungibile: il `hint` si applica solo quando il nuovo minimo e' anticipato, quindi un milione di timer per la settimana prossima produce **una** sveglia | nessuno |
| chiave KV contesa in un bundle | il bundle si serializza su di essa. **Non e' un bug, e' il rischio accettato §18.2**, e la difesa e' documentale: le chiavi di gate sono per messaggio, mai condivise | `kv_op_duration p99` |
| un fuoco attende un lock di partizione tenendo lock timer | succede, ed e' il residuo vero: per la durata dell'attesa una cancel su quei timer **aspetta**, non risponde `too_late`. Limitato dalla latenza del push e dal timeout con cancel server-side. Non e' un deadlock e non puo' diventarlo, perche' nessuno che tenga un lock di partizione attende mai un lock timer | conteggio `40P01` per SQLSTATE |
| re-apply dello schema al boot mentre lo sweeper gira | `schema.rs` prende l'advisory di sessione e ritenta cinque volte sui deadlock proprio perche' incrocia i cicli di fondo di una replica viva. 024 e 025 sono fatti solo di `CREATE ... IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, storage params senza rewrite e `CREATE OR REPLACE FUNCTION`; l'unico `DROP FUNCTION` e' quello della claim, che nomina la firma **corrente** ed e' la meta' di idempotenza al boot, non un passo di upgrade | nessuno |
| SIGTERM mentre il broker tiene claim | le richieste HTTP in volo drenano, il task viene abortito col runtime, i claim restano e quei timer partono fino a `LEASE_MS` piu' tardi. Non vale la pena aggiungere un rilascio ordinato: il costo e' un limite di 30 s su un deploy, contro un percorso di shutdown in piu' da mantenere | nessuno |
| rollup lento che non gira | le quote diventano stantie e le gauge si congelano. Non blocca ne' fuoco ne' schedule, perche' l'enforcer e' il delta locale (§9.3) | `computed_at` che invecchia |

### 12.1 La scala di degrado

Un ordine dichiarato, con il trigger misurabile e chi lo decide. **La proprieta' che deve reggere: la prima cosa che cede e' la cosa nuova, non il prodotto.**

| # | stadio | trigger | chi | effetto |
|---|---|---|---|---|
| 1 | 429 per tenant | token bucket vuoto | broker, e proxy se c'e' | il cliente sopra il suo tasso rallenta per primo, e sa che e' suo |
| 2 | 503 `kv_unavailable` | pool KV esaurito o in attesa | il pool | la cella e' lenta, non e' colpa del tenant: status diverso apposta |
| 3 | potatura KV sospesa | pressione sullo sweeper | sweeper | le letture restano **corrette**, cresce solo lo spazio. Si sacrifica per prima perche' e' l'unica fase che costa solo disco |
| 4 | rollup quote sospeso | idem | sweeper | le quote diventano piu' stantie. Si sacrifica per seconda: e' precisione, non consegna |
| 5 | scritture KV standalone rifiutate | pressione sostenuta | broker | le scritture KV **dentro il wire continuano**: la transazione e' il valore del prodotto, la POST e' la comodita' |
| 6 | 403 quota | occupazione oltre il cap | broker | scritture bloccate, letture e delete permesse |
| 7 | kill switch KV di cella | operatore | `queen.system_state` | 503 su tutte le rotte KV, l'array `kv` nel wire rifiutato con errore **permanente**, cosi' i client non ritentano in loop |
| 8 | pausa dello **schedule** dei timer | operatore | `queen.system_state` | si smette di **accettare** timer nuovi, si continua a fare **fuoco** di quelli esistenti |

**Il fuoco dei timer non si spegne mai automaticamente.** Sotto pressione lo sweeper riduce il batch e allunga il sonno; il risultato visibile e' `fire_lag` che sale. Spegnerlo trasformerebbe un ritardo in cio' che il cliente legge come una perdita. Priorita' interna dello sweeper, dall'alto: **fuoco, poi rollup quote, poi potatura KV**, e si spegne dal fondo. I due kill switch dei timer sono **distinti** (`timers_schedule_enabled`, `timers_fire_enabled`) perche' le due meta' hanno costi opposti: fermare lo schedule e' innocuo e istantaneo, fermare il fuoco accumula lavoro promesso.

**Un solo livello di interruttore, ed e' il kill switch.** Vive in `queen.system_state`, sulla forma gia' usata da `maintenance_mode` e `pop_maintenance_mode`: flag in-process autorevole sul percorso caldo, riga in DB come specchio best-effort per propagazione e restart, rilettura fresca sul GET. Un incidente alle tre di notte non si risolve con un rollout. **Sopra non c'e' piu' niente:** il livello di boot e' stato tolto (§0), quindi il caso "la superficie non esiste su questa cella" non esiste, e nessuna rotta kv o timers risponde 404 per essere spenta. Una superficie in pausa risponde **503** con `Retry-After` sulle rotte, e l'array corrispondente nel wire viene rifiutato con errore **permanente**, perche' un bundle porta messaggi e ritentarlo in loop contro una cella deliberatamente in pausa e' una tempesta sul percorso caldo.

**La distinzione che regge tutto questo, e che va scritta in `deploy/state.mdx` e nell'header di `switches.rs`, non solo qui: un gate si accende per provare, un kill switch si spegne per fermare.** Il gate si legge una volta al boot, decide se la superficie **esiste**, cambiarlo e' un rollout, e ogni cella della flotta puo' legittimamente rispondere in modo diverso; e' la cosa che questo broker non ha piu'. Il kill switch si legge a ogni chiamata, la superficie esiste comunque, scatta subito su una cella che qualcuno ha in mano, e ci si aspetta che venga **riacceso**. Si somigliano e sono opposti, ed e' per questo che vanno nominati diversamente ovunque: chiamare "kill switch" un gate e' il modo in cui una feature spenta di default sopravvive alla decisione che l'aveva accesa.

Corollario di forma, che e' il punto in cui la distinzione diventa codice: **una riga assente vale ACCESO.** `db::get_system_flag` risponde `false` per una riga assente, perche' i suoi chiamanti chiedono "la maintenance e' ON?"; questi chiedono "la feature e' ancora ON?", quindi passano da `get_system_flag_opt`, dove `None` significa "nessuno ha mai toccato questo interruttore". Al contrario, ogni cella nuova nascerebbe con la feature morta e senza una riga che spieghi perche'.

**Nessun flag per coda.** Il KV non ha una coda, e i timer una coda di destinazione ce l'hanno ma un flag per coda sarebbe una colonna in `queen.queues`, e `/configure` **resetta le colonne** (difetto confermato 2026-08-05): il flag si spegnerebbe da solo alla prima riconfigurazione, in silenzio.

---

## 13. Sicurezza e isolamento per tenant

### 13.1 Il modello, e perche' non serve il gate degli ack

Il gate degli ack esiste (`005_log_ack.sql:930-941`) perche' il percorso ack e' **pid-keyed**: un id opaco puo' trapelare, quindi serve una pre-passata che risolva `partitionId` verso il tenant e sollevi prima di qualunque lavoro. Per kv e timers la situazione e' strutturalmente diversa e va detto, altrimenti qualcuno copia un gate che non serve o, peggio, conclude che non serve niente.

`queen.kv` ha PK `(tenant_id, namespace, key)` e `queen.log_timers` ha PK `(tenant_id, queue, timer_key)`. Non esiste nessun identificatore opaco che il chiamante possa presentare: presenta solo nomi che sceglie lui. **Il tenant non e' un filtro applicato a un id ricevuto, e' parte della chiave.**

| proprieta' | come e' garantita | come si rompe |
|---|---|---|
| non leggibile | ogni `SELECT` porta `tenant_id = p_tenant`, e `p_tenant` e' un **argomento della funzione**, mai un campo dell'op | un futuro lettore HTTP che costruisce SQL contro `queen.kv` fuori dalle SP. Con `GRANT ... TO PUBLIC` e zero RLS, l'isolamento e' **esattamente una clausola WHERE dentro le SP**. Il test grep di §15 lo rende meccanico |
| non scrivibile | il target di `ON CONFLICT` e' la PK **completa**, quindi un conflitto non puo' mai essere cross-tenant | un indice unico su `(namespace, key)` "per efficienza" |
| non rivelabile | `putIfAbsent` sulla chiave di un altro tenant crea la **mia** riga e non rivela niente; una cancel di un timer altrui risponde `absent`, indistinguibile da inesistente | il canale laterale di §13.3 |

### 13.2 I tre punti dove l'isolamento e' facile da rompere

1. **Il raggruppamento del fuoco per `(queue, partition)` senza tenant** (§6.2 punto 8). E' facilissimo da scrivere, non produce nessun errore, e fonde i timer di due tenant in un segmento. Test di regressione a due tenant come criterio di merge.
2. **`queen.log_dlq` non ha colonna tenant**: lo scoping passa solo dal `partition_id`, quindi la partizione va risolta sotto il tenant del timer. Il consumer group sintetico `__timer__` va **rifiutato dal percorso di replay** (§4.5).
3. **L'header di tenant inoltrato dal proxy** quando il flag di invio e' spento (§9.9).

E il precedente che rende tutto questo non teorico: l'audit proxy ha gia' trovato due buchi di isolamento vivi in produzione, una join di retention per nome e delle traces aperte al proxy. Questa feature aggiunge la prima superficie in cui l'identita' e' **solo** un nome scelto dall'utente.

**Contro il punto 3 la difesa strutturale e' al boot, e' fatale, ed e' incondizionata.** `QUEEN_TENANCY_HEADER=1` senza `QUEEN_KV_TRUSTED_PROXY=1` non parte (§9.4 punto 4). Il requisito e' keyed sulla **sola modalita' di tenancy**: prima era in coppia con `QUEEN_KV_ENABLED=1`, ma quel flag non esiste piu' (§0) e la coppia nascondeva comunque la cosa vera. **La cosa insicura non e' il KV: e' un'identita' di tenant opaca che nessuno valida** (`server/src/tenant.rs:16-19`). Il KV l'ha solo resa visibile, essendo la prima superficie indirizzabile puramente per nome, senza nessun id opaco e quindi senza nessun gate di ownership da superare. Detto al rovescio, perche' non si legga come una tassa imposta dal KV: chi gira con l'header e senza un proxy che lo scriva ha **gia' oggi** un problema di isolamento, e questa feature lo rende sfruttabile con una GET. Per questo l'interlock non ha una terza uscita: non c'e' piu' un flag da spegnere per rendere sicura la combinazione, e l'unica risposta corretta e' non girare con l'header, oppure metterci davanti il proxy.

### 13.3 Il canale laterale, dichiarato e accettato

`queen.kv_version_seq` e' **globale**: due scritture dello stesso tenant che ottengono 90101 e 90140 rivelano che nel frattempo sono avvenute 38 scritture di qualcun altro. E' un canale reale, di bassa severita', e le contromisure ovvie sono peggiori del male: una sequenza per tenant e' una riga calda per tenant sul percorso di scrittura, cioe' esattamente la cliff di §2.4 D7.

**Correzione di un argomento sbagliato che circolava:** `CACHE 1000` **non** lo chiude. L'idea era che ogni backend prelevi un blocco e che le versioni consecutive dello stesso tenant sulla stessa connessione differiscano di uno. Non segue: il pool consegna la connessione al prossimo che la chiede, di qualunque tenant, quindi due scritture dello stesso tenant raramente cadono sullo stesso backend, e quando ci cadono un altro tenant puo' aver consumato dallo stesso blocco in mezzo. Il canale diventa piu' **grossolano**, non chiuso. `CACHE` alto resta, ma per la ragione giusta (un WAL record per `nextval` sul percorso caldo), e va commentato cosi', perche' il commento sbagliato dice al prossimo lettore di non preoccuparsi.

Cosa si fa invece: **`GRANT USAGE` e non `SELECT`** sulla sequenza, altrimenti `SELECT last_value` trasforma il canale in un contatore globale di scritture leggibile da chiunque abbia una connessione.

### 13.4 Payload, cifratura, e cosa sta in chiaro

La cifratura per frame avviene al **push handler**, prima del packing, e la chiave e' di processo. Quindi la cifratura del payload di un timer avviene allo **schedule**, non al fuoco, e il payload non sta in chiaro in `queen.log_timers`. Conseguenza da dichiarare e non scoprire: **una coda la cui cifratura viene abilitata dopo che un timer e' stato schedulato consegna quel frame in chiaro.** Se la chiave ruota fra schedule e fuoco, il frame diventa indecifrabile: e' il prezzo di cifrare presto, ed e' preferibile all'alternativa (payload in chiaro a riposo per giorni).

**`queen.kv` e' in chiaro in Postgres.** Va scritto, non dedotto. Non c'e' cifratura per valore in v1 e non e' un non-goal per timidezza: la chiave e' di processo, e cifrare uno store che si legge per prefisso e si confronta per uguaglianza toglierebbe entrambe le cose.

### 13.5 Igiene degli errori e dei log

- Il `MESSAGE` del `RAISE` di `required` e' **opaco**; namespace e chiave stanno solo nel `DETAIL`, che il broker legge programmaticamente. Gli handler rigirano i messaggi DB al client, e quei nomi finirebbero nei log condivisi e negli aggregatori di errori.
- **Nessun prefisso nella query string** (§5.5).
- Le rotte KV sono escluse dal logging della query string comunque, come difesa in profondita', ma la difesa vera e' che non c'e' niente di sensibile da mettere in una query string.

---

## 14. Osservabilita'

### 14.1 Regola di cardinalita', che questa feature e' la prima a rischiare davvero

> La label `tenant` e' **ammessa solo sulle gauge di occupazione** (una serie per tenant, scritta dallo sweeper, cardinalita' uguale al numero di cluster sulla cella, limitata dal control plane). E' **vietata sui contatori per operazione**, dove sarebbe tenant per op per esito, cioe' cardinalita' decisa dall'utente: la stessa malattia dell'endpoint che stiamo difendendo. Il per tenant sul percorso caldo vive nella riga di log top-N e nell'endpoint JSON, non in Prometheus.

Con una eccezione motivata: **`queen_timers_fire_lag_seconds` porta la label `tenant`**, perche' e' una gauge di occupazione e perche' senza di essa un backlog causato da un tenant non nomina il colpevole (§6.2).

### 14.2 Le serie

```
queen_kv_ops_total{op,result}                     counter   result = applied|rejected|error
queen_kv_op_duration_milliseconds{op,quantile}    gauge
queen_kv_bytes_total{dir}                         counter
queen_kv_rows{tenant}                             gauge     dallo sweeper
queen_kv_bytes{tenant}                            gauge     STIMA, etichettata tale
queen_kv_quota_ratio{tenant,kind}                 gauge
queen_kv_expired_not_pruned                       gauge     <-- ALLARME, cappata
queen_kv_expiry_lag_seconds                       gauge
queen_kv_read_rejected_total{reason}              counter   rate_limited|quota|pool|disabled
queen_kv_pool{state}                              gauge     size|available|waiting
queen_kv_singleflight_coalesced_total             counter

queen_timers_pending{tenant}                      gauge     dal rollup lento, su TUTTA la tabella
queen_timers_due                                  gauge     dalla sonda, CAPPATA
queen_timers_due_capped                           gauge     0/1
queen_timers_oldest_late_seconds                  gauge     <-- ALLARME
queen_timers_fire_lag_seconds{tenant,quantile}    gauge
queen_timers_fired_total{result}                  counter   fired|duplicate|stale
                                                            duplicate SEMPRE ZERO in v1 (§6.2, §20.7)
queen_timers_dlq_total                            counter
queen_timers_fire_failures_total{class}           counter   transient|permanent|config
queen_timers_poisoned_total                       counter
queen_timers_schedule_rejected_total{reason}      counter

queen_sweeper_cycle_milliseconds{phase,quantile}  gauge     phase = fire|kv_expire|usage
queen_sweeper_rows_total{phase}                   counter
queen_sweeper_skip_locked_total                   counter
queen_sweeper_phase_skipped_total{phase}          counter   la scala di degrado, resa visibile
queen_sweeper_sleep_milliseconds                  gauge
```

Il pattern di accumulo e' quello gia' in casa: atomici cumulativi `sum` e `count` piu' un `max` drenato con `swap(0)` a ogni flush, e un ring per gli RTT del ciclo che da' p50 e p99 gratis.

### 14.3 I sei segnali che precedono l'incidente, ordinati per quanto presto parlano

1. **`queen_kv_read_rejected_total{reason="rate_limited"}` diverso da zero su un tenant che prima era a zero.** Non e' un guasto: e' il **preavviso dell'unico guasto nuovo** che questa feature introduce, cioe' un cliente che ha appena messo le letture KV sul percorso dei suoi utenti finali. Deve essere un pannello con la **lista dei tenant**, non un totale: il totale non dice a chi telefonare. Allarme informativo alla prima occorrenza, non alla decima.
2. **`queen_timers_due_backlog` sostenuto.** Warn a 60 s continui, page a 300 s. E' il primo segnale che conta come guasto: un backlog di timer e' lavoro promesso non consegnato, e il cliente lo vede come "il messaggio non e' arrivato", cioe' come una perdita.
3. **`queen_timers_fire_lag_seconds` p95 sopra 5 s.** Piu' sensibile del backlog quando questo oscilla attorno a zero, e con la label tenant nomina il colpevole.
4. **`queen_kv_expired_not_pruned` sopra soglia**, 50 000 righe **oppure** `expiry_lag > 600 s`, il primo dei due. **Non e' ridondante con la 3:** e' l'unico segnale che distingue "sweeper indietro" da "tutto bene" in una modalita' di guasto che si traveste da successo, perche' le letture restano perfettamente corrette mentre la tabella cresce. E con lo sweeper condiviso, uno sciame di timer velenosi che ritentano fa slittare la potatura: e' esattamente lo scenario in cui questa gauge e' l'unico segnale.
5. **`queen_kv_quota_ratio > 0.8` per tenant.** Con una quota morbida l'80% e' gia' tardi. Il preavviso al cliente riusa il meccanismo di annuncio quota che il proxy **ha gia'**, con due tipi di evento nuovi: zero meccanismo nuovo, zero pannello nuovo per il cliente.
6. **Dead tuple ratio sopra 5** su `queen.kv` e `queen.log_timers`, da `pg_stat_user_tables`, che il prodotto legge gia'.

E la discrepanza fra `queen_timers_pending` (dal rollup, su **tutta** la tabella) e `queen_timers_due` (dalla sonda, sugli shard scanditi) e' il modo in cui si vede un errore di configurazione dello sweeper.

### 14.4 Righe di log

**Nessun target periodico nuovo.** Un terzo blocco e' una riga che nessuno guarda; `rates` e `sizes` sono gia' quelli che si guardano in un incidente. Quindi: campi nuovi in `rates` (`kv_ops_s`, `kv_p99_ms`, `kv_rej_s`, `timers_fire_s`, `timers_backlog`, `fire_lag_p95`) e in `sizes` (`kv=<N>rows/<M>MB`, `kv_unpruned=<N>`, `timers=<N>pending`, `kv_pool=<n>/<max>`), piu' righe top-N per tenant sul modello di quelle per coda gia' esistenti.

**Un target `sweeper` nuovo, ma solo on-change**, esattamente come la transizione di ingresso e uscita dalla modalita' buffered dello spool, che e' il precedente della casa per "il segnale davvero importante si logga quando cambia, non ogni intervallo": ingresso e uscita da ciascuno stadio della scala di degrado, timer velenoso mandato in DLQ, e apertura o chiusura di qualunque breaker.

Rate **e** dimensioni, aggregati a finestra, mai per operazione e mai per timer: e' la regola di logging della casa e vale interamente qui.

### 14.5 Piano cluster

Un blocco `timers` e un blocco `kv` in `queen.get_prometheus_metrics_v1` (`023_prometheus.sql`), che leggono **`queen.kv_usage`** e non contano le tabelle: il rollup e' gia' su cadenza lenta, e un `count(*)` dentro l'endpoint Prometheus lo farebbe girare a ogni scrape.

---

## 15. Piano di test (agganciato alle fasi)

Ogni riga chiude una fase, e la fase non e' finita finche' la riga non e' verde.

| tipo | contenuto | chiude |
|---|---|---|
| **Unit SQL** | i sei helper puri, con i casi limite: `kv_prefix_end_v1` su prefisso vuoto, su U+D7FF (il surrogato che farebbe sollevare `chr()`), sull'ultimo code point; `kv_ver_v1` sull'assente e sullo scaduto; `kv_num_v1` sul non numerico | F1 |
| **Idempotenza di boot** | applicare `schema.rs` **due volte** su un database vergine e una volta su uno gia' popolato. Il secondo boot deve passare: e' il test che cattura un `DROP FUNCTION` scritto male (che al secondo boot da' `42723` e bricka il processo) e un `ADD COLUMN` dimenticato | F1 |
| **Verifica `42P22`** | un confronto colonna `COLLATE "C"` contro parametro `text` in ogni forma usata dalle SP, sul rig. Se solleva, ogni confronto va scritto con la collation esplicita (§2.4 C3) | F1 |
| **Unit Rust** | il classificatore SQLSTATE estratto (le tre classi, l'assenza di SQLSTATE), il calcolo del sonno con clamp e backoff, `pack_segment`, e il test che pinna che il testo SQL del fuoco **non** contiene `log_push_multi_v1` | F3 |
| **Concorrenza sull'ordine di lock** | N bundle concorrenti su un insieme di chiavi kv, timer e partizioni **deliberatamente incrociato**, e **fallisce su qualunque `40P01`**. E' l'unica cosa che tiene in piedi la dimostrazione di §2, che oggi vive solo in un header, e cinque dei sei cicli di §2.4 sono l'implementazione **ovvia** di ciascun pezzo. Con una nota nel test che dice che il suo scopo e' l'ordine di lock, cosi' chi lo trova lento non lo cancella. **Criterio di merge, non raccomandazione** | F4 |
| **Isolamento fra tenant** | due tenant, **stessi nomi di coda, stessi `timer_key`, stessi namespace KV**: nessuna riga vista, nessuna scritta, nessun segmento fuso al fuoco, nessuna riga DLQ sulla partizione sbagliata. **Criterio di merge** | F3, F4 |
| **Grep meccanico** | nessun file in `server/src/handlers/` nomina `queen.kv` o `queen.log_timers` fuori dai wrapper in `db.rs`. Dieci righe, e rende meccanica l'unica garanzia di isolamento che oggi e' documentale (§13.1) | F2 |
| **Fault injection sui timer** | broker ucciso dopo la claim e prima del fuoco; lease scaduto e riclaim; cancel in corsa col claim; segmento `stale` che deve **rilasciare** le righe che aveva bloccato (§6.2 punto 3); un velenoso in un batch da 200 con e senza isolamento; database irraggiungibile durante il fuoco che **non** consuma `attempts` | F3 |
| **Semantica KV** | esattamente un vincitore su N `putIfAbsent` concorrenti; **`expect:N>0` su chiave assente non crea la riga** (il bug piu' grave riparato); riga scaduta che legge come assente; `incr` con `max` che rifiuta senza consumare budget; `incr` la cui **prima** chiamata con `delta > max` viene rifiutata (§5.4 riparazione 2); `incr` su riga scaduta non numerica che riparte da zero (riparazione 3); prefisso con `%` e `_` trattati come letterali; ogni scrittura senza scadenza rifiutata | F2 |
| **Transazionali** | un gate perso blocca il push **e** il timer; `commit` non solleva sulla precondizione; un ack fallito per lease scaduta **annulla la scrittura KV** (e' la transazione, non il TTL e non `expect`, a fermare lo zombie); risultati index-allineati nello spazio piatto | F4 |
| **Parity gate** | matrice completa delle suite su payload che **non portano ne' `kv` ne' `timers`**, bit-identica al baseline. Era "con entrambi i flag OFF", e quella configurazione non esiste piu' (§0): la baseline non e' piu' un modo di **configurare** il broker, e' un modo di non usarlo. Va detto che il gate e' piu' debole di prima, perche' prima dimostrava che il codice nuovo non veniva nemmeno registrato e ora dimostra solo che non viene attraversato | F1, ogni fase |
| **Perf gate** | bundle senza `kv` e senza `timers`, prima e dopo la patch, payload byte-identico: p50 e p99 del giro entro il rumore, e **CPU per messaggio entro l'1%**, che e' la metrica che ha catturato la regressione push del seg v2 (la latenza e' troppo rumorosa a questi ordini di grandezza). Piu': `pg_stat_statements` non deve mostrare **nessuna query nuova con `calls > 0`** | F4 |
| **Perf gate sweeper** | percorso caldo invariato con lo sweeper acceso e una tabella di timer non vuota; e **costo del ciclo a tabella vuota** su una cella 2-core, che e' il costo che pagherebbero tutti quelli che non useranno mai la feature | F3 |
| **Vacuum gate** | soak che dimostra assenza di wobble con `vacuum_truncate = off`, piu' il numero di worker autovacuum occupati dalle tabelle nuove in stato stazionario contro `autovacuum_max_workers` | F5 |
| **Client** | integrazione per client piu' unit senza broker contro un plan server scriptato, che asserisce il **corpo JSON esatto** di ogni op (e' il contratto verso il broker) e che `commit` ritorna sulla precondizione e solleva sul resto | F6 |
| **Esempi** | i due full example, eseguiti in CI, che asseriscono un conteggio con deadline ed escono non zero | F7 |

Tutto gira su `test/run.sh`, stack single e HA, e la campagna di misura su **PG :5455**, mai sullo stack live `:5432`, che e' channel-ts.

---

## 16. Fasi e stime

Ogni fase e' spedibile da sola e ha un criterio di "fatto" **verificabile**, non un giudizio.

La colonna che qui prima si chiamava "spedisce al buio?" **non esiste piu'**, ed e' la conseguenza piu' grande di §0: non c'e' nessuna fase che possa atterrare in produzione con il proprio codice inerte. Al suo posto c'e' la domanda vera, che e' quando un cliente puo' toccare la cosa.

| fase | contenuto | raggiungibile da un cliente? | criterio di fatto | stima |
|---|---|---|---|---|
| **F0** | Contratti. Le dodici decisioni aperte chiuse (§20), un solo DDL canonico, un solo corpo del fuoco, la tabella env unica, l'header di ordine di lock scritto | non spedisce niente | il documento non ha piu' due versioni di nessun artefatto condiviso, e le tre porte a senso unico sul formato sono decise | 3 gg |
| **F1** | `024_kv.sql` e `025_log_timers.sql`, tabelle e helper puri, riga in `PROCEDURES`, check versione PG al boot. **Nessun lettore** | **no**, e per l'unica ragione che resta valida: i chiamanti non sono scritti. Non e' un flag spento, e' codice che non esiste ancora | doppio boot verde su vergine e su popolato; parity suite bit-identica; `42P22` verificato sul rig; unit degli helper | 4 gg |
| **F2** | `kv_apply_v1` completa, rotte KV, pool dedicato, token bucket, single-flight, regole `auth.rs` piu' fingerprint | **si', dal minuto del deploy.** Le rotte KV esistono e rispondono su ogni cella | test di semantica KV verdi; grep meccanico verde; suite bit-identica sui payload senza kv; le rotte rispondono e i livelli d'accesso di §8.1 sono quelli dichiarati | 8 gg |
| **F3** | `025` completa (apply, due, claim, fire, fail, dlq, peek, list), `sweeper.rs`, estrazione di `pack_segment` e del classificatore, metriche | **si'.** Da qui i timer si schedulano e partono, ovunque, la notte stessa | fault injection completa; isolamento due tenant; perf gate sweeper, incluso il costo a tabella vuota | 10 gg |
| **F4** | **Il centro di rischio.** Innesto nel wire (step 0 e 0b), demux e mux in `handlers/data.rs`, `txn_fail_body` esteso, instradamento dei bundle solo-KV | si', ed e' anche il percorso caldo di tutti gli altri | test di concorrenza sull'ordine di lock verde; test transazionali verdi; **perf gate con CPU per messaggio entro l'1%** e zero query nuove in `pg_stat_statements` | 6 gg |
| **F5** | Quote, rollup incrementale, scala di degrado, kill switch di runtime, blocchi obs | si' | vacuum gate; una simulazione di sforo che dimostra il bound dichiarato in §9.3 | 5 gg |
| **F6** | Client: HTTP e JS, poi py e go, poi rust, poi php e cpp | si' | per client, integrazione piu' unit contro plan server | 22 gg totali, parallelizzabili |
| **F7** | Webdoc: fase A internals, poi generatori, poi reference, poi concetti, poi i due full example | si' | `pnpm gen && pnpm verify` verde dopo **ogni** sotto-fase, con il margine del brief rimisurato dopo la fase A | 8 gg |
| **F8** | Proxy: P1 gate, P2 misura, P3 limiti, P4 metering. **Dipendenza esterna** (§9.8) | si' | la quota storage vede i byte delle due tabelle; un soak intero con `kv_expired_not_pruned` sotto soglia | dipende dall'orchestrator |

**Nota su F5, che l'ordine rende meno ovvio di prima:** i kill switch di runtime arrivano in F5, cioe' **dopo** che F2 e F3 hanno reso le superfici raggiungibili. Con i flag di boot quella sfasatura era innocua, perche' fino all'accensione non c'era niente da fermare. Adesso non lo e': fra F2 e F5 esiste una finestra in cui la sola leva contro un tenant che abusa del KV e' un rollback del binario. O i tre switch salgono a F2 (sono un booleano, una riga di `system_state` e un handler, non una fase), oppure la finestra va dichiarata e accettata per iscritto. **Raccomandazione: salgono a F2**, ed e' il primo pezzo di §16 da rinegoziare se le stime si stringono.

**Cosa e' utile anche se il resto slitta:** F0 e F1 lo sono, perche' il DDL canonico e la dimostrazione dell'ordine di lock valgono da soli e chiudono per sempre la classe di contraddizioni che ha bloccato questo piano. F2 e' utile senza F3 e viceversa: le due feature sono indipendenti **nell'ordine di sviluppo** (non piu' nel rilascio, §0), e un cliente puo' volere solo i timer.

**Cosa spedisce al buio: niente, e non e' un dettaglio di documentazione.** Questa sezione diceva "tutto fino a F3 compresa: le tabelle esistono, le SP esistono, nessuno le chiama", e piu' sotto descriveva una scala di accensione che cominciava con *broker completo, entrambi i flag `false`, le rotte non sono nemmeno registrate*. Quella era una strategia di rilascio precisa: il codice atterrava in produzione inerte, l'accensione era un **secondo evento**, separato nel tempo, per cella, decidibile a freddo dopo aver guardato una cella vera per una settimana. **Con i flag via, non esiste piu'.** Dal momento in cui il binario atterra, kv e timer sono vivi su ogni cella che lo esegue: F2 in produzione significa che un cliente puo' scrivere una chiave quel giorno, F3 significa che un timer parte quella notte.

**Cosa lo sostituisce come rete di sicurezza, e perche' e' piu' sottile.** Quattro cose, tutte piu' deboli di un flag spento, e vanno guardate come tali invece che elencate per rassicurare.

1. **I kill switch di runtime** (§12.1). Reversibili in un secondo, senza rollout, senza perdita. Ma agiscono **dopo** che il problema si e' visto: non impediscono il primo danno, lo fermano. E il piu' importante dei tre e' anche quello che costa di piu' usare, perche' fermare il fuoco dei timer accumula lavoro promesso, che un cliente legge come perdita.
2. **Le quote** (§9). Con `QUEEN_TENANCY_HEADER` attivo l'assenza di una riga in `queen.kv_quota` e' un **diniego** (§9.4 punto 1): su una cella con tenancy la superficie e' viva ma non **concessa** finche' il control plane non scrive una riga. E' qui che il gate per cella e' andato a finire, e nel trasloco e' migliorato, perche' passa dall'env al DB e da per cella a **per tenant**. Il prezzo e' che non copre il caso self-hosted, dove la tenancy e' spenta e non c'e' nessun gate; ed e' voluto, perche' li' l'operatore e' il cliente stesso.
3. **Le due tabelle nascono vuote.** Nessuna cella esistente diventa piu' cara di quanto sia oggi finche' qualcuno non scrive la prima chiave o non schedula il primo timer: la crescita e' interamente guidata dall'uso, e le gauge di occupazione la seguono dal primo byte. Il costo che invece si paga comunque, e che il flag spento evitava, e' il ciclo dello sweeper a tabella vuota (§7.1), che ha il suo perf gate su cella 2-core proprio per questo.
4. **Il proxy nega ancora, ma solo sul cloud.** `routes.rs:158` classifica come `Blocked` qualunque path sotto `/api/` che `classify` non riconosce e risponde 404 (§9.8), quindi finche' F8 P1 non atterra le celle cloud negano le due rotte fail-closed. E' l'unica rete che somiglia a un interruttore, e va detto cosa **non** copre: non copre il self-hosted e il dedicato senza proxy, non copre l'array `kv` dentro il transaction wire (che passa da una rotta gia' classificata), e non copre niente il giorno in cui F8 P1 la toglie.

**E la rete che manca, dichiarata:** non c'e' piu' modo di spedire il binario e guardare una cella per una settimana prima che qualcuno lo usi. La verifica che si faceva in produzione a superficie spenta va fatta **prima** del deploy, sul rig, e questo alza i gate di F3 e F4 da criteri di fase a criteri di rilascio.

**Dove sta il rischio:** F4, l'innesto nel wire. E' l'unica fase che tocca il percorso piu' caldo del prodotto, e' quella dove vivono cinque dei sei cicli di deadlock, ed e' quella dove il demux HTTP verso wire (che nessun design aveva progettato) puo' disallineare un contratto di indici su cui due client dipendono gia'. **La review avversariale va pianificata li'**, e il corpo del fuoco (F3) va letto da qualcun altro prima del merge.

**Ordine di rilascio**, che non e' piu' un ordine di accensione, e la differenza e' l'intero contenuto di questa sezione. Non c'e' nessun passo in cui qualcuno "accende": c'e' un passo in cui il binario atterra, e da li' in poi si tratta solo di **chi puo' arrivarci** e **quanto gli e' concesso**.

| passo | cosa cambia | chi lo decide |
|---|---|---|
| 1 | il binario atterra su cella self-hosted o dedicata **senza** proxy: kv e timer sono vivi, nessuna quota, nessuna concessione da chiedere | nessuno lo decide, e' il deploy. L'operatore e' il cliente stesso |
| 2 | il binario atterra su cella cloud: le rotte esistono sul broker e il **proxy le nega ancora** (§9.8), quindi la superficie e' viva dentro la cella e irraggiungibile da fuori | il proxy, per omissione, finche' P1 non atterra |
| 3 | F8 P1 (gate): la riga di `queen.kv_quota` diventa la concessione, e senza riga e' 403 `feature_gated` | control plane, **per tenant**, reversibile con un UPDATE |
| 4 | F8 P2 (misura) piu' un soak completo: la quota storage vede i byte delle due tabelle | control plane, e da qui la quota e' onesta |
| 5 | F8 P3 e P4 | **cella condivisa** |

**La regola resta, ed e' l'unica rimasta:** niente cella condivisa finche' la misura di P2 non e' in produzione da un soak intero, con `kv_expired_not_pruned` sotto soglia per tutta la durata. Un limite senza misura e' un limite finto, e adesso e' anche l'unica cosa che sta fra un tenant e il disco di una cella che condivide con altri.

---

## 17. Rollback e porte a senso unico

La sezione che manca a `PLAN_S3_ARCHIVE.md` e che qui e' obbligatoria, perche' due delle tre porte sono sul formato del wire.

### 17.1 Spegnere, e cosa si puo' spegnere davvero

**Non ci sono piu' flag di boot da spegnere** (§0). Restano i tre kill switch di runtime, piu' `QUEEN_SWEEPER`, e le due cose vanno tenute separate anche in tabella, perche' fanno lavori opposti: un kill switch ferma una superficie e lascia in piedi la manutenzione, `QUEEN_SWEEPER` ferma la manutenzione e lascia in piedi le superfici.

| scenario | comportamento deciso |
|---|---|
| `timers_fire_enabled` a `false` con timer pendenti | i timer **restano in tabella e non partono**. Non si perdono e non si drenano di corsia. Riaccendendo, il drenaggio riparte dal piu' vecchio. Non si rifiuta lo spegnimento: un interruttore d'emergenza che si rifiuta di scattare non e' un interruttore. E' pero' quello che costa di piu' usare, perche' accumula **lavoro promesso**, quindi non basta un WARN: serve la gauge di backlog con la sua soglia (§12, `queen_timers_due_backlog`) |
| `timers_schedule_enabled` a `false` | si smette di accettare timer nuovi, il fuoco **continua**. Innocuo e istantaneo, perche' niente e' stato promesso: e' il primo dei due da girare, ed e' il rung 8 della scala di degrado |
| `kv_enabled` a `false` con chiavi vive | 503 con `Retry-After` sulle rotte, array `kv` del wire rifiutato con errore permanente. Le chiavi restano e **continuano a essere potate**, perche' lo sweeper non guarda questo interruttore: la potatura e' igiene di tabella, non superficie di prodotto, ed e' il caso buono |
| `QUEEN_SWEEPER=false` | l'unica manopola che ferma il task. **Non e' un gate e non e' un kill switch di feature:** le superfici restano vive e perdono il mietitore, quindi le chiavi scadute non vengono potate mai e i timer non partono mai. E' la sola combinazione che accumula spazio in silenzio, ed e' quella che il WARN di boot deve gridare piu' forte, nominando i due conteggi (§7.1) |
| kill switch di runtime, in generale | reversibili in un secondo, senza rollout, senza perdita (§12.1) |

**Non esiste un percorso di rollback che perda dati**, ed e' una differenza sostanziale rispetto ad archiviare blob: qui nessuna colonna esistente viene svuotata, nessun dato viene spostato altrove, e spegnere non rende niente illeggibile. Con un'eccezione che non e' un rollback e che §17.3 nomina per quello che e': **tornare indietro sulla decisione di §0 non e' un rollback, e' una perdita di dati.**

### 17.2 Rollback del binario

Le tabelle nuove restano (non si droppano mai in un rollback: droppare `queen.kv_quota` cancellerebbe la configurazione di un operatore). Le SP nuove restano, orfane e innocue. Il rischio vero e' **l'ambiguita' di overload**, e in v1 non e' raggiungibile perche' **non si droppa nessuna firma esistente**: `log_transaction_wire_v1` resta `CREATE OR REPLACE` con la stessa firma `(p JSONB)`, e `log_pop_list_v1` **non viene toccata affatto**, perche' il rider e' fuori dalla v1.

Va scritto adesso per quando il rider arrivera' (Fase 9), perche' e' un guasto brutto: aggiungere parametri a `log_pop_list_v1` creando una firma nuova e droppando la vecchia funziona **in avanti** ma non all'indietro. Un rollback riapplica il 004 vecchio, il cui `DROP FUNCTION IF EXISTS` della firma corta non trova nulla (gia' droppata), e poi ricrea la firma corta: a quel punto **coesistono due overload** e una chiamata con esattamente dodici argomenti solleva `42725 function is not unique`. Guasto: rollback d'emergenza alle tre di notte, il broker riparte, applica lo schema, e **ogni pop del cluster fallisce**, compresi i broker nuovi ancora vivi, perche' il difetto e' nel catalogo e non nel binario. Nessuna riparazione automatica, serve un `DROP FUNCTION` manuale in produzione, cioe' esattamente la classe di intervento che il modello always-virgin promette di non richiedere. **La forma corretta e' non droppare la firma corta**, ma renderla un wrapper `plpgsql` che delega a quella lunga, cosi' il vecchio 004 che la ricrea sovrascrive un wrapper con un wrapper. Il precedente in casa elenca **piu'** firme storiche, non meno.

### 17.3 Le porte a senso unico

| porta | perche' non si torna indietro | decisione |
|---|---|---|
| **l'unita' del TTL sul wire** | il nome del campo sul wire non e' rinominabile dopo che sette client lo mandano | CHIUSA: `ttlSeconds`, §20.1 |
| **il `txn` del timer attraverso un reschedule** (preservato o sovrascritto) | se preservato, `txn` diventa **opzionale** nell'op `schedule`, quindi cambia la forma del wire | §20.2 |
| **`messageId` promesso allo schedule** | toglierlo dopo rompe i client che ci contano | deciso: si promette. Costa 16 byte per timer e regala all'API di schedule una risposta forte |
| **TTL obbligatorio** | un campo obbligatorio si puo' rendere opzionale dopo, mai il contrario | deciso: obbligatorio. E' la porta presa nel verso giusto |
| **200 su `applied:false`** | passare a 4xx dopo romperebbe ogni client che ci fa `switch` | deciso: 200, una regola sola |
| **modulo shard a 64** | cambiarlo ri-sharda in silenzio le righe gia' scritte | deciso: fisso per sempre, nessuna env |
| **una chiave al massimo una volta per bundle** | vietarlo e' rilassabile dopo, ammetterlo no (definirebbe per sempre un ordine di valutazione intra-batch) | deciso: vietato, e marcato load-bearing per la dimostrazione |
| **kv e timer vivi di default, senza flag di boot** | e' reversibile sulla carta e non nei fatti, ed e' la porta che questo piano ha aperto per ultima. Reintrodurre i due flag e' un commit revertibile finche' **nessuno ha usato la superficie**; dal momento in cui i clienti ci scrivono dentro, rimetterli e spegnerli non e' un cambio di configurazione, e' rendere irraggiungibile lo stato che hanno gia' scritto e non far partire timer che il prodotto ha gia' promesso, cioe' **una perdita di dati con un altro nome**. Vale anche per la variante mite (rimetterli con default `true`): un flag che nessuno gira non protegge nessuno, e il primo che lo gira per sbaglio spegne un motore. E la finestra in cui la porta e' ancora aperta non si chiude a una data, si chiude **alla prima chiave scritta da un cliente vero**, che con §16 puo' essere il giorno del deploy di F2 | deciso: vivi, senza flag (§0, §20.4) |

### 17.4 Client vecchi e broker nuovi, e viceversa

- **Client vecchio contro broker nuovo:** nessun effetto. Non manda `kv` ne' `timers`, quindi il ritorno e' byte-identico (§6.3) e il costo e' zero.
- **Client nuovo contro broker vecchio:** 400 pulito e nominativo dal demux (`"supports only push and ack operations"`). E' il miglior fallimento possibile e va documentato in `reference/compatibility.mdx`.
- **Broker nuovo contro database vecchio con `QUEEN_APPLY_SCHEMA=0`:** un bundle senza kv funziona, uno con kv fallisce con `42883`, classe 42, permanente, non ritentato. E' il comportamento giusto ed e' l'unico prezzo del modello always-virgin.

---

## 18. Rischi e cosa NON facciamo in v1

### 18.1 TAGLIATO: il rider KV sul pop

E' il taglio piu' grande del piano e va motivato per intero, perche' e' anche l'esempio piu' vistoso dei design.

Elimina, in un colpo: il `DROP FUNCTION` piu' `CREATE` della funzione **piu' calda del motore** e con essa il guasto di ambiguita' di §17.2; cinque parametri e i due testi di statement che devono restare byte-identici fra loro perche' condividono la cache dei prepared; il threading attraverso il percorso di **pop fusion**, che nessun design nominava e che documenta la propria lista di argomenti come "esattamente quella di `db::pop_list`"; il breaker automatico e le sue due env; una metrica di durata separata; una decisione aperta; e soprattutto **l'unica parte della superficie SDK che rompe le firme tipizzate** di Go, Rust e C++ (§10.1).

E chiude tre difetti di correttezza che il design del rider aveva: il tetto era di **righe e non di byte** (1000 per 64 KiB sono 64 MB dentro la transazione che tiene i lock consumer); il clamp duro dichiarato viveva **solo nella config Rust**, quindi il percorso fusion e il broker embedded ottenevano 1000; e `keysOnly` **non garantisce l'assenza di detoast**, perche' `key = ANY(array)` con `ORDER BY key` fa mettere un Sort al planner, e un Sort che spilla appiattisce i tuple e detoasta tutto lo stesso. Lo stesso design riconosceva questo comportamento del planner per la sonda dei timer e lo correggeva con un LATERAL, ma non lo applicava al percorso piu' caldo del prodotto.

**Costo del taglio, dichiarato:** un round trip in piu' per lo stato compattato per partizione. L'SDK fa una `getMany` di seguito, usando le partizioni che ha appena visto nel batch. Il design stesso lo prescriveva gia' per il pop wildcard, dove la partizione non e' nota prima della risposta, quindi la v1 ha **una** forma invece di due, ed e' quella onesta.

Quando tornera' (Fase 9, con la sua revisione di piano): solo la forma a chiavi esplicite, tetto in byte, clamp in SQL, LATERAL invece di Sort, solo MVCC, e il wrapper di §17.2.

### 18.2 ACCETTATO: `queen.kv` e' lo spazio di lock piu' esterno, quindi il piu' a lungo tenuto

Lo step 0 e' l'unica posizione priva di deadlock (§2.5), ma la conseguenza e' che un row lock KV preso allo step 0 resta preso durante il provisioning, il pre-lock, tutti i push con i loro blob, tutti gli ack, fino alla fsync. Una chiave contesa allunga la tenuta di un lock che sta **davanti** ai lock di partizione, quindi una feature "di stato" puo' degradare il percorso messaggi, che e' il contrario del motivo per cui la si aggiunge.

Il numero: un bundle e' un commit a durabilita' piena, circa 10 ms p50 con circa 4 ms di fsync misurati su questo stack, quindi **una chiave di gate condivisa cappa a circa 100 bundle al secondo**.

Contromisure in v1: cap di 64 op per bundle **piu' il budget per chiamata su chiavi e byte** (§6.1 punto 4), `getPrefix` vietata nel wire, instradamento dei bundle solo-KV fuori dal wire. **Rischio residuo accettato:** non c'e' niente che impedisca a un array `kv` di entrare in una transazione gia' grande, e non c'e' un tetto per tenant sulle precondizioni perse al secondo. Non c'e' nemmeno un limite sul lavoro totale di un bundle, ma quello non esiste per **nessuna** transazione oggi, quindi non e' una regressione introdotta qui.

Difesa documentale, che va ripetuta accanto a **ogni** esempio: le chiavi di gate sono **per messaggio** (`done:<txId>`, che non collide mai e non costa niente), mai condivise. Gli esempi del design mettevano un gate in ogni consumer, cioe' insegnavano a tutti a pagare il lock piu' esterno; e' corretto quando la chiave e' per messaggio ed e' un disastro quando non lo e', e la differenza non era scritta dove l'utente la legge.

### 18.3 TAGLIATI dalla v1, additivi dopo

- **Il frame mesh `T_TIMER_DUE` e tutto l'apparato di sveglia cross-broker.** Con `MAX_SLEEP_MS = 1000` lo sweeper e' comunque un poll a 1 Hz: l'apparato compra sotto-secondo di latenza su una semantica dichiarata "non prima di, mai esattamente a". Costa un tag di protocollo, un handler nuovo, il coalescing, una env e il wiring nel broker embedded. Additivo.
- **`getPrefix` nel wire e nel rider.** Permanente, non additivo: e' un non-goal (§5.5).
- **`merge`, `deletePrefix`, `expire`, CAS sul valore** (§5.6).
- **Le ricorrenze.** Permanente in v1.
- **`timers list` e `peek`** possono slittare a F3-bis senza rompere niente: sono lettura pura.
- **PHP, C++ e CLI** slittano a fine F6.
- **Quattro dei sei full example** slittano dopo il GA (§11.4).

### 18.4 ACCETTATO: le due feature condividono un task, quindi condividono i guasti

Uno sciame di timer velenosi che ritentano occupa i cicli e fa slittare la potatura KV; le scadenze smettono di potare mentre le letture restano corrette; nessuno se ne accorge finche' non guarda `kv_expired_not_pruned`. C'e' una metrica con soglia, **non c'e' un limite**: le due fasi non hanno budget separati, e non ce l'hanno perche' due task avrebbero due lock, due connessioni e due cadenze da spiegare per una feature che deve essere **un** componente di fondo, non due. Se dovesse mordere, la riparazione e' **additiva**: un budget di ciclo per fase, non un secondo task.

### 18.5 ACCETTATO: la quota di occupazione e' morbida per un motivo strutturale

§9.3. Un contatore esatto sarebbe un lock per tenant nello spazio piu' esterno. Il limite di sforo e' l'unica garanzia che si puo' dare, va scritta in documentazione **con la formula vera** e non nascosta dietro la parola "morbida". Con il delta locale come enforcer, lo sforo si riduce a quello fra broker, che e' il bound migliore ottenibile senza un aggregato sul percorso caldo.

### 18.6 ACCETTATO: quattro numeri non sono misurati

E vanno misurati sul rig (**PG :5455**, mai lo stack live) prima del GA, non dichiarati adesso. Le pagine internals devono dire che non lo sono, con la stessa formula dei blocchi datati di `001_log_schema.sql`.

1. **`fillfactor = 70`** su entrambe le tabelle: e' per analogia, e su `log_timers` l'analogia e' persino debole, perche' la claim e' non-HOT per costruzione (`visible_at` e' indicizzata), quindi l'headroom compra localita' di pagina e non catene HOT.
2. **Quanti timer al secondo regge una macchina** prima che l'autovacuum di `queen.log_timers` entri nel profilo. E' il numero che deciderebbe se la scelta di `visible_at` indicizzata (giusta contro il guasto, sbagliata contro il carico) e' sostenibile.
3. **Il costo del rollup** sopra i 5 milioni di righe, e la soglia oltre la quale degrada a campionamento.
4. **Il numero di worker autovacuum** occupati dalle tabelle nuove in stato stazionario, contro `autovacuum_max_workers` (default 3). Se mordesse, il piano deve dire di alzarlo **quando le due tabelle iniziano a riempirsi**, e non piu' "quando la cella accende la feature", perche' quel momento non esiste piu' (§0): il trigger e' una misura di occupazione, non un evento di configurazione. Altrimenti il primo sintomo e' un rallentamento del log senza colpevole.

Piu' due criteri di accettazione che sono numeri, non giudizi: **CPU per messaggio entro l'1%** su un payload che non porta ne' `kv` ne' `timers` (era "con i flag spenti", e la configurazione che rendeva vera quella frase non esiste piu', §0), e **tempo di tenuta p99 del lock `log_consumers`** invariato quando le due superfici sono in uso.

### 18.7 ACCETTATO: la dimostrazione dell'ordine di lock e' manutenuta da un header

Nulla nel database la applica. L'unica cosa che la tiene in piedi e' la regola di §2.2 scritta in un commento, e **cinque dei sei cicli di §2.4 sono l'implementazione ovvia di ciascun pezzo**. Le contromisure sono tre, tutte in §15, e non costano niente: il test di concorrenza che fallisce su `40P01` (oggi un deadlock in produzione diventa un retry riuscito e nessuno lo vede, perche' il ciclo di retention ingoia gli errori e il boot ritenta i deadlock), la nota nel test che dice a cosa serve, e il conteggio dei `40P01` per SQLSTATE aggregato a finestra, che e' l'unico segnale che distingue "ordine rotto" da "carico alto".

### 18.8 ACCETTATO: la superficie che rende la feature sicura vive negli SDK

Il TTL obbligatorio, il divieto di istanti assoluti, i cap e il divieto di `getPrefix` nel wire vivono nella SP, quindi valgono ovunque, compreso il broker embedded. Ma il conio meccanico della chiave di stato, `expect` timbrato da solo, e il gate `once()` vivono **nei client**, e i client sono sette. Un cliente che parla HTTP diretto, o un SDK aggiornato in ritardo, ottiene un KV in cui una `put` non fencata e' a una battuta di distanza, ha lo stesso aspetto nel diff, e non e' protetta da niente. La lost update e' **rilevabile**, perche' la versione esiste, ma solo da chi si e' ricordato di passare `expect`. **La sola difesa strutturale disponibile e' che la forma sbagliata sia piu' lunga da scrivere di quella giusta**, e va trattata come un requisito di progettazione della superficie SDK, non come un auspicio.

---

### 18.9 TAGLIATO: kv e timer dentro `log_streams_cycle_v1`

Il ciclo streams **non** riceve gli array kv e timers, e la riga 11 di §2.3 resta com'e'. Non e' un'omissione: e' l'applicazione, all'unico attore che ci inciampa, di una regola che §2.1 gia' scrive.

Il ciclo streams e' **l'unico attore del prodotto che prende un advisory lock bloccante** (spazio 3, `ADV`, "bloccante" per il ciclo contro "try, mai bloccante" per il pop). L'ordine totale mette `queen.kv` e `queen.log_timers` **prima** di `ADV`, quindi un ciclo che li toccasse terrebbe i due spazi piu' esterni **mentre si blocca** su un advisory. La regola gia' scritta in §2.1 lo vieta con queste parole: *chi prende un advisory lock bloccante deve prenderlo prima di toccare kv o timers, oppure non prenderlo affatto.* Il ciclo streams non puo' fare la prima cosa senza un ordine totale diverso per lui solo, che andrebbe dimostrato da capo e non e' una estensione gratuita.

Il guasto concreto che si evita e' esattamente quello che ha fatto cancellare la correzione C2 sul percorso del fuoco (§2.4): una `cancel` dentro un transaction wire finirebbe ad **aspettare** un ciclo streams lento, tenendo la sua riga per tutta la durata del bundle. Li' e' stato giudicato grave abbastanza da riscrivere la SP; qui si evita non scrivendola.

**Conseguenza dichiarata, e va scritta in `use/streams.mdx` perche' e' la prima cosa che qualcuno prova:** il pacing dall'interno di uno stream non e' esprimibile in v1. Il rate limiter inverso (`incr` con `max` che decide, timer che rimanda) si scrive in un **consumatore piano**. Da uno stream si emette su una coda e il timer si mette nel consumatore **di quella coda**, non nell'operatore. E' una riga di architettura in piu' e nessuna perdita di garanzia, ma scoperta da soli sembra un limite arbitrario.

Quando tornera', servira' una revisione dell'ordine totale limitata a quell'attore, con la sua dimostrazione, non un array in piu' nella SP.

## 19. Registro delle critiche avversariali

Ogni difetto BLOCCANTE e SERIO sollevato dai tre revisori, con la sezione che lo chiude oppure la dichiarazione esplicita di rischio accettato. Nessuno e' ignorato.

### 19.1 Correttezza e concorrenza

| # | difetto | esito |
|---|---|---|
| B1 | `incr`: `v_next` inesistente, fail-closed silenzioso | **corretto**, §5.4 riparazione 1, helper `kv_num_v1` |
| B2 | `incr`: il ramo INSERT non applica `max` ne' `min` | **corretto**, §5.4 riparazione 2, validazione di `delta` |
| B3 | `COLLATE "C"` assente su `log_timers`, zero precedenti nel repo | **corretto**, §2.4 C3 e §3.3: collation su tutte e quattro le colonne, pinning esplicito sugli `ORDER BY`, e verifica `42P22` come criterio di uscita di F1 |
| B4 | Il fuoco con `FOR UPDATE` ordinato su una colonna `id` inesistente | **corretto**, §2.4 C2: la correzione e' **cancellata**, e l'invariante "il fuoco non aspetta mai su T" e' scritta nell'header |
| B5 | `queen.kv_quota` con quattro definizioni incompatibili | **corretto**, §3.2: una sola definizione autoritativa, `ADD COLUMN IF NOT EXISTS` col precedente di `019_worker_metrics.sql` |
| B6 | Ownership degli shard contraddittoria, con righe orfane per sempre | **corretto**, §1.10: nessuna ownership, `QUEEN_SWEEPER_SHARDS` non esiste, e la frase opposta e' cancellata dal DDL |
| S7 | `incr` su riga scaduta non numerica blocca la chiave | **corretto**, §5.4 riparazione 3 |
| S8 | Il fuoco non rilascia le righe di un segmento `stale` | **corretto**, §6.2 punto 3, con rilascio simmetrico anche nel ramo `duplicate`. Il rilascio del ramo `stale` e' esercitato; quello del ramo `duplicate` non lo e', perche' il ramo e' irraggiungibile in v1 (§6.2, §20.7) |
| S9 | Rollback e ambiguita' di overload su `log_pop_list_v1` | **non raggiungibile in v1** (§18.1: la funzione non si tocca), e la forma corretta e' scritta per quando servira' (§17.2) |
| S10 | Il cap di 64 op non limita il lavoro sotto il lock piu' esterno | **corretto**, §6.1 punto 4: budget per chiamata su op, chiavi **e** byte, applicato anche nel wire |
| S11 | Il rider fa il `getPrefix` che il wire vieta | **eliminato**, §18.1 |
| S12 | `absent` su un timer gia' consegnato letto come successo | **corretto**, §4.4: contratto in grassetto, `ok:false`, `txn` nella risposta, e l'esempio della saga deve mostrare la verifica lato consumatore |
| S13 | "Contatore esattamente una volta" non e' exactly-once | **corretto**, §11.3 (`reference/http/index.mdx`: `incr` non e' idempotente) e §11.4: l'esempio, quando arrivera', porta il gate come prima op del bundle |
| S14 | `expect: undefined` degradato in silenzio a upsert | **corretto**, §5.3: `expect` presente e vuoto e' un errore client-side |
| S15 | Nessuna garanzia che il fuoco avvenga entro il lease | **corretto**, §7.2: claim e fire nella stessa iterazione, tetto sul lavoro in volo, e `claimed_until > now()` aggiunto alla verifica |
| S16 | `SET STORAGE` prende ACCESS EXCLUSIVE a ogni boot | **corretto**, §2.4 D8: cancellato da entrambe le DDL |
| S17 | La versione restituita a chi perde e' letta fuori dal lock | **corretto**, §5.3: discriminazione nello stesso statement, e `version` dichiarata advisory |

### 19.2 Percorso caldo e prestazioni

| # | difetto | esito |
|---|---|---|
| B1 | `p_verified = -1` annulla il front-dedup dentro il serializzatore di push | **corretto**, §6.2: il fuoco passa `v_last` |
| B2 | Rollup come seq scan su heap che non si ritrae, con misura sbagliata | **corretto**, §7.5: `pg_column_size(k.value)`, conteggio incrementale, full scan raro e guardato |
| B3 | Rider senza tetto in byte, `keysOnly` che non evita il detoast | **eliminato**, §18.1 |
| B4 | Le scritture KV su HTTP rubano slot `Lane::Push` | **corretto**, §8.4 punto 2: nessuna corsia per le scritture standalone, pool dedicato, piu' un tetto aggregato di cella |
| B5 | Due corpi contraddittori per `log_timers_fire_v1` | **corretto**, §6.2: un solo corpo canonico, piu' la distinzione acquisire contro ri-toccare in §2.2 |
| S6 | Sweeper acceso di default con entrambe le feature spente | **superato**, §7.1: la condizione non esiste piu', perche' non esistono piu' due feature spente (§0). La correzione originale (non spawnare il task) e' caduta con i flag; il task si spawna sempre e l'unica difesa e' il **backoff a tabella vuota**, col suo perf gate su cella 2-core. Il costo che il revisore aveva nominato e' reale e ora si paga |
| S7 | `min_sleep` 5 ms e' uno spin durante il burst | **corretto**, §7.2: backoff con jitter sulla claim vuota |
| S8 | Le sonde di salute sono O(n) proprio nel guasto che rilevano | **corretto**, §6.2 e §14.2: `due` cappata a 2000 con `dueCapped`, `expired_not_pruned` cappata e riportata come `>= N` |
| S9 | `cost_delay 0` su sette tabelle affama l'autovacuum del motore | **corretto**, §3.2: `cost_delay 0` solo su `kv` e `log_timers`; piu' §18.6 punto 4 come misura |
| S10 | Il livello HTTP verso wire non e' specificato | **corretto**, §8.2, con la mappa degli indici e la guardia riscritta |
| S11 | `SET STORAGE` trovato e non propagato | **corretto**, §2.4 D8, entrambe le DDL |
| S12 | Il cap per shard della claim e' 3, e con batch bassi 1 | **corretto**, §6.2: pavimento a 8 e comportamento di `LIMIT` piu' `SKIP LOCKED` scritto nell'header |
| M13 | Contraddizioni di default fra i cinque documenti | **corretto**, F0 e §19.4 |
| M14 | "Monotone" e l'argomento sbagliato su `CACHE 1000` | **corretto**, §3.2 e §13.3: token unico e opaco, e `CACHE` motivato dal WAL |
| M15 | Gli esempi insegnano a pagare il lock piu' esterno | **corretto**, §18.2: chiavi di gate per messaggio, ripetuto accanto a ogni esempio |

### 19.3 Sicurezza, quote, e realizzabilita'

| # | difetto | esito |
|---|---|---|
| B1 | `producerSub`, `messageId` e `txn` forgiabili dal client | **corretto**, §4.2 e §6.2: argomenti separati, `RAISE 22023` sulla presenza nell'op |
| B2 | Un tenant sopra quota non puo' cancellare i timer | **corretto**, §9.6: rotta e classe separate per la cancel, mai bloccabile |
| B3 | Il limite di sforo della quota sbagliato di un ordine di grandezza | **corretto**, §9.3: il delta locale e' l'enforcer, la formula pubblicata e' quella vera |
| B4 | Tenant non validato, mappe non cappate, default fail-open | **corretto**, §9.4: assenza di riga uguale diniego, mappe cappate che negano, boot che rifiuta la combinazione pericolosa |
| B5 | `kv_quota` non estendibile in un modello senza migrazioni | **corretto**, §3.2 |
| B6 | Nessuna equita' fra tenant nello sweeper, e la manopola non e' implementabile | **corretto**, §6.2: round robin sui tenant dentro la claim, piu' la label `tenant` su `fire_lag` (§14.1) |
| S7 | Rotte nuove nel fallthrough `ReadWrite` di `auth.rs` | **corretto**, §8.1: la tabella dei livelli, dentro il blocco giusto |
| S8 | Il proxy inoltra l'header di tenant del client con il flag spento | **richiesta al proxy**, §9.9 |
| S9 | `version` non monotona e la mitigazione che non regge | **corretto**, §13.3, piu' `GRANT USAGE` e non `SELECT` |
| S10 | Chi misura quali shard, e il costo N volte | **corretto**, §3.2 e §7.5 |
| S11 | DLQ del timer velenoso: tenant e replay | **corretto**, §6.2 e §4.5 |
| S12 | Metering per chiamata invece che per timer | **corretto**, §9.7 |
| S13 | Prefissi nella query string | **corretto**, §5.5: prefisso solo via POST, la rotta GET con prefisso non esiste |
| B2' | Il livello HTTP verso wire mancante, `handlers/data.rs` fuori da ogni lista | **corretto**, §8.2 |
| B3' | Nessun taglio v1, volume non stimato | **corretto**, §16 e §18 |
| B4' | Sette client non progettati, e il rider rompe le API tipizzate | **corretto**, §10, e il taglio del rider rende la superficie additiva ovunque |
| B5' | SQL con variabili e colonne inesistenti | **corretto**: §5.4 (`v_next`), §2.4 C2 (`t.id`), §18.1 (le variabili del rider spariscono col rider) |
| B6' | La quota storage del proxy dichiarata no-op mentre e' viva | **corretto**, §9.1 |
| S1' | `getPrefix` vietata nel wire e ammessa sul pop | **coerente ora**: vietata in entrambi (§5.5, §18.1) |
| S2' | `txn_fail_body` cambia per tutti i client | **contato**, §8.3 e §10.3 |
| S3' | Il percorso cloud dipende da un file non modificabile | **dichiarato dipendenza esterna con data**, §9.8 e F8 |
| S4' | Fase dei full example sovradimensionata | **ridotta**, §11.4: due esempi, tre linguaggi |
| S5' | Aritmetica del budget del brief incompleta | **corretto con misura**, §11.1: 719 byte, non 1985 |
| S6' | File Rust dimenticati (pop fusion, hotlist) | **non piu' applicabili**, §18.1 |
| S7' | La DLQ apre una superficie di isolamento nuova | **corretto**, §13.2, con test di merge |
| S8' | `SET STORAGE` ancora nelle DDL dopo essere stato bocciato | **corretto**, §2.4 D8 |

### 19.4 Le contraddizioni cross-documento, chiuse

| tema | decisione |
|---|---|
| ownership shard | nessuna, `QUEEN_SWEEPER_SHARDS` non esiste (§1.10) |
| `COLLATE "C"` su `log_timers` | presente (§3.3) |
| lock T nel fuoco | `SKIP LOCKED` sempre, mai attesa (§2.4 C2) |
| `SET STORAGE` | cancellato ovunque (§2.4 D8) |
| forma di `kv_quota` e `kv_usage` | una sola, in 024, estendibile (§3.2) |
| `CLAIM_BATCH` | 200 (§7.2), con il pavimento per shard a 8 |
| quota storage proxy | viva; manca **la misura** (§9.1) |
| `p_in_wire` | flag, non due SP (§6.1) |
| NOTIFY | nessuno, ne' Postgres ne' mesh in v1 (§7.4) |
| enforcement quota | delta locale, misura per il rilascio (§9.3) |

### 19.5 Difetti minori, chiusi in una riga ciascuno

`hashtextextended` non stabile fra major: `STORED` lo congela e nessuna query lo ricalcola, scritto nel commento di colonna (§3.2). `created_at` azzerato sulla resurrezione: dichiarato accanto alla colonna (§5.7). `absent` su tenant altrui con `ok:true`: diventa `ok:false` (§4.4). Lo shard non contiene il tenant: scritto nel commento, non e' un'unita' di isolamento (§1.10). Nomi nel `MESSAGE` del `RAISE`: solo nel `DETAIL` (§13.5). Nessuna guardia meccanica contro SQL costruito fuori dalle SP: test grep (§15). Versione minima di PostgreSQL non dichiarata: 14, verificata al boot (§0). `forever` negli esempi di CI: vietato (§10.4). `#buildPopParams` morto nel client JS: cancellato nella stessa patch (§10.4). Il campo di misura letto con un aggregato al volo ogni 10 s: vietato, si legge dalla cache (§9.8 P2).

---

## 20. Decisioni aperte (Alice)

Cinque erano aperte. **Le tre porte a senso unico sul wire sono state chiuse da Alice il 2026-08-17** (20.1, 20.2, 20.3), e **20.4 e' stata chiusa il 2026-08-18 con l'esito rovesciato rispetto alla proposta**: restano tutte qui come registro, non come domande. Ne restano **tre**: una reversibile (20.5), **una porta a senso unico aperta proprio dalla ratifica di 20.1** (20.6, l'unita' di durata sul wire dei timer), che va chiusa insieme a quella o il prodotto resta con due convenzioni non dichiarate, e **una emersa in verifica dell'implementazione e non nel design** (20.7, il ramo `duplicate` del fuoco), reversibile ma da chiudere prima del GA. Ognuna ha un default proposto e una scadenza, **tranne 20.7**, dove il default e' deliberatamente assente e la ragione e' scritta li'.

**20.1 L'unita' del TTL sul wire. RATIFICATA: `ttlSeconds`.**
Decisione di Alice, 2026-08-17. Vince la coerenza col server, che parla secondi ovunque (`dedup_window_seconds`, `lease_seconds`, `min_pop_wait_time`, `retention_seconds`): il prodotto resta a **una sola convenzione di durata**, e la conversione da forme comode la fanno i sette SDK. Conseguenze applicate in tutto il piano: il campo del wire e' `ttlSeconds` (intero maggiore di zero) in `kv_apply_v1`, nelle rotte HTTP e in ogni superficie SDK; **non si introduce `ttlMillis` in nessun client**, perche' reintrodurrebbe dalla porta di servizio esattamente la doppia convenzione che questa decisione elimina. Lo zucchero ammesso e' `until: <data>`, convertito in delta di secondi al momento dell'invio, e arrotondato **per eccesso** al secondo (un TTL arrotondato per difetto puo' far scadere un marcatore prima della finestra che doveva coprire). I timer **non sono automaticamente allineati** da questa decisione, e la cosa apre una porta nuova della stessa classe: vedi §20.6.

**20.2 Il `txn` del timer attraverso un reschedule. RATIFICATA: sovrascritto.**
Decisione di Alice, 2026-08-17. Ogni reschedule conia un `txn` nuovo, cioe' e' un messaggio nuovo. Cosi' "questo timer, riprogrammato, ha consegnato questo messaggio" e' rispondibile senza ambiguita', `txn` resta **obbligatorio** nell'op `schedule` (la forma del wire non cambia), e un payload sostituito non condivide mai un identificatore con quello che ha rimpiazzato. Il `txn` fisso resta la rete di dedup del **singolo** tentativo di fuoco, che e' il caso per cui esiste. **Correzione post-verifica, che non tocca la decisione ma ne cancella una premessa: quella rete non esiste.** Con `p_verified = v_last` (§6.2) la sonda di dedup non gira mai per il fuoco, quindi il `txn` fisso non protegge nemmeno il singolo tentativo; a proteggerlo e' `DELETE` piu' push nella stessa transazione. La ratifica resta valida com'e' (un reschedule conia comunque un `txn` nuovo, ed e' l'unica forma che rende rispondibile "questo timer, riprogrammato, ha consegnato questo messaggio"), e il corollario da scrivere in documentazione diventa **piu' forte, non piu' debole**: non solo la rete di dedup non attraversa un reschedule, ma **non c'e' nessuna rete di dedup sul fuoco**, quindi riprogrammare o ripubblicare un timer gia' partito produce un secondo messaggio nel log e nessuno strato lo ferma. Vedi §20.7 per la decisione che ne discende.

**20.3 Il metering fattura la promessa. RATIFICATA: si'.**
Decisione di Alice, 2026-08-17. Uno schedule conta un messaggio, un reschedule ne conta un altro, la cancel zero e senza rimborso, il fuoco zero. E' l'unica forma che non riscrive il modello di metering, dato che il fuoco non attraversa mai il gateway (§9.7), e rende i timer una variante di Produce, per cui i blocchi push valgono automaticamente, con l'eccezione obbligatoria della cancel (§9.6). Da mettere nella pagina dei prezzi con queste parole, prima del primo conguaglio: **si fattura la promessa, non la consegna.**

**20.4 `QUEEN_KV_ENABLED` e `QUEEN_TIMERS_ENABLED` restano `false` per il GA. RATIFICATA il 2026-08-18, con l'esito ROVESCIATO rispetto alla proposta: i due flag non esistono piu'.**

Decisione di Alice, 2026-08-18, nelle sue parole: **"KV e timer non sono feature: sono IL DEFAULT."** Ripetuta due volte, perche' la prima implementazione si era limitata a girare i due booleani da `false` a `true` e a chiamare "kill switch" quello che restava. **Non basta: finche' esiste `QUEEN_KV_ENABLED`, kv e' una feature.** Non esiste `QUEEN_PUSH_ENABLED` ne' `QUEEN_POP_ENABLED`, e l'esistenza del flag di boot **e'** l'affermazione che la cosa sia opzionale. Quindi il flag va **tolto**, non girato, e con esso tutto l'apparato che esisteva solo perche' la superficie poteva mancare: la registrazione condizionale delle rotte, il ramo del wire rifiutato per configurazione, il 404 come risposta a "spento", lo spawn condizionale dello sweeper, la gamba KV dell'interlock di tenancy, il `preview` motivato dallo spegnimento, e il caveat "se abilitato" in ogni pagina e in ogni SDK. Conseguenze applicate in §0, §7.1, §9.4, §9.5, §11.2, §11.3, §12.1, §13.2, §15, §16, §17.1, §17.3, §18.6 e §19.2.

**Cosa NON e' stato toccato, e la distinzione e' il cuore della decisione.** I kill switch di **runtime** di `server/src/switches.rs` restano tutti (`kv_enabled`, `timers_schedule_enabled`, `timers_fire_enabled`) e nascono **accesi**: dove `main.rs` faceva `Switches::new(cfg.kv_enabled, cfg.timers_enabled)`, ora nascono a `true` e basta. Un gate si accende per provare una cosa; un kill switch si spegne per fermare una cosa che sta gia' facendo male. Si somigliano e sono opposti, ed e' esattamente per questo che la distinzione va scritta dove serve e non solo qui: nell'header di `switches.rs`, in `deploy/state.mdx`, e in §12.1.

**Il ragionamento OFF, conservato come superato e non cancellato**, perche' questo piano e' anche un registro di cosa si e' scartato. Proponeva **OFF di default con accensione per cella**, su tre argomenti. Primo: la quota di occupazione e' morbida per costruzione e si appoggia a una misura che il giorno uno non esiste in produzione, quindi acceso di default significa che il primo tenant che tratta il KV come Redis degrada WAL e autovacuum di una cella condivisa prima che qualcuno possa vederlo. Secondo, l'asimmetria: un default si cambia da `false` a `true` dopo una campagna di misura, il contrario e' una regressione per chi lo usa gia'. Terzo: la condizione di accensione era esplicita e verificabile, non un giudizio, cioe' la misura in produzione da un soak intero con `kv_expired_not_pruned` sotto soglia. Era prevista anche una variante, accendere il KV di default sul solo self-hosted, dove l'operatore e' il cliente stesso e non ci sono vicini.

**Perche' non ha vinto, e cosa si e' accettato pagando.** Il primo e il terzo argomento non sono stati negati: sono stati **spostati**, dal boot al control plane. Il gate per cella diventa la riga di `queen.kv_quota`, che con la tenancy attiva e' un diniego quando manca (§9.4 punto 1), quindi su cella condivisa la concessione resta esplicita e diventa **per tenant** invece che per cella, che e' un posto migliore; e la regola del soak di §16 resta in piedi identica. Il secondo argomento invece resta vero e **non e' stato risolto**: e' stato accettato, ed e' registrato come porta a senso unico in §17.3 con il suo prezzo scritto per intero (dal momento in cui i clienti scrivono nel KV, spegnere non e' configurazione, e' perdita di dati). Quello che si perde davvero e' il rilascio al buio, e non va nascosto in una nota: e' §16 a dirlo per esteso, insieme alle quattro reti piu' sottili che lo sostituiscono.

**20.5 `client-cli` riceve kv e timers, si' o no. Scadenza: inizio F6.**
Proposta: **no in v1**, e la pagina della CLI **deve dirlo esplicitamente**, perche' un'omissione silenziosa si legge come dimenticanza e produce issue. Ragione: la CLI e' l'unico client il cui valore per questa feature e' l'ispezione (peek e list dei timer, get di una chiave), non la scrittura, e l'ispezione la copre gia' l'HTTP diretto. Se invece la vuoi, sono circa 1,5 giorni e cambia `reference/queenctl.mdx` e la sua lista di sorgenti.

**20.6 `delayMs` o `delaySeconds` sul wire dei timer. APERTA, e vale come porta a senso unico. Scadenza: fine F0, con 20.1.**
Emersa applicando 20.1. §4.2 stabilisce che il wire dei timer porta **solo durate relative** (`delayMs`), mai istanti assoluti, perche' l'orologio deve essere uno solo ed e' quello di Postgres. Con `ttlSeconds` ratificato per il KV, il prodotto si ritrova comunque **due unita' di durata sul wire**, cioe' esattamente la doppia convenzione che 20.1 voleva eliminare.

Proposta: **tenere `delayMs`**, e dichiarare la regola invece di subirla. Le due quantita' sono diverse in natura: un backoff di retry a 250 ms e' un caso d'uso reale e centrale per i timer, un TTL sotto il secondo non lo e' per nessuno. La regola diventa una riga scrivibile in documentazione, *le durate che possono essere sotto il secondo sono in millisecondi, quelle che non possono sono in secondi*, ed e' una regola, non un incidente. L'alternativa e' `delaySeconds`, che compra l'uniformita' totale e **perde il backoff sotto il secondo**, che dei timer e' uno degli usi migliori.

Se si sceglie `delaySeconds`, va scelto **adesso**: dopo che sette client mandano `delayMs` il campo non e' rinominabile, esattamente come per 20.1.

**20.7 Il ramo `duplicate` del fuoco e' irraggiungibile: si toglie dalla tassonomia, o il fuoco paga la sonda che §6.2 ha rifiutato. APERTA, reversibile. Scadenza: fine F5, cioe' prima che la pagina internals dei timer si scriva in F7 e prima del GA.**
Emersa in verifica dell'implementazione, non nel design, ed e' il motivo per cui non ha un default proposto: le due uscite scambiano un costo sul percorso caldo contro una tassonomia pubblica, §6.2 quel costo lo ha gia' rifiutato una volta con una misura, e riproporlo qui come default sarebbe rovesciare quella decisione di soppiatto. Il fatto, misurato e non dedotto, e' in §6.2: con `p_verified = v_last` la sonda di dedup non gira (`003_log_push.sql:129`, `:148-149`), l'unica altra via al ramo e' chiusa da un `RAISE` (§6.2 punto 8), e un timer il cui `txn` e' gia' nel log viene appeso una seconda volta (`server/tests/timers_fault_injection.rs:374-395`). Il ramo resta implementato, corretto e morto.

**Ramo A, togliere `duplicate` dalla tassonomia dichiarata.** Il codice non si tocca, il ramo resta come difesa in profondita' dell'allocatore condiviso; cambia la tassonomia pubblica di §4.1, §12 e §14.2, che diventa `fired | stale`. Costo: circa mezza giornata (tre punti nel piano, la riga di metrica, la pagina internals, la nota nel test che vieta di "aggiustarlo" facendo comparire il `duplicate`). Zero costo sul percorso caldo. Si paga in due punti, entrambi da accettare esplicitamente: la contabilita' di righe del ramo resta **per sempre** senza copertura end to end, e `queen_timers_fired_total{result="duplicate"}` resta una serie che il broker sa emettere ma che il contratto non nomina, quindi il giorno in cui comparisse nessun cruscotto la aspetta. E soprattutto si accetta la conseguenza di prodotto: **il fuoco non ha nessuna difesa contro un `txn` gia' consegnato**, quindi un reschedule dopo il fuoco, o un `txn` riusato da un client, produce un secondo messaggio nel log (§20.2).

**Ramo B, far pagare al fuoco la sonda.** Rende il ramo raggiungibile e restituisce al fuoco la difesa, al prezzo che §6.2 aveva rifiutato. Non esiste la versione economica, e va detto prima di stimare: `queen.log_txns` ha PK `(partition_id, base_offset)` e tiene gli hash in una colonna `BYTEA` senza nessun indice (`001_log_schema.sql:112-119`), quindi cercare **un solo** `txn` costa comunque lo srotolamento della finestra ritenuta della partizione, esattamente come cercarne `count[i]`. L'unica leva vera e' **dove** si paga.
- **B1, `p_verified = -1`.** E' la forma gia' bocciata: il lavoro finisce **dentro** il serializzatore di push della partizione di destinazione, dopo il `FOR UPDATE`, quindi lo pagano i produttori normali e la telemetria lo attribuisce al push, non al fuoco. Costo: nessun lavoro di sviluppo, e la regressione descritta in §6.2 con il suo scenario numerico. La misura che lo bocciava non e' cambiata.
- **B2, sonda mirata sul solo `txn` del timer.** Stessa span di scansione di B1 ma un solo hash sul lato entrante, sotto il pre-lock e cappata alla finestra ritenuta. Costo: 1 giorno di SQL, piu' **1 giorno di misura sul rig obbligatoria e non stimabile a tavolino** (PG :5455, mai lo stack live), perche' e' esattamente la classe di costo di §18.6; piu' il test end to end che oggi non esiste. Se la misura dice che dentro il serializzatore non ci sta, la variante e' spostare la sonda **prima** del pre-lock, il che la degrada a un suggerimento che puo' perdere la corsa, e allora va chiamata suggerimento e non rete.

La domanda per Alice e' una sola: **il fuoco deve avere una difesa contro un `txn` gia' consegnato, oppure la difesa e' il contratto "delete piu' push in una transazione" e basta, con il resto scritto in documentazione?** Se la risposta e' la seconda, e' il ramo A e va chiuso subito, perche' tre sezioni del piano dichiarano oggi una tassonomia che il prodotto non produce.

**Non aperte, decise in questo piano** e ricordate qui solo perche' i design le lasciavano in sospeso: `QUEEN_SWEEPER_PARALLELISM` a 1 (sbaglia dalla parte del percorso messaggi, si alza solo su misura); `messageId` promesso allo schedule; `too_late` simmetrico fra cancel e reschedule, una regola sola; `p_in_wire` come flag e non come seconda SP; nessun `NOTIFY` di nessun tipo; il rider pop fuori dalla v1; `putIfAbsent` che ritorna **il valore** del vincitore e non solo il booleano, perche' altrimenti il perdente paga un round trip sul percorso piu' caldo del prodotto.





