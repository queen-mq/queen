# 19 — Wildcard candidate hot-list (broker-side discovery)

Stato: IMPLEMENTATA e validata su VM (2026-07-24). Flag `QUEEN_HOTLIST` default ON (kill switch =0).
Obiettivo: eliminare la candidate scan SQL per-pop del wildcard, sostituendola
con una selezione in memoria nel broker. L'SQL resta l'unica autorità su claim,
lease, cursori e visibilità: la hot-list può solo essere *stale in eccesso*
(falso positivo = un claim SKIP LOCKED a vuoto ~0,2ms), mai nascondere lavoro
oltre il floor del reseed.

## 1. Motivazione (misurata 2026-07-23, VM 32c)

La wildcard candidate query (`log_partitions ⋈ log_consumers`, filtri hot/
pending/lease-free, `ORDER BY random() LIMIT cap`) costa 12ms a 1000 partizioni
e 35ms sotto churn dell'allocatore (dead versions al rate dei messaggi; il
vacuum non tiene il passo). A 2.500 pop/s = 20-30 core PG di sola selezione,
pagati anche (soprattutto) da code quasi vuote. Ceiling misurato del workload
channel-manager: ~29k eventi/s, interamente scan-bound. In idle il costo scala
con consumer × partizioni × poll-rate — proibitivo per il multitenant.

## 2. Architettura

Per (coda, gruppo) il broker mantiene l'insieme delle partizioni
"probabilmente pending". Tre sorgenti di verità approssimata, tutte già
disponibili su choke point esistenti:

- **mark** — la flush della fusion, a COMMIT riuscito, conosce (coda,
  partizione, n. messaggi): `pending=true; epoch++`. Incondizionato.
- **clear-su-ack** — l'Ack Registry conosce `(partition, group) → batch_end`:
  se lo shadow last_offset ≤ batch_end acked → clear.
- **clear-su-pop-vuoto** — SOLO via epoch-CAS (protocollo §4) e MAI su code con
  visibilità differita (§6): lì diventa `revisit_at`.

Il wildcard pop (handler) con flag attivo: prende K candidati dal ring (§3),
chiama `queen.log_pop_list_v1(ids[], ...)` — il loop candidati esistente su
`FROM unnest($ids)`, riusa `log_pop_v1` INVARIATO — e ri-accoda i candidati
claimati/parziali. Hot-list vuota ⇒ park SENZA toccare PG; la wake arriva dal
mark (locale o mesh).

## 3. Strutture (target: 100k/200k/500k partizioni)

Regole: niente stringhe nei percorsi caldi, niente strutture che fanno resize,
niente O(partizioni) a regime (lezione OOM 2026-07-23: mai allocazioni
proporzionali all'intero stato).

- **Interning**: partizione → indice denso `u32`, registry per coda,
  append-only a blocchi. Le stringhe vivono solo qui (~35MB @ 500k).
- **Per (coda, gruppo)**: ring intrusivo su array piatti indicizzati per
  partizione: `next,prev: u32; epoch: u32; flags: u8; revisit_at: i64;
  batch_count: u32` ≈ 24-28B per (P,G). Insert O(1) solo alla transizione
  vuoto→pending; estrazione K dalla testa; re-append in coda (round-robin equo,
  sostituisce ORDER BY random()). Push su partizione già pending = bump atomico
  epoch, zero ring op — a 1M msg/s i mark sono quasi tutti no-op atomici.
- **Sub-sharding**: ring spezzato per `indice % S` (S ~ shard fusion) per
  evitare un mutex caldo su una singola mega-coda; il pop preleva dai sub-ring
  round-robin.
- Budget: 500k partizioni × 10 gruppi ≈ 120-140MB. 100k×10 ≈ 24MB.

## 4. Protocollo epoch-CAS (la race push-vs-pop-vuoto)

- mark (post-COMMIT): `pending=true; epoch++` — sempre.
- pop dispatch: fotografa `e = epoch` prima di inviare l'SQL.
- risposta vuota: clear solo se `epoch == e`.

Interleaving: commit-dopo-snapshot+mark-prima-del-clear ⇒ CAS fallisce ✓;
commit-prima ⇒ pop non vuoto ✓; mark RITARDATO oltre il clear ⇒ il clear passa
ma il mark tardivo ri-aggiunge (set incondizionato) ⇒ autocorrezione in µs ✓.
Ogni race degrada a falso positivo, mai a falso negativo oltre il reseed.
Crash tra COMMIT e mark ⇒ restart ⇒ cold start (§8). Hint mesh perso ⇒ reseed.

## 5. Cross-broker: hint sulla mesh TCP (richiesto da subito)

Sulla mesh esistente (già usata per le wake dei parked pop) viaggia
`dirty(queue_id, partition)` alla TRANSIZIONE pending, non per messaggio:
coalescing per finestra 10-50ms, batch di indici — a 1M msg/s su partizioni
calde sono decine di hint/s. Ricezione = mark (stesso path, stessa semantica
epoch). La mesh è il fast path; il floor di correttezza cross-broker resta il
reseed (§8). Nessun nuovo protocollo di consistenza: gli hint sono idempotenti
e commutativi per costruzione (set+bump).

## 6. Visibilità differita: delayed + windowBuffer unificati

Entrambi = "pending ma non ancora visibile". Regola: con deferral attivo il
pop vuoto NON fa mai hard-clear.

- **Timer wheel gerarchica** (granularità ~100ms, stesso nodo del ring, un link
  in più): mark su coda con delay D ⇒ entry nella wheel a `commit_ts_pg + D`;
  allo scadere: promossa al ready ring + wake dei parked pop.
- **Pop vuoto su coda deferral** ⇒ `revisit_at` (fase 1: backoff bounded
  50ms→1s; fase 2: l'SQL del pop espone nel meta "earliest invisible visible
  at T" e il CAS imposta revisit=T).
- **Orologio**: scadenze SEMPRE da timestamp PG (commit flush / meta pop) con
  padding ~300ms; il clock del broker serve solo a schedulare il retry.
- **windowBuffer, semantica piena**: consegna a `min(scadenza finestra, batch
  pieno)` — il contatore `batch_count` nell'entry permette la promozione
  anticipata quando il batch è già grasso. Verificare PRIMA quanto del
  windowBuffer del rows engine è realmente implementato nel log engine e dare
  qui la semantica pulita.

## 7. Lease, renew e transaction

Le lease NON sono modellate nella hot-list: l'arbitro resta `log_pop_v1`.
Il punto d'incontro è il risultato per-candidato di `log_pop_list_v1`, che è
un TRI-STATO (il dato è già nella riga letta dal claim, costo zero):

- `preso N` — normale;
- `vuoto` — clear via epoch-CAS (§4); su code deferral → revisit (§6);
- `leased fino a T` — MAI clear: `revisit_at = T + pad`. Un candidato occupato
  non è un candidato esaurito.

Hook simmetrico sull'ack (registry e path transazionale): oltre al
clear-se-coperto, se l'entry è ancora pending (push arrivati durante la lease)
la PROMUOVE subito al ready ring + wake — ack = lease liberata = claimabile
ora, non alla scadenza. Il renew lease non ha hook: la revisita a T trova la
lease rinnovata, legge il nuovo expires_at e si ri-arma (costo: un claim
~0,2ms per intervallo di renewal). Worker morto / rollback implicito: alla
scadenza della lease la revisita trova il batch redeliverabile — copertura
naturale, nessun caso speciale.

**Transaction — principio: il mark va su OGNI path che fa comparire
messaggi.** I push transazionali NON passano dalla flush della fusion:
committano nella procedura di transaction. Enumerazione dei path con mark
obbligatorio:

1. flush fusion (caso base, §2);
2. commit della transaction: mark su tutte le (coda, partizione) coinvolte +
   hook ack/promote per le partizioni ackate nel txn;
3. move verso DLQ (dopo max-retry): mark sulla hot-list della coda DLQ;
4. requeue amministrativi / replay file_buffer post-crash: se rientrano dal
   push path ereditano il mark, altrimenti hook esplicito.

Rollback safe per costruzione: ogni hook di clear/promote scatta solo su
evidenza COMMITTATA; un txn abortito non fa clear di nulla, un suo mark in
eccesso è il solito falso positivo. Un path dimenticato degrada a latenza di
scoperta bounded (reseed), mai a perdita.

## 8. Reseed e cold start

- **Reseed**: la candidate query di oggi degrada a fallback, paginata KEYSET
  (chunk ~10k in ordine di id, throttled, staggered 30-60s per coda/gruppo).
  Mai più ORDER BY random() su set grandi. È il floor di correttezza di tutto
  (§4, §5).
- **Cold start**: al boot niente scan — TUTTO pending (all-set), i pop vuoti
  puliscono via CAS. 500k claim a vuoto × 0,2ms spalmati sui consumer ≈ 100
  core-secondi una tantum, e converge allo stato vero da solo.

## 9. Modifiche

- SQL: `log_pop_list_v1(ids uuid[]/…)` = loop candidati esistente su unnest,
  con risultato TRI-STATO per candidato (preso/vuoto/leased-fino-a-T, §7);
  `log_pop_v1` invariato. (Fase 2: campo earliest-visible nel meta.)
- Broker: modulo hotlist (interning, ring, wheel, epoch-CAS, reseed) + hook in
  flush-commit (fusion.rs), process_acks/registry, render_pop_parts (empty
  path), parked-wake, mesh (hint dirty). Flag `QUEEN_HOTLIST` default off:
  spento = percorso attuale byte-identico.

## 10. Validazione

- Unit: ring/interning (transizioni, round-robin, no-resize), epoch-CAS (tutti
  gli interleaving §4 come test deterministici), wheel (promozione, revisit,
  batch-fat early promotion), reseed keyset.
- Locale (rig :5457): cm-mode PASS con flag on/off; openloop smoke; test
  funzionali lease (candidato leased → revisit, ack → promote immediata),
  transaction (push+ack txn → mark/clear corretti, rollback neutro), DLQ
  (move → mark della coda DLQ); test funzionali delayed e windowBuffer (visibilità rispettata, nessun messaggio
  perso/anticipato); kill-e-restart broker a metà run (cold start converge).
- VM (quando disponibili): A/B col workload Test 3 — numeri da battere:
  ceiling 29k eventi/s e 20-30 core di candidate scan in pg_stat_statements
  che devono sparire; idle: PG a riposo con consumer parcheggiati.

## 11. Attese quantitative (dalle misure 2026-07-23)

- Sotto carico (forma Test 3): PG −35-40% a parità di rate; ceiling ~2x
  (29k → push-bound ~45-50k eventi/s).
- Forma T1/T2: 3-8 core recuperati, nessuna regressione possibile.
- Idle: costo SQL ~0 indipendente dal numero di code/partizioni — il costo di
  fondo scala col traffico, non con la flotta. Prerequisito multitenant.
