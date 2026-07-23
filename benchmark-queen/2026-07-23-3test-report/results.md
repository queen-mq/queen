# 2026-07-23 — Report finale 3 test (mockup Alice): 10 min ciascuno, risoluzione 1s

VM: bench queen-01 139.59.151.52 (32c/62GB), loader 138.68.87.245 (48c).
Broker `queen-seg-rust:fix7` (= fix6 + reloptions churn) su branch rustserverandstorage.
Samplers: bench-sampler.sh + loader-sampler.sh (1 Hz), goload `-report 1`.
PNG unico: `queen-3test-report.png` (3 colonne × 6 righe). Raw in `raw/`.

## Test 3 — Channel manager (real-app), PRIMO per richiesta

**25.000 eventi/s end-to-end × 600 s — CERTIFICATO con ordine totale.**

- 2 flussi (availability + prices ~2KB), 4 code × 1000 partizioni (1 per property),
  push singoli in ingresso, dedup 300 s, ack espliciti bulk per batch, wildcard pop
  (pop-partitions 10, batch 100) come da direttiva.
- Consegnati **88.503.408 messaggi** su 12 stream (2 intermedi + 10 gruppi OTA):
  **0 duplicati, 0 perdite, 0 violazioni d'ordine** (verifier su file di log per-stage,
  frontiera contigua per property). ackErr=0, shed=0.
- Regime: push ~50k msg/s (25k ingressi singoli + 25k derivati batched), pop ~150k msg/s,
  ack ~150k msg/s ⇒ ~350k operazioni-messaggio/s sostenute. e2e p50 ~1,5-1,8 s,
  p99 ~8-12 s (dominato dalle attese di coda multi-hop + sleep di lavoro simulato
  10-30 ms per hop). PG ~19 core, Queen ~7 core, commit ~14k/s, DB plateau ~8 GB
  (retention attiva).
- Loader: ~14 core, ~0,5 Gbps per direzione.

### Percorso per arrivarci (fix implementati oggi)

1. **Warmup pre-rated** (goload `-warmup`, default on): push seq-0 per ogni property a
   concorrenza 96 + drain, PRIMA del pacer. In produzione le property pre-esistono; la
   creazione fredda di 4000 partizioni sotto carico è un wedge misurato.
2. **Livelock da creazione di massa (bug reale, fixato in SQL)**: `ON CONFLICT DO NOTHING`
   su chiavi in inserimento da altre txn aperte aspetta lo xid dell'ALTRA txn fino al
   commit; con pop wildcard multi-candidato in ordine random le attese si concatenano
   (misurato: 200+ backend su Lock:transactionid, txn pop vecchie minuti, commit/s→~0,
   nessun ciclo ⇒ deadlock detector muto). Fix:
   - 042: le INSERT di provisioning (log_queues/queues/log_partitions) ora ORDINATE
     sulla chiave unica (stesso principio del pre-lock `ORDER BY p.id`).
   - 043 `log_pop_v1`: guard `pg_try_advisory_xact_lock` NON bloccante attorno alla
     creazione lazy della riga consumer (uno crea, gli altri saltano il giro).
3. **Emissione derivata batched**: i messaggi derivati di una property escono in UN solo
   push batched (l'ordine dell'array = ordine seq, append atomico server-side) invece di
   N push singoli seriali: senza, il ciclo di pop durava secondi a RTT congestionato.
4. **Churn scans (bloat)**: a 30k+ update/s le scan candidate wildcard su
   log_partitions/log_consumers passavano da ~2ms a 12-35ms (verioni morte tra i vacuum).
   Mitigazioni: reloptions threshold-based su entrambe le tabelle (041) + vacuum-keeper
   1 Hz durante il run (`/root/vacuum-keeper.sh`, finding per tuning di prodotto).
5. Tuning boot per push singoli: FHOLD=5ms, FSHARDS=32, MAXINFLIGHT=192, BUNDLEMAX=64,
   Vegas push lane PINIT=512/PMAX=2048 (il tetto ingress a 25k req/s era l'ammissione).

**Limite trovato (wildcard-only)**: a 35k eventi/s l'ingresso tiene (~36k push singoli/s)
ma il consumo intermedio satura ~29k eventi/s ⇒ lag ingress cresce lineare. Il costo
strutturale è la candidate scan wildcard sotto churn dell'allocatore. 25k è il punto
sostenibile con margine.

## Test 1 — autoAck (QOS0) + dedup 300 s

**1M msg/s per lato push-batch 100, 100 partizioni, 850 consumer, pop-batch 1000, pp10.**

- Regime (t=30→~580 s): 1M/s pieni, p50 ~118 ms, p99 ~305-334 ms, lag ~40k, errori ~500
  cumulativi su 558M messaggi.
- **Run finale (4° tentativo) — PULITO**: 585.023.400 offerti → 584.522.800 consegnati
  (99,91%), **shed=0, popErr=0**, pushErr 3.032 (0,0005%), lag finale 3.300.
  Regime: p50 ~90-120 ms, p99 ~230-450 ms. PG ~15 core, Queen ~9-10 core,
  commit ~5-6k/s, DB plateau ~20 GB. Un hiccup con recupero completo a t≈575
  (dip push + surge pop, lag torna ~0 in <10 s).
- **Finding nuovo dal formato 10-minuti (3 OOM diagnosticati e fixati)**: ai ~580 s il
  budget RAM della VM (62 GB) esplodeva —
  1° tentativo: OOM broker con cache dedup cap 28 GB (64 B/hash × 300 s × 1M/s = 19,2 GB
  steady + PG shared_buffers 24 GB). 2°: cap 20 GB tiene (19,8 GB piatti) ma pg_mem 41 GB
  ⇒ OOM t≈583 s. 3°: work_mem 12 MB non basta — il colpo finale è il burst della prima
  passata autovacuum su log_segments (4 worker × maintenance_work_mem 2 GB).
  **Fix validato**: shared_buffers 16 GB + work_mem 12 MB + maintenance_work_mem 512 MB
  + cache 20 GB ⇒ oom=false. NB: la RSS totale del broker arriva a ~35 GB (cache 20 +
  overhead/malloc) — rafforza il follow-up densità cache (16-20 B/hash ⇒ 19,2→~5 GB).
  I run da 300 s non lo vedevano MAI: finivano esattamente prima del riempimento.

## Test 2 — Ack espliciti (QOS1) + dedup 300 s

**900k msg/s per lato, 200 partizioni, 600 consumer, pop-batch 500, pp1, ack-async.**

- **PULITO**: 526.506.800 offerti → 526.368.500 consegnati (99,97%), **shed=0,
  popErr=0**; acked 526.299.200, ackErr 15.600 (0,003%, il wobble noto), ackAvg ~67 ms.
  Regime: p50 ~125-146 ms, p99 ~301-358 ms, ack rate ≈ pop rate ~900k/s.
- **Escursione con recupero**: t≈220-430 il pop rallenta (~30 s) e il lag sale fino a
  ~6M (≈7 s di buffer), poi pieno recupero a lag≈0 senza perdite — stessa classe del
  wobble ackErr noto (da investigare: coincide con la prima ondata retention/vacuum).

## Note di lettura del PNG

- Riga 1: throughput msg/s (push/pop/ack; per T3 anche eventi e2e/s).
- Riga 2: lag (T3: per coda).
- Riga 3: latenza p50/p99 in ms, scala log (T3 = end-to-end multi-hop CO-corretta
  dall'istante schedulato del producer; T1/T2 = push→pop).
- Riga 4: core e memoria di PG e Queen (bench-sampler, cgroup).
- Riga 5: commit/s, WAL fsync/s, dimensione DB.
- Riga 6: loader (goload core, RSS, rete).
- Gli aggregati `[final]` di goload includono il teardown (errori di drain conteggiati
  in blocco): leggere le serie per-secondo, non i totali finali.
