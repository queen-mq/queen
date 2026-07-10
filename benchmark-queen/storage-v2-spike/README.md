# Storage v2 spike — segments engine, greenfield

Prototipo funzionante e misurato del motore storage v2 di Queen: **una riga =
un segmento di K messaggi** (frame length-prefixed impacchettati dal broker e
compressi insieme con zstd), su schema greenfield senza vincoli di
compatibilità. Confrontato head-to-head con il motore v1 reale
(`queen.messages` + `push_messages_v3` / `pop_unified_batch_v4` /
`ack_messages_v2`, le stesse chiamate che dispatcha `lib/queen/pending_job.hpp`).

## Design

- **Identità segmento = `(partition_id, seq bigint)`**, con `seq` allocato da
  `UPDATE q2.partitions SET last_seq = last_seq + 1` — il row lock serializza i
  push per partizione (sostituisce l'advisory lock PUSHSER) e garantisce
  seq-order == commit-order: un pop non può mai vedere seq N+1 senza N.
- **Zero indici secondari sulla tabella hot**: la PK è anche il percorso del
  pop. Un solo btree in tutto il motore messaggi.
- **Cursore = `(next_seq, next_off)`** per (partition, consumer_group), con
  guardia esplicita: l'offset si applica solo se la prima riga letta ha
  esattamente `seq = next_seq` (caso segmento-cancellato-da-retention).
- **Dedup opt-in per coda** (`queues.dedup_window_seconds`, 0 = off) su tabella
  a finestra `q2.dedup (partition_id, hashtextextended(txn))`: probe+insert in
  un solo statement `ON CONFLICT`; un duplicato abortisce la call (rollback →
  niente gap di seq) e il chiamante risolve con `find_dups_v1` e reimpacchetta
  (path raro per design). Quando il transactionId è generato dal server, la
  dedup si salta del tutto — lavoro che v1 non può evitare.
- **Retention a watermark** (`partitions.retention_seq`): il boundary walk
  riparte da dove si era fermato, mai dalla testa morta dell'indice.
- **Ack per posizione assoluta** clampata al batch in lease; ack parziale a
  metà segmento = redelivery dal frame esatto.

## Risultati (run-20260710-112620-K50)

600k messaggi, payload JSON ~256B, K=50, 8 partizioni, 4 worker, PG 17
pinnato a 2 core / shared_buffers 512MB / synchronous_commit=on.

| metrica | v1 (righe) | v2 (segmenti) | guadagno |
|---|---|---|---|
| ingest msg/s | 36.2k | 50.7k | **1.4x** (a -27% CPU PG) |
| ingest WAL/msg | 970 B | 392 B (119 B senza dedup) | **2.5x / 8.1x** |
| heap B/msg | 512 | 120 | **4.3x** |
| indici B/msg | 242 | 1.4 | **173x** |
| totale B/msg (incl. dedup window) | 754 | 267 | **2.8x** |
| solo tabella segmenti B/msg | 754 | 122 | **6.2x** |
| consume msg/s | 22.4k | 116k | **5.2x** (a 1/4 della CPU) |
| consume WAL/msg | 754 B | 4 B | **~190x** |
| retention sweep 600k | 5.4 s | 0.34 s | **16x** |
| retention WAL/msg | 566 B | 137 B | **4.1x** |
| zstd di gruppo | — | 3.32x | |

A/B/A/B alternato (300k, variabilità da laptop): v1 46-49k, v2 63-88k,
v2-senza-dedup 78-90k msg/s.

Nota di lettura: la dedup window in steady-state è proporzionale alla
*finestra*, non alla retention — i 146 B/msg di dedup del bench (finestra mai
scaduta) sono il caso peggiore; il numero strutturale della coda è 122 B/msg.

## Onestà del confronto

A favore di v1 (i numeri v2 sono conservativi):
- il driver fa zstd e slicing in JS single-thread (in produzione lo fa il
  broker C++ multi-thread);
- il bench NON fa pagare a v1 `update_partition_lookup_v1` dopo ogni push né
  altri costi di produzione (watermark, stats).

A favore di v2:
- pop per partizione esplicita (niente scan wildcard) per entrambi;
- payload sintetici con chiavi ripetute (ma è la forma tipica del traffico MQ);
  il 3.32x di compressione va rivalidato su payload di produzione veri.

Non coperto dallo spike: integrazione nel server C++, pop wildcard/watermark,
DLQ, traces, streams, renew lease, cifratura.

## Come si usa

```bash
./run-bench.sh                       # bench completo v1 vs v2 (K=BATCH, default 50)
PGURL=... node correctness-test.mjs  # 19 assert: round-trip, dedup, ack parziale,
                                     # lease, retention guard, exactly-once concorrente
MSGS=... BATCH=... DEDUP=0 node bench-driver.mjs <fase>   # fasi singole
```

Il motore è passato da una review avversariale multi-agente (concorrenza,
meccanica PG, fairness del bench); i fix applicati: fillfactor 70 su
partitions (HOT), watermark retention, guardia NULL nell'ack, UUID
deterministici nel driver, dedup single-statement.
