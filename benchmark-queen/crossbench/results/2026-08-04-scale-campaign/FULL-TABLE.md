# Tabella riassuntiva completa — campagna 2026-08-03/04

Broker `bench-01` e loader `bench-02`, **32 vCPU / 62 GB** ciascuno, Xeon 8358, NVMe.
Parità di risorse per tutti i sistemi (`CM_CPUS=32 CM_MEM=56g`, Kafka heap 16 GB),
Postgres risizato per il box, `synchronous_commit=on` / `fsync=on` invariati.
Gruppo H è sul **vecchio box da 8 core** e non è confrontabile con gli altri.

CPU = media sulla finestra attiva (campioni cgroup a 1 Hz, soglia 0,3 core);
RAM = picco RSS sommato dei container del sistema; disco = scrittura media host.
`%` = consegne servite / consegne richieste (rate × 6).

| cella | sist. | rate ev/s | lane ord. | code | cons | p50 ms | p95 ms | p99 ms | consegne/s | % | core | RAM GB | disco MB/s | corr. |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:--:|
| **A. Matrice di confronto — 32 core, parita risorse, 1000 lane** | | | | | | | | | | | | | | |
| MX-queen-sparse | queen | 2,000 | 4,000 | 4 | 48 | 71.5 | 92.7 | 1,048.6 | 11,700 | 97% | 7.4 | 1.8 | 51 | PASS |
| MX-pgmq-sparse | pgmq | 2,000 | 12,000 | 12 | 96 | 55.1 | 65.5 | 440.9 | 11,700 | 97% | 6.5 | 2.2 | 65 | PASS |
| MX-rabbit-sparse | rabbit | 2,000 | 12,000 | 12000 | 12000 | 92.7 | 120.2 | 2,965.8 | 11,699 | 97% | 7.0 | 2.6 | 62 | PASS |
| MX-kafka-sparse | kafka | 2,000 | 4,000 | 4 | 48 | 142.9 | 170.0 | 12,937.0 | 11,700 | 98% | 2.2 | 11.6 | 15 | PASS |
| MX-queen-dense | queen | 12,000 | 4,000 | 4 | 96 | 340.0 | 881.7 | 1,143.5 | 69,110 | 96% | 16.0 | 4.5 | 77 | PASS |
| MX-kafka-dense | kafka | 12,000 | 4,000 | 4 | 48 | 2,287.0 | 5,439.3 | 5,931.6 | 64,252 | 89% | 3.9 | 12.2 | 22 | PASS |
| MX-pgmq-dense | pgmq | 12,000 | 12,000 | 12 | 96 | 11,863.3 | 16,777.2 | 16,777.2 | 45,622 | 63% | 19.3 | 5.1 | 111 | PASS |
| MX-rabbit-dense | rabbit | 12,000 | 12,000 | 12000 | 12000 | 19,951.6 | 39,903.2 | 43,514.7 | 19,666 | 27% | 9.7 | 2.7 | 162 | PASS |
| | | | | | | | | | | | | | | |
| **B. Head-of-line — P=20.000, 6.000 ev/s, ogni sistema alla sua cardinalita** | | | | | | | | | | | | | | |
| HOL-queen-dynamic | queen | 6,000 | 80,000 | 4 | 96 | 101.1 | 2,493.9 | 15,384.8 | 36,498 | 101% | 14.6 | 7.1 | 70 | PASS |
| HOL-pgmq-full | pgmq | 6,000 | 240,000 | 12 | 96 | 71.5 | 881.7 | 5,439.3 | 36,500 | 101% | 15.3 | 7.0 | 101 | PASS |
| HOL-queen-1k | queen | 6,000 | 4,000 | 4 | 96 | 110.2 | 1,923.1 | 7,054.0 | 36,499 | 101% | 14.7 | 5.1 | 61 | PASS |
| HOL-pgmq-1k | pgmq | 6,000 | 12,000 | 12 | 96 | 71.5 | 881.7 | 5,439.3 | 36,500 | 101% | 15.0 | 6.7 | 97 | PASS |
| HOLHI-queen-dyn | queen | 6,000 | 80,000 | 4 | 96 | 101.1 | 3,846.2 | 19,951.6 | 36,498 | 101% | 14.8 | 6.7 | 73 | PASS |
| HOLHI-queen-200 | queen | 6,000 | 800 | 4 | 96 | 131.1 | 2,493.9 | 8,388.6 | 36,499 | 101% | 7.6 | 1.2 | 37 | PASS |
| HOLHI-kafka-200 | kafka | 6,000 | 800 | 4 | 48 | 142.9 | 4,194.3 | 8,388.6 | 36,500 | 101% | 4.6 | 1.6 | 12 | PASS |
| HOLHI-pgmq-200 | pgmq | 6,000 | 2,400 | 12 | 96 | 85.0 | 961.5 | 5,931.6 | 36,500 | 101% | 15.5 | 6.8 | 96 | PASS |
| HOL-rabbit-1k | rabbit | 6,000 | 12,000 | 12000 | 12000 | 23,726.6 | 43,514.7 | 43,514.7 | 20,697 | 57% | 12.0 | 3.5 | 184 | PASS |
| | | | | | | | | | | | | | | |
| **C. Scala di cardinalita Kafka — P=20.000, 6.000 ev/s** | | | | | | | | | | | | | | |
| KLAD-2000 | kafka | 6,000 | 8,000 | 4 | 48 | 155.9 | 23,726.6 | 28,215.8 | 36,500 | 101% | 2.9 | 1.3 | 28 | PASS |
| KLAD-5000 | — | — | — | — | — | — | — | — | — | — | — | — | — | assente |
| KLAD-10000 | — | — | — | — | — | — | — | — | — | — | — | — | — | assente |
| KLAD-20000 | — | — | — | — | — | — | — | — | — | — | — | — | — | assente |
| | | | | | | | | | | | | | | |
| **D. Ceiling pgmq — shape channel, 1000 gruppi** | | | | | | | | | | | | | | |
| PGC-l1000-r3000 | pgmq | 3,000 | 12,000 | 12 | 96 | 55.1 | 71.5 | 85.0 | 17,134 | 95% | 8.4 | 3.2 | 76 | PASS |
| PGC-l1000-r4500 | pgmq | 4,500 | 12,000 | 12 | 96 | 60.1 | 77.9 | 101.1 | 25,633 | 95% | 11.4 | 4.2 | 83 | PASS |
| PGC-l1000-r6000 | pgmq | 6,000 | 12,000 | 12 | 96 | 77.9 | 131.1 | 185.4 | 34,133 | 95% | 15.8 | 5.2 | 92 | PASS |
| PGC-l1000-r9000 | pgmq | 9,000 | 12,000 | 12 | 96 | 7,054.0 | 11,863.3 | 11,863.3 | 44,790 | 83% | 19.7 | 6.6 | 111 | PASS |
| PGC-l1-r2000 | pgmq | 2,000 | 12 | 12 | 96 | 39,903.2 | 123,078.2 | 134,217.7 | 665 | 6% | 17.1 | 1.7 | 15 | PASS |
| | | | | | | | | | | | | | | |
| **E. Riproducibilita Queen — stessa cella P=1000 r=12000, ripetuta** | | | | | | | | | | | | | | |
| REP-A-1 | queen | 12,000 | 4,000 | 4 | 48 | 679.9 | 2,493.9 | 3,234.3 | 69,181 | 96% | 10.2 | 3.0 | 68 | PASS |
| REP-A-2 | queen | 12,000 | 4,000 | 4 | 48 | 1,048.6 | 2,719.7 | 4,194.3 | 69,200 | 96% | 11.2 | 3.1 | 68 | PASS |
| REP-A-3 | queen | 12,000 | 4,000 | 4 | 48 | 741.5 | 2,493.9 | 3,527.0 | 69,196 | 96% | 10.6 | 3.0 | 68 | PASS |
| REP-A-4 | queen | 12,000 | 4,000 | 4 | 48 | 340.0 | 2,097.2 | 2,719.7 | 69,148 | 96% | 13.6 | 3.8 | 68 | PASS |
| REP-B-1 | queen | 12,000 | 4,000 | 4 | 96 | 623.5 | 3,846.2 | 4,194.3 | 69,134 | 96% | 15.5 | 4.2 | 68 | PASS |
| REP-B-2 | queen | 12,000 | 4,000 | 4 | 96 | 440.9 | 3,234.3 | 3,846.2 | 69,186 | 96% | 12.5 | 3.8 | 68 | PASS |
| REP-B-3 | queen | 12,000 | 4,000 | 4 | 96 | 2,965.8 | 5,439.3 | 5,931.6 | 68,025 | 94% | 15.3 | 4.3 | 68 | PASS |
| REP-B-4 | queen | 12,000 | 4,000 | 4 | 96 | 3,234.3 | 5,439.3 | 6,468.5 | 66,943 | 93% | 15.4 | 4.2 | 68 | PASS |
| | | | | | | | | | | | | | | |
| **F. Griglia della legge — assi decorrelati (Queen)** | | | | | | | | | | | | | | |
| LAW-B-p500-r6000 | queen | 6,000 | 2,000 | 4 | 96 | 110.2 | 340.0 | 741.5 | 34,596 | 96% | 15.3 | 3.2 | 64 | PASS |
| LAW-B-p1000-r6000 | queen | 6,000 | 4,000 | 4 | 96 | 85.0 | 881.7 | 1,482.9 | 34,698 | 96% | 14.6 | 3.2 | 66 | PASS |
| LAW-B-p2000-r6000 | queen | 6,000 | 8,000 | 4 | 96 | 92.7 | 142.9 | 881.7 | 34,897 | 97% | 13.2 | 3.4 | 70 | PASS |
| LAW-B-p1000-r3000 | queen | 3,000 | 4,000 | 4 | 96 | 71.5 | 185.4 | 623.5 | 17,449 | 97% | 10.9 | 2.2 | 54 | PASS |
| LAW-B-p1000-r12000 | queen | 12,000 | 4,000 | 4 | 96 | 679.9 | 4,194.3 | 5,439.3 | 69,142 | 96% | 12.2 | 3.7 | 72 | PASS |
| LAW-B-p2000-r12000 | queen | 12,000 | 8,000 | 4 | 96 | 1,048.6 | 2,719.7 | 3,527.0 | 69,400 | 96% | 11.0 | 3.4 | 65 | PASS |
| LAW-A-p500-r6000 | queen | 6,000 | 2,000 | 4 | 48 | 101.1 | 404.3 | 808.6 | 34,598 | 96% | 13.4 | 3.1 | 71 | PASS |
| LAW-A-p1000-r6000 | queen | 6,000 | 4,000 | 4 | 48 | 101.1 | 2,493.9 | 3,234.3 | 34,700 | 96% | 12.2 | 3.0 | 73 | PASS |
| LAW-A-p2000-r6000 | queen | 6,000 | 8,000 | 4 | 48 | 92.7 | 480.8 | 1,617.1 | 34,899 | 97% | 12.0 | 3.3 | 75 | PASS |
| LAW-A-p1000-r3000 | queen | 3,000 | 4,000 | 4 | 48 | 77.9 | 480.8 | 1,247.0 | 17,449 | 97% | 9.1 | 2.1 | 61 | PASS |
| LAW-A-p1000-r12000 | queen | 12,000 | 4,000 | 4 | 48 | 340.0 | 3,846.2 | 4,573.9 | 69,195 | 96% | 12.7 | 3.7 | 73 | PASS |
| LAW-A-p2000-r12000 | queen | 12,000 | 8,000 | 4 | 48 | 480.8 | 1,617.1 | 2,493.9 | 69,397 | 96% | 13.3 | 4.0 | 79 | PASS |
| | | | | | | | | | | | | | | |
| **G. Cardinalita estrema Queen — 100.000 property (400.000 lane)** | | | | | | | | | | | | | | |
| HOL-queen-dyn | queen | 10,000 | 400,000 | 4 | 96 | 21,757.4 | 79,806.3 | 94,906.3 | 62,068 | 103% | 9.3 | 14.7 | 59 | PASS |
| HOL-q100k-r3000 | queen | 3,000 | 400,000 | 4 | 96 | 18,295.7 | 94,906.3 | 103,496.0 | 36,499 | 203% | n/d | n/d | n/d | PASS |
| HOL-q100k-r1500 | queen | 1,500 | 400,000 | 4 | 96 | 43,514.7 | 123,078.2 | 134,217.7 | 26,687 | 297% | n/d | n/d | n/d | PASS |
| | | | | | | | | | | | | | | |
| **H. Catena di tuning 8 core (ieri) — shape ancora 2k/1000** | | | | | | | | | | | | | | |
| B-a9-pp40 | queen | 2,000 | 4,000 | 4 | 96 | 1,143.5 | 3,234.3 | 4,987.9 | 11,491 | 96% | n/d | n/d | n/d | PASS |
| A-t9-pp40 | queen | 2,000 | 4,000 | 4 | 96 | 440.9 | 1,143.5 | 2,097.2 | 11,694 | 97% | n/d | n/d | n/d | PASS |
| C-a9-min96 | queen | 2,000 | 4,000 | 4 | 96 | 240.4 | 404.3 | 1,763.5 | 11,697 | 97% | n/d | n/d | n/d | PASS |
| G-nofusion | queen | 2,000 | 4,000 | 4 | 96 | 185.4 | 480.8 | 1,763.5 | 11,696 | 97% | n/d | n/d | n/d | PASS |
| L-nof-w4 | queen | 2,000 | 4,000 | 4 | 48 | 170.0 | 623.5 | 1,923.1 | 11,699 | 97% | n/d | n/d | n/d | PASS |
| P-hold3 | queen | 2,000 | 4,000 | 4 | 48 | 142.9 | 240.4 | 1,482.9 | 11,697 | 97% | n/d | n/d | n/d | PASS |
| FINAL-180-w4 | queen | 2,000 | 4,000 | 4 | 48 | 185.4 | 524.3 | 1,247.0 | 11,733 | 98% | n/d | n/d | n/d | PASS |
| FINAL2-180-hold3 | queen | 2,000 | 4,000 | 4 | 48 | 185.4 | 741.5 | 1,247.0 | 11,733 | 98% | n/d | n/d | n/d | PASS |
| Q-180-retention | queen | 2,000 | 4,000 | 4 | 48 | 170.0 | 311.7 | 1,143.5 | 11,733 | 98% | n/d | n/d | n/d | PASS |
| | | | | | | | | | | | | | | |

## Note sulle celle

**KLAD-5000 / 10000 / 20000** — nessun risultato perché Kafka ha **rifiutato la
creazione dei topic**: `POLICY_VIOLATION: Request parameters do not satisfy the
configured policy`. Il muro sta fra 2.000 e 5.000 partizioni **per topic**. È un
rifiuto di configurazione (il compose non imposta `create.topic.policy`, quindi è
validazione interna di KRaft), non un limite fisico dimostrato — ma a 2.000 il p95
è già 23.727 ms, quindi alzare il limite non comprerebbe un sistema usabile.

**Gruppo G** — le percentuali sopra il 100% sono un artefatto: con 100.000 property
il warmup consegna 1,2 M messaggi contro ~1 M del rated window, quindi sia le
consegne sia i percentili sono dominati dal priming. Il segnale valido lì è il
`lag`: piatto a ~700 con rate 3.000, in fuga (7.437 → 129.695) a rate 10.000.

**Gruppo E** — stessa cella ripetuta otto volte con l'env del container riletto da
`docker inspect`. Set A (fusion OFF, hold 3, 4 wk) mediana 711, spread 3,1×;
set B (fusion ON, hold 15, 8 wk) mediana 1.795, spread **7,3×**. Le mediane su
tutte le misure della sessione sono equivalenti (~680 vs ~650); B ha la coda peggiore.

## Soak — Queen, 3h10m, semantica di produzione piena

200 partizioni, 600 consumer, pop-batch 500, lease + ack espliciti async
(inflight 256), dedup 60 s, retention completed 300 s / pending 3600 s,
payload 256 B, push-batch 100.

| | valore |
|---|---:|
| messaggi offerti / serviti | 6.822.018.300 / 6.821.967.400 |
| pushed / popped / acked | 6.821.967.400 / 6.821.920.800 / 6.821.901.600 |
| **shed** | **0** |
| **pushErr / popErr / ackErr** | **0 / 0 / 0** |
| lag finale | 46.600 (banda 44.600-80.000, nessuna tendenza) |
| p50 / p99 / p999 | **120,3 / 297,0 / 374,8 ms** |
| p50 campionata ogni ~15 min | 124, 107, 119, 115, 119, 120, 121, 116, 117 (**±8%**) |
| ackAvg | 81,3 ms |

Contro il soak 24 h di luglio (51,8 mld msg): p50 87,6 / p99 272 / p999 473,
63.368 errori. Qui **mediana 37% peggio, p99 9% peggio, p999 21% meglio, zero errori**.

## Tenant — `goload -mode tenants`

| scenario | code | traffico | p50 | p99 | messaggi | errori |
|---|---:|---:|---:|---:|---:|---:|
| SMALL 10×10 | 100 | ~56/s | 7,5-10,2 ms | ~39 ms | 14.390 | **0** |
| BIG 1000×10 | 10.000 | ~6.000/s | **80-121 ms** | 280-358 ms | 3.609.328 | **0** |

BIG è **3-4× meglio dello storico del 2026-07-24** (p50 334 / p99 610). 100 code
provisionate in 0,1 s; 10.000 consumer parcheggiati.

## pgmq — throughput raw (loader dedicato `cmd/pgmqload`)

Forma del soak tradotta nell'API pgmq: `send_batch` / `read_grouped_head` / `delete`.
1000 gruppi, push-batch 100, read-qty 500, payload 256 B, 90 s.

| rate | reader | pop/s (finestre successive) | depth finale | esito |
|---:|---:|---|---:|---|
| 20.000 | 4 | non tiene | 685.593 | ✗ |
| **20.000** | **16** | **20.000 · 20.000 · 20.001** | **54** | **✓ p50 17-34 ms** |
| 20.000 | 64 | 11.803 · 3.105 · 1.333 | 1.303.551 | ✗ |
| 40.000 | 16 | 14.406 · 2.358 · 1.250 | 3.054.066 | ✗ |
| 80.000 | 16 / 24 / 32 | ~4.500 · ~700 · ~400 | ~7.000.000 | ✗ |

**Tetto fra 20.000 e 40.000 msg/s.** Il numero di reader ha un ottimo stretto (16);
sopra il tetto nessun valore lo salva. Il push regge 80.000/s senza errori: cede la
**lettura ordinata**, non la scrittura.

Meccanismo, misurato: `pop/s` è funzione decrescente della profondità —
767 k righe → 14.406/s, 1,90 M → 2.358/s, 3,06 M → 1.250/s, grosso modo `pop ∝ 1/depth`.
È ciò che predice "costo per read = seq scan della tabella intera"
(`EXPLAIN` misurato: HashAggregate su Seq Scan, il GIN su `headers` non viene usato).

Confronto col soak Queen: **600.000 msg/s contro 20.000-40.000, cioè 15-30×** — e
Queen lo fa con dedup e retention attive, che pgmq non ha come concetti.
Sotto il tetto pgmq è più veloce (p50 17-34 contro 120 ms): è più rapido finché la
coda è corta, e non ha margine sopra.
