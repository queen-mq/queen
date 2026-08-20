# DIFETTO — stack overflow del broker a 100k code (1.0.4)

**2026-08-20, rig 32c/62GB, `ghcr.io/queen-mq/queen:1.0.4`, PG 18.6.**
Il broker **e' morto**: container `Exited (139)` = SIGSEGV.

```
thread 'tokio-rt-worker' (26) has overflowed its stack
fatal runtime error: stack overflow, aborting
```

Raw completo in `raw/crash-100k.txt`; la run e' `raw/idle100k.*`.

## Come riprodurlo

```sh
# broker 1.0.4 stock (DB_POOL_SIZE default 160), PG sulla stessa macchina
goload -mode tenants -url http://BROKER:6632 \
       -tenants 10000 -queues-per-tenant 10 -idle-only \
       -duration 1500 -provision-conc 128 -idle-conns 120000
```

Cioe' **100.000 code, 100.000 consumer long-poll, traffico ZERO**. Il provisioning
riesce (100.000 code create, 100.000 ring hot-list). Il crash arriva dopo alcuni
minuti di consumer che tentano di parcheggiare.

## Lo stato subito prima, dai log del broker

```
sizes: pool="160/160" pool_waiting=10352 hotlist="100000rings/0ready/0wheel" rss_gb="7.75"
WARN pool: connection pool saturated — requests are queueing on the DB pool waiting=203 size=160 max=160
rates: parked=3584 pop_empty_pct="100.0" pool_waiting=203 adm_mode="normal"
       adm_lanes=push:0/2w0 pop:2/2w31821 ack:0/2w0 maint:1/2w0
       oldest_wait_ms="1424.2" adm_last="saturated"
```

Tre fatti:

1. **Il pool DB e' saturo**: 160/160 con fino a **10.352 richieste in coda**.
2. **Solo 3.584 consumer su 100.000 erano parcheggiati.** Gli altri 96.416 erano
   bloccati prima di arrivarci — il loader ha contato **3.720.577 errori di pop**.
3. **31.821 waiter nella corsia pop** dell'arbitro di admission, `adm_last="saturated"`.

Poi lo stack overflow su un worker tokio.

## Cosa NON dice questa run

**I numeri di risorsa della run 100k sono inutilizzabili** (PG 1,24 core / Queen 4,19
core di media): descrivono un sistema in fallimento con 96% dei consumer non
funzionanti, non il costo di 100.000 consumer parcheggiati. Non vanno usati per
estrapolare densita'.

## Cosa dice, ed e' solido

* **Il refresh e' O(code) e a 100k code costa 1.384 ms per chiamata** — misurato su 154
  chiamate, 36% del tempo DB. Server-side, indipendente dal fallimento dei consumer.
  Conferma l'estrapolazione fatta in `results.md` (previsti ~1,2 s). Su cadenza 10 s e'
  il 14% dell'intervallo consumato da una query sola: il Tier 3 di
  `PLAN_STATS_REFRESH.md` sull'asse CODE e' lavoro reale, non ipotesi.
* **Una cella con `DB_POOL_SIZE=160` non arriva a 100k code con un consumer ciascuna.**
  Il muro e' il pool, non la CPU. A 10k code (stessa forma) tutto regge senza un errore.

## Ipotesi da verificare (NON verificate)

Il crash e' arrivato **sotto saturazione del pool**, non sotto memoria (RSS 7,75 GB su
62 GB). Uno stack overflow in un worker tokio sotto backpressure suggerisce una catena
di future/retry che cresce con l'attesa piu' che un dato grande. Le due direzioni da
guardare per prime, entrambe da confermare con un repro ridotto:

1. una ricorsione (o una catena di combinator) sul path di **wait/park del pop** che
   si approfondisce quando l'acquisizione del pool o della corsia di admission fallisce
   ripetutamente;
2. dimensione di stack dei worker tokio piu' piccola del necessario per quel path (in
   quel caso il fix e' comunque togliere la crescita, non alzare lo stack).

**Priorita': alta.** Un broker che ABORTA e' peggio di un broker che rifiuta: sotto
questa forma di carico un cliente vede il processo sparire, non un 429. E la
saturazione del pool e' una condizione raggiungibile in produzione (basta un fan-out di
consumer piu' grande del pool), non un artefatto di questo rig.

**Non riprodotto in prod**: prod gira 54k partizioni con un numero di consumer molto
piu' basso e non ha mai mostrato questo. La condizione che serve e' *molti consumer
long-poll contemporanei contro un pool saturo*.
