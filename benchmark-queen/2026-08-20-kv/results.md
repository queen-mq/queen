# 2026-08-20 — KV sotto carico (queen 1.0.4): la prima misura

`queen.kv` e' arrivato in 1.0.3, gira **senza flag** su ogni cella e **non era mai
stato messo sotto carico**: il giro di implementazione era verifica funzionale. Questo
e' il primo numero.

**Rig**: broker 1.0.4 su VM 32c/62GB, PG 18.6 nativo, loader `kvload` (in `kvload/`)
da una VM 8c nella stessa VPC. 100 namespace x 20.000 chiavi, 120 s per configurazione.
Raw in `raw/`.

Le tre forme misurate sono quelle che i chiamanti reali usano, e costano cose diverse
nella stored procedure:

* **`incr`** — rate limiter a finestra fissa, UNA chiamata per decisione, TTL
  create-only. E' quello che fa il meter di Gate.
* **`putIfAbsent`** — marker di idempotenza; desugar a `put` con `expect:0`, quindi
  prende il ramo insert e il perdente riceve il valore del vincitore senza secondo giro.
* **`get`** — lettura cache, il path economico.

## I numeri

| configurazione | throughput | p50 | p90 | p99 | PG CPU | Queen CPU |
|---|--:|--:|--:|--:|--:|--:|
| mix incr/get/pia 60/30/10, **batch 1** | **37.415 ops/s** | 1,69 ms | 2,49 ms | 3,51 ms | 13,2 core | 3,2 core |
| stesso mix, **batch 25** | **153.930 ops/s** | 4,93 ms | 7,00 ms | 9,27 ms | 12,6 core | 1,5 core |
| **solo `incr`** (forma rate-limiter), batch 1 | **32.983 ops/s** | 1,82 ms | 2,47 ms | 3,42 ms | 12,5 core | 2,9 core |

Zero errori sulle due run non batchate.

**Il batching e' la leva**: 4x le operazioni allo STESSO costo PG (12,6 contro 13,2
core), perche' l'overhead per-transazione si ammortizza su 25 operazioni. Senza batch
KV e' commit-bound come tutto il resto del prodotto — la latenza p50 di 1,7-1,8 ms e'
sostanzialmente un commit.

Nota sul p50 del batch-25 (4,93 ms): e' la latenza della RICHIESTA da 25 operazioni,
non della singola operazione, che sta sotto i 0,2 ms ammortizzati.

## Il finding operativo: il rate limiter di default e' basso

La prima passata ha prodotto **~10 milioni di errori `rate_limited` e 305 ops/s utili**.
Non era KV che cedeva: era il suo limitatore per-tenant ai valori di default.

```
kv_rate = 200r / 400rb / 200w(default 100) / 400wb / 2000cell
```

Cioe' **200 letture/s e 100 scritture/s per tenant**, con un tetto di cella a 2000.
I numeri della tabella sopra sono stati ottenuti alzando:

```
QUEEN_KV_READ_RATE=200000 QUEEN_KV_READ_BURST=400000
QUEEN_KV_WRITE_RATE=200000 QUEEN_KV_WRITE_BURST=400000 QUEEN_KV_CELL_RATE=0
```

**Perche' conta**: il meter di Gate fa un `incr` per decisione. Ai default, **un tenant
puo' prendere ~100 decisioni di rate-limit al secondo** prima di essere lui stesso
rate-limited. Per un limiter di egress e' un tetto che si incontra presto, e si
incontra come `429` dal proprio broker, non come lentezza. Va deciso se il default
serve a proteggere la cella (allora la doc deve dirlo forte, e Gate deve documentare
che alza il limite) o se e' semplicemente basso.

La cella regge 37k ops/s con un solo tenant: il default a 100 w/s e' 0,3% di quello
che la cella fa. Il limitatore non sta proteggendo la cella da un tenant: sta
impedendo a un tenant di usarla.

## Riproduzione

```sh
cd kvload && GOWORK=off GOOS=linux GOARCH=amd64 go build -o kvload-linux-amd64 .
./kvload -url http://HOST:6632 -duration 120 -namespaces 100 -keys 20000 \
         -incr 60 -get 30 -pia 10 -batch 1 -workers 64
```

**Limite noto del loader**: `applied`/`notApplied` tornano 0 — il parser non estrae il
campo dalle risposte batch. Le percentuali di successo sono comunque valide (HTTP 200
+ zero errori classificati); solo la distinzione applied/non-applied manca, che
servirebbe per misurare la contesa su `putIfAbsent`. Da sistemare se si vuole misurare
quella.
