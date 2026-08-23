# Cella Queen Cloud su VM — 1 Postgres, 2 broker, 1 proxy

Banco per il TCO della cella: quante entita' regge una cella e quanto costa.
Costruita il 2026-08-22 sulle stesse due VM della campagna isolation ([RIG.md](../2026-08-22-isolation/RIG.md)).

## Topologia

```
                    queen-02 (loader)                queen-01 (LA CELLA)
                    142.93.170.82                    46.101.186.250
                                                     ┌──────────────────────────────┐
   goload -mode cloud ──── :6711 ─────────────────►  │ cell-proxy                   │
   Host: cell01.<qualsiasi>                          │   enforce=true               │
   Bearer qk_live_...                                │        │                     │
                                                     │        ▼ http://cell-lb:6630 │
                                                     │ cell-lb (nginx)              │
                                                     │      ├──► cell-broker-a :6632│
                                                     │      └──► cell-broker-b :6632│
                                                     │             │ mesh :6633 │   │
                                                     │             └──────┬─────┘   │
                                                     │                    ▼         │
                                                     │ cell-pg  (queen + queen_proxy)│
                                                     └──────────────────────────────┘
```

Perche' **due** broker e un bilanciatore: `helm_v1/broker/values.yaml` gira
`replicas: 2` e `helm_v1/sql/03-bootstrap-proxy.sql` punta il `base_url` della
cella a un **Service Kubernetes**, che bilancia sulle repliche. Su una VM sola
l'equivalente fedele e' un bilanciatore L7: nginx sta al posto del Service, la
cella resta **un solo `base_url`** con due broker dietro, come in produzione.

Perche' **un** Postgres con **due** database (`queen` + `queen_proxy`): in
produzione sono separati, ma la cella condivisa e' la forma economica ed e'
quella di cui stiamo misurando il costo.

Tutti i componenti stanno su una rete docker dedicata e si chiamano per **nome
container**. Non e' estetica: pubblicare la porta e parlare a 127.0.0.1 fa
passare il traffico dal processo userland `docker-proxy`, che aggiunge latenza e
addebita la CPU **fuori** dal cgroup del container — il difetto trovato e
corretto in `vm-cell.sh` il 2026-07-29.

## Uso

```bash
./cell-up.sh up        # ricostruisce da zero
./cell-up.sh status    # componenti + healthz + stato mesh
./cell-up.sh down
```

Sul broker box lo script sta in `/root/cell-up.sh`. Immagini: `queen:1.1.0`,
`queen-proxy:1.1.0`, `postgres:18`, `nginx:alpine`.

| endpoint | dove | note |
|---|---|---|
| proxy | `:6711` (pubblica) | **l'unica porta di ingresso** |
| balancer | `:6630` (pubblica) | comodita' di debug |
| broker A / B | `127.0.0.1:6632` / `:6642` | **solo loopback**, vedi sotto |
| Postgres | `:5432` | `queen`, `queen_proxy` |

## Tre trappole pagate nel montarla

1. **Il broker RIFIUTA di partire** con `QUEEN_TENANCY_HEADER=1` se non c'e'
   anche `QUEEN_KV_TRUSTED_PROXY=1`:

   > *the tenant header is opaque and validated against nothing, so any caller
   > could read and write another tenant's KV state BY NAME*

   L'affermazione qui e' **vera** — davanti c'e' il proxy — ma resta vera solo
   perche' le porte host dei broker sono legate a **loopback**. Se le pubblichi
   sulla VPC, un client puo' scavalcare il proxy e forgiare `x-queen-tenant`, e
   l'affermazione diventa una bugia. Non pubblicarle.

2. **Il cluster si risolve dalla PRIMA label DNS dell'header `Host`.** Non c'e'
   fallback (`QUEEN_PROXY_DEFAULT_CLUSTER` deliberatamente non impostata). Il
   cluster qui e' `cell01`, quindi ogni richiesta vuole `Host: cell01.<qualsiasi>`.
   Senza, il proxy risponde 421 e sembra un problema di rete.

3. **Il subscription mode di default e' `new`.** Un consumer group creato DOPO
   un push non vede quel messaggio. Nel primo smoke sembrava consegna persa: non
   lo era. Stabilire il gruppo con una pop, poi pushare.

## Verifica di montaggio (2026-08-22)

```
broker on :6632 health=200      broker on :6642 health=200
proxy /healthz : {"enforce":true,"status":"ok","tenant_header":true}
lb -> broker   : 200
cell-broker-a: mesh_active=true peers=[("cell-broker-b", 6633)]
cell-broker-b: mesh_active=true peers=[("cell-broker-a", 6633)]
```

End-to-end attraverso proxy → bilanciatore → broker: push 201, poi le pop
restituiscono i messaggi di **entrambe** le partizioni e poi vuoto. Il proxy
interroga `/api/v1/resources/queues` attraverso il bilanciatore ogni 10 s
(`QUEEN_PROXY_RECONCILE_MS`) e prende 200.

Tenant di prova gia' creato: slug `bench`, cluster `cell01`, piano `dedicated-s`,
cella `cell-01`. **La API key e' mostrata una volta sola** da
`queen_proxy.bootstrap_tenant` (in DB c'e' solo lo SHA-256): quella corrente sta
sul box in `/root/cell-tenant.json`. Per rifarla, ri-esegui la bootstrap con un
`p_key_name` diverso.

## Cosa manca (il test vero)

Questo documento certifica che la **cella sta in piedi**, non che sia stata
misurata. Il passo successivo e' la scala di densita' con `goload -mode cloud`
dal loader: N tenant sulla stessa cella, rampa fino a rompere uno SLO dichiarato,
con la CPU campionata per container (PG vs broker vs proxy separati, che e' il
dato che serve al TCO — nella campagna isolation PG era l'80% del costo).

Due cose da decidere prima di lanciarla, perche' cambiano il risultato:

- **la forma del tenant** — quante code, quanti consumer parcheggiati, che rate;
- **lo SLO** — quale p99 e quale tasso di errore contano come "cella piena".

Da tenere presente: a 100k code il broker 1.0.4 e' morto di stack overflow
([CRASH-100k.md](../2026-08-20-tenants-1.0.4/CRASH-100k.md)), quindi la scala va
fermata prima o va rifatta la misura su 1.1.0 sapendo cosa si sta cercando.
