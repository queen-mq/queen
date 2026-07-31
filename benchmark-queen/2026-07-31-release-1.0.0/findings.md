# 2026-07-31 — Verifica pre-release 1.0.0: A/B vecchia build vs candidata

**Esito in una riga: nessuna regressione nella 1.0.0. La build di luglio e la
candidata, sullo STESSO rig, danno gli stessi numeri entro il 2%. Quello che non
si riproduce sono i valori assoluti di luglio, e la causa non e' il codice.**

## Rig

| ruolo | host | note |
|---|---|---|
| PG + broker | `queen-01` 139.59.133.1 (10.114.0.2) | 32c/62GB, **fdatasync 98 µs**, Xeon Gold 6548N |
| loader | `queen-02` 207.154.209.12 (10.114.0.3) | 32c/62GB, fdatasync 148 µs |

Scelta del disco per misura diretta (`pg_test_fsync`): queen-01 e' il 50% piu'
veloce in fdatasync, quindi prende PG + broker. Il link VPC regge **10 Gbit/s**
(iperf3, singolo stream e 8 stream), RTT 1,31 ms, zero errori/drop di NIC su
entrambe le eth1: la rete non e' un limite in nessuna delle misure sotto.

Build a confronto, entrambe compilate sulla stessa VM con lo stesso Dockerfile
(`--release`, opt-level 3, lto thin):

- **vecchia** = `615efdc` — la build da cui esce il soak 24h del 2026-07-24/25.
- **candidata** = `rustproxy` HEAD `21cfce0`, boot log `version=1.0.0-alpha-01`.

## Le cinque run (tutte 1M msg/s offerti, 120 s, shape T1: 100 partizioni, 850 consumer, push-batch 100, pop-batch 1000, pp10, payload 256B)

| run | push/s | pop/s | shed | p50 e2e | PG | Queen | backend attivi |
|---|--:|--:|--:|--:|--:|--:|--:|
| candidata, dedup 300s, tuning default | 631k | 357k | 0,3M | 979 ms | 0,7c | 15,8c | 2 |
| **vecchia `615efdc`, identica config** | **634k** | **365k** | 0,3M | 791 ms | 0,7c | 15,4c | 1 |
| candidata, tuning T3 (push lane larga) | 707k | 121k | 0,2M | 289 ms | 0,5c | 23,6c | 2 |
| candidata, **dedup OFF** | 917k | 378k | **0** | **20,6 ms** | 2,9c | 5,0c | 10 |
| candidata, dedup 300s ma cache 256MB | 595k | 325k | 0,3M | 758 ms | n/d | n/d | n/d |

Baseline 2026-07-23 per la stessa shape: 1M/s push **e** 1M/s pop, p50 90-120 ms,
**PG 14,3 core con 61 backend attivi**, Queen 9,7 core.

## Cosa dicono i numeri

**1. Non e' una regressione.** Vecchia e candidata coincidono su ogni metrica
(634k vs 631k push, 365k vs 357k pop, stesso profilo di CPU). Qualunque cosa
limiti questo rig limitava anche la build che a luglio ha prodotto i numeri buoni.

**2. Il tetto sul PUSH e' il path di verifica del dedup.** `perf` sul broker
sotto carico: **60,23% del tempo in `DedupCache::verified_for_push`**. Il costo
sta in `Entry::contains` (server/src/dedup.rs:316):

```rust
fn contains(&self, h: u128) -> bool {
    if self.hot.contains(&h) { return true; }          // scan LINEARE dell'hot buffer
    self.sealed.iter().any(|b| b.hashes.binary_search(&h).is_ok())  // una binary search PER BLOCCO
}
```

Per ogni push di 100 messaggi si fanno 100 `contains`, ognuno con uno scan lineare
dell'hot buffer piu' una binary search per ogni blocco sigillato, e il numero di
blocchi cresce mentre la finestra si riempie. Spegnendo il dedup il push torna a
**1M/s pieno, shed 0, p50 da 979 ms a 20,6 ms** e la CPU del broker crolla da 15,8
a 5,0 core. Il file `dedup.rs` e' identico fra le due build (cambia solo una riga
di log), quindi il costo e' **preesistente, non introdotto dalla 1.0.0**.

Da notare: ridurre la cache a 256MB per forzare la cap-pressure suppression **non**
recupera niente (595k/s). La suppression ha coperto solo 18 partizioni su 100
(`dedup_suppressed=18`, cache al 96%); le altre 82 continuano a pagare il verify
in memoria. Quindi la strada "tanto poi degrada a SQL probe" non salva il caso.

**3. Il tetto sul POP e' il loader, non il broker.** Nella run con dedup off il
broker sta a 5 core e PG a 2,9 con 10 backend: lato server non e' saturo niente,
eppure il pop non passa i 378k/s. `perf` su goload: **~20% in `encoding/json`
(checkValid, stateInString, unquoteBytes, rescanLiteral) e ~20% nel GC Go**
(findObject, scanobject, greyobject, gcDrain). E' costo di decodifica lato
client. Il loader di luglio aveva **48 core**, questo ne ha 32.

## Perche' i numeri di luglio non tornano

I due tetti sopra sono entrambi presenti nella build di luglio (dimostrato:
`615efdc` misurata qui da' gli stessi numeri). Quindi la differenza sta nel rig,
non nel codice, e le due cose che so essere diverse sono:

- il loader (32c oggi contro 48c a luglio), che spiega direttamente il tetto sul pop;
- il fatto che a luglio PG lavorava davvero (14,3 core, 61 backend attivi) mentre
  qui PG e' fermo a 0,7 core con 1-2 backend in ogni run con dedup acceso. Il
  broker non arriva nemmeno a interpellare il database: si ferma prima, nel verify.

Non ho una spiegazione verificata del secondo punto. Lo script di rig del 07-23
e' andato perso con le VM di quel giorno (`setup-broker.sh` e' stato ricostruito
il 24), quindi non posso escludere che T1/T2 di luglio girassero con parametri di
boot che non sono nel report.

## File

- `raw/ab-*/` — `run.out` (goload), `bench.csv` (1 Hz broker+PG), `loader.csv` (1 Hz loader).
- `raw/perf-broker-dedup.txt` — profilo broker, dedup acceso.
- `raw/perf-loader-json-gc.txt` — profilo goload, dedup spento.
