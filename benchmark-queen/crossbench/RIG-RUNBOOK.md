# Rig runbook — come si fanno le prove sulle VM (per agenti)

Scritto 2026-08-03 dopo una giornata di campagne. Ogni trappola elencata in
fondo è stata pagata davvero: leggile prima di lanciare qualsiasi cosa.

## Le macchine

| ruolo | ssh | note |
|---|---|---|
| broker (bench-01) | `root@104.248.245.59` | 8 vCPU/16GB; qui girano i container sotto test e il sampler |
| loader (bench-02) | `root@159.89.104.168` | qui gira SOLO cmbench; deve restare sotto ~10% CPU |

Dal loader il broker si raggiunge via VPC: **`10.114.0.2`** (Postgres :5432,
Queen :6632, Kafka :9092). Mai generare carico sul broker VM.

## Broker VM — layout e procedura stack

- `~/cmbench/deploy/` — i compose (`docker-compose.queen.yml`, pgmq, kafka,
  rabbit) + `postgres.conf` + `init-pgmq.sql`. I compose leggono `${VAR:-default}`
  dall'ambiente della shell.
- `~/arbenv.sh` — l'env corrente (QUEEN_IMAGE + knobs). Modificalo, poi `source`.
- `~/queen-src/server/` — sorgente broker. Build immagine:
  `cd ~/queen-src/server && docker build -t queen:<tag> .`
  (logga su file; successo = riga `naming to docker.io/library/queen:<tag>`).
  Sync dal laptop: `rsync -az server/src root@104.248.245.59:queen-src/server/`.
- `~/sampler.sh <cont> [cont2] > ~/samples/<nome>.csv` — CPU/RSS/disco a 1Hz.

Stack fresco (SEMPRE così, per ogni run pulita):

```bash
docker rm -f cmbench-queen cmbench-queen-pg   # possono appartenere a un ALTRO progetto compose
. ~/arbenv.sh
cd ~/cmbench/deploy
docker compose -f docker-compose.queen.yml down -v
docker compose -f docker-compose.queen.yml up -d
sleep 15 && curl -s -o /dev/null -w "%{http_code}" http://localhost:6632/health  # atteso 200
docker inspect cmbench-queen --format '{{range .Config.Env}}{{println .}}{{end}}' | grep QUEEN_  # verifica env
```

## Loader VM — lanciare una run

```bash
cd ~/cmbench && mkdir -p results/<nome>
echo "<comando esatto>" > results/<nome>/invocation.txt   # OBBLIGATORIO (SPEC §5.2)
nohup ./cmbench -system queen -queen-url http://10.114.0.2:6632 \
  -queen-pop-mode wildcard -queen-pop-partitions 8 \
  -rate 2000 -properties 1000 -duration 180 -ramp 10 -drain 90 \
  -logdir results/<nome> > results/<nome>/run.log 2>&1 < /dev/null & disown
```

Esiti in `results/<nome>/`: `result.json` (latency_ms, correctness, flow,
broker_stats), `run.log` (righe `t=` a 1Hz con `lag=` + VERDICT finale),
12 stage log, `produced.meta`. Re-verifica offline:
`./cmbench -verify-only results/<nome> -properties 1000`.

Shape canoniche: **ancora** 2k/1000 (180s ramp10 drain90; baseline storica
311,7ms su trace9-Vegas, arbiter9 ~1,1s — delta APERTO, vedi memoria);
held-out 1k/200 pp8 (arbiter9: 170ms) e 5k/1000 (overload). Discriminatori
rapidi: `-duration 60 -ramp 5 -drain 30` (transitorio pesante: solo per sì/no).

## Telemetria (immagini arbiter*)

- `docker logs cmbench-queen | grep "broker rates"` → `adm_budget`,
  `adm_lanes` (inflight/cap w waiter per corsia), `trains_s`, `cycle_ms`,
  `oldest_wait_ms`, `visits_s`, `cands_visit`, `ready_age_p50/p95`,
  `ring_oldest_ms`, `ring_depth`.
- `grep "queue rates"` → `lag_ms` PER CODA (il segnale che localizza).
  **`hot=N/M` è solo il contatore top-N del log — NON è un limite.**
- `QUEEN_ADMISSION_TRACE=1` → righe `lane probe kept/reverted` con motivo.
- `QUEEN_HOTLIST_TRACE=<prefisso-coda>` → `[hlt]` per-evento su stderr:
  mark/promote/take/attempt(`adm_wait`,`pool_wait`)/sqldone/fusedwait/served/
  tri/rqout. Volume alto: campiona con `docker logs --since 20s`.
- Taratura treni: `docker exec cmbench-queen-pg psql -U postgres -tA -c
  "SELECT wal_sync FROM pg_stat_wal"` due volte a 10s → delta = fsync/s reali.

## Gate di correttezza

Ogni run deve chiudere **0 gaps / 0 order violations / 0 dups**. Exit 3 =
FAIL di correttezza: è un RISULTATO, si tiene e si pubblica. Stream vuoti =
run rotta, mai "pulita".

## Trappole (tutte pagate il 2026-08-03)

1. **pkill del sampler**: `pkill -f sampler.sh` uccide la TUA sessione ssh se
   il comando remoto contiene la stringa. Usa `p=ampler; pkill -f "s${p}.sh"`
   e MAI nello stesso script che poi rilancia `~/sampler.sh` (letterale!).
2. **`docker rm -f` prima del compose up**: i container possono appartenere a
   un progetto compose diverso col medesimo nome → `down -v` non li possiede.
3. **Sempre `invocation.txt`**: le run manuali senza record sono costate una
   giornata di forensics (262 vs 623).
4. **Mai due cmbench sovrapposti** (`pgrep -x cmbench` prima di lanciare): due
   run accavallate hanno contaminato la fair-matrix.
5. **`:latest` non pinnata**: laptop e VM avevano pgmq 1.11.1 vs 1.12.0.
   Annota SEMPRE il digest immagine nel report.
6. Il broker sta in `health: starting` ~30s: aspetta il 200 prima di lanciare.
7. Warning `deprecated knob` al boot = compose/env vecchio (QUEEN_SEG_*/VEGAS_*
   sono morti): pulisci prima di misurare.
8. A fine campagna copia `result.json`/`run.log`/`invocation.txt` (+ CSV del
   sampler) in `benchmark-queen/crossbench/results/<campagna>/` nel repo.
9. Se un'ipotesi è testabile cambiando SOLO env o flag, quel run viene PRIMA
   di qualunque lettura di codice (lezione del reviewer, confermata 5 volte).
