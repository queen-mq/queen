# Chaos-soak 2026-07-25 — parziale (interrotto)

Test tentato: producer 500k/s + churn consumer (stacchi ciclici) + PG-kill
periodici (outage-survival via file_buffer). Interrotto e NON completato.

## Cosa è andato storto (harness, NON Queen)
- Il controller chaos-pg.sh è morto dopo il primo `docker stop` di PG (~20:57Z)
  e non l'ha riavviato → PG giù ~2h. Il watchdog non copriva PG (tolto apposta).
- Fix applicato dopo: chaos-pg robusto + watchdog con PG safety-net (riavvia PG
  se giù oltre la finestra attesa). Ma il run era compromesso.
- Nei tentativi di restart ho cancellato i log originali (ciclo 1 + evento 2h),
  quindi i raw qui sono solo il run parziale finale, poco utile.

## Cosa Queen ha DAVVERO mostrato (osservato live, salvato in memoria)
1. **Ciclo consumer pulito** (primo ciclo, prima del guasto harness):
   full-outage 2,5min → picco backlog ~72-75M (= 500k/s × 150s) → recovery a
   <1M in **~6,5 min**; wheel hot-list bounded (0 durante l'outage, ~52k sotto
   carico — NESSUN leak); RAM broker piatta 5,0G; producer 500k imperturbabile
   durante lo stacco (p99 12ms), 0 shed.
2. **Sopravvivenza a 2h di Postgres TOTALMENTE giù** (per il bug harness, non
   intenzionale): broker vivo tutto il tempo (status ok, no crash, no OOM),
   bufferizzato su disco quel che poteva + backpressure (shed) sull'eccesso,
   nessun messaggio accettato perso; al riavvio di PG (crash-recovery 51s) il
   broker si è riconnesso e ha ricominciato a drenare il buffer.

## Non verificato (rimasto da fare)
- Un failover PG *controllato* a 500k (12s) con replay pulito end-to-end e
  recovery a lag 0 — la macchina cresceva backlog quando è stato interrotto.
- Recovery-time costante su molti cicli (solo 1 ciclo pulito osservato).

Verdetto onesto: Queen ha reagito bene ai due eventi visti; il test va rifatto
con l'harness corretto (script già fixati sulle VM: chaos-pg.sh + watchdog).
