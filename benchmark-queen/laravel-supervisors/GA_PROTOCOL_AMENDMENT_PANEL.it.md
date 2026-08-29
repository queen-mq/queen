# Emendamento al protocollo GA — pannello di supervisione

Registrato il 29 agosto 2026 alle 05:44:11 UTC, commit base
`245165e0df4012f6fbc11a0d827511056087d57b`, dopo la domanda esplicita del
product owner e prima dell'implementazione o dei test del pannello.

Questo emendamento aggiunge un requisito di feature parity; non modifica
workload, endpoint o regole statistiche del protocollo prestazionale congelato.

## Stato iniziale

La dashboard broker Queen mostra code, consumer group, messaggi, analytics e
DLQ. I supervisor Laravel PHP/Rust pubblicano invece stato e controllo soltanto
nel loro `state_directory`, consultabile con `queen:supervisor`. Non esiste
ancora un pannello equivalente alla control surface di Horizon.

## Gate della release candidate

Il client Laravel deve fornire un pannello applicativo, separato dalla dashboard
del broker, con almeno:

- stato live/stale, engine, instance ID, PID e ultimo heartbeat;
- worker attivi e draining per supervisor/coda;
- restart state/failure e prossima retry per pool;
- configurazione risolta non sensibile e profondità delle code configurate;
- riepilogo e accesso al lifecycle dei failed job Laravel/Queen DLQ;
- azioni `pause`, `continue` e `terminate` mediante richieste POST autenticate;
- aggiornamento periodico e rappresentazione esplicita di dati non disponibili.

Il pannello deve essere disabilitabile e avere prefisso/middleware configurabili.
In ambiente production l'accesso è deny-by-default finché l'applicazione non
registra un'autorizzazione esplicita. Le azioni modificanti richiedono la stessa
autorizzazione, CSRF e il fencing `instance_id` già usato dal controllo CLI.
Credenziali, bearer token, header e path privati non vengono restituiti.

## Perimetro e limiti dichiarati

La prima versione è Laravel-native e legge lo stato del supervisor locale. È
coerente con la topologia attualmente supportata, che richiede un solo master
per applicazione/consumer group. Non viene presentata come vista multi-host.

Una futura vista centralizzata richiede heartbeat condivisi e leadership
fenced; una semplice chiave KV con TTL non basta a rendere due supervisor
active-active. La dashboard broker esistente resta il pannello per metriche e
operazioni della queue, mentre il nuovo pannello copre il control plane Laravel.

## Test minimi

- route assenti quando il pannello è disabilitato;
- deny-by-default in production e accesso autorizzato esplicitamente;
- escaping dei dati, nessuna esposizione di segreti e Content Security Policy;
- stato live, paused, stale, draining e pool multipli;
- POST di controllo con comando e `instance_id` corretti;
- rifiuto di comando non autorizzato, CSRF mancante e supervisor sostituito;
- failed-job provider disponibile/non disponibile e broker irraggiungibile;
- render e API contract verificati senza dipendere da CDN o asset remoti.
