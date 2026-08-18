# Runbook di pubblicazione 1.0.3

Documento operativo. Ogni passo ha un comando da incollare e un gate di verifica eseguito dal
punto di vista di chi installa il pacchetto, non dal punto di vista del repo.

**Fatti fissati (verificati il 2026-08-18, non dedotti):**

| Cosa | Valore |
| --- | --- |
| Branch di lavoro | `kvtimer` a `64080e22` |
| `origin` | `git@github.com:queen-mq/queen.git` |
| `origin/release` | `6a55ce20` (1.0.2), antenato di `kvtimer`, fast-forward pulito (`git rev-list --left-right --count origin/release...HEAD` -> `0 14`) |
| Versione | **1.0.3**, fonte di verita' `server/server.json` |
| npm `queen-mq` | pubblicato `1.0.0` (dist-tag `beta = 0.16.0-beta.0`) |
| PyPI `queen-mq` | pubblicato `1.0.0` |
| crates.io | `queen-mq 1.0.0`, `queen-protocol 1.0.0`, `queen-engine 1.0.0` |
| Go proxy | `clients/client-go v1.0.0` |
| Packagist `smartpricing/queen-mq` | **non esiste**, 404 |
| Tag 1.0.3 esistenti | nessuno, in nessuna forma |

Nota sul module path Go: i moduli si chiamano `github.com/smartpricing/queen/clients/client-*`
mentre il repo e' `queen-mq/queen`. Funziona perche' GitHub serve un redirect 301 da
`smartpricing/queen` a `queen-mq/queen` (verificato), e il proxy ha registrato per v1.0.0
`Origin.URL = https://github.com/smartpricing/queen`, `Hash = 8ee448d9` che e' esattamente il tag
`v1.0.0` di questo repo. Quindi i tag si pushano su `origin` e il proxy li vede. Questo e' un
punto fragile: se qualcuno registra un nuovo repo `smartpricing/queen` il redirect muore e con
esso i due moduli Go. Non e' un problema di oggi, e' un rischio da annotare.

Fonte documentale interna: `webdoc/src/content/docs/internals/contributing/release.mdx`.
Questo runbook la segue e non la contraddice.

---

## 0. Checklist prerequisiti, da spuntare PRIMA del passo 1

Nessun passo di pubblicazione parte finche' queste caselle non sono chiuse. Sono divise in
bloccanti duri (il comando fallisce o produce un artefatto rotto) e bloccanti di contenuto (il
comando riesce ma pubblica una bugia irreversibile).

### 0.A Bloccanti duri

- [ ] **`clients/client-rust/Cargo.lock` e `crates/queen-protocol/Cargo.lock` dicono ancora
      `1.0.0`.** `server/Cargo.lock` e' gia' a `1.0.3`, `proxy/Cargo.lock` pinna ancora
      `queen-proxy 1.0.2`. La CI `tests` su `kvtimer@64080e2` e' rossa proprio qui
      (`cannot update the lock file ... because --locked was passed`), e i job
      `rust-client-unit`, `rust-client-msrv`, `suites (rust-client)` cadono tutti da questo.
      Fix:
      ```bash
      cd /Users/alice/Work/queen
      cargo update -p queen-protocol --manifest-path crates/queen-protocol/Cargo.toml
      cargo update -p queen-mq -p queen-protocol --manifest-path clients/client-rust/Cargo.toml
      cargo update -p queen-proxy --manifest-path proxy/Cargo.toml
      git add crates/queen-protocol/Cargo.lock clients/client-rust/Cargo.lock proxy/Cargo.lock
      ```
      Verifica: `grep -A1 'name = "queen-mq"' clients/client-rust/Cargo.lock` deve dire `1.0.3`.
      Bonus da sistemare nello stesso commit: il commento in `clients/client-rust/Cargo.toml`
      giustifica `rust-version = "1.86"` con "the lockfile alone (v4)" mentre il lock committato
      e' `version = 3`.

- [ ] **`server/src/handlers/kv.rs:1074` chiama `axum::serve` in un test in-file.** In build lean
      (`--no-default-features`) la feature server di axum non c'e' e il job
      `server-msrv-and-lean` fallisce con `error[E0425]: cannot find function 'serve' in crate
      'axum'`. Rompe la forma embedder che quel job certifica, e sta nel crate che va su
      crates.io al passo 8. Va messo dietro il cfg della feature server o riscritto senza
      `axum::serve`.
      Verifica: `cargo check --manifest-path server/Cargo.toml --no-default-features --all-targets`.

- [ ] **`clients/client-py/queen/__init__.py:29` dice `__version__ = "0.15.0"`** mentre
      `pyproject.toml:7` dice `1.0.3`. `clients/client-py/publish.sh:41` legge la versione da li'
      e stampa "Publish version 0.15.0 to PyPI?" oltre a URL PyPI sbagliati. La wheel uscirebbe
      giusta lo stesso perche' la versione statica sta nel pyproject, ma il prompt di conferma
      mente.
      Fix: portare `__version__` a `"1.0.3"`.
      Verifica: `cd clients/client-py && python -c "from queen import __version__; print(__version__)"`.

- [ ] **`clients/client-py/dist/` contiene ancora `queen_mq-1.0.0-py3-none-any.whl` e
      `queen_mq-1.0.0.tar.gz`** (datati 31 luglio). `publish.sh` fa `rm -rf dist/` da solo, la
      via manuale no: un `twine upload dist/*` senza pulizia tenta di ricaricare la 1.0.0 e
      fallisce con `File already exists`, o peggio confonde.
      Fix: `rm -rf clients/client-py/dist clients/client-py/build clients/client-py/*.egg-info`.

- [ ] **`clients/client-cli/go.mod:10` pinna `github.com/smartpricing/queen/clients/client-go
      v0.15.0`.** In locale `go.work` maschera il pin, per l'utente finale no. Riprodotto:
      ```bash
      cd /Users/alice/Work/queen/clients/client-cli && GOWORK=off go build ./...
      # cmd/blocked.go:26:71: he.Code undefined (type *queen.HTTPError has no field or method Code)
      # cmd/queue.go:156-158: unknown field RetentionEnabled / DeadLetterQueue / DlqAfterMaxRetries
      ```
      Non e' una regressione di questo ciclo: il `go.mod` pubblicato a `clients/client-cli/v1.0.0`
      porta lo stesso pin, quindi `go install .../cmd/queenctl@latest` e' rotto dalla 1.0.0.
      Il fix e' il passo 11 e **non si puo' anticipare**: richiede che
      `clients/client-go/v1.0.3` sia gia' sul proxy.

- [ ] **La CI `tests` deve essere verde sul commit che si rilascia.**
      `gh run list --workflow=tests --branch kvtimer --limit 3`. Al momento il run `32117917442`
      e' rosso con 4 job su 13 falliti, tutti coperti dalle due caselle sopra.

### 0.B Bloccanti di contenuto

- [ ] **`clients/client-js/README.md:31`** dice `Key/Value State and Timers ... off by default on
      the broker`, che e' l'esatto contrario di questo rilascio e della riga 379 dello stesso
      file ("Both surfaces are always there. There is nothing to enable"). La riga 31 sta nella
      bullet-list in cima, cioe' e' la prima cosa che si legge sulla pagina npm. **Il README di
      npm si aggiorna solo ripubblicando**: se esce cosi', resta cosi' fino alla 1.0.4.

- [ ] **`clients/client-rust/README.md:8`** dice `queen-mq = "1.0.0"`. Quel README e' dentro il
      `.crate` ed e' la prima pagina su crates.io. Su crates.io il README **non e' modificabile
      dopo la pubblicazione**, mai, per nessuna via.

- [ ] **Nessuno dei sette README dichiara il broker minimo**, e tutti (client-js:381,
      client-go:430, client-rust:59, client-py:341, client-laravel:323, client-cpp:560) dicono
      testualmente che un 404 su kv/timers e' un bug. Contro un broker 1.0.2 quel 404 e' il
      comportamento normale. Serve una riga "kv e timers richiedono un broker >= 1.0.3" nei sei
      README che espongono la superficie. Vale per npm, PyPI e crates.io, dove il testo e'
      congelato dalla pubblicazione.

- [ ] **`crates/queen-protocol` non ha `README.md` ne' campo `readme` in `Cargo.toml`.** La sua
      pagina crates.io sara' vuota, di nuovo per sempre a questa versione. Aggiungerne uno anche
      minimo e' 10 minuti adesso e impossibile dopo.

- [ ] **Packagist: decisione strutturale, vedi passo 12.** Non e' un tag dimenticato. Va deciso
      prima di iniziare, perche' se la scelta e' spostare il manifest allora tocca il commit di
      rilascio.

### 0.C Igiene, non bloccante ma da fare nello stesso commit

- [ ] `clients/client-cpp/bin/test_client` (1.118.488 byte) e' **tracciato** in git nonostante
      `.gitignore:21`: `git rm --cached clients/client-cpp/bin/test_client`.
- [ ] 33 riferimenti a `PLAN_KV_TIMERS.md`, documento che nel repo non esiste, in 21 file di
      sorgente spedito. Il caso user-facing e' `clients/client-go/kv.go:106`, un messaggio di
      errore a runtime che rimanda a un doc interno inesistente; in Python stanno nei docstring
      dei moduli pubblici (`queen/kv/__init__.py:1`, `queen/timers/__init__.py:1`) e escono da
      `help()`. Almeno il messaggio Go va ripulito prima del tag.
- [ ] `clients/client-js/package.json` `"files"` spedisce `test-v2/**/*`: 46 file su 95 del
      tarball sono test. `.npmignore` li elenca ma non conta, perche' `"files"` lo sovrascrive.
      Peso e superficie, non un blocco.
- [ ] `clients/client-cpp/QUICK_START.md:19` contiene il path assoluto della tua macchina
      (`cd /Users/alice/Work/queen/clients/client-cpp`) e `IMPLEMENTATION_SUMMARY.md` descrive un
      header "~2,300 lines" senza kv ne' timers. Sono spediti al tag insieme all'header.
- [ ] `app/package.json:3` e `proxy/console/package.json:3` sono ancora a `1.0.0`. Privati, mai
      pubblicati, ma la regola di allineamento scritta in `release.mdx` li nomina.
- [ ] `README.md:279` propone `queen-engine = { version = "1.0.0", ... }`.
- [ ] `release.mdx` righe 186-191 parla ancora di `cli.yml` e `cpp-server-build.yml` come
      esistenti: sono stati rimossi in `043b9cd7`. Il frontmatter e' gia' aggiornato, la prosa no.
- [ ] `examples/apps/run.sh:153` e `examples/tutorials/run.sh:147` skippano ancora il C++ per
      `json.hpp` mancante, che questo commit ha vendorizzato in `clients/server/vendor/json.hpp`.
      Codice morto.

### 0.D Credenziali, tutte locali

**Non esiste un solo secret di registry in CI.** Grep su `secrets.` nei quattro workflow: due
occorrenze, entrambe `GITHUB_TOKEN` automatico (`docker-build.yml:80`, `release-cli.yml:68`).
Nessun `NPM_TOKEN`, `PYPI_API_TOKEN`, `CARGO_REGISTRY_TOKEN`, `TWINE_*` e' configurato da nessuna
parte, ne' serve. Tutte le pubblicazioni di pacchetto sono a mano, con le credenziali locali
dell'autrice.

| Nome | Dove vive | Presente | Come si verifica |
| --- | --- | --- | --- |
| `NPM_TOKEN` | `~/.npmrc` locale | si | `npm whoami` |
| `PYPI_API_TOKEN` | `~/.pypirc` locale, `[pypi]` con user `__token__` | si | `python -m twine check dist/*` non lo prova, si prova solo caricando |
| `CARGO_REGISTRY_TOKEN` | `~/.cargo/credentials.toml` locale | si | `cargo publish --dry-run` non lo usa, `cargo owner --list queen-mq` si |
| `GITHUB_TOKEN` | automatico in CI, niente da ruotare | si | n/a |
| `write:packages` GHCR | locale, solo per build a mano fuori CI | da rifare a scadenza | `gh auth token \| docker login ghcr.io -u <user> --password-stdin` |
| Account Packagist | credenziali web + OAuth GitHub | **da creare o verificare** | login su packagist.org |

`~/.pypirc` ha permessi `0644` (gli altri due sono `0600`). Vale la pena `chmod 600 ~/.pypirc`.

---

## Grafo delle dipendenze d'ordine

```
[0 prerequisiti]
      |
      v
[1 commit di rilascio] --> [2 push release] --> [3 tag v1.0.3]
                                  |                    |
                                  |                    +--> immagini GHCR 1.0.3
                                  |                    +--> input di [12 Packagist]
                                  |                    +--> input di [13 C++ header]
                                  v
      +--------------+-------------+----------------+
      |              |             |                |
      v              v             v                v
   [4 npm]      [5 PyPI]   [6 queen-protocol]  [9 tag client-go]
                                  |                    |
                             +----+----+               v
                             v         v        [10 bump go.mod cli]
                        [7 queen-mq] [8 queen-engine]   |
                                                        v
                                                [11 tag client-cli]
```

Vincoli non negoziabili:

1. **6 prima di 7 e 8.** `clients/client-rust/Cargo.toml:20` e `server/Cargo.toml` dichiarano
   `queen-protocol = { path = ..., version = "1.0.3" }`; cargo cancella il `path` quando
   impacchetta, quindi finche' 1.0.3 non e' sull'indice entrambi falliscono. Riprodotto:
   `cargo publish --dry-run --allow-dirty` in client-rust da
   `failed to select a version for the requirement queen-protocol = "^1.0.3" / candidate
   versions found which didn't match: 1.0.0`.
2. **9 prima di 10 prima di 11.** Il `go.mod` della CLI non puo' puntare a un `client-go v1.0.3`
   che il proxy non conosce ancora: `go mod tidy` fallirebbe.
3. **3 prima di 12 e 13.** Packagist e il consumo dell'header C++ pendono dal tag semver piano.
4. 4 e 5 sono liberi, nessuna dipendenza incrociata. Restano dopo 1-3 solo per la convenzione
   scritta in `release.mdx` (ogni SDK si rilascia contro un broker gia' pubblicato).

---

## I punti di non ritorno, in una riga ciascuno

| Passo | Cosa diventa irreversibile | Ultimo momento per accorgersene |
| --- | --- | --- |
| 4 npm | il numero 1.0.3 e il README della pagina npm | `npm publish --dry-run` e `npm pack --dry-run --json` |
| 5 PyPI | il numero 1.0.3, per sempre, anche dopo delete | l'upload su TestPyPI che `publish.sh` propone per primo |
| 6 queen-protocol | contenuto del `.crate` e pagina crates.io | `cargo publish --dry-run` in `crates/queen-protocol` |
| 7 queen-mq | idem, incluso il README con `= "1.0.0"` se non corretto | `cargo publish --dry-run`, possibile solo dopo il 6 |
| 8 queen-engine | idem | `cargo publish --dry-run`, possibile solo dopo il 6 |
| 9 tag client-go | il proxy e sum.golang.org fissano hash e contenuto per sempre | prima del `git push` del tag |
| 11 tag client-cli | idem, piu' la GitHub Release dei binari | prima del `git push` del tag |

Passi 2, 3, 12, 13 sono reversibili. Passi 4-9 e 11 no. La regola pratica: **tutto cio' che si
puo' sbagliare nei testi va sistemato al passo 1**, perche' dal passo 4 in poi il testo e'
congelato per versione.

---

## Passi

### Passo 1. Commit di rilascio su `kvtimer`

Chiude tutte le caselle di 0.A, 0.B e 0.C in un unico commit, cosi' che il commit taggato sia
esattamente quello che si pubblica.

```bash
cd /Users/alice/Work/queen
git status --short
git add -A
git commit -m "1.0.3: release fixes (locks, py __version__, README broker floor, kv.rs lean build)"
git push origin kvtimer
```

Nota: al momento il working tree ha gia' modificato `CHANGELOG.md` (blocco rinominato da
`## 1.1.0 (unreleased)` a `## 1.0.3 (2026-08-18)`, fatto) e sei file sotto `webdoc/`.

**Gate:** `gh run list --workflow=tests --branch kvtimer --limit 1` verde. `tests.yml` gira su
`push:` senza filtri di path, quindi parte da solo. Non proseguire con la CI rossa: i due job
Rust rossi sono esattamente i tre `cargo publish` dei passi 6-8.

### Passo 2. Portare `release` a 1.0.3 (fast-forward)

```bash
git push origin kvtimer:release
```

`release` e' antenato di `kvtimer` (`0 14`), quindi e' un fast-forward pulito, niente merge e
niente conflitti. Questo push fa partire `docker-build.yml`, che triggera su
`branches: [ "release" ]` (righe 13-19).

**Perche' serve:** su `kvtimer` il workflow **non parte** ne' per branch ne' per tag. Le immagini
`sha-64080e2` gia' in produzione sono state costruite a mano con `./build.sh --push --multiarch`
fuori CI, ed e' per questo che GHCR non ha un `1.0.3` ne' un `latest` prodotti dalla pipeline.
Il pattern storico di 1.0.1 e 1.0.2 e' due run per versione: uno su `release`, uno sul tag.

**Gate:**
```bash
gh run list --workflow="Docker Images" --branch release --limit 2
docker buildx imagetools inspect ghcr.io/queen-mq/queen:1.0.3
docker buildx imagetools inspect ghcr.io/queen-mq/queen-proxy:1.0.3
```
Le due versioni sono risolte separatamente dal workflow: la broker da `server/server.json` con
`jq`, la proxy da `proxy/Cargo.toml` con `awk`. Entrambe dicono 1.0.3, quindi devono uscire due
immagini `:1.0.3`. Verificare anche che il manifest sia multi-arch (`linux/amd64` e
`linux/arm64` presenti nella lista).

**Rollback:** completo. I tag GHCR sono mutabili, si sovrascrivono o si cancellano dal pannello
package. `release` si riporta indietro con un force-push. Nessun danno permanente.

### Passo 3. Tag broker `v1.0.3`

```bash
git tag -a v1.0.3 -m "Queen MQ 1.0.3: kv and timers as part of the engine" 64080e22
# oppure il nuovo sha del passo 1, se il commit di rilascio ha spostato HEAD
git push origin v1.0.3
```

Il tag ricostruisce e ripubblica le stesse immagini (`tags: [ "v*" ]`), esattamente come per
1.0.1 e 1.0.2. Serve anche come ancora per Packagist (passo 12) e per il consumo dell'header C++
(passo 13). I tag precedenti in questa forma sono `v1.0.1` e `v1.0.2`, entrambi lightweight.

**Gate:**
```bash
git ls-remote --tags origin | grep -w "refs/tags/v1.0.3"
gh run list --workflow="Docker Images" --limit 3
```

**Rollback:** possibile ma sgradevole. `git push origin --delete v1.0.3` funziona finche' nessuno
ha fatto fetch e finche' Packagist non ha ancora sincronizzato. Dopo il passo 12 il tag e' de
facto immutabile.

### Passo 4. npm, pacchetto `queen-mq`

Credenziale: `NPM_TOKEN` in `~/.npmrc` locale, **non** un secret di CI.

```bash
cd /Users/alice/Work/queen/clients/client-js
npm whoami
npm pack --dry-run --json | python3 -c "import json,sys; d=json.load(sys.stdin)[0]; print(d['name'], d['version'], d['entryCount'], d['size'])"
npm publish --dry-run
npm publish
```

Il pacchetto **non e' scoped** (`"name": "queen-mq"`), quindi niente `--access public`. Il
`npm pack --dry-run` verificato da' 95 file, 212 kB, `queen-mq-1.0.3.tgz`, con
`client-v2/kv/Kv.js`, `client-v2/kv/expiry.js` e `client-v2/builders/TimerBuilder.js` dentro: il
glob `client-v2/**/*` copre tutto il nuovo. Nel tarball **non c'e' nessun LICENSE** mentre
`package.json` dichiara `"license": "Apache-2.0"` e il badge del README punta a `LICENSE.md`, che
sta solo alla root del repo: link rotto sulla pagina npm. Se lo si vuole risolvere, va fatto al
passo 1 con `cp LICENSE.md clients/client-js/LICENSE` piu' l'aggiunta a `"files"`.

**Gate utente:**
```bash
npm view queen-mq version                     # deve dire 1.0.3
npm view queen-mq dist-tags
cd $(mktemp -d) && npm install queen-mq@1.0.3 && node -e "console.log(Object.keys(require('queen-mq')))"
```
Attenzione al dist-tag `beta = 0.16.0-beta.0` che resta li': `npm publish` senza `--tag` sposta
solo `latest`, il che e' corretto.

**PUNTO DI NON RITORNO.** L'unpublish su npm ha una finestra di 72 ore dalla pubblicazione, e
anche dentro la finestra e' consentito solo se nessun altro pacchetto pubblico dipende da quella
versione. Fuori dalle 72 ore serve il supporto npm e i criteri sono piu' stretti. In ogni caso
**una versione unpublished non e' riutilizzabile**: il numero 1.0.3 resta bruciato e la strada
diventa 1.0.4. L'alternativa non distruttiva, e quella da preferire quasi sempre, e'
`npm deprecate queen-mq@1.0.3 "usare 1.0.4"`, che lascia il pacchetto installabile ma stampa un
avviso.

### Passo 5. PyPI, pacchetto `queen-mq`

Credenziale: `PYPI_API_TOKEN` in `~/.pypirc` locale, sezione `[pypi]` con username `__token__`.
Non e' un secret di CI.

```bash
cd /Users/alice/Work/queen/clients/client-py
pip install --upgrade build twine
python -c "from queen import __version__; print(__version__)"   # DEVE dire 1.0.3
./publish.sh
```

`publish.sh` e' interattivo e fa la cosa giusta: pulisce `dist/`, costruisce con `python -m
build`, propone TestPyPI prima della produzione, poi carica con twine. Il suo prompt di conferma
legge la versione da `queen/__init__.py`, non dal pyproject: se la casella 0.A sul
`__version__` non e' chiusa, il prompt dice "Publish version 0.15.0 to PyPI?" e gli URL stampati
sono sbagliati, mentre la wheel esce comunque 1.0.3.

Via manuale, se si preferisce non usare lo script:
```bash
rm -rf dist/ build/ *.egg-info queen_mq.egg-info
python -m build
python -m twine check dist/*
python -m twine upload dist/*
```
Il `rm -rf dist/` non e' opzionale: `dist/` contiene ancora `queen_mq-1.0.0-*` del 31 luglio.

**Gate utente:**
```bash
curl -s https://pypi.org/pypi/queen-mq/json | python3 -c "import json,sys; print(json.load(sys.stdin)['info']['version'])"
cd $(mktemp -d) && python -m venv v && ./v/bin/pip install "queen-mq==1.0.3" && ./v/bin/python -c "import queen, queen.kv, queen.timers; print(queen.__version__)"
```
Quel `queen.__version__` e' anche la prova che la casella 0.A e' stata chiusa davvero.

**PUNTO DI NON RITORNO, il piu' duro dei tre registry.** Su PyPI si puo' fare *yank* (PEP 592)
dalla web UI del progetto, che lascia il file scaricabile ma lo esclude dalla risoluzione a meno
che non sia pinnato esattamente. Si puo' anche cancellare la release dalla UI. **In nessuno dei
due casi il numero di versione torna disponibile**: PyPI rifiuta per sempre il riutilizzo di un
nome di file gia' visto. Twine non ha comando di yank, e' solo interfaccia web. L'ultimo momento
utile per accorgersi di un errore e' l'upload su TestPyPI che `publish.sh` propone: dopo quello,
`pip install -i https://test.pypi.org/simple queen-mq==1.0.3` e' l'unica prova reale prima
dell'irreversibile.

### Passo 6. crates.io, `queen-protocol`. PRIMO DEI TRE

Credenziale: `CARGO_REGISTRY_TOKEN` in `~/.cargo/credentials.toml` locale. Non e' un secret di CI.

```bash
cd /Users/alice/Work/queen/crates/queen-protocol
cargo package --list
cargo publish --dry-run
cargo publish
```

Il `.crate` verificato contiene `Cargo.toml` piu' `src/{ack,admin,error,kv,lib,pop,push,streams,
timers,transaction}.rs`. Se la casella 0.B sul README non e' stata chiusa, la pagina crates.io di
questo crate esce vuota e resta vuota.

**Gate utente:**
```bash
curl -s -A queen-release https://crates.io/api/v1/crates/queen-protocol | python3 -c "import json,sys; print(json.load(sys.stdin)['crate']['max_version'])"
cd $(mktemp -d) && cargo new probe && cd probe && cargo add queen-protocol@1.0.3 && cargo check
```
Attendere che l'indice si aggiorni prima del passo 7: di norma sono secondi, ma il gate corretto
e' il `cargo add` sopra che risolve, non un timer.

**PUNTO DI NON RITORNO.** Su crates.io non esiste unpublish. L'unica leva e'
`cargo yank --version 1.0.3` (e `--undo` per annullare lo yank), che **non cancella niente**:
il `.crate` resta scaricabile per sempre, ogni `Cargo.lock` che lo pinna continua a risolverlo, e
lo yank impedisce solo che nuove risoluzioni lo scelgano. README, descrizione, keywords e
contenuto del `.crate` di una versione pubblicata non sono modificabili in nessun modo.

### Passo 7. crates.io, `queen-mq` (client Rust)

Bloccato dal passo 6.

```bash
cd /Users/alice/Work/queen/clients/client-rust
cargo package --list
cargo publish --dry-run
cargo publish
```

`Cargo.toml` non ha ne' `include` ne' `exclude`, quindi il `.crate` porta anche `tests/`, 12 file.
Non e' un blocco, e' peso. `src/kv.rs` e `src/timers.rs` entrano di default.

**Gate utente:**
```bash
curl -s -A queen-release https://crates.io/api/v1/crates/queen-mq | python3 -c "import json,sys; print(json.load(sys.stdin)['crate']['max_version'])"
cd $(mktemp -d) && cargo new probe && cd probe && cargo add queen-mq@1.0.3 && cargo check
```

**PUNTO DI NON RITORNO**, stesse condizioni del passo 6. Qui si congela anche
`clients/client-rust/README.md:8`, che oggi dice `queen-mq = "1.0.0"`.

### Passo 8. crates.io, `queen-engine` (server)

Bloccato dal passo 6, indipendente dal 7.

```bash
cd /Users/alice/Work/queen/server
cargo publish --dry-run
cargo publish
```

Il pacchetto si chiama `queen-engine` perche' il nome `queen` su crates.io e' occupato da un
crate estraneo; la `[lib]` tiene comunque l'import path `use queen::...`. `exclude` gia' toglie
gli artefatti di bench e ops, e **non** esclude `webapp/dist`, che serve a rust-embed per far
compilare la feature `server` da un crate impacchettato. La `rust-version` di questo crate e'
`1.88`, diversa dalla `1.86` del client: alberi di dipendenze diversi, e' voluto.

**Gate utente:**
```bash
curl -s -A queen-release https://crates.io/api/v1/crates/queen-engine | python3 -c "import json,sys; print(json.load(sys.stdin)['crate']['max_version'])"
cd $(mktemp -d) && cargo new probe && cd probe && cargo add queen-engine@1.0.3 && cargo check
```

**PUNTO DI NON RITORNO**, come sopra.

### Passo 9. Tag del modulo Go `clients/client-go/v1.0.3`

**Forma del tag, esatta e obbligatoria:**

```
clients/client-go/v1.0.3
```

Il module path e' `github.com/smartpricing/queen/clients/client-go` (`clients/client-go/go.mod:1`)
e il modulo non sta alla root del repo, quindi Go pretende il prefisso di sottocartella. Non e'
convenzione dedotta: `https://proxy.golang.org/github.com/smartpricing/queen/clients/client-go/@latest`
restituisce oggi `"Ref":"refs/tags/clients/client-go/v1.0.0"`.

**Cosa succede se la forma e' sbagliata:** un tag `v1.0.3` nudo (che comunque esiste gia' dal
passo 3 e vale per il repo root) non viene considerato per il modulo annidato; il proxy non vede
alcuna nuova versione, `go get github.com/smartpricing/queen/clients/client-go@v1.0.3` risponde
`invalid version: unknown revision` oppure resta fermo a v1.0.0, e `@latest` continua a servire
v1.0.0. Un tag con prefisso sbagliato (per esempio `client-go/v1.0.3` senza `clients/`) e' inerte
allo stesso modo. Non c'e' un errore rumoroso, c'e' silenzio: e' per questo che serve il gate.

```bash
cd /Users/alice/Work/queen
git tag -a clients/client-go/v1.0.3 -m "client-go 1.0.3" $(git rev-parse HEAD)
git push origin clients/client-go/v1.0.3
# scaldare il proxy: la prima richiesta e' quella che lo fa ingoiare il tag
GOPROXY=https://proxy.golang.org GONOSUMDB= GOFLAGS= \
  go list -m github.com/smartpricing/queen/clients/client-go@v1.0.3
```

**Gate utente:**
```bash
curl -s https://proxy.golang.org/github.com/smartpricing/queen/clients/client-go/@v/list
curl -s https://proxy.golang.org/github.com/smartpricing/queen/clients/client-go/@latest
cd $(mktemp -d) && go mod init probe && GOFLAGS=-mod=mod go get github.com/smartpricing/queen/clients/client-go@v1.0.3
```
`@latest` deve riportare `"Version":"v1.0.3"` e `"Ref":"refs/tags/clients/client-go/v1.0.3"`.

**PUNTO DI NON RITORNO.** Il modulo pubblicato e' immutabile per costruzione: `proxy.golang.org`
conserva lo zip e `sum.golang.org` registra l'hash del contenuto, per sempre. Cancellare il tag
da GitHub (`git push origin --delete clients/client-go/v1.0.3`) **non ritira niente**: il proxy
continua a servire la versione dalla sua cache, e ripubblicare lo stesso tag su un commit diverso
produce un errore di checksum sui client, che e' peggio del bug che si voleva correggere. Se la
v1.0.3 e' sbagliata, l'unica strada e' pubblicare v1.0.4 e aggiungere una direttiva `retract` nel
`go.mod` di quella versione successiva.

### Passo 10. Bump del `go.mod` della CLI

Bloccato dal passo 9: `go mod tidy` deve poter risolvere `client-go v1.0.3` sul proxy.

```bash
cd /Users/alice/Work/queen/clients/client-cli
GOWORK=off go get github.com/smartpricing/queen/clients/client-go@v1.0.3
GOWORK=off go mod tidy
GOWORK=off go build ./...        # DEVE compilare: e' la prova che il fix ha preso
cd /Users/alice/Work/queen
git add clients/client-cli/go.mod clients/client-cli/go.sum
git commit -m "client-cli: pin client-go v1.0.3"
git push origin kvtimer && git push origin kvtimer:release
```

`GOWORK=off` non e' pedanteria: con `go.work` attivo il sorgente locale copre il pin e il comando
"riesce" senza aver verificato niente. Oggi `go.sum` pinna `client-go v0.15.0 h1:WQS6...` e va
sostituito.

**Gate:** `grep client-go clients/client-cli/go.mod` deve dire `v1.0.3`, e il `GOWORK=off go
build ./...` sopra deve essere pulito. Se ancora escono `he.Code undefined` o `unknown field
RetentionEnabled`, il bump non ha preso e il tag del passo 11 pubblicherebbe di nuovo un
`go install` rotto.

### Passo 11. Tag della CLI `clients/client-cli/v1.0.3`. ULTIMO

**Forma del tag, esatta e obbligatoria:**

```
clients/client-cli/v1.0.3
```

Stessa ragione del passo 9, piu' una in piu': `release-cli.yml` triggera su
`tags: - 'clients/client-cli/v*'`, quindi con un tag di forma diversa **il workflow non parte
affatto** e non escono ne' binari ne' GitHub Release. Il workflow spoglia il prefisso in
`GORELEASER_CURRENT_TAG` e gira con `--skip=validate` proprio perche' il semver nudo non esiste
come tag su quel commit.

```bash
cd /Users/alice/Work/queen
git tag -a clients/client-cli/v1.0.3 -m "queenctl 1.0.3" $(git rev-parse HEAD)
git push origin clients/client-cli/v1.0.3
```

**Gate:**
```bash
gh run list --workflow="Release CLI" --limit 2
gh release view v1.0.3 --repo queen-mq/queen        # GoReleaser pubblica sotto owner queen-mq / name queen
curl -s https://proxy.golang.org/github.com/smartpricing/queen/clients/client-cli/@latest
cd $(mktemp -d) && GOBIN=$PWD go install github.com/smartpricing/queen/clients/client-cli/cmd/queenctl@v1.0.3 && ./queenctl version
```
GoReleaser v2 produce 5 archivi piu' checksums piu' completions. `bin/` e `completions/` sono
gitignorati, quindi il pre-build del workflow non sporca il tree.

Nota di contesto: nel diff `v1.0.2..HEAD` la CLI ha **zero righe di prodotto**, solo 19 righe di
README (la sezione "Not in queenctl: key/value and timers"). Questo tag serve all'allineamento
delle versioni e a ripubblicare i binari con il `go.mod` corretto, non a spedire codice nuovo.
Il che, per inciso, e' il vero motivo per cui vale la pena farlo: e' il passo che ripara
`go install ...@latest`, rotto dalla 1.0.0.

**PUNTO DI NON RITORNO**, stesse condizioni del passo 9 per la parte modulo. La GitHub Release
invece e' cancellabile e ricreabile senza conseguenze.

### Passo 12. Packagist, `smartpricing/queen-mq`. Richiede una decisione, non un comando

**Stato reale, verificato:** `https://repo.packagist.org/p2/smartpricing/queen-mq.json` risponde
404 `no packages here` e la ricerca `queen-mq` su packagist da' `{"results":[],"total":0}`. Il
vendor `smartpricing` non esiste affatto su Packagist. Quindi il
`composer require smartpricing/queen-mq` di `clients/client-laravel/README.md:9` oggi fallisce per
chiunque, e la procedura descritta in `release.mdx:128` ("push a vX.Y.Z tag; a webhook makes
Packagist pull it") non puo' funzionare: **il webhook non puo' tirare un pacchetto che non e' mai
stato sottomesso**.

C'e' un secondo problema, strutturale: **Packagist legge il `composer.json` alla root del
repository**. Qui il manifest sta in `clients/client-laravel/composer.json` e alla root non c'e'
nessun `composer.json` (verificato). Sottomettere `https://github.com/queen-mq/queen` cosi' com'e'
fallisce con "no composer.json found in root".

Tre strade, in ordine di preferenza:

**A. Repo split read-only, alimentato da subtree split.** E' lo standard dei monorepo PHP
(Symfony, Laravel). Si crea `queen-mq/queen-mq-php`, ci si spinge la sottocartella, e si registra
quello su Packagist.
```bash
cd /Users/alice/Work/queen
git subtree split --prefix=clients/client-laravel -b php-split
git push git@github.com:queen-mq/queen-mq-php.git php-split:main
cd $(mktemp -d) && git clone git@github.com:queen-mq/queen-mq-php.git && cd queen-mq-php
git tag v1.0.3 && git push origin v1.0.3
```
Poi submit una volta sola su `https://packagist.org/packages/submit` con l'URL del repo split, e
si abilita il webhook GitHub cosi' i tag successivi entrano da soli. Su repo grandi
`splitsh-lite` e' molto piu' veloce di `git subtree split`.

**B. Spostare il manifest alla root del monorepo.** Un solo `composer.json` in cima con
`autoload` che punta a `clients/client-laravel/src`. Costa poco ma dichiara il monorepo intero
come pacchetto PHP, il che e' falso e trascina ogni tag futuro dentro Packagist.

**C. Rimandare, e correggere il README.** Se la 1.0.3 esce senza Packagist, allora
`clients/client-laravel/README.md:9` non puo' continuare a dire `composer require
smartpricing/queen-mq`: va sostituito con l'installazione via repository VCS in `composer.json`,
oppure con una riga onesta che dica che il pacchetto non e' ancora su Packagist. Lasciarlo com'e'
significa spedire 2.900 righe nuove di PHP dietro un comando che non funziona.

**Forma del tag per Packagist, se si va per la A o la B:** semver piano, `v1.0.3`, sulla root del
repo che si e' registrato. Packagist **non accetta** il prefisso di sottocartella: un tag
`clients/client-laravel/v1.0.3` viene semplicemente ignorato e nessuna versione compare. Cioe' Go
e PHP vogliono due forme di tag diverse sullo stesso commit, ed e' la trappola numero uno di
questo rilascio.

`clients/client-laravel/composer.json` giustamente **non** ha campo `version`: la versione la
determina il tag. Non aggiungerlo.

**Gate utente:**
```bash
curl -s -o /dev/null -w "%{http_code}\n" https://repo.packagist.org/p2/smartpricing/queen-mq.json   # deve essere 200
cd $(mktemp -d) && composer require smartpricing/queen-mq:1.0.3
```

**Rollback:** questo e' l'unico canale genuinamente ritirabile. Packagist rispecchia i tag del
repo: cancellato il tag, alla sincronizzazione successiva la versione sparisce. Il pacchetto si
puo' anche cancellare del tutto dalla sua pagina.

### Passo 13. client-cpp: niente da pubblicare, solo da verificare

Non esiste un registry. E' header-only e i consumatori prendono
`clients/client-cpp/queen_client.hpp` al tag `v1.0.3`, che il passo 3 ha gia' creato.

Va pero' verificato che al tag l'header sia autosufficiente: include
`../server/vendor/json.hpp` e `../server/include/threadpool.hpp`, e questo commit li ha aggiunti
sotto `clients/server/` (24.765 e 1.172 righe). Il path `clients/server/` accanto a
`clients/client-*/` e' confondente e vale la pena rinominarlo, ma non in questo rilascio.

**Gate utente:**
```bash
cd $(mktemp -d)
git clone --depth 1 --branch v1.0.3 https://github.com/queen-mq/queen.git
cd queen/clients/client-cpp && make run-unit
```
Deve stampare `All KV/timer wire tests passed`. Se fallisce per un header non trovato, l'archivio
al tag e' incompleto e va corretto prima di annunciare qualunque cosa lato C++.

**Non si pubblica**, e per la stessa ragione, anche: `app/` e `proxy/console/` (interni, mai
distribuiti, ancora a 1.0.0 nei rispettivi `package.json`); `clients/client-go` non ha manifest da
bumpare perche' in Go **il tag e' la versione**; `clients/client-cli` non ha campo versione per lo
stesso motivo.

### Passo 14. Post-release

```bash
# 1. smoke test da directory pulita, non da link locale
cd $(mktemp -d) && npm install queen-mq@1.0.3
cd $(mktemp -d) && python -m venv v && ./v/bin/pip install queen-mq==1.0.3
cd $(mktemp -d) && go mod init probe && go get github.com/smartpricing/queen/clients/client-go@v1.0.3
cd $(mktemp -d) && cargo new probe && cd probe && cargo add queen-mq@1.0.3 queen-engine@1.0.3

# 2. note di rilascio su GitHub
gh release create v1.0.3 --repo queen-mq/queen --title "Queen MQ 1.0.3" --notes-file <(sed -n '/^## 1.0.3/,/^## 1.0.2/p' /Users/alice/Work/queen/CHANGELOG.md)

# 3. il sito non ha deploy in CI: docs.yml fa solo gen:check + build + lint, il deploy Cloudflare e' a mano
cd /Users/alice/Work/queen/webdoc && pnpm gen:check && pnpm build
```

Da ricontrollare a mano dopo:

- Il tag immagine citato nella webdoc in `/start` e in `/deploy/` deve corrispondere a quello
  effettivamente pubblicato.
- **Il blocco CHANGELOG della 1.0.3 non dice una parola sui client**, dove sono finite le ~5373
  righe aggiunte in questo ciclo (client-go +4.704, client-py +3.679, client-rust +3.262,
  client-js +2.931, client-laravel +2.900, client-cpp +2.308, queen-protocol +2.259). E il vuoto
  pesa piu' del normale: npm, PyPI e crates.io erano fermi tutti a 1.0.0, la 1.0.1 e la 1.0.2 non
  sono mai state pubblicate per nessun SDK. Per chi installa dal registry questo e' il salto
  intero 1.0.0 -> 1.0.3, e le note di rilascio sono l'unico posto dove puo' leggerlo.
- Deprecare esplicitamente niente: non c'e' nulla di rimosso in questo ciclo, il diff sui client
  e' additivo (circa 5373 aggiunte, 48 rimozioni, nessun simbolo pubblico tolto).

---

## Riepilogo rollback per canale

| Canale | Si ritira? | Come, con precisione |
| --- | --- | --- |
| GHCR | si | tag mutabili, si sovrascrivono o si cancellano dal pannello package |
| tag `v1.0.3` | si, finche' nessuno ha fetchato | `git push origin --delete v1.0.3` |
| npm | parzialmente | `npm unpublish queen-mq@1.0.3` entro 72 ore e solo senza dipendenti pubblici; dopo serve il supporto npm. In ogni caso il numero 1.0.3 non e' riutilizzabile. Preferire `npm deprecate` |
| PyPI | no | yank (web UI, PEP 592) esclude dalla risoluzione ma lascia il file; delete rimuove ma **non libera il numero di versione**, mai. Twine non ha yank |
| crates.io | no | solo `cargo yank --version 1.0.3` (annullabile con `--undo`), che non cancella nulla: i lock esistenti continuano a risolverlo e il `.crate` resta scaricabile per sempre. README e metadati di una versione non sono modificabili |
| Go proxy | no | proxy e sum.golang.org sono immutabili; cancellare il tag non ritira il modulo e ritaggare su altro commit rompe i checksum. Unica via avanti: v1.0.4 con `retract` |
| Packagist | si | rispecchia i tag; cancellato il tag, alla sync successiva la versione sparisce |
| GitHub Release queenctl | si | `gh release delete v1.0.3` |
