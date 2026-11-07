# Feed streams: one behaviour in every SDK

`Stream.fromFeed` (the tenant feed, windows, patterns, joins, gathers, table
joins, event time, snapshots, shards, live state) started in the JS SDK. The
Python, Go and Rust SDKs run the same engine. This directory checks that they
behave the same: the same inputs give the same outputs, the same ids, the
same position record and the same state in KV.

The JS SDK is the reference. Its results are the golden files in `expected/`.

```
make-cases.mjs         writes cases/: every scenario, in readable JavaScript
cases/<name>.json      a stream (source, chain, run options) and a script
expected/<name>.json   what the JS SDK did with it (normalized); <name>.live.json for live runs
fake-broker.mjs        a broker in memory over HTTP: feed, kv, transaction, locks, ephemeral
harness.mjs            runs a case against one SDK and compares with expected/
runners/js.mjs         the JS runner (the other SDKs keep theirs in their own tree)
runners.json           how to build and start each SDK's runner
```

```bash
node clients/conformance/feed/make-cases.mjs                 # after editing a scenario
node clients/conformance/feed/harness.mjs --sdk js --write   # regenerate expected/ from the JS SDK
node clients/conformance/feed/harness.mjs --sdk python       # compare python with expected/
node clients/conformance/feed/harness.mjs --sdk go --case window-tumbling --keep   # results/go/<case>.json
node clients/conformance/feed/harness.mjs --sdk rust --live http://127.0.0.1:17872   # live cases on a real broker
node clients/conformance/feed/harness.mjs --sdk python --cross js,python   # runners alternate SDKs within a case
```

`FEED_JS_SDK=<path to client-v2/index.js>` points the JS runner (and the
live pushes) at another copy of the JS SDK, for instance a frozen one while
the repository's is being changed.

## 1. The runner: one small program per SDK

Each SDK has a runner program that builds a stream from a case and runs it
with that SDK's own `Stream.fromFeed`. The harness starts it, talks to it on
stdin/stdout, and kills it when a case says so.

```
<runner> run  --url <broker url> --case <case.json> [--runner <name>] [--run '<json>']
<runner> view --url <broker url> --query-id <id> --op get|list|status [--step <s>] [--key <k>] [--limit <n>] [--shards <n>] [--timeout-ms <n>]
```

`run` builds `Stream.fromFeed(queen, case.source)` + `case.chain`, then
`run(case.run merged with --run)`. `--run` overrides run options (the harness
passes timing options). The process writes one JSON object per line on
stdout, and nothing else on stdout (logs go to stderr):

| line | when |
|---|---|
| `{"event":"started","role":"active"\|"standby"}` | `run()` resolved |
| `{"event":"failed","message":"...","code":"..."}` | `run()` threw; then exit 2 |
| `{"event":"foreach","value":<json>,"ctx":<json>}` | a `.foreach()` call (the runner's foreach records what it got) |
| `{"event":"error","message":"...","phase":"...","action":"..."}` | the `onError` hook was called (`ctx.phase`, `ctx.action` if any) |
| `{"event":"metrics","metrics":{...}}` | answer to `metrics` |
| `{"event":"state","answer":{...}}` | answer to `state` / `list` |
| `{"event":"done","metrics":{...}}` | the runner stopped on its own (`done` resolved: poison stop, fatal error); then exit 0 |
| `{"event":"stopped","metrics":{...}}` | after `stop`; then exit 0 |

Commands on stdin, one per line:

| command | does |
|---|---|
| `stop` | `runner.stop()`, then print `stopped` with the metrics, exit 0 |
| `metrics` | print `metrics` |
| `state <step> <key>` | print `state` with `runner.state(step).get(key)` |
| `list <step> <limit>` | print `state` with `runner.state(step).list(limit)` |

End of stdin = `stop`. Metrics use the JS names (`role`, `emitted`, `outputs`,
`effects`, `cycles`, `commits`, `conflicts`, `retries`, `poisoned`, `skipped`,
`deadLettered`, `late`, `takeovers`, `snapshots`, `replays`, `replayEntries`,
`replaySkipped`, `tableRows`, `open`, `stateKeys`, `stopped`, ...) in camelCase,
whatever the SDK calls them inside.

`view` runs `Stream.view(queen, queryId, {shards, timeoutMillis})` once, prints
`{"event":"view","answer":<json>}` (or `{"event":"failed",...}`) and exits.

## 2. A case

```json
{
  "name": "window-tumbling",
  "about": "one line: what it checks",
  "mode": "fake",
  "source": { "queues": ["orders"], "from": "earliest" },
  "chain": [ ...steps, below... ],
  "run": { "queryId": "c-window-tumbling", "onPoison": "stop" },
  "script": [ ...steps, below... ]
}
```

`mode`: `fake` (needs the fake broker: it moves the clock), `live` (runs on a
real broker too: no clock moves), or `both`.

### The chain

Each step is one call on the stream, in order. Function slots hold an
EXPRESSION (section 3). Option names are the JS names.

| step | call |
|---|---|
| `{"op":"where","args":[field, op, value]}` | `.where(field, op, value)` (value left out for `exists`) |
| `{"op":"filter","fn":E}` | `.filter(fn)` — args `(msg, ctx)` |
| `{"op":"map","fn":E}` | `.map(fn)` — args `(msg, ctx)` |
| `{"op":"flatMap","fn":E}` | `.flatMap(fn)` — args `(msg, ctx)`, answers a list |
| `{"op":"keyBy","fn":E}` | `.keyBy(fn)` — args `(msg)` |
| `{"op":"windowTumbling","opts":{...}}` | `.windowTumbling(opts)`: `seconds`, `gracePeriod`, `eventTime` (E, args `(msg)`, or `false`), `allowedLateness`, `onLate`, `idle` |
| `{"op":"windowSliding","opts":{...}}` | `size`, `slide`, the same time options |
| `{"op":"windowSession","opts":{...}}` | `gap`, the same time options |
| `{"op":"windowCron","opts":{...}}` | `every`, the same time options |
| `{"op":"reduce","fn":E,"initial":<json>}` | `.reduce(fn, initial)` — args `(acc, value)`; `initial` left out = none |
| `{"op":"aggregate","fields":{"count":E,"sum":E,...}}` | `.aggregate(extractors)` — each args `(value)`, field order kept |
| `{"op":"pattern","opts":{...}}` | `start`, `then` (args `(msg)`), `within`, `key` `(msg, value)`, `onMatch` `(start, then)`, `onTimeout` `(start)`, `onRepeat`, `keep` `(msg, value)`, `eventTime` `(msg)` or `false`, `allowedLateness`, `onLate`, `idle`, `name` |
| `{"op":"join","opts":{...}}` | two sides: `left`, `right` `(msg)`, `select` `(left, right)`, `onTimeout` `(msg, side)`, and the pattern's other options |
| `{"op":"join","opts":{"sides":{...},...}}` | many sides: `sides` = `{name: E (msg, value) or "<queue name>"}`, `select` `(got, key)`, `onTimeout` `(got, missing, key)`, `onRepeat` `keep`/`latest`, `keep`, time options |
| `{"op":"gather","opts":{...}}` | `steps` (list of queue names, or `{name: E (msg, value) or "<queue>"}`) or `count` E `(msg, value)`, `key`, `within`, `onComplete` `(got, key)`, `onTimeout` `(got, missing, key)`, `onRepeat`, `keep`, time options |
| `{"op":"joinTable","opts":{...}}` | `queue`, `on` `(msg, value)`, `key` `(row)`, `value` `(row)`, `select` `(value, row, msg)`, `missing`, `initial` |
| `{"op":"name","name":"perUser"}` | `.name(name)` |
| `{"op":"to","target":"out"}` | `.to(queen.queue("out"))`; a list: every queue; `{"fn":E}`: `.to(fn)` args `(value, msg)` answering a queue name, `{queue, partition}`, a list of those, or null; optional `"partition"`: a string, or `{"fn":E}` args `(value)` |
| `{"op":"foreach","fn":E}` | `.foreach(fn)` args `(value, ctx)`: the runner prints a `foreach` line with `value` and `ctx`, then evaluates E (so E can throw) |

`run` holds `run()` options with their JS names: `queryId`, `onPoison`
(`"stop"`, `"skip"`, or a queue name), `retries`, `retryDelayMillis`,
`exclusive`, `lockTtl`, `reset`, `shard` `{index, count}`, `shards`, `shardBy`
(an E, args `(msg)`, or absent), `snapshotEveryMillis`, `snapshotMaxEntries`,
`snapshotChunkChars`, `snapshotPieceBytes`, `commitEveryMillis`,
`positionCommitMillis`, `maxPendingOutputs`, `waitMillis`, `maxRecords`,
`serveState`, `from`. `source` holds `queues`, `partitions`, `from`, `since`.

### The script

Steps run in order; the harness waits for each one.

| step | does |
|---|---|
| `{"tx":[{"at":"<iso>","push":[{"queue","partition","data","transactionId"}]}, ...]}` | appends transactions to the log, all at once: one position each, stamped `at` (fake), or pushed through the JS client (live) |
| `{"clock":"<iso>"}` | fake: the broker's clock is now that time (the next read's `resolvedTs`), and long polls wake |
| `{"start":"A","run":{...}}` | starts runner `A` (`run` merged into the case's run options) and waits for `started` or `failed` |
| `{"idle":"A"}` | waits until runner `A` committed the end of the log and nothing moved for a while (no push, no snapshot chunk, no new position) |
| `{"stop":"A"}` | `stop` on stdin, waits for `stopped` |
| `{"kill":"A"}` | SIGKILL |
| `{"wait":"A","for":"done"}` | waits for `done` |
| `{"waitRole":"B","role":"active"}` | polls `metrics` until the role is that |
| `{"metrics":"A","as":"m1"}` | records `A`'s metrics under `m1` in the result |
| `{"state":"A","step":"s","key":"k","as":"x"}` | records `runner.state(s).get(k)` |
| `{"list":"A","step":"s","limit":10,"as":"x"}` | records `runner.state(s).list(limit)` |
| `{"view":{"op":"get","step":"s","key":"k"},"as":"x"}` | runs `<runner> view ...` and records the answer |
| `{"sleep":200}` | waits that many ms |

### What is compared

After the script, the harness reads the broker and builds the RESULT:

- `outputs`: every message the query pushed (its transaction ids start with
  the query's prefix), in log order: `{queue, partition, data, from}`, where
  `from` is the script transaction the output came from (`"S3.1"`: the 3rd
  `tx` step, its 1st transaction, and `#i` the output's index) for an output
  made from a feed message, or `"emit"` for one made by a close (a window, a
  deadline, a released message). Ids of message outputs must equal
  `fd-<sha256("queen-feed\n"+queryId)[:8]>-<sha256(scope\nepoch\nposition\nqueue\npartition\noffset\ni)[:40]>`;
  ids of emits must have the prefix, 40 hex after it, and be unique.
- `position`: the position record `queen-feed`/`<id>`: `position` (as a script
  place, `end` for the log's next position), `epoch`, `hash` (the chain hash:
  must match byte for byte), `clock`, `units`, `skips`, `shard`, `resetToken`,
  `readTo`, `caughtUp`, `open`, `held`, `stopped` (without `at` and the error's
  text), and the snapshot pointer's `position`, `units` and `metas`.
- `about`: `queen-feed`/`<queryId>#about`, without `client`.
- `layout`: `<queryId>#layout` (`shards`, `shardBy`, `resetToken`).
- `state`: the snapshot it names, decoded (every layer applied in order):
  per step `{kind, entries}` with entries sorted by key; message positions and
  ids normalized to script places.
- `foreach`, `errors` (`phase`/`action`, not the message text), `failed`,
  and what `metrics`/`state`/`list`/`view` steps recorded (metrics: the keys
  the case lists in `"compareMetrics"`, default
  `emitted, outputs, effects, poisoned, skipped, deadLettered, late, replaySkipped`).

Numbers compare by value (`2` = `2.0`); object keys in any order; lists in
order.

## 3. Expressions

A function slot holds a JSON expression, evaluated with the function's
arguments. MISSING is JavaScript's `undefined` (a field that is not there).

| expression | value |
|---|---|
| a number, string, `true`, `false`, `null` | itself |
| `[E, ...]` | a list (a MISSING item becomes `null`, as `JSON.stringify` does) |
| `{"k": E, ...}` (no key starting with `$`) | an object; a field whose value is MISSING is left out |
| `{"$get":"1.data.amount"}` | argument 1, then field `data`, then `amount`; a number indexes a list; MISSING when anything on the way is missing or not an object/list. `"0"` is argument 0 itself |
| `{"$eq":[A,B]}` / `{"$ne":[A,B]}` | deep equality of JSON values (MISSING equals only MISSING) |
| `{"$lt":[A,B]}` `$lte` `$gt` `$gte` | two numbers or two strings compared; anything else is `false` |
| `{"$and":[...]}` / `{"$or":[...]}` / `{"$not":E}` | truthiness as in JS (`false`, `0`, `""`, `null`, MISSING are false); answers `true`/`false`; `$and`/`$or` stop early |
| `{"$if":[C,A,B]}` | `A` when `C` is true, else `B` (only that one is evaluated) |
| `{"$add":[...]}` | the sum of the numbers (anything else counts 0) |
| `{"$mod":[A,B]}` | `A % B` on numbers |
| `{"$str":E}` | JavaScript `String(v)`: a string as is, a number as JS prints it (`3`, not `3.0`), `true`, `null`; MISSING is `"undefined"` |
| `{"$concat":[...]}` | the `$str` of each, joined |
| `{"$len":E}` | length of a list or string, else 0 |
| `{"$keys":E}` | an object's keys, sorted |
| `{"$missing":true}` | MISSING (a function that answers `undefined`) |
| `{"$throw":"text"}` | throws an error with message `text` |
| `{"$flaky":{"times":N,"key":E,"then":E}}` | throws the first N times it is called with that key (in this process), then answers `then` |

A function answers the expression's value; MISSING is `undefined` in JS,
`None` in Python only where the SDK's own API reads absence (onMatch,
select, onTimeout, a `.to()` function, a table row's value), and so on.
Messages are the JS shapes: a feed message has `position`, `kind`, `queue`,
`partition`, `partitionId`, `offset`, `id`, `transactionId`, `data`, `ts`,
`createdAt`; a step's result seen by a later pattern, join or gather has
`position`, `queue` (null), `partition`, `key`, `data`, `ctx`, `ts`,
`derived`. Each SDK hands its own message type to the expression as that JSON
shape.

## 4. Live

Cases with `mode` `live` or `both` also run on a real broker
(`--live <url>`; a 3-node cluster URL checks the follower reads too). `clock`
steps are skipped there, so those cases use event time, or only check
counts. The live run also checks moves BETWEEN SDKs: a stream started by one
SDK, stopped (or killed), and continued by another with the same queryId
must end with the same outputs as one SDK alone (`--cross js,python,go,rust`).
