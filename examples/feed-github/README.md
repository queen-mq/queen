# A day of GitHub through Queen feed streams

A day of public GitHub activity (GitHub Archive, 2025-10-01, plus the next
morning: 4.1 million events) pushed into Queen as history, and processed by
four feed streams **on event time**: by the time GitHub recorded, not by the
time the events were pushed. Every result is then checked against the same
day computed in batch.

What it shows:

- **History, processed as a stream.** The 30 hours go in at about 55,000
  events a second. The streams answer by GitHub's clock: an issue opened at
  09:00 and answered at 09:40 is "answered within the hour", although both
  were pushed a millisecond apart.
- **One order across queues.** Issues and comments, pull requests and reviews
  are separate queues; a stream reads them in one order and joins them by
  entity.
- **Patterns, gathers, chained windows and sessions on event time.** Out of
  order by up to two minutes (`allowedLateness`), each message takes its place;
  later than that, it is dropped and counted.
- **Exactly once across a crash.** `killtest.mjs` kills the process with
  SIGKILL partway; the next one takes over and every result is still exactly
  the batch one, sent once.
- **Live state.** `page.mjs` shows the streams as they run: their watermarks,
  the issues waiting for an answer right now (the pattern's own state), and
  this hour's stars.

## The streams (`streams.mjs`)

| Stream | Kind | What it sends |
|---|---|---|
| `unanswered` | pattern | an issue opened, and no reply from someone else within an hour; or how long the first reply took |
| `prs` | gather | a pull request reviewed and merged within 6 hours, or merged with no review |
| `trending` | two windows | stars per repo per hour, then each hour's top 10 (the second window runs on the first one's event times) |
| `bursts` | session window | one account's sessions of 100 pushes or more, with gaps under 5 minutes |

## Run it

You need a broker with the feed (built from the `srf-sdk` branch). In this
repo, `.claude/launch.json` has `queen-feed-demo` (port 17862); otherwise set
`QUEEN_URL`. Data goes to `~/Work/queen-data/gharchive` (`GH_DATA` to change).

1. Download the 30 hours (2.9 GB): `./fetch.sh`
2. Slim them to the fields the streams read (about a minute, 580 MB): `node prepare.mjs`
3. Push them: `node push.mjs` (as fast as it goes), or `node push.mjs --speed 1200` (30 hours in 90 seconds, to watch)
4. Run the streams: `node streams.mjs`, and the page: `node page.mjs`, then open http://127.0.0.1:4390
5. Kill and check, on a fresh prefix: `GH_PREFIX=gh2 node push.mjs`, then
   `GH_PREFIX=gh2 GH_IDLE=off node killtest.mjs` (or `node check.mjs` on its
   own, once the streams caught up)

`GH_PREFIX` (default `gh`) prefixes every queue and stream, so several runs
fit on one broker. Keep one `GH_IDLE` setting per prefix: `idle` is part of a
stream's steps, and a stream whose steps changed is refused while it has state.

## Numbers (Alice's Mac, 2026-11-07)

One scratch broker (release build of `srf-sdk`), one Node process for all four streams.

| What | Result |
|---|---|
| Downloaded | 30 hours, 4,975,203 events, 2.9 GB |
| Prepared | 4,127,739 events kept, 580 MB, 66 s |
| Pushed | 4,127,739 events in 78-81 s (about 51,000-55,000 a second), broker at 1.4 GB |
| Streams caught up (no crash) | 32 s for all four, 4.1 million messages read by them (3.2 million by `bursts` alone) |
| Killed with SIGKILL at 50% | the next process past the end 31 s after the kill (15 s of it the dead process's lock running out); 52 s in all |
| Checked against batch | all exact, each result once: 39,174 issue verdicts (863 answered within the hour), 42,225 pull requests (5,432 reviewed and merged, 36,793 merged with no review), 29 hourly top 10s, 468 push bursts; 12 pushes late and dropped, the same in both |

## Notes

- **Partitions.** A day has about a million repos and half a million accounts
  pushing. One partition each is a lot for a broker on a laptop, so repos (and,
  for pushes, accounts) are hashed into 1,024 partitions; every stream keys by
  the repo or account itself, and a partition keeps the order of each one.
- **Late events.** GitHub Archive records a few events late (up to an hour).
  Two minutes of `allowedLateness` takes nearly all of them in order; the rest
  are dropped and counted (`late` on the page), the same way in `check.mjs`.
- **A push covers 30 seconds at most.** The feed gives the messages of one
  transaction grouped by partition, so a batch that covered more time than
  `allowedLateness` would make its own messages late.
- **Idle.** With `idle` (30 s) on each stream's first step, a quiet feed moves
  that step's watermark on with the feed's clock (the trending stream's second
  window follows the first one's), so the last windows and deadlines close
  after the import too, at the pace of that clock. `check.mjs` compares
  everything up to the watermark the import itself reached; `GH_IDLE=off` keeps
  the streams there.
