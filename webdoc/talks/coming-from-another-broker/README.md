# Coming from another message broker?

An interactive talk about QueenMQ, 48 slides. One booking goes through eight chapters. Each chapter shows how the work is done today, Kafka first with RabbitMQ and SQS next to it, and then in QueenMQ. Three production flows and "Try it" close it.

It is one version, safe to publish: no company names, internal services or incident numbers.

## Build and look

```sh
python3 webdoc/talks/coming-from-another-broker/build.py
cd webdoc/public && python3 -m http.server 4395
```

Open http://localhost:4395/talks/coming-from-another-broker/, or the same path on the docs dev server. Keys: arrows to move, F for full screen.

The build writes the talk into the site, `webdoc/public/talks/coming-from-another-broker/`, and the site serves it from https://queenmq.com/talks/coming-from-another-broker/. Edit `src/`, then build again: the copy in `public/` is generated. Under the site's security policy (`public/_headers`) the scripts are files (`theme.js`, `deck.js`), never inline, and the two fonts are the site's own. The page is `noindex`: it is reached from the homepage, not from search.

## Files

| File | What it holds |
| --- | --- |
| `src/head.html`, `src/foot.html` | the page around the slides |
| `src/style.css` | the site's tokens (`--nb-*` from `src/styles/globals.css`) and the slide layout |
| `src/core.js` | navigation, theme, and the helpers the demos share |
| `src/ch0` to `src/ch10` | one chapter each: `.html` the slides, `.js` the demos. `ch0` is the opening, `ch9` the production flows, `ch10` the end |

The wordmark comes from `webdoc/src/assets/queen.svg` and the scene from `webdoc/public/queen-scene-on-*.svg`, read at build time. The demos are simulated in the page: no broker runs.
