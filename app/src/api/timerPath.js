// The address of one timer, and why it is its own module.
//
// A timer key is ARBITRARY CALLER TEXT — `wh.deliver:promotion-publication`,
// `order/9f1`, anything a producer typed — and the broker takes it as a
// wildcard path segment (`/api/v1/timers/:queue/*timerKey`,
// server/src/handlers/timers.rs). Two things follow, and only the first is
// obvious:
//
//   · a `/` inside an unencoded key still ROUTES (the wildcard swallows it),
//     so the bug hides: peek and cancel keep working for every key anybody
//     tries by hand, right up to the first key carrying `?`, `#` or a space,
//     which addresses a different timer or nothing at all;
//   · the convention is not uniform across this product. client-js and
//     client-php send `%2F` (clients/client-js/test-v2/kv-unit/timerWire.test.js
//     pins `/api/v1/timers/<q>/order%2F9f1`), while client-go deliberately
//     un-escapes `%2F` back to `/` (clients/client-go/timers.go). The dashboard
//     follows client-js, and the test next door is what says so.
//
// It lives here rather than inside api/index.js because that module reaches the
// network through api/client.js, which needs `import.meta.env` and the identity
// store: nothing that imports it can be loaded by `node --test`, and an
// untested encoder is exactly the kind of one-liner a later "simplification"
// turns back into a template literal.

/** `/api/v1/timers/<queue>/<timerKey>`, both segments percent-encoded. */
export function timerAddr(queue, timerKey) {
  return `/api/v1/timers/${encodeURIComponent(queue)}/${encodeURIComponent(timerKey)}`
}

/** `/api/v1/timers/<queue>` — the list and count address. */
export function timerQueueAddr(queue) {
  return `/api/v1/timers/${encodeURIComponent(queue)}`
}
