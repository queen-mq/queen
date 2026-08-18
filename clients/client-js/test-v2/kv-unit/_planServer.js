/**
 * A real node:http server driven by a canned response plan, recording every
 * request it receives (method, url, parsed body).
 *
 * Same shape as test-v2/http-unit/retry429.test.js's withPlanServer and
 * streams-unit/fakeServer.js -- no mocking framework, a real socket, real
 * fetch, real JSON. The difference is that these tests assert the REQUEST,
 * not the retry behaviour: the exact JSON body of every KV and timer
 * operation is the contract towards the broker, and a plan server is the only
 * place that contract can be pinned without a database.
 */

import { createServer } from 'node:http'

export function withPlanServer(plan, defaultResponse, run) {
  const hits = []
  let index = 0

  const server = createServer((req, res) => {
    let raw = ''
    req.on('data', chunk => { raw += chunk })
    req.on('end', () => {
      let body = null
      if (raw.length > 0) {
        try {
          body = JSON.parse(raw)
        } catch {
          body = { __unparseable: raw }
        }
      }
      hits.push({ method: req.method, url: req.url, raw, body })

      const descriptor = index < plan.length ? plan[index] : defaultResponse
      index++

      const status = descriptor.status ?? 200
      const headers = { 'Content-Type': 'application/json', ...(descriptor.headers || {}) }
      res.writeHead(status, headers)
      res.end(JSON.stringify(descriptor.body ?? { ok: true }))
    })
  })

  return new Promise((resolve, reject) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address()
      const teardown = () => new Promise(r => server.close(r))
      Promise.resolve()
        .then(() => run(`http://127.0.0.1:${port}`, hits))
        .then(
          async (value) => { await teardown(); resolve(value) },
          async (err) => { await teardown(); reject(err) }
        )
    })
  })
}

/** One canned 200 with this JSON body. */
export function ok(body) {
  return { status: 200, body }
}

/** The KV batch envelope: {"results":[...]}, index-aligned to the input. */
export function kvResults(...results) {
  return ok({ results })
}
