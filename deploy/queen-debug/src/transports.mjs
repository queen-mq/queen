// Stdio for one machine, HTTP on loopback for a pod reached by port-forward.
//
// The HTTP side has no login: Kubernetes decides who can open the forward. So
// it must not answer a browser. While a forward is open it sits on the user's
// localhost, where any web page could otherwise post to it.

import { createServer } from 'node:http'
import { createInterface } from 'node:readline'

import { PROTOCOL_VERSIONS } from './protocol.mjs'

const MAX_REQUEST_BYTES = 1024 * 1024
const LOOPBACK_NAMES = new Set(['localhost', '127.0.0.1', '[::1]'])

const plain = (res, status, text, headers = {}) => {
  res.writeHead(status, { 'content-type': 'text/plain; charset=utf-8', 'x-content-type-options': 'nosniff', ...headers })
  res.end(`${text}\n`)
}
const json = (res, status, body) => {
  res.writeHead(status, { 'content-type': 'application/json', 'x-content-type-options': 'nosniff' })
  res.end(JSON.stringify(body))
}

/** The host name of a Host header, whatever local port the forward picked. */
export function hostName(header) {
  const h = String(header ?? '').trim().toLowerCase()
  if (h.startsWith('[')) return h.slice(0, h.indexOf(']') + 1)
  return h.split(':')[0]
}

/** Why a request must not be served, or null. */
export function refusal(headers) {
  if (!LOOPBACK_NAMES.has(hostName(headers.host))) return 'host'
  if (headers.origin !== undefined) return 'origin'
  const site = headers['sec-fetch-site']
  if (site !== undefined && site !== 'none') return 'origin'
  return null
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let total = 0
    req.on('data', (c) => {
      total += c.length
      if (total > MAX_REQUEST_BYTES) {
        reject(Object.assign(new Error('too large'), { tooLarge: true }))
        req.resume()
        return
      }
      chunks.push(c)
    })
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
    req.on('error', reject)
  })
}

export function createHandler(protocol, log = () => {}) {
  return async function handle(req, res) {
    const path = new URL(req.url, 'http://localhost').pathname.replace(/\/+$/, '')

    const why = refusal(req.headers)
    if (why) {
      log({ at: new Date().toISOString(), event: 'refused', why })
      plain(res, 403, 'This server answers MCP clients on localhost only.')
      return
    }
    if (path === '/healthz' && req.method === 'GET') {
      plain(res, 200, 'ok')
      return
    }
    if (path !== '/mcp') {
      plain(res, 404, 'Not found. The MCP endpoint is /mcp.')
      return
    }
    if (req.method !== 'POST') {
      plain(res, 405, 'POST only.', { allow: 'POST' })
      return
    }
    const version = req.headers['mcp-protocol-version']
    if (version && !PROTOCOL_VERSIONS.includes(version)) {
      json(res, 400, { jsonrpc: '2.0', id: null, error: { code: -32600, message: `Unsupported MCP-Protocol-Version "${version}".` } })
      return
    }
    if (!String(req.headers['content-type'] ?? '').toLowerCase().startsWith('application/json')) {
      json(res, 415, { jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Content-Type must be application/json.' } })
      return
    }

    let parsed
    try {
      parsed = JSON.parse(await readBody(req))
    } catch (e) {
      if (e.tooLarge) json(res, 413, { jsonrpc: '2.0', id: null, error: { code: -32600, message: 'Request too large.' } })
      else json(res, 400, { jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error.' } })
      return
    }
    const batch = Array.isArray(parsed)
    const messages = batch ? parsed : [parsed]
    if (!messages.length) {
      json(res, 400, { jsonrpc: '2.0', id: null, error: { code: -32600, message: 'Empty batch.' } })
      return
    }
    const replies = (await Promise.all(messages.map((m) => protocol.dispatch(m)))).filter(Boolean)
    if (!replies.length) {
      res.writeHead(202)
      res.end()
      return
    }
    json(res, 200, batch ? replies : replies[0])
  }
}

export function serveHttp(protocol, { host, port }, log = () => {}) {
  const handle = createHandler(protocol, log)
  const server = createServer((req, res) => {
    handle(req, res).catch((e) => {
      log({ at: new Date().toISOString(), event: 'internal_error', detail: String(e?.stack ?? e) })
      if (!res.headersSent) plain(res, 500, 'Internal error.')
      else res.destroy()
    })
  })
  server.headersTimeout = 10_000
  server.requestTimeout = 120_000
  return new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(port, host, () => {
      server.off('error', reject)
      resolve(server)
    })
  })
}

/** One JSON message per line. Only protocol messages go to stdout. */
export function serveStdio(protocol, { input = process.stdin, output = process.stdout } = {}) {
  const lines = createInterface({ input, crlfDelay: Infinity })
  const running = new Set()
  const send = (m) => output.write(`${JSON.stringify(m)}\n`)

  lines.on('line', (line) => {
    if (!line.trim()) return
    let parsed
    try {
      parsed = JSON.parse(line)
    } catch {
      send({ jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error.' } })
      return
    }
    // Answered as they finish: a slow tool must not hold up a ping.
    const work = (async () => {
      if (Array.isArray(parsed)) {
        const replies = (await Promise.all(parsed.map((m) => protocol.dispatch(m)))).filter(Boolean)
        if (replies.length) send(replies)
      } else {
        const reply = await protocol.dispatch(parsed)
        if (reply) send(reply)
      }
    })()
    running.add(work)
    work.finally(() => running.delete(work))
  })

  return new Promise((resolve) => {
    lines.on('close', async () => {
      await Promise.allSettled([...running])
      resolve()
    })
  })
}
