import assert from 'node:assert/strict'
import { request } from 'node:http'
import { PassThrough } from 'node:stream'
import { after, before, test } from 'node:test'

import { ConfigError, loadConfig } from '../src/config.mjs'
import { createProtocol } from '../src/protocol.mjs'
import { hostName, refusal, serveHttp, serveStdio } from '../src/transports.mjs'

const protocol = createProtocol({
  tools: [{ name: 'echo', title: 'Echo', description: 'd', inputSchema: { type: 'object', properties: { x: { type: 'string' } }, additionalProperties: false }, run: async (a) => a }],
  info: { name: 'test', version: '0' },
  instructions: 'i',
})

let server
let port

before(async () => {
  server = await serveHttp(protocol, { host: '127.0.0.1', port: 0 })
  port = server.address().port
})
after(() => server.close())

// fetch cannot set Host or Origin freely, so this is a plain request.
function send({ method = 'POST', path = '/mcp', headers = {}, body }) {
  return new Promise((resolve, reject) => {
    const req = request({ host: '127.0.0.1', port, method, path, headers: { host: `localhost:${port}`, ...headers } }, (res) => {
      let text = ''
      res.on('data', (c) => { text += c })
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, text }))
    })
    req.on('error', reject)
    if (body !== undefined) req.write(body)
    req.end()
  })
}
const JSON_HEADERS = { 'content-type': 'application/json' }
const ping = JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'ping' })

test('a local MCP client is answered', async () => {
  const r = await send({ headers: JSON_HEADERS, body: ping })
  assert.equal(r.status, 200)
  assert.deepEqual(JSON.parse(r.text), { jsonrpc: '2.0', id: 1, result: {} })
})

test('whatever local port the forward picked, the host is accepted', async () => {
  for (const host of ['localhost:17700', '127.0.0.1:9', '[::1]:7700', 'LOCALHOST:1', 'localhost']) {
    const r = await send({ headers: { ...JSON_HEADERS, host }, body: ping })
    assert.equal(r.status, 200, host)
  }
})

test('a browser is refused, even from localhost', async () => {
  for (const origin of ['https://evil.test', 'http://localhost:3000', 'http://127.0.0.1:7700', 'null']) {
    const r = await send({ headers: { ...JSON_HEADERS, origin }, body: ping })
    assert.equal(r.status, 403, origin)
  }
  const fetched = await send({ headers: { ...JSON_HEADERS, 'sec-fetch-site': 'cross-site' }, body: ping })
  assert.equal(fetched.status, 403)
})

test('DNS rebinding is refused', async () => {
  for (const host of ['evil.test', 'evil.test:7700', 'localhost.evil.test', '127.0.0.1.evil.test:80', '10.0.0.5:7700']) {
    const r = await send({ headers: { ...JSON_HEADERS, host }, body: ping })
    assert.equal(r.status, 403, host)
  }
})

test('no answer carries a CORS header, a preflight included', async () => {
  for (const r of [
    await send({ headers: JSON_HEADERS, body: ping }),
    await send({ method: 'OPTIONS', headers: { 'access-control-request-method': 'POST' } }),
    await send({ method: 'OPTIONS', headers: { origin: 'https://evil.test', 'access-control-request-method': 'POST' } }),
  ]) {
    assert.ok(!Object.keys(r.headers).some((h) => h.startsWith('access-control-')), JSON.stringify(r.headers))
    assert.notEqual(r.status, 204)
  }
})

test('a form post cannot reach the protocol', async () => {
  for (const type of ['text/plain', 'application/x-www-form-urlencoded', 'multipart/form-data']) {
    const r = await send({ headers: { 'content-type': type }, body: ping })
    assert.equal(r.status, 415, type)
  }
})

test('methods, paths and limits', async () => {
  assert.equal((await send({ method: 'GET' })).status, 405)
  assert.equal((await send({ method: 'DELETE' })).status, 405)
  assert.equal((await send({ path: '/other', headers: JSON_HEADERS, body: ping })).status, 404)
  assert.equal((await send({ method: 'GET', path: '/healthz' })).status, 200)
  assert.equal((await send({ headers: JSON_HEADERS, body: 'not json' })).status, 400)
  assert.equal((await send({ headers: { ...JSON_HEADERS, 'mcp-protocol-version': '1999-01-01' }, body: ping })).status, 400)
  assert.equal((await send({ headers: JSON_HEADERS, body: JSON.stringify({ x: 'y'.repeat(2 * 1024 * 1024) }) })).status, 413)
})

test('a notification gets 202 and no body', async () => {
  const r = await send({ headers: JSON_HEADERS, body: JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) })
  assert.equal(r.status, 202)
  assert.equal(r.text, '')
})

test('the host check reads names correctly', () => {
  assert.equal(hostName('LocalHost:80'), 'localhost')
  assert.equal(hostName('[::1]:7700'), '[::1]')
  assert.equal(refusal({ host: 'localhost:1' }), null)
  assert.equal(refusal({ host: undefined }), 'host')
  assert.equal(refusal({ host: 'localhost:1', 'sec-fetch-site': 'none' }), null)
})

test('it refuses to listen anywhere but loopback', () => {
  const load = (listen) => loadConfig(['--brokers', 'localhost:6632', '--listen', listen], {})
  assert.equal(load('127.0.0.1:7700').listen.port, 7700)
  assert.equal(load('[::1]:7700').listen.host, '::1')
  assert.equal(load('localhost:7700').listen.host, 'localhost')
  for (const bad of ['0.0.0.0:7700', '[::]:7700', '10.0.0.5:7700', 'example.test:7700', '7700']) {
    assert.throws(() => load(bad), ConfigError, bad)
  }
})

test('the token comes from the environment, never from a flag', () => {
  assert.throws(() => loadConfig(['--brokers', 'localhost:6632', '--token', 'x'], {}), ConfigError)
  assert.equal(loadConfig(['--brokers', 'localhost:6632'], { QUEEN_DEBUG_TOKEN: 'abc' }).token, 'abc')
})

test('configuration errors', () => {
  assert.throws(() => loadConfig([], {}), ConfigError, 'no brokers')
  assert.throws(() => loadConfig(['--brokers', 'ftp://x:1'], {}), ConfigError)
  assert.throws(() => loadConfig(['--brokers', 'http://user:pw@x:1'], {}), ConfigError)
  assert.throws(() => loadConfig(['--brokers', 'x:1', '--mask', 'guest.email'], {}), ConfigError)
  assert.throws(() => loadConfig(['--brokers', 'x:1', '--transport', 'tcp'], {}), ConfigError)
  const defaults = loadConfig(['--brokers', 'x:1'], {})
  assert.equal(defaults.readMessages, false, 'bodies are off by default')
  assert.equal(defaults.allowProbe, false, 'the write is off by default')
  assert.equal(defaults.listen.host, '127.0.0.1')
})

test('stdio: one line in, one line out, nothing else on stdout', async () => {
  const input = new PassThrough()
  const output = new PassThrough()
  let out = ''
  output.on('data', (c) => { out += c })
  const done = serveStdio(protocol, { input, output })
  input.write(`${ping}\n`)
  input.write('\n')
  input.write('not json\n')
  input.write(`${JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' })}\n`)
  input.write(`${JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'echo', arguments: { x: 'hi' } } })}\n`)
  input.end()
  await done
  const lines = out.trim().split('\n').map((l) => JSON.parse(l))
  assert.equal(lines.length, 3)
  assert.ok(lines.some((l) => l.id === 1))
  assert.ok(lines.some((l) => l.error?.code === -32700))
  assert.equal(JSON.parse(lines.find((l) => l.id === 2).result.content[0].text).x, 'hi')
})
