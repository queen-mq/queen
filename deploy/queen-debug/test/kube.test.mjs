// The Kubernetes client against a real HTTPS server with its own CA, so the
// certificate check and the token handling run as they do in a pod.

import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { createServer } from 'node:https'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, before, test } from 'node:test'

import { SERVICE_ACCOUNT_DIR } from '../src/config.mjs'
import { KubeError, createKube } from '../src/kube.mjs'

const hasOpenssl = spawnSync('openssl', ['version']).status === 0
const skip = hasOpenssl ? false : 'needs the openssl command'

let dir
let server
let port
let cert
const seen = []

const POD = {
  metadata: { name: 'queen-0' },
  spec: { nodeName: 'k1', containers: [{ name: 'broker' }] },
  status: {
    podIP: '10.0.0.9',
    phase: 'Running',
    conditions: [{ type: 'Ready', status: 'True' }],
    containerStatuses: [{ name: 'broker', restartCount: 2, state: { running: { startedAt: 't' } }, lastState: { terminated: { exitCode: 75, reason: 'Error', finishedAt: 't0' } } }],
  },
}

function makeCert(name) {
  const key = join(dir, `${name}.key`)
  const crt = join(dir, `${name}.crt`)
  const r = spawnSync('openssl', ['req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1', '-days', '1', '-keyout', key, '-out', crt])
  assert.equal(r.status, 0, String(r.stderr))
  return { key: readFileSync(key), cert: readFileSync(crt) }
}

before(async () => {
  if (skip) return
  dir = mkdtempSync(join(tmpdir(), 'queen-debug-kube-'))
  cert = makeCert('server')
  server = createServer({ key: cert.key, cert: cert.cert }, (req, res) => {
    const url = new URL(req.url, 'https://localhost')
    seen.push({ method: req.method, path: url.pathname, query: Object.fromEntries(url.searchParams), auth: req.headers.authorization })
    if (req.headers.authorization === 'Bearer refused') {
      res.writeHead(403, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ message: 'pods is forbidden' }))
    } else if (url.pathname.endsWith('/log')) {
      res.end('line one\nline two\n')
    } else if (url.pathname.endsWith('/pods')) {
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ items: [POD] }))
    } else if (url.pathname.endsWith('/events')) {
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ items: [
        { lastTimestamp: '2026-01-01T00:00:00Z', type: 'Normal', reason: 'Pulled', message: 'old', count: 1 },
        { lastTimestamp: '2026-01-02T00:00:00Z', type: 'Warning', reason: 'BackOff', message: 'new', count: 3 },
      ] }))
    } else {
      res.writeHead(404)
      res.end('{}')
    }
  })
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  port = server.address().port
})

after(() => {
  server?.close()
  server?.closeAllConnections?.()
  if (dir) rmSync(dir, { recursive: true, force: true })
})

function kube({ token = () => 'tok', ca = () => cert.cert } = {}) {
  const read = (path) => {
    if (path === `${SERVICE_ACCOUNT_DIR}/token`) return token()
    if (path === `${SERVICE_ACCOUNT_DIR}/ca.crt`) return ca()
    throw new Error(`unexpected read: ${path}`)
  }
  return createKube({ namespace: 'queen', selector: 'run=queen-mq', env: { KUBERNETES_SERVICE_HOST: 'localhost', KUBERNETES_SERVICE_PORT: String(port) }, read })
}

test('outside a cluster there is no client', () => {
  assert.equal(createKube({ namespace: 'q', selector: 's', env: {} }), null)
})

test('it lists the broker pods, with its own token', { skip }, async () => {
  seen.length = 0
  const pods = await kube().pods()
  assert.equal(pods[0].name, 'queen-0')
  assert.equal(pods[0].ip, '10.0.0.9')
  assert.equal(pods[0].restarts, 2)
  assert.equal(pods[0].lastExit.code, 75)
  assert.deepEqual(seen[0], { method: 'GET', path: '/api/v1/namespaces/queen/pods', query: { labelSelector: 'run=queen-mq' }, auth: 'Bearer tok' })
})

test('it reads a rotated token on the next call', { skip }, async () => {
  seen.length = 0
  let current = 'first'
  const k = kube({ token: () => current })
  await k.pods()
  current = 'second\n'
  await k.pods()
  assert.deepEqual(seen.map((s) => s.auth), ['Bearer first', 'Bearer second'])
})

test('it reads the log of the run that died', { skip }, async () => {
  seen.length = 0
  const text = await kube().logs('queen-0', { container: 'broker', previous: true, tailLines: 50, sinceSeconds: 600 })
  assert.equal(text, 'line one\nline two\n')
  assert.equal(seen[0].path, '/api/v1/namespaces/queen/pods/queen-0/log')
  assert.deepEqual(seen[0].query, { container: 'broker', previous: 'true', tailLines: '50', sinceSeconds: '600', timestamps: 'true', limitBytes: '2097152' })
})

test('it reads a pod\'s events, newest first', { skip }, async () => {
  seen.length = 0
  const events = await kube().events('queen-0')
  assert.deepEqual(events.map((e) => e.reason), ['BackOff', 'Pulled'])
  assert.equal(seen[0].query.fieldSelector, 'involvedObject.name=queen-0,involvedObject.kind=Pod')
})

test('a refusal says what Kubernetes said', { skip }, async () => {
  await assert.rejects(kube({ token: () => 'refused' }).pods(), (e) => e instanceof KubeError && e.status === 403 && /pods is forbidden/.test(e.message))
})

test('it does not talk to a server its CA did not sign', { skip }, async () => {
  const other = makeCert('other')
  seen.length = 0
  await assert.rejects(kube({ ca: () => other.cert }).pods(), KubeError)
  assert.equal(seen.length, 0, 'the token must not have been sent')
})

test('a pod name cannot climb out of its path', { skip }, () => {
  assert.throws(() => kube().logs('..'), RangeError)
})
