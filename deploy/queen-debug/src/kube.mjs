// Three reads of the Kubernetes API, with this pod's own service account: the
// broker pods, one pod's log, one pod's events. They need `get` and `list` on
// pods, `get` on pods/log and `list` on events, in the brokers' namespace.

import { request } from 'node:https'
import { readFileSync } from 'node:fs'

import { SERVICE_ACCOUNT_DIR } from './config.mjs'
import { segment, withQuery } from './routes.mjs'

const MAX_BYTES = 4 * 1024 * 1024
const TIMEOUT_MS = 5_000

export class KubeError extends Error {
  constructor(message, status = null) {
    super(message)
    this.name = 'KubeError'
    this.status = status
  }
}

function fetchText({ host, port, ca, token, path }) {
  return new Promise((resolve, reject) => {
    const req = request({ host, port, path, method: 'GET', ca, timeout: TIMEOUT_MS, headers: { authorization: `Bearer ${token}`, accept: 'application/json, */*' } }, (res) => {
      const chunks = []
      let total = 0
      res.on('data', (c) => {
        total += c.length
        if (total > MAX_BYTES) {
          req.destroy(new KubeError('the Kubernetes API answered more than this server reads at once'))
          return
        }
        chunks.push(c)
      })
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8')
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(text)
          return
        }
        let said = ''
        try {
          said = JSON.parse(text).message || ''
        } catch { /* a plain-text error */ }
        reject(new KubeError(`Kubernetes answered ${res.statusCode}${said ? `: ${said.slice(0, 240)}` : ''}`, res.statusCode))
      })
      res.on('error', reject)
    })
    req.on('timeout', () => req.destroy(new KubeError(`Kubernetes did not answer within ${TIMEOUT_MS} ms`)))
    req.on('error', (e) => reject(e instanceof KubeError ? e : new KubeError(`Kubernetes could not be reached: ${e.message}`)))
    req.end()
  })
}

/** What a debugger needs from a pod: is it running, and how did it last die. */
export function podSummary(p) {
  const statuses = p.status?.containerStatuses || []
  const main = statuses.find((c) => c.name === p.spec?.containers?.[0]?.name) || statuses[0] || null
  const last = main?.lastState?.terminated || null
  const now = main?.state?.terminated || null
  const died = now || last
  return {
    name: p.metadata?.name,
    ip: p.status?.podIP ?? null,
    phase: p.status?.phase ?? null,
    ready: (p.status?.conditions || []).some((c) => c.type === 'Ready' && c.status === 'True'),
    host: p.spec?.nodeName ?? null,
    container: main?.name ?? p.spec?.containers?.[0]?.name ?? null,
    restarts: main?.restartCount ?? 0,
    waiting: main?.state?.waiting?.reason ?? null,
    startedAt: main?.state?.running?.startedAt ?? null,
    lastExit: died
      ? { code: died.exitCode, reason: died.reason ?? null, at: died.finishedAt ?? null, signal: died.signal ?? null }
      : null,
    deleting: Boolean(p.metadata?.deletionTimestamp),
  }
}

export function createKube({ namespace, selector, env = process.env, read = readFileSync, get = fetchText }) {
  const host = env.KUBERNETES_SERVICE_HOST
  const port = Number(env.KUBERNETES_SERVICE_PORT || 443)
  if (!host) return null

  const call = (path) => {
    // Read on every call: a projected service account token rotates.
    const token = String(read(`${SERVICE_ACCOUNT_DIR}/token`, 'utf8')).trim()
    const ca = read(`${SERVICE_ACCOUNT_DIR}/ca.crt`)
    return get({ host, port, ca, token, path })
  }
  const ns = segment(namespace, 'namespace')

  return {
    async pods() {
      const text = await call(withQuery(`/api/v1/namespaces/${ns}/pods`, { labelSelector: selector }))
      return (JSON.parse(text).items || []).map(podSummary).sort((a, b) => a.name.localeCompare(b.name))
    },

    logs(pod, { container, previous = false, tailLines = 200, sinceSeconds } = {}) {
      return call(withQuery(`/api/v1/namespaces/${ns}/pods/${segment(pod, 'pod')}/log`, {
        container,
        previous: previous ? 'true' : undefined,
        tailLines,
        sinceSeconds,
        timestamps: 'true',
        limitBytes: 2 * 1024 * 1024,
      }))
    },

    async events(pod) {
      const text = await call(withQuery(`/api/v1/namespaces/${ns}/events`, {
        fieldSelector: `involvedObject.name=${pod},involvedObject.kind=Pod`,
        limit: 50,
      }))
      return (JSON.parse(text).items || [])
        .map((e) => ({
          at: e.lastTimestamp || e.eventTime || e.metadata?.creationTimestamp || null,
          type: e.type ?? null,
          reason: e.reason ?? null,
          message: String(e.message ?? '').slice(0, 300),
          count: e.count ?? 1,
        }))
        .sort((a, b) => String(b.at).localeCompare(String(a.at)))
    },
  }
}
