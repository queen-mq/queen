import { randomBytes } from 'node:crypto'
import { hostname } from 'node:os'
import * as logger from '../utils/logger.js'

// One opt-in reporter per consume invocation. Handler calls are not ACKs.
export class Supervision {
  constructor(http, config, options) {
    if (!config || typeof config !== 'object' || typeof config.group !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$/.test(config.group) || config.group === 'coordination' || /[\r\n]/.test(config.group)) throw new Error('supervision.group must be a valid application/deployment name')
    if (!Number.isInteger(options.concurrency) || options.concurrency < 1 || options.concurrency > 4096) throw new Error('supervision requires concurrency between 1 and 4096')
    this.http = http
    this.options = options
    this.group = config.group
    this.id = randomBytes(16).toString('hex')
    this.started = Date.now()
    this.monotonic = performance.now()
    this.running = this.completed = this.failed = this.sequence = 0
    this.last = this.pending = this.timer = null
    this.active = new Map()
    this.state = 'running'
    this.warned = false
  }
  wrap(handler) {
    return async (...args) => {
      const id = this.sequence++
      this.active.set(id, performance.now())
      try { const result = await handler(...args); this.completed++; return result }
      catch (error) { this.failed++; throw error }
      finally { this.active.delete(id); this.last = Math.floor(Date.now() / 1000) }
    }
  }
  start() {
    this.publish()
    this.timer = setInterval(() => this.publish(), 10_000)
    this.timer.unref()
  }
  document() {
    const now = performance.now()
    return {
      schema: 'queen.consumer.status/v1', instance_id: this.id, engine: 'js', execution_model: 'async-tasks',
      hostname: hostname(), pid: process.pid, state: this.state, updated_at_epoch: Math.floor(Date.now() / 1000),
      started_at_epoch: Math.floor(this.started / 1000), uptime_seconds: Math.floor((now - this.monotonic) / 1000),
      configuration: { heartbeat_timeout: 30 },
      pool_status: [{ name: 'consumer', queue: this.options.queue || null, namespace: this.options.namespace || null,
        task: this.options.task || null, consumer_group: this.options.group || '__QUEUE_MODE__',
        desired: this.options.concurrency, running: this.running, busy: this.active.size,
        completed: this.completed, failed: this.failed, last_completed_at_epoch: this.last,
        oldest_inflight_seconds: this.active.size ? Math.floor((now - Math.min(...this.active.values())) / 1000) : null }],
    }
  }
  publish() {
    if (this.pending) return this.pending
    this.pending = this.send().finally(() => { this.pending = null })
    return this.pending
  }
  async send() {
    try {
      const bytes = Buffer.from(JSON.stringify(this.document()))
      if (bytes.length > 45_000) throw new Error('Consumer status exceeds one chunk')
      const write = randomBytes(16).toString('hex'), slot = `${this.group}/${this.id}`
      const ops = [
        { op: 'put', ns: 'queen-supervisor', key: `${slot}/head`, value: { format: 'queen.supervisor.remote-status/v1', write, chunks: 1, bytes: bytes.length }, ttlSeconds: 60 },
        { op: 'put', ns: 'queen-supervisor', key: `${slot}/chunk/0000`, value: { write, index: 0, data: bytes.toString('base64') }, ttlSeconds: 60 },
      ]
      const result = await this.http.post('/api/v1/kv', { operations: ops }, 2000, null, null, AbortSignal.timeout(2000))
      if (!Array.isArray(result?.results) || result.results.length !== 2 || result.results.some(r => r.applied !== true)) throw new Error('Consumer status publication was not applied')
      this.warned = false
    } catch {
      if (!this.warned) logger.warn('Consumer.supervision', 'Status publication failed; consumption continues')
      this.warned = true
    }
  }
  async stop() {
    clearInterval(this.timer)
    if (this.pending) await this.pending
    this.state = 'stopped'
    await this.publish()
  }
}
