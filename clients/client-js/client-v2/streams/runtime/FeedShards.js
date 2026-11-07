/**
 * FeedShardGroup — `run({ queryId, shards: N })`: the N shards of one feed
 * stream in this process, one FeedRunner each (see FeedRunner, Shards).
 *
 * Each shard has its own lock: the same code started in several processes
 * runs each shard in the first process that takes its lock, and keeps it as
 * a standby in the others, which take over when that process stops or dies.
 * To place shards on purpose, run `shard: { index, count }` in each process
 * instead.
 *
 * The handle answers like one runner: stop(), done, metrics() (the sums, and
 * every shard's), and state(step).get(key) / list(limit) over the shards
 * this process runs.
 */
import { FeedRunner } from './FeedRunner.js'

export class FeedShardGroup {
  #runners = []
  #opts
  #count

  constructor({ shards, ...opts }) {
    if (!Number.isInteger(shards) || shards < 2 || shards > 4096) {
      throw new Error(`run({ shards }): shards is a whole number from 2 to 4096, got ${JSON.stringify(shards)}`)
    }
    if (opts.shard !== undefined) throw new Error('run(): give shards (all of them here) or shard (one), not both')
    this.#count = shards
    this.#opts = opts
  }

  get shards() {
    return this.#count
  }

  get runners() {
    return [...this.#runners]
  }

  async start() {
    for (let i = 0; i < this.#count; i++) {
      this.#runners.push(new FeedRunner({ ...this.#opts, shard: { index: i, count: this.#count } }))
    }
    // One at a time: the first shard writes the layout the others then find.
    try {
      for (const r of this.#runners) await r.start()
    } catch (err) {
      await Promise.all(this.#runners.map((r) => r.stop().catch(() => {})))
      throw err
    }
  }

  async stop() {
    await Promise.all(this.#runners.map((r) => r.stop()))
  }

  get done() {
    return Promise.all(this.#runners.map((r) => r.done))
  }

  metrics() {
    const shards = this.#runners.map((r) => r.metrics())
    const sum = (k) => shards.reduce((n, m) => n + (Number.isFinite(m[k]) ? m[k] : 0), 0)
    const out = { shards: this.#count, running: shards.filter((m) => m.role === 'active').length }
    for (const k of ['cycles', 'entriesRead', 'emitted', 'outputs', 'effects', 'batches', 'commits', 'conflicts', 'errors', 'retries',
      'poisoned', 'skipped', 'deadLettered', 'late', 'ticks', 'takeovers', 'snapshots', 'replays', 'replayEntries',
      'tableRows', 'pendingOutputs', 'pendingBatch', 'open', 'stateKeys']) {
      out[k] = sum(k)
    }
    out.perShard = shards
    return out
  }

  /** The live state of one step over the shards this process runs. */
  state(step) {
    return {
      get: (key) => {
        for (const r of this.#runners) {
          const v = r.state(step).get(key)
          if (v.found || v.error) return { ...v, shard: r.shard ? r.shard.index : 0 }
        }
        return { found: false }
      },
      list: (limit = 100) => {
        const items = []
        let truncated = false
        for (const r of this.#runners) {
          const part = r.state(step).list(limit)
          if (part.error) return part
          items.push(...part.items)
          truncated = truncated || part.truncated
        }
        return { items: items.slice(0, limit), truncated: truncated || items.length > limit }
      }
    }
  }
}
