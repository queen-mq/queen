/**
 * Tiny logger. Off by default; enable via QUEEN_STREAMS_LOG=1 or by passing
 * a `logger` option to Stream.run.
 */

const enabled = (() => {
  const v = process.env.QUEEN_STREAMS_LOG
  return v === '1' || v === 'true' || v === 'yes'
})()

export function makeLogger(custom) {
  if (custom && typeof custom.info === 'function') return custom
  return {
    info: (msg, ctx) => {
      if (enabled) {
        if (ctx) console.log(`[queen-streams] ${msg}`, ctx)
        else console.log(`[queen-streams] ${msg}`)
      }
    },
    warn: (msg, ctx) => {
      if (ctx) console.warn(`[queen-streams] WARN ${msg}`, ctx)
      else console.warn(`[queen-streams] WARN ${msg}`)
    },
    error: (msg, ctx) => {
      if (ctx) console.error(`[queen-streams] ERROR ${msg}`, ctx)
      else console.error(`[queen-streams] ERROR ${msg}`)
    }
  }
}
