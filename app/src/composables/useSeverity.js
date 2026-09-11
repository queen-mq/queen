// Severity — the one place that decides whether a number is worth a colour.
//
// THE POLICY, in one sentence: amber means "attention", red means "failing or
// broken", and a healthy system shows neither. Everything else — scope,
// identity, emphasis, "this is information" — is someone else's colour, and a
// count with no verdict attached is plain ink.
//
// WHY THIS FILE EXISTS. The rules used to live as ternaries inside the views,
// one copy per surface, and almost all of them tested a raw count against a
// constant: `ackFailed > 0 → amber`, `deadLetter > 0 → amber`,
// `pending >= 1000 → amber`. On a cell doing ~14 msg/s that paints a perfectly
// healthy hour amber — 90 failed acks against ~50 000 acks is 0.18% of the
// work, i.e. nothing — and a dashboard that cries wolf on a good day cannot be
// read on a bad one. A raw count is not a verdict; a RATE is. So every rule
// below is either
//
//   * a ratio against the work that was actually done in the window, or
//   * a measure that is already proportional by construction (an age in
//     seconds, an event-loop lag in ms, a pool's utilisation), or
//   * a fallback used only when the denominator is missing, and marked as one.
//
// The thresholds are collected in THRESHOLDS so the test file can pin the
// numbers rather than re-typing them, and so a future argument about "is 1%
// the right line?" happens in one place and moves every surface at once.
//
// Pure functions, no imports: the views consume them and `app/test` exercises
// them without a DOM, exactly as useConflation / useWorkload already do.

/** Tone vocabulary. '' = no verdict, so the value keeps its default ink. */
export const SEV_NONE = ''
export const SEV_MUTE = 'mute'
export const SEV_OK = 'ok'
export const SEV_WARN = 'warn'
export const SEV_BAD = 'bad'

export const THRESHOLDS = Object.freeze({
  // --- Ack failures -------------------------------------------------------
  // An ack that fails is usually a lease that expired under a slow handler:
  // normal at low rates, a real problem as a share of the work. The window's
  // ack ATTEMPTS (success + failed) are the denominator because that is the
  // population the failures are drawn from.
  ackWarnRate: 0.01,        // 1% of acks failing is worth a look
  ackBadRate: 0.05,         // 5% is a handler or a lease budget that is broken
  // …but red also has to mean "at a scale that matters". 90 failures out of
  // 900 acks is 10% and still only 90 messages: that is attention, not an
  // incident, so red additionally requires real volume.
  ackBadMinFailed: 500,
  // Under this many FAILURES a ratio is noise rather than a rate — 3 failures
  // out of 40 acks is not a 7.5% failure rate, it is three failures — so
  // nothing below it is painted at all. The floor is on the numerator, not on
  // the window: 64 failures out of 160 acks is a thin window but a very solid
  // 40%, and refusing to call that one would be the opposite mistake.
  ackRateMinFailed: 10,
  ackAbsWarn: 100,          // fallback only: no usable denominator
  ackAbsBad: 1000,          // fallback only

  // --- Dead letters -------------------------------------------------------
  // A DLQ that merely EXISTS is history, not news: nobody purges it, so its
  // depth is monotonic and an amber tied to `> 0` never goes out again. What
  // deserves attention is a DLQ that GREW in the window being looked at.
  dlqGrowthRate: 0.01,      // ≥1% of the window's acks newly dead-lettered
  dlqGrowthAbs: 100,        // fallback only: no usable denominator
  dlqGrowthFloor: 10,       // below this, one bad message is not a trend

  // --- Backlog ------------------------------------------------------------
  // A pending count has no verdict on its own: 50 000 pending drains in 4
  // seconds at 12k/s and never drains at 0/s. Measured in SECONDS OF WORK at
  // the observed drain rate, it does.
  backlogWarnSeconds: 300,      // 5 minutes of work waiting
  backlogBadSeconds: 1800,      // half an hour
  backlogFloor: 100,            // messages; below this, never paint

  // --- Push vs ack drift over the window ----------------------------------
  // Expressed as a share of what was pushed: "we did not ack 10% of what
  // arrived" is a verdict, "+1 000 messages" is a number.
  driftWarnShare: 0.1,
  driftBadShare: 0.5,
  driftFloor: 100,              // messages; below this the drift is noise

  // --- Message age (already proportional: it is a duration) ---------------
  lagOkSeconds: 60,             // under a minute old = fresh
  lagWarnSeconds: 60,
  lagBadSeconds: 300,

  // --- Queue-level average lag, in ms -------------------------------------
  lagMsFresh: 1000,
  lagMsWarn: 10_000,
  lagMsBad: 60_000,

  // --- Broker health (host measures, proportional by construction) --------
  eventLoopWarnMs: 50,
  eventLoopBadMs: 100,
  poolWarnUtil: 0.8,
  poolBadUtil: 1,

  // --- Is the queue keeping up? -------------------------------------------
  // pop ÷ push over the same window. The old rule called 0.84 "elevated",
  // which is ordinary sampling jitter on a queue that is perfectly level.
  keepUpOk: 0.98,
  keepUpFine: 0.8,
  keepUpWarn: 0.5,              // below this it is falling behind for real
  keepUpMinRate: 1,             // msgs/s; under this there is nothing to judge

  // --- Loss (ephemeral rings, eviction) -----------------------------------
  lossWarnRate: 0.01,           // ≥1% of what the group read was dropped first
  lossFloor: 1,
})

const T = THRESHOLDS

/** Finite number or null — every input here arrives from an API that is
 *  allowed to omit a field, and `undefined > 0` must never decide a colour. */
const n = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

/**
 * Strip a non-alarming tone. A `.num` class of 'ok' or 'mute' would repaint a
 * healthy figure; on the metric rows and counts strips the healthy state is
 * simply default ink, so those tones collapse to ''.
 */
export const numTone = (sev) => (sev === SEV_WARN || sev === SEV_BAD ? sev : SEV_NONE)

/**
 * Ack failures as a share of the acks attempted in the same window.
 *
 * @param {object} o
 * @param {number|null} o.failed    ack failures in the window
 * @param {number|null} [o.succeeded] successful acks in the window
 * @param {number|null} [o.attempts]  failed + succeeded, when the caller has it
 * @returns {''|'warn'|'bad'}
 */
export function ackFailureSeverity({ failed, succeeded = null, attempts = null } = {}) {
  const f = n(failed) || 0
  if (f <= 0) return SEV_NONE

  const s = n(succeeded)
  const total = n(attempts) !== null ? n(attempts) : (s === null ? null : s + f)

  // No denominator (the window reported failures but no ack total): fall back
  // to absolutes, and keep them high enough that a working system stays quiet.
  if (total === null || total <= 0) {
    if (f >= T.ackAbsBad) return SEV_BAD
    if (f >= T.ackAbsWarn) return SEV_WARN
    return SEV_NONE
  }

  if (f < T.ackRateMinFailed) return SEV_NONE

  const rate = f / total
  if (rate >= T.ackBadRate && f >= T.ackBadMinFailed) return SEV_BAD
  if (rate >= T.ackWarnRate) return SEV_WARN
  return SEV_NONE
}

/**
 * Dead letters ADDED in the window. Never red: a dead letter is a message the
 * system has already given up on and parked somewhere safe, which is a state
 * to work through, not an outage. Depth alone returns ''.
 *
 * @param {object} o
 * @param {number|null} o.added    new dead letters in the window (null = unknown)
 * @param {number|null} [o.attempts] ack attempts in the window, as denominator
 * @returns {''|'warn'}
 */
export function dlqGrowthSeverity({ added, attempts = null } = {}) {
  const a = n(added)
  if (a === null || a < T.dlqGrowthFloor) return SEV_NONE
  const total = n(attempts)
  if (total === null || total <= 0) return a >= T.dlqGrowthAbs ? SEV_WARN : SEV_NONE
  return a / total >= T.dlqGrowthRate ? SEV_WARN : SEV_NONE
}

/**
 * Backlog expressed as seconds of work at the observed drain rate.
 *
 * @param {object} o
 * @param {number|null} o.pending       messages waiting
 * @param {number|null} o.drainPerSec   acks (or pops) per second, right now
 * @returns {''|'warn'|'bad'}
 */
export function backlogSeverity({ pending, drainPerSec } = {}) {
  const p = n(pending)
  const rate = n(drainPerSec)
  // Nothing to judge: no depth, no measured drain, or a drain of zero (which
  // is the Time lag row's story to tell — an age, not a count).
  if (p === null || p < T.backlogFloor) return SEV_NONE
  if (rate === null || rate <= 0) return SEV_NONE
  const seconds = p / rate
  if (seconds >= T.backlogBadSeconds) return SEV_BAD
  if (seconds >= T.backlogWarnSeconds) return SEV_WARN
  return SEV_NONE
}

/**
 * Cumulative (pushed − acked) across the window, judged as a share of what was
 * pushed. Negative = the tenant is catching up, which is a positive state and
 * keeps its green.
 *
 * @param {object} o
 * @param {number|null} o.delta   pushed − acked
 * @param {number|null} o.pushed  pushed in the same window (the denominator)
 * @returns {''|'ok'|'warn'|'bad'}
 */
export function pendingDriftSeverity({ delta, pushed } = {}) {
  const d = n(delta)
  if (d === null) return SEV_NONE
  if (Math.abs(d) < T.driftFloor) return SEV_NONE
  if (d < 0) return SEV_OK
  const p = n(pushed)
  if (p === null || p <= 0) return SEV_NONE
  const share = d / p
  if (share >= T.driftBadShare) return SEV_BAD
  if (share >= T.driftWarnShare) return SEV_WARN
  return SEV_NONE
}

/**
 * Age of the oldest un-consumed message, in seconds. A duration is already
 * proportional — "five minutes late" means the same thing at 14 msg/s and at
 * 140 000 — so this one keeps the thresholds it always had.
 *
 * @returns {'mute'|'ok'|'warn'|'bad'}
 */
export function timeLagSeverity(seconds) {
  const s = n(seconds)
  if (s === null || s <= 0) return SEV_MUTE
  if (s < T.lagOkSeconds) return SEV_OK
  if (s < T.lagBadSeconds) return SEV_WARN
  return SEV_BAD
}

/** The same measure in milliseconds, as the queue grids report it. */
export function lagMsSeverity(ms) {
  const v = n(ms)
  if (v === null || v <= 0) return SEV_MUTE
  if (v < T.lagMsFresh) return SEV_OK
  if (v < T.lagMsWarn) return SEV_MUTE
  if (v < T.lagMsBad) return SEV_WARN
  return SEV_BAD
}

/** Node's event loop lag on a broker worker. Degradation of the host itself. */
export function eventLoopSeverity(ms) {
  const v = n(ms)
  if (v === null || v <= 0) return SEV_NONE
  if (v >= T.eventLoopBadMs) return SEV_BAD
  if (v >= T.eventLoopWarnMs) return SEV_WARN
  return SEV_NONE
}

/** Postgres pool saturation: a full pool means requests are queueing. */
export function poolSeverity({ active, size } = {}) {
  const a = n(active)
  const s = n(size)
  if (a === null || s === null || s <= 0) return SEV_NONE
  const util = a / s
  if (util >= T.poolBadUtil) return SEV_BAD
  if (util > T.poolWarnUtil) return SEV_WARN
  return SEV_NONE
}

/**
 * Is the queue keeping up? pop ÷ push, with a floor under the push rate: at
 * 0.2 msgs/s a single bucket boundary swings the ratio from 2.0 to 0.5 and
 * the colour would be reporting arithmetic, not health.
 *
 * @returns {'mute'|'ok'|'warn'|'bad'}
 */
export function keepUpSeverity({ pop, push } = {}) {
  const o = n(pop) || 0
  const i = n(push) || 0
  if (i < T.keepUpMinRate && o < T.keepUpMinRate) return SEV_MUTE
  if (i <= 0) return o > 0 ? SEV_OK : SEV_MUTE
  const ratio = o / i
  if (ratio >= T.keepUpOk) return SEV_OK
  if (ratio >= T.keepUpFine) return SEV_MUTE
  if (ratio >= T.keepUpWarn) return SEV_WARN
  return SEV_BAD
}

/**
 * Messages dropped before a reader reached them (an ephemeral ring evicting by
 * ttl or by bounds). Loss is by policy here, so it is attention, never red —
 * and only once it is a real share of what that reader actually got.
 *
 * @returns {''|'warn'}
 */
export function lossSeverity({ dropped, delivered } = {}) {
  const d = n(dropped)
  if (d === null || d < T.lossFloor) return SEV_NONE
  const got = n(delivered)
  if (got === null) return SEV_WARN     // no denominator: loss is still loss
  const total = got + d
  if (total <= 0) return SEV_NONE
  return d / total >= T.lossWarnRate ? SEV_WARN : SEV_NONE
}

/**
 * Consumer-group state. `state` is the broker's own verdict and is kept: a
 * group the broker calls Lagging IS behind. The lag in seconds escalates it.
 * `partitionsWithLag > 0` on its own does NOT: on a busy queue a partition is
 * momentarily behind all the time, and that is what "working" looks like.
 *
 * @returns {'mute'|'ok'|'warn'|'bad'}
 */
export function consumerGroupSeverity({ state, maxTimeLag } = {}) {
  const lag = n(maxTimeLag) || 0
  if (state === 'Dead') return SEV_MUTE
  if (lag >= T.lagBadSeconds) return SEV_BAD
  if (state === 'Lagging' || lag >= T.lagWarnSeconds) return SEV_WARN
  return SEV_OK
}

/**
 * Partitions of a group that are behind, as a share of the partitions it holds.
 * A count on its own says nothing: 3 of 512 is noise, 3 of 4 is a stall.
 *
 * @returns {'mute'|'warn'|'bad'}
 */
export function laggingPartitionsSeverity({ behind, total } = {}) {
  const b = n(behind)
  if (b === null || b <= 0) return SEV_MUTE
  const all = n(total)
  if (all === null || all <= 0) return SEV_MUTE
  const share = b / all
  if (share >= 0.75) return SEV_BAD
  if (share >= 0.25) return SEV_WARN
  return SEV_MUTE
}
