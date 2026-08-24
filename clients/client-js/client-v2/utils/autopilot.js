/**
 * Pop autopilot, client side.
 *
 * The broker owns a controller that sizes a pop from state this client cannot
 * see: how many partitions of the (queue, group) are ready, how old their
 * oldest ready message is, at what rate messages are arriving. Two knobs are
 * under its control — `partitions` (the sweep width) and `batch` (the message
 * budget for the sweep).
 *
 * THE RULE, and it is the only one: an explicit user value is sacred. Autopilot
 * applies ONLY to the knobs the user left unset, and it applies to them one by
 * one. A consumer that pins `partitions(1)` and says nothing about batch keeps
 * its single-partition claim forever and lets the broker size the batch; the
 * pinned dimension is never "adjusted", not even towards a value the controller
 * would consider better.
 *
 * The wire shape follows the conflation precedent (see conflation.js): a client
 * that is not engaging autopilot sends the byte-identical request it sent
 * before this feature existed.
 *
 *   autopilot=true      emitted ONLY when at least one of the two knobs is
 *                       being left to the broker. Never as autopilot=false.
 *   partitions / batch  OMITTED for the dimensions the broker is choosing,
 *                       sent exactly as before for the ones the user set.
 *
 * WHAT AN OLD BROKER DOES, and why there is no capability check here. A broker
 * older than 1.2 ignores unknown query params: the request succeeds, and the
 * two omitted knobs fall back to the SERVER-side defaults (batch 200,
 * partitions 1) instead of the old client-side ones. That is a sizing
 * difference, not a correctness one — nothing is lost, misordered or delivered
 * twice — so unlike conflation (which silently hands a last-value consumer a
 * whole backlog, hence CONFLATION_UNSUPPORTED) this degrades quietly and on
 * purpose. Callers who need the old numbers against an old broker set them
 * explicitly, or turn autopilot off.
 */

/**
 * The environment variable that disables pop autopilot for a whole process:
 * QUEEN_SDK_POP_AUTOPILOT=off restores the client-side defaults this SDK
 * applied before autopilot existed, byte for byte. It is read once, in the
 * Queen constructor, so a single deployment can be rolled back without touching
 * code.
 *
 * "off", "false", "0", "no" and "disabled" all disable it (case-insensitive,
 * surrounding space ignored). Every other value, including the empty one,
 * leaves autopilot on.
 */
export const ENV_POP_AUTOPILOT = 'QUEEN_SDK_POP_AUTOPILOT'

const DISABLING_VALUES = new Set(['off', 'false', '0', 'no', 'disabled'])

/** Whether ENV_POP_AUTOPILOT asks for the pre-autopilot behavior. */
export function popAutopilotDisabledByEnv() {
  // `process` is absent in a browser bundle; there the variable cannot be set
  // and autopilot is simply on.
  const raw = typeof process !== 'undefined' && process.env ? process.env[ENV_POP_AUTOPILOT] : undefined
  return DISABLING_VALUES.has(String(raw ?? '').trim().toLowerCase())
}

/**
 * The batch/partitions/autopilot decision for one pop — the values that travel
 * and the ones that do not.
 *
 * IT EXISTS SO THERE IS EXACTLY ONE COPY OF THE EMISSION RULE. pop() and
 * consume() build their query strings separately (QueueBuilder.pop's inline
 * params vs ConsumerManager#buildParams) and the two have drifted before — the
 * standing comment in QueueBuilder.js is there because of it. A rule with three
 * branches and a per-dimension carve-out is precisely the kind that gets copied
 * wrong, so both builders call this and then only PLACE what it returns; where
 * each key sits in the query string stays with the builder, because the
 * pre-autopilot key order is part of what "byte-identical" means here.
 *
 * @param {object} opts
 * @param {number|null} opts.batch - the USER's batch. null/0/undefined means
 *   unset: the dimension the broker gets to choose. Neither builder may
 *   substitute a default before calling this.
 * @param {number|null} opts.maxPartitions - the USER's sweep width, same
 *   convention.
 * @param {number} opts.fallbackBatch - client-side default applied to an unset
 *   batch when autopilot is NOT engaged.
 * @param {boolean} opts.autopilot - the resolved decision for this call.
 * @returns {{autopilot: boolean, batch: string|null, partitions: string|null}}
 *   Strings are ready to append; null means "this key does not travel".
 */
export function popSizing({ batch, maxPartitions, fallbackBatch, autopilot }) {
  const batchSet = typeof batch === 'number' && batch > 0
  const partitionsSet = typeof maxPartitions === 'number' && maxPartitions > 0

  // Note the case that looks like an omission and is not: when the user set
  // BOTH knobs there is nothing left for the controller to decide, so
  // autopilot=true is NOT emitted and the request is byte-identical to the one
  // this SDK sent before autopilot existed. Sending the flag anyway would be
  // harmless on the broker and dishonest in a packet capture.
  if (autopilot && !(batchSet && partitionsSet)) {
    return {
      autopilot: true,
      batch: batchSet ? String(batch) : null,
      partitions: partitionsSet ? String(maxPartitions) : null
    }
  }

  return {
    autopilot: false,
    batch: String(batchSet ? batch : fallbackBatch),
    // The legacy gate: partitions travels only above 1, because 1 IS the
    // server-side default and a v4-era client never sent it.
    partitions: partitionsSet && maxPartitions > 1 ? String(maxPartitions) : null
  }
}

/**
 * What the broker chose for one pop, echoed back in the response under
 * "autopilot" when the request engaged autopilot. It is additive: a broker that
 * does not send it, or a pop that never asked, yields null everywhere it is
 * exposed.
 *
 * Reading it is optional — the messages are already sized by it — but it is the
 * only way to see the controller working from the client side, and the only
 * input to the empty-poll pacing below.
 *
 * Unknown keys inside it are ignored, and an unknown-shaped value is treated as
 * absent rather than as an error: this field is the broker telling the client
 * what it did, and a client that refuses to run because a newer broker grew a
 * fourth number would be a self-inflicted outage.
 *
 * @param {object|null} result - parsed pop response (null for a 204)
 * @returns {{partitions: number, batch: number, waitMillis: number}|null}
 */
export function parseAutopilotDecision(result) {
  const raw = result && typeof result === 'object' ? result.autopilot : null
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null

  const num = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : 0)
  return {
    /** Sweep width the broker used for this pop. */
    partitions: num(raw.partitions),
    /** Message budget the broker used for this pop. */
    batch: num(raw.batch),
    /**
     * The broker's advice on how long to wait before polling again (wire name:
     * waitMs). Present only when the broker has an opinion, and it is advice,
     * not a lease: the consume loop honors it for the sleep it was already
     * taking between empty non-waiting pops, nothing more. 0 = no advice.
     */
    waitMillis: num(raw.waitMs)
  }
}

/**
 * The sleep the consume loop has always taken between two empty pops that are
 * NOT long-polling. A waiting pop already blocks on the broker, so it never
 * reaches here.
 */
export const EMPTY_POLL_BACKOFF_MILLIS = 100

/**
 * How long to wait after an empty pop: the broker's advice when it gave one,
 * the historical constant otherwise.
 *
 * The advice is honored as given, without a ceiling of this client's invention.
 * The sleep it feeds is raced against the caller's abort signal by the loop, so
 * even an absurd value cannot outlive a cancellation — which is the only
 * property that has to hold locally.
 */
export function emptyPollDelayMillis(decision) {
  if (decision && decision.waitMillis > 0) return decision.waitMillis
  return EMPTY_POLL_BACKOFF_MILLIS
}
