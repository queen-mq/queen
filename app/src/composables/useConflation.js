// Conflation — the console's rules for a last-value consumer group, in one
// place (PLAN_CONFLATION §5.4).
//
// `conflation` is a boolean the broker stores on the consumer group FOR A QUEUE
// (queen.consumer_groups_metadata), surfaced on /api/v1/consumer-groups since
// §2.6. A conflating group is delivered exactly one message per partition — the
// newest — so the numbers the console has always rendered stop meaning what
// their labels say:
//
//   * pending / offset_lag / totalLag  = LOG depth: positions still to retire.
//   * partitionsWithLag                = WORK depth: partitions whose handler
//                                        still has to run — the figure an
//                                        operator sizes consumers against.
//
// A conflating group at 4 000 000 pending across 12 partitions behind is
// healthy; the same two numbers on a non-conflating group are an incident. Every
// surface that renders one of them has to say which of the two it is, and three
// copies of that rule is three chances to disagree — hence this module.
//
// Pure functions, no imports: the views consume them and `app/test` exercises
// them directly under `node --test`.

/**
 * True only for an explicit server-side `true`.
 *
 * A missing field is NOT a yes: an older broker (or a group listing that never
 * carried the field) must read as "not conflating", so nothing is relabelled on
 * a guess.
 */
export function isConflating(group) {
  return group?.conflation === true
}

/**
 * Identity of a consumer group in the console: the group name AND the queue,
 * because `conflation` is declared per (group, queue) — one group can conflate
 * on one queue and not on another.
 *
 * NUL separator, not '@': group and queue names are arbitrary caller text, and
 * `a@b` on `c` must not collide with `a` on `b@c`.
 */
export function groupKey(name, queueName) {
  return `${name ?? ''}\u0000${queueName ?? ''}`
}

/**
 * The `groupKey`s of every conflating group in a /consumer-groups listing.
 *
 * Lets a payload that carries no conflation field of its own — the lagging
 * PARTITION rows, which are keyed `consumer_group` / `queue_name` — be labelled
 * from the group listing the page already holds, with no extra request.
 */
export function conflatingKeys(groups) {
  const keys = new Set()
  for (const g of groups || []) {
    if (isConflating(g)) keys.add(groupKey(g.name, g.queueName))
  }
  return keys
}

/** True when a lagging-partition row belongs to one of those groups. */
export function partitionConflates(row, keys) {
  if (!row || !keys || keys.size === 0) return false
  return keys.has(groupKey(row.consumer_group, row.queue_name))
}

/**
 * Is this group behind at all? The inclusive rule the Consumers page already
 * uses for its "Lagging only" filter — deliberately the broadest of the three
 * lag rules in the app, because it decides whether an advisory is allowed to go
 * quiet (§5.4) and a narrow rule there would silence a real backlog.
 */
export function isGroupLagging(group) {
  if (!group) return false
  if (group.state === 'Lagging') return true
  return (group.maxTimeLag || 0) > 0 || (group.partitionsWithLag || 0) > 0
}

/**
 * What the lagging groups on ONE queue say about its pending count.
 *
 *   'unknown'     — no answer yet (never asked, or still in flight)
 *   'unavailable' — the lookup failed; unknown is not healthy, so this reads
 *                   the same as 'mixed' at every call site
 *   'none'        — nothing is behind: whatever is pending, conflation is not
 *                   the reason, so nothing may be explained away by it
 *   'conflating'  — every group that is behind conflates: pending is log depth
 *   'mixed'       — at least one group that is behind does NOT conflate
 *
 * @param {Array|null} groups Group rows for this queue, or null when unknown.
 */
export function laggingGroupsVerdict(groups, { failed = false } = {}) {
  if (failed) return 'unavailable'
  if (!Array.isArray(groups)) return 'unknown'
  const lagging = groups.filter(isGroupLagging)
  if (lagging.length === 0) return 'none'
  return lagging.every(isConflating) ? 'conflating' : 'mixed'
}

/** Pending count above which QueueDetail speaks up about depth at all. */
export const PENDING_DEPTH_FLOOR = 10000

/**
 * Which pending-depth advisory QueueDetail should render, if any.
 *
 *   'none'      — below the floor; the page stays quiet, as it always has
 *   'hold'      — above the floor, but the conflation answer has not landed
 *                 yet. Say nothing FOR NOW rather than flash a warning that is
 *                 retracted a round trip later; the pending count itself is on
 *                 screen in the counts strip the whole time
 *   'log-depth' — every lagging group conflates: state the fact, do not warn
 *   'fire'      — the original advisory, unchanged. This is also the answer
 *                 when the lookup failed and when nothing is lagging: an
 *                 unknown must never buy silence
 */
export function pendingDepthAdvisory(pending, verdict, floor = PENDING_DEPTH_FLOOR) {
  if (!(Number(pending) > floor)) return 'none'
  if (verdict === 'conflating') return 'log-depth'
  if (verdict === 'unknown') return 'hold'
  return 'fire'
}
