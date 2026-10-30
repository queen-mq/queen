// Entity → colour slot, kept for the session.
//
// Colour follows the entity, never its rank: a namespace keeps the slot it was
// first given whatever the sort order, the metric or a filter does to the rows
// around it, so smartchat is the same blue on the stacked area and the
// activity map, and is still that blue after a refresh reorders the top four.
// Slots are per KIND (namespace, task, the queues of one namespace):
// switching the group-by starts from the first colour again, and the queues
// of each namespace have a map of their own, because they share nothing with
// the queues of another — coming back to a namespace finds its queues in
// their old colours.
//
// Five slots (useChartTheme categoryPalette). Past them the answer is null and
// the caller paints grey and folds the entity into "Other".
export const CATEGORY_SLOTS = 5

const slots = new Map() // kind -> Map(key -> slot index)

/** The slot for `key` of `kind`, assigned on first sight; null past the palette. */
export function categorySlot(kind, key) {
  let m = slots.get(kind)
  if (!m) { m = new Map(); slots.set(kind, m) }
  if (!m.has(key)) m.set(key, m.size)
  const s = m.get(key)
  return s < CATEGORY_SLOTS ? s : null
}

/** The slot `key` of `kind` already holds, without giving it one; null if none. */
export function heldSlot(kind, key) {
  const s = slots.get(kind)?.get(key)
  return s !== undefined && s < CATEGORY_SLOTS ? s : null
}

/**
 * One view's entities → their slots. `ranked` is the view's rows, busiest
 * first, as `{ key, active }`. An active entity is given a slot on first
 * sight, so on a first load the five busiest take the five hues in order; an
 * idle one keeps the slot it holds, or has none: a namespace with nothing in
 * this window does not take a colour from one that may be busy in the next.
 *
 * @param {string} kind
 * @param {Array<{key: string, active: boolean}>} ranked
 * @returns {Map<string, number|null>}
 */
export function entitySlots(kind, ranked) {
  const out = new Map()
  for (const { key, active } of ranked) out.set(key, active ? categorySlot(kind, key) : heldSlot(kind, key))
  return out
}

/**
 * The entities a chart draws in their own colour, in SLOT order, and the
 * rest, which fold into one grey "Other". Slot order keeps a stack's
 * neighbours the palette's neighbours: those are the only pairs it passes.
 *
 * @template T
 * @param {T[]} items
 * @param {(item: T) => number|null|undefined} slotOf
 * @returns {{hued: T[], rest: T[]}}
 */
export function splitBySlot(items, slotOf) {
  const hued = []
  const rest = []
  for (const it of items) ((slotOf(it) ?? null) === null ? rest : hued).push(it)
  hued.sort((a, b) => slotOf(a) - slotOf(b))
  return { hued, rest }
}

/**
 * Forget every assignment of one kind, or of every kind when none is given
 * (another cluster: its namespaces are not this one's).
 */
export function resetCategorySlots(kind) {
  if (kind === undefined) slots.clear()
  else slots.delete(kind)
}
