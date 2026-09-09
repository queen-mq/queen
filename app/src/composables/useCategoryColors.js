// Entity → colour slot, kept for the session.
//
// Colour follows the entity, never its rank: a namespace keeps the slot it was
// first given whatever the sort order, the metric or a filter does to the rows
// around it, so smartchat is the same blue on the stacked area, the share bars
// and the activity map, and is still that blue after a refresh reorders the
// top four. Slots are per KIND (namespace, task, queue): switching the
// group-by starts from the first colour again, and the queue map is reset on
// every drill-down, because the queues of one namespace share nothing with the
// queues of another.
//
// Five slots (useChartTheme categoryPalette). Past them the answer is null and
// the caller paints grey; the flow chart folds that tail into "Other" anyway.
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

/** Forget every assignment of one kind (a drill-down changes the queue set). */
export function resetCategorySlots(kind) { slots.delete(kind) }
