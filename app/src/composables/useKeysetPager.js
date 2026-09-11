// Keyset paging, as a cursor STACK — the app's first pager that is not an
// OFFSET pager, and the difference is not a detail.
//
// Messages.vue pages with `offset = (page - 1) * pageSize`, so it can render
// "page 7" and jump. The KV and timer listings cannot: their stored procedures
// page by an EXCLUSIVE cursor on the key (`p_after`, under COLLATE "C"), which
// is what makes page 135 of 27 000 cost the same single index-range read as
// page 1. The price of that is that the only page a keyset reader can name is
// the next one — there is no total without a count, and no way to land on a
// page it has not walked to.
//
// So: no page numbers, no jumping, and PREVIOUS IS NOT A SUBTRACTION. Going
// back means re-asking with the cursor that produced the page before, which is
// the one this stack keeps. Push on next, pop on previous, clear whenever the
// query itself changes (a different queue, namespace or prefix is a different
// sequence and an old cursor would silently address it).
//
// Pure except for the refs — no HTTP, no route, no store. The caller owns the
// request and feeds back what each page said about its own end (`truncated`,
// `nextAfter`), because that verdict belongs to the SP, not to this counter:
// a page can end early on the byte budget with `truncated:true`, and the last
// page of a queue answers `truncated:false` with a null cursor.
import { computed, ref } from 'vue'

export function useKeysetPager() {
  // One cursor per page BEYOND the first; the top of the stack is the cursor
  // that produced what is on screen. Empty means "the first page", whose
  // cursor is null — the SP reads NULL as "start at the beginning".
  const cursors = ref([])
  // What the page on screen said about its own end. Cleared the moment a move
  // is made, so a second click before the new page lands cannot push the same
  // cursor twice.
  const tail = ref({ truncated: false, nextAfter: null })

  /** The cursor to send with the next request: null on the first page. */
  const current = () => (cursors.value.length ? cursors.value[cursors.value.length - 1] : null)

  const canPrev = computed(() => cursors.value.length > 0)
  const canNext = computed(() => tail.value.nextAfter !== null)
  /** How many pages have been walked, 1-based. NOT "page 3 of 47": there is no 47. */
  const page = computed(() => cursors.value.length + 1)

  /**
   * Record the page that just landed. `{truncated, nextAfter}` straight off
   * the response — a missing or malformed pair means "this is the end", which
   * is the safe direction: a Next that does nothing is worse than no Next.
   *
   * The cursor is kept ONLY under `truncated:true`, so the two fields cannot
   * disagree anywhere else: the SPs return a null `nextAfter` on the last page
   * and this is where that invariant is enforced rather than re-checked at
   * each use.
   */
  const received = (result) => {
    const truncated = result?.truncated === true
    const cursor = typeof result?.nextAfter === 'string' && result.nextAfter !== '' ? result.nextAfter : null
    tail.value = { truncated, nextAfter: truncated ? cursor : null }
  }

  /**
   * Advance. Returns false when there is nowhere to go — the page said
   * `truncated:false`, or carried no cursor — so the caller can skip the
   * refetch instead of re-requesting the page it is already showing.
   */
  const next = (nextAfter = tail.value.nextAfter) => {
    if (typeof nextAfter !== 'string' || nextAfter === '') return false
    cursors.value = [...cursors.value, nextAfter]
    tail.value = { truncated: false, nextAfter: null }
    return true
  }

  /** Back one page. False on the first page, where there is no cursor to pop. */
  const prev = () => {
    if (!canPrev.value) return false
    cursors.value = cursors.value.slice(0, -1)
    tail.value = { truncated: false, nextAfter: null }
    return true
  }

  /** A new query (queue, namespace, prefix, cluster): the old cursors address
   *  a sequence that no longer exists, so they go rather than travel. */
  const reset = () => {
    cursors.value = []
    tail.value = { truncated: false, nextAfter: null }
  }

  /**
   * The whole walk, for a caller that has to UNDO a move whose page never
   * landed.
   *
   * `next()` has to push before the request goes out — it is what produces the
   * cursor to send — so a failed page leaves the stack one deeper than what is
   * on screen and the footer counts a page nobody is looking at. Rather than
   * teach this counter about requests, the caller marks before the move and
   * restores when the answer does not arrive. Both fields go back, the tail
   * included: a Next that failed must leave Next exactly as usable as it was.
   *
   * Safe to hold: `cursors` and `tail` are REPLACED on every move, never
   * mutated, so the snapshot cannot drift under the caller.
   */
  const mark = () => ({ cursors: cursors.value, tail: tail.value })

  const restore = (snapshot) => {
    if (!snapshot || typeof snapshot !== 'object') return
    cursors.value = Array.isArray(snapshot.cursors) ? snapshot.cursors : []
    tail.value = snapshot.tail && typeof snapshot.tail === 'object'
      ? snapshot.tail
      : { truncated: false, nextAfter: null }
  }

  return { current, canPrev, canNext, page, received, next, prev, reset, mark, restore }
}
