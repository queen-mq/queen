import { test } from 'node:test'
import assert from 'node:assert/strict'

import { useKeysetPager } from '../src/composables/useKeysetPager.js'

// A page as the KV and timer SPs return it: rows plus the two fields that
// describe the page's own end (025_log_timers.sql log_timers_list_v1).
const page = (nextAfter) =>
  nextAfter === null ? { truncated: false, nextAfter: null } : { truncated: true, nextAfter }

test('the first page sends no cursor and cannot go back', () => {
  const p = useKeysetPager()
  assert.equal(p.current(), null)
  assert.equal(p.canPrev.value, false)
  assert.equal(p.canNext.value, false)
  assert.equal(p.page.value, 1)
  assert.equal(p.prev(), false)
})

test('next pushes the page cursor, prev pops back to the one before', () => {
  const p = useKeysetPager()

  p.received(page('wh.deliver:a'))
  assert.equal(p.canNext.value, true)
  assert.equal(p.next(), true)
  assert.equal(p.current(), 'wh.deliver:a')
  assert.equal(p.page.value, 2)
  assert.equal(p.canPrev.value, true)
  // Until the new page lands nothing is known about ITS end, so Next is dead:
  // a second click must not push the same cursor twice.
  assert.equal(p.canNext.value, false)
  assert.equal(p.next(), false)

  p.received(page('wh.deliver:b'))
  assert.equal(p.next(), true)
  assert.equal(p.current(), 'wh.deliver:b')
  assert.equal(p.page.value, 3)

  assert.equal(p.prev(), true)
  assert.equal(p.current(), 'wh.deliver:a')
  assert.equal(p.page.value, 2)
  assert.equal(p.prev(), true)
  assert.equal(p.current(), null)
  assert.equal(p.page.value, 1)
  assert.equal(p.prev(), false)
})

test('a page that is not truncated is the last one, whatever it carries', () => {
  const p = useKeysetPager()
  p.received(page('k1'))
  p.next()

  p.received(page(null))
  assert.equal(p.canNext.value, false)
  assert.equal(p.next(), false)
  assert.equal(p.current(), 'k1')
  assert.equal(p.page.value, 2)

  // truncated:false with a cursor anyway is a contradiction the SPs never
  // send; the pager believes `truncated` and stops.
  p.received({ truncated: false, nextAfter: 'k2' })
  assert.equal(p.canNext.value, false)
  assert.equal(p.next(), false)
  assert.equal(p.page.value, 2)
})

test('cursors travel byte-for-byte — they are keys, not numbers', () => {
  const p = useKeysetPager()
  const key = 'wh.deliver:promotion-publication:b15f6d46/#? é'
  p.received(page(key))
  p.next()
  assert.equal(p.current(), key)
})

test('a missing or malformed page verdict ends the walk', () => {
  const p = useKeysetPager()
  p.received(undefined)
  assert.equal(p.canNext.value, false)
  p.received({ truncated: true, nextAfter: '' })
  assert.equal(p.canNext.value, false)
  assert.equal(p.next(), false)
  p.received({ truncated: true })
  assert.equal(p.next(), false)
})

test('a move whose page never landed can be walked back, tail and all', () => {
  const p = useKeysetPager()
  p.received(page('k1'))
  p.next()
  p.received(page('k2'))

  // Next has to push before the request can be sent, so a page that fails
  // leaves the stack one deeper than the rows on screen: "Page 3" over page 2.
  const before = p.mark()
  assert.equal(p.next(), true)
  assert.equal(p.page.value, 3)
  assert.equal(p.canNext.value, false)

  p.restore(before)
  assert.equal(p.page.value, 2)
  assert.equal(p.current(), 'k1')
  // The tail comes back too: a Next that failed must leave Next exactly as
  // usable as it was, not dead until the operator presses Previous.
  assert.equal(p.canNext.value, true)
  assert.equal(p.next(), true)
  assert.equal(p.current(), 'k2')

  // Identity is what the caller compares to decide whether anything else moved
  // the walk meanwhile — every move REPLACES both refs rather than mutating.
  const snap = p.mark()
  p.received(page('k3'))
  assert.notEqual(p.mark().tail, snap.tail)
  p.prev()
  assert.notEqual(p.mark().cursors, snap.cursors)

  // A garbage snapshot is the first page, never a crash.
  p.restore(null)
  p.restore({})
  assert.equal(p.page.value, 1)
  assert.equal(p.canNext.value, false)
})

test('reset clears the stack: a new query is a new sequence', () => {
  const p = useKeysetPager()
  p.received(page('a'))
  p.next()
  p.received(page('b'))
  p.next()
  assert.equal(p.page.value, 3)

  p.reset()
  assert.equal(p.current(), null)
  assert.equal(p.page.value, 1)
  assert.equal(p.canPrev.value, false)
  assert.equal(p.canNext.value, false)
})
