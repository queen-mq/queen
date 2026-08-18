/**
 * Timer integration suite (PLAN_KV_TIMERS.md §4, §9.6, §20.2).
 *
 * Repeatable only because run.js purges `queen.log_timers` for the `test-%`
 * queues before the run: a pending timer left behind by a previous run fires
 * into the queue of the next one and shows up as a phantom message in an
 * unrelated test.
 *
 * Every delay here is either short (it is meant to fire) or minutes long (it is
 * meant not to). Nothing in this file schedules anything that outlives the
 * suite.
 */

import { TIMER_QUEUE, popUntil } from './_kvtimers.js'

export async function timerSchedulesAndFires(client) {
  const queueName = `${TIMER_QUEUE}-fire`
  const queue = await client.queue(queueName).create()
  if (!queue.configured) return { success: false, message: 'Queue not created' }

  // A per-run marker, so a message left in this queue by an earlier run can
  // never be mistaken for this run's delivery. It is the one place in these
  // suites where per-run uniqueness is right: the assertion is about THIS
  // timer's identity, and the purge cannot be relied on to prove identity.
  const run = `fire-${Date.now()}`
  const res = await client.timer(queueName)
    .key('fire-1')
    .delay('500ms')
    .payload({ kind: 'reminder', run })
    .schedule()

  if (res.status !== 'scheduled' || !res.messageId || !res.deliverAt) {
    return { success: false, message: `schedule answered ${JSON.stringify(res)}` }
  }

  // deliverAt is "not before", never "exactly at", and the visibility of a
  // fired timer to a consumer is bounded by the hot-list reseed, not by the
  // delay -- see popUntil, which measured it. The assertion is arrival.
  const mine = await popUntil(client, queueName, m => m.data && m.data.run === run)

  if (mine.length !== 1) {
    return { success: false, message: 'the scheduled timer was never delivered into the queue' }
  }
  // The two identifiers promised at schedule time are the two the delivered
  // frame carries -- which is what lets a client correlate without a second API.
  if (mine[0].transactionId !== res.txn) {
    return { success: false, message: `delivered txn ${mine[0].transactionId} != promised ${res.txn}` }
  }
  if (mine[0].id !== res.messageId) {
    return { success: false, message: `delivered messageId ${mine[0].id} != promised ${res.messageId}` }
  }

  await client.ack(mine[0])
  return { success: true, message: 'a scheduled timer was delivered carrying the txn and messageId promised at schedule time' }
}

export async function timerCancelBeforeFire(client) {
  const queueName = `${TIMER_QUEUE}-cancel`
  const queue = await client.queue(queueName).create()
  if (!queue.configured) return { success: false, message: 'Queue not created' }

  await client.timer(queueName).key('cancel-1').delay('30m').payload({ n: 1 }).schedule()

  const pending = await client.timer(queueName).key('cancel-1').peek()
  if (pending.found !== true) {
    return { success: false, message: `peek did not find the pending timer: ${JSON.stringify(pending)}` }
  }

  const cancelled = await client.timer(queueName).key('cancel-1').cancel()
  const gone = await client.timer(queueName).key('cancel-1').peek()

  const ok = cancelled.ok === true && cancelled.status === 'cancelled' && gone.found === false
  return {
    success: ok,
    message: ok
      ? 'a pending timer was peeked, cancelled and is gone'
      : `cancel=${JSON.stringify(cancelled)} peek=${JSON.stringify(gone)}`
  }
}

/**
 * §4.4, the point where a user gets hurt: there is no tombstone. A cancel on
 * something that is not pending answers `absent` with **ok:false**, and echoes
 * the txn so the log -- the only authority on "did it already go out?" -- can
 * be consulted without a second API.
 */
export async function timerCancelAbsentCarriesOkFalseAndTheTxn(client) {
  const queueName = `${TIMER_QUEUE}-absent`
  const expected = 'txn-that-may-already-have-been-delivered'
  const res = await client.timer(queueName).key('never-scheduled').txn(expected).cancel()

  const ok = res.ok === false && res.status === 'absent' && res.txn === expected
  return {
    success: ok,
    message: ok
      ? 'absent answers ok:false and hands back the txn to check against the log'
      : JSON.stringify(res)
  }
}

/**
 * §20.2: a reschedule is an UPSERT that mints a new txn, because a rescheduled
 * timer is a new message. The corollary belongs in the docs, not in an
 * assertion: there is no dedup net on the fire, so rescheduling one that has
 * already gone out produces a second message.
 */
export async function timerRescheduleOverwritesTheTxn(client) {
  const queueName = `${TIMER_QUEUE}-resched`
  const queue = await client.queue(queueName).create()
  if (!queue.configured) return { success: false, message: 'Queue not created' }

  const first = await client.timer(queueName).key('resched-1').delay('30m').payload({ v: 1 }).schedule()
  const second = await client.timer(queueName).key('resched-1').delay('45m').payload({ v: 2 }).schedule()
  const pending = await client.timer(queueName).key('resched-1').peek()

  const ok = first.status === 'scheduled'
    && second.status === 'rescheduled'
    && second.txn !== first.txn
    && pending.found === true
    && pending.txn === second.txn

  return {
    success: ok,
    message: ok
      ? 'the second schedule upserted the same key, reported "rescheduled" and replaced the txn'
      : `first=${JSON.stringify(first)} second=${JSON.stringify(second)} peek=${JSON.stringify(pending)}`
  }
}

export async function timerListIsKeysetOverOneQueue(client) {
  const queueName = `${TIMER_QUEUE}-list`
  const queue = await client.queue(queueName).create()
  if (!queue.configured) return { success: false, message: 'Queue not created' }

  for (const k of ['list-a', 'list-b', 'list-c']) {
    await client.timer(queueName).key(k).delay('30m').payload({ k }).schedule()
  }

  const page1 = await client.timer(queueName).list({ limit: 2 })
  if (page1.rows.length !== 2 || page1.truncated !== true || !page1.nextAfter) {
    return { success: false, message: `first page: ${JSON.stringify(page1)}` }
  }

  const page2 = await client.timer(queueName).list({ after: page1.nextAfter, limit: 2 })
  const keys = [...page1.rows, ...page2.rows].map(r => r.timerKey)

  const ok = keys.length === 3 && keys.join(',') === 'list-a,list-b,list-c'
  return {
    success: ok,
    message: ok
      ? 'list pages over one queue on an exclusive keyset cursor'
      : `saw ${JSON.stringify(keys)}`
  }
}

/**
 * The timer as a rider of a transaction: it commits with the push, in the same
 * transaction, or not at all. This is the shape a saga uses -- schedule the
 * compensation together with the work that may need compensating.
 */
export async function timerRidesTheTransaction(client) {
  const queueName = `${TIMER_QUEUE}-txn`
  const queue = await client.queue(queueName).create()
  if (!queue.configured) return { success: false, message: 'Queue not created' }

  const res = await client.transaction()
    .queue(queueName)
    .push([{ data: { step: 'reserved' } }])
    .timer(queueName).key('compensate-1').delay('30m').payload({ step: 'compensate' }).schedule()
    .commit()

  if (res.success !== true) {
    return { success: false, message: `the bundle should commit: ${JSON.stringify(res)}` }
  }

  const pending = await client.timer(queueName).key('compensate-1').peek()
  const cancelled = await client.timer(queueName).key('compensate-1').cancel()

  const ok = pending.found === true && cancelled.ok === true
  return {
    success: ok,
    message: ok
      ? 'a timer scheduled inside a transaction is pending after the commit'
      : `peek=${JSON.stringify(pending)} cancel=${JSON.stringify(cancelled)}`
  }
}

/**
 * A delay in the past is LEGAL and fires on the first cycle (§4.2). It is also
 * the cheapest end-to-end proof that the sweeper is running at all.
 */
export async function timerWithAPastDelayFiresImmediately(client) {
  const queueName = `${TIMER_QUEUE}-past`
  const queue = await client.queue(queueName).create()
  if (!queue.configured) return { success: false, message: 'Queue not created' }

  const run = `past-${Date.now()}`
  await client.timer(queueName).key('past-1').delayMs(-5000).payload({ late: true, run }).schedule()

  const mine = await popUntil(client, queueName, m => m.data && m.data.run === run)
  if (mine.length === 1) await client.ack(mine[0])

  return {
    success: mine.length === 1,
    message: mine.length === 1
      ? 'a delayMs in the past fired on the first sweep cycle'
      : 'the late timer was never delivered'
  }
}
