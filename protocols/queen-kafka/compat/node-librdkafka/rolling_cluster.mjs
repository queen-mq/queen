// Idempotent librdkafka producers, under load, through rolling restarts and
// leader kills of an EMBEDDED Queen cluster (compat/cluster/rig-embedded.sh) —
// the shape that killed four stage producers on 2026-10-19.
//
//   KAFKA_BOOTSTRAP=127.0.0.1:59092,127.0.0.1:59093,127.0.0.1:59094 \
//   RIG=../cluster/rig-embedded.sh node rolling_cluster.mjs
//
// Environment:
//   PHASES     rig commands to run one after another, separated by `;`
//              (default: "rolling term 8;leader-kill;rolling kill 8")
//   PRODUCERS  idempotent producers (default 6)
//   RATE       target records per second across all of them (default 3000)
//   SETTLE_S   seconds of load between phases (default 12)
//   RUN_ID     topic suffix (default: now)
//
// What it asserts, per producer and partition, from what it reads back:
//   * no send failed FATALLY, and every producer can still send at the end;
//   * every acknowledged record is there (nothing lost);
//   * the records are in the order they were sent (first copies);
// and it reports how many were written twice (the documented, bounded cost).
import { KafkaJS } from '@confluentinc/kafka-javascript'
import { execSync } from 'node:child_process'

const BOOTSTRAP = process.env.KAFKA_BOOTSTRAP || '127.0.0.1:59092,127.0.0.1:59093,127.0.0.1:59094'
const RIG = process.env.RIG || new URL('../cluster/rig-embedded.sh', import.meta.url).pathname
const RUN = process.env.RUN_ID || String(Date.now())
const PHASES = (process.env.PHASES || 'rolling term 8;leader-kill;rolling kill 8')
  .split(';').map((s) => s.trim()).filter(Boolean)
const PRODUCERS = Number(process.env.PRODUCERS || 6)
const RATE = Number(process.env.RATE || 3000)
const SETTLE_S = Number(process.env.SETTLE_S || 12)
const LANES = 4
const BATCH = 10

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
let failures = 0
const say = (m) => console.log(`\n=== ${m}`)
const info = (m) => console.log(`  . ${m}`)
const check = (ok, m) => {
  console.log(`  ${ok ? 'PASS' : 'FAIL'} ${m}`)
  if (!ok) failures++
}

const topics = [
  ...Array.from({ length: 8 }, (_, i) => ({ name: `rc-${RUN}-s${i}`, partitions: 1 })),
  { name: `rc-${RUN}-wide-a`, partitions: 4 },
  { name: `rc-${RUN}-wide-b`, partitions: 4 },
]
const slots = topics.flatMap((t) => Array.from({ length: t.partitions }, (_, p) => ({ topic: t.name, partition: p })))

const kafka = new KafkaJS.Kafka({
  kafkaJS: { brokers: BOOTSTRAP.split(','), clientId: `rolling-${RUN}`, logLevel: KafkaJS.logLevel.NOTHING },
})

function rig(cmd) {
  const started = Date.now()
  const out = execSync(`${RIG} ${cmd}`, { encoding: 'utf8', timeout: 240000 })
  info(`rig ${cmd}: ${out.trim().split('\n').join(' | ')} (${((Date.now() - started) / 1000).toFixed(1)}s)`)
}

async function main() {
  say(`${PRODUCERS} idempotent producers at ~${RATE} rec/s over ${slots.length} partitions; phases: ${PHASES.join(' / ')}`)
  const admin = kafka.admin()
  await admin.connect()
  await admin.createTopics({ topics: topics.map((t) => ({ topic: t.name, numPartitions: t.partitions })) })
  await admin.disconnect()

  // Per producer: the next sequence per slot, what was acknowledged, what failed.
  const state = Array.from({ length: PRODUCERS }, () => ({
    next: new Map(), acked: new Map(), failed: [], fatal: [],
  }))
  const producers = []
  for (let p = 0; p < PRODUCERS; p++) {
    const producer = kafka.producer({
      'linger.ms': 5,
      'max.in.flight.requests.per.connection': 5,
      'message.timeout.ms': 180000,
      kafkaJS: { acks: -1, idempotent: true },
    })
    await producer.connect()
    producers.push(producer)
  }

  let stop = false
  let sent = 0
  const perLaneDelay = (PRODUCERS * LANES * BATCH * 1000) / RATE
  const lane = async (p, l) => {
    const st = state[p]
    let i = l
    while (!stop) {
      const slot = slots[(i += 7) % slots.length]
      const key = `${slot.topic}/${slot.partition}`
      const base = st.next.get(key) || 0
      st.next.set(key, base + BATCH)
      const values = Array.from({ length: BATCH }, (_, k) => `${p}|${key}|${base + k}`)
      const t0 = Date.now()
      try {
        await producers[p].send({
          topic: slot.topic,
          messages: values.map((v) => ({ value: Buffer.from(v), partition: slot.partition })),
        })
        const a = st.acked.get(key) || []
        a.push(...values.map((_, k) => base + k))
        st.acked.set(key, a)
        sent += BATCH
      } catch (e) {
        const fatal = /fatal/i.test(e.message) || e.fatal === true
        ;(fatal ? st.fatal : st.failed).push(`${new Date().toISOString()} ${key}@${base}: ${e.message}`)
        if (fatal) return
      }
      const wait = perLaneDelay - (Date.now() - t0)
      if (wait > 0) await sleep(wait)
    }
  }
  const lanes = []
  for (let p = 0; p < PRODUCERS; p++) for (let l = 0; l < LANES; l++) lanes.push(lane(p, l))

  const ticker = setInterval(() => info(`${sent} acknowledged so far`), 10000)
  await sleep(SETTLE_S * 1000)
  for (const phase of PHASES) {
    say(`phase: ${phase}`)
    if (phase === 'leader-kill') {
      const leader = execSync(`${RIG} leader`, { encoding: 'utf8' }).trim()
      rig(`restart ${leader} kill`)
    } else {
      rig(phase)
    }
    await sleep(SETTLE_S * 1000)
  }
  stop = true
  await Promise.all(lanes)
  clearInterval(ticker)

  say('producers')
  const fatal = state.flatMap((s) => s.fatal)
  const failed = state.flatMap((s) => s.failed)
  check(fatal.length === 0, `no send failed fatally (${fatal.length}${fatal.length ? `; first: ${fatal[0].slice(0, 200)}` : ''})`)
  info(`${failed.length} send(s) failed retriably and were not counted as acknowledged${failed.length ? `; first: ${failed[0].slice(0, 160)}` : ''}`)
  let finals = 0
  for (const producer of producers) {
    const ok = await producer
      .send({ topic: topics[0].name, messages: [{ value: Buffer.from('final'), partition: 0 }] })
      .then(() => true, () => false)
    if (ok) finals++
  }
  check(finals === PRODUCERS, `every producer still sends after the phases (${finals}/${PRODUCERS})`)
  for (const producer of producers) await producer.disconnect().catch(() => {})

  // Read everything back.
  say('reading back')
  const want = state.reduce((n, s) => n + [...s.acked.values()].reduce((m, a) => m + a.length, 0), 0)
  const seen = new Map() // `${p}|${key}` -> sequences in read order
  let read = 0
  const consumer = kafka.consumer({ kafkaJS: { groupId: `rolling-${RUN}-check`, fromBeginning: true } })
  await consumer.connect()
  await consumer.subscribe({ topics: topics.map((t) => t.name) })
  let last = Date.now()
  consumer.run({
    eachMessage: async ({ message }) => {
      const v = String(message.value)
      if (v === 'final') return
      const [p, key, seq] = v.split('|')
      const k = `${p}|${key}`
      const arr = seen.get(k) || []
      arr.push(Number(seq))
      seen.set(k, arr)
      read++
      last = Date.now()
    },
  })
  const deadline = Date.now() + 300000
  while (Date.now() < deadline) {
    await sleep(2000)
    let have = 0
    for (const arr of seen.values()) have += new Set(arr).size
    if (have >= want && Date.now() - last > 5000) break
  }
  await consumer.disconnect().catch(() => {})

  let missing = 0
  let disordered = 0
  let twice = 0
  const examples = []
  for (let p = 0; p < PRODUCERS; p++) {
    for (const [key, acked] of state[p].acked) {
      const got = seen.get(`${p}|${key}`) || []
      const firsts = []
      const once = new Set()
      for (const s of got) {
        if (once.has(s)) { twice++; continue }
        once.add(s)
        firsts.push(s)
      }
      for (const s of acked) {
        if (!once.has(s)) {
          missing++
          if (examples.length < 5) examples.push(`missing ${p}|${key}@${s}`)
        }
      }
      for (let i = 1; i < firsts.length; i++) {
        if (firsts[i] < firsts[i - 1]) {
          disordered++
          if (examples.length < 5) examples.push(`out of order ${p}|${key}: ${firsts[i - 1]} then ${firsts[i]}`)
        }
      }
    }
  }
  info(`${want} acknowledged, ${read} read`)
  check(missing === 0, `every acknowledged record was read back (${missing} missing)`)
  check(disordered === 0, `every producer's records are in send order per partition (${disordered} out of order)`)
  info(`${twice} record(s) were written twice (bounded by the batches in flight at each restart)`)
  for (const e of examples) info(e)

  say(failures === 0 ? 'ALL PASSED' : `${failures} FAILED`)
  process.exit(failures === 0 ? 0 : 1)
}

main().catch((e) => {
  console.error(e)
  process.exit(2)
})
