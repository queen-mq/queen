import { Queen } from './client-v2/index.js';

const queen = new Queen({ urls: ['http://localhost:6632'] });
const Q = 'repro-queue';
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

await queen.queue(Q).config({ leaseTime: 60 }).create();

let produced = 0, received = 0;
const seen = new Map();
let consumerActive = true;
let producerRunning = true;

const consumePromise = (async () => {
  while (consumerActive) {
    try {
      const messages = await queen.queue(Q)
        .batch(10).wait(true).timeoutMillis(1000).limit(1).each().pop();
      if (messages && messages.length > 0) {
        received += messages.length;
        for (const m of messages) seen.set(m.transactionId, (seen.get(m.transactionId) || 0) + 1);
        await queen.ack(messages);
      }
    } catch (e) { if (consumerActive) console.error('consumer err', e.message); }
    await sleep(10);
  }
})();

const producerPromise = (async () => {
  while (producerRunning) {
    const batch = [];
    for (let i = 0; i < 10; i++) batch.push({ data: { n: produced + i } });
    await queen.queue(Q).push(batch);
    produced += 10;
    await sleep(1000);
  }
})();

// run for ~20 seconds
await sleep(20000);
producerRunning = false;
await producerPromise;
console.log('produced', produced);
// drain
for (let i = 0; i < 10 && received < produced; i++) await sleep(1000);
consumerActive = false;
await consumePromise;

const dups = [...seen.values()].filter(v => v > 1).length;
const maxDup = Math.max(0, ...seen.values());
console.log(JSON.stringify({ produced, received, uniqueTxns: seen.size, dupTxns: dups, maxDeliveriesOfOneTxn: maxDup }));
process.exit(0);
