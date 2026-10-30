<?php
// Queen in five minutes: the program behind the homepage guide, in PHP.
//
// The homepage shows the regions between `guide:start(<step>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each step did what the page says.
// PHP has no streams client, so this program skips step 6.
//
// Run it against the node that step 1 of the guide starts:
//   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.7.0
//   php 00-five-minutes.php
// run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.

require __DIR__ . '/vendor/autoload.php';

// guide:start(configure)
use Queen\Queen;
// guide:end

function check(bool $ok, string $what): void
{
    if (!$ok) {
        fwrite(STDERR, "FAIL: {$what}\n");
        exit(1);
    }
}

// The request/reply worker runs as a second process, as it would in real life.
if (($argv[1] ?? '') === 'worker') {
    $queen = new Queen('http://localhost:6632');
    // guide:start(worker)
    $result = $queen->ephemeral()->pop('price-requests', ['group' => 'pricers', 'wait' => true]);
    foreach ($result['messages'] as $m) {
        $queen->ephemeral()->push($m['payload']['replyTo'], [['sku' => $m['payload']['sku'], 'price' => 59.9]]);
    }
    $queen->ephemeral()->ack('price-requests', $result['messages'], ['group' => 'pricers']);
    // guide:end
    exit(0);
}

$admin = new Queen('http://localhost:6632');
foreach (['orders', 'shipments', 'reminders', 'reports'] as $q) {
    try {
        $admin->queue($q)->delete()->execute();
    } catch (Throwable) {
    }
}

// guide:start(configure)
$queen = new Queen('http://localhost:6632');

$queen->queue('orders')
    ->namespace('shop')              // groups queues in the dashboard
    ->config([
        'leaseTime' => 30,           // seconds a worker holds a message before it is redelivered
        'retryLimit' => 3,           // failures before the message goes to the dead-letter queue
        'retentionEnabled' => true,  // retention runs only when this is on
        'retentionSeconds' => 86400, // then messages older than a day are removed
    ])
    ->create()
    ->execute();
// guide:end
echo "ok: configure\n";

// guide:start(push)
$queen->queue('orders')
    ->partition('customer-42')        // one ordered partition per customer, created by this push
    ->push([[
        'transactionId' => 'order-1001', // the dedup key: pushing the same id again is a no-op
        'data' => ['orderId' => 1001, 'customer' => 42, 'total' => 59.9],
    ]])
    ->execute();
// guide:end
echo "ok: push\n";

// guide:start(consume)
$queen->queue('orders')
    ->each()                          // the handler gets one message at a time
    ->limit(1)                        // stop after one message; a worker leaves this out
    ->consume(function (array $msg) { // acked when it returns, retried if it throws
        print_r($msg['data']);
    })
    ->execute();
// guide:end
check(count($queen->queue('orders')->wait(false)->pop()) === 0, 'queue mode acked the order');
echo "ok: consume\n";

// guide:start(consume-group)
$queen->queue('orders')
    ->group('billing')                // every group gets every message
    ->subscriptionMode('all')         // a new group starts at the tail unless told otherwise
    ->batch(100)                      // up to 100 messages per pop...
    ->partitions(10)                  // ...from up to 10 partitions
    ->renewLease(true, 2000)          // renew every 2 s, between messages
    ->autoAck(false)                  // the handler decides the outcome
    ->each()
    ->limit(1)
    ->consume(function (array $msg) use ($queen) {
        $ok = $msg['data']['total'] > 0;
        $queen->ack($msg, $ok ? 'completed' : 'failed', ['group' => 'billing']);
    })
    ->execute();
// guide:end
// consume() with limit(1) returns only after one message: reaching here is the check.
echo "ok: consume-group\n";

// guide:start(transaction)
$queen->queue('orders')
    ->group('fulfillment')
    ->subscriptionMode('all')
    ->autoAck(false)
    ->each()
    ->limit(1)
    ->consume(function (array $msg) use ($queen) {
        $id = $msg['data']['orderId'];
        $queen->transaction()
            ->ack($msg, 'completed', ['consumerGroup' => 'fulfillment']) // the input is done
            ->kv('orders')->put("order-{$id}", ['status' => 'paid'], ['ttlSeconds' => 7 * 86400])
            ->queue('shipments')->partition($msg['partition'])           // the next event
            ->push([['transactionId' => "ship-{$id}", 'data' => ['orderId' => $id]]])
            ->timers('reminders')->schedule("review-{$id}", 24 * 3600 * 1000, // tomorrow, in ms
                ['orderId' => $id, 'customer' => $msg['data']['customer']])
            ->commit();
    })
    ->execute();
// guide:end
$state = $queen->kv()->get('orders', 'order-1001');
check(($state['value']['status'] ?? null) === 'paid', 'the KV write committed: ' . json_encode($state));
$shipped = $queen->queue('shipments')->wait(false)->pop();
check(count($shipped) === 1, 'the shipment was pushed');
check($shipped[0]['partition'] === 'customer-42', 'into the same customer partition');
echo "ok: transaction\n";

// guide:start(replay)
$queen->queue('orders')
    ->group('audit')                           // a new group, with its own cursor
    ->subscriptionFrom('2026-10-22T00:00:00Z') // everything pushed from this time on
    ->each()
    ->limit(1)
    ->consume(function (array $msg) {
        print_r($msg['data']);
    })
    ->execute();
// guide:end
echo "ok: replay\n";

// guide:start(fetch)
$read = $queen->queue('orders')->fetch(['customer-42']); // no group, lease or ack
foreach ($read['customer-42']['records'] as $record) {
    echo $record['offset'], ' ', json_encode($record['payload']), "\n";
}
// guide:end
check(($read['customer-42']['records'][0]['payload']['orderId'] ?? null) === 1001, 'fetch read the partition');
echo "ok: fetch\n";

// guide:start(lock)
$lock = $queen->lock('daily-report', ['ttlSeconds' => 30]); // a lease with a fencing token
if ($lock->acquire()) {                                    // false: another worker has it
    try {
        $queen->transaction()
            ->guard($lock)                                 // commits only while the lock is ours
            ->queue('reports')
            ->push([['transactionId' => 'report-2026-10-22', 'data' => ['day' => '2026-10-22']]])
            ->commit();
    } finally {
        $lock->release();
    }
}
// guide:end
check(count($queen->queue('reports')->wait(false)->pop()) === 1, 'the guarded report was pushed');
echo "ok: lock\n";

$worker = proc_open([PHP_BINARY, __FILE__, 'worker'], [1 => STDOUT, 2 => STDERR], $pipes);
usleep(500_000); // the worker is already waiting when a caller asks
// guide:start(caller)
$inbox = 'inbox-' . bin2hex(random_bytes(8));      // a private reply queue
$queen->ephemeral()->push('price-requests', [['sku' => 'A-1', 'replyTo' => $inbox]]);
$reply = $queen->ephemeral()->pop($inbox, ['wait' => true, 'timeout' => 5000, 'commitOnDelivery' => true]);
print_r($reply['messages'][0]['payload']);          // [sku => A-1, price => 59.9]
// guide:end
check(proc_close($worker) === 0, 'the worker exited cleanly');
check(($reply['messages'][0]['payload']['price'] ?? null) === 59.9, 'the reply came back');
echo "ok: ephemeral\n";

$queen->close();
$admin->close();
echo "\nPASS\n";
