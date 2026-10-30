<?php
// The little book of QueenMQ: the examples of chapter 2 ("Writing applications")
// that are not already in the five-minute guide (00-five-minutes.php), in PHP.
//
// The book shows the regions between `guide:start(<id>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each example does what the book
// says it does.
//
// PHP has no streams client, so the four stream examples (stream-sliding,
// stream-session, stream-hourly, stream-gate) are not here. wire-forms needs
// the zstd extension (pecl install zstd): without it, that example is skipped
// with a note. consume-run needs ext-pcntl and ext-posix, to stop on SIGTERM.
//
// Run it against the node that the book's first section starts:
//   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.7.0
//   php book-client-guide.php
// run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.

require __DIR__ . '/vendor/autoload.php';

use Queen\Queen;

function check(bool $ok, string $what): void
{
    if (!$ok) {
        fwrite(STDERR, "FAIL: {$what}\n");
        exit(1);
    }
}

$queen = new Queen('http://localhost:6632');
foreach (['jobs', 'emails', 'events', 'reviews', 'prices', 'metrics', 'tasks', 'payments',
    'invoices'] as $q) {
    try {
        $queen->queue($q)->delete()->execute();
    } catch (Throwable) {
    }
}

// --- Configure ------------------------------------------------------------
// guide:start(configure-jobs)
$queen->queue('jobs')->config([
    'leaseTime' => 120,                  // 2 minutes per job, then another worker gets it
    'retryLimit' => 5,                   // after 5 failures, the dead-letter queue
    'retryDelay' => 10000,               // a failed job comes back after 10 s, not at once
    'delayedProcessing' => 30,           // a new job waits 30 s before anyone can take it
    'windowBuffer' => 5,                 // a burst waits 5 s, then is handed over at once
    'dedupWindowSeconds' => 86400,       // a transactionId is remembered for a day
    'retentionEnabled' => true,          // retention runs only when this is on:
    'completedRetentionSeconds' => 3600, //   jobs every group has finished go after an hour
    'maxWaitTimeSeconds' => 604800,      // anything older than a week goes, finished or not
])->create()->execute();
// guide:end
$options = $queen->admin()->getQueue('jobs')['options'] ?? [];
check(($options['leaseTime'] ?? null) === 120, 'leaseTime: ' . json_encode($options));
check(($options['retryLimit'] ?? null) === 5, 'retryLimit');
check(($options['retryDelay'] ?? null) === 10000, 'retryDelay');
check(($options['delayedProcessing'] ?? null) === 30, 'delayedProcessing');
check(($options['windowBuffer'] ?? null) === 5, 'windowBuffer');
check(($options['dedupWindowSeconds'] ?? null) === 86400, 'dedupWindowSeconds');
check(($options['retentionEnabled'] ?? null) === true, 'retentionEnabled');
check(($options['completedRetentionSeconds'] ?? null) === 3600, 'completedRetentionSeconds');
check(($options['maxWaitTimeSeconds'] ?? null) === 604800, 'maxWaitTimeSeconds');
echo "ok: configure-jobs\n";

// --- Push -----------------------------------------------------------------
// guide:start(push-callbacks)
$answers = $queen->queue('emails')
    ->partition('customer-42')
    ->push([['transactionId' => 'welcome-42', 'data' => ['to' => 'ada@example.com']]])
    ->onSuccess(fn ($items) => printf("stored %d\n", count($items)))
    ->onDuplicate(fn ($items) => printf("already stored %s\n", $items[0]['transactionId']))
    ->onError(fn ($items, $error) => error_log("refused {$error->getMessage()}"))
    ->execute();
// In PHP an item the broker refuses alone reaches no callback: read its status.
foreach ((array) $answers as $answer) {
    if (($answer['status'] ?? null) === 'error') {
        error_log("refused {$answer['transaction_id']}");
    }
}
// guide:end
check(($answers[0]['status'] ?? null) === 'queued', 'the push answered per item: ' . json_encode($answers));
$duplicate = null;
$queen->queue('emails')->partition('customer-42')
    ->push([['transactionId' => 'welcome-42', 'data' => ['to' => 'ada@example.com']]])
    ->onDuplicate(function (array $items) use (&$duplicate): void {
        $duplicate = $items[0]['transactionId'];
    })
    ->execute();
check($duplicate === 'welcome-42', 'the second push of the same id reached onDuplicate');
echo "ok: push-callbacks\n";

// guide:start(push-buffer)
for ($i = 0; $i < 1000; $i++) {
    $queen->queue('events')
        ->partition('device-' . ($i % 10))
        ->buffer([
            'messageCount' => 100,       // 100 at a time,
            'timeMillis' => 1000,        // or every second (checked at the next push)
        ])
        ->push([[
            'transactionId' => "reading-{$i}",
            'data' => ['device' => $i % 10, 'value' => $i],
        ]])
        ->execute();
}
$queen->flushAllBuffers();               // before exiting, send what is left
// guide:end
{
    $names = array_map(fn (int $i) => "device-{$i}", range(0, 9));
    $read = $queen->queue('events')->fetch($names);
    $total = array_sum(array_map(fn (array $p) => count($p['records']), $read));
    check($total === 1000, "every buffered reading was stored ({$total})");
    echo "ok: push-buffer\n";
}

// The binary forms need the zstd extension. Record what goes over the wire through
// the client's Guzzle handler, to check the forms below.
if (!extension_loaded('zstd')) {
    echo "skip: wire-forms (needs the zstd extension: pecl install zstd)\n";
} else {
    $wire = [];
    $recorder = GuzzleHttp\HandlerStack::create();
    $recorder->push(GuzzleHttp\Middleware::history($wire));
    $wireData = null;
    // guide:start(wire-forms)
    $fast = new Queen([
        'url' => 'http://localhost:6632',
        'handler' => $recorder, // guide:hide
        'binaryPush' => true,       // a push travels as one binary body, not JSON
        'pushCompression' => true,  // with binaryPush: each partition's batch is
                                    // zstd-compressed here (needs ext-zstd)
        'framedPop' => true,        // a pop gets the records as stored, and unpacks them here
    ]);

    $fast->queue('invoices')->partition('customer-42')->push([
        ['transactionId' => 'invoice-2001', 'data' => ['orderId' => 2001, 'total' => 12.5]],
    ])->execute();

    $fast->queue('invoices')->each()->limit(1)->consume(function (array $msg) {
        $GLOBALS['wireData'] = $msg['data']; // guide:hide
        print_r($msg['data']);           // [orderId => 2001, total => 12.5], as with JSON
    })->execute();
    // guide:end
    $push = $pop = null;
    foreach ($wire as $exchange) {
        $uri = $exchange['request']->getUri();
        if ($push === null && str_contains($uri->getPath(), '/api/v1/push')) {
            $push = $exchange;
        }
        if ($pop === null && str_contains($uri->getPath(), '/api/v1/pop/')
            && str_contains($uri->getQuery(), 'frames=true')) {
            $pop = $exchange;
        }
    }
    check($push !== null && $pop !== null, 'the push and the framed pop went over the wire');
    check($push['request']->getHeaderLine('Content-Type') === 'application/x-queen-push',
        'the push went as a binary body');
    check(str_contains((string) $push['request']->getBody(), "\x28\xb5\x2f\xfd"),
        'the binary push carried a zstd frame');
    check(($pop['response']?->getHeaderLine('Content-Type') ?? '') === 'application/x-queen-pop',
        'the broker answered the pop with frames');
    check($wireData === ['orderId' => 2001, 'total' => 12.5], 'framed pop: ' . json_encode($wireData));
    echo "ok: wire-forms\n";
}

// --- Consume --------------------------------------------------------------
$archived = 0;
// guide:start(consume-batch)
$queen->queue('events')
    ->group('archiver')
    ->subscriptionMode('all')
    ->batch(100)                         // up to 100 messages per call
    ->limit(1000)
    ->consume(function (array $messages) { // no ->each(): the handler gets the whole batch
        echo 'archiving ', count($messages), "\n";
        $GLOBALS['archived'] += count($messages); // guide:hide
    })                                   // all acked if it returns, all nacked if it throws
    ->execute();
// guide:end
check($archived === 1000, "the batch handler saw every reading once ({$archived})");
echo "ok: consume-batch\n";

// guide:start(pop-ack)
$messages = $queen->queue('events')
    ->group('auditor')
    ->subscriptionMode('all')
    ->batch(50)
    ->wait(true)                         // long poll: wait for messages, never return empty
    ->pop();
echo 'auditing ', count($messages), "\n";
$queen->ack($messages, 'completed', ['group' => 'auditor']);   // the whole batch at once
// guide:end
check(count($messages) > 0 && count($messages) <= 50, 'pop returned a batch');
echo "ok: pop-ack\n";

$queen->queue('reviews')->partition('shop-3')->push([
    ['transactionId' => 'review-1', 'data' => ['stars' => 5]],
    ['transactionId' => 'review-2', 'data' => ['stars' => 1]],
])->execute();
[$first, $second] = $queen->queue('reviews')->partition('shop-3')->group('moderation')
    ->subscriptionMode('all')->batch(2)->wait(true)->pop();
// guide:start(ack-each-status)
$queen->ack([
    [...$first, '_status' => 'completed'],
    [...$second, '_status' => 'failed', '_error' => 'timeout'],
], 'completed', ['group' => 'moderation']);
// guide:end
{
    // A new pop after the queue's retryDelay (1 s by default) gets the failed review.
    $back = null;
    for ($i = 0; $i < 5 && $back === null; $i++) {
        $back = $queen->queue('reviews')->partition('shop-3')->group('moderation')
            ->batch(2)->wait(true)->timeoutMillis(1500)->pop()[0] ?? null;
    }
    check(($back['transactionId'] ?? null) === 'review-2', 'only the failed review came back');
    $queen->ack($back, 'completed', ['group' => 'moderation']);
    echo "ok: ack-each-status\n";
}

// The helpers the examples call. In a real program they are yours.
final class PermanentError extends RuntimeException
{
}
function sendEmail(array $email): void
{
}

// guide:start(consume-callbacks)
$queen->queue('emails')
    ->group('mailer')
    ->subscriptionMode('all')
    ->autoAck(false)                     // PHP: the callbacks below ack, not the loop
    ->each()
    ->limit(1)
    ->consume(fn (array $msg) => sendEmail($msg['data']))
    ->onSuccess(fn (array $msg) => $queen->ack($msg, 'completed', ['group' => 'mailer']))
    ->onError(fn (array $msg, Throwable $error) => $queen->ack($msg,
        $error instanceof PermanentError ? 'dlq' : 'failed',
        ['group' => 'mailer', 'error' => $error->getMessage()]))
    ->execute();                         // with autoAck(false), a throw also ends execute()
// guide:end
$mailed = count($queen->queue('emails')->group('mailer')->wait(false)->pop());
check($mailed === 0, 'the onSuccess callback acked the email');
echo "ok: consume-callbacks\n";

foreach ([10, 11, 12] as $price) {
    $queen->queue('prices')->partition('sku-A-1')
        ->push([['transactionId' => "price-A-1-{$price}", 'data' => ['sku' => 'A-1', 'price' => $price]]])
        ->execute();
}
// guide:start(conflation)
[$latest] = $queen->queue('prices')
    ->group('repricer')
    ->subscriptionMode('all')
    ->conflation()                       // only the newest message of each partition
    ->wait(false)
    ->pop();
print_r($latest['data']);                // [sku => A-1, price => 12]: 10 and 11 are skipped
$queen->ack($latest, 'completed', ['group' => 'repricer']);
// guide:end
check($latest['data']['price'] === 12, 'conflation delivered the newest price');
check(count($queen->queue('prices')->group('repricer')->conflation()->wait(false)->pop()) === 0,
    'and moved past the older ones');
echo "ok: conflation\n";

for ($i = 0; $i < 5; $i++) {
    $queen->queue('metrics')->partition('cpu')
        ->push([['transactionId' => "cpu-{$i}", 'data' => ['load' => $i]]])->execute();
}
// guide:start(commit-on-delivery)
$readings = $queen->queue('metrics')
    ->group('live-chart')
    ->subscriptionMode('all')
    ->commitOnDelivery()                 // the cursor moves as they are handed out: no ack
    ->batch(100)
    ->wait(false)
    ->pop();
// guide:end
check(count($readings) === 5, 'commitOnDelivery handed out every reading');
check(count($queen->queue('metrics')->group('live-chart')->wait(false)->pop()) === 0,
    'and nothing is left to ack or redeliver');
echo "ok: commit-on-delivery\n";

// guide:start(seek)
$queen->admin()->seekConsumerGroup('archiver', 'events', [
    'timestamp' => '2026-10-22T00:00:00Z',
]);
// the archiver group now reads again everything pushed since that time

$queen->admin()->seekConsumerGroup('archiver', 'events', ['toEnd' => true]);
// and now it skips everything it has not read yet
// guide:end
check(count($queen->queue('events')->group('archiver')->wait(false)->pop()) === 0,
    'after a seek to the end the group has nothing left');
echo "ok: seek\n";

for ($i = 0; $i < 40; $i++) {
    $queen->queue('tasks')->partition('account-' . ($i % 8))
        ->push([['transactionId' => "task-{$i}", 'data' => ['n' => $i]]])->execute();
}
// The task the example runs. After the 40th, this process sends itself the SIGTERM a
// deploy would send, which is what stops the consumer below.
$ran = 0;
function runTask(array $task): void
{
    $GLOBALS['ran']++;
    usleep(5000);
    if ($GLOBALS['ran'] === 40) {
        posix_kill(getmypid(), SIGTERM);
    }
}
// guide:start(consume-run)
// A deploy's SIGTERM stops the loop cleanly: consume() handles it itself (ext-pcntl).
$queen->queue('tasks')
    ->group('workers')
    ->subscriptionMode('all')
    ->concurrency(4)                     // four pops at once; their handlers run in turn
    ->renewLease(true, 10000)            // leases renewed every 10 s, between tasks
    ->idleMillis(60000)                  // or stop after a minute with nothing to do
    ->each()
    ->consume(fn (array $task) => runTask($task['data']))
    ->execute();
// guide:end
check($ran === 40, "four pollers ran every task, then the signal stopped them ({$ran})");
echo "ok: consume-run\n";

// --- Retries and the dead-letter queue --------------------------------------
$queen->queue('payments')->config(['retryLimit' => 1, 'retryDelay' => 100])->create()->execute();
$queen->queue('payments')->partition('order-7')
    ->push([['transactionId' => 'charge-7', 'data' => ['amount' => 25]]])->execute();
// PHP's consume() nacks a handler that throws without its message, so this worker
// fails the payment with an explicit ack that carries the reason.
$queen->queue('payments')->group('charger')->subscriptionMode('all')->autoAck(false)
    ->each()->limit(2)->idleMillis(5000)
    ->consume(fn (array $m) => $queen->ack($m, 'failed', ['group' => 'charger', 'error' => 'card declined']))
    ->execute();
usleep(300_000);
// guide:start(dlq-read)
$dead = $queen->queue('payments')->dlq('charger')->limit(20)->get();
foreach ($dead['messages'] as $letter) {
    echo $letter['transactionId'], ' ', $letter['errorMessage'], ' ',
        $letter['retryCount'], "\n";
}
// guide:end
check(count($dead['messages']) === 1, 'the failing payment is in the dead-letter queue');
check($dead['messages'][0]['errorMessage'] === 'card declined', 'with its error');
echo "ok: dlq-read\n";

// guide:start(dlq-replay)
foreach ($dead['messages'] as $letter) {
    // back into its partition, and out of the dead-letter queue
    $queen->admin()->retryMessage($letter['partitionId'], $letter['transactionId']);
}
// guide:end
{
    $again = $queen->queue('payments')->group('charger')->batch(10)->wait(true)->timeoutMillis(3000)->pop();
    check(count($again) === 1, 'the replayed payment is delivered again');
    $queen->ack($again, 'completed', ['group' => 'charger']);
    $left = $queen->queue('payments')->dlq('charger')->get();
    check(count($left['messages']) === 0, 'and it left the dead-letter queue');
    echo "ok: dlq-replay\n";
}

{
    // The four ack statuses the book's table describes, checked here. 'retry'
    // gives a message back at once without spending a retry; 'dlq' files it now.
    $queen->queue('payments')->partition('order-8')
        ->push([['transactionId' => 'charge-8', 'data' => ['amount' => 9]]])->execute();
    $queen->queue('payments')->partition('order-9')
        ->push([['transactionId' => 'charge-9', 'data' => ['amount' => 4]]])->execute();
    [$r1] = $queen->queue('payments')->partition('order-8')->group('statuses')
        ->subscriptionMode('all')->batch(1)->wait(true)->pop();
    $queen->ack($r1, 'retry', ['group' => 'statuses']);
    [$r2] = $queen->queue('payments')->partition('order-8')->group('statuses')
        ->batch(1)->wait(true)->pop();
    check($r2['transactionId'] === $r1['transactionId'], "'retry' redelivered the same message at once");
    check(($r2['retryCount'] ?? 0) === ($r1['retryCount'] ?? 0), "'retry' spent no retry");
    $queen->ack($r2, 'dlq', ['group' => 'statuses', 'error' => 'bad payload']);
    $filed = $queen->queue('payments')->dlq('statuses')->get();
    check(count($filed['messages']) === 1, "'dlq' filed the message in the dead-letter queue");
    check($filed['messages'][0]['errorMessage'] === 'bad payload', "'dlq' kept the error");
    echo "ok: ack statuses\n";
}

// --- KV -------------------------------------------------------------------
$queen->kv()->delete('carts', 'customer-42');
$queen->kv()->delete('limits', 'api:customer-42');
// guide:start(kv)
// Every write names its lifetime: a ttl, or forever.
$queen->kv()->put('carts', 'customer-42', ['items' => 3], ['ttlSeconds' => 7 * 86400]);
$cart = $queen->kv()->get('carts', 'customer-42');      // [found, value, version, ...]

// Write only if nobody wrote since we read it: the version is the condition.
$saved = $queen->kv()->put('carts', 'customer-42', ['items' => 4],
    ['ttlSeconds' => 7 * 86400, 'expect' => $cart['version']]);
var_dump($saved['applied']);                            // false if the version moved

// A counter with a ceiling: past max, the increment does not apply.
$hit = $queen->kv()->incr('limits', 'api:customer-42', 1,
    ['max' => 100, 'ttlSeconds' => 60]);
if (!$hit['applied']) echo "over the limit\n";          // reason: 'limit'
// guide:end
{
    check($cart['found'] === true, 'the cart was read back');
    check($cart['value'] === ['items' => 3], 'the cart: ' . json_encode($cart['value']));
    check($saved['applied'] === true, 'the conditional write applied: ' . json_encode($saved));
    $stale = $queen->kv()->put('carts', 'customer-42', ['items' => 9],
        ['ttlSeconds' => 7 * 86400, 'expect' => $cart['version']]);
    check($stale['applied'] === false, 'a write from the old version is refused');
    check(($stale['reason'] ?? null) === 'version', 'reason: ' . json_encode($stale));
    check($stale['value'] === ['items' => 4], 'and answers the current value: ' . json_encode($stale));
    check($hit['applied'] === true, 'the first increment applied: ' . json_encode($hit));
    $capped = $queen->kv()->incr('limits', 'api:customer-42', 100, ['max' => 100, 'ttlSeconds' => 60]);
    check($capped['applied'] === false, 'an increment past max does not apply');
    check(($capped['reason'] ?? null) === 'limit', 'reason: ' . json_encode($capped));
    check($capped['value'] === 1, 'and the counter keeps its value: ' . json_encode($capped));
    echo "ok: kv\n";
}

// --- Streams ----------------------------------------------------------------
// PHP has no streams client: the Stream builder exists in the JavaScript, Python,
// Go and Rust clients only, so stream-sliding, stream-session, stream-hourly and
// stream-gate are not in this program.

$queen->close();
echo "\nPASS\n";
