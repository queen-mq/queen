<?php
// The little book of QueenMQ: the use cases of chapter 3 ("Real applications examples"),
// in PHP.
//
// The book shows the regions between `guide:start(<id>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each use case does what the book
// says it does.
//
// PHP has no threads: where several workers run at once (uc-work, uc-reply,
// uc-singleton), they are processes made with pcntl_fork, so this program needs
// ext-pcntl. PHP has no streams client, so uc-trace and uc-rate are not here.
//
// Run it against a node:
//   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.7.0
//   php book-use-cases.php
// run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.

// The webhook receiver of uc-webhook: PHP's built-in server runs this same file as
// its router (see below). It fails the first call, and keeps each idempotency key once.
if (PHP_SAPI === 'cli-server') {
    $state = getenv('BOOK_HOOK_STATE');
    $calls = (int) @file_get_contents("{$state}/calls") + 1;
    file_put_contents("{$state}/calls", (string) $calls);
    if ($calls === 1) {
        http_response_code(500);
        return;
    }
    $key = $_SERVER['HTTP_IDEMPOTENCY_KEY'] ?? '';
    file_put_contents("{$state}/received", $key . ' ' . file_get_contents('php://input') . "\n",
        FILE_APPEND | LOCK_EX);
    return;
}

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
foreach (['thumbnails', 'order-placed', 'chat', 'order-events', 'shipping', 'availability',
    'review-reminders', 'webhooks', 'daily-reports'] as $q) {
    try {
        $queen->queue($q)->delete()->execute();
    } catch (Throwable) {
    }
}
try {
    $queen->kv()->delete('orders', 'order-2001');
    // A reminder an earlier run left pending, and quote requests it left unanswered.
    $queen->timers()->cancel('review-reminders', 'review-2001');
    $queen->timers()->cancel('review-reminders', 'review-2002');
    $queen->ephemeral()->reset('quotes');
} catch (Throwable) {
}

// --- Work queue: competing consumers ---------------------------------------
// The work the example does. In a real program it is yours. The workers are
// processes, so each one writes what it did to a file the checks read.
$doneFile = tempnam(sys_get_temp_dir(), 'book-uc-work-');
function resize(string $photo, string $worker): void
{
    file_put_contents($GLOBALS['doneFile'], "{$photo} {$worker}\n", FILE_APPEND | LOCK_EX);
    usleep(50_000);
}
$codes = [];
// guide:start(uc-work)
// Twelve photos to resize, from four users.
for ($i = 1; $i <= 12; $i++) {
    $queen->queue('thumbnails')->partition('user-' . ($i % 4))
        ->push([['data' => ['photo' => "p{$i}.jpg"]]])
        ->execute();
}

// Three workers compete for the jobs: each job goes to exactly one of them.
$worker = fn (string $name) => $queen->queue('thumbnails')
    ->partitions(1)                                 // one user's photos at a time
    ->each()
    ->idleMillis(1500)                              // stop when there is nothing left
    ->consume(fn (array $job) => resize($job['data']['photo'], $name))
    ->execute();

$pids = [];
foreach (['w1', 'w2', 'w3'] as $name) {             // PHP has no threads: three processes
    if (($pid = pcntl_fork()) === 0) {
        $worker($name);
        exit(0);
    }
    $pids[] = $pid;
}
foreach ($pids as $pid) {
    pcntl_waitpid($pid, $status);                   // wait until all three have stopped
    $codes[] = pcntl_wexitstatus($status); // guide:hide
}
// guide:end
{
    check($codes === [0, 0, 0], 'the three workers exited cleanly: ' . json_encode($codes));
    $done = array_map(fn (string $line) => explode(' ', $line), file($doneFile, FILE_IGNORE_NEW_LINES));
    unlink($doneFile);
    $photos = array_column($done, 0);
    check(count($photos) === 12, 'twelve jobs handled: ' . count($photos));
    check(count(array_unique($photos)) === 12, 'each job once');
    $perWorker = array_count_values(array_column($done, 1));
    ksort($perWorker);
    check(count($perWorker) >= 2, 'the jobs were shared: ' . json_encode($perWorker));
    echo 'ok: uc-work ', json_encode($perWorker), "\n";
}

// --- Publish/subscribe and fan-out: consumer groups ------------------------
$seen = ['billing' => [], 'shipping' => [], 'analytics' => []];
// guide:start(uc-pubsub)
$subscribe = fn (string $group) => $queen->queue('order-placed')
    ->group($group)                                 // each group gets every message
    ->subscriptionMode('all')
    ->each()
    ->idleMillis(1000) // guide:hide
    ->consume(function (array $event) use ($group) {
        $GLOBALS['seen'][$group][] = $event['data']['orderId']; // guide:hide
        echo $group, ' ', $event['data']['orderId'], "\n";
    })
    ->execute();

foreach ([3001, 3002, 3003] as $orderId) {
    $queen->queue('order-placed')->partition("order-{$orderId}")
        ->push([['data' => ['orderId' => $orderId]]])
        ->execute();
}
$subscribe('billing');           // one after the other here: in production each group
$subscribe('shipping');          // is a worker process of its own

// Months later, analytics subscribes too, and reads every order ever placed.
$subscribe('analytics');
// guide:end
{
    foreach (['billing', 'shipping', 'analytics'] as $g) {
        sort($seen[$g]);
        check($seen[$g] === [3001, 3002, 3003], "{$g} got every order: " . json_encode($seen[$g]));
    }
    // A group that does not ask for 'all' starts at the end of the queue.
    $late = $queen->queue('order-placed')->group('late')->wait(false)->batch(10)->pop();
    check(count($late) === 0, 'a new group without subscriptionMode all skips the history');
    echo "ok: uc-pubsub\n";
}

// --- 1. Chat processing: a burst is one batch (windowBuffer) ---------------
$t0 = 0.0;
$calls = 0;
$question = '';
// guide:start(uc-chat)
// A burst waits 2 seconds, then goes to the handler as one batch.
$queen->queue('chat')->config(['windowBuffer' => 2])->create()->execute();
$t0 = microtime(true); // guide:hide

// The guest sends three messages in a row.
foreach (['Hi!', 'Is breakfast included?', 'And is there parking?'] as $text) {
    $queen->queue('chat')->partition('conv-42')->push([['data' => ['text' => $text]]])
        ->execute();
}

// The assistant answers the whole burst once.
$queen->queue('chat')
    ->group('assistant')
    ->subscriptionMode('all')
    ->batch(10)                          // the handler gets every message of the burst
    ->limit(3) // guide:hide
    ->consume(function (array $messages) {
        $GLOBALS['calls']++; // guide:hide
        $question = implode(' ', array_map(fn (array $m) => $m['data']['text'], $messages));
        echo $question, "\n";            // Hi! Is breakfast included? And is there parking?
        $GLOBALS['question'] = $question; // guide:hide
    })
    ->execute();
// guide:end
{
    check($calls === 1, "the three messages reached the handler in one call ({$calls})");
    check($question === 'Hi! Is breakfast included? And is there parking?', $question);
    $waited = (int) round((microtime(true) - $t0) * 1000);
    check($waited >= 1500, "the burst was held for the window ({$waited} ms)");
    echo "ok: uc-chat (one call, after {$waited} ms)\n";
}

// --- 2. Order workflow: transitions that cannot race (expect) --------------
$queen->kv()->put('orders', 'order-2001', ['status' => 'paid'], ['ttlSeconds' => 30 * 86400]);
$queen->queue('order-events')->partition('order-2001')
    ->push([['transactionId' => 'order-2001-paid', 'data' => ['orderId' => 'order-2001']]])
    ->execute();
$verdicts = [];
// guide:start(uc-order)
$queen->queue('order-events')
    ->group('workflow')
    ->subscriptionMode('all')
    ->autoAck(false)
    ->each()
    ->limit(1) // guide:hide
    ->consume(function (array $msg) use ($queen) {
        $orderId = $msg['data']['orderId'];
        $order = $queen->kv()->get('orders', $orderId);       // value: [status => paid]
        if ($order['value']['status'] !== 'paid') {           // already moved: just ack
            $queen->ack($msg, 'completed', ['group' => 'workflow']);
            return;
        }

        // Do the work of this step here: charge the card, book the courier, ...

        $result = $queen->transaction()
            ->ack($msg, 'completed', ['consumerGroup' => 'workflow'])
            ->kv('orders')->put($orderId, ['status' => 'shipping'], [
                'ttlSeconds' => 30 * 86400,
                'expect' => $order['version'],                // only from what we read
                'required' => true,
            ])
            ->queue('shipping')->partition($orderId)->push([
                ['transactionId' => "ship-{$orderId}", 'data' => ['orderId' => $orderId]],
            ])
            ->commit();
        $GLOBALS['verdicts'][] = $result['success'] !== false; // guide:hide
        if ($result['success'] === false) {
            return;                        // moved by someone else: nothing written
        }
    })
    ->execute();
// guide:end
{
    check($verdicts === [true], 'the step committed: ' . json_encode($verdicts));
    $order = $queen->kv()->get('orders', 'order-2001');
    check(($order['value']['status'] ?? null) === 'shipping', 'the order moved to shipping');
    // The same transition from the old version is refused, and writes nothing.
    $stale = $queen->transaction()
        ->kv('orders')->put('order-2001', ['status' => 'shipping'],
            ['ttlSeconds' => 30 * 86400, 'expect' => $order['version'] - 1, 'required' => true])
        ->queue('shipping')->partition('order-2001')
        ->push([['transactionId' => 'ship-again', 'data' => ['orderId' => 'order-2001']]])
        ->commit();
    check($stale['success'] === false && ($stale['reason'] ?? null) === 'kv_precondition',
        'a transition from a stale version rolls back: ' . json_encode($stale));
    $read = $queen->queue('shipping')->fetch(['order-2001']);
    check(count($read['order-2001']['records']) === 1, 'one shipment, not two');
    echo "ok: uc-order\n";
}

// --- 3. Trace management (session window) and 4. rate limiting (gate) -------
// Both are streams, and PHP has no streams client: uc-trace and uc-rate are not
// in this program.

// --- 5. Only the newest value (conflation) ----------------------------------
// guide:start(uc-latest)
// The availability of a hotel changes often; the screen needs only the last value.
foreach ([12, 11, 9] as $rooms) {
    $queen->queue('availability')->partition('hotel-7')
        ->push([['data' => ['hotel' => 7, 'rooms' => $rooms]]])
        ->execute();
}

[$latest] = $queen->queue('availability')
    ->group('screen')
    ->subscriptionMode('all')
    ->conflation()                   // the newest message of each hotel, the rest skipped
    ->wait(false)
    ->pop();
print_r($latest['data']);            // [hotel => 7, rooms => 9]
$queen->ack($latest, 'completed', ['group' => 'screen']);
// guide:end
{
    check($latest['data'] === ['hotel' => 7, 'rooms' => 9], json_encode($latest['data']));
    // PHP refuses a plain pop of a conflating group, so the check asks for conflation too.
    $again = $queen->queue('availability')->group('screen')->conflation()->wait(false)->pop();
    check(count($again) === 0, 'the older values were skipped, not delivered later');
    echo "ok: uc-latest\n";
}

// --- 6. Reminders and delayed jobs (timers) ---------------------------------
$statuses = [];
// guide:start(uc-reminder)
// When an order is delivered, ask for a review in 24 hours.
$timer = $queen->timers()->schedule('review-reminders', 'review-2001', 24 * 3600 * 1000,
    ['orderId' => 2001]);                           // $timer['status']: 'scheduled'
$statuses[] = $timer['status']; // guide:hide

// The delivery was late: schedule the same key again, and the reminder moves.
$timer = $queen->timers()->schedule('review-reminders', 'review-2001', 48 * 3600 * 1000,
    ['orderId' => 2001]);                           // $timer['status']: 'rescheduled'
$statuses[] = $timer['status']; // guide:hide

// The guest wrote a review already: no reminder at all.
$queen->timers()->cancel('review-reminders', 'review-2001');
// guide:end
{
    check($statuses === ['scheduled', 'rescheduled'], json_encode($statuses));
    $peek = $queen->timers()->peek('review-reminders', 'review-2001');
    check(($peek['found'] ?? false) === false || ($peek['status'] ?? null) !== 'scheduled',
        'the cancelled reminder is gone');
    // A timer fires as a real message of its queue.
    $queen->timers()->schedule('review-reminders', 'review-2002', 200, ['orderId' => 2002]);
    $fired = null;
    $queen->queue('review-reminders')->group('mailer')->subscriptionMode('all')->each()->limit(1)
        ->consume(function (array $m) use (&$fired): void {
            $fired = $m['data'];
        })
        ->execute();
    check($fired === ['orderId' => 2002], 'the timer fired as a message: ' . json_encode($fired));
    echo "ok: uc-reminder\n";
}

// --- 7. Request and reply between services (ephemeral queues) --------------
// guide:start(uc-reply)
// The pricing service answers quote requests, in memory.
$pricing = function () use ($queen): void {
    $messages = $queen->ephemeral()
        ->pop('quotes', ['group' => 'pricing', 'wait' => true])['messages'];
    foreach ($messages as $m) {
        $queen->ephemeral()->push($m['payload']['replyTo'],
            [['room' => $m['payload']['room'], 'price' => 129]]);
    }
    $queen->ephemeral()->ack('quotes', $messages, ['group' => 'pricing']);
};

// The booking page asks, and waits at most half a second for the answer.
$quote = function (string $room) use ($queen): array {
    $inbox = 'inbox-' . bin2hex(random_bytes(8));
    $queen->ephemeral()->push('quotes', [['room' => $room, 'replyTo' => $inbox]]);
    $reply = $queen->ephemeral()->pop($inbox,
        ['wait' => true, 'timeout' => 500, 'commitOnDelivery' => true]);
    // too slow: show "ask us"
    return $reply['messages'][0]['payload'] ?? ['room' => $room, 'price' => null];
};

if (($pid = pcntl_fork()) === 0) {                  // PHP has no threads: pricing runs
    $pricing();                                     // in a process of its own
    exit(0);
}
$deluxe = $quote('deluxe');
pcntl_waitpid($pid, $status);
print_r($deluxe);                                   // [room => deluxe, price => 129]
// guide:end
{
    check(pcntl_wexitstatus($status) === 0, 'the pricing process exited cleanly');
    check($deluxe === ['room' => 'deluxe', 'price' => 129], json_encode($deluxe));
    $t = microtime(true);
    $suite = $quote('suite');                       // nobody answers now
    check($suite === ['room' => 'suite', 'price' => null], json_encode($suite));
    check(microtime(true) - $t < 3, 'the caller gave up after its timeout');
    $left = $queen->ephemeral()->pop('quotes', ['group' => 'pricing', 'wait' => false]);
    $queen->ephemeral()->ack('quotes', $left['messages'], ['group' => 'pricing']);
    echo "ok: uc-reply\n";
}

// --- 8. Calling a webhook (retries, dead letters, idempotency key) ----------
// The receiver: PHP's built-in server on a free port, with this file as its router
// (the cli-server block at the top). Its state is two files in a temporary directory.
$hookState = sys_get_temp_dir() . '/book-hooks-' . bin2hex(random_bytes(6));
mkdir($hookState);
$free = stream_socket_server('tcp://127.0.0.1:0');
$port = (int) substr(strrchr(stream_socket_get_name($free, false), ':'), 1);
fclose($free);
$receiver = proc_open([PHP_BINARY, '-S', "127.0.0.1:{$port}", __FILE__],
    [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']],
    $pipes, null, ['BOOK_HOOK_STATE' => $hookState] + getenv());
for ($i = 0; $i < 50; $i++) {
    if ($probe = @fsockopen('127.0.0.1', $port)) {
        fclose($probe);
        break;
    }
    usleep(100_000);
}
$hookUrl = "http://127.0.0.1:{$port}/hooks";
$queen->queue('webhooks')->config(['retryLimit' => 5, 'retryDelay' => 1000])->create()->execute();
$queen->queue('webhooks')->partition('customer-42')->push([['transactionId' => 'evt-9001',
    'data' => ['url' => $hookUrl, 'event' => ['type' => 'booking.confirmed', 'id' => 9001]]]])
    ->execute();
// guide:start(uc-webhook)
// A deploy's SIGTERM stops the sender cleanly: consume() handles it itself.
$http = new GuzzleHttp\Client();

$queen->queue('webhooks')
    ->group('sender')
    ->subscriptionMode('all')
    ->each()
    ->limit(2) // guide:hide
    ->consume(function (array $msg) use ($http) {
        $res = $http->post($msg['data']['url'], [
            'headers' => ['idempotency-key' => $msg['transactionId']],
            'json' => $msg['data']['event'],
            'http_errors' => false,
        ]);
        if ($res->getStatusCode() >= 300) {         // retried, then dead-lettered
            throw new RuntimeException("HTTP {$res->getStatusCode()}");
        }
    })
    ->execute();
// guide:end
{
    proc_terminate($receiver);
    proc_close($receiver);
    $callsSeen = (int) @file_get_contents("{$hookState}/calls");
    $received = @file("{$hookState}/received", FILE_IGNORE_NEW_LINES) ?: [];
    array_map('unlink', glob("{$hookState}/*"));
    rmdir($hookState);
    check($callsSeen === 2, "the failed call was retried once ({$callsSeen} calls)");
    $keys = array_map(fn (string $line) => explode(' ', $line, 2)[0], $received);
    check($keys === ['evt-9001'], 'the receiver got the event under its key: ' . json_encode($received));
    $body = json_decode(explode(' ', $received[0], 2)[1] ?? 'null', true);
    check($body === ['type' => 'booking.confirmed', 'id' => 9001], 'with the event as its body');
    echo "ok: uc-webhook\n";
}

// --- 9. A job only one worker runs (lock + guard) ---------------------------
function nightlyWorker(Queen $queen): string
{
    // guide:start(uc-singleton)
    $lock = $queen->lock('nightly-report', ['ttlSeconds' => 30]);
    if (!$lock->acquire(1.0)) {                     // waits up to 1 s for it
        return 'skipped';                           // another worker has it
    }
    try {
        $report = ['day' => '2026-10-24', 'bookings' => 412];
        $result = $queen->transaction()
            ->guard($lock)                          // commits only while the lock is ours
            ->queue('daily-reports')
            ->push([['transactionId' => "report-{$report['day']}", 'data' => $report]])
            ->commit();
        if ($result['success'] === false) {
            return 'lost the lock';
        }
        usleep(1_500_000); // guide:hide
        return 'done';
    } finally {
        $lock->release();
    }
    // guide:end
}
{
    // Three workers started at once, as processes; each reports its outcome as its exit code.
    $outcomeCodes = ['done' => 10, 'skipped' => 11, 'lost the lock' => 12];
    $pids = [];
    foreach (['a', 'b', 'c'] as $name) {
        if (($pid = pcntl_fork()) === 0) {
            exit($outcomeCodes[nightlyWorker($queen)]);
        }
        $pids[] = $pid;
    }
    $outcomes = [];
    foreach ($pids as $pid) {
        pcntl_waitpid($pid, $status);
        $outcomes[] = array_search(pcntl_wexitstatus($status), $outcomeCodes, true) ?: 'crashed';
    }
    check(count(array_keys($outcomes, 'done', true)) === 1, 'one worker ran it: ' . json_encode($outcomes));
    $read = $queen->queue('daily-reports')->fetch(['Default']);
    check(count($read['Default']['records']) === 1, 'one report');
    echo 'ok: uc-singleton ', json_encode($outcomes), "\n";
}

$queen->close();
echo "all use cases checked\n";
