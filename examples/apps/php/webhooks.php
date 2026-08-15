<?php // docs:start(app-php-webhooks)
//
// A webhook delivery system.
//
// Every SaaS product ends up writing this one, and it is harder than it looks:
// deliveries to one customer's endpoint must arrive in order, a customer whose
// endpoint is down must not slow down anybody else's, failures must be retried
// a bounded number of times, and what never succeeds has to end up somewhere a
// human can look at.
//
// The shape here is one ordered lane per destination, created by the first
// delivery to it. A dead endpoint backs up its own lane and no other; retries
// are the broker's retry budget rather than a loop in your code; and what
// exhausts the budget lands in the dead-letter queue with the error attached.
//
//   webhook-deliveries (one partition per destination)
//     └── group "sender"  posts each delivery, fails on a dead endpoint
//           └── retryLimit exhausted -> dead-letter queue
//
// Run it:
//   QUEEN_URL=http://localhost:6632 php webhooks.php

require __DIR__ . '/vendor/autoload.php';

use Queen\Queen;

$QUEEN_URL = getenv('QUEEN_URL') ?: 'http://localhost:6632';
$RUN = base_convert((string) (int) (microtime(true) * 1000), 10, 36);
$DELIVERIES = "app-php-webhooks-{$RUN}";
$GROUP = 'sender';

// Three subscribers. One of them has let its certificate expire, which is the
// most common way a webhook endpoint dies: it answers, but it answers 500.
$ENDPOINTS = [
    'acme.example' => ['healthy' => true],
    'globex.example' => ['healthy' => true],
    'initech.example' => ['healthy' => false],
];
$EVENTS_PER_ENDPOINT = 3;
$RETRY_LIMIT = 2;

$checks = 0;
$assert = function (bool $condition, string $description) use (&$checks): void {
    if (!$condition) {
        throw new RuntimeException($description);
    }
    $checks++;
    echo "  ok: {$description}\n";
};

// Stands in for the HTTP POST to the subscriber. A real sender would use Guzzle
// and treat any non-2xx as a failure, which is exactly what throwing does here.
$postToEndpoint = function (string $endpoint, array $event) use ($ENDPOINTS): array {
    if (!$ENDPOINTS[$endpoint]['healthy']) {
        throw new RuntimeException("{$endpoint} answered 500");
    }
    return ['status' => 200];
};

$queen = new Queen($QUEEN_URL);
$exitCode = 0;

try {
    echo "broker {$QUEEN_URL}\n";

    // retryLimit is the delivery budget, and dlqAfterMaxRetries is what happens
    // when it runs out. Without the second flag an exhausted message is simply
    // marked failed and stays put; with it, the broker moves it to the
    // dead-letter table with the last error on the row.
    //
    // leaseTime is the other half of the contract: it is how long the broker
    // waits for a sender that took a delivery and never came back before handing
    // that delivery to someone else.
    //
    // config() fills in the queue defaults around whatever you name here, so the
    // three keys below are the whole configuration decision.
    $queen->queue($DELIVERIES)->config([
        'leaseTime' => 30,
        'retryLimit' => $RETRY_LIMIT,
        'dlqAfterMaxRetries' => true,
    ])->create()->execute();

    // ------------------------------------------------------------------ queuing
    //
    // The application emits events. Each one goes into the partition of the
    // endpoint it is destined for, which is what makes "in order per subscriber"
    // a property of the storage rather than of the sender.
    echo "\nqueuing deliveries\n";
    for ($seq = 1; $seq <= $EVENTS_PER_ENDPOINT; $seq++) {
        foreach (array_keys($ENDPOINTS) as $endpoint) {
            $queen->queue($DELIVERIES)->partition($endpoint)->push([[
                // The event id makes the enqueue idempotent: an application that
                // retries its own emit does not create a second delivery.
                'transactionId' => "{$endpoint}-evt-{$seq}",
                'data' => [
                    'endpoint' => $endpoint,
                    'seq' => $seq,
                    'type' => 'invoice.paid',
                    'invoiceId' => "INV-{$seq}",
                ],
            ]])->execute();
        }
    }
    echo '  ' . ($EVENTS_PER_ENDPOINT * count($ENDPOINTS)) . " deliveries queued\n";

    // ------------------------------------------------------------------ sending
    //
    // The sender pool. concurrency(3) is three long polls in flight at once on
    // one cURL multi-handle rather than three threads, and each poll claims a
    // partition of its own, so the three destinations are drained side by side.
    //
    // autoAck is off, and that is a deliberate choice rather than a formality.
    // The automatic path in this client nacks a throwing handler with a status
    // and nothing else: the delivery would be retried and eventually dead-
    // lettered exactly the same way, but the row a support engineer opens would
    // have an empty error. Acknowledging by hand is what puts the reason on it.
    // Everything else about retrying is unchanged: the budget is the broker's,
    // and it survives this process dying mid-flight, which a loop inside the
    // handler would not.
    //
    // timeoutMillis(1000) caps how long one poll parks on the broker. A round
    // ends only when every worker's poll has come back, so with the 30 s default
    // the last round of a drained queue would sit there for half a minute before
    // the idle bound could fire.
    echo "\nsending\n";
    $deliveredTo = [];
    $attempts = [];

    $queen
        ->queue($DELIVERIES)
        ->group($GROUP)
        ->subscriptionMode('all')
        ->concurrency(3)
        ->each()
        ->autoAck(false)
        // Enough turns for every good delivery plus every attempt at the bad
        // ones. This client spreads the bound over the pool, so each of the
        // three workers stops after a third of it, and the thirds add up to the
        // same total: no delivery is left without a worker allowed to take it.
        ->limit($EVENTS_PER_ENDPOINT * 2 + $EVENTS_PER_ENDPOINT * ($RETRY_LIMIT + 1))
        ->idleMillis(6000)
        ->timeoutMillis(1000)
        ->consume(function (array $msg) use ($queen, $postToEndpoint, $GROUP, &$deliveredTo, &$attempts): void {
            $endpoint = $msg['data']['endpoint'];
            $seq = $msg['data']['seq'];

            // A popped message here carries its payload, its ids and its lease,
            // and no attempt counter, so a sender that wants to back off, or to
            // give up early on an error it knows is permanent, counts its own
            // attempts. That is what this map is.
            $attempts[$endpoint] = ($attempts[$endpoint] ?? 0) + 1;

            try {
                $postToEndpoint($endpoint, $msg['data']);
            } catch (Throwable $failure) {
                // The nack spends one unit of the retry budget, and the error
                // travels with it: it is what the broker writes on the row when
                // the budget finally runs out.
                $nack = $queen->ack($msg, 'failed', ['group' => $GROUP, 'error' => $failure->getMessage()]);
                if (($nack[0]['success'] ?? false) !== true) {
                    throw new RuntimeException("the broker refused the nack for {$endpoint}/{$seq}");
                }
                echo "  {$endpoint} <- event {$seq} failed: {$failure->getMessage()}\n";
                return;
            }

            // Delivered. The ack names the consumer group explicitly: an ack sent
            // without it commits the queue's own cursor instead of this group's.
            // The reply arrives under an envelope whose outer success flag is set
            // before the broker is even read, so the numbered row beneath it is
            // the only proof the acknowledgement was taken.
            $ack = $queen->ack($msg, 'completed', ['group' => $GROUP]);
            if (($ack[0]['success'] ?? false) !== true) {
                throw new RuntimeException("the broker refused the ack for {$endpoint}/{$seq}");
            }

            $deliveredTo[$endpoint][] = $seq;
            echo "  {$endpoint} <- event {$seq}\n";
        })
        ->execute();

    // ------------------------------------------------------------------ checking
    echo "\nchecking\n";

    foreach ($ENDPOINTS as $endpoint => $meta) {
        if (!$meta['healthy']) {
            continue;
        }
        $seqs = $deliveredTo[$endpoint] ?? [];
        $assert(count($seqs) === $EVENTS_PER_ENDPOINT, "{$endpoint} received all {$EVENTS_PER_ENDPOINT} events");
        $assert($seqs === [1, 2, 3], "{$endpoint} received them in the order they happened");
    }

    $assert(
        count($deliveredTo['initech.example'] ?? []) === 0,
        'the dead endpoint received nothing, as it should'
    );
    $assert(
        ($attempts['initech.example'] ?? 0) > $EVENTS_PER_ENDPOINT,
        'the dead endpoint was retried rather than dropped on the first failure'
    );

    // The dead-letter queue is a table you can read, not a log line. Each row
    // carries the payload, the endpoint it was for, and the last error, which is
    // what a support engineer needs to answer "why did this customer not get it".
    $dlq = $queen->queue($DELIVERIES)->dlq()->limit(50)->get();
    $messages = $dlq['messages'] ?? [];
    $dead = array_values(array_filter($messages, fn(array $m): bool => $m['data']['endpoint'] === 'initech.example'));

    $assert(count($dead) === $EVENTS_PER_ENDPOINT, "all {$EVENTS_PER_ENDPOINT} dead deliveries are in the dead-letter queue");
    $assert(
        count(array_filter($dead, fn(array $m): bool => str_contains($m['errorMessage'] ?? '', 'answered 500'))) === count($dead),
        'each dead-letter row carries the error that killed it'
    );
    $assert(
        count(array_filter($messages, fn(array $m): bool => $m['data']['endpoint'] === 'initech.example')) === count($messages),
        'no healthy endpoint put anything in the dead-letter queue'
    );

    echo "\n  dead letters: " . implode(', ', array_map(
        fn(array $m): string => "{$m['data']['endpoint']}/{$m['data']['invoiceId']}",
        $dead
    )) . "\n";

    $queen->queue($DELIVERIES)->delete()->execute();

    echo "\nPASS: {$checks} checks\n";
} catch (Throwable $error) {
    fwrite(STDERR, "\nFAIL: " . $error->getMessage() . "\n");
    $exitCode = 1;
} finally {
    $queen->close();
}

exit($exitCode);
// docs:end
