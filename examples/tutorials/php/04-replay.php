<?php // docs:start(tut-php-replay)
//
// Tutorial 4 of 4: replay.
//
// Acknowledging a message does not delete it. Consumption is a cursor per
// consumer group, and the messages stay until retention removes them, so a new
// group can read the whole history and an existing group can be moved back.
//
// This is the tutorial that shows what a cursor buys you: reprocessing after a
// bug, backfilling a new consumer, and auditing what was delivered, all without
// asking the producer to send anything twice.
//
// Run it:
//   QUEEN_URL=http://localhost:6632 php 04-replay.php

require __DIR__ . '/vendor/autoload.php';

use Queen\Queen;

$QUEEN_URL = getenv('QUEEN_URL') ?: 'http://localhost:6632';
$RUN = base_convert((string) (int) (microtime(true) * 1000), 10, 36);
$EVENTS = "tut-php-replay-{$RUN}";

$EVENTS_IN = [
    ['seq' => 1, 'type' => 'created'],
    ['seq' => 2, 'type' => 'updated'],
    ['seq' => 3, 'type' => 'shipped'],
    ['seq' => 4, 'type' => 'delivered'],
];

$checks = 0;
$assert = function (bool $condition, string $description) use (&$checks): void {
    if (!$condition) {
        throw new RuntimeException($description);
    }
    $checks++;
    echo "  ok: {$description}\n";
};

$queen = new Queen($QUEEN_URL);
$exitCode = 0;

// A closure captures by value at the point it is written, so the client has to
// exist before this helper does. It reads one lane to the end and returns the
// sequence numbers it saw, in arrival order.
$drain = function (string $group, int $expected, ?string $mode = null) use ($queen, $EVENTS): array {
    $seen = [];
    $builder = $queen
        ->queue($EVENTS)
        ->partition('order-1')
        ->group($group)
        ->each()
        ->limit($expected)
        ->idleMillis(4000);
    if ($mode !== null) {
        $builder->subscriptionMode($mode);
    }
    $builder
        ->consume(function (array $msg) use (&$seen): void {
            $seen[] = $msg['data']['seq'];
        })
        ->execute();

    return $seen;
};

try {
    echo "broker {$QUEEN_URL}\n";

    foreach ($EVENTS_IN as $event) {
        $queen->queue($EVENTS)->partition('order-1')->push([['data' => $event]])->execute();
    }
    echo 'pushed ' . count($EVENTS_IN) . " events\n";

    // The live consumer. It drains the lane and commits as it goes.
    echo "\nthe live consumer\n";
    $live = $drain('tut-php-live', 4, 'all');
    echo '  saw ' . implode(', ', $live) . "\n";
    $assert($live === [1, 2, 3, 4], 'the live group read the lane in order');

    // A second group, created now, after every message was already stored and
    // acknowledged by someone else. subscriptionMode('all') is what points its
    // new cursor at the beginning: the default for a new group is the tail, so
    // without it this group would sit idle waiting for the next event.
    //
    // The mode applies when the cursor is created and never again, so it cannot
    // rewind a group that already exists. That is what seek below is for.
    echo "\na new group, backfilled from the beginning\n";
    $audit = $drain('tut-php-audit', 4, 'all');
    echo '  saw ' . implode(', ', $audit) . "\n";
    $assert($audit === [1, 2, 3, 4], 'a new group replayed the whole history');

    // Nothing was re-pushed and nothing was copied: both groups read the same
    // stored messages through their own cursors.
    echo "\nrewinding an existing group\n";

    // Move the live group's cursor back an hour, which is before anything in this
    // run was pushed. The seek also releases any live lease, so an in-flight batch
    // is abandoned rather than acknowledged.
    //
    // admin() is a method on this client, not a property, and it hands back the
    // same Admin instance every time. The timestamp goes over the wire as a
    // string, so it has to be ISO 8601 in UTC and not a DateTime object.
    $anHourAgo = (new DateTimeImmutable('-1 hour', new DateTimeZone('UTC')))->format('Y-m-d\TH:i:s.v\Z');
    $queen->admin()->seekConsumerGroup('tut-php-live', $EVENTS, ['timestamp' => $anHourAgo]);

    $replayed = $drain('tut-php-live', 4);
    echo '  saw ' . implode(', ', $replayed) . "\n";
    $assert(
        $replayed === [1, 2, 3, 4],
        'the rewound group read the same events again, in the same order'
    );

    // Replay is per group. The audit group was not moved, so it stays where it
    // was and sees nothing new.
    $auditAgain = $queen
        ->queue($EVENTS)
        ->partition('order-1')
        ->group('tut-php-audit')
        ->batch(10)
        ->wait(false)
        ->pop();
    $assert(count($auditAgain) === 0, 'rewinding one group left the other where it was');

    $queen->queue($EVENTS)->delete()->execute();

    echo "\nPASS: {$checks} checks\n";
} catch (Throwable $error) {
    fwrite(STDERR, "\nFAIL: " . $error->getMessage() . "\n");
    $exitCode = 1;
} finally {
    $queen->close();
}

exit($exitCode);
// docs:end
