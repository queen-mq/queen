<?php // docs:start(tut-php-transaction-ack-push)
//
// Tutorial 3 of 4: acknowledge and push in one transaction.
//
// Tutorial 2 handed work from one queue to the next in two steps: push the
// derived message, then let the loop acknowledge the source. Between those two
// steps a crash duplicates work, and in the other order it loses work.
//
// A Queen transaction closes that window: the acknowledgement of the input and
// the push of the output are one PostgreSQL transaction. Both land or neither
// does.
//
// Run it:
//   QUEEN_URL=http://localhost:6632 php 03-transaction-ack-push.php

require __DIR__ . '/vendor/autoload.php';

use Queen\Queen;

$QUEEN_URL = getenv('QUEEN_URL') ?: 'http://localhost:6632';
$RUN = base_convert((string) (int) (microtime(true) * 1000), 10, 36);
$ORDERS = "tut-php-tx-orders-{$RUN}";
$INVOICES = "tut-php-tx-invoices-{$RUN}";
$GROUP = 'tut-php-invoicing';

$INPUT = [
    ['orderId' => 'A-1', 'customer' => 'acme', 'total' => 120.5],
    ['orderId' => 'B-1', 'customer' => 'globex', 'total' => 88.75],
    ['orderId' => 'C-1', 'customer' => 'initech', 'total' => 310.0],
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

try {
    echo "broker {$QUEEN_URL}\n";

    foreach ($INPUT as $order) {
        $queen->queue($ORDERS)->partition($order['customer'])->push([['data' => $order]])->execute();
    }
    echo 'pushed ' . count($INPUT) . " orders\n";

    echo "\ninvoicing\n";
    $invoiced = [];

    // autoAck(false) is what makes this tutorial possible: the loop must not
    // acknowledge behind your back, because the acknowledgement is part of the
    // transaction below.
    $queen
        ->queue($ORDERS)
        ->group($GROUP)
        ->subscriptionMode('all')
        ->each()
        ->autoAck(false)
        ->limit(count($INPUT))
        ->idleMillis(5000)
        ->consume(function (array $msg) use ($queen, $INVOICES, $GROUP, &$invoiced): void {
            // One commit carries both operations. The ack names the consumer group
            // explicitly: the transaction builder does not read it off the message,
            // and an ack sent without it commits the wrong cursor.
            $result = $queen
                ->transaction()
                ->queue($INVOICES)
                ->partition($msg['data']['customer'])
                ->push([[
                    'data' => [
                        'invoiceId' => "INV-{$msg['data']['orderId']}",
                        'orderId' => $msg['data']['orderId'],
                        'amount' => $msg['data']['total'],
                    ],
                ]])
                ->ack($msg, 'completed', ['consumerGroup' => $GROUP])
                ->commit();

            // Check the transaction, not just the absence of an exception.
            // commit() raises a RuntimeException when the broker refuses, so
            // reaching this line already means both operations landed; the
            // explicit check is what puts that contract in the code.
            if (!($result['success'] ?? false)) {
                throw new RuntimeException('transaction rejected: ' . ($result['error'] ?? 'unknown'));
            }

            $invoiced[] = $msg['data']['orderId'];
            echo "  {$msg['data']['orderId']} -> INV-{$msg['data']['orderId']}\n";
        })
        ->execute();

    $assert(count($invoiced) === count($INPUT), 'every order was invoiced once');

    // The commit fails if the lease has expired, which is what stops a slow
    // consumer from acking work the broker has already handed to someone else.
    // Nothing to assert here: the check above is that assertion, since a failed
    // commit would have thrown.

    echo "\nchecking the output queue\n";

    // The invoices went to one partition per customer, and a pop claims a single
    // partition unless you say otherwise: partitions(10) lets this one call claim
    // up to ten of them, with batch as the total budget across all of them.
    $invoices = $queen
        ->queue($INVOICES)
        ->batch(10)
        ->partitions(10)
        ->wait(true)
        ->pop();

    $assert(count($invoices) === count($INPUT), count($INPUT) . ' invoices exist');

    $ids = array_map(fn(array $m): string => $m['data']['orderId'], $invoices);
    sort($ids);
    $expected = array_column($INPUT, 'orderId');
    sort($expected);
    $assert($ids === $expected, 'each invoice matches an order, none duplicated');

    // And the input queue is committed for this group: the acks were part of the
    // same transactions that produced those invoices, so the two states cannot
    // disagree.
    $leftovers = $queen->queue($ORDERS)->group($GROUP)->batch(10)->wait(false)->pop();
    $assert(count($leftovers) === 0, 'the source queue is committed for this group');

    $queen->queue($ORDERS)->delete()->execute();
    $queen->queue($INVOICES)->delete()->execute();

    echo "\nPASS: {$checks} checks\n";
} catch (Throwable $error) {
    fwrite(STDERR, "\nFAIL: " . $error->getMessage() . "\n");
    $exitCode = 1;
} finally {
    $queen->close();
}

exit($exitCode);
// docs:end
