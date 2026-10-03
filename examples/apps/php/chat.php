<?php // docs:start(app-php-chat)
//
// A chat backend: one ordered partition per conversation.
//
// Queen started as the broker of a hotel messaging product. Some conversations
// need a translation or an agent before their next message can be handled, and
// on a hashed Kafka topic one slow conversation held up every conversation that
// shared its partition. Here every conversation is a partition of its own,
// created by the first message sent to it, so a slow conversation waits on
// itself and on nothing else.
//
//   chat-messages (one partition per conversation)
//     ├── group "delivery"    marks each message delivered, fast
//     ├── group "enrichment"  translates the Japanese conversation, slow
//     └── group "sentiment"   added later, reads the whole history
//
// The program checks what the design promises: every message reaches each
// group once and in the order of its conversation, and the English
// conversations finish while the Japanese one is still being translated.
//
// Run it:
//   QUEEN_URL=http://localhost:6632 php chat.php
//
// Needs pcntl, which php-cli ships with on Linux and macOS: the enrichment
// pool below is three processes, because that is what a PHP worker pool is.

require __DIR__ . '/vendor/autoload.php';

use Queen\Queen;

$QUEEN_URL = getenv('QUEEN_URL') ?: 'http://localhost:6632';
// A fresh queue per run, so two runs never read each other's messages.
$RUN = base_convert((string) (int) (microtime(true) * 1000), 10, 36);
$MESSAGES = "app-php-chat-{$RUN}";

// Three conversations. The Japanese one needs a translation pass, 400 ms a
// message against 10 ms for the others. It is listed first, so its messages are
// the oldest in the queue and its partition is usually handed out first: a
// consumer that let one conversation hold up another would fail the timing
// check below.
$CONVERSATIONS = [
    'conv-jp-1' => ['locale' => 'jp', 'needsTranslation' => true],
    'conv-en-1' => ['locale' => 'en', 'needsTranslation' => false],
    'conv-en-2' => ['locale' => 'en', 'needsTranslation' => false],
];
$MESSAGES_PER_CONVERSATION = 6;
$WORKERS = 3;

$checks = 0;
$assert = function (bool $condition, string $description) use (&$checks): void {
    if (!$condition) {
        throw new RuntimeException($description);
    }
    $checks++;
    echo "  ok: {$description}\n";
};

// PHP sleeps in microseconds, and a handler that sleeps blocks its whole
// process: that is the one fact the enrichment section below is built around.
$sleepMillis = fn(int $millis) => usleep($millis * 1000);

// There is no signal-handling option to turn off on this client: it installs
// SIGINT and SIGTERM handlers only for the duration of a consume loop and
// restores the previous ones when the loop returns, so this script keeps
// control of its own shutdown.
$queen = new Queen($QUEEN_URL);
$exitCode = 0;

try {
    echo "broker {$QUEEN_URL}\n";

    // Checked before anything is created, so a build without pcntl fails here
    // and leaves no queue behind.
    if (!function_exists('pcntl_fork')) {
        throw new RuntimeException('this example forks its worker pool and needs the pcntl extension');
    }

    // A crashed worker's messages come back when its lease expires, and
    // retryLimit bounds how often a failing message is retried before it goes to
    // the dead-letter queue.
    $queen->queue($MESSAGES)->config(['leaseTime' => 60, 'retryLimit' => 3])->create()->execute();

    // ------------------------------------------------------------------ sending
    //
    // Sending a message is one push into the conversation's partition. Nothing
    // was declared for the conversation beforehand, and nothing has to be
    // cleaned up when it goes quiet.
    echo "\nsending\n";
    $sent = 0;
    for ($seq = 1; $seq <= $MESSAGES_PER_CONVERSATION; $seq++) {
        foreach ($CONVERSATIONS as $conversationId => $meta) {
            $message = [
                'conversationId' => $conversationId,
                'seq' => $seq,
                'locale' => $meta['locale'],
                'body' => "message {$seq} in {$conversationId}",
                'sentAt' => (int) (microtime(true) * 1000),
            ];
            // The phone's own id for the message. A phone that retries a send
            // it never saw answered writes nothing the second time, and gets
            // the first message's id back.
            $queen->queue($MESSAGES)->partition($conversationId)->push([[
                'transactionId' => "{$conversationId}-{$seq}",
                'data' => $message,
            ]])->execute();
            $sent++;
        }
    }
    echo "  {$sent} messages across " . count($CONVERSATIONS) . " conversations\n";

    // The phone resends message 1 because the answer got lost on a bad network.
    // execute() hands back one result per item pushed, and the result carries
    // the verdict: this client does not throw on a duplicate.
    $resend = $queen->queue($MESSAGES)->partition('conv-en-1')->push([[
        'transactionId' => 'conv-en-1-1',
        'data' => ['conversationId' => 'conv-en-1', 'seq' => 1, 'body' => 'resent by the phone'],
    ]])->execute();
    $assert($resend[0]['status'] === 'duplicate', 'a resent message was recognised and not stored twice');

    // --------------------------------------------------------------- delivering
    //
    // Marking messages delivered is fast work and must never wait behind slow
    // work, so it is a consumer group of its own, with its own cursor.
    //
    // concurrency(3) on this client is three long polls in flight at once on
    // one cURL multi-handle. The polls overlap, and the handlers run one after
    // another in this process. partitions(1) makes every pop take ONE
    // conversation: by default a pop may sweep up several ready conversations,
    // and a worker handles the messages of one pop in order, so a slow
    // conversation would delay the others that came with it. Long polls end
    // after a second and a worker stops after two quiet seconds, which is what
    // lets this program finish; a service calls consume() without those two
    // lines and runs until stopped.
    echo "\ndelivering\n";
    $delivered = [];
    $queen
        ->queue($MESSAGES)
        ->group('delivery')
        ->subscriptionMode('all') // a group created after the messages starts at the tail otherwise
        ->concurrency(3)
        ->partitions(1)
        ->each()
        ->timeoutMillis(1000)
        ->idleMillis(2000)
        ->consume(function (array $msg) use (&$delivered, $sleepMillis): void {
            $sleepMillis(10);
            $delivered[$msg['data']['conversationId']][] = $msg['data']['seq'];
        })
        ->execute();

    $deliveredCount = array_sum(array_map('count', $delivered));
    $assert($deliveredCount === $sent, "delivery saw all {$sent} messages once (got {$deliveredCount})");
    foreach ($delivered as $conversationId => $seqs) {
        $assert(
            $seqs === range(1, count($seqs)),
            "{$conversationId} was delivered in order: " . implode(',', $seqs)
        );
    }

    // --------------------------------------------------------------- enrichment
    //
    // The slow group reads the same messages through its own cursor. On a topic
    // with a few hashed partitions, the Japanese conversation would sit in a
    // partition shared with English ones and hold them up. Here each worker
    // holds one conversation at a time, so the English conversations finish
    // while the Japanese one is still being translated.
    //
    // A worker pool in PHP is processes. There is no event loop to interleave a
    // sleeping handler with a running one, so three overlapping polls in one
    // process would still translate and deliver strictly one after another, and
    // the clock would say nothing about partitions. Forking is what a PHP
    // deployment actually does, a queue:work fleet of three, and it is what
    // makes the measurement mean something.
    echo "\nenriching\n";
    $startedAt = microtime(true);
    $reportPaths = [];
    $children = [];

    for ($slot = 0; $slot < $WORKERS; $slot++) {
        $reportPath = sys_get_temp_dir() . "/app-php-chat-{$RUN}-{$slot}.json";
        $reportPaths[$slot] = $reportPath;

        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new RuntimeException('could not fork an enrichment worker');
        }

        if ($pid === 0) {
            // Child. It builds its own client: the parent's cURL handles and
            // sockets are shared across the fork, and two processes taking turns
            // on one connection corrupt each other's replies.
            $childCode = 0;
            try {
                $worker = new Queen($QUEEN_URL);
                $finishedAt = [];
                $enriched = 0;

                $worker
                    ->queue($MESSAGES)
                    ->group('enrichment')
                    ->subscriptionMode('all')
                    // One conversation per pop, as above. Without it the first
                    // process to poll can take all three conversations and
                    // translate the Japanese one before the English ones.
                    ->partitions(1)
                    ->each()
                    ->timeoutMillis(1000)
                    ->idleMillis(2000)
                    ->consume(function (array $msg) use (
                        $CONVERSATIONS, $startedAt, $sleepMillis, &$finishedAt, &$enriched
                    ): void {
                        $conversationId = $msg['data']['conversationId'];
                        $sleepMillis($CONVERSATIONS[$conversationId]['needsTranslation'] ? 400 : 10);
                        $finishedAt[$conversationId] = (int) round((microtime(true) - $startedAt) * 1000);
                        $enriched++;
                    })
                    ->execute();

                file_put_contents($reportPath, json_encode([
                    'finishedAt' => $finishedAt,
                    'enriched' => $enriched,
                ]));
                $worker->close();
            } catch (Throwable $error) {
                file_put_contents($reportPath, json_encode(['error' => $error->getMessage()]));
                $childCode = 1;
            }
            // exit, and never return: a child that fell through would run the
            // parent's remaining checks and delete the queue underneath it.
            exit($childCode);
        }

        $children[] = $pid;
    }

    // The parent only waits. Every worker reports the elapsed milliseconds at
    // which it last touched each conversation, and the merge keeps the latest of
    // those, which is when that conversation was finished with.
    $finishedAt = [];
    $enriched = 0;
    foreach ($children as $slot => $pid) {
        pcntl_waitpid($pid, $status);
        $report = json_decode((string) @file_get_contents($reportPaths[$slot]), true) ?: [];
        @unlink($reportPaths[$slot]);
        if (isset($report['error'])) {
            throw new RuntimeException("enrichment worker {$slot} failed: {$report['error']}");
        }
        if (!pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
            throw new RuntimeException("enrichment worker {$slot} did not exit cleanly");
        }
        foreach ($report['finishedAt'] ?? [] as $conversationId => $millis) {
            $finishedAt[$conversationId] = max($finishedAt[$conversationId] ?? 0, $millis);
        }
        $enriched += $report['enriched'] ?? 0;
    }

    $assert($enriched === $sent, "the three processes enriched all {$sent} messages once (got {$enriched})");

    $slow = $finishedAt['conv-jp-1'] ?? 0;
    $fast = max($finishedAt['conv-en-1'] ?? 0, $finishedAt['conv-en-2'] ?? 0);
    echo "  english done after {$fast} ms, japanese after {$slow} ms\n";

    $assert(
        $fast < $slow,
        'the English conversations finished while the Japanese one was still being translated'
    );
    $assert(
        $slow >= $MESSAGES_PER_CONVERSATION * 400,
        'the Japanese conversation really took its six translations'
    );

    // ----------------------------------------------------------------- backfill
    //
    // A feature added later, sentiment scoring, wants every message ever sent.
    // It is one more consumer group starting from the oldest message: no
    // producer change, and no second copy of the data.
    echo "\nbackfilling a new group\n";
    $scored = 0;
    $queen
        ->queue($MESSAGES)
        ->group('sentiment')
        ->subscriptionMode('all')
        ->concurrency(3)
        ->partitions(1)
        ->each()
        ->timeoutMillis(1000)
        ->idleMillis(2000)
        ->consume(function (array $msg) use (&$scored): void {
            $scored++;
        })
        ->execute();

    $assert($scored === $sent, "a group created now read the whole history ({$scored} messages)");

    $queen->queue($MESSAGES)->delete()->execute();

    echo "\nPASS: {$checks} checks\n";
} catch (Throwable $error) {
    fwrite(STDERR, "\nFAIL: " . $error->getMessage() . "\n");
    $exitCode = 1;
} finally {
    $queen->close();
}

exit($exitCode);
// docs:end
