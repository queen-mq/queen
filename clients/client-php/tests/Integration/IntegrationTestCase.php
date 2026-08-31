<?php

namespace Queen\Tests\Integration;

use PHPUnit\Framework\TestCase;
use Queen\Exceptions\HttpException;
use Queen\Queen;

/**
 * Base for the tests that talk to a real broker.
 *
 * These are excluded from the default `Unit` suite in phpunit.xml and run by
 * test/runners/laravel against a live stack. Without QUEEN_HTTP_URL they skip,
 * so `vendor/bin/phpunit` on a laptop stays a pure unit run.
 *
 * THE NAMES ARE FIXED ON PURPOSE, AND THAT IS WHAT MAKES cleanupTestData
 * LOAD-BEARING. A run-unique suffix would hide a broken purge forever: with
 * stable keys, a purge that stops working turns this suite red on its SECOND
 * execution, which is the failure the cleanup exists to prevent. The concrete
 * shapes:
 *
 *   * a putIfAbsent test is green on a virgin database and red for ever after,
 *     because the key it expects to win with is already there;
 *   * an incr test accumulates across runs and starts failing the day the total
 *     crosses whatever ceiling it asserts.
 *
 * The purge runs at BOTH ends. In tearDown so the next run starts clean, and in
 * setUp because a run that crashed halfway never reached its tearDown, and a
 * suite that can only pass after a manual database wipe is a suite nobody
 * trusts. Each delete sits in its own try/catch: a key may never have been
 * written, a queue may never have been created, and neither is a reason to fail
 * a test that has not run yet.
 *
 * AND `forever` IS BANNED IN HERE. A test that puts a key with forever:true and
 * then fails before its cleanup leaves state that no TTL will ever collect, in
 * a database other suites share. Every key written by these tests carries a
 * ttlSeconds, so the worst case of a catastrophic failure is a database that
 * cleans itself up.
 */
abstract class IntegrationTestCase extends TestCase
{
    protected Queen $queen;

    /** Namespaces this suite owns. Everything it writes lives under one of them. */
    protected const NS_STATE = 'phpstate';
    protected const NS_COUNTER = 'phpcounter';

    /** Destination queue for every timer these tests schedule. */
    protected const TIMER_QUEUE = 'php-timers-test';

    /**
     * Every key the suite may have written, listed by hand.
     *
     * There is no deletePrefix and there is not going to be one: with a
     * mandatory TTL on every write, the sweeper does that job, and a
     * prefix-delete would be an unbounded write inside the outermost lock
     * space. So cleanup enumerates. The cost of that decision is this list, and
     * the rule that goes with it: a test that invents a key adds it here.
     */
    protected const KV_KEYS = [
        self::NS_STATE => [
            'marker', 'cas', 'fence', 'expiring', 'null-value', 'slashed/key/here',
            'txn-marker', 'txn-gate', 'batch-a', 'batch-b', 'prefix:a', 'prefix:b', 'prefix:c',
        ],
        self::NS_COUNTER => [
            'plain', 'capped', 'floored', 'window',
        ],
    ];

    /** Timer keys the suite may have scheduled on TIMER_QUEUE. */
    protected const TIMER_KEYS = [
        'soon', 'later', 'replaced', 'cancelled', 'listed-a', 'listed-b', 'txn-timer',
    ];

    protected function setUp(): void
    {
        $url = getenv('QUEEN_HTTP_URL') ?: getenv('QUEEN_URL');
        if (!$url) {
            $this->markTestSkipped('QUEEN_HTTP_URL is not set; integration tests need a live broker');
        }

        $this->queen = new Queen([
            'url' => $url,
            'timeoutMillis' => 15000,
            // One attempt: a test that needs three tries to reach a broker in
            // the same docker network is telling us something, and retries here
            // would turn a fast failure into a slow one.
            'retryAttempts' => 1,
        ]);

        $this->requireWritableTenant();
        $this->cleanupTestData();
    }

    protected function tearDown(): void
    {
        $this->cleanupTestData();
    }

    /**
     * THE ONE REMAINING SKIP, and the one that used to be here next to it.
     *
     * THE GONE ONE. This method also used to probe for a 404 and skip on it,
     * because KV and timers were opt-in per cell (`QUEEN_KV_ENABLED`,
     * `QUEEN_TIMERS_ENABLED`, both defaulting to off) and a cell that did not
     * serve the surface was a legitimate configuration. Both flags are gone
     * (2026-08-18): the surfaces are part of every broker, like push and pop.
     * So there is nothing to detect, the 404 has no producer left anywhere in
     * the broker, and this suite RUNS. A test that skips is a test that says
     * nothing, and that was only ever an acceptable trade while the absence was
     * a supported deployment.
     *
     * What did NOT go away is the operator's runtime kill switch, which answers
     * 503 `kv_disabled` / `timers_disabled`. That is deliberately NOT a skip: a
     * cell somebody has paused mid-incident is not a cell to run an integration
     * suite against, and a red suite is the correct report.
     *
     * THE ONE THAT STAYS, because it is about tenancy and not about the surface
     * existing. 403 `feature_gated` on a WRITE with reads still working means
     * the cell runs with the tenant header on, and there the ABSENCE of a grant
     * row is a denial and not a permission (§9.4): the header is opaque and
     * validated against nothing, so a client rotating it per request would
     * otherwise mint a fresh unlimited tenant on every call. Granting is a
     * control-plane act with no client-side API, so this suite cannot arrange
     * it. This is exactly what the `tenanted` topology of test/run.sh does, and
     * it is why that lane skips the integration tree while `single` runs it.
     *
     * The probe is a DELETE of a key the purge is about to remove anyway: it
     * passes through the same write ladder as a put — the grant rung does not
     * care that a delete adds no rows — and it cannot leave anything behind.
     */
    private function requireWritableTenant(): void
    {
        try {
            $this->queen->kv()->delete(self::NS_STATE, 'marker');
        } catch (HttpException $e) {
            if ($e->statusCode === 403) {
                $this->markTestSkipped(
                    'KV writes answer 403 ' . ($e->reason ?? $e->getMessage())
                    . ': this cell runs with the tenant header on, where a tenant with no '
                    . 'queen.kv_quota grant may read and may not write. Provisioning a grant is a '
                    . 'control-plane act with no client API.'
                );
            }
            throw $e;
        }
    }

    /**
     * Purge everything this suite can have created. Each step in its own
     * try/catch, because "it was never there" and "it is there and would not
     * go" must not be the same outcome for a cleanup.
     */
    protected function cleanupTestData(): void
    {
        foreach (self::KV_KEYS as $namespace => $keys) {
            foreach ($keys as $key) {
                try {
                    $this->queen->kv()->delete($namespace, $key);
                } catch (\Throwable $e) {
                    // The key may never have existed, the surface may be paused
                    // by an operator. Neither is a test failure HERE: the setUp
                    // probe is what decides whether the suite can run at all.
                }
            }
        }

        foreach (self::TIMER_KEYS as $key) {
            try {
                // The route that is never blocked, which is also the route that
                // works when the destination queue was never created.
                $this->queen->timers()->cancel(self::TIMER_QUEUE, $key);
            } catch (\Throwable $e) {
            }
        }

        try {
            // Fired timers become real messages: dropping the queue removes
            // them and the partition the next run would otherwise inherit.
            $this->queen->queue(self::TIMER_QUEUE)->delete()->execute();
        } catch (\Throwable $e) {
        }
    }

    /**
     * Pop from a test queue, seeing everything in it.
     *
     * `subscriptionMode('all')` is not decoration and it is not a workaround.
     * The product's default for a NEW consumer group is `new`: the group starts
     * at the moment it is created and never sees what was written before. Every
     * delivery assertion in this suite writes first (a timer fires, a bundle
     * pushes) and reads afterwards with a group that did not exist yet, so the
     * default would make the assertion depend on whether an earlier test had
     * already created the group — green in a full run, red when run alone, and
     * the failure would read as "the timer never arrived".
     */
    protected function popAll(string $queue): array
    {
        return $this->queen->queue($queue)->subscriptionMode('all')->pop();
    }

    /**
     * Poll until $probe returns a non-empty result, or give up.
     *
     * Timers are "not before", never "exactly at": a delivery lands within a
     * sweep cycle of its delay, so an integration test asserts that it ARRIVES
     * and not that it arrives at an instant. Anything sharper would be a flaky
     * test asserting a guarantee the product does not make.
     *
     * WHY THE BUDGET IS A MINUTE AND NOT A FEW SECONDS, measured rather than
     * guessed. The FIRE is prompt — the broker logs `sweeper: swept fired=1`
     * about 250 ms after the delay elapses, and the message is in the queue
     * immediately afterwards (`messages.pending: 1` on the queue listing). What
     * is not prompt is the first POP of a queue whose hot-list ring does not
     * exist yet: on a cold stack that first delivery took roughly thirty
     * seconds to become poppable, in line with the reseed cadence the broker
     * reports (`hotlist: reseed ... per_s="0.03"`). Every later delivery into
     * the same queue arrives in well under a second.
     *
     * So a 15 s budget fails exactly one test — the first one — and looks like
     * "the timer never arrived" when the timer had arrived and only the reader
     * had not caught up. Do not tighten this back down without re-measuring on
     * a cold stack.
     */
    protected function waitFor(callable $probe, int $timeoutMillis = 60000, int $everyMillis = 250): mixed
    {
        $deadline = microtime(true) + $timeoutMillis / 1000;

        do {
            $result = $probe();
            if ($result !== null && $result !== [] && $result !== false) {
                return $result;
            }
            usleep($everyMillis * 1000);
        } while (microtime(true) < $deadline);

        return null;
    }
}
