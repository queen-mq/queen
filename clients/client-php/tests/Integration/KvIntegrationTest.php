<?php

namespace Queen\Tests\Integration;

use Queen\Exceptions\HttpException;
use Queen\Support\KvOp;

/**
 * The KV surface against a live broker.
 *
 * Every write here carries a ttlSeconds. `forever` is banned in this suite —
 * see IntegrationTestCase.
 */
class KvIntegrationTest extends IntegrationTestCase
{
    public function testGetOnAnAbsentKeyIsFoundFalseAndNotAnError(): void
    {
        $result = $this->queen->kv()->get(self::NS_STATE, 'marker');

        $this->assertFalse($result['found']);
        $this->assertSame('marker', $result['key']);
        $this->assertArrayNotHasKey('value', $result);
    }

    public function testPutThenGetRoundTripsValueAndVersion(): void
    {
        $kv = $this->queen->kv();

        $put = $kv->put(self::NS_STATE, 'marker', ['step' => 'reserved', 'n' => 7], ['ttlSeconds' => 300]);
        $this->assertTrue($put['applied']);
        $this->assertGreaterThan(0, $put['version']);

        $got = $kv->get(self::NS_STATE, 'marker');
        $this->assertTrue($got['found']);
        // assertEquals and not assertSame: the value is stored as JSONB, which
        // normalizes object key order, so `{"step":…,"n":…}` comes back as
        // `{"n":…,"step":…}`. Worth knowing before somebody hashes a KV value
        // and wonders why the digest moved.
        $this->assertEquals(['step' => 'reserved', 'n' => 7], $got['value']);
        $this->assertSame($put['version'], $got['version']);
        $this->assertNotNull($got['expiresAt']);

        // THE VERSION IS AN OPAQUE MONOTONIC TOKEN, NOT A COUNT OF WRITES ON
        // THIS KEY. It comes from one sequence shared by every key, cached per
        // connection, so consecutive writes to the same key jump by whatever
        // other traffic drew from the sequence — here, by about a thousand.
        // It is strictly greater, and that is the entire contract. Anyone who
        // writes `expect => $version + 1` has invented a number that will
        // essentially never be the next one.
        $second = $kv->put(self::NS_STATE, 'marker', ['step' => 'charged'], ['ttlSeconds' => 300]);
        $this->assertTrue($second['applied']);
        $this->assertGreaterThan($put['version'], $second['version']);
    }

    /**
     * `null` is a value, not an absence. Collapsing the two would make an
     * idempotency marker whose payload is null read as "never written".
     */
    public function testNullIsAValueAndNotAnAbsence(): void
    {
        $kv = $this->queen->kv();
        $kv->put(self::NS_STATE, 'null-value', null, ['ttlSeconds' => 300]);

        $got = $kv->get(self::NS_STATE, 'null-value');

        $this->assertTrue($got['found']);
        $this->assertNull($got['value']);
        $this->assertArrayHasKey('value', $got);
    }

    /**
     * The idempotency marker, which is use case number one. Exactly one caller
     * wins, and the loser gets the winner's value and version without a second
     * round trip — that is the whole point.
     *
     * THIS IS THE TEST THAT GOES RED FOREVER WITHOUT cleanupTestData: the
     * second run finds the key already there and loses the race it expects to
     * win.
     */
    public function testExactlyOnePutIfAbsentWins(): void
    {
        $kv = $this->queen->kv();

        $first = $kv->putIfAbsent(self::NS_STATE, 'marker', ['owner' => 'w1'], ['ttlSeconds' => 300]);
        $second = $kv->putIfAbsent(self::NS_STATE, 'marker', ['owner' => 'w2'], ['ttlSeconds' => 300]);

        $this->assertTrue($first['applied']);
        $this->assertFalse($second['applied']);
        $this->assertSame('exists', $second['reason']);
        $this->assertSame(['owner' => 'w1'], $second['value']);
        $this->assertSame($first['version'], $second['version']);
    }

    /**
     * expect:N is a PURE update and must never create. The naive shape falls
     * into the INSERT arm on an absent key — in a saga that means firing the
     * compensation the precondition existed to prevent.
     */
    public function testExpectOnAnAbsentKeyRefusesInsteadOfCreating(): void
    {
        $kv = $this->queen->kv();

        $result = $kv->put(self::NS_STATE, 'cas', ['v' => 1], ['ttlSeconds' => 300, 'expect' => 5]);

        $this->assertFalse($result['applied']);
        $this->assertSame('absent', $result['reason']);
        $this->assertFalse($kv->get(self::NS_STATE, 'cas')['found']);
    }

    public function testCompareAndSwapSucceedsOnTheRightVersionAndFailsOnAStaleOne(): void
    {
        $kv = $this->queen->kv();
        $v1 = $kv->put(self::NS_STATE, 'cas', ['v' => 1], ['ttlSeconds' => 300])['version'];

        $ok = $kv->put(self::NS_STATE, 'cas', ['v' => 2], ['ttlSeconds' => 300, 'expect' => $v1]);
        $this->assertTrue($ok['applied']);

        $stale = $kv->put(self::NS_STATE, 'cas', ['v' => 3], ['ttlSeconds' => 300, 'expect' => $v1]);
        $this->assertFalse($stale['applied']);
        $this->assertSame('version', $stale['reason']);
        // The loser is told where things actually stand.
        $this->assertSame(['v' => 2], $stale['value']);
        $this->assertSame($ok['version'], $stale['version']);
    }

    public function testDeleteWithExpectFencesAndAPlainDeleteIsIdempotent(): void
    {
        $kv = $this->queen->kv();
        $version = $kv->put(self::NS_STATE, 'fence', ['v' => 1], ['ttlSeconds' => 300])['version'];

        $wrong = $kv->delete(self::NS_STATE, 'fence', ['expect' => $version + 1]);
        $this->assertFalse($wrong['applied']);
        $this->assertSame('version', $wrong['reason']);

        $this->assertTrue($kv->delete(self::NS_STATE, 'fence', ['expect' => $version])['applied']);
        // Deleting nothing is not an error: delete-before-create is an SDK
        // idiom, so the second answers applied:false with a reason.
        $again = $kv->delete(self::NS_STATE, 'fence');
        $this->assertFalse($again['applied']);
        $this->assertSame('absent', $again['reason']);
    }

    public function testIncrCountsAndItsTtlIsCreateOnly(): void
    {
        $kv = $this->queen->kv();

        $this->assertSame(1, $kv->incr(self::NS_COUNTER, 'plain', 1, ['ttlSeconds' => 300])['value']);
        $this->assertSame(4, $kv->incr(self::NS_COUNTER, 'plain', 3, ['ttlSeconds' => 300])['value']);

        // A live row keeps its own expiry: incr must not extend it, or a
        // fixed-window limiter on a busy client would never close its window,
        // i.e. would stop limiting exactly under load.
        $first = $kv->get(self::NS_COUNTER, 'plain')['expiresAt'];
        $kv->incr(self::NS_COUNTER, 'plain', 1, ['ttlSeconds' => 86400]);
        $this->assertSame($first, $kv->get(self::NS_COUNTER, 'plain')['expiresAt']);
    }

    /**
     * With `max`, `applied` IS the admission decision. The ceiling does not
     * saturate: a call that would overshoot does not apply at all, so budget is
     * never spent by a request that was going to be refused.
     *
     * And the first call of a window is guarded too — the naive shape lets the
     * INSERT arm through unchecked, so a delta above the ceiling is admitted
     * once per window rotation, which is exactly when a limiter is under
     * attack.
     */
    public function testIncrCeilingRefusesInsteadOfSaturating(): void
    {
        $kv = $this->queen->kv();

        $overshoot = $kv->incr(self::NS_COUNTER, 'capped', 10, ['ttlSeconds' => 300, 'max' => 5]);
        $this->assertFalse($overshoot['applied'], 'the first call of a window must be guarded too');
        $this->assertSame('limit', $overshoot['reason']);
        $this->assertFalse($kv->get(self::NS_COUNTER, 'capped')['found']);

        $this->assertTrue($kv->incr(self::NS_COUNTER, 'capped', 4, ['ttlSeconds' => 300, 'max' => 5])['applied']);
        $refused = $kv->incr(self::NS_COUNTER, 'capped', 4, ['ttlSeconds' => 300, 'max' => 5]);
        $this->assertFalse($refused['applied']);
        $this->assertSame('limit', $refused['reason']);
        // The current value, never the would-be one.
        $this->assertSame(4, $refused['value']);
    }

    public function testGetManyReportsMissingAsADatum(): void
    {
        $kv = $this->queen->kv();
        $kv->put(self::NS_STATE, 'batch-a', 1, ['ttlSeconds' => 300]);

        $result = $kv->getMany(self::NS_STATE, ['batch-a', 'batch-b']);

        $this->assertCount(1, $result['rows']);
        $this->assertSame('batch-a', $result['rows'][0]['key']);
        // Absence is a list, not something the caller computes by difference.
        $this->assertSame(['batch-b'], $result['missing']);
    }

    public function testGetPrefixPagesWithAKeysetCursor(): void
    {
        $kv = $this->queen->kv();
        foreach (['a', 'b', 'c'] as $suffix) {
            $kv->put(self::NS_STATE, "prefix:{$suffix}", $suffix, ['ttlSeconds' => 300]);
        }

        $page = $kv->getPrefix(self::NS_STATE, 'prefix:', ['limit' => 2]);
        $this->assertCount(2, $page['rows']);
        $this->assertTrue($page['truncated']);
        $this->assertSame('prefix:b', $page['nextAfter']);

        $rest = $kv->getPrefix(self::NS_STATE, 'prefix:', ['after' => $page['nextAfter']]);
        $this->assertCount(1, $rest['rows']);
        $this->assertSame('prefix:c', $rest['rows'][0]['key']);
        $this->assertFalse($rest['truncated']);

        // starts_with, never LIKE: the prefix has no metacharacters, so an
        // underscore or a percent in it means itself.
        $this->assertSame([], $kv->getPrefix(self::NS_STATE, 'prefix_', ['limit' => 10])['rows']);
    }

    /**
     * §5.1, and it is the broker that enforces it so all seven clients inherit
     * the rule: exactly one of ttlSeconds and forever, on every write. Zero
     * declarations is the same error as two, because both mean the caller did
     * not decide — and a default here is how a marker becomes immortal.
     */
    public function testAWriteWithNoExpiryIsRefusedByTheBroker(): void
    {
        try {
            $this->queen->kv()->put(self::NS_STATE, 'expiring', 1);
            $this->fail('a put with no expiry must be refused');
        } catch (HttpException $e) {
            $this->assertSame(400, $e->statusCode);
            // `error` is the branchable code and stays coarse (kv_bad_request);
            // `reason` is the finer stable identifier and is what tells the
            // caller which rule they broke. Branch on either, never on prose.
            $this->assertSame('kv_expiry_not_specified', $e->reason);
            $this->assertStringContainsString('exactly one of ttlSeconds', $e->detail);
        }
    }

    /**
     * `required: true` escalates a lost precondition into a refusal of the
     * WHOLE batch, and the whole batch really does roll back — the writes that
     * would have applied did not.
     *
     * The verdict arrives as HTTP 200 with no `results`, so the client hands it
     * back verbatim rather than inventing an element for it: a lost
     * precondition is the expected outcome of every legitimate redelivery, and
     * it must not enter a retry policy.
     */
    public function testARequiredGateRollsBackTheWholeBatch(): void
    {
        $kv = $this->queen->kv();
        $kv->putIfAbsent(self::NS_STATE, 'txn-gate', ['owner' => 'first'], ['ttlSeconds' => 300]);

        $verdict = $kv->batch([
            KvOp::put(self::NS_STATE, 'batch-a', 'written', ['ttlSeconds' => 300]),
            KvOp::putIfAbsent(self::NS_STATE, 'txn-gate', ['owner' => 'second'], ['ttlSeconds' => 300, 'required' => true]),
        ]);

        $this->assertArrayNotHasKey('results', $verdict);
        $this->assertFalse($verdict['ok']);
        $this->assertSame('kv_precondition', $verdict['reason']);
        $this->assertSame(1, $verdict['failedIndex']);
        $this->assertSame('exists', $verdict['kvReason']);
        $this->assertSame(['owner' => 'first'], $verdict['value']);

        // The sibling write is gone with it.
        $this->assertFalse($kv->get(self::NS_STATE, 'batch-a')['found']);
    }

    /**
     * A batch is one call and one transaction, and its results are aligned to
     * the input by index.
     */
    public function testBatchResultsAreIndexAligned(): void
    {
        $body = $this->queen->kv()->batch([
            KvOp::put(self::NS_STATE, 'batch-a', 'a', ['ttlSeconds' => 300]),
            KvOp::incr(self::NS_COUNTER, 'plain', 2, ['ttlSeconds' => 300]),
            KvOp::get(self::NS_STATE, 'batch-a'),
        ]);

        $this->assertCount(3, $body['results']);
        $this->assertSame([0, 1, 2], array_column($body['results'], 'index'));
        $this->assertSame(['put', 'incr', 'get'], array_column($body['results'], 'op'));
        // Read-your-writes inside one call: the get sees the put beside it.
        $this->assertSame('a', $body['results'][2]['value']);
    }

    public function testASlashedKeyIsOneKey(): void
    {
        $kv = $this->queen->kv();
        $kv->put(self::NS_STATE, 'slashed/key/here', ['ok' => true], ['ttlSeconds' => 300]);

        $this->assertSame(['ok' => true], $kv->get(self::NS_STATE, 'slashed/key/here')['value']);
    }
}
