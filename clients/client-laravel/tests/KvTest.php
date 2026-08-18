<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Exceptions\HttpException;
use Queen\Kv;
use Queen\Queen;
use Queen\Support\KvOp;
use Queen\Tests\Support\PlanHandler;

/**
 * The KV wire contract, asserted as the EXACT JSON body of every operation.
 *
 * This is the only thing that catches a wrong wire shape before production:
 * the broker's stored procedure closes the taxonomy of `op`, and a field named
 * `ttl` instead of `ttlSeconds`, or a `getPrefix` smuggled into a transaction,
 * fails at the database with a 400 that nobody sees until a live queue is
 * involved. Asserting the serialized body here means a rename on either side
 * fails a test in this file.
 *
 * Everything goes through POST /api/v1/kv, which PLAN_KV_TIMERS.md §8.1 calls
 * "la superficie completa" and the only route that accepts getPrefix and incr.
 * The three path routes (GET/PUT/DELETE /api/v1/kv/:ns/*key) are sugar for the
 * cases people write by hand with curl; an SDK that used them would need a
 * second code path for the two operations they cannot express.
 */
class KvTest extends TestCase
{
    // ===========================
    // Operation shapes (pure)
    // ===========================

    public function testGetShape(): void
    {
        $this->assertSame(
            '{"op":"get","ns":"orders","key":"order-9f1"}',
            json_encode(KvOp::get('orders', 'order-9f1'))
        );
    }

    /**
     * A key may contain slashes — `order/9f1/items` is the shape the broker's
     * catch-all path route exists for. It travels in the BODY here, so PHP's
     * default escaping of `/` is cosmetic and must round-trip unchanged; what
     * would not round-trip is a client that re-encoded the key for a URL.
     */
    public function testASlashedKeySurvivesSerialization(): void
    {
        $decoded = json_decode(json_encode(KvOp::get('orders', 'order/9f1/items')), true);

        $this->assertSame('order/9f1/items', $decoded['key']);
    }

    public function testGetManyShape(): void
    {
        // A JSON ARRAY of keys, never an object: json_encode turns a PHP array
        // with a hole in its keys into an object, so the list is re-indexed.
        $keys = [0 => 'a', 2 => 'b'];

        $this->assertSame(
            '{"op":"getMany","ns":"orders","keys":["a","b"]}',
            json_encode(KvOp::getMany('orders', $keys))
        );
    }

    public function testGetPrefixShapeWithEveryOption(): void
    {
        $this->assertSame(
            '{"op":"getPrefix","ns":"quota","prefix":"acme:","after":"acme:m","limit":50,"keysOnly":true}',
            json_encode(KvOp::getPrefix('quota', 'acme:', [
                'after' => 'acme:m',
                'limit' => 50,
                'keysOnly' => true,
            ]))
        );
    }

    public function testPutShape(): void
    {
        $this->assertSame(
            '{"op":"put","ns":"saga","key":"order-1","value":{"step":"reserved"},"ttlSeconds":3600}',
            json_encode(KvOp::put('saga', 'order-1', ['step' => 'reserved'], ['ttlSeconds' => 3600]))
        );
    }

    /**
     * `null` is a legal VALUE — 'null'::jsonb is a real datum and
     * {found:true,value:null} differs from {found:false} (§5.5). The field must
     * therefore be emitted, not dropped as "empty".
     */
    public function testPutEmitsANullValue(): void
    {
        $this->assertSame(
            '{"op":"put","ns":"saga","key":"k","value":null,"forever":true}',
            json_encode(KvOp::put('saga', 'k', null, ['forever' => true]))
        );
    }

    public function testPutIfAbsentIsItsOwnOpName(): void
    {
        // It desugars to put+expect:0 inside the stored procedure, one code
        // path, but it travels under its own name because that is the name of
        // the thing and because `applied` answers "did I win?".
        $this->assertSame(
            '{"op":"putIfAbsent","ns":"lock","key":"job-7","value":{"owner":"w1"},"ttlSeconds":30}',
            json_encode(KvOp::putIfAbsent('lock', 'job-7', ['owner' => 'w1'], ['ttlSeconds' => 30]))
        );
    }

    public function testPutWithExpectAndRequired(): void
    {
        $this->assertSame(
            '{"op":"put","ns":"saga","key":"o","value":1,"ttlSeconds":60,"expect":41,"required":true}',
            json_encode(KvOp::put('saga', 'o', 1, [
                'ttlSeconds' => 60,
                'expect' => 41,
                'required' => true,
            ]))
        );
    }

    public function testDeleteShape(): void
    {
        $this->assertSame(
            '{"op":"delete","ns":"saga","key":"o"}',
            json_encode(KvOp::delete('saga', 'o'))
        );
        $this->assertSame(
            '{"op":"delete","ns":"saga","key":"o","expect":7,"required":true}',
            json_encode(KvOp::delete('saga', 'o', ['expect' => 7, 'required' => true]))
        );
    }

    public function testIncrShape(): void
    {
        $this->assertSame(
            '{"op":"incr","ns":"quota","key":"acme:2026-08","delta":1,"ttlSeconds":86400,"max":1000}',
            json_encode(KvOp::incr('quota', 'acme:2026-08', 1, [
                'ttlSeconds' => 86400,
                'max' => 1000,
            ]))
        );
    }

    // ===========================
    // The client-side rules
    // ===========================

    /**
     * §5.3: an explicitly null `expect` is a bug in the caller's code, never a
     * silent downgrade to an unconditional upsert. The user wrote the word
     * `expect`, so they declared the intention to fence.
     */
    public function testExplicitlyNullExpectThrows(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('expect');
        KvOp::put('saga', 'o', 1, ['ttlSeconds' => 60, 'expect' => null]);
    }

    /**
     * A misspelled option must fail loudly. Silently dropping `ttl` (the name
     * every other queue product uses) would send a write with no expiry
     * declaration at all, and the caller would read the broker's
     * `kv_expiry_not_specified` without ever suspecting their own spelling.
     */
    public function testUnknownOptionThrowsAndNamesTheField(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('ttl');
        KvOp::put('saga', 'o', 1, ['ttl' => 60]);
    }

    /**
     * §6.1 point 6: the tenant is an argument of the stored procedure, never a
     * field of an operation. The broker rejects it; refusing here too means the
     * caller finds out at the first unit test instead of in an audit.
     */
    public function testTenantIsNotAnInput(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('tenant');
        KvOp::put('saga', 'o', 1, ['ttlSeconds' => 60, 'tenant' => 'acme']);
    }

    /**
     * The expiry rule (§5.1) is NOT re-implemented client-side: exactly one of
     * ttlSeconds and forever lives in kv_apply_v1 so that all seven clients and
     * the embedded broker inherit it without a line of their own. What the
     * client must not do is DEFAULT one — a put that silently inherited or
     * invented a TTL is the fastest way to make a marker immortal.
     */
    public function testNoExpiryIsForwardedAsAbsentRatherThanDefaulted(): void
    {
        $op = KvOp::put('saga', 'o', 1);

        $this->assertArrayNotHasKey('ttlSeconds', $op);
        $this->assertArrayNotHasKey('forever', $op);
        $this->assertSame('{"op":"put","ns":"saga","key":"o","value":1}', json_encode($op));
    }

    // ===========================
    // The client: route and body
    // ===========================

    public function testSingleOpPostsTheCompleteSurfaceRoute(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => [
            'results' => [['index' => 0, 'op' => 'get', 'found' => false, 'key' => 'k']],
        ]]);

        $result = $this->kvFor($handler)->get('orders', 'k');

        $request = $handler->requests[0];
        $this->assertSame('POST', $request->getMethod());
        $this->assertSame('/api/v1/kv', $request->getUri()->getPath());
        $this->assertSame(
            '{"operations":[{"op":"get","ns":"orders","key":"k"}]}',
            (string) $request->getBody()
        );
        // A single-op call answers with the ELEMENT, not a one-element array.
        $this->assertFalse($result['found']);
        $this->assertSame('k', $result['key']);
    }

    public function testBatchPreservesOperationOrderAndReturnsTheEnvelope(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => [
            'results' => [
                ['index' => 0, 'op' => 'putIfAbsent', 'applied' => true, 'key' => 'a', 'version' => 1],
                ['index' => 1, 'op' => 'incr', 'applied' => true, 'key' => 'b', 'value' => 5],
            ],
        ]]);

        $body = $this->kvFor($handler)->batch([
            KvOp::putIfAbsent('saga', 'a', ['x' => 1], ['ttlSeconds' => 60]),
            KvOp::incr('quota', 'b', 4, ['ttlSeconds' => 60]),
        ]);

        $this->assertSame(
            '{"operations":['
            . '{"op":"putIfAbsent","ns":"saga","key":"a","value":{"x":1},"ttlSeconds":60},'
            . '{"op":"incr","ns":"quota","key":"b","delta":4,"ttlSeconds":60}'
            . ']}',
            (string) $handler->requests[0]->getBody()
        );
        // Results are index-aligned to the input array (§6.4), so the envelope
        // is handed back whole rather than reshaped into a map.
        $this->assertCount(2, $body['results']);
        $this->assertSame(0, $body['results'][0]['index']);
        $this->assertSame(1, $body['results'][1]['index']);
    }

    /**
     * §8.1, the single rule about status codes: the status describes the
     * outcome of the CALL, never the verdict of the business predicate. A lost
     * putIfAbsent is 200 with applied:false, and it must reach the caller as a
     * value — not as an exception, and not as a truthy object either.
     */
    public function testALostRaceIsAValueAndNotAnException(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => [
            'results' => [[
                'index' => 0,
                'op' => 'put',
                'applied' => false,
                'reason' => 'exists',
                'key' => 'job-7',
                'value' => ['owner' => 'w1'],
                'version' => 90101,
            ]],
        ]]);

        $result = $this->kvFor($handler)->putIfAbsent('lock', 'job-7', ['owner' => 'w2'], ['ttlSeconds' => 30]);

        $this->assertFalse($result['applied']);
        $this->assertSame('exists', $result['reason']);
        // The loser gets the winner's value and version without a second round
        // trip — that is the entire point of the idempotency marker (§5.3).
        $this->assertSame(['owner' => 'w1'], $result['value']);
        $this->assertSame(90101, $result['version']);
    }

    /**
     * §8.3 on the standalone route: an opt-in `required:true` that loses its
     * precondition answers HTTP 200 with a verdict body that has no `results`.
     * It must not become a 500 or an exception, and the caller must be able to
     * tell the two shapes apart, so the verdict is returned verbatim.
     */
    public function testARequiredPreconditionVerdictIsReturnedVerbatim(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => [
            'ok' => false,
            'reason' => 'kv_precondition',
            'failedIndex' => 0,
            'kvReason' => 'version',
            'version' => 42,
            'value' => ['step' => 'reserved'],
        ]]);

        $result = $this->kvFor($handler)->put('saga', 'o', 1, ['ttlSeconds' => 60, 'expect' => 41, 'required' => true]);

        $this->assertFalse($result['ok']);
        $this->assertSame('kv_precondition', $result['reason']);
        $this->assertSame('version', $result['kvReason']);
        $this->assertArrayNotHasKey('applied', $result);
    }

    /**
     * §5.5: getPrefix is allowed only in the POST body and never as a query
     * string. `?prefix=quota:acme:` would be recorded by the broker's access
     * log, the proxy's, the meter sample, the per-request tracing span and any
     * ingress in front — a mitigation living in one component out of four is
     * not a mitigation. The client must therefore never put a prefix in a URL.
     */
    public function testGetPrefixTravelsInTheBodyAndNeverInTheUrl(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => [
            'results' => [['index' => 0, 'op' => 'getPrefix', 'rows' => [], 'truncated' => false, 'nextAfter' => null]],
        ]]);

        $this->kvFor($handler)->getPrefix('quota', 'acme:');

        $request = $handler->requests[0];
        $this->assertSame('', $request->getUri()->getQuery());
        $this->assertStringNotContainsString('acme', (string) $request->getUri());
        $this->assertStringContainsString('"prefix":"acme:"', (string) $request->getBody());
    }

    /**
     * The KV envelope is {error, reason, detail}: `error` is the branchable
     * code, `reason` a finer stable identifier and `detail` the human half,
     * which is the only field that names WHICH operation of a batch was wrong.
     * A client that kept only `error` would hand the caller "kv_bad_request"
     * and nothing to act on, on the one surface whose operations arrive in
     * batches of up to 256.
     */
    public function testAnErrorCarriesItsReasonAndDetail(): void
    {
        $handler = new PlanHandler([], ['status' => 400, 'json' => [
            'error' => 'kv_bad_request',
            'reason' => 'kv_expiry_not_specified',
            'detail' => 'op at index 3: exactly one of ttlSeconds (integer > 0) and forever:true is required, got 0',
        ]]);

        try {
            $this->kvFor($handler)->get('orders', 'k');
            $this->fail('a 400 must throw');
        } catch (HttpException $e) {
            $this->assertSame(400, $e->statusCode);
            $this->assertSame('kv_expiry_not_specified', $e->reason);
            $this->assertStringContainsString('op at index 3', $e->detail);
            // And the message says all three, because an assertion that reads
            // only "kv_bad_request" costs an hour.
            $this->assertStringContainsString('kv_bad_request', $e->getMessage());
            $this->assertStringContainsString('kv_expiry_not_specified', $e->getMessage());
        }
    }

    public function testQueenExposesKvAndReusesTheSameInstance(): void
    {
        $queen = new Queen('http://queen.test:6632');

        $this->assertInstanceOf(Kv::class, $queen->kv());
        $this->assertSame($queen->kv(), $queen->kv());
    }

    private function kvFor(PlanHandler $handler): Kv
    {
        return (new Queen([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
        ]))->kv();
    }
}
