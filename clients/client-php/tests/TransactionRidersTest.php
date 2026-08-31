<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Builders\TransactionBuilder;
use Queen\Http\HttpClient;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

/**
 * KV and timer riders on POST /api/v1/transaction.
 *
 * WHERE THEY LIVE ON THE REQUEST, AND WHY IT IS NOT NEGOTIABLE (§6.3, §10.4).
 * `kv` and `timers` are TOP-LEVEL fields of the request body, never elements of
 * `operations`. The reason is a silent failure in a sibling client: in Go, two
 * struct fields carrying the same JSON key at the same level are BOTH DROPPED
 * by encoding/json, with no error and no warning, so growing the operation type
 * a `kv` leg would let a body go out with zero KV ops while the broker
 * committed a transaction with no gate — the putIfAbsent the bundle existed for
 * would simply never have happened, and nothing anywhere would say so. PHP does
 * not have that failure mode, but the wire is shared by seven clients and the
 * shape is the same for all of them.
 *
 * THE OTHER HALF is the flat result space: `[0, ops)` is `operations` exactly
 * as today, then the `kv` array, then `timers`. A push or an ack never changes
 * index because a rider is present, and a bundle carrying neither array is
 * byte-identical to today's.
 */
class TransactionRidersTest extends TestCase
{
    // ===========================
    // Wire shape
    // ===========================

    public function testKvAndTimersAreTopLevelArraysNotOperations(): void
    {
        $httpClient = $this->createMock(HttpClient::class);
        $httpClient->expects($this->once())
            ->method('post')
            ->with(
                '/api/v1/transaction',
                $this->callback(function (array $body) {
                    // One ack, and nothing else in `operations`.
                    $this->assertCount(1, $body['operations']);
                    $this->assertSame('ack', $body['operations'][0]['type']);

                    $this->assertSame([[
                        'op' => 'putIfAbsent',
                        'ns' => 'saga',
                        'key' => 'order-1',
                        'value' => ['step' => 'reserved'],
                        'ttlSeconds' => 3600,
                        'required' => true,
                    ]], $body['kv']);

                    $this->assertCount(1, $body['timers']);
                    $this->assertSame('schedule', $body['timers'][0]['op']);
                    $this->assertSame('payments.retry', $body['timers'][0]['queue']);
                    $this->assertSame(60000, $body['timers'][0]['delayMs']);

                    return true;
                })
            )
            ->willReturn(['success' => true, 'transactionId' => 'abc', 'results' => []]);

        $tx = new TransactionBuilder($httpClient);
        $tx->ack([['transactionId' => 'tx-1', 'partitionId' => 'p1']])
            ->kv('saga')->putIfAbsent('order-1', ['step' => 'reserved'], ['ttlSeconds' => 3600, 'required' => true])
            ->timers('payments.retry')->schedule('order-1', 60_000, ['orderId' => 1], ['txn' => 't'])
            ->commit();
    }

    /**
     * §6.3: a bundle that carries neither array must be byte-identical to
     * today's. Emitting `"kv": null` — which is what a naive optional field
     * produces — would make the stored procedure's jsonb_array_length RAISE on
     * a transaction that works today.
     */
    public function testABundleWithNoRidersOmitsBothKeysEntirely(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['success' => true]]);
        $tx = $this->queenFor($handler)->transaction();

        $tx->ack([['transactionId' => 'tx-1', 'partitionId' => 'p1']])->commit();

        $body = json_decode((string) $handler->requests[0]->getBody(), true);
        $this->assertArrayNotHasKey('kv', $body);
        $this->assertArrayNotHasKey('timers', $body);
    }

    /**
     * The riders serialize as JSON ARRAYS, not objects. A PHP array whose keys
     * are not a dense 0..n-1 range encodes as an object, which the broker's
     * jsonb_typeof check would reject — and would reject only for the caller
     * who happened to build the bundle in an unusual order.
     */
    public function testRidersSerializeAsArraysEvenAfterInterleavedBuilding(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['success' => true]]);
        $tx = $this->queenFor($handler)->transaction();

        $tx->kv('saga')->put('a', 1, ['ttlSeconds' => 60]);
        $tx->timers('q')->schedule('t1', 1000, null, ['txn' => 'x']);
        $tx->kv('saga')->delete('b');
        $tx->timers('q')->cancel('t2');
        $tx->commit();

        $raw = (string) $handler->requests[0]->getBody();
        $this->assertStringContainsString('"kv":[{', $raw);
        $this->assertStringContainsString('"timers":[{', $raw);
        $body = json_decode($raw, true);
        $this->assertSame([0, 1], array_keys($body['kv']));
        $this->assertSame([0, 1], array_keys($body['timers']));
    }

    /**
     * §5.5: getPrefix is forbidden in the transaction wire — read work whose
     * cost the caller does not bound a priori, inside the transaction that
     * holds the outermost lock space and, downstream, the partition ones. The
     * boundary is COST, not the kind of operation, which is why get and getMany
     * are allowed. The builder must not offer it at all.
     */
    public function testTheTransactionKvBuilderHasNoGetPrefix(): void
    {
        $tx = (new Queen('http://queen.test:6632'))->transaction();

        $this->assertFalse(method_exists($tx->kv('saga'), 'getPrefix'));
        $this->assertTrue(method_exists($tx->kv('saga'), 'getMany'));
    }

    /**
     * §8.2 point 6: a bundle of only KV is routed straight at kv_apply_v1, so a
     * transaction with riders and no operations is legal. The empty-transaction
     * guard must still fire when there is nothing at all to commit.
     */
    public function testARiderOnlyBundleCommitsButAnEmptyOneStillThrows(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['success' => true]]);
        $queen = $this->queenFor($handler);

        $queen->transaction()->kv('saga')->put('a', 1, ['ttlSeconds' => 60])->commit();
        $body = json_decode((string) $handler->requests[0]->getBody(), true);
        $this->assertSame([], $body['operations']);
        $this->assertCount(1, $body['kv']);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('no operations');
        $queen->transaction()->commit();
    }

    // ===========================
    // The precondition branch (§8.3)
    // ===========================

    /**
     * THE WIRE CHANGE THIS FEATURE FORCES ON EVERY CLIENT.
     *
     * A lost `required` KV gate is the EXPECTED outcome of every legitimate
     * redelivery — it is the idempotency marker doing its job. The broker
     * answers HTTP 200 with success:false and reason:"kv_precondition"
     * precisely so it pollutes neither the error metrics nor the retry policies
     * of seven clients. commit() must therefore RETURN it, not throw: a thrown
     * exception on the single most frequent outcome of the product's number-one
     * use case would put the happy path inside every caller's catch block.
     */
    public function testCommitReturnsOnAKvPrecondition(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $httpClient->method('post')->willReturn([
            'transactionId' => 'tx-abc',
            'success' => false,
            'reason' => 'kv_precondition',
            'error' => 'kv_precondition_failed',
            'results' => [],
            'ok' => false,
            'failedIndex' => 2,
            'kvReason' => 'exists',
            'version' => 90101,
            'value' => ['step' => 'reserved'],
        ]);

        $tx = new TransactionBuilder($httpClient);
        $result = $tx->ack([['transactionId' => 'tx-1', 'partitionId' => 'p1']])
            ->kv('saga')->putIfAbsent('order-1', ['step' => 'new'], ['ttlSeconds' => 60, 'required' => true])
            ->commit();

        $this->assertFalse($result['success']);
        $this->assertSame('kv_precondition', $result['reason']);
        // failedIndex is in the FLAT space: `operations` first, then kv, then
        // timers. Indexing the wrong array points the caller at somebody else's
        // operation.
        $this->assertSame(2, $result['failedIndex']);
        $this->assertSame('exists', $result['kvReason']);
        $this->assertSame(90101, $result['version']);
        $this->assertSame(['step' => 'reserved'], $result['value']);
    }

    /**
     * And nothing else changes: every other failure still throws, so a lease
     * that expired or a duplicate still reaches the caller the way it does
     * today. The precondition branch is a single named reason, not a general
     * softening of commit().
     */
    public function testEveryOtherFailureStillThrows(): void
    {
        foreach (['ack_rejected', 'duplicate', 'db_error', 'bad_request', null] as $reason) {
            $httpClient = $this->createStub(HttpClient::class);
            $httpClient->method('post')->willReturn(array_filter([
                'transactionId' => 'tx-abc',
                'success' => false,
                'reason' => $reason,
                'error' => 'QTXN lease expired',
            ], fn($v) => $v !== null));

            $tx = new TransactionBuilder($httpClient);
            $tx->ack([['transactionId' => 'tx-1', 'partitionId' => 'p1']]);

            try {
                $tx->commit();
                $this->fail('commit() must throw for reason ' . var_export($reason, true));
            } catch (\RuntimeException $e) {
                $this->assertStringContainsString('tx-abc', $e->getMessage());
            }
        }
    }

    private function queenFor(PlanHandler $handler): Queen
    {
        return new Queen([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
        ]);
    }
}
