<?php

namespace Queen\Tests;

use DateTimeImmutable;
use DateTimeInterface;
use GuzzleHttp\HandlerStack;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Queue\Failed\CountableFailedJobProvider;
use Illuminate\Queue\Failed\DatabaseUuidFailedJobProvider;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Queue\Failed\FileFailedJobProvider;
use Illuminate\Queue\Failed\PrunableFailedJobProvider;
use PHPUnit\Framework\TestCase;
use Queen\Exceptions\HttpException;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\Queue\SyncedFailedJobProvider;
use Queen\Tests\Support\PlanHandler;

final class SyncedFailedJobProviderLifecycleTest extends TestCase
{
    public function testPersistentIndexOperationsAreDelegated(): void
    {
        $inner = new LifecycleFailedJobProvider();
        $provider = new SyncedFailedJobProvider($inner, fn (string $connection) => null);
        $payload = json_encode(['uuid' => 'failed-1'], JSON_THROW_ON_ERROR);

        $this->assertSame(
            'failed-1',
            $provider->log('queen', 'critical', $payload, new \RuntimeException('probe')),
        );
        $this->assertSame(['failed-1'], $provider->ids());
        $this->assertSame(['failed-1'], $provider->ids('critical'));
        $this->assertCount(1, $provider->all());
        $this->assertSame('failed-1', $provider->find('failed-1')->id);
        $this->assertSame(1, $provider->count());
        $this->assertSame(1, $provider->count('queen', 'critical'));
        $this->assertSame(0, $provider->count('queen', 'default'));
    }

    public function testPrunePreservesTheDatabaseProviderCutoffBoundary(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['success' => true]]]);
        $queue = $this->queue($handler);
        $cutoff = new DateTimeImmutable('2026-01-02 00:00:00');
        $inner = new LifecycleFailedJobProvider([
            $this->record('older', '2026-01-01 23:59:59'),
            $this->record('boundary', '2026-01-02 00:00:00'),
            $this->record('newer', '2026-01-02 00:00:01'),
        ]);
        $provider = new SyncedFailedJobProvider($inner, fn (string $connection) => $queue);

        $this->assertSame(1, $provider->prune($cutoff));
        $this->assertNull($inner->find('older'));
        $this->assertNotNull($inner->find('boundary'));
        $this->assertNotNull($inner->find('newer'));
        $this->assertCount(1, $handler->requests);
        $this->assertSame(
            '/api/v1/messages/partition-older/transaction-older',
            $handler->requests[0]->getUri()->getPath(),
        );
    }

    public function testFlushCleansOnlyRowsTheInnerProviderWillRemove(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['success' => true]]]);
        $queue = $this->queue($handler);
        $inner = new LifecycleFailedJobProvider([
            $this->record('old-queen', '2020-01-01 00:00:00'),
            $this->record('recent-queen', date('Y-m-d H:i:s')),
            $this->record('old-sync', '2020-01-01 00:00:00', 'sync'),
        ]);
        $provider = new SyncedFailedJobProvider(
            $inner,
            fn (string $connection) => $connection === 'queen' ? $queue : null,
        );

        $provider->flush(24);

        $this->assertNull($inner->find('old-queen'));
        $this->assertNull($inner->find('old-sync'));
        $this->assertNotNull($inner->find('recent-queen'));
        $this->assertCount(1, $handler->requests);
        $this->assertSame(
            '/api/v1/messages/partition-old-queen/transaction-old-queen',
            $handler->requests[0]->getUri()->getPath(),
        );
    }

    public function testFlushKeepsEveryIndexRowWhenAnyDlqCleanupFails(): void
    {
        $handler = new PlanHandler([], ['status' => 503, 'json' => ['error' => 'unavailable']]);
        $inner = new LifecycleFailedJobProvider([
            $this->record('old-1', '2020-01-01 00:00:00'),
            $this->record('old-2', '2020-01-01 00:00:00'),
        ]);
        $provider = new SyncedFailedJobProvider($inner, fn (string $connection) => $this->queue($handler));

        try {
            $provider->flush();
            $this->fail('A failed Queen cleanup must abort the Laravel flush.');
        } catch (HttpException $exception) {
            $this->assertSame(503, $exception->statusCode);
        }

        $this->assertNotNull($inner->find('old-1'));
        $this->assertNotNull($inner->find('old-2'));
    }

    public function testFlushDeletesOnlySnapshottedIdsWhenANewFailureArrivesConcurrently(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['success' => true]]]);
        $inner = new LifecycleFailedJobProvider([
            $this->record('old', '2020-01-01 00:00:00'),
        ]);
        $lockAcquisitions = 0;
        $provider = new SyncedFailedJobProvider(
            $inner,
            fn (string $connection) => $this->queue($handler),
            function (\Closure $operation) use (&$lockAcquisitions, $inner): mixed {
                ++$lockAcquisitions;
                $result = $operation(static function (): void {
                });
                if ($lockAcquisitions === 1) {
                    // A new failure is committed after the snapshot lock is
                    // released but before the first exact-ID mutation lock.
                    $inner->add($this->record('concurrent', date('Y-m-d H:i:s')));
                }

                return $result;
            },
        );

        $provider->flush();

        $this->assertNull($inner->find('old'));
        $this->assertNotNull($inner->find('concurrent'));
        $this->assertCount(1, $handler->requests);
        $this->assertSame(2, $lockAcquisitions, 'snapshot and exact-ID mutation use separate locks');
    }

    public function testExpiredLockAfterDlqCleanupKeepsTheLaravelIndexRow(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['success' => true]]]);
        $inner = new LifecycleFailedJobProvider([$this->record('failed-1', '2020-01-01 00:00:00')]);
        $provider = new SyncedFailedJobProvider(
            $inner,
            fn (string $connection) => $this->queue($handler),
            fn (\Closure $operation): mixed => $operation(
                static function (): void {
                    throw new \RuntimeException('lock expired');
                },
            ),
        );

        try {
            $provider->forget('failed-1');
            $this->fail('An expired lock must fail closed.');
        } catch (\RuntimeException $exception) {
            $this->assertSame('lock expired', $exception->getMessage());
        }

        $this->assertNotNull($inner->find('failed-1'));
        $this->assertCount(1, $handler->requests);
    }

    public function testFlushDoesNotDeleteANewerGenerationReusingTheSameJobUuid(): void
    {
        $handler = new PlanHandler();
        $inner = new LifecycleFailedJobProvider([
            $this->record('reused', '2020-01-01 00:00:00'),
        ]);
        $lockAcquisitions = 0;
        $provider = new SyncedFailedJobProvider(
            $inner,
            fn (string $connection) => $this->queue($handler),
            function (\Closure $operation) use (&$lockAcquisitions, $inner): mixed {
                ++$lockAcquisitions;
                $result = $operation(static function (): void {
                });
                if ($lockAcquisitions === 1) {
                    $inner->add($this->record('reused', date('Y-m-d H:i:s')));
                }

                return $result;
            },
        );

        $provider->flush();

        $this->assertNotNull($inner->find('reused'));
        $this->assertCount(0, $handler->requests);
        $this->assertSame(2, $lockAcquisitions);
    }

    public function testLaravelRetryForgetCannotDeleteANewFailureWithTheSameUuid(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['success' => true]],
            ['status' => 201, 'json' => [['status' => 'queued']]],
        ]);
        $inner = new LifecycleFailedJobProvider([
            $this->record('reused', '2026-01-01 00:00:00'),
        ]);
        $lockHeld = false;
        $lockAcquisitions = 0;
        $provider = null;
        $queue = (new QueenConnector(
            failedJobRetryHandler: function (string $fence, \Closure $republish) use (&$provider): mixed {
                if (!$provider instanceof SyncedFailedJobProvider) {
                    throw new \LogicException('Test provider was not initialized.');
                }

                return $provider->retryWithFence($fence, $republish);
            },
        ))->connect([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
            'retry_attempts' => 1,
        ]);
        $provider = new SyncedFailedJobProvider(
            $inner,
            fn (string $connection) => $queue,
            function (\Closure $operation) use (&$lockHeld, &$lockAcquisitions): mixed {
                $this->assertFalse($lockHeld, 'The failed-store lock must not be re-entered.');
                ++$lockAcquisitions;
                $lockHeld = true;
                try {
                    return $operation(function () use (&$lockHeld): void {
                        $this->assertTrue($lockHeld, 'The retry must retain lock ownership.');
                    });
                } finally {
                    $lockHeld = false;
                }
            },
        );

        // This is Laravel RetryCommand's real sequence: find(), pushRaw(),
        // then forget(). find() returns a detached, fenced payload.
        $selected = $provider->find('reused');
        $this->assertNotNull($selected);
        $selectedPayload = json_decode($selected->payload, true, 512, JSON_THROW_ON_ERROR);
        $this->assertMatchesRegularExpression(
            '/^[0-9a-f]{64}$/D',
            $selectedPayload['_queen']['retry_fence'],
        );

        $this->assertSame('reused', $queue->pushRaw($selected->payload, 'critical'));
        $this->assertNull($inner->find('reused'), 'pushRaw removes the exact old row before releasing the lock');

        // Model the retried job failing after pushRaw returns but before
        // RetryCommand reaches its unconditional forget($id).
        $provider->log(
            'queen',
            'critical',
            $this->record('reused', '2026-01-01 00:00:01')->payload,
            new \RuntimeException('new generation'),
        );
        $newGeneration = $inner->find('reused');
        $this->assertNotNull($newGeneration);

        $this->assertFalse($provider->forget('reused'));
        $this->assertSame($newGeneration, $inner->find('reused'));
        $this->assertSame(2, $lockAcquisitions, 'retry and new log lock; post-retry forget is a one-shot no-op');
        $this->assertCount(2, $handler->requests, 'only publish and old DLQ cleanup reach Queen');

        $published = json_decode((string) $handler->requests[1]->getBody(), true, 512, JSON_THROW_ON_ERROR);
        $this->assertArrayNotHasKey('retry_fence', $published['items'][0]['payload']['_queen']);
        $this->assertSame(
            '/api/v1/messages/partition-reused/transaction-reused',
            $handler->requests[0]->getUri()->getPath(),
        );
    }

    public function testFencedRetryRejectsAReplacementGenerationBeforePublishing(): void
    {
        $handler = new PlanHandler();
        $inner = new LifecycleFailedJobProvider([
            $this->record('reused', '2026-01-01 00:00:00'),
        ]);
        $provider = null;
        $queue = (new QueenConnector(
            failedJobRetryHandler: function (string $fence, \Closure $republish) use (&$provider): mixed {
                if (!$provider instanceof SyncedFailedJobProvider) {
                    throw new \LogicException('Test provider was not initialized.');
                }

                return $provider->retryWithFence($fence, $republish);
            },
        ))->connect([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
            'retry_attempts' => 1,
        ]);
        $provider = new SyncedFailedJobProvider(
            $inner,
            fn (string $connection) => $queue,
            fn (\Closure $operation): mixed => $operation(static function (): void {
            }),
        );

        $selected = $provider->find('reused');
        $this->assertNotNull($selected);
        $replacement = $this->record('reused', '2026-01-01 00:00:01');
        $inner->add($replacement);

        try {
            $queue->pushRaw($selected->payload, 'critical');
            $this->fail('A stale failed-job generation must not be published.');
        } catch (\RuntimeException $exception) {
            $this->assertSame(
                'The Laravel failed job changed after queue:retry selected it; the stale generation was not published.',
                $exception->getMessage(),
            );
        }

        $this->assertSame($replacement, $inner->find('reused'));
        $this->assertCount(0, $handler->requests);
    }

    public function testSynchronizedManualRetryWithoutAProviderFenceFailsClosed(): void
    {
        $handler = new PlanHandler();
        $retryHandlerCalled = false;
        $queue = (new QueenConnector(
            failedJobRetryHandler: function (
                string $fence,
                \Closure $republish,
            ) use (&$retryHandlerCalled): mixed {
                $retryHandlerCalled = true;

                return $republish();
            },
        ))->connect([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
            'retry_attempts' => 1,
        ]);

        try {
            $queue->pushRaw($this->record('unfenced', '2026-01-01 00:00:00')->payload, 'critical');
            $this->fail('A synchronized manual retry must originate from the failed provider find().');
        } catch (\RuntimeException $exception) {
            $this->assertSame(
                'The Queen failed-job retry fence is missing; select the job through Laravel queue:retry.',
                $exception->getMessage(),
            );
        }

        $this->assertFalse($retryHandlerCalled);
        $this->assertCount(0, $handler->requests);
    }

    public function testFencedRetryUsesTheFailedStoreIdInsteadOfTheJobUuid(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['success' => true]],
            ['status' => 201, 'json' => [['status' => 'queued']]],
        ]);
        $record = $this->record('job-uuid', '2026-01-01 00:00:00');
        $record->id = 42;
        $inner = new LifecycleFailedJobProvider([$record]);
        $provider = null;
        $queue = (new QueenConnector(
            failedJobRetryHandler: function (string $fence, \Closure $republish) use (&$provider): mixed {
                if (!$provider instanceof SyncedFailedJobProvider) {
                    throw new \LogicException('Test provider was not initialized.');
                }

                return $provider->retryWithFence($fence, $republish);
            },
        ))->connect([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
            'retry_attempts' => 1,
        ]);
        $provider = new SyncedFailedJobProvider(
            $inner,
            fn (string $connection) => $queue,
            fn (\Closure $operation): mixed => $operation(static function (): void {
            }),
        );

        $selected = $provider->find(42);
        $this->assertNotNull($selected);
        $this->assertSame('job-uuid', $queue->pushRaw($selected->payload, 'critical'));

        $this->assertNull($inner->find(42));
        $this->assertFalse($provider->forget('42'), 'Artisan string IDs consume the same one-shot fence');
        $this->assertCount(2, $handler->requests);
    }

    public function testFencedRetryAllowsDatabaseUuidStoreToLogTheNextGeneration(): void
    {
        $database = new Capsule();
        $database->addConnection([
            'driver' => 'sqlite',
            'database' => ':memory:',
        ], 'failed');
        $database->getConnection('failed')->getSchemaBuilder()->create(
            'failed_jobs',
            function (Blueprint $table): void {
                $table->id();
                $table->string('uuid')->unique();
                $table->text('connection');
                $table->text('queue');
                $table->longText('payload');
                $table->longText('exception');
                $table->timestamp('failed_at')->useCurrent();
            },
        );

        $old = $this->record('reused', '2026-01-01 00:00:00');
        $connection = $database->getConnection('failed');
        $connection->table('failed_jobs')->insert([
            'uuid' => 'reused',
            'connection' => $old->connection,
            'queue' => $old->queue,
            'payload' => $old->payload,
            'exception' => $old->exception,
            'failed_at' => $old->failed_at,
        ]);
        $inner = new DatabaseUuidFailedJobProvider(
            $database->getDatabaseManager(),
            'failed',
            'failed_jobs',
        );
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['success' => true]],
            ['status' => 201, 'json' => [['status' => 'queued']]],
        ]);
        $provider = null;
        $queue = (new QueenConnector(
            failedJobRetryHandler: function (string $fence, \Closure $republish) use (&$provider): mixed {
                if (!$provider instanceof SyncedFailedJobProvider) {
                    throw new \LogicException('Test provider was not initialized.');
                }

                return $provider->retryWithFence($fence, $republish);
            },
        ))->connect([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
            'retry_attempts' => 1,
        ]);
        $provider = new SyncedFailedJobProvider(
            $inner,
            fn (string $failedConnection) => $queue,
            fn (\Closure $operation): mixed => $operation(static function (): void {
            }),
        );

        $selected = $provider->find('reused');
        $this->assertNotNull($selected);
        $queue->pushRaw($selected->payload, 'critical');
        $this->assertNull($inner->find('reused'));

        // A database-uuids provider enforces a unique UUID. This insert can
        // succeed only because retryWithFence removed the old generation
        // before releasing the synchronization boundary.
        $new = $this->record('reused', '2026-01-01 00:00:01');
        $newPayload = json_decode($new->payload, true, 512, JSON_THROW_ON_ERROR);
        $newPayload['_queen']['manual_retry'] = 'retry-reused-next';
        $newPayload['_queen']['failed_source']['transaction_id'] = 'transaction-reused-next';
        $connection->table('failed_jobs')->insert([
            'uuid' => 'reused',
            'connection' => $new->connection,
            'queue' => $new->queue,
            'payload' => json_encode($newPayload, JSON_THROW_ON_ERROR),
            'exception' => 'next generation',
            'failed_at' => $new->failed_at,
        ]);

        $this->assertFalse($provider->forget('reused'));
        $this->assertSame('next generation', $inner->find('reused')->exception);
        $this->assertCount(1, $connection->table('failed_jobs')->get());
    }

    public function testLockAcquisitionFailureAttemptsNoCleanupOrIndexMutation(): void
    {
        $handler = new PlanHandler();
        $inner = new LifecycleFailedJobProvider([$this->record('failed-1', '2020-01-01 00:00:00')]);
        $provider = new SyncedFailedJobProvider(
            $inner,
            fn (string $connection) => $this->queue($handler),
            static function (\Closure $operation): never {
                throw new \RuntimeException('lock unavailable');
            },
        );

        try {
            $provider->forget('failed-1');
            $this->fail('A failed lock acquisition must fail closed.');
        } catch (\RuntimeException $exception) {
            $this->assertSame('lock unavailable', $exception->getMessage());
        }

        $this->assertNotNull($inner->find('failed-1'));
        $this->assertCount(0, $handler->requests);
    }

    public function testBoundedFileStoreCleansOldestDlqBeforeItCanBeEvicted(): void
    {
        $path = tempnam(sys_get_temp_dir(), 'queen-failed-');
        $this->assertIsString($path);
        $handler = new PlanHandler([['status' => 200, 'json' => ['success' => true]]]);
        $inner = new FileFailedJobProvider($path, 2);
        $provider = new SyncedFailedJobProvider(
            $inner,
            fn (string $connection) => $this->queue($handler),
            fn (\Closure $operation) => $operation(static function (): void {
            }),
        );

        try {
            foreach (['oldest', 'middle', 'newest'] as $id) {
                $provider->log(
                    'queen',
                    'critical',
                    json_encode([
                        'uuid' => $id,
                        '_queen' => [
                            'failed_source' => [
                                'partition_id' => 'partition-'.$id,
                                'transaction_id' => 'transaction-'.$id,
                            ],
                        ],
                    ], JSON_THROW_ON_ERROR),
                    new \RuntimeException('probe'),
                );
            }

            $this->assertSame(['newest', 'middle'], $inner->ids());
            $this->assertCount(1, $handler->requests);
            $this->assertSame(
                '/api/v1/messages/partition-oldest/transaction-oldest',
                $handler->requests[0]->getUri()->getPath(),
            );
        } finally {
            @unlink($path);
        }
    }

    private function queue(PlanHandler $handler): \Queen\Laravel\Queue\QueenQueue
    {
        return (new QueenConnector())->connect([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
            'retry_attempts' => 1,
        ]);
    }

    private function record(
        string $id,
        string $failedAt,
        string $connection = 'queen',
    ): object {
        return (object) [
            'id' => $id,
            'connection' => $connection,
            'queue' => 'critical',
            'payload' => json_encode([
                'uuid' => $id,
                '_queen' => [
                    'manual_retry' => 'retry-'.$id,
                    'failed_source' => [
                        'partition_id' => 'partition-'.$id,
                        'transaction_id' => 'transaction-'.$id,
                    ],
                ],
            ], JSON_THROW_ON_ERROR),
            'exception' => 'probe',
            'failed_at' => $failedAt,
        ];
    }
}

final class LifecycleFailedJobProvider implements
    FailedJobProviderInterface,
    CountableFailedJobProvider,
    PrunableFailedJobProvider
{
    /** @var array<string, object> */
    private array $records = [];

    /** @param list<object> $records */
    public function __construct(array $records = [], private bool $inclusivePrune = false)
    {
        foreach ($records as $record) {
            $this->records[(string) $record->id] = $record;
        }
    }

    public function log($connection, $queue, $payload, $exception)
    {
        $decoded = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        $id = (string) $decoded['uuid'];
        $this->records[$id] = (object) [
            'id' => $id,
            'connection' => $connection,
            'queue' => $queue,
            'payload' => $payload,
            'exception' => (string) $exception,
            'failed_at' => date('Y-m-d H:i:s'),
        ];

        return $id;
    }

    public function ids($queue = null)
    {
        return array_keys(array_filter(
            $this->records,
            fn (object $record): bool => $queue === null || $record->queue === $queue,
        ));
    }

    public function all()
    {
        return array_values($this->records);
    }

    public function add(object $record): void
    {
        $this->records[(string) $record->id] = $record;
    }

    public function find($id)
    {
        return $this->records[(string) $id] ?? null;
    }

    public function forget($id)
    {
        if (!isset($this->records[(string) $id])) {
            return false;
        }
        unset($this->records[(string) $id]);

        return true;
    }

    public function flush($hours = null)
    {
        $cutoff = $hours ? time() - ((int) $hours * 3600) : null;
        foreach ($this->records as $id => $record) {
            if ($cutoff === null || strtotime($record->failed_at) <= $cutoff) {
                unset($this->records[$id]);
            }
        }
    }

    public function prune(DateTimeInterface $before)
    {
        $deleted = 0;
        foreach ($this->records as $id => $record) {
            $timestamp = strtotime($record->failed_at);
            if ($timestamp < $before->getTimestamp()
                || ($this->inclusivePrune && $timestamp === $before->getTimestamp())) {
                unset($this->records[$id]);
                ++$deleted;
            }
        }

        return $deleted;
    }

    public function count($connection = null, $queue = null)
    {
        return count(array_filter(
            $this->records,
            fn (object $record): bool => ($connection === null || $record->connection === $connection)
                && ($queue === null || $record->queue === $queue),
        ));
    }
}
