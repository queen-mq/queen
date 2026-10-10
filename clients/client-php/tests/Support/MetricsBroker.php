<?php

namespace Queen\Tests\Support;

use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use Illuminate\Queue\CallQueuedHandler;
use Psr\Http\Message\RequestInterface;
use Queen\Queen;

/**
 * A broker for a Laravel worker and the job metrics it records. Pops serve
 * the scripted deliveries in order, then none; ACKs and transactions are
 * accepted; the key/value store keeps what a worker puts and lists it as
 * POST /api/v1/resources/kv/list does, so JobMetricsReader reads back what
 * the workers wrote.
 *
 * With a log path, every request is also appended there as one JSON line
 * with the pid that sent it: what a forked worker sent reaches the parent
 * that way, and fromLog() replays its writes.
 */
final class MetricsBroker
{
    /** @var list<array{pid: int, method: string, path: string, body: mixed}> */
    public array $requests = [];

    /** @var array<string, array<string, mixed>> namespace => key => value */
    public array $kv = [];

    /** @param list<array> $deliveries see delivery() */
    public function __construct(private array $deliveries = [], private ?string $log = null)
    {
    }

    /** What the next pops serve: a forked worker sets its own after the fork. */
    public function serve(array $deliveries): void
    {
        $this->deliveries = $deliveries;
    }

    /** Append every request from now on to $log too, see logged(). */
    public function logTo(string $log): void
    {
        $this->log = $log;
    }

    public function handler(): HandlerStack
    {
        return HandlerStack::create($this);
    }

    public function queen(): Queen
    {
        return new Queen([
            'url' => 'http://queen.test:6632',
            'retryAttempts' => 1,
            'retryDelayMillis' => 0,
            'handler' => $this->handler(),
        ]);
    }

    /**
     * One leased delivery of a queued command, as the broker hands it out.
     * $deliveryAttempt above the worker's --tries makes Laravel fail it
     * before it runs.
     */
    public static function delivery(object $command, string $id, int $deliveryAttempt = 1): array
    {
        return [
            'id' => 'message-' . $id,
            'transactionId' => 'transaction-' . $id,
            'partitionId' => '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
            'partition' => $id,
            'leaseId' => 'lease-' . $id,
            'consumerGroup' => 'workers',
            'deliveryAttempt' => $deliveryAttempt,
            'data' => [
                'uuid' => $id,
                'displayName' => $command::class,
                'job' => CallQueuedHandler::class . '@call',
                'maxTries' => null,
                'maxExceptions' => null,
                'failOnTimeout' => false,
                'backoff' => null,
                'timeout' => null,
                'retryUntil' => null,
                'data' => [
                    'commandName' => $command::class,
                    'command' => serialize($command),
                ],
                'createdAt' => time(),
                '_queen' => ['partition' => $id, 'attempts' => 0],
            ],
        ];
    }

    /** A broker holding the key/value writes the log recorded, from the processes $pids, or all. */
    public static function fromLog(string $log, ?array $pids = null): self
    {
        $broker = new self();
        foreach (self::logged($log) as $request) {
            if ($pids === null || in_array($request['pid'], $pids, true)) {
                $broker->apply($request['path'], $request['body']);
            }
        }

        return $broker;
    }

    /** @return list<array{pid: int, method: string, path: string, body: mixed}> */
    public static function logged(string $log): array
    {
        $lines = is_file($log) ? file($log, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) : [];

        return array_map(static fn (string $line): array => json_decode($line, true, 512, JSON_THROW_ON_ERROR), $lines ?: []);
    }

    /**
     * Every key/value put whose key starts with $prefix, in order.
     *
     * @return list<array{key: string, value: mixed, ttlSeconds: mixed}>
     */
    public function puts(string $prefix = 'jobs/v1/'): array
    {
        return self::putsIn($this->requests, $prefix);
    }

    /**
     * @param list<array{path: string, body: mixed}> $requests
     * @return list<array{pid: int, key: string, value: mixed, ttlSeconds: mixed}>
     */
    public static function putsIn(array $requests, string $prefix = 'jobs/v1/'): array
    {
        $puts = [];
        foreach ($requests as $request) {
            if ($request['path'] !== '/api/v1/kv') {
                continue;
            }
            foreach ($request['body']['operations'] ?? [] as $operation) {
                if (($operation['op'] ?? null) === 'put' && str_starts_with((string) $operation['key'], $prefix)) {
                    $puts[] = [
                        'pid' => $request['pid'] ?? 0,
                        'key' => $operation['key'],
                        'value' => $operation['value'],
                        'ttlSeconds' => $operation['ttlSeconds'] ?? null,
                    ];
                }
            }
        }

        return $puts;
    }

    public function __invoke(RequestInterface $request, array $options): PromiseInterface
    {
        $path = $request->getUri()->getPath();
        $body = json_decode((string) $request->getBody(), true);
        $entry = ['pid' => getmypid(), 'method' => $request->getMethod(), 'path' => $path, 'body' => $body];
        $this->requests[] = $entry;
        if ($this->log !== null) {
            file_put_contents($this->log, json_encode($entry, JSON_THROW_ON_ERROR) . "\n", FILE_APPEND | LOCK_EX);
        }

        return self::json($this->apply($path, $body));
    }

    private function apply(string $path, mixed $body): mixed
    {
        if (str_starts_with($path, '/api/v1/pop')) {
            $delivery = array_shift($this->deliveries);

            return $delivery === null
                ? ['success' => true, 'messages' => []]
                : [
                    'success' => true,
                    'queue' => 'default',
                    'leaseId' => $delivery['leaseId'],
                    'consumerGroup' => $delivery['consumerGroup'],
                    'messages' => [$delivery],
                ];
        }
        if (str_starts_with($path, '/api/v1/ack')) {
            return [['success' => true, 'leaseReleased' => true]];
        }
        if ($path === '/api/v1/transaction') {
            return ['success' => true, 'transactionId' => 'recorded'];
        }
        if ($path === '/api/v1/kv') {
            return ['results' => array_map(fn (array $operation): array => $this->operation($operation), $body['operations'] ?? [])];
        }
        if ($path === '/api/v1/resources/kv/list') {
            return $this->rows((string) ($body['namespace'] ?? ''), (string) ($body['prefix'] ?? ''), (string) ($body['after'] ?? ''), (int) ($body['limit'] ?? 1000));
        }

        return [];
    }

    private function operation(array $operation): array
    {
        $namespace = (string) ($operation['ns'] ?? '');
        if (($operation['op'] ?? null) === 'put') {
            $this->kv[$namespace][(string) $operation['key']] = $operation['value'];

            return ['applied' => true];
        }
        if (($operation['op'] ?? null) === 'getPrefix') {
            return $this->rows($namespace, (string) $operation['prefix'], (string) ($operation['after'] ?? ''), (int) ($operation['limit'] ?? 1000));
        }

        return [];
    }

    /** @return array{rows: list<array{key: string, value: mixed}>, truncated: bool} */
    private function rows(string $namespace, string $prefix, string $after, int $limit): array
    {
        $keys = array_filter(
            array_keys($this->kv[$namespace] ?? []),
            static fn (string $key): bool => str_starts_with($key, $prefix) && strcmp($key, $after) > 0,
        );
        sort($keys, SORT_STRING);
        $rows = array_map(fn (string $key): array => ['key' => $key, 'value' => $this->kv[$namespace][$key]], array_slice($keys, 0, $limit));

        return ['rows' => $rows, 'truncated' => count($keys) > $limit];
    }

    private static function json(mixed $body): PromiseInterface
    {
        return new FulfilledPromise(new Response(200, ['Content-Type' => 'application/json'], json_encode($body)));
    }
}
