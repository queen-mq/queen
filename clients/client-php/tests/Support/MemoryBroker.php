<?php

namespace Queen\Tests\Support;

use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\RequestInterface;

/**
 * A broker in memory, as a Guzzle handler: what is pushed to a queue is
 * popped from it, in order, once; acknowledgements are recorded; a timer
 * (Laravel's later()) is recorded, not fired. Enough for a job to travel
 * from a dispatch to a worker and back, with every request kept for the
 * assertions.
 */
final class MemoryBroker
{
    /** @var list<RequestInterface> */
    public array $requests = [];

    /** @var array<string, list<array<string, mixed>>> push items not yet popped, by queue */
    private array $ready = [];

    private int $nextId = 0;

    public function __invoke(RequestInterface $request, array $options): PromiseInterface
    {
        $this->requests[] = $request;
        $path = $request->getUri()->getPath();
        $body = json_decode((string) $request->getBody(), true);

        return match (true) {
            $path === '/api/v1/push' => $this->push($body['items'] ?? []),
            $path === '/api/v1/timers' => self::json(200, ['results' => array_map(
                static fn (array $operation): array => [
                    'ok' => true,
                    'status' => 'scheduled',
                    'queue' => $operation['queue'] ?? null,
                    'txn' => $operation['txn'] ?? null,
                ],
                $body['operations'] ?? [],
            )]),
            str_starts_with($path, '/api/v1/pop/queue/') => $this->pop(rawurldecode(substr($path, strlen('/api/v1/pop/queue/')))),
            $path === '/api/v1/ack/batch' => self::json(200, array_map(
                static fn (): array => ['success' => true, 'leaseReleased' => true],
                $body['acknowledgments'] ?? $body['items'] ?? [[]],
            )),
            $path === '/api/v1/ack' => self::json(200, ['success' => true, 'leaseReleased' => true]),
            default => self::json(200, ['success' => true, 'transactionId' => 'recorded', 'results' => []]),
        };
    }

    /** @return list<RequestInterface> the requests to $path, in order */
    public function requestsTo(string $path): array
    {
        return array_values(array_filter(
            $this->requests,
            static fn (RequestInterface $request): bool => $request->getUri()->getPath() === $path,
        ));
    }

    /** @return list<array<string, mixed>> every item pushed, in order */
    public function pushed(): array
    {
        $items = [];
        foreach ($this->requestsTo('/api/v1/push') as $request) {
            array_push($items, ...(json_decode((string) $request->getBody(), true)['items'] ?? []));
        }

        return $items;
    }

    /** @return list<array<string, mixed>> the query of every pop, in order, with its queue */
    public function pops(): array
    {
        $pops = [];
        foreach ($this->requests as $request) {
            $path = $request->getUri()->getPath();
            if (str_starts_with($path, '/api/v1/pop/queue/')) {
                parse_str($request->getUri()->getQuery(), $query);
                $pops[] = ['queue' => rawurldecode(substr($path, strlen('/api/v1/pop/queue/'))), ...$query];
            }
        }

        return $pops;
    }

    /** @param list<array<string, mixed>> $items */
    private function push(array $items): PromiseInterface
    {
        foreach ($items as $item) {
            $this->ready[(string) ($item['queue'] ?? '')][] = $item;
        }

        return self::json(201, array_map(static fn (): array => ['status' => 'queued'], $items));
    }

    private function pop(string $queue): PromiseInterface
    {
        $item = array_shift($this->ready[$queue]);
        if ($item === null) {
            // An empty pop is a bodiless 204.
            return new FulfilledPromise(new Response(204));
        }
        $id = ++$this->nextId;
        $leaseId = "lease-{$id}";

        return self::json(200, [
            'success' => true,
            'queue' => $queue,
            'leaseId' => $leaseId,
            'messages' => [[
                'id' => "message-{$id}",
                'transactionId' => $item['transactionId'] ?? "transaction-{$id}",
                'partitionId' => sprintf('0198f2c1-4d3a-7c10-9f2b-%012d', $id),
                'partition' => $item['partition'] ?? 'Default',
                'leaseId' => $leaseId,
                'deliveryAttempt' => 1,
                'data' => $item['payload'] ?? [],
            ]],
        ]);
    }

    private static function json(int $status, mixed $body): PromiseInterface
    {
        return new FulfilledPromise(new Response($status, ['Content-Type' => 'application/json'], json_encode($body)));
    }
}
