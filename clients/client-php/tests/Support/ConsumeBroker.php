<?php

namespace Queen\Tests\Support;

use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Promise\RejectedPromise;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\RequestInterface;
use Queen\Queen;

/**
 * A scripted broker for queen:consume: pops are answered from $pops in order,
 * acks from $acks (or accepted), and a pop past the script fails the test
 * instead of letting a broken loop spin forever.
 *
 * A pop answer is a list of messages, a Throwable to reject the request with,
 * the string 'refused' for a connection error, or a Closure returning one of
 * those. Every request is kept, with its decoded body, in order.
 */
final class ConsumeBroker
{
    /** @var list<array{method: string, path: string, query: array, body: mixed}> */
    public array $requests = [];

    /** @var list<array> Every ack or nack body, in order. */
    public array $acks = [];

    private int $emptyPops = 0;

    /**
     * @param list<mixed> $pops
     * @param list<mixed> $ackAnswers JSON bodies for the acks, in order; accepted when absent.
     * @param int $emptyPopsAfterScript empty answers served once $pops ran out, before the test fails
     * @param int $emptyPopMicros how long each of those empty answers takes, as a short long poll
     */
    public function __construct(
        private array $pops = [],
        private array $ackAnswers = [],
        private int $emptyPopsAfterScript = 0,
        private int $emptyPopMicros = 0,
    ) {
    }

    public function queen(): Queen
    {
        return new Queen([
            'url' => 'http://queen.test:6632',
            'retryAttempts' => 1,
            'retryDelayMillis' => 0,
            'handler' => HandlerStack::create($this),
        ]);
    }

    public function __invoke(RequestInterface $request, array $options): PromiseInterface
    {
        parse_str($request->getUri()->getQuery(), $query);
        $body = json_decode((string) $request->getBody(), true);
        $path = $request->getUri()->getPath();
        $this->requests[] = ['method' => $request->getMethod(), 'path' => $path, 'query' => $query, 'body' => $body];

        if (str_starts_with($path, '/api/v1/pop')) {
            return $this->pop($request);
        }
        if (str_starts_with($path, '/api/v1/ack')) {
            $this->acks[] = $body;
            $answer = array_shift($this->ackAnswers) ?? $this->accepted($body);

            return self::json($answer);
        }

        return self::json([]);
    }

    /** @return list<array> The pop requests' query strings, in order. */
    public function pops(): array
    {
        return array_values(array_map(
            fn (array $request): array => $request['query'],
            array_filter($this->requests, fn (array $request): bool => str_starts_with($request['path'], '/api/v1/pop')),
        ));
    }

    /** One leased message as the broker hands it out. */
    public static function message(string $transactionId, string $leaseId = 'lease-1', array $data = ['n' => 1]): array
    {
        return [
            'id' => $transactionId,
            'transactionId' => $transactionId,
            'partitionId' => 'partition-1',
            'partition' => 'Default',
            'leaseId' => $leaseId,
            'consumerGroup' => 'ledger',
            'deliveryAttempt' => 1,
            'data' => $data,
        ];
    }

    /** A per-item ack answer that refuses every item with $error. */
    public static function refused(int $items, string $error): array
    {
        $answer = [];
        for ($index = 0; $index < $items; ++$index) {
            $answer[] = ['index' => $index, 'transactionId' => "tx-{$index}", 'success' => false, 'error' => $error];
        }

        return $answer;
    }

    private function pop(RequestInterface $request): PromiseInterface
    {
        if ($this->pops === []) {
            if ($this->emptyPops++ < $this->emptyPopsAfterScript) {
                usleep($this->emptyPopMicros);

                return self::json(['messages' => []]);
            }
            throw new \LogicException('ConsumeBroker: the test scripted no more pops.');
        }

        $answer = array_shift($this->pops);
        if ($answer instanceof \Closure) {
            $answer = $answer();
        }
        if ($answer === 'refused') {
            return new RejectedPromise(new ConnectException(
                'cURL error 7: Failed to connect to queen.test port 6632: Connection refused',
                $request,
            ));
        }
        if ($answer instanceof \Throwable) {
            return new RejectedPromise($answer);
        }

        return self::json(['messages' => $answer]);
    }

    private function accepted(mixed $body): array
    {
        $items = is_array($body['acknowledgments'] ?? null) ? $body['acknowledgments'] : [$body];
        $answer = [];
        foreach (array_values($items) as $index => $item) {
            $answer[] = [
                'index' => $index,
                'transactionId' => $item['transactionId'] ?? '',
                'success' => true,
                'error' => null,
            ];
        }

        return $answer;
    }

    private static function json(mixed $body): PromiseInterface
    {
        return new FulfilledPromise(new Response(200, ['Content-Type' => 'application/json'], json_encode($body)));
    }
}
