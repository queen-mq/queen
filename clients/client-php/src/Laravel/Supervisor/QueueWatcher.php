<?php

namespace Queen\Laravel\Supervisor;

use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;

/**
 * Event-driven scaling for the PHP engine: wakes the master when a watched
 * queue receives jobs, instead of the master waiting for its next poll.
 *
 * Queen has no push channel, but `POST /api/v1/fetch` parks until one of the
 * listed partitions grows. It takes no lease and moves no consumer cursor, so
 * watching a queue never takes a job from a worker. One watcher per
 * connection lists the Laravel partition stripes of every autoscaling pool. A
 * job pushed outside the stripes (QueenPartitionable) is found by the regular
 * poll, as before. A queue that does not exist yet would release the long
 * poll at once, so its lanes are left out and probed again every
 * MISSING_REPROBE_SECONDS.
 *
 * A grown partition answers with the segment that holds its first new
 * record, payloads included, so an answer larger than MAX_ANSWER_BYTES is not
 * read: it counts as growth on every lane.
 *
 * The Rust engine does the same in a thread (supervisor/src/watch.rs); this
 * engine parks inside its loop for at most WAIT_MILLIS at a time, so control
 * commands and signals are still handled within about a second.
 */
final class QueueWatcher
{
    /** The shortest time between two wakes, and between two event-driven evaluations of one pool. */
    public const WAKE_INTERVAL_SECONDS = 1.0;

    /** How long one loop iteration parks on the broker. */
    public const WAIT_MILLIS = 1000;

    /** An offset past every log: the broker answers it at once with the bounds. */
    public const PROBE_OFFSET = 4611686018427387903;

    /** The broker's limit of entries per fetch. */
    public const MAX_ENTRIES = 1024;

    public const MAX_ANSWER_BYTES = 8 * 1024 * 1024;

    private const RETRY_SECONDS = 5.0;

    private const MISSING_REPROBE_SECONDS = 30.0;

    private const OUT_OF_RANGE = 'OFFSET_OUT_OF_RANGE';

    /** Exception code: no endpoint can serve the watcher. */
    private const UNAVAILABLE = 1;

    /** @var list<array{queue: string, partition: string}> the lanes of queues that exist */
    private array $armed = [];

    /** @var list<int> the high watermark of every armed lane, index-aligned */
    private array $highs = [];

    private bool $probed = false;

    /** Set while some lanes' queues did not exist: when to probe them again. */
    private ?float $reprobeAt = null;

    private bool $off = false;

    private float $idleUntil = 0.0;

    private Client $http;

    /**
     * @param array{urls: list<string>, bearer_token?: ?string, headers?: array<string, string>} $connection
     * @param list<array{queue: string, partition: string}> $lanes at most MAX_ENTRIES
     * @param (\Closure(string, string): void)|null $output
     * @param (\Closure(): float)|null $clock
     * @param callable|null $handler a Guzzle handler, for tests
     */
    public function __construct(
        private array $connection,
        private string $name,
        private array $lanes,
        private float $timeoutSeconds,
        private ?\Closure $output = null,
        private ?\Closure $clock = null,
        ?callable $handler = null,
    ) {
        $this->lanes = array_slice($lanes, 0, self::MAX_ENTRIES);
        $this->http = new Client($handler !== null ? ['handler' => HandlerStack::create($handler)] : []);
    }

    /**
     * The lanes to watch per connection: every stripe of every queue of the
     * pools whose worker count follows the backlog, in pool-name order.
     *
     * @param array<string, mixed> $config the resolved engine contract
     * @return array<string, list<array{queue: string, partition: string}>>
     */
    public static function lanes(array $config): array
    {
        $stripes = $config['event_driven']['stripes'] ?? [];
        $supervisors = $config['supervisors'] ?? [];
        ksort($supervisors);
        $queues = [];
        foreach ($supervisors as $options) {
            if (!self::followsBacklog($options)) {
                continue;
            }
            foreach ($options['queues'] as $queue) {
                $queues[$options['connection']][$queue] = true;
            }
        }
        $lanes = [];
        foreach ($queues as $connection => $names) {
            $stripe = $stripes[$connection] ?? null;
            if (!is_array($stripe)) {
                continue;
            }
            foreach (array_keys($names) as $queue) {
                for ($slot = 0; $slot < $stripe['count']; ++$slot) {
                    $lanes[$connection][] = ['queue' => (string) $queue, 'partition' => sprintf('%s-%04d', $stripe['prefix'], $slot)];
                }
            }
        }

        return $lanes;
    }

    /** @param array<string, mixed> $options */
    public static function followsBacklog(array $options): bool
    {
        return $options['balance'] !== 'simple' && $options['min_processes'] < $options['max_processes'];
    }

    public static function key(string $connection, string $queue): string
    {
        return $connection . "\0" . $queue;
    }

    /** False once no endpoint can serve this watcher, or while it backs off or rests after a wake. */
    public function ready(): bool
    {
        return !$this->off && $this->now() >= $this->idleUntil;
    }

    /**
     * Probe the bounds when they are unknown or due again, otherwise park up
     * to $waitMillis until a lane grows.
     *
     * @return list<string> the queues that grew
     */
    public function wait(int $waitMillis): array
    {
        if (!$this->ready()) {
            return [];
        }
        $started = $this->now();
        try {
            if (!$this->probed || ($this->reprobeAt !== null && $started >= $this->reprobeAt)) {
                return $this->probe($started);
            }
            $answer = $this->send($this->entries($this->armed, $this->highs), $waitMillis);
            if ($answer === null) {
                // Too much arrived to read: every lane may have grown.
                $this->probed = false;
                $grown = array_values(array_unique(array_column($this->armed, 'queue')));
            } else {
                [$grown, $this->highs, $broken] = self::grown($this->armed, $this->highs, $answer);
                $this->probed = !$broken;
            }
            // After a wake, and after a long poll released early without
            // growth, rest a wake interval: never a busy loop on the broker.
            $this->idleUntil = $grown !== []
                ? $this->now() + self::WAKE_INTERVAL_SECONDS
                : max($this->idleUntil, $started + self::WAKE_INTERVAL_SECONDS);

            return $grown;
        } catch (\Throwable $error) {
            if ($error->getCode() === self::UNAVAILABLE && $error instanceof \DomainException) {
                $this->off = true;
                $this->emit("event-driven: watching connection [{$this->name}] is off, polling continues: {$error->getMessage()}\n");

                return [];
            }
            $this->probed = false;
            $this->idleUntil = $this->now() + self::RETRY_SECONDS;
            $this->emit("event-driven: watching connection [{$this->name}] failed, retrying: {$error->getMessage()}\n");
        }

        return [];
    }

    /** @return list<string> the queues that grew since the last answer */
    private function probe(float $started): array
    {
        $answer = $this->send($this->entries($this->lanes, array_fill(0, count($this->lanes), self::PROBE_OFFSET)), 0);
        if ($answer === null) {
            throw new \RuntimeException('the bounds probe answer is larger than the watcher reads');
        }
        [$armed, $highs, $missing] = self::arm($this->lanes, $answer);
        // Growth between the last answer and this probe still wakes.
        $before = [];
        foreach ($this->armed as $index => $lane) {
            $before[$lane['queue'] . "\0" . $lane['partition']] = $this->highs[$index];
        }
        $grown = [];
        foreach ($armed as $index => $lane) {
            $old = $before[$lane['queue'] . "\0" . $lane['partition']] ?? null;
            if ($old !== null && $highs[$index] > $old) {
                $grown[$lane['queue']] = true;
            }
        }
        $this->armed = $armed;
        $this->highs = $highs;
        $this->probed = true;
        $this->reprobeAt = $missing ? $started + self::MISSING_REPROBE_SECONDS : null;
        if ($armed === []) {
            $this->idleUntil = $started + self::MISSING_REPROBE_SECONDS;
        }

        return array_map('strval', array_keys($grown));
    }

    /**
     * One fetch, every endpoint in turn. The watcher turns off only when none
     * can serve it; an oversized answer (null) is an answer, not an endpoint
     * failure.
     *
     * @param list<array<string, mixed>> $entries
     * @return array<string, mixed>|null
     */
    private function send(array $entries, int $waitMillis): ?array
    {
        $token = $this->connection['bearer_token'] ?? null;
        $headers = ['Content-Type' => 'application/json'];
        foreach ($this->connection['headers'] ?? [] as $header => $value) {
            if ($token === null || strcasecmp((string) $header, 'Authorization') !== 0) {
                $headers[$header] = $value;
            }
        }
        if ($token !== null) {
            $headers['Authorization'] = 'Bearer ' . $token;
        }
        $transient = null;
        $unavailable = null;
        foreach ($this->connection['urls'] as $url) {
            // A streamed body's timeout bounds each read, not the answer: a
            // connection gone silent mid-answer times every read out empty
            // and never ends. The whole answer is bounded instead, or the
            // master loop waiting on it stalls for good.
            $deadline = hrtime(true) + (int) (($this->timeoutSeconds + $waitMillis / 1000) * 1e9);
            try {
                $response = $this->http->request('POST', rtrim($url, '/') . '/api/v1/fetch', [
                    'body' => json_encode(['entries' => $entries, 'maxWaitMs' => $waitMillis], JSON_THROW_ON_ERROR),
                    'headers' => $headers,
                    'timeout' => $this->timeoutSeconds + $waitMillis / 1000,
                    'connect_timeout' => min(5.0, $this->timeoutSeconds),
                    'http_errors' => false,
                    // A broker endpoint is authoritative: a redirect could
                    // forward the bearer token to another host.
                    'allow_redirects' => false,
                    'stream' => true,
                ]);
            } catch (\Throwable $error) {
                $transient = $error;
                continue;
            }
            $status = $response->getStatusCode();
            if (in_array($status, [401, 403, 404], true)) {
                $unavailable = new \DomainException(
                    "POST /api/v1/fetch answered {$status}; the broker needs the fetch endpoint and a token that may consume",
                    self::UNAVAILABLE,
                );
                continue;
            }
            if ($status < 200 || $status >= 300) {
                $transient = new \RuntimeException("POST /api/v1/fetch answered {$status}");
                continue;
            }
            $length = $response->getHeaderLine('Content-Length');
            if ($length !== '' && (int) $length > self::MAX_ANSWER_BYTES) {
                return null;
            }
            $body = $response->getBody();
            $read = '';
            $stalled = false;
            while (!$body->eof() && strlen($read) <= self::MAX_ANSWER_BYTES) {
                if (hrtime(true) >= $deadline) {
                    $stalled = true;
                    break;
                }
                $read .= $body->read(65536);
            }
            if ($stalled) {
                $transient = new \RuntimeException('POST /api/v1/fetch stalled in the middle of its answer');
                continue;
            }
            if (strlen($read) > self::MAX_ANSWER_BYTES) {
                return null;
            }
            $answer = json_decode($read, true);
            if (!is_array($answer)) {
                throw new \UnexpectedValueException('The fetch answer is not a JSON object.');
            }

            return $answer;
        }

        throw $transient ?? $unavailable ?? new \DomainException('the connection has no URL', self::UNAVAILABLE);
    }

    /**
     * @param list<array{queue: string, partition: string}> $lanes
     * @param list<int> $offsets
     * @return list<array{queue: string, partition: string, offset: int, maxBytes: int}>
     */
    private function entries(array $lanes, array $offsets): array
    {
        $entries = [];
        foreach ($lanes as $index => $lane) {
            $entries[] = [...$lane, 'offset' => $offsets[$index], 'maxBytes' => 1];
        }

        return $entries;
    }

    /**
     * The lanes a probe found, with their high watermarks, and whether some
     * were left out: an unknown queue would release every long poll at once.
     *
     * @param list<array{queue: string, partition: string}> $lanes
     * @param array<string, mixed> $answer
     * @return array{0: list<array{queue: string, partition: string}>, 1: list<int>, 2: bool}
     */
    public static function arm(array $lanes, array $answer): array
    {
        $armed = [];
        $highs = [];
        $missing = false;
        foreach (self::fetched($answer, count($lanes)) as $index => $lane) {
            $error = $lane['error'] ?? null;
            if ($error !== null && $error !== self::OUT_OF_RANGE) {
                $missing = true;
                continue;
            }
            $armed[] = $lanes[$index];
            $highs[] = max(0, $lane['highWatermark']);
        }

        return [$armed, $highs, $missing];
    }

    /**
     * The queues whose lanes grew past the watermarks the fetch was armed
     * with, the watermarks to arm the next one with, and whether a lane
     * answered an error that calls for a new probe. A lane retention moved
     * past is re-armed at its new end without counting as growth.
     *
     * @param list<array{queue: string, partition: string}> $lanes
     * @param list<int> $armed
     * @param array<string, mixed> $answer
     * @return array{0: list<string>, 1: list<int>, 2: bool}
     */
    public static function grown(array $lanes, array $armed, array $answer): array
    {
        $grown = [];
        $highs = [];
        $broken = false;
        foreach (self::fetched($answer, count($lanes)) as $index => $lane) {
            $high = max(0, $lane['highWatermark']);
            $error = $lane['error'] ?? null;
            if ($error === null && $high > $armed[$index]) {
                $grown[$lanes[$index]['queue']] = true;
            } elseif ($error !== null && $error !== self::OUT_OF_RANGE) {
                $broken = true;
            }
            $highs[] = $high;
        }

        return [array_map('strval', array_keys($grown)), $highs, $broken];
    }

    /**
     * @param array<string, mixed> $answer
     * @return list<array{highWatermark: int, error?: string}>
     */
    private static function fetched(array $answer, int $expected): array
    {
        $entries = $answer['entries'] ?? null;
        if (!is_array($entries) || !array_is_list($entries) || count($entries) !== $expected) {
            throw new \UnexpectedValueException("The fetch answered for a different number of lanes than the {$expected} watched.");
        }
        foreach ($entries as $entry) {
            if (!is_array($entry) || !is_int($entry['highWatermark'] ?? null)
                || (isset($entry['error']) && !is_string($entry['error']))) {
                throw new \UnexpectedValueException('The fetch answered a malformed entry.');
            }
        }

        return $entries;
    }

    private function now(): float
    {
        return $this->clock !== null ? ($this->clock)() : microtime(true);
    }

    private function emit(string $line): void
    {
        if ($this->output !== null) {
            ($this->output)($line, 'err');
        }
    }
}
