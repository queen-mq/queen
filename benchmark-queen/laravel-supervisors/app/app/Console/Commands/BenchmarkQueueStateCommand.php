<?php

namespace App\Console\Commands;

use DateTimeImmutable;
use DateTimeZone;
use Illuminate\Console\Command;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use InvalidArgumentException;
use JsonException;
use RuntimeException;
use Throwable;

final class BenchmarkQueueStateCommand extends Command
{
    private const SCHEMA = 'queen.laravel-supervisors.queue-state/v1';

    protected $signature = 'bench:queue-state
        {--run-id= : Run identifier associated with this observation}
        {--connection= : Queue connection; defaults to BENCH_CONNECTION}
        {--queue= : Queue name; defaults to BENCH_QUEUE}
        {--wait=30 : Maximum seconds to wait for quiescence}
        {--poll-ms=100 : Poll interval while waiting}
        {--settle-ms=1000 : Continuous empty interval required for success}';

    protected $description = 'Wait for and print a normalized final queue state';

    public function handle(QueueFactory $queues): int
    {
        $runId = $this->stringOption('run-id', 'unknown');
        $connectionName = $this->stringOption('connection', (string) config('benchmark.connection'));
        $queueName = $this->stringOption('queue', (string) config('benchmark.queue'));
        $this->identifier($runId, 'run-id');
        $this->identifier($connectionName, 'connection');
        $this->identifier($queueName, 'queue', forbidComma: true);

        $waitSeconds = $this->integerOption('wait', 0, 86_400);
        $pollMs = $this->integerOption('poll-ms', 10, 60_000);
        $settleMs = $this->integerOption('settle-ms', 0, 60_000);
        $startedAt = hrtime(true);
        $deadline = $startedAt + ($waitSeconds * 1_000_000_000);
        $settleNs = $settleMs * 1_000_000;
        $emptySince = null;
        $observation = $this->emptyObservation();
        $checks = 0;
        $probeErrorCount = 0;
        $lastProbeError = null;
        $quiescent = false;

        do {
            ++$checks;
            try {
                $observation = $this->probe($queues, $connectionName, $queueName);
                if ($observation['errors'] !== []) {
                    $probeErrorCount += count($observation['errors']);
                    $lastProbeError = implode('; ', $observation['errors']);
                }
            } catch (Throwable $exception) {
                ++$probeErrorCount;
                $lastProbeError = $exception::class.': '.$exception->getMessage();
                $observation = $this->emptyObservation([$lastProbeError]);
            }

            $observedAt = hrtime(true);
            if ($this->isEmpty($observation)) {
                $emptySince ??= $observedAt;
                if ($observedAt - $emptySince >= $settleNs) {
                    $quiescent = true;
                    break;
                }
            } else {
                $emptySince = null;
            }

            $remainingNs = $deadline - $observedAt;
            if ($remainingNs <= 0) {
                break;
            }
            usleep((int) min($pollMs * 1000, max(1, intdiv($remainingNs + 999, 1000))));
        } while (true);

        $finishedAt = hrtime(true);
        $payload = [
            'schema' => self::SCHEMA,
            'run_id' => $runId,
            'connection' => $connectionName,
            'queue' => $queueName,
            'implementation' => $observation['implementation'],
            'captured_at' => (new DateTimeImmutable('now', new DateTimeZone('UTC')))
                ->format('Y-m-d\TH:i:s.u\Z'),
            'started_at_ns' => $startedAt,
            'finished_at_ns' => $finishedAt,
            'elapsed_ns' => $finishedAt - $startedAt,
            'wait_ns' => $waitSeconds * 1_000_000_000,
            'poll_ns' => $pollMs * 1_000_000,
            'settle_ns' => $settleNs,
            'settled_for_ns' => $emptySince === null ? 0 : max(0, $finishedAt - $emptySince),
            'checks' => $checks,
            'quiescent' => $quiescent,
            'timed_out' => !$quiescent,
            'state' => $observation['state'],
            'supported' => $observation['supported'],
            'probe_errors' => $observation['errors'],
            'probe_error_count' => $probeErrorCount,
            'last_probe_error' => $lastProbeError,
        ];

        $this->line($this->json($payload));

        return $quiescent ? self::SUCCESS : self::FAILURE;
    }

    /**
     * @return array{
     *     implementation: string|null,
     *     state: array{size: int|null, ready: int|null, reserved: int|null, delayed: int|null},
     *     supported: array{ready: bool, reserved: bool, delayed: bool},
     *     errors: list<string>
     * }
     */
    private function probe(QueueFactory $queues, string $connectionName, string $queueName): array
    {
        $connection = $queues->connection($connectionName);
        $state = [
            'size' => $this->count($connection->size($queueName), 'size'),
            'ready' => null,
            'reserved' => null,
            'delayed' => null,
        ];
        $supported = ['ready' => false, 'reserved' => false, 'delayed' => false];
        $errors = [];

        foreach ([
            'ready' => 'pendingSize',
            'reserved' => 'reservedSize',
            'delayed' => 'delayedSize',
        ] as $label => $method) {
            if (!is_callable([$connection, $method])) {
                continue;
            }
            $supported[$label] = true;
            try {
                $state[$label] = $this->count($connection->{$method}($queueName), $label);
            } catch (Throwable $exception) {
                $errors[] = $label.': '.$exception::class.': '.$exception->getMessage();
            }
        }

        return [
            'implementation' => $connection::class,
            'state' => $state,
            'supported' => $supported,
            'errors' => $errors,
        ];
    }

    /**
     * @param array{
     *     implementation: string|null,
     *     state: array{size: int|null, ready: int|null, reserved: int|null, delayed: int|null},
     *     supported: array{ready: bool, reserved: bool, delayed: bool},
     *     errors: list<string>
     * } $observation
     */
    private function isEmpty(array $observation): bool
    {
        if ($observation['errors'] !== [] || $observation['state']['size'] !== 0) {
            return false;
        }
        foreach (['ready', 'reserved', 'delayed'] as $label) {
            if ($observation['supported'][$label] && $observation['state'][$label] !== 0) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param list<string> $errors
     * @return array{
     *     implementation: null,
     *     state: array{size: null, ready: null, reserved: null, delayed: null},
     *     supported: array{ready: false, reserved: false, delayed: false},
     *     errors: list<string>
     * }
     */
    private function emptyObservation(array $errors = []): array
    {
        return [
            'implementation' => null,
            'state' => ['size' => null, 'ready' => null, 'reserved' => null, 'delayed' => null],
            'supported' => ['ready' => false, 'reserved' => false, 'delayed' => false],
            'errors' => $errors,
        ];
    }

    private function count(mixed $value, string $label): int
    {
        if (!is_int($value) || $value < 0) {
            throw new RuntimeException("Queue {$label} must be a non-negative integer.");
        }

        return $value;
    }

    private function integerOption(string $name, int $minimum, int $maximum): int
    {
        $raw = $this->option($name);
        if (!is_string($raw) || preg_match('/^(0|[1-9][0-9]*)$/D', $raw) !== 1) {
            throw new InvalidArgumentException("--{$name} must be an integer in {$minimum}..{$maximum}.");
        }

        $value = filter_var($raw, FILTER_VALIDATE_INT);
        if ($value === false || $value < $minimum || $value > $maximum) {
            throw new InvalidArgumentException("--{$name} must be an integer in {$minimum}..{$maximum}.");
        }

        return $value;
    }

    private function stringOption(string $name, string $default): string
    {
        $value = $this->option($name);

        return is_string($value) && $value !== '' ? $value : $default;
    }

    private function identifier(string $value, string $label, bool $forbidComma = false): void
    {
        if (strlen($value) > 128
            || preg_match('/^[A-Za-z0-9._:-]+$/D', $value) !== 1
            || ($forbidComma && str_contains($value, ','))) {
            throw new InvalidArgumentException(
                "--{$label} must be 1..128 ASCII letters, digits, dot, underscore, colon or dash.",
            );
        }
    }

    /** @param array<string, mixed> $value */
    private function json(array $value): string
    {
        try {
            return json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
        } catch (JsonException $exception) {
            throw new RuntimeException('Unable to encode queue state.', previous: $exception);
        }
    }
}
