<?php

namespace Queen\Tests\Support;

/**
 * tests/Fixtures/Supervisor/worker-invocation.json: for a few pools, the
 * document Laravel exports to the supervisor and what a worker of that pool
 * receives from it. The Rust master (supervisor/src/main.rs), the PHP engine
 * and the fork server are each tested against the same file, so a change to
 * one of them that the others do not follow fails here, not in production.
 */
final class WorkerInvocationFixture
{
    public const PATH = __DIR__ . '/../Fixtures/Supervisor/worker-invocation.json';

    /**
     * @return list<array{
     *     name: string,
     *     supervisor: string,
     *     queue: string,
     *     config: array<string, mixed>,
     *     queue_connections: array<string, array<string, mixed>>,
     *     pool: array<string, mixed>,
     *     arguments: list<string>,
     *     environment: array<string, string|true>,
     * }>
     */
    public static function cases(): array
    {
        $document = json_decode((string) file_get_contents(self::PATH), true, 512, JSON_THROW_ON_ERROR);

        // The application's configuration is one for every case.
        return array_map(
            static fn (array $case): array => $case + [
                'config' => $document['config'],
                'queue_connections' => $document['queue_connections'],
            ],
            $document['cases'],
        );
    }

    /** The value of `--queue=` among a worker's arguments. */
    public static function workerQueue(array $arguments): string
    {
        foreach ($arguments as $argument) {
            if (str_starts_with($argument, '--queue=')) {
                return substr($argument, strlen('--queue='));
            }
        }

        throw new \LogicException('The arguments carry no --queue.');
    }
}
