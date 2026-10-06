<?php

namespace Queen\Laravel\Queue;

use InvalidArgumentException;

/**
 * Builds the lease renewer of a Laravel consumer.
 *
 * The queue connection's workers (QueenConnector) and queen:consume both build
 * theirs here, so both read the lease_renewal_* keys the same way, refuse the
 * same unsafe timing, and get the same renewer: the native supervisor's lease
 * service when the worker has one, else a helper process. The renewer is lazy:
 * nothing starts until the first lease is tracked, so a web process that only
 * dispatches never pays for it.
 *
 * The container holds one (QueenServiceProvider), which is how a test gives
 * queen:consume a renewer of its own: lease renewal refuses the internal test
 * HTTP handler, since the helper builds its own client.
 */
class LeaseRenewerFactory
{
    /**
     * The lease_renewal_* keys of a queen connection or of config/queen.php,
     * validated. Read even where renewal is off, so a bad value fails at start.
     * An unset or empty interval is a third of the lease.
     *
     * @return array{interval: int, timeout: int, killGrace: int, safetyMargin: int}
     */
    public static function timing(array $config, int $leaseSeconds): array
    {
        $interval = $config['lease_renewal_interval'] ?? null;

        return [
            'interval' => QueenConnector::boundedInteger(
                $interval === null || $interval === '' ? max(1, intdiv($leaseSeconds, 3)) : $interval,
                'lease_renewal_interval',
                1,
                PHP_INT_MAX,
            ),
            'timeout' => QueenConnector::boundedInteger(
                $config['lease_renewal_timeout'] ?? 5,
                'lease_renewal_timeout',
                1,
                PHP_INT_MAX,
            ),
            'killGrace' => QueenConnector::boundedInteger(
                $config['lease_renewal_kill_grace'] ?? 2,
                'lease_renewal_kill_grace',
                0,
                PHP_INT_MAX,
            ),
            'safetyMargin' => QueenConnector::boundedInteger(
                $config['lease_renewal_safety_margin'] ?? 1,
                'lease_renewal_safety_margin',
                1,
                PHP_INT_MAX,
            ),
        ];
    }

    /**
     * Refuse a timing that does not fit in the lease, then build the renewer.
     *
     * @param array $clientConfig the Queen client configuration the renewer connects with
     * @param array{interval: int, timeout: int, killGrace: int, safetyMargin: int} $timing from timing()
     * @param string $leaseName how the error names the lease: retry_after, --lease
     */
    public function make(array $clientConfig, int $leaseSeconds, array $timing, string $leaseName = 'retry_after'): LeaseRenewer
    {
        if (array_key_exists('handler', $clientConfig)) {
            throw new InvalidArgumentException(
                'Queen Laravel lease_renewal cannot use the internal test HTTP handler override.',
            );
        }

        $urls = $clientConfig['urls'] ?? null;
        $backendCount = is_array($urls) && $urls !== [] ? count($urls) : 1;
        if ($timing['timeout'] > intdiv(PHP_INT_MAX, $backendCount)) {
            throw new InvalidArgumentException('Queen Laravel lease renewal request budget is too large.');
        }
        $requestBudget = $timing['timeout'] * $backendCount;
        // One scheduled attempt plus one bounded retry must fit before a
        // TERM/KILL fence and the previous lease's safety margin.
        if (!self::sumIsBelow(
            [
                $timing['interval'],
                $requestBudget,
                $requestBudget,
                1,
                $timing['killGrace'],
                $timing['safetyMargin'],
            ],
            $leaseSeconds,
        )) {
            throw new InvalidArgumentException(
                "Queen Laravel lease_renewal timing is unsafe: interval + two request budgets + retry + kill grace + safety margin must be shorter than {$leaseName}.",
            );
        }

        $renewerTiming = [
            $leaseSeconds,
            $timing['interval'],
            $timing['timeout'],
            $requestBudget,
            $timing['killGrace'],
            $timing['safetyMargin'],
        ];

        return new LazyLeaseRenewer(
            static function () use ($clientConfig, $renewerTiming): LeaseRenewer {
                // The native supervisor serves renewal for its workers; a
                // worker it refuses renews through its own helper.
                $socket = getenv('QUEEN_SUPERVISOR_LEASE_SOCKET');
                if (is_string($socket) && $socket !== '') {
                    try {
                        return new SupervisorLeaseRenewer($socket, $clientConfig, ...$renewerTiming);
                    } catch (\Throwable $exception) {
                        error_log('Queen lease renewal falls back to a helper process: ' . $exception->getMessage());
                    }
                }

                return new ProcessLeaseRenewer($clientConfig, ...$renewerTiming);
            },
        );
    }

    /** @param list<int> $values */
    private static function sumIsBelow(array $values, int $limit): bool
    {
        $sum = 0;
        foreach ($values as $value) {
            if ($value >= $limit - $sum) {
                return false;
            }
            $sum += $value;
        }

        return true;
    }
}
