<?php

namespace Queen\Laravel\Queue;

use Queen\Queen;

/** @internal Executed by ProcessLeaseRenewer in a clean PHP subprocess. */
final class LeaseRenewalWorker
{
    private const RETRY_DELAY_MILLIS = 1000;

    private const MAX_ERROR_BYTES = 128;

    /**
     * Line-oriented worker protocol. Configuration and bearer credentials are
     * received over stdin, never exposed in argv or written to disk.
     */
    public static function main(): void
    {
        // Rust deliberately signals the Laravel worker's process group during
        // drain/shutdown. Leave that group before confirming readiness so the
        // helper can keep the active lease alive while the worker handles TERM
        // gracefully. The private pipe remains the parent-death fence.
        if (!function_exists('posix_setpgid') || !@posix_setpgid(0, 0)) {
            self::emit(['event' => 'startup_failed', 'error' => 'unable to isolate renewal process group']);
            return;
        }

        $line = fgets(STDIN);
        $init = is_string($line) ? json_decode($line, true) : null;
        if (!is_array($init) || ($init['command'] ?? null) !== 'init' || !is_array($init['client'] ?? null)) {
            self::emit(['event' => 'startup_failed', 'error' => 'invalid initialization message']);
            return;
        }

        try {
            $queen = new Queen($init['client']);
        } catch (\Throwable $exception) {
            self::emit(['event' => 'startup_failed', 'error' => $exception->getMessage()]);
            return;
        }

        $parentPid = (int) ($init['parent_pid'] ?? 0);
        $leaseSeconds = (int) ($init['lease_seconds'] ?? 0);
        $intervalMillis = (int) ($init['interval_millis'] ?? 0);
        $requestBudgetMillis = (int) ($init['request_budget_millis'] ?? 0);
        $killGraceMillis = (int) ($init['kill_grace_millis'] ?? 0);
        $safetyMarginMillis = (int) ($init['safety_margin_millis'] ?? 0);
        if (PHP_INT_SIZE < 8 || $parentPid < 1 || $leaseSeconds < 1 || $intervalMillis < 1
            || $requestBudgetMillis < 1 || $killGraceMillis < 0 || $safetyMarginMillis < 1) {
            self::emit(['event' => 'startup_failed', 'error' => 'invalid renewal timing']);
            return;
        }
        $initialReserveMillis = 2 * $requestBudgetMillis
            + self::RETRY_DELAY_MILLIS
            + $killGraceMillis
            + $safetyMarginMillis;

        stream_set_blocking(STDIN, false);
        // Startup/tracking handshakes are safety-critical and very low volume.
        // Unsafe diagnostics switch temporarily to a bounded best-effort write
        // only after TERM/KILL fencing has been armed.
        stream_set_blocking(STDOUT, true);
        if (!self::emit(['event' => 'ready'])) {
            return;
        }

        /** @var array<string, array{expires: int, next: int, kill_at: ?int, error: ?string}> $leases */
        $leases = [];
        while (true) {
            if (!self::readCommands(
                $leases,
                $intervalMillis,
                $initialReserveMillis,
            )) {
                return;
            }

            $now = self::monotonicMillis();
            foreach ($leases as $leaseId => &$state) {
                if ($state['kill_at'] !== null) {
                    if ($now >= $state['kill_at']) {
                        self::killParent($parentPid, SIGKILL);
                        return;
                    }
                    continue;
                }

                if ($now < $state['next']) {
                    continue;
                }

                // Do not begin an HTTP attempt that cannot finish before the
                // previous lease's safety boundary. TERM gives Laravel a brief
                // chance to finish and ACK; KILL fences a still-running handler
                // before another consumer may receive the lease.
                if ($now + $requestBudgetMillis + $safetyMarginMillis >= $state['expires']) {
                    self::markUnsafe(
                        $leases,
                        $leaseId,
                        $parentPid,
                        $killGraceMillis,
                        $safetyMarginMillis,
                        'renewal deadline exhausted',
                    );
                    continue;
                }

                $renewalStarted = $now;
                try {
                    $result = $queen->renew($leaseId, $leaseSeconds);
                    $success = is_array($result) && ($result['success'] ?? false) === true;
                    $error = $success
                        ? null
                        : self::boundedError((string) ($result['error'] ?? 'broker rejected lease renewal'));
                } catch (\Throwable $exception) {
                    $success = false;
                    $error = self::boundedError($exception->getMessage());
                }

                $now = self::monotonicMillis();
                if ($success) {
                    // The broker renewed during the request, never after the
                    // response reached us. Anchoring at request start is the
                    // conservative monotonic equivalent and avoids adding RTT
                    // to the real server deadline.
                    $state['expires'] = $renewalStarted + $leaseSeconds * 1000;
                    $state['next'] = min(
                        $now + $intervalMillis,
                        max($now, $state['expires'] - $initialReserveMillis),
                    );
                    $state['error'] = null;
                    continue;
                }

                // An ACK racing this request may have closed the lease and
                // queued `forget`. Drain it before treating success:false as a
                // live-job failure.
                unset($state);
                if (!self::readCommands(
                    $leases,
                    $intervalMillis,
                    $initialReserveMillis,
                )) {
                    return;
                }
                if (!isset($leases[$leaseId])) {
                    continue;
                }

                $leases[$leaseId]['error'] = $error;
                $leases[$leaseId]['next'] = $now + self::RETRY_DELAY_MILLIS;
                if ($leases[$leaseId]['next'] + $requestBudgetMillis + $safetyMarginMillis
                    >= $leases[$leaseId]['expires']) {
                    self::markUnsafe(
                        $leases,
                        $leaseId,
                        $parentPid,
                        $killGraceMillis,
                        $safetyMarginMillis,
                        $error,
                    );
                }
            }
            unset($state);

            $waitMillis = 250;
            $now = self::monotonicMillis();
            foreach ($leases as $state) {
                $due = $state['kill_at'] ?? $state['next'];
                $waitMillis = min($waitMillis, max(0, (int) ceil($due - $now)));
            }

            $read = [STDIN];
            $write = null;
            $except = null;
            $seconds = intdiv($waitMillis, 1000);
            $micros = ($waitMillis % 1000) * 1000;
            $selected = @stream_select($read, $write, $except, $seconds, $micros);
            if ($selected === false) {
                // The scheduler can no longer guarantee that it will wake for
                // kill_at. Fence immediately; TERM alone is only a graceful
                // Laravel quit and may let the active job outlive its lease.
                self::killParent($parentPid, SIGKILL);
                return;
            }
        }
    }

    /**
     * @param array<string, array{expires: int, next: int, kill_at: ?int, error: ?string}> $leases
     */
    private static function readCommands(
        array &$leases,
        int $intervalMillis,
        int $initialReserveMillis,
    ): bool
    {
        while (($line = fgets(STDIN)) !== false) {
            $command = json_decode($line, true);
            if (!is_array($command)) {
                continue;
            }
            if (($command['command'] ?? null) === 'shutdown') {
                return false;
            }

            $leaseId = $command['lease_id'] ?? null;
            if (!is_string($leaseId) || $leaseId === '') {
                continue;
            }
            if (($command['command'] ?? null) === 'forget') {
                unset($leases[$leaseId]);
                continue;
            }
            if (($command['command'] ?? null) === 'track') {
                $deadline = $command['deadline_monotonic_millis'] ?? null;
                if (!is_int($deadline) || $deadline < 1) {
                    if (!self::emit([
                        'event' => 'unsafe',
                        'lease_id' => $leaseId,
                        'error' => 'invalid monotonic lease deadline',
                    ])) {
                        return false;
                    }
                    continue;
                }
                if (isset($leases[$leaseId])) {
                    if (!self::emit(['event' => 'tracked', 'lease_id' => $leaseId])) {
                        return false;
                    }
                    continue;
                }
                $now = self::monotonicMillis();
                $leases[$leaseId] = [
                    'expires' => $deadline,
                    'next' => min(
                        $now + $intervalMillis,
                        max($now, $deadline - $initialReserveMillis),
                    ),
                    'kill_at' => null,
                    'error' => null,
                ];
                if (!self::emit(['event' => 'tracked', 'lease_id' => $leaseId])) {
                    unset($leases[$leaseId]);
                    return false;
                }
            }
        }

        return !feof(STDIN);
    }

    /**
     * @param array<string, array{expires: int, next: int, kill_at: ?int, error: ?string}> $leases
     */
    private static function markUnsafe(
        array &$leases,
        string $leaseId,
        int $parentPid,
        int $killGraceMillis,
        int $safetyMarginMillis,
        string $error,
    ): void {
        if (!isset($leases[$leaseId]) || $leases[$leaseId]['kill_at'] !== null) {
            return;
        }

        // Arm the fence before touching the diagnostic pipe. The parent is
        // normally executing user code and may not read stdout until the job
        // returns, so safety must never depend on pipe capacity.
        self::killParent($parentPid, SIGTERM);
        $now = self::monotonicMillis();
        $lastSafeInstant = $leases[$leaseId]['expires'] - $safetyMarginMillis;
        $leases[$leaseId]['kill_at'] = min($now + $killGraceMillis, $lastSafeInstant);
        self::emitBestEffort([
            'event' => 'unsafe',
            'lease_id' => $leaseId,
            'error' => self::boundedError($error),
        ]);
    }

    private static function killParent(int $parentPid, int $signal): void
    {
        // The pipe is the primary parent-death fence. The PPID check prevents
        // signalling an unrelated process if a PID is ever recycled.
        if (function_exists('posix_getppid')
            && function_exists('posix_kill')
            && posix_getppid() === $parentPid) {
            @posix_kill($parentPid, $signal);
        }
    }

    private static function emit(array $event): bool
    {
        $payload = json_encode($event, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR) . "\n";
        $offset = 0;
        $length = strlen($payload);
        while ($offset < $length) {
            $written = @fwrite(STDOUT, substr($payload, $offset));
            if ($written === false || $written === 0) {
                return false;
            }
            $offset += $written;
        }

        return @fflush(STDOUT);
    }

    /** Emit one bounded, atomic best-effort diagnostic without blocking fencing. */
    private static function emitBestEffort(array $event): void
    {
        try {
            $payload = json_encode(
                $event,
                JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE | JSON_THROW_ON_ERROR,
            )."\n";
        } catch (\Throwable) {
            return;
        }

        // Lease IDs are capped at 255 bytes and errors at MAX_ERROR_BYTES, so
        // this stays well below PIPE_BUF on supported Unix systems. A single
        // non-blocking write is therefore all-or-nothing for the helper pipe.
        if (strlen($payload) > 1024) {
            return;
        }
        @stream_set_blocking(STDOUT, false);
        @fwrite(STDOUT, $payload);
        @fflush(STDOUT);
        @stream_set_blocking(STDOUT, true);
    }

    private static function boundedError(string $error): string
    {
        $printable = preg_replace('/[^\x20-\x7E]/', '?', $error);
        $printable = is_string($printable) ? $printable : 'lease renewal failed';

        return substr($printable, 0, self::MAX_ERROR_BYTES);
    }

    private static function monotonicMillis(): int
    {
        return intdiv(hrtime(true), 1_000_000);
    }
}
