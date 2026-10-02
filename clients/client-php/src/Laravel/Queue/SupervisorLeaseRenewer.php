<?php

namespace Queen\Laravel\Queue;

use RuntimeException;

/**
 * Lease renewal served by the native supervisor master over a private Unix
 * socket, instead of one PHP helper process per worker.
 *
 * The master runs the helper's algorithm (LeaseRenewalWorker) on a thread per
 * worker and fences this process itself: SIGTERM, then SIGKILL, when a lease
 * cannot be renewed in time, and SIGKILL when the connection breaks while a
 * lease is tracked. A `shutdown` is the only way to stop renewal without a
 * fence. The supervisor exports QUEEN_SUPERVISOR_LEASE_SOCKET only on Linux,
 * where the worker also dies with its master.
 */
final class SupervisorLeaseRenewer implements LeaseRenewer
{
    use RenewsOneLease;

    private const CONNECT_TIMEOUT_SECONDS = 1.0;

    private const READY_TIMEOUT_MILLIS = 5000;

    private const WRITE_TIMEOUT_MILLIS = 1000;

    /** @var resource|null */
    private $socket = null;

    /** The process that owns the connection; a fork must not share it. */
    private ?int $owner = null;

    private string $buffer = '';

    /** @var list<array> */
    private array $events = [];

    private bool $closedByMaster = false;

    /** Whether the master sends a crashed worker's journal (its `ready` says so). */
    private bool $handsBack = false;

    private ?HandBackJournal $journal = null;

    public function __construct(
        private string $socketPath,
        private array $clientConfig,
        private int $leaseSeconds,
        private int $intervalSeconds,
        private int $requestTimeoutSeconds,
        private int $requestBudgetSeconds,
        private int $killGraceSeconds = 2,
        private int $safetyMarginSeconds = 1,
    ) {
        if (!self::isSupported()) {
            throw new RuntimeException('Queen supervisor lease renewal requires Unix sockets and 64-bit PHP.');
        }
        if ($socketPath === '' || $socketPath[0] !== '/') {
            throw new RuntimeException('Queen supervisor lease socket must be an absolute path.');
        }
        $maximumSeconds = intdiv(PHP_INT_MAX, 1000);
        foreach ([$leaseSeconds, $intervalSeconds, $requestTimeoutSeconds, $requestBudgetSeconds, $safetyMarginSeconds] as $seconds) {
            if ($seconds < 1 || $seconds > $maximumSeconds) {
                throw new RuntimeException('Queen Laravel lease renewal received invalid timing values.');
            }
        }
        if ($killGraceSeconds < 0 || $killGraceSeconds > $maximumSeconds) {
            throw new RuntimeException('Queen Laravel lease renewal received invalid timing values.');
        }
        $this->connect();
    }

    public static function isSupported(): bool
    {
        return DIRECTORY_SEPARATOR === '/'
            && PHP_INT_SIZE >= 8
            && function_exists('stream_socket_client')
            && function_exists('hrtime');
    }

    public function __destruct()
    {
        $this->close();
    }

    public function track(string $leaseId, int $deadlineMonotonicMillis): void
    {
        $this->validateLeaseId($leaseId);
        if ($this->owner !== getmypid()) {
            // A forked child inherited the parent's session: never speak on
            // it, since a `shutdown` would stop the parent's renewals.
            $this->abandonConnection();
            $this->connect();
        }
        if (isset($this->tracked[$leaseId])) {
            $this->assertHealthy($leaseId);
            return;
        }
        $this->assertTrackable($leaseId, $deadlineMonotonicMillis);

        $this->send([
            'command' => 'track',
            'lease_id' => $leaseId,
            'deadline_monotonic_millis' => $deadlineMonotonicMillis,
        ]);
        try {
            $this->waitUntilTracked($leaseId, $deadlineMonotonicMillis);
        } catch (\Throwable $exception) {
            // The registration outcome is ambiguous. `shutdown` stops every
            // renewal of this session without fencing this worker, as closing
            // the helper's pipe does.
            $this->close();
            throw $exception;
        }
        $this->tracked[$leaseId] = true;
        $this->assertHealthy($leaseId);
    }

    public function forget(string $leaseId): void
    {
        // A forked child must not stop the renewal of its parent's lease.
        if (!isset($this->tracked[$leaseId]) || $this->owner !== getmypid()) {
            return;
        }

        unset($this->tracked[$leaseId], $this->failures[$leaseId]);
        if (is_resource($this->socket)) {
            try {
                $this->send(['command' => 'forget', 'lease_id' => $leaseId]);
            } catch (\Throwable) {
                // The lease is already settled here; a lost session is no
                // longer a safety issue for it.
            }
        }
    }

    public function assertHealthy(string $leaseId): void
    {
        if ($this->owner !== null && $this->owner !== getmypid()) {
            throw new RuntimeException('Queen lease renewal belongs to another process.');
        }
        $this->drain();
        $this->assertLeaseSafe($leaseId);
        if ($this->closedByMaster || !is_resource($this->socket)) {
            throw new RuntimeException('Queen supervisor lease renewal stopped unexpectedly.');
        }
    }

    public function close(): void
    {
        if (!is_resource($this->socket)) {
            $this->reset();
            return;
        }
        if ($this->owner === getmypid()) {
            // A clean exit owes nothing; a fork's journal is its parent's.
            $this->journal?->discard();
            if (!$this->closedByMaster) {
                try {
                    $this->send(['command' => 'shutdown']);
                } catch (\Throwable) {
                }
            }
        }
        $this->abandonConnection();
    }

    /**
     * Next to the master's socket, where it finds the journal by this
     * process's PID once the process has exited.
     */
    public function handBackJournal(): ?HandBackJournal
    {
        if (!$this->handsBack || $this->owner !== getmypid() || !is_resource($this->socket)) {
            return null;
        }

        return $this->journal ??= new HandBackJournal(dirname($this->socketPath) . '/hand-back-' . getmypid());
    }

    private function connect(): void
    {
        $errorCode = 0;
        $errorMessage = '';
        $socket = @stream_socket_client(
            'unix://' . $this->socketPath,
            $errorCode,
            $errorMessage,
            self::CONNECT_TIMEOUT_SECONDS,
        );
        if (!is_resource($socket)) {
            throw new RuntimeException("Unable to reach the Queen supervisor lease service: {$errorMessage}");
        }
        stream_set_blocking($socket, false);
        $this->socket = $socket;
        $this->owner = getmypid();
        $this->closedByMaster = false;

        $this->send([
            'command' => 'init',
            'client' => $this->serviceClientConfig(),
            'lease_seconds' => $this->leaseSeconds,
            'interval_millis' => $this->intervalSeconds * 1000,
            'request_budget_millis' => $this->requestBudgetSeconds * 1000,
            'kill_grace_millis' => $this->killGraceSeconds * 1000,
            'safety_margin_millis' => $this->safetyMarginSeconds * 1000,
            'monotonic_millis' => self::monotonicMillis(),
        ]);
        $deadline = self::monotonicMillis() + self::READY_TIMEOUT_MILLIS;
        while (self::monotonicMillis() < $deadline) {
            $event = $this->nextEvent();
            if (($event['event'] ?? null) === 'ready') {
                $this->handsBack = ($event['hand_back'] ?? false) === true;
                return;
            }
            if (($event['event'] ?? null) === 'startup_failed') {
                $this->abandonConnection();
                throw new RuntimeException(
                    'The Queen supervisor lease service refused this worker: ' . (string) ($event['error'] ?? 'unknown error'),
                );
            }
            if ($this->closedByMaster) {
                break;
            }
            $this->waitForInput(max(1, min(100, $deadline - self::monotonicMillis())));
        }

        $this->abandonConnection();
        throw new RuntimeException('The Queen supervisor lease service did not accept this worker.');
    }

    /**
     * The subset of the client configuration the master's HTTP client uses,
     * with one bounded request per renewal attempt.
     */
    private function serviceClientConfig(): array
    {
        $urls = $this->clientConfig['urls'] ?? null;
        if (!is_array($urls) || $urls === []) {
            $urls = [(string) ($this->clientConfig['url'] ?? 'http://localhost:6632')];
        }
        $headers = [];
        foreach ((array) ($this->clientConfig['headers'] ?? []) as $name => $value) {
            if (is_array($value)) {
                $value = implode(', ', array_map('strval', $value));
            }
            if (is_string($name) && is_scalar($value)) {
                $headers[$name] = (string) $value;
            }
        }
        $token = $this->clientConfig['bearerToken'] ?? null;

        return [
            'urls' => array_values(array_map('strval', $urls)),
            'bearerToken' => is_string($token) && $token !== '' ? $token : null,
            // An object even when empty, so it encodes as a JSON map.
            'headers' => (object) $headers,
            'timeoutMillis' => $this->requestTimeoutSeconds * 1000,
        ];
    }

    private function waitUntilTracked(string $leaseId, int $deadlineMonotonicMillis): void
    {
        $waitDeadline = min(
            self::monotonicMillis() + 1000,
            $deadlineMonotonicMillis - $this->safetyMarginSeconds * 1000,
        );
        while (self::monotonicMillis() < $waitDeadline) {
            $event = $this->nextEvent();
            if (($event['event'] ?? null) === 'tracked' && ($event['lease_id'] ?? null) === $leaseId) {
                return;
            }
            $this->recordFailure($event);
            if ($this->closedByMaster) {
                break;
            }
            $this->waitForInput(max(1, min(100, $waitDeadline - self::monotonicMillis())));
        }

        throw new RuntimeException("The Queen supervisor lease service did not confirm tracking [{$leaseId}].");
    }

    private function send(array $command): void
    {
        if (!is_resource($this->socket) || $this->closedByMaster) {
            throw new RuntimeException('The Queen supervisor lease service connection is closed.');
        }
        $payload = json_encode($command, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR) . "\n";
        $offset = 0;
        $length = strlen($payload);
        $deadline = self::monotonicMillis() + self::WRITE_TIMEOUT_MILLIS;
        while ($offset < $length) {
            $written = @fwrite($this->socket, substr($payload, $offset));
            if ($written === false) {
                throw new RuntimeException('Unable to communicate with the Queen supervisor lease service.');
            }
            $offset += $written;
            if ($offset < $length) {
                $left = $deadline - self::monotonicMillis();
                $read = null;
                $write = [$this->socket];
                $except = null;
                if ($left <= 0 || @stream_select($read, $write, $except, 0, $left * 1000) < 1) {
                    throw new RuntimeException('The Queen supervisor lease service does not read its socket.');
                }
            }
        }
    }

    private function drain(): void
    {
        while (($event = $this->nextEvent()) !== null) {
            $this->recordFailure($event);
        }
    }

    private function nextEvent(): ?array
    {
        if ($this->events !== []) {
            return array_shift($this->events);
        }
        if (!is_resource($this->socket) || $this->closedByMaster) {
            return null;
        }
        $chunk = @fread($this->socket, 65536);
        if (!is_string($chunk) || $chunk === '') {
            if (feof($this->socket)) {
                $this->closedByMaster = true;
            }
            return null;
        }
        $this->buffer .= $chunk;
        while (($newline = strpos($this->buffer, "\n")) !== false) {
            $line = substr($this->buffer, 0, $newline);
            $this->buffer = substr($this->buffer, $newline + 1);
            $event = json_decode($line, true);
            $this->events[] = is_array($event) ? $event : [];
        }

        return $this->events !== [] ? array_shift($this->events) : null;
    }

    private function waitForInput(int $timeoutMillis): void
    {
        if (!is_resource($this->socket)) {
            return;
        }
        $read = [$this->socket];
        $write = null;
        $except = null;
        @stream_select($read, $write, $except, intdiv($timeoutMillis, 1000), ($timeoutMillis % 1000) * 1000);
    }

    /** Close this process's descriptor without a word to the master. */
    private function abandonConnection(): void
    {
        if (is_resource($this->socket)) {
            @fclose($this->socket);
        }
        $this->socket = null;
        $this->owner = null;
        $this->reset();
    }

    private function reset(): void
    {
        $this->handsBack = false;
        $this->journal = null;
        $this->tracked = [];
        $this->failures = [];
        $this->buffer = '';
        $this->events = [];
        $this->closedByMaster = false;
    }
}
