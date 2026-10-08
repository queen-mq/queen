<?php

namespace Queen;

use Queen\Exceptions\LockNotHeldException;

/**
 * One holder's hold on one lock, or on one permit of a semaphore. Reached
 * through `$queen->lock()` and `$queen->semaphore()`.
 *
 *     $lock = $queen->lock('daily-report', ['ttlSeconds' => 30]);
 *     if (!$lock->acquire()) {
 *         return;                              // somebody else has it
 *     }
 *     try {
 *         $queen->transaction()
 *             ->guard($lock)                   // commits only while the lock is ours
 *             ->queue('reports')->push([['data' => $report]])
 *             ->commit();
 *     } finally {
 *         $lock->release();
 *     }
 *
 * IT IS A LEASE, NOT A MUTEX. A permit EXPIRES, and nobody tells its holder. A
 * process that is paused or slow keeps running past its lifetime while somebody
 * else acquires. So the lock alone never makes two holders impossible; what
 * makes their WORK exclusive is the token:
 *
 *   - inside Queen, `->guard($lock)` on a transaction: the acks, pushes, KV
 *     writes and timers of the step commit only if the permit is still this
 *     holder's, in the same log entry. A holder that was replaced commits
 *     nothing.
 *   - outside Queen, `$lock->token()`: a number that only rises on a lock. A
 *     resource that remembers the highest token it accepted and refuses a
 *     lower one refuses the holder that was replaced. Accept an EQUAL one: a
 *     holder writes many times with one token.
 *
 * NOTHING RENEWS THIS IN THE BACKGROUND. PHP has no thread to do it, so the
 * lifetime has to cover the work, or the work has to call keepAlive() at its
 * checkpoints (inside a loop, between batches): it renews once a third of the
 * lifetime has passed and costs nothing before that. A handle whose lifetime
 * ran out on this machine's clock says held() === false, and its next guarded
 * step is refused by the broker.
 *
 * THE TOKEN CHANGES AT EVERY RENEW: a renew rewrites the row. The handle keeps
 * the current one; read token() and guard() when you use them.
 *
 * THE OWNER is the holder's identity, minted per handle. With it a call is
 * safe to send again when its answer was lost: the broker answers the permit
 * the first attempt took. Two handles with one owner are one holder.
 */
class Lock
{
    private Locks $locks;
    private string $name;
    private int $ttlSeconds;
    private int $limit;
    private string $owner;
    private float $retryMin;
    private float $retryMax;

    private ?int $token = null;
    private ?int $slot = null;
    /**
     * The broker's own guard of the current lease period, kept as answered:
     * where a permit's row lives is the broker's rule, written once, there.
     */
    private ?array $guard = null;
    /** hrtime seconds past which the permit must be considered gone. */
    private float $validUntil = 0.0;
    private float $renewedAt = 0.0;

    /**
     * @param array $opts ttlSeconds (required), limit (1), owner,
     *   retryMinMs (100), retryMaxMs (1000).
     */
    public function __construct(Locks $locks, string $name, array $opts = [])
    {
        foreach (array_keys($opts) as $option) {
            if (!in_array($option, ['ttlSeconds', 'limit', 'owner', 'retryMinMs', 'retryMaxMs'], true)) {
                throw new \InvalidArgumentException(
                    "unknown lock option `{$option}`; a lock accepts: ttlSeconds, limit, owner, retryMinMs, retryMaxMs"
                );
            }
        }

        $this->locks = $locks;
        $this->name = Locks::name($name);
        $this->ttlSeconds = Locks::ttlSeconds($opts);
        $this->limit = Locks::limit($opts['limit'] ?? 1);
        $this->owner = array_key_exists('owner', $opts) ? Locks::owner($opts['owner']) : self::mintOwner();
        $this->retryMin = ((int) ($opts['retryMinMs'] ?? 100)) / 1000;
        $this->retryMax = max($this->retryMin, ((int) ($opts['retryMaxMs'] ?? 1000)) / 1000);
    }

    public function name(): string
    {
        return $this->name;
    }

    public function owner(): string
    {
        return $this->owner;
    }

    public function limit(): int
    {
        return $this->limit;
    }

    /**
     * Whether this handle holds a permit, as far as it can know: the broker
     * granted or renewed it, and its lifetime has not run out on this
     * machine's clock. A belief with a deadline, not a proof — the proof is
     * the guard on the transaction.
     */
    public function held(): bool
    {
        return $this->token !== null && self::now() < $this->validUntil;
    }

    /** The fencing token of the current lease period; null when not held. */
    public function token(): ?int
    {
        return $this->held() ? $this->token : null;
    }

    /** The semaphore slot this handle holds (0 for a lock); null when not held. */
    public function slot(): ?int
    {
        return $this->held() ? $this->slot : null;
    }

    /** Seconds left of the lease on this machine's clock; 0.0 when not held. */
    public function expiresIn(): float
    {
        return $this->held() ? $this->validUntil - self::now() : 0.0;
    }

    /**
     * The KV operation that holds while the permit is this handle's: a `check`
     * of the permit's row at the current token, required. `->guard($lock)` on
     * a transaction adds it for you; use this one to put it in a KV batch
     * yourself, at the moment you send.
     */
    public function guard(): array
    {
        if (!$this->held() || $this->guard === null) {
            throw new LockNotHeldException("lock '{$this->name}' is not held; there is nothing to guard with");
        }

        return $this->guard;
    }

    /**
     * Take the permit. true or false — a boolean, so `if ($lock->acquire())`
     * means what it reads as.
     *
     * With $waitSeconds it keeps trying until the permit is free or the wait
     * is over, coming back every 100 ms to 1 s with jitter. It BLOCKS for that
     * long: this is PHP.
     */
    public function acquire(float $waitSeconds = 0.0): bool
    {
        if ($this->held()) {
            return true;
        }

        $deadline = self::now() + $waitSeconds;
        $pause = $this->retryMin;
        $opts = ['ttlSeconds' => $this->ttlSeconds, 'owner' => $this->owner];
        if ($this->limit > 1) {
            $opts['limit'] = $this->limit;
        }

        while (true) {
            $sentAt = self::now();
            $result = $this->locks->acquire($this->name, $opts);
            if (($result['acquired'] ?? false) === true) {
                $this->take($result, $sentAt);
                return true;
            }

            $left = $deadline - self::now();
            if ($left <= 0) {
                return false;
            }
            // A random quarter off, so a crowd of waiters spreads.
            $jittered = $pause * (0.75 + mt_rand() / mt_getrandmax() * 0.25);
            usleep((int) (min($jittered, $left) * 1_000_000));
            $pause = min($pause * 1.5, $this->retryMax);
        }
    }

    /**
     * Extend the lease now. true with a new token in place, or false: the
     * permit is gone. Throws when the broker could not be asked — the permit
     * is then neither renewed nor known lost, and its deadline stands.
     */
    public function renew(): bool
    {
        if ($this->token === null) {
            return false;
        }

        $sentAt = self::now();
        $result = $this->locks->renew($this->name, $this->token, [
            'ttlSeconds' => $this->ttlSeconds,
            'slot' => $this->slot ?? 0,
            'owner' => $this->owner,
        ]);

        if (($result['renewed'] ?? false) !== true) {
            $this->drop();
            return false;
        }

        $this->token = $result['token'];
        $this->guard = $result['guard'];
        $this->validUntil = $sentAt + $this->ttlSeconds;
        $this->renewedAt = $sentAt;

        return true;
    }

    /**
     * Renew if it is due: once a third of the lifetime has passed since the
     * permit was taken or last renewed. Call it at the checkpoints of long
     * work; it sends nothing before then.
     *
     * @return bool whether the permit is still held. false means stop: it
     *   expired here, or the broker says it is somebody else's.
     */
    public function keepAlive(): bool
    {
        if (!$this->held()) {
            if ($this->token !== null) {
                $this->drop();
            }
            return false;
        }

        if (self::now() - $this->renewedAt < $this->ttlSeconds / 3) {
            return true;
        }

        return $this->renew();
    }

    /**
     * Give the permit back. true when the broker removed it; false when it
     * was not this handle's any more or was never held. Either way the handle
     * holds nothing afterwards and can acquire again.
     */
    public function release(): bool
    {
        if ($this->token === null) {
            return false;
        }

        [$token, $slot] = [$this->token, $this->slot ?? 0];
        $this->drop();
        $result = $this->locks->release($this->name, $token, $slot);

        return ($result['released'] ?? false) === true;
    }

    /**
     * Acquire, call $fn($lock), release — whatever $fn does.
     *
     * @return array ['acquired' => false] when the permit could not be had
     *   (after $waitSeconds, if given), else ['acquired' => true, 'value' => …].
     */
    public function run(callable $fn, float $waitSeconds = 0.0): array
    {
        if (!$this->acquire($waitSeconds)) {
            return ['acquired' => false];
        }

        try {
            return ['acquired' => true, 'value' => $fn($this)];
        } finally {
            try {
                $this->release();
            } catch (\Throwable) {
                // The permit expires by itself; the block's own outcome wins.
            }
        }
    }

    /**
     * The broker said the permit is not this handle's.
     *
     * @internal Used by TransactionBuilder::guard
     */
    public function markLost(): void
    {
        $this->drop();
    }

    private function take(array $result, float $sentAt): void
    {
        $this->token = $result['token'];
        $this->slot = $result['slot'];
        $this->guard = $result['guard'];
        // Counted from when the request was SENT, so it is never later than
        // the broker's own deadline.
        $this->validUntil = $sentAt + $this->ttlSeconds;
        $this->renewedAt = $sentAt;
    }

    private function drop(): void
    {
        $this->token = null;
        $this->slot = null;
        $this->guard = null;
        $this->validUntil = 0.0;
        $this->renewedAt = 0.0;
    }

    private static function now(): float
    {
        return hrtime(true) / 1e9;
    }

    private static function mintOwner(): string
    {
        $host = substr((string) (gethostname() ?: 'host'), 0, 128);

        return sprintf('%s:%d:%s', $host, getmypid() ?: 0, bin2hex(random_bytes(6)));
    }
}
