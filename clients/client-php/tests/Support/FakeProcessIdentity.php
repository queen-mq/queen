<?php

namespace Queen\Tests\Support;

use Queen\Laravel\Supervisor\ProcessIdentity;

/**
 * Stands in for the user a process runs as, so that a test running as an
 * ordinary user can play root or a stranger. The filesystem calls still run
 * as the test's real user, so the state such a test reads belongs to that
 * real user.
 */
final class FakeProcessIdentity extends ProcessIdentity
{
    /** @param array<int, string> $names the accounts in the user database, by uid */
    public function __construct(private int $uid, private array $names = [])
    {
    }

    public function effectiveUid(): int
    {
        return $this->uid;
    }

    public function name(int $uid): ?string
    {
        return $this->names[$uid] ?? null;
    }
}
