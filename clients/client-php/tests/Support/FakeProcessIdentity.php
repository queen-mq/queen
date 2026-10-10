<?php

namespace Queen\Tests\Support;

use Queen\Laravel\Supervisor\ProcessIdentity;

/**
 * Stands in for the user a process runs as, so that a test running as an
 * ordinary user can play root or a stranger. assume() records the switch and
 * makes effectiveUid() answer the assumed uid until the returned closure is
 * called. The filesystem calls still run as the test's real user, so a test
 * that plays root makes the state belong to that real user.
 */
final class FakeProcessIdentity extends ProcessIdentity
{
    /** @var list<array{uid:int,name:string,gid:int}> */
    public array $assumed = [];

    public int $restored = 0;

    /** @param array<int, string> $names the accounts in the user database, by uid */
    public function __construct(private int $uid, private array $names = [])
    {
    }

    public function effectiveUid(): int
    {
        return $this->uid;
    }

    public function account(int $uid): ?array
    {
        return isset($this->names[$uid]) ? ['name' => $this->names[$uid], 'gid' => 4321] : null;
    }

    public function assume(int $uid, string $name, int $gid): \Closure
    {
        $this->assumed[] = ['uid' => $uid, 'name' => $name, 'gid' => $gid];
        $previous = $this->uid;
        $this->uid = $uid;

        return function () use ($previous): void {
            $this->uid = $previous;
            $this->restored++;
        };
    }
}
