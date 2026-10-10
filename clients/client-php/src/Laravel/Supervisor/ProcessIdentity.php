<?php

namespace Queen\Laravel\Supervisor;

/**
 * @internal The user this process runs as, and the names of users, for the
 * ownership checks of the supervisor state and the refusals they give.
 *
 * SupervisorState asks this class instead of calling posix_geteuid() itself,
 * so that tests can play root or another user without running as one.
 */
class ProcessIdentity
{
    public function effectiveUid(): int
    {
        return posix_geteuid();
    }

    /** The name of $uid in the user database, or null when it has no entry there. */
    public function name(int $uid): ?string
    {
        $entry = function_exists('posix_getpwuid') ? @posix_getpwuid($uid) : false;
        $name = is_array($entry) ? ($entry['name'] ?? null) : null;

        return is_string($name) && $name !== '' ? $name : null;
    }
}
