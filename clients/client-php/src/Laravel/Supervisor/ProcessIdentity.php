<?php

namespace Queen\Laravel\Supervisor;

use RuntimeException;

/**
 * @internal The user this process acts as towards the supervisor state, and
 * the switch that lets root read another user's state as that user.
 *
 * SupervisorState asks this class instead of calling posix_geteuid() itself,
 * so that tests can stand in for root without running as root.
 */
class ProcessIdentity
{
    public function effectiveUid(): int
    {
        return posix_geteuid();
    }

    /**
     * The name and primary group of $uid in the user database, or null when
     * it has no entry there.
     *
     * @return array{name: string, gid: int}|null
     */
    public function account(int $uid): ?array
    {
        $entry = function_exists('posix_getpwuid') ? @posix_getpwuid($uid) : false;
        if (!is_array($entry)
            || !is_string($entry['name'] ?? null)
            || $entry['name'] === ''
            || !is_int($entry['gid'] ?? null)) {
            return null;
        }

        return ['name' => $entry['name'], 'gid' => $entry['gid']];
    }

    /**
     * Act as $uid, with its primary group and the supplementary groups the
     * user database gives $name, until the returned closure is called. This
     * is what su does, kept reversible: the real and saved uids stay root.
     * Only root can call it.
     *
     * @return \Closure(): void restores the identity this process had before
     */
    public function assume(int $uid, string $name, int $gid): \Closure
    {
        if (!function_exists('posix_seteuid')
            || !function_exists('posix_setegid')
            || !function_exists('posix_initgroups')) {
            throw new RuntimeException('This PHP build cannot switch users.');
        }
        $previousUid = posix_geteuid();
        $previousGid = posix_getegid();
        $previous = $this->account($previousUid);
        if ($previousUid !== 0 || $previous === null) {
            throw new RuntimeException('Only root, with an entry in the user database, can act as another user.');
        }

        $restore = static function () use ($previousUid, $previousGid, $previous): void {
            // The uid first: only root may set the group and the groups back.
            // PHP has no setgroups(), so the supplementary groups come back
            // from the user database, which is where a container runtime takes
            // root's groups from. Groups added from outside it, such as a
            // Kubernetes fsGroup, do not come back; a probe exits right after.
            if (!posix_seteuid($previousUid)
                || !posix_setegid($previousGid)
                || !posix_initgroups($previous['name'], $previousGid)) {
                throw new RuntimeException(
                    'Unable to restore the user of this process after reading the Queen supervisor state: '
                    . posix_strerror(posix_get_last_error()),
                );
            }
        };

        // The groups before the uid: once the effective uid is not root, the
        // process can no longer change them.
        if (!posix_initgroups($name, $gid) || !posix_setegid($gid) || !posix_seteuid($uid)) {
            $reason = posix_strerror(posix_get_last_error());
            $restore();
            throw new RuntimeException("Root could not act as uid {$uid} ({$name}): {$reason}.");
        }

        return $restore;
    }
}
