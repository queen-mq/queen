<?php

namespace Queen\Laravel\Supervisor\Prefork;

/**
 * What the booted fork server holds that a fork does not carry over safely.
 *
 * fork() copies one thread, the caller: any other thread of the server (a
 * gRPC or Kafka extension, an APM agent, libcurl's resolver) is missing in
 * every child, along with whatever lock it held. And a socket the server
 * opened while Laravel booted is shared by every child, so two workers that
 * use it corrupt each other's conversation.
 *
 * Read from Linux's /proc. Elsewhere both answers are unknown, and the fork
 * server forks as before.
 */
final class ForkSafety
{
    public function __construct(private string $proc = '/proc/self')
    {
    }

    /**
     * The names of the server's threads besides the main one, or null when
     * /proc cannot tell.
     *
     * @return list<string>|null
     */
    public function otherThreads(): ?array
    {
        $tasks = @scandir($this->proc . '/task');
        if ($tasks === false) {
            return null;
        }
        $main = basename((string) @readlink($this->proc));
        $names = [];
        foreach ($tasks as $task) {
            if (!ctype_digit($task) || $task === $main) {
                continue;
            }
            $name = @file_get_contents($this->proc . "/task/{$task}/comm", length: 64);
            $names[] = is_string($name) && trim($name) !== '' ? trim($name) : $task;
        }
        sort($names);

        return $names;
    }

    /**
     * The other threads once they had up to `$seconds` to end: libcurl keeps
     * a resolver thread for about two seconds after a request, so a provider
     * that called an API while Laravel booted leaves one for a moment.
     *
     * @return list<string>|null
     */
    public function threadsThatStay(float $seconds): ?array
    {
        $deadline = microtime(true) + $seconds;
        while (($threads = $this->otherThreads()) !== null && $threads !== [] && microtime(true) < $deadline) {
            usleep(100_000);
        }

        return $threads;
    }

    /**
     * The open sockets from descriptor `$from` up; the ones below belong to
     * the master's protocol (stdin, stdout, stderr, the events at fd 3).
     *
     * @return list<int>
     */
    public function sockets(int $from = 4): array
    {
        $entries = @scandir($this->proc . '/fd');
        if ($entries === false) {
            return [];
        }
        $sockets = [];
        foreach ($entries as $entry) {
            if (!ctype_digit($entry) || (int) $entry < $from) {
                continue;
            }
            $target = @readlink($this->proc . "/fd/{$entry}");
            if (is_string($target) && str_starts_with($target, 'socket:')) {
                $sockets[] = (int) $entry;
            }
        }
        sort($sockets);

        return $sockets;
    }
}
