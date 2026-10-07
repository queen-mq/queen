<?php

namespace App\Examples;

/**
 * The processes of this host and their memory, read from /proc on Linux and
 * from ps elsewhere (macOS).
 */
final class Processes
{
    /** @return array<int, array{pid: int, ppid: int, rss_kib: int, command: string}> by pid */
    public static function all(): array
    {
        return is_dir('/proc/self') ? self::fromProc() : self::fromPs();
    }

    /**
     * The pids of $root and of every process below it.
     *
     * @param array<int, array{pid: int, ppid: int, rss_kib: int, command: string}> $all
     * @return list<int>
     */
    public static function tree(array $all, int $root): array
    {
        $pids = isset($all[$root]) ? [$root] : [];
        for ($i = 0; $i < count($pids); $i++) {
            foreach ($all as $process) {
                if ($process['ppid'] === $pids[$i]) {
                    $pids[] = $process['pid'];
                }
            }
        }

        return $pids;
    }

    public static function alive(int $pid): bool
    {
        return posix_kill($pid, 0);
    }

    /**
     * Linux: resident set, proportional set and private memory, in KiB, from
     * /proc/<pid>/smaps_rollup; null where the kernel does not have it.
     *
     * @return array{rss: int, pss: int, private: int}|null
     */
    public static function smaps(int $pid): ?array
    {
        $text = @file_get_contents("/proc/{$pid}/smaps_rollup");
        if (!is_string($text)) {
            return null;
        }
        $field = fn (string $name): int => preg_match("/^{$name}:\\s+(\\d+) kB/m", $text, $m) ? (int) $m[1] : 0;

        return [
            'rss' => $field('Rss'),
            'pss' => $field('Pss'),
            'private' => $field('Private_Clean') + $field('Private_Dirty'),
        ];
    }

    private static function fromProc(): array
    {
        $all = [];
        foreach (glob('/proc/[0-9]*') ?: [] as $dir) {
            $stat = @file_get_contents("{$dir}/stat");
            $status = @file_get_contents("{$dir}/status");
            $cmdline = @file_get_contents("{$dir}/cmdline");
            // The command name in stat may hold spaces and parentheses: the
            // fields after it start at the last ')'.
            if (!is_string($stat) || !is_string($status) || !is_string($cmdline)) {
                continue;
            }
            $fields = explode(' ', substr($stat, strrpos($stat, ')') + 2));
            $pid = (int) basename($dir);
            $all[$pid] = [
                'pid' => $pid,
                'ppid' => (int) $fields[1],
                'rss_kib' => preg_match('/^VmRSS:\s+(\d+) kB/m', $status, $m) ? (int) $m[1] : 0,
                'command' => trim(str_replace("\0", ' ', $cmdline)),
            ];
        }

        return $all;
    }

    private static function fromPs(): array
    {
        exec('ps -A -o pid=,ppid=,rss=,command=', $lines);
        $all = [];
        foreach ($lines as $line) {
            if (preg_match('/^\s*(\d+)\s+(\d+)\s+(\d+)\s+(.*)$/', $line, $m)) {
                $all[(int) $m[1]] = ['pid' => (int) $m[1], 'ppid' => (int) $m[2], 'rss_kib' => (int) $m[3], 'command' => $m[4]];
            }
        }

        return $all;
    }
}
