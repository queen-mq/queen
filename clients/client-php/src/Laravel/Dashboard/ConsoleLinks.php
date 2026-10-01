<?php

namespace Queen\Laravel\Dashboard;

/**
 * Links from the dashboard to the Queen web console (queen.dashboard.console_url),
 * which browses the messages themselves: a queue's page, and its message list
 * filtered to the jobs waiting or running.
 *
 * The base URL is configuration, checked like the dashboard's other options: an
 * http or https URL with a host, without user info (a credential would reach
 * every page), query or fragment (the links append their own). The error never
 * repeats the value, for the same reason.
 */
final class ConsoleLinks
{
    private const MAX_URL_BYTES = 2048;

    private function __construct(private string $base)
    {
    }

    /**
     * @throws \InvalidArgumentException for a value that is set but unsafe
     */
    public static function fromConfig(mixed $url): ?self
    {
        if ($url === null || $url === '') {
            return null;
        }
        $parts = is_string($url)
            && strlen($url) <= self::MAX_URL_BYTES
            && preg_match('/[\x00-\x20\x7F]/', $url) !== 1
            && filter_var($url, FILTER_VALIDATE_URL) !== false
                ? parse_url($url)
                : false;
        if (!is_array($parts)
            || !in_array(strtolower((string) ($parts['scheme'] ?? '')), ['http', 'https'], true)
            || !is_string($parts['host'] ?? null)
            || $parts['host'] === ''
            || isset($parts['user'])
            || isset($parts['pass'])
            || isset($parts['query'])
            || isset($parts['fragment'])) {
            throw new \InvalidArgumentException(
                'queen.dashboard.console_url must be an http or https URL without user info, query or fragment.',
            );
        }

        return new self(rtrim($url, '/'));
    }

    public function queue(string $queue): string
    {
        return $this->base . '/queues/' . rawurlencode($queue);
    }

    /** The queue's jobs waiting for a worker. */
    public function waiting(string $queue): string
    {
        return $this->messages($queue, 'pending');
    }

    /** The queue's jobs leased by a worker now. */
    public function running(string $queue): string
    {
        return $this->messages($queue, 'processing');
    }

    private function messages(string $queue, string $status): string
    {
        return $this->base . '/messages?queue=' . rawurlencode($queue) . '&status=' . $status;
    }
}
