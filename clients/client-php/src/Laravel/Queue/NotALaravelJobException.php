<?php

namespace Queen\Laravel\Queue;

use UnexpectedValueException;

/**
 * A delivery that carries no Laravel job. It never will, so the worker files
 * it in the dead-letter queue at once and reports this, instead of leaving it
 * to lease expiry: an expiry never charges the broker's retry budget, so the
 * delivery would come back forever and hold its partition.
 */
final class NotALaravelJobException extends UnexpectedValueException
{
    /** How much of the delivery's data the report quotes. */
    private const QUOTED_BYTES = 120;

    public static function for(array $message, string $queue, string $reason): self
    {
        $data = $message['data'] ?? $message['payload'] ?? null;
        $raw = is_string($data)
            ? $data
            : (string) json_encode($data, JSON_UNESCAPED_SLASHES | JSON_PARTIAL_OUTPUT_ON_ERROR | JSON_INVALID_UTF8_SUBSTITUTE);
        $quoted = (string) json_encode(
            substr($raw, 0, self::QUOTED_BYTES),
            JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE,
        );

        return new self(sprintf(
            'Queen Laravel filed delivery [%s] of queue [%s], partition [%s], delivery attempt %d, in the dead-letter '
            . 'queue: it carries no Laravel job (%s). Its first bytes: %s%s',
            self::identity($message['transactionId'] ?? $message['id'] ?? null),
            $queue,
            self::identity($message['partition'] ?? $message['partitionId'] ?? null),
            max(1, (int) ($message['deliveryAttempt'] ?? 1)),
            $reason,
            $quoted,
            strlen($raw) > self::QUOTED_BYTES ? ' (' . strlen($raw) . ' bytes in all)' : '',
        ));
    }

    private static function identity(mixed $value): string
    {
        return is_string($value) || is_int($value)
            ? (string) preg_replace('/[\x00-\x1F\x7F]+/', ' ', substr((string) $value, 0, 128))
            : '?';
    }
}
