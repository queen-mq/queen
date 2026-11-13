<?php

namespace Queen\Support;

/**
 * A message's headers: name => text pairs beside the payload, which `where`
 * (`headers.type = 'order.paid'`), a reading queue's `partitionBy`
 * (`headers.customer`), the console and monitoring read without the payload.
 *
 *     $queen->queue('orders')->partition('order-9137')->push([[
 *         'data' => ['orderId' => 9137, 'total' => 120],
 *         'headers' => ['type' => 'order.paid', 'traceparent' => $traceparent],
 *     ]])->execute();
 *
 * Names are 1 to 64 bytes of a-z 0-9 . _ - (names starting with "queen-" are
 * the broker's), values any UTF-8 text, and a message carries 64 headers and
 * 8 KiB of them by default (QUEEN_MAX_HEADERS, QUEEN_MAX_HEADER_BYTES). The
 * broker checks all of that and refuses the push (400 bad_body); this class
 * checks only what PHP can get wrong, a value that is not text.
 *
 * Headers are never encrypted, even on an encrypted queue, and never part of
 * dedup (transactionId stays the key): no secrets or personal data in them.
 * They need a cluster that reads catalogue version 8 (cluster_version
 * otherwise).
 */
final class Headers
{
    /**
     * $headers as they go on the wire, or null for none: the key is then left
     * out, and the request is the one this client sent before headers. A value
     * is text; an int, a float or a Stringable is written as its text.
     *
     * The array comes back as it is, except when PHP holds the names as the
     * integer keys 0..n-1 (headers named "0", "1", ...): json_encode() would
     * write that as a list, so it comes back as an object, which it writes as
     * the JSON object the broker reads.
     *
     * @throws \InvalidArgumentException for headers that are not an array of text
     */
    public static function wire(mixed $headers): array|\stdClass|null
    {
        if ($headers instanceof \stdClass) {
            $headers = (array) $headers;
        }
        if ($headers === null || $headers === []) {
            return null;
        }
        if (!is_array($headers)) {
            throw new \InvalidArgumentException('headers must be an array of name => text, got ' . get_debug_type($headers));
        }
        foreach ($headers as $name => $value) {
            if (is_string($value)) {
                continue;
            }
            if (is_int($value) || is_float($value) || $value instanceof \Stringable) {
                $headers[$name] = (string) $value;
                continue;
            }
            throw new \InvalidArgumentException("header [{$name}] must be text, got " . get_debug_type($value));
        }

        return array_is_list($headers) ? (object) $headers : $headers;
    }
}
