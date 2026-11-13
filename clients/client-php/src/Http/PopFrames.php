<?php

namespace Queen\Http;

use UnexpectedValueException;

/**
 * The framed pop answer: what a broker sends for a pop with `frames=true`.
 * The messages come as the stored records, each still the zstd frame the
 * producer (or the broker) made, and are unpacked here.
 *
 *     "QPF1" | u8 1 | u32 len, the answer's own keys as one JSON object
 *     u16 len, queue | u16 len, leaseId | u16 len, consumerGroup | u32 claims
 *     per claim:  u64 partitionId | u16 len, partition | u64 start | u64 end | u32 deliveryAttempt | u32 records
 *     per record: u64 base_offset | i64 created_at_us | u32 count | u8 codec (0 raw, 1 zstd) | u32 len | bytes
 *
 * All integers little-endian. A record's bytes are `count` frames:
 *
 *     u32 body_len | u8 flags | u8[16] message id | [u8[16] trace id, flag 1]
 *     | u16 len, transactionId | [u16 len, producer, flag 2]
 *     | [u16 len, headers, flag 8] | payload
 *
 * A broker answers JSON instead whenever a record holds an encrypted
 * message, a message with headers or a link (flags 4, 8, 16): the headers
 * are read here all the same, and the other two refused as damage.
 *
 * Message i of a record has offset base_offset+i and belongs to the delivery
 * when that lies in the claim's start..=end. The JSON object in front holds
 * what the JSON answer has beside its messages: the conflation echo and the
 * autopilot's choice.
 */
final class PopFrames
{
    public const CONTENT_TYPE = 'application/x-queen-pop';

    private const MAGIC = 'QPF1';
    private const FLAG_TRACE = 1;
    private const FLAG_PRODUCER = 2;
    private const FLAG_ENCRYPTED = 4;
    private const FLAG_HEADERS = 8;
    private const FLAG_LINK = 16;

    /** Whether a pop answer's bytes are the framed one. */
    public static function isFrames(string $bytes): bool
    {
        return strlen($bytes) > 4 && str_starts_with($bytes, self::MAGIC);
    }

    /**
     * Unpack a framed pop answer into the array the JSON answer decodes to:
     *
     *     [success, queue, partition, partitionId, leaseId, consumerGroup,
     *      messages => [[id, transactionId, traceId, data, producerSub,
     *                    createdAt, partitionId, partition, leaseId,
     *                    consumerGroup, deliveryAttempt, offset], ...],
     *      partitionsClaimed, ...the answer's own keys]
     *
     * The messages are in delivery order. Everything that reads a pop answer
     * (the conflation guard, the autopilot's choice, the consumer loops) reads
     * this array as it reads the JSON one.
     *
     * @throws UnexpectedValueException on an answer that is not whole
     */
    public static function decode(string $bytes): array
    {
        if (!self::isFrames($bytes)) {
            throw self::damaged('not a framed pop answer');
        }
        $at = 4;
        $short = 'truncated';
        $version = self::u8($bytes, $at, $short);
        $own = null;
        if ($version === 1) {
            $text = self::take($bytes, $at, self::u32($bytes, $at, $short), $short);
            if ($text !== '') {
                try {
                    $own = json_decode($text, true, 512, JSON_THROW_ON_ERROR);
                } catch (\JsonException $error) {
                    throw self::damaged('its own keys: ' . $error->getMessage());
                }
            }
        } elseif ($version !== 0) {
            throw self::damaged("unknown version {$version}");
        }

        $queue = self::text($bytes, $at, $short);
        $leaseId = self::text($bytes, $at, $short);
        $consumerGroup = self::text($bytes, $at, $short);
        $claims = self::u32($bytes, $at, $short);
        $messages = [];
        $firstPartition = '';
        $firstPartitionId = '';

        for ($claim = 0; $claim < $claims; $claim++) {
            // An unsigned 64-bit id, as the text the JSON answer carries.
            $partitionId = sprintf('%u', self::u64($bytes, $at, $short));
            $partition = self::text($bytes, $at, $short);
            $start = self::u64($bytes, $at, $short);
            $end = self::u64($bytes, $at, $short);
            $deliveryAttempt = self::u32($bytes, $at, $short);
            $records = self::u32($bytes, $at, $short);
            if ($claim === 0) {
                $firstPartition = $partition;
                $firstPartitionId = $partitionId;
            }
            for ($record = 0; $record < $records; $record++) {
                $base = self::u64($bytes, $at, $short);
                $createdAt = self::iso(self::u64($bytes, $at, $short));
                $count = self::u32($bytes, $at, $short);
                $codec = self::u8($bytes, $at, $short);
                $raw = self::take($bytes, $at, self::u32($bytes, $at, $short), $short);
                if ($codec === 1) {
                    try {
                        $raw = Zstd::uncompress($raw);
                    } catch (UnexpectedValueException $error) {
                        throw self::damaged('zstd: ' . $error->getMessage());
                    }
                } elseif ($codec !== 0) {
                    throw self::damaged("unknown codec {$codec}");
                }
                $in = 0;
                $shortRecord = "a record is shorter than its {$count} frames";
                for ($i = 0; $i < $count; $i++) {
                    $frame = self::take($raw, $in, self::u32($raw, $in, $shortRecord), $shortRecord);
                    $p = 0;
                    $malformed = 'a frame is malformed';
                    $flags = self::u8($frame, $p, $malformed);
                    $id = self::take($frame, $p, 16, $malformed);
                    $trace = $flags & self::FLAG_TRACE ? self::take($frame, $p, 16, $malformed) : null;
                    $transactionId = self::text($frame, $p, $malformed);
                    $producerSub = $flags & self::FLAG_PRODUCER ? self::text($frame, $p, $malformed) : null;
                    $headers = $flags & self::FLAG_HEADERS ? self::headers(self::text($frame, $p, $malformed), $malformed) : null;
                    if ($flags & self::FLAG_ENCRYPTED) {
                        throw self::damaged('a frame is encrypted');
                    }
                    if ($flags & self::FLAG_LINK) {
                        throw self::damaged('a frame is a link');
                    }
                    $offset = $base + $i;
                    if ($offset < $start || $offset > $end) {
                        continue;
                    }
                    $payload = substr($frame, $p);
                    try {
                        $data = $payload === '' ? null : json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
                    } catch (\JsonException $error) {
                        throw self::damaged('a payload: ' . $error->getMessage());
                    }
                    $message = [
                        'id' => PushBinary::uuidText($id),
                        'transactionId' => $transactionId,
                        'traceId' => $trace === null ? null : PushBinary::uuidText($trace),
                        'data' => $data,
                        'producerSub' => $producerSub,
                    ];
                    // Where the JSON answer places them, and only when there are some.
                    if ($headers !== null) {
                        $message['headers'] = $headers;
                    }
                    $messages[] = $message + [
                        'createdAt' => $createdAt,
                        'partitionId' => $partitionId,
                        'partition' => $partition,
                        'leaseId' => $leaseId,
                        'consumerGroup' => $consumerGroup,
                        'deliveryAttempt' => $deliveryAttempt,
                        'offset' => $offset,
                    ];
                }
            }
        }

        $answer = [
            'success' => true,
            'queue' => $queue,
            'partition' => $firstPartition,
            'partitionId' => $firstPartitionId,
            'leaseId' => $leaseId,
            'consumerGroup' => $consumerGroup,
            'messages' => $messages,
            'partitionsClaimed' => $claims,
        ];
        if (is_array($own)) {
            // `frames` only says the answer is this one; the JSON answer has no such key.
            unset($own['frames']);
            $answer = array_merge($answer, $own);
        }

        return $answer;
    }

    private static function damaged(string $what): UnexpectedValueException
    {
        return new UnexpectedValueException("framed pop answer: {$what}");
    }

    /**
     * A headers block, `u16 count | (u8 len, name | u16 len, value) x count`,
     * as the [name => value] array the JSON answer decodes to.
     */
    private static function headers(string $block, string $malformed): array
    {
        $at = 0;
        $headers = [];
        for ($n = self::u16($block, $at, $malformed); $n > 0; $n--) {
            $name = self::take($block, $at, self::u8($block, $at, $malformed), $malformed);
            $headers[$name] = self::text($block, $at, $malformed);
        }
        if ($at !== strlen($block)) {
            throw self::damaged($malformed);
        }

        return $headers;
    }

    /** @param-out int $at */
    private static function take(string $bytes, int &$at, int $length, string $short): string
    {
        if ($length < 0 || strlen($bytes) - $at < $length) {
            throw self::damaged($short);
        }
        $piece = substr($bytes, $at, $length);
        $at += $length;

        return $piece;
    }

    private static function u8(string $bytes, int &$at, string $short): int
    {
        return ord(self::take($bytes, $at, 1, $short));
    }

    private static function u16(string $bytes, int &$at, string $short): int
    {
        return unpack('v', self::take($bytes, $at, 2, $short))[1];
    }

    private static function u32(string $bytes, int &$at, string $short): int
    {
        return unpack('V', self::take($bytes, $at, 4, $short))[1];
    }

    private static function u64(string $bytes, int &$at, string $short): int
    {
        return unpack('P', self::take($bytes, $at, 8, $short))[1];
    }

    private static function text(string $bytes, int &$at, string $short): string
    {
        return self::take($bytes, $at, self::u16($bytes, $at, $short), $short);
    }

    /** Microseconds since the epoch as the broker writes `createdAt`. */
    private static function iso(int $micros): string
    {
        return gmdate('Y-m-d\TH:i:s', intdiv($micros, 1_000_000))
            . sprintf('.%03dZ', intdiv($micros % 1_000_000, 1000));
    }
}
