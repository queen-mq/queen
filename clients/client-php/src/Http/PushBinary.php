<?php

namespace Queen\Http;

/**
 * The binary push body: an alternative to the JSON body of POST /api/v1/push.
 *
 *     "QPB1" | u8 flags | [u32 raw_len, when zstd] | body
 *     body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n
 *            | n x ([u16 len, transactionId, when flag 4] | u32 len, payload)
 *
 * All integers little-endian. flags: 1 = the body is one zstd frame of raw_len
 * bytes, 2 = answer with the counts only, 4 = every item carries its
 * transaction id (empty = the broker mints it). No trace id and no headers.
 * Each payload is one JSON document.
 *
 * With flag 8 (encodeFrames) a group is the messages as the broker stores
 * them, compressed here once:
 *
 *     body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n | u32 z_len | z
 *     z:     one zstd frame of n frames: u32 body_len | u8 flags | u8[16] message id
 *            | u16 len, transactionId | [u16 len, headers, flag 8] | payload
 *
 * The message id is a version-7 UUID minted by the client; a message without
 * its own transaction id uses the id's text. A frame's flags are 0, or 8 when
 * the message has headers: `u16 count | (u8 len, name | u16 len, value) x
 * count`, sorted by name (headersBlock). The broker checks every frame and
 * keeps the compressed bytes as they are, so it does not compress them again.
 *
 * A broker that does not know this body answers 400; a client sends it only
 * when `binaryPush` is set, so by default a push is the JSON request every
 * broker knows.
 *
 * The binary transaction body (encodeTransactionFrames), an alternative to the
 * JSON body of POST /api/v1/transaction, carries the pushes the same way:
 *
 *     "QTB1" | u8 flags | u32 len, head | u32 groups | the groups, as with flag 8 above
 *
 * head is the JSON transaction body without its push operations: the acks and
 * the links in `operations`, `requiredLeases`, `kv`, `timers`. flags: 2 = the answer leaves
 * out the pushes' and the acks' results and counts them (`pushed`,
 * `duplicate`, `acked`); the riders' results stay. In the answer the head's
 * operations come first and the groups' messages after them, in the order
 * written; the riders follow, as in the JSON body.
 */
final class PushBinary
{
    public const CONTENT_TYPE = 'application/x-queen-push';

    private const MAGIC = 'QPB1';
    private const TXN_MAGIC = 'QTB1';
    private const FLAG_ZSTD = 1;
    private const FLAG_COUNTS = 2;
    private const FLAG_TXN = 4;
    private const FLAG_FRAMES = 8;
    /** A stored frame's flag: its headers block follows the transaction id. */
    private const FRAME_HEADERS = 8;

    /**
     * Encode batches as a binary push body.
     *
     * @param list<array{queue: string, partition: string, payloads: list<string>, transactionIds?: list<string>|null}> $batches
     *        one partition's messages each; a payload is the text of one JSON
     *        document, and transactionIds, when given, holds one per payload
     *        ('' = the broker mints it)
     * @param bool $compress the body is zstd-compressed
     * @param bool $counts   the broker answers {count, queued, duplicate, error}
     *                       instead of one object per message
     *
     * @throws \LengthException when a name or a transaction id is over 65535 bytes
     * @throws \InvalidArgumentException for a batch with headers, which only frames carry
     */
    public static function encode(array $batches, bool $compress = false, bool $counts = false): string
    {
        $withTxn = false;
        foreach ($batches as $batch) {
            $withTxn = $withTxn || isset($batch['transactionIds']);
            if (array_filter($batch['headerBlocks'] ?? [], static fn (?string $block): bool => $block !== null) !== []) {
                throw new \InvalidArgumentException('the plain binary push body has no place for headers: frame them (encodeFrames)');
            }
        }
        $flags = ($counts ? self::FLAG_COUNTS : 0) | ($withTxn ? self::FLAG_TXN : 0);
        $parts = [pack('V', count($batches))];
        foreach ($batches as $batch) {
            self::check($batch);
            $parts[] = self::text16($batch['queue'], 'queue name');
            $parts[] = self::text16($batch['partition'], 'partition name');
            $parts[] = pack('V', count($batch['payloads']));
            foreach ($batch['payloads'] as $i => $payload) {
                if ($withTxn) {
                    $parts[] = self::text16($batch['transactionIds'][$i] ?? '', 'transaction id');
                }
                $parts[] = pack('V', strlen($payload));
                $parts[] = $payload;
            }
        }
        $body = implode('', $parts);
        if (!$compress) {
            return self::MAGIC . chr($flags) . $body;
        }

        return self::MAGIC . chr($flags | self::FLAG_ZSTD) . pack('V', strlen($body)) . Zstd::compress($body);
    }

    /**
     * Encode batches as a binary push body whose groups are the messages
     * framed as the broker stores them and compressed once, here. Same batches
     * as encode(), and headerBlocks, when given, holds one headersBlock() (or
     * null, for none) per payload; $counts asks for the short answer.
     *
     * @param list<array{queue: string, partition: string, payloads: list<string>, transactionIds?: list<string>|null, headerBlocks?: list<?string>|null}> $batches
     *
     * @throws \LengthException when a name or a transaction id is over 65535 bytes
     */
    public static function encodeFrames(array $batches, bool $counts = false): string
    {
        return self::MAGIC . chr(self::FLAG_FRAMES | ($counts ? self::FLAG_COUNTS : 0)) . self::frameGroups($batches);
    }

    /**
     * Encode a binary transaction body: $head, the JSON transaction body
     * without its push operations (null or '' for none), then the batches as
     * groups of messages framed as the broker stores them and compressed once,
     * here. $counts: the broker counts the pushes and the acks in its answer
     * instead of rendering one result for each.
     *
     * @param list<array{queue: string, partition: string, payloads: list<string>, transactionIds?: list<string>|null, headerBlocks?: list<?string>|null}> $batches
     *
     * @throws \LengthException when a name or a transaction id is over 65535 bytes
     */
    public static function encodeTransactionFrames(?string $head, array $batches, bool $counts = false): string
    {
        $head ??= '';

        return self::TXN_MAGIC . chr($counts ? self::FLAG_COUNTS : 0) . pack('V', strlen($head)) . $head
            . self::frameGroups($batches);
    }

    /**
     * `u32 groups | per group: u16 len, queue | u16 len, partition | u32 n |
     * u32 z_len | z`: each batch's messages framed as the broker stores them,
     * with a version-7 id minted here, as one zstd frame.
     */
    private static function frameGroups(array $batches): string
    {
        $parts = [pack('V', count($batches))];
        foreach ($batches as $batch) {
            self::check($batch);
            $count = count($batch['payloads']);
            // A version-7 UUID per message: the time in milliseconds, then
            // random bits under the version and variant marks.
            $stamp = substr(pack('J', (int) floor(microtime(true) * 1000)), 2);
            $noise = $count > 0 ? random_bytes(10 * $count) : '';
            $frames = [];
            foreach ($batch['payloads'] as $i => $payload) {
                $tail = substr($noise, 10 * $i, 10);
                $tail[0] = chr(0x70 | (ord($tail[0]) & 0x0f));
                $tail[2] = chr(0x80 | (ord($tail[2]) & 0x3f));
                $id = $stamp . $tail;
                $txn = $batch['transactionIds'][$i] ?? '';
                if ($txn === '') {
                    $txn = self::uuidText($id);
                }
                $frames[] = self::frame($id, $txn, $payload, $batch['headerBlocks'][$i] ?? null);
            }
            $compressed = Zstd::compress(implode('', $frames));
            $parts[] = self::text16($batch['queue'], 'queue name')
                . self::text16($batch['partition'], 'partition name')
                . pack('V', $count)
                . pack('V', strlen($compressed));
            $parts[] = $compressed;
        }

        return implode('', $parts);
    }

    /**
     * One message framed as the broker stores it: `u32 body_len | u8 flags |
     * u8[16] id | u16 len, transactionId | [u16 len, headers] | payload`,
     * flags 8 when $headerBlock (a headersBlock()) is given and 0 otherwise.
     *
     * @throws \LengthException when the transaction id is over 65535 bytes
     */
    public static function frame(string $id, string $transactionId, string $payload, ?string $headerBlock = null): string
    {
        $txn = self::text16($transactionId, 'transaction id');
        $headers = $headerBlock === null ? '' : self::text16($headerBlock, 'headers block');
        $flags = $headerBlock === null ? 0 : self::FRAME_HEADERS;

        return pack('V', 17 + strlen($txn) + strlen($headers) + strlen($payload)) . chr($flags) . $id . $txn . $headers . $payload;
    }

    /**
     * Headers ([name => text], at least one: a frame without headers carries
     * no block) as the block a stored frame carries: `u16 count | (u8 len,
     * name | u16 len, value) x count`, sorted by name, as the broker writes
     * and checks it. Null when they
     * cannot travel in it: a value that is not UTF-8 text, a name over 255
     * bytes, a value or a block over 65535. The caller then sends the JSON
     * body, and the broker answers in its own words (it refuses far smaller
     * headers than these: 64 bytes a name and 8 KiB a message by default).
     */
    public static function headersBlock(array $headers): ?string
    {
        ksort($headers, SORT_STRING);
        $parts = [pack('v', count($headers))];
        foreach ($headers as $name => $value) {
            $name = (string) $name;
            if (!is_string($value) || strlen($name) > 0xff || strlen($value) > 0xffff
                || preg_match('//u', $name . $value) !== 1) {
                return null;
            }
            $parts[] = chr(strlen($name)) . $name . pack('v', strlen($value)) . $value;
        }
        $block = implode('', $parts);

        return strlen($block) <= 0xffff ? $block : null;
    }

    /**
     * The items of a JSON push ({queue, partition, payload, transactionId,
     * headers}) as a binary push body, or null when one of them cannot travel
     * in it: a trace id, headers in a body that is not framed ($compress
     * false) or that the block cannot carry, a transaction id that is not a
     * string, a payload JSON does not carry, a name over 65535 bytes. The
     * caller then sends the JSON body, and the broker answers that as it
     * always did. Consecutive items of one partition share a group, so the
     * order of the items is the order on the wire.
     *
     * $compress: the messages framed and compressed here, kept by the broker
     * as they are.
     */
    public static function body(array $items, bool $compress): ?string
    {
        if ($items === [] || !array_is_list($items)) {
            return null;
        }
        $batches = [];
        foreach ($items as $item) {
            if (!self::addItem($batches, $item, $compress)) {
                return null;
            }
        }
        try {
            return $compress ? self::encodeFrames($batches) : self::encode($batches);
        } catch (\LengthException) {
            // The JSON push says so in the broker's words.
            return null;
        }
    }

    /**
     * Adds one pushed item to $batches, in the last one when it is the same
     * partition's. False when the item cannot travel in a binary body; with
     * headers, only a framed one ($frames) has a place for them.
     */
    private static function addItem(array &$batches, mixed $item, bool $frames): bool
    {
        if (!is_array($item) || !is_string($item['queue'] ?? null) || !array_key_exists('payload', $item)) {
            return false;
        }
        if (($item['traceId'] ?? null) !== null) {
            return false;
        }
        $txn = $item['transactionId'] ?? '';
        $partition = $item['partition'] ?? 'Default';
        if (!is_string($txn) || !is_string($partition)) {
            return false;
        }
        $block = null;
        $headers = $item['headers'] ?? null;
        if ($headers instanceof \stdClass) {
            $headers = (array) $headers;
        }
        if ($headers !== null && $headers !== []) {
            $block = $frames && is_array($headers) ? self::headersBlock($headers) : null;
            if ($block === null) {
                return false;
            }
        }
        try {
            $payload = json_encode($item['payload'], JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return false;
        }
        $last = count($batches) - 1;
        if ($last < 0 || $batches[$last]['queue'] !== $item['queue'] || $batches[$last]['partition'] !== $partition) {
            $batches[++$last] = [
                'queue' => $item['queue'],
                'partition' => $partition,
                'payloads' => [],
                'transactionIds' => [],
                'headerBlocks' => [],
            ];
        }
        $batches[$last]['payloads'][] = $payload;
        $batches[$last]['transactionIds'][] = $txn;
        $batches[$last]['headerBlocks'][] = $block;

        return true;
    }

    /**
     * A JSON transaction body ({operations, requiredLeases, kv, timers}) as
     * the binary one, when its pushes can travel in it: there is at least one,
     * and none carries a trace id or headers the block cannot carry.
     * Everything but the pushes (the acks, the links) stays in the JSON head;
     * consecutive pushed items of one partition share a group, so the pushes
     * keep their order. Null otherwise: the caller then sends the JSON body.
     *
     * The second value is null when the broker's answer lists the operations
     * as the caller added them (every other operation before every push).
     * Otherwise $order[$k] is the place, among the caller's operations, of the
     * k-th result the broker sends: it lists the head's operations first, then
     * the pushes.
     *
     * @return array{0: string, 1: list<int>|null}|null
     */
    public static function transactionFrames(array $request): ?array
    {
        $operations = $request['operations'] ?? null;
        if (!is_array($operations) || !array_is_list($operations)) {
            return null;
        }
        $headOperations = [];
        $batches = [];
        $headAt = [];
        $pushAt = [];
        $flat = 0;
        foreach ($operations as $operation) {
            if (!is_array($operation) || ($operation['type'] ?? null) !== 'push') {
                $headOperations[] = $operation;
                $headAt[] = $flat++;
                continue;
            }
            // A push names its items, or is one item itself.
            $items = $operation['items'] ?? null;
            foreach (is_array($items) && array_is_list($items) ? $items : [$operation] as $item) {
                if (!self::addItem($batches, $item, true)) {
                    return null;
                }
                $pushAt[] = $flat++;
            }
        }
        if ($batches === []) {
            return null;
        }
        try {
            $head = json_encode(['operations' => $headOperations] + $request, JSON_THROW_ON_ERROR);
            $body = self::encodeTransactionFrames($head, $batches);
        } catch (\JsonException|\LengthException) {
            // The JSON request says so as it always did.
            return null;
        }
        $order = array_merge($headAt, $pushAt);

        return [$body, $order === array_keys($order) ? null : $order];
    }

    /**
     * Puts the results of a binary transaction back in the order the caller
     * added its operations in, and gives each its own index again. The riders'
     * results, after the operations', are already in place.
     *
     * @param list<int>|null $order
     */
    public static function restoreResultOrder(mixed $answer, ?array $order): mixed
    {
        if ($order === null || !is_array($answer) || !is_array($answer['results'] ?? null)
            || count($answer['results']) < count($order)) {
            return $answer;
        }
        $results = array_values($answer['results']);
        $restored = $results;
        foreach ($order as $k => $at) {
            $result = $results[$k];
            $restored[$at] = is_array($result) ? ['index' => $at] + $result : $result;
        }
        $answer['results'] = $restored;

        return $answer;
    }

    /** The lower-case hyphenated text of 16 id bytes. */
    public static function uuidText(string $bytes): string
    {
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
    }

    private static function text16(string $text, string $what): string
    {
        if (strlen($text) > 0xffff) {
            throw new \LengthException("{$what} longer than 65535 bytes");
        }

        return pack('v', strlen($text)) . $text;
    }

    private static function check(array $batch): void
    {
        if (isset($batch['transactionIds']) && count($batch['transactionIds']) !== count($batch['payloads'])) {
            throw new \InvalidArgumentException(
                count($batch['transactionIds']) . ' transaction ids for ' . count($batch['payloads']) . ' payloads'
            );
        }
        if (isset($batch['headerBlocks']) && count($batch['headerBlocks']) !== count($batch['payloads'])) {
            throw new \InvalidArgumentException(
                count($batch['headerBlocks']) . ' header blocks for ' . count($batch['payloads']) . ' payloads'
            );
        }
    }
}
