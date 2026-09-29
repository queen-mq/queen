<?php

namespace Queen\Laravel\Supervisor;

use Queen\Support\KvOp;

/**
 * Key/value wire format of a published supervisor status document.
 *
 * A key/value value is capped at 64 KiB (QUEEN_KV_MAX_VALUE_BYTES) while a
 * status document may reach the 1 MiB status-file ceiling, so the document is
 * split across keys that share one prefix:
 *
 *   <key>/head        {format, write, chunks, bytes}
 *   <key>/chunk/0000  {write, index, data}   data: base64 of a document slice
 *   ...
 *
 * The writer puts the head and every chunk in ONE batch call, which the broker
 * applies in one transaction. The reader fetches them with ONE getPrefix call,
 * which is one read-committed snapshot. Every chunk carries the head's `write`
 * id, so a chunk left behind by an earlier, larger document is recognised and
 * ignored; leftovers expire with their TTL. The head's byte count catches a
 * short read, and the document is still validated field by field by its
 * reader. Base64 keeps each value's size independent of JSON string escaping.
 *
 * Both engines publish it: the Rust supervisor writes exactly this format in
 * supervisor/src/remote_status.rs, because the dashboard reads nothing else.
 * A change here must land in both writers.
 */
final class RemoteStatusDocument
{
    public const FORMAT = 'queen.supervisor.remote-status/v1';

    /** The status-file ceiling; a larger document was never valid locally either. */
    public const MAX_DOCUMENT_BYTES = 1048576;

    /** Raw bytes per chunk: 60 000 base64 characters, well under the 64 KiB value cap. */
    public const CHUNK_BYTES = 45000;

    public const MAX_CHUNKS = 24;

    /** Head, current chunks and at most one earlier generation of leftovers. */
    public const PREFIX_LIMIT = 100;

    /**
     * @param array<string, mixed> $document
     * @return list<array<string, mixed>> KvOp put operations for one batch call
     */
    public static function operations(
        array $document,
        string $namespace,
        string $key,
        int $ttlSeconds,
        string $writeId,
    ): array {
        $encoded = json_encode($document, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
        $bytes = strlen($encoded);
        if ($bytes > self::MAX_DOCUMENT_BYTES) {
            throw new \RuntimeException(
                "the status document is {$bytes} bytes, above the " . self::MAX_DOCUMENT_BYTES . '-byte ceiling',
            );
        }

        $chunks = str_split($encoded, self::CHUNK_BYTES);
        $options = ['ttlSeconds' => $ttlSeconds];
        $operations = [KvOp::put($namespace, self::headKey($key), [
            'format' => self::FORMAT,
            'write' => $writeId,
            'chunks' => count($chunks),
            'bytes' => $bytes,
        ], $options)];
        foreach ($chunks as $index => $chunk) {
            $operations[] = KvOp::put($namespace, self::chunkKey($key, $index), [
                'write' => $writeId,
                'index' => $index,
                'data' => base64_encode($chunk),
            ], $options);
        }

        return $operations;
    }

    /**
     * Reassemble the document from the rows of one getPrefix call.
     *
     * @param mixed $rows the `rows` field of the getPrefix result
     * @return array<string, mixed>|null null unless one complete, consistent generation is present
     */
    public static function decode(mixed $rows, string $key): ?array
    {
        if (!is_array($rows) || !array_is_list($rows)) {
            return null;
        }
        $values = [];
        foreach ($rows as $row) {
            if (is_array($row) && is_string($row['key'] ?? null)) {
                $values[$row['key']] = $row['value'] ?? null;
            }
        }

        $head = $values[self::headKey($key)] ?? null;
        if (!is_array($head)
            || ($head['format'] ?? null) !== self::FORMAT
            || !is_string($head['write'] ?? null)
            || preg_match('/\A[0-9a-f]{32}\z/D', $head['write']) !== 1
            || !is_int($head['chunks'] ?? null)
            || $head['chunks'] < 1
            || $head['chunks'] > self::MAX_CHUNKS
            || !is_int($head['bytes'] ?? null)
            || $head['bytes'] < 1
            || $head['bytes'] > self::MAX_DOCUMENT_BYTES) {
            return null;
        }

        $encoded = '';
        for ($index = 0; $index < $head['chunks']; ++$index) {
            $chunk = $values[self::chunkKey($key, $index)] ?? null;
            if (!is_array($chunk)
                || ($chunk['write'] ?? null) !== $head['write']
                || ($chunk['index'] ?? null) !== $index
                || !is_string($chunk['data'] ?? null)) {
                return null;
            }
            $decoded = base64_decode($chunk['data'], true);
            if ($decoded === false) {
                return null;
            }
            $encoded .= $decoded;
            if (strlen($encoded) > $head['bytes']) {
                return null;
            }
        }
        if (strlen($encoded) !== $head['bytes']) {
            return null;
        }

        try {
            $document = json_decode($encoded, true, 64, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return null;
        }

        return is_array($document) && !array_is_list($document) ? $document : null;
    }

    public static function prefix(string $key): string
    {
        return $key . '/';
    }

    public static function newWriteId(): string
    {
        return bin2hex(random_bytes(16));
    }

    private static function headKey(string $key): string
    {
        return $key . '/head';
    }

    private static function chunkKey(string $key, int $index): string
    {
        return sprintf('%s/chunk/%04d', $key, $index);
    }
}
