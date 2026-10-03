<?php

namespace Queen\Laravel\Dashboard;

use Queen\Laravel\Supervisor\RemoteStatusDocument;
use Queen\Queen;

/**
 * Reads the status documents supervisors published to the broker's key/value
 * store (see RemoteStatusPublisher and RemoteStatusDocument): one per
 * supervisor instance, so every host or pod sharing the key is listed.
 *
 * The documents are untrusted input exactly like a local status.json: the
 * repository normalizes every field before anything reaches a view. This
 * reader only enforces the transport format.
 */
final class RemoteStatusReader
{
    /** Bounds one dashboard render; each page is bounded by the broker. */
    private const MAX_PAGES = 16;

    public function __construct(
        private Queen $queen,
        private string $namespace,
        private string $key,
    ) {
    }

    /**
     * @return list<array<string, mixed>> every complete published document, in
     *   key order; empty when none is published or the broker cannot be read
     */
    public function read(): array
    {
        $prefix = RemoteStatusDocument::prefix($this->key);
        $rows = [];
        $after = null;
        for ($page = 0; $page < self::MAX_PAGES; ++$page) {
            $options = ['limit' => RemoteStatusDocument::PREFIX_LIMIT];
            if ($after !== null) {
                $options['after'] = $after;
            }
            try {
                $result = $this->queen->kv()->getPrefix($this->namespace, $prefix, $options);
            } catch (\Throwable) {
                break;
            }

            $pageRows = $result['rows'] ?? null;
            $truncated = $result['truncated'] ?? null;
            if (!is_array($pageRows) || !array_is_list($pageRows) || !is_bool($truncated)) {
                break;
            }
            $keys = [];
            foreach ($pageRows as $row) {
                if (!is_array($row) || !is_string($row['key'] ?? null)) {
                    break 2;
                }
                $keys[] = $row['key'];
            }
            if (!$truncated) {
                array_push($rows, ...$pageRows);
                break;
            }
            if ($pageRows === []) {
                break;
            }

            // Every page is its own snapshot. The slot the page cut through is
            // held back and read whole on the next page, so no document is
            // assembled from two snapshots.
            $keep = $this->completeSlotRows($keys);
            $resumeAfter = $keys[$keep - 1];
            if ($resumeAfter === $after) {
                break;
            }
            array_push($rows, ...array_slice($pageRows, 0, $keep));
            $after = $resumeAfter;
        }

        return RemoteStatusDocument::decodeAll($rows, $this->key);
    }

    /**
     * How many leading rows of a truncated page belong to slots the page
     * holds whole: all but the trailing run of the last row's slot. A page
     * holding nothing but that run is kept as it is, because no page could
     * hold the slot whole.
     *
     * @param non-empty-list<string> $keys
     */
    private function completeSlotRows(array $keys): int
    {
        $count = count($keys);
        $last = RemoteStatusDocument::slot($keys[$count - 1], $this->key);
        if ($last === null) {
            return $count;
        }
        $keep = $count;
        while ($keep > 0 && RemoteStatusDocument::slot($keys[$keep - 1], $this->key) === $last) {
            --$keep;
        }

        return $keep > 0 ? $keep : $count;
    }
}
