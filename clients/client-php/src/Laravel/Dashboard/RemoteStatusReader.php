<?php

namespace Queen\Laravel\Dashboard;

use Queen\Laravel\Supervisor\RemoteStatusDocument;
use Queen\Queen;

/**
 * Reads the status document a supervisor published to the broker's key/value
 * store (see RemoteStatusPublisher and RemoteStatusDocument).
 *
 * The document is untrusted input exactly like a local status.json: the
 * repository normalizes every field before anything reaches a view. This
 * reader only enforces the transport format.
 */
final class RemoteStatusReader
{
    public function __construct(
        private Queen $queen,
        private string $namespace,
        private string $key,
    ) {
    }

    /**
     * @return array<string, mixed>|null null when the key is missing or expired,
     *   or when the broker cannot be read
     */
    public function read(): ?array
    {
        try {
            // One getPrefix call is one snapshot, so the head and its chunks
            // come from the same committed write.
            $result = $this->queen->kv()->getPrefix(
                $this->namespace,
                RemoteStatusDocument::prefix($this->key),
                ['limit' => RemoteStatusDocument::PREFIX_LIMIT],
            );
        } catch (\Throwable) {
            return null;
        }

        if (($result['truncated'] ?? null) !== false) {
            return null;
        }

        return RemoteStatusDocument::decode($result['rows'] ?? null, $this->key);
    }
}
