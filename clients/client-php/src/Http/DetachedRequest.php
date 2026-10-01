<?php

namespace Queen\Http;

/**
 * A request CurlTransport started and has not settled yet; dropping it frees
 * its cURL handle.
 *
 * @internal
 */
final class DetachedRequest
{
    /** @var list<string> Filled by the header callback while the answer arrives. */
    public array $retryAfter = [];

    private ?\Closure $release;

    public function __construct(
        public readonly \CurlHandle $handle,
        public readonly string $url,
        \Closure $release,
    ) {
        $this->release = $release;
    }

    public function release(): void
    {
        $release = $this->release;
        $this->release = null;
        if ($release !== null) {
            $release();
        }
    }

    public function __destruct()
    {
        $this->release();
    }
}
