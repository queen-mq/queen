<?php

namespace Queen\Http;

/**
 * A request CurlTransport started and has not settled yet.
 *
 * @internal
 */
final class DetachedRequest
{
    /** Filled by the header callback while the answer arrives. */
    public string $retryAfter = '';

    public function __construct(
        public readonly \CurlHandle $handle,
        public readonly string $url,
    ) {
    }
}
