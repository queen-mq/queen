<?php

namespace Queen\Http;

/** No HTTP answer arrived: the connection failed or timed out. */
final class TransportException extends \RuntimeException
{
    public static function fromCurl(int $code, string $error, string $url): self
    {
        $host = parse_url($url, PHP_URL_HOST);
        $where = is_string($host) ? " ({$host})" : '';

        return new self("cURL error {$code}: {$error}{$where}", $code);
    }
}
