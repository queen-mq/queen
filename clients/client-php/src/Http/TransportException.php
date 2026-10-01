<?php

namespace Queen\Http;

use GuzzleHttp\Exception\TransferException;

/**
 * No HTTP answer arrived: the connection failed or timed out. A Guzzle
 * TransferException, as the Guzzle path threw, so existing catch blocks hold.
 */
final class TransportException extends TransferException
{
    public static function fromCurl(int $code, string $error, string $url): self
    {
        $host = parse_url($url, PHP_URL_HOST);
        $where = is_string($host) ? " ({$host})" : '';

        return new self("cURL error {$code}: {$error}{$where}", $code);
    }
}
