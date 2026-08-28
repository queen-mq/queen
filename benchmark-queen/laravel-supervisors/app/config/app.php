<?php

return [
    'name' => env('APP_NAME', 'Queen supervisor benchmark'),
    'env' => env('APP_ENV', 'benchmark'),
    'debug' => (bool) env('APP_DEBUG', false),
    'url' => env('APP_URL', 'http://localhost'),
    'timezone' => 'UTC',
    'locale' => 'en',
    'fallback_locale' => 'en',
    'faker_locale' => 'en_US',
    'cipher' => 'AES-256-CBC',
    // No encrypted application data is produced; this deterministic key only
    // keeps framework services bootable in the isolated benchmark fixture.
    'key' => env('APP_KEY', 'base64:cXVlZW4tYmVuY2htYXJrLWtleS0wMTIzNDU2Nzg5MDE='),
    'previous_keys' => [],
    'maintenance' => [
        'driver' => 'file',
        'store' => 'array',
    ],
];
