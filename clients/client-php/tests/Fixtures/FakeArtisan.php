<?php

echo json_encode([
    'arguments' => array_slice($argv, 1),
    'telemetry_directory' => getenv('QUEEN_SUPERVISOR_TELEMETRY_DIR'),
    'consumer_group' => getenv('QUEEN_LARAVEL_CONSUMER_GROUP'),
    'connection' => getenv('QUEEN_LARAVEL_CONNECTION'),
    'supervisor' => getenv('QUEEN_LARAVEL_SUPERVISOR'),
    'retry_after' => getenv('QUEEN_LARAVEL_RETRY_AFTER'),
    'block_for' => getenv('QUEEN_LARAVEL_BLOCK_FOR'),
], JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR), PHP_EOL;
