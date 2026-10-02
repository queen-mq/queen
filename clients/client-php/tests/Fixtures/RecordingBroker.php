<?php

// A router for `php -S`: logs each request as one JSON line and answers
// every one with a committed transaction.
file_put_contents((string) getenv('QUEEN_TEST_BROKER_LOG'), json_encode([
    'method' => $_SERVER['REQUEST_METHOD'] ?? '',
    'path' => parse_url((string) ($_SERVER['REQUEST_URI'] ?? ''), PHP_URL_PATH),
    'authorization' => $_SERVER['HTTP_AUTHORIZATION'] ?? null,
    'body' => file_get_contents('php://input'),
]) . "\n", FILE_APPEND | LOCK_EX);
header('Content-Type: application/json');
echo json_encode(['success' => true, 'transactionId' => 'recorded', 'results' => []]);
