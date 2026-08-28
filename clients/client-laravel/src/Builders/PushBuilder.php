<?php

namespace Queen\Builders;

use Queen\Http\HttpClient;
use Queen\Buffer\BufferManager;

class PushBuilder
{
    private HttpClient $httpClient;
    private BufferManager $bufferManager;
    private string $queueName;
    private string $partition;
    private array $formattedItems;
    private ?array $bufferOptions;
    private ?\Closure $onSuccessCallback = null;
    private ?\Closure $onErrorCallback = null;
    private ?\Closure $onDuplicateCallback = null;

    public function __construct(
        HttpClient $httpClient,
        BufferManager $bufferManager,
        string $queueName,
        string $partition,
        array $formattedItems,
        ?array $bufferOptions
    ) {
        $this->httpClient = $httpClient;
        $this->bufferManager = $bufferManager;
        $this->queueName = $queueName;
        $this->partition = $partition;
        $this->formattedItems = $formattedItems;
        $this->bufferOptions = $bufferOptions;
    }

    public function onSuccess(\Closure $callback): static
    {
        $this->onSuccessCallback = $callback;
        return $this;
    }

    public function onError(\Closure $callback): static
    {
        $this->onErrorCallback = $callback;
        return $this;
    }

    public function onDuplicate(\Closure $callback): static
    {
        $this->onDuplicateCallback = $callback;
        return $this;
    }

    public function execute(): mixed
    {
        // Client-side buffering
        if ($this->bufferOptions !== null) {
            // addMessage applies backpressure now: it can raise because the
            // buffer is at its maxSize bound (the message was refused), or
            // because the inline flush it triggered could not land inside
            // maxWaitMillis (the message is buffered, but undelivered). Either
            // way it is UNCONFIRMED, and reporting the whole batch as buffered
            // would be exactly the false success this fix removes — so count
            // only the adds that returned, and hand the rest to onError.
            $accepted = [];
            try {
                foreach ($this->formattedItems as $item) {
                    $queueAddress = "{$item['queue']}/{$item['partition']}";
                    $this->bufferManager->addMessage($queueAddress, $item, $this->bufferOptions);
                    $accepted[] = $item;
                }
            } catch (\Throwable $error) {
                $unconfirmed = array_slice($this->formattedItems, count($accepted));
                if ($this->onErrorCallback !== null) {
                    ($this->onErrorCallback)($unconfirmed, $error);
                    return null;
                }
                throw $error;
            }

            $result = ['buffered' => true, 'count' => count($accepted)];

            if ($this->onSuccessCallback !== null) {
                ($this->onSuccessCallback)($accepted);
            }

            return $result;
        }

        // Immediate push
        try {
            $results = $this->httpClient->post('/api/v1/push', ['items' => $this->formattedItems]);

            if (is_array($results) && !isset($results[0]) && isset($results['error'])) {
                // Non-array error response
                $error = new \RuntimeException($results['error']);
                if ($this->onErrorCallback !== null) {
                    ($this->onErrorCallback)($this->formattedItems, $error);
                    return $results;
                }
                throw $error;
            }

            if (is_array($results) && isset($results[0])) {
                // Array of per-item results
                $successful = [];
                $duplicates = [];
                $failed = [];

                foreach ($results as $i => $result) {
                    $originalItem = $this->formattedItems[$i] ?? null;

                    if (($result['status'] ?? null) === 'duplicate') {
                        $duplicates[] = array_merge($originalItem ?? [], ['result' => $result]);
                    } elseif (($result['status'] ?? null) === 'failed') {
                        $failed[] = array_merge($originalItem ?? [], ['result' => $result, 'error' => $result['error'] ?? null]);
                    } elseif (($result['status'] ?? null) === 'queued') {
                        $successful[] = array_merge($originalItem ?? [], ['result' => $result]);
                    }
                }

                if (!empty($duplicates) && $this->onDuplicateCallback !== null) {
                    ($this->onDuplicateCallback)($duplicates, new \RuntimeException('Duplicate transaction IDs detected'));
                }

                if (!empty($failed) && $this->onErrorCallback !== null) {
                    ($this->onErrorCallback)($failed, new \RuntimeException($failed[0]['error'] ?? 'Push failed'));
                }

                if (!empty($successful) && $this->onSuccessCallback !== null) {
                    ($this->onSuccessCallback)($successful);
                }

                if (!empty($failed) && $this->onErrorCallback === null) {
                    throw new \RuntimeException($failed[0]['error'] ?? 'Push failed');
                }

                return $results;
            }

            if ($this->onSuccessCallback !== null) {
                ($this->onSuccessCallback)($this->formattedItems);
            }

            return $results;
        } catch (\Throwable $error) {
            if ($this->onErrorCallback !== null) {
                ($this->onErrorCallback)($this->formattedItems, $error);
                return null;
            }
            throw $error;
        }
    }
}
