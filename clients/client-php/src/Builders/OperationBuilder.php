<?php

namespace Queen\Builders;

use Queen\Http\HttpClient;

class OperationBuilder
{
    private HttpClient $httpClient;
    private string $method;
    private string $path;
    private ?array $body;
    private ?\Closure $onSuccessCallback = null;
    private ?\Closure $onErrorCallback = null;
    /** Throws when the answer does not show the request took effect. */
    private ?\Closure $check;

    public function __construct(HttpClient $httpClient, string $method, string $path, ?array $body, ?\Closure $check = null)
    {
        $this->httpClient = $httpClient;
        $this->method = $method;
        $this->path = $path;
        $this->body = $body;
        $this->check = $check;
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

    public function execute(): mixed
    {
        try {
            $result = match ($this->method) {
                'GET' => $this->httpClient->get($this->path),
                'POST' => $this->httpClient->post($this->path, $this->body),
                'PUT' => $this->httpClient->put($this->path, $this->body),
                'DELETE' => $this->httpClient->delete($this->path),
            };

            if (is_array($result) && isset($result['error'])) {
                $error = new \RuntimeException($result['error']);
                if ($this->onErrorCallback !== null) {
                    ($this->onErrorCallback)($error);
                    return ['success' => false, 'error' => $result['error']];
                }
                throw $error;
            }

            // As an error answer: to onError, or thrown.
            if ($this->check !== null) {
                ($this->check)($result);
            }

            if ($this->onSuccessCallback !== null) {
                ($this->onSuccessCallback)($result);
            }

            return $result;
        } catch (\Throwable $error) {
            if ($this->onErrorCallback !== null) {
                ($this->onErrorCallback)($error);
                return ['success' => false, 'error' => $error->getMessage()];
            }
            throw $error;
        }
    }
}
