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

    public function __construct(HttpClient $httpClient, string $method, string $path, ?array $body)
    {
        $this->httpClient = $httpClient;
        $this->method = $method;
        $this->path = $path;
        $this->body = $body;
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
