<?php

namespace Queen\Builders;

use Queen\Http\HttpClient;
use Queen\Queen;
use Queen\Consumer\ConsumerManager;

class ConsumeBuilder
{
    private HttpClient $httpClient;
    private Queen $queen;
    private \Closure $handler;
    private array $options;
    private ?\Closure $onSuccessCallback = null;
    private ?\Closure $onErrorCallback = null;

    public function __construct(HttpClient $httpClient, Queen $queen, \Closure $handler, array $options)
    {
        $this->httpClient = $httpClient;
        $this->queen = $queen;
        $this->handler = $handler;
        $this->options = $options;
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

    public function execute(): void
    {
        $consumerManager = new ConsumerManager($this->httpClient, $this->queen);

        $handler = $this->handler;
        $onSuccess = $this->onSuccessCallback;
        $onError = $this->onErrorCallback;

        // Callbacks are observers — autoAck remains as configured.
        // ConsumerManager handles ack/nack. The wrapper just adds
        // onSuccess/onError notifications around the user handler.
        $wrappedHandler = function (array|object $msgOrMsgs) use ($handler, $onSuccess, $onError): void {
            try {
                $handler($msgOrMsgs);

                if ($onSuccess !== null) {
                    $onSuccess($msgOrMsgs);
                }
            } catch (\Throwable $error) {
                if ($onError !== null) {
                    $onError($msgOrMsgs, $error);
                    // Don't re-throw — onError handled it.
                    // ConsumerManager's autoAck will NACK via its own catch.
                    // We need to re-throw so ConsumerManager sees the failure.
                }
                throw $error;
            }
        };

        $consumerManager->start($wrappedHandler, $this->options);
    }
}
