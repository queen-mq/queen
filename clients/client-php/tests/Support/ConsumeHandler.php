<?php

namespace Queen\Tests\Support;

/**
 * A queen:consume handler whose behaviour a test sets. Bind an instance in
 * the container; the command resolves the class through it.
 */
final class ConsumeHandler
{
    /** @var list<array> What handle() received, in order. */
    public array $received = [];

    /** @param (\Closure(array): void)|null $callback */
    public function __construct(private ?\Closure $callback = null)
    {
    }

    public function handle(array $messages): void
    {
        $this->received[] = $messages;
        if ($this->callback !== null) {
            ($this->callback)($messages);
        }
    }
}
