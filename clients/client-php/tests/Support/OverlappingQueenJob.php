<?php

namespace Queen\Tests\Support;

use Fiber;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\WithoutOverlapping;
use RuntimeException;

class OverlappingQueenJob implements ShouldQueue
{
    use InteractsWithQueue;

    /** @var list<string> */
    public static array $entered = [];

    /** @var list<string> */
    public static array $completed = [];

    public function __construct(
        public string $name,
        public string $lockKey,
        public int $releaseAfter = 0,
        public int $expiresAfter = 30,
        public bool $suspend = false,
        public bool $shared = false,
        public ?string $enteredSignal = null,
        public ?string $releaseSignal = null,
        public int $signalTimeoutSeconds = 10,
        public bool $failAfterEntering = false,
    ) {
    }

    /** @return list<WithoutOverlapping> */
    public function middleware(): array
    {
        $middleware = (new WithoutOverlapping($this->lockKey))
            ->releaseAfter($this->releaseAfter)
            ->expireAfter($this->expiresAfter);

        if ($this->shared) {
            $middleware->shared();
        }

        return [$middleware];
    }

    public function handle(): void
    {
        self::$entered[] = $this->name;

        if ($this->enteredSignal !== null) {
            if (file_put_contents($this->enteredSignal, $this->name, LOCK_EX) === false) {
                throw new RuntimeException('Unable to publish the overlap-lock test signal.');
            }

            $deadline = microtime(true) + $this->signalTimeoutSeconds;
            while ($this->releaseSignal === null || !is_file($this->releaseSignal)) {
                if (microtime(true) >= $deadline) {
                    throw new RuntimeException('Timed out while holding the overlap-lock test fixture.');
                }
                usleep(10_000);
            }
        } elseif ($this->suspend) {
            Fiber::suspend($this->name);
        }

        if ($this->failAfterEntering) {
            throw new RuntimeException('Intentional overlap-lock job failure.');
        }

        self::$completed[] = $this->name;
    }

    public static function reset(): void
    {
        self::$entered = [];
        self::$completed = [];
    }
}
