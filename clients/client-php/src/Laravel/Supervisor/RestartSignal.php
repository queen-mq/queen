<?php

namespace Queen\Laravel\Supervisor;

use Illuminate\Contracts\Container\Container;

/**
 * The signal `php artisan queue:restart` leaves in the cache, read the way
 * Laravel's Worker reads it: a baseline when the process starts, and a
 * restart once the value differs from it.
 */
final class RestartSignal
{
    /** Where `php artisan queue:restart` leaves its signal: Laravel's Worker reads the same key. */
    public const KEY = 'illuminate:queue:restart';

    private mixed $baseline = null;

    /** Whether the cache answered when the baseline was read. */
    private bool $known = false;

    private function __construct(private Container $app)
    {
    }

    /** Read the baseline now. */
    public static function remember(Container $app): self
    {
        $signal = new self($app);
        try {
            $signal->baseline = $app->make('cache.store')->get(self::KEY);
            $signal->known = true;
        } catch (\Throwable) {
            // Without a baseline nothing can be called newer.
        }

        return $signal;
    }

    /** Whether `queue:restart` ran after the baseline was read. */
    public function restarted(): bool
    {
        if (!$this->known) {
            return false;
        }
        try {
            $signal = $this->app->make('cache.store')->get(self::KEY);
        } catch (\Throwable) {
            return false;
        }

        // Laravel's Worker compares the timestamps loosely too.
        return $signal !== null && $signal != $this->baseline;
    }
}
