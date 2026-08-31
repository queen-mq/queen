<?php

namespace Queen\Laravel;

use Illuminate\Support\Facades\Facade;

/**
 * @method static \Queen\Builders\QueueBuilder queue(?string $name = null)
 * @method static \Queen\Builders\TransactionBuilder transaction()
 * @method static \Queen\Admin admin()
 * @method static array ack(array|string $message, bool|string $status = true, array $context = [])
 * @method static array renew(string|array $messageOrLeaseId, ?int $seconds = null)
 * @method static void flushAllBuffers()
 * @method static array getBufferStats()
 * @method static void close()
 *
 * @see \Queen\Queen
 */
class QueenFacade extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'queen';
    }
}
