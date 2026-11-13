<?php

namespace Queen\Laravel\Contracts;

/**
 * Give a Laravel job's Queen message headers.
 *
 * The driver sends them beside the job payload, never inside it, so a reader
 * selects or re-keys the jobs on them without unserializing one: a filtered
 * group (`where headers.job = 'App\Jobs\SendInvoice'`), a reading queue
 * (`partitionBy: 'headers.customer'`), the console.
 *
 *     public function queenHeaders(): array
 *     {
 *         return ['job' => static::class, 'customer' => (string) $this->customerId];
 *     }
 *
 * Names are 1 to 64 bytes of a-z 0-9 . _ - ("queen-" is the broker's), values
 * text: an int or a float is written as its text. Headers are not encrypted:
 * no secrets or personal data. They need a cluster that reads catalogue
 * version 8.
 *
 * The job keeps them wherever the driver puts it back on the queue: a delay
 * (its timer's message carries them), a release, a hand-back, and a
 * queue:retry (the failed copy names them in its `_queen` block, and the
 * retried push sends them as the message's again).
 */
interface QueenHeaders
{
    /** @return array<string, string|int|float> */
    public function queenHeaders(): array;
}
