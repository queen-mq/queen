<?php

namespace Queen\Support;

/**
 * Factories for the timer operations, and the ONE place their wire shape is
 * written down. Shared by Queen\Timers and by the timer rider of a transaction,
 * for the same reason KvOp is shared.
 *
 * TWO RULES OF THE WIRE, both of which are cheap to break silently.
 *
 * ONLY RELATIVE DURATIONS, IN MILLISECONDS. The field is `delayMs`. An absolute
 * instant is not expressible: `deliverAt` is computed inside Postgres as
 * now() + the delay, so there is exactly one clock and no skew between brokers
 * can enter anywhere. The product's declared convention is "durations that can
 * be sub-second are in milliseconds, the ones that cannot are in seconds" — a
 * 250 ms retry backoff is a real and central use of a timer, a sub-second TTL
 * is not a real use for anybody. A delay in the PAST is legal and fires on the
 * first sweep.
 *
 * `producerSub`, `messageId` AND THE TENANT ARE NOT INPUTS. Present in an
 * operation they are a 400, never a silent drop: a caller posting
 * {"producerSub":"billing-service"} would otherwise get, a second later, a
 * frame in the log whose provenance is attested by the broker and forged by the
 * client, and producer_sub is the one non-repudiable field a frame has.
 *
 * AND THE ONE THAT HURTS USERS, about cancel. There is no tombstone: a
 * delivered timer no longer has a row, so a cancel that arrives afterwards
 * answers `absent`. **`absent` means "no longer pending" and may mean ALREADY
 * DELIVERED.** The authority is the log — look for the timer's `txn` in the
 * destination queue, which is why cancel echoes it back. Any saga that cancels
 * a compensation timer must have the compensation consumer check the saga's KV
 * state before compensating; without it, "the timer fired 5 ms before the
 * cancel" unwinds a reservation that has already shipped, and the cancel
 * answered `absent` while looking like a success.
 */
final class TimerOp
{
    /**
     * Fields the SERVER owns, plus the two spellings of a duration this wire
     * does not speak. The broker refuses every one of them; refusing here means
     * a caller finds out at their first unit test.
     */
    private const NOT_AN_INPUT = [
        'producerSub' => 'the producer identity is stamped from the authenticated request',
        'producer_sub' => 'the producer identity is stamped from the authenticated request',
        'messageId' => 'the message id is minted by the broker and returned to you at schedule time',
        'message_id' => 'the message id is minted by the broker and returned to you at schedule time',
        'tenant' => 'the tenant comes from the authenticated request',
        'tenantId' => 'the tenant comes from the authenticated request',
        '_tenant' => 'the tenant comes from the authenticated request',
        'deliverAt' => 'an absolute instant is not expressible; the wire carries only the relative delayMs',
        'deliver_at' => 'an absolute instant is not expressible; the wire carries only the relative delayMs',
        'delaySeconds' => 'the field is `delayMs`: durations that can be sub-second are in milliseconds',
        'delay_seconds' => 'the field is `delayMs`: durations that can be sub-second are in milliseconds',
        'attempts' => 'the retry budget is the broker\'s',
        'claimToken' => 'the claim is the broker\'s',
        'claimedUntil' => 'the claim is the broker\'s',
    ];

    /**
     * Upsert on (queue, timerKey). `deliverAt` is "not before", never "exactly
     * at": a healthy timer lands within a sweep cycle of its delay.
     *
     * @param int $delayMs milliseconds from now. Negative is legal and fires on
     *   the first cycle.
     * @param mixed $payload any JSON-serializable value. It is JSON-encoded and
     *   then base64-encoded, so the consumer of the destination queue reads the
     *   same shape it would have read from a push.
     * @param array $opts txn, partition.
     */
    public static function schedule(string $queue, string $timerKey, int $delayMs, mixed $payload, array $opts = []): array
    {
        return self::upsert('schedule', $queue, $timerKey, $delayMs, $payload, $opts);
    }

    /**
     * The SAME upsert as schedule, which is what makes a client retry after a
     * crash safe by construction. `attempts` goes back to zero and the last
     * error is cleared: a rescheduled timer is a NEW timer under an OLD name,
     * and a freshly corrected payload must not inherit the budget spent by the
     * one that was poisoning things.
     *
     * Answers `too_late` if the timer is already claimed — granting it would
     * deliver the OLD payload after the caller believes it replaced it.
     */
    public static function reschedule(string $queue, string $timerKey, int $delayMs, mixed $payload, array $opts = []): array
    {
        return self::upsert('reschedule', $queue, $timerKey, $delayMs, $payload, $opts);
    }

    /**
     * @param string|null $txn the txn you expect this timer to carry. Echoed
     *   back on `absent` so the "was it already delivered?" check needs no
     *   second API call.
     */
    public static function cancel(string $queue, string $timerKey, ?string $txn = null): array
    {
        $op = ['op' => 'cancel', 'queue' => $queue, 'timerKey' => $timerKey];

        if ($txn !== null) {
            $op['txn'] = $txn;
        }

        return $op;
    }

    private static function upsert(string $kind, string $queue, string $timerKey, int $delayMs, mixed $payload, array $opts): array
    {
        foreach ($opts as $name => $value) {
            if (isset(self::NOT_AN_INPUT[$name])) {
                throw new \InvalidArgumentException(
                    "timer option `{$name}` is not part of the wire: " . self::NOT_AN_INPUT[$name]
                );
            }

            if (!in_array($name, ['txn', 'partition'], true)) {
                throw new \InvalidArgumentException(
                    "unknown timer option `{$name}`; schedule accepts: txn, partition"
                );
            }

            if ($value === null) {
                throw new \InvalidArgumentException(
                    "timer option `{$name}` is null; drop the option instead of passing null"
                );
            }
        }

        $op = [
            'op' => $kind,
            'queue' => $queue,
            'timerKey' => $timerKey,
            'delayMs' => $delayMs,
            // The txn is mandatory on the wire and identifies the frame this
            // timer will become. Absent, mint one here rather than leaving the
            // broker to do it — the same contract QueueBuilder::push keeps for
            // transactionId, and the reason a cancel can be verified against
            // the destination queue at all.
            'txn' => $opts['txn'] ?? Uuid::v7(),
            'payload' => base64_encode(json_encode($payload, JSON_THROW_ON_ERROR)),
        ];

        if (isset($opts['partition'])) {
            $op['partition'] = $opts['partition'];
        }

        return $op;
    }
}
