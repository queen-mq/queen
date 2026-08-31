<?php

namespace Queen\Support;

/**
 * Pop autopilot, client side.
 *
 * The broker owns a controller that sizes a pop from state this client cannot
 * see: how many partitions of the (queue, group) are ready, how old their
 * oldest ready message is, at what rate messages are arriving. Two knobs are
 * under its control — `partitions` (the sweep width) and `batch` (the message
 * budget for the sweep).
 *
 * THE RULE, and it is the only one: an explicit user value is sacred. Autopilot
 * applies ONLY to the knobs the user left unset, and it applies to them one by
 * one. A consumer that pins `partitions(1)` and says nothing about batch keeps
 * its single-partition claim forever and lets the broker size the batch; the
 * pinned dimension is never "adjusted", not even towards a value the controller
 * would consider better.
 *
 * The wire shape follows the conflation precedent (see ConflationGuard): a
 * client that is not engaging autopilot sends the byte-identical request it
 * sent before this feature existed.
 *
 *   autopilot=true      emitted ONLY when at least one of the two knobs is
 *                       being left to the broker. Never as autopilot=false.
 *   partitions / batch  OMITTED for the dimensions the broker is choosing,
 *                       sent exactly as before for the ones the user set.
 *
 * WHAT AN OLD BROKER DOES, and why there is no capability check here. A broker
 * older than 1.2 ignores unknown query params: the request succeeds, and the
 * two omitted knobs fall back to the SERVER-side defaults (batch 200,
 * partitions 1) instead of the old client-side ones. That is a sizing
 * difference, not a correctness one — nothing is lost, misordered or delivered
 * twice — so unlike conflation (which silently hands a last-value consumer a
 * whole backlog, hence ConflationUnsupportedException) this degrades quietly
 * and on purpose. Callers who need the old numbers against an old broker set
 * them explicitly, or turn autopilot off.
 */
final class PopAutopilot
{
    /**
     * The environment variable that disables pop autopilot for a whole process:
     * QUEEN_SDK_POP_AUTOPILOT=off restores the client-side defaults this SDK
     * applied before autopilot existed, byte for byte. It is read once, in the
     * Queen constructor, so a single deployment can be rolled back without
     * touching code.
     *
     * "off", "false", "0", "no" and "disabled" all disable it
     * (case-insensitive, surrounding space ignored). Every other value,
     * including the empty one, leaves autopilot on.
     */
    public const ENV_VAR = 'QUEEN_SDK_POP_AUTOPILOT';

    private const DISABLING = ['off', 'false', '0', 'no', 'disabled'];

    /** The sleep between two empty pops that are NOT long-polling, in microseconds. */
    public const EMPTY_POLL_BACKOFF_MICROS = 100_000;

    /** Whether ENV_VAR asks for the pre-autopilot behavior. */
    public static function disabledByEnv(): bool
    {
        // getenv() misses variables that only ever reached $_ENV/$_SERVER (a
        // php-fpm pool, a Laravel .env loaded into the superglobals), and those
        // are exactly the places an operator would set a rollback switch.
        $raw = getenv(self::ENV_VAR);
        if ($raw === false) {
            $raw = $_ENV[self::ENV_VAR] ?? $_SERVER[self::ENV_VAR] ?? '';
        }

        return in_array(strtolower(trim((string) $raw)), self::DISABLING, true);
    }

    /**
     * The batch/partitions/autopilot decision for one pop — the values that
     * travel and the ones that do not.
     *
     * IT EXISTS SO THERE IS EXACTLY ONE COPY OF THE EMISSION RULE. This SDK has
     * THREE pop parameter builders (QueueBuilder::pop, ConsumerManager and
     * HighLevelConsumer) and they have drifted before — PLAN_CONFLATION §4 opens
     * on a bug of exactly that shape. A rule with three branches and a
     * per-dimension carve-out is precisely the kind that gets copied wrong, so
     * all three call this and then only PLACE what it returns; where each key
     * sits in the query string stays with the builder, because the
     * pre-autopilot key order is part of what "byte-identical" means here.
     *
     * @param int|null $batch          the USER's batch. null or 0 means unset:
     *                                 the dimension the broker gets to choose.
     *                                 No builder may substitute a default first.
     * @param int|null $maxPartitions  the USER's sweep width, same convention.
     * @param int      $fallbackBatch  client-side default applied to an unset
     *                                 batch when autopilot is NOT engaged.
     * @param bool     $autopilot      the resolved decision for this call.
     *
     * @return array{autopilot: bool, batch: ?string, partitions: ?string}
     *         Strings are ready to place; null means "this key does not travel".
     */
    public static function sizing(?int $batch, ?int $maxPartitions, int $fallbackBatch, bool $autopilot): array
    {
        $batchSet = $batch !== null && $batch > 0;
        $partitionsSet = $maxPartitions !== null && $maxPartitions > 0;

        // Note the case that looks like an omission and is not: when the user
        // set BOTH knobs there is nothing left for the controller to decide, so
        // autopilot=true is NOT emitted and the request is byte-identical to the
        // one this SDK sent before autopilot existed. Sending the flag anyway
        // would be harmless on the broker and dishonest in a packet capture.
        if ($autopilot && !($batchSet && $partitionsSet)) {
            return [
                'autopilot' => true,
                'batch' => $batchSet ? (string) $batch : null,
                'partitions' => $partitionsSet ? (string) $maxPartitions : null,
            ];
        }

        return [
            'autopilot' => false,
            'batch' => (string) ($batchSet ? $batch : $fallbackBatch),
            // The legacy gate: partitions travels only above 1, because 1 IS the
            // server-side default and a v4-era client never sent it.
            'partitions' => $partitionsSet && $maxPartitions > 1 ? (string) $maxPartitions : null,
        ];
    }

    /**
     * What the broker chose for one pop, echoed back in the response under
     * "autopilot" when the request engaged autopilot. Additive: a broker that
     * does not send it, or a pop that never asked, yields null.
     *
     * Reading it is optional — the messages are already sized by it — but it is
     * the only way to see the controller working from the client side, and the
     * only input to the empty-poll pacing below.
     *
     * Unknown keys inside it are ignored, and an unknown-shaped value is treated
     * as absent rather than as an error: this field is the broker telling the
     * client what it did, and a client that refuses to run because a newer
     * broker grew a fourth number would be a self-inflicted outage.
     *
     * @param  array|null $result  parsed pop response (null for a 204)
     * @return array{partitions: int, batch: int, waitMillis: int}|null
     */
    public static function decision(?array $result): ?array
    {
        $raw = $result['autopilot'] ?? null;
        if (!is_array($raw)) {
            return null;
        }

        $num = static fn ($v): int => is_int($v) || is_float($v) ? (int) $v : 0;

        return [
            // Sweep width the broker used for this pop.
            'partitions' => $num($raw['partitions'] ?? null),
            // Message budget the broker used for this pop.
            'batch' => $num($raw['batch'] ?? null),
            // The broker's advice on how long to wait before polling again
            // (wire name: waitMs). Present only when the broker has an opinion,
            // and it is advice, not a lease: the consume loop honors it for the
            // sleep it was already taking between empty non-waiting pops,
            // nothing more. 0 = no advice.
            'waitMillis' => $num($raw['waitMs'] ?? null),
        ];
    }

    /**
     * How long to wait after an empty pop, in microseconds: the broker's advice
     * when it gave one, the historical 100ms otherwise.
     *
     * The advice is honored as given, without a ceiling of this client's
     * invention: the broker knows the arrival rate on this queue and this
     * client does not.
     *
     * @param array{partitions: int, batch: int, waitMillis: int}|null $decision
     */
    public static function emptyPollDelayMicros(?array $decision): int
    {
        if ($decision !== null && $decision['waitMillis'] > 0) {
            return $decision['waitMillis'] * 1000;
        }

        return self::EMPTY_POLL_BACKOFF_MICROS;
    }
}
