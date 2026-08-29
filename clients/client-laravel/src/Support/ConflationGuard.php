<?php

namespace Queen\Support;

use Queen\Exceptions\ConflationUnsupportedException;
use Queen\Exceptions\ConflationPolicyMismatchException;

/**
 * The SDK half of the conflation contract (PLAN_CONFLATION §3.3, §4). Every pop
 * response this client receives passes through here — there are four places one
 * arrives (QueueBuilder::pop, the two ConsumerManager loops and the two
 * HighLevelConsumer methods) and they must all reach the same verdict.
 *
 * Two response keys, both emitted by the broker ONLY when true:
 *
 *   "conflation":true          this pop was served under last-value delivery.
 *   "conflationConflict":true  this pop declared a policy the group does not
 *                              have; the STORED group setting won.
 *
 * The verdict:
 *
 *   asked | conflation | conflict | outcome
 *   ------+------------+----------+-------------------------------------------
 *    no   |     -      |    no    | nothing to say
 *    no   |    yes     |    -     | RAISE before returning messages: this
 *         |            |          | consumer requires full delivery
 *    yes  |    yes     |    -     | working as declared (warn once on conflict)
 *    yes  |     no     |   yes    | the broker UNDERSTOOD the flag and declined
 *         |            |          | it — warn once, keep consuming (§3.3: a
 *         |            |          | reject here would take down the half of a
 *         |            |          | rolling deploy that is already correct)
 *    yes  |     no     |    no    | RAISE — broker older than 1.1.0 (§4)
 *
 * The last two rows are the whole reason the conflict key exists as a separate
 * signal: without it, a group whose stored policy is `false` would be
 * indistinguishable from a broker that never heard of conflation, and the SDK
 * would have to choose between killing correct consumers and silently draining
 * backlogs. It gets to do neither.
 */
final class ConflationGuard
{
    /**
     * §4, verbatim. Names the minimum broker version because "conflation did
     * not happen" is not actionable on its own — the operator needs to know
     * that the fix is a broker upgrade and not a consumer setting.
     */
    public const UNSUPPORTED_MESSAGE =
        'conflation was requested but this broker did not apply it — requires broker >= 1.1.0';

    /**
     * (queue, group) pairs already warned about. Per PROCESS, per §3.3: a
     * consumer polls thousands of times a minute and a per-response warning
     * would bury the one line that matters under its own repetitions.
     *
     * @var array<string, true>
     */
    private static array $warned = [];

    /**
     * @param mixed  $response  Decoded pop response. Null is what a bodiless
     *                          204 decodes to — the empty pop of an OLD broker,
     *                          and precisely the case that must still raise.
     * @param bool   $requested Whether THIS consumer asked for conflation.
     */
    public static function check(
        mixed $response,
        bool $requested,
        ?string $queue,
        ?string $group,
        ?string $namespace = null,
        ?string $task = null,
    ): void {
        $body = is_array($response) ? $response : [];
        $conflict = ($body['conflationConflict'] ?? false) === true;
        $applied = ($body['conflation'] ?? false) === true;

        // Pop maintenance is not version skew: the broker refused the pop before
        // it reached the claim path, so there is no policy to echo and nothing
        // to conclude from the absence of one. Without this, an operator pausing
        // pops would stop every conflating consumer in the fleet with a "broker
        // too old" exception.
        $paused = ($body['paused'] ?? false) === true;

        // Ordinary queue consumers require every message, so silently joining
        // a group whose persisted policy is conflation=true would discard
        // intermediate jobs. The broker echoes the effective policy even when
        // this client omitted the opt-in flag; fail before returning messages.
        if (!$requested && $applied) {
            throw new ConflationPolicyMismatchException(
                "consumer group '{$group}' on '" . ($queue ?? (($namespace ?? '*') . '/' . ($task ?? '*')))
                . "' has conflation enabled, but this consumer requires conflation=false"
            );
        }

        if ($conflict) {
            self::warnOnce($queue, $group, $namespace, $task, $applied);
        }

        if (!$requested || $applied || $conflict || $paused) {
            return;
        }

        throw new ConflationUnsupportedException(self::UNSUPPORTED_MESSAGE);
    }

    /**
     * Drop the warned-pairs ledger. Exists for tests — "once per process" is
     * otherwise untestable in a single-process runner, where the first test to
     * warn would silence every test after it.
     */
    public static function reset(): void
    {
        self::$warned = [];
    }

    private static function warnOnce(
        ?string $queue,
        ?string $group,
        ?string $namespace,
        ?string $task,
        bool $applied,
    ): void {
        $target = $queue ?? (($namespace ?? '*') . '/' . ($task ?? '*'));
        $groupName = $group ?? '__QUEUE_MODE__';
        $key = $target . "\0" . $groupName;

        if (isset(self::$warned[$key])) {
            return;
        }
        self::$warned[$key] = true;

        $inForce = $applied ? 'on' : 'off';

        trigger_error(
            "queen: consumer declared a conflation policy that consumer group "
            . "'{$groupName}' on '{$target}' does not have — the stored group setting "
            . "wins and conflation is {$inForce} for this group. A group's delivery "
            . "policy is fixed at its first registration and is not re-negotiated by "
            . "a later consumer. This is reported once per queue and group.",
            E_USER_WARNING
        );
    }
}
