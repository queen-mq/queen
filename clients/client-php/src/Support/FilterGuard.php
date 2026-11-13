<?php

namespace Queen\Support;

use Queen\Exceptions\RoutingUnsupportedException;

/**
 * The client half of the filtered-group contract (PLAN_ROUTING.md §4): every
 * pop answer of a pop that sent a `where` passes through here, in the same
 * places as ConflationGuard (QueueBuilder::pop, the two ConsumerManager loops
 * and the two HighLevelConsumer methods).
 *
 * A broker that filters echoes `"filtered":true` on every answer of such a pop,
 * the empty ones included (a 200 with that body, never a bodiless 204). A
 * broker that does not ignores the `where` and hands out every message of the
 * queue; its answer is a 204 or has no echo, and that is raised before a
 * message is returned.
 */
final class FilterGuard
{
    /**
     * @param mixed $response  Decoded pop answer; null is what a bodiless 204
     *                         decodes to.
     * @param bool  $requested Whether this pop sent a `where`.
     */
    public static function check(mixed $response, bool $requested, ?string $queue = null, ?string $group = null): void
    {
        if (!$requested || (is_array($response) && ($response['filtered'] ?? false) === true)) {
            return;
        }

        throw new RoutingUnsupportedException(
            RoutingUnsupportedException::FILTERING_MESSAGE
            . " (group '" . ($group ?? '__QUEUE_MODE__') . "' on '" . ($queue ?? '*') . "' would get every message)"
        );
    }
}
