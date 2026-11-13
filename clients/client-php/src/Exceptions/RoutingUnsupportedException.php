<?php

namespace Queen\Exceptions;

/**
 * The client asked for routing (a filtered group's `where`, a reading queue's
 * `from`) and the broker did not apply it.
 *
 * A broker that does not route ignores the unknown parameter or option and
 * answers as it always did: a pop of the whole queue, a queue configured as an
 * ordinary one. Nothing in that answer looks wrong, so the only signal is a
 * positive one: a filtered group's pop echoes `"filtered":true`, on empty
 * answers too, and a reading queue's configure answer carries its `route`.
 * Their absence is raised on the first round trip, before a message is
 * handled; the fix is a broker upgrade, not a client setting.
 *
 * A type of its own, as ConflationUnsupportedException is: the consume loops
 * re-throw it by type, ahead of the branches that would otherwise retry it.
 */
class RoutingUnsupportedException extends \RuntimeException
{
    public const FILTERING_MESSAGE = 'this broker does not filter consumer groups; upgrade it';

    public const READING_MESSAGE = 'this broker does not read other queues; upgrade it';
}
