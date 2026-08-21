<?php

namespace Queen\Exceptions;

/**
 * The consumer asked for conflation and the broker did not apply it
 * (PLAN_CONFLATION §4).
 *
 * No SDK in this product negotiates capabilities, so a broker older than 1.1.0
 * simply ignores an unknown `conflation=true` query parameter and answers with
 * the whole backlog. Nothing about that response looks wrong: the consumer
 * processes every message it was built to skip, correctly and forever. The only
 * signal is a POSITIVE one — a conflating pop echoes `"conflation":true`, on
 * empty responses too — so its absence is a hard error raised on the FIRST
 * round trip, before a single message is handled.
 *
 * It is a distinct type, and not a message to string-match on, because branching
 * on prose is forbidden throughout this product: the consume loops re-throw it
 * by type, ahead of the timeout/network branches that would otherwise swallow
 * anything whose message happened to read the wrong way.
 */
class ConflationUnsupportedException extends \RuntimeException
{
}
