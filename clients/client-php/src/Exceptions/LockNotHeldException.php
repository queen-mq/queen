<?php

namespace Queen\Exceptions;

/**
 * Something needed a held lock and the handle holds none: a guard was asked of
 * it, or a transaction that asked for a guard was committed without the lock.
 *
 * Not an HttpException: nothing was sent. A step that asked for a guard must
 * never go out without one, so this is thrown BEFORE the request.
 *
 * A lock somebody else holds is not this — `Lock::acquire()` answers false for
 * it, and the wire answers `acquired: false` with HTTP 200.
 */
class LockNotHeldException extends \RuntimeException
{
}
