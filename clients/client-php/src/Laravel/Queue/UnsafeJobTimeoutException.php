<?php

namespace Queen\Laravel\Queue;

use LogicException;

/**
 * A job whose timeout its lease cannot cover. The job class or the
 * connection is wrong, and only a deploy can fix it: a supervised worker
 * leaves at its next loop (see WorkerPopGuard), and the delivery waits, to
 * lease expiry, for a worker that runs the fixed code.
 */
final class UnsafeJobTimeoutException extends LogicException
{
}
