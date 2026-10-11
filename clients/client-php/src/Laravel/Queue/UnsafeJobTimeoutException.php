<?php

namespace Queen\Laravel\Queue;

use LogicException;

/**
 * A job whose timeout its lease cannot cover. The job class or the
 * connection is wrong, and only a deploy can fix it: the job fails at once,
 * with this exception, as Laravel fails a job (dead-letter queue, failed-job
 * row, failed()), and the worker goes on with the next one.
 */
final class UnsafeJobTimeoutException extends LogicException
{
}
