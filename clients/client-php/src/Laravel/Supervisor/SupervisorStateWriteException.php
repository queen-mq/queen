<?php

namespace Queen\Laravel\Supervisor;

use RuntimeException;

/**
 * @internal A supervisor state file could not be written, on a full or
 *           failing disk: not a state directory that changed under the
 *           supervisor, which fails closed.
 */
final class SupervisorStateWriteException extends RuntimeException
{
}
