<?php

namespace Queen\Exceptions;

/**
 * The token callable configured as `bearerToken` gave no token: it threw
 * (its exception is getPrevious()), or it returned something that is not a
 * token, an empty string or one that cannot go in a header.
 *
 * Not an HttpException: nothing was sent. So no backend is to blame, and the
 * client neither retries the request nor fails it over: another attempt would
 * ask the same callable.
 */
class TokenSourceException extends \RuntimeException
{
}
