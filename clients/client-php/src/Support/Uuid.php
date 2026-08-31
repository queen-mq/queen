<?php

namespace Queen\Support;

use Ramsey\Uuid\Uuid as RamseyUuid;

class Uuid
{
    public static function v7(): string
    {
        return RamseyUuid::uuid7()->toString();
    }
}
