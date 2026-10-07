<?php

use Illuminate\Foundation\Application;
use Illuminate\Foundation\Configuration\Exceptions;

// Console only: the examples are Artisan commands (app/Console/Commands, found
// by withCommands()) and the workers they start. withExceptions() binds
// Laravel's exception handler, which the queue worker reports through.
return Application::configure(basePath: dirname(__DIR__))
    ->withCommands()
    ->withExceptions(function (Exceptions $exceptions): void {
    })
    ->create();
