<?php

namespace Queen\Laravel\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Http\Response;
use Queen\Laravel\Dashboard\DashboardScript;

final class DashboardScriptController
{
    public function __invoke(
        Request $request,
        string $version,
        DashboardScript $script,
    ): Response {
        abort_unless(hash_equals($script->version(), $version), 404);

        $response = response($script->contents(), 200, [
            'Content-Type' => 'text/javascript; charset=UTF-8',
        ]);
        $response->setEtag($script->version());
        $response->isNotModified($request);

        return $response;
    }
}
