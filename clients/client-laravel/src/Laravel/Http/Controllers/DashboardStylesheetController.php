<?php

namespace Queen\Laravel\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Http\Response;
use Queen\Laravel\Dashboard\DashboardStylesheet;

final class DashboardStylesheetController
{
    public function __invoke(
        Request $request,
        string $version,
        DashboardStylesheet $stylesheet,
    ): Response {
        abort_unless(hash_equals($stylesheet->version(), $version), 404);

        $response = response($stylesheet->contents(), 200, [
            'Content-Type' => 'text/css; charset=UTF-8',
        ]);
        $response->setEtag($stylesheet->version());
        $response->isNotModified($request);

        return $response;
    }
}
