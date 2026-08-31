<?php

namespace Queen\Laravel\Http\Controllers;

use Illuminate\Http\JsonResponse;
use Queen\Laravel\Dashboard\DashboardRepository;

final class DashboardStatusController
{
    public function __invoke(DashboardRepository $dashboard): JsonResponse
    {
        return response()->json($dashboard->snapshot());
    }
}
