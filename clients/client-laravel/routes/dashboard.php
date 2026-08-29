<?php

use Illuminate\Support\Facades\Route;
use Queen\Laravel\Http\Controllers\DashboardController;
use Queen\Laravel\Http\Controllers\DashboardStatusController;
use Queen\Laravel\Http\Controllers\DashboardStylesheetController;
use Queen\Laravel\Http\Controllers\SupervisorControlController;

Route::get('/assets/dashboard-{version}.css', DashboardStylesheetController::class)
    ->where('version', '[a-f0-9]{64}')
    ->name('stylesheet');
Route::get('/', DashboardController::class)->name('index');
Route::get('/api/status', DashboardStatusController::class)->name('status');
Route::post('/control/{command}', SupervisorControlController::class)
    ->whereIn('command', ['pause', 'continue', 'terminate'])
    ->name('control');
