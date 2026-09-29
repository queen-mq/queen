<?php

use Illuminate\Support\Facades\Route;
use Queen\Laravel\Dashboard\DashboardSections;
use Queen\Laravel\Http\Controllers\DashboardController;
use Queen\Laravel\Http\Controllers\DashboardScriptController;
use Queen\Laravel\Http\Controllers\DashboardStatusController;
use Queen\Laravel\Http\Controllers\DashboardStylesheetController;
use Queen\Laravel\Http\Controllers\FailedJobController;
use Queen\Laravel\Http\Controllers\SupervisorControlController;

Route::get('/assets/dashboard-{version}.css', DashboardStylesheetController::class)
    ->where('version', '[a-f0-9]{64}')
    ->name('stylesheet');
Route::get('/assets/dashboard-{version}.js', DashboardScriptController::class)
    ->where('version', '[a-f0-9]{64}')
    ->name('script');
Route::get('/', DashboardController::class)->defaults('section', 'overview')->name('index');
Route::get('/workload', DashboardController::class)->defaults('section', 'workload')->name('workload');
Route::get('/supervisors', DashboardController::class)->defaults('section', 'supervisors')->name('supervisors');
Route::get('/failed-jobs', DashboardController::class)->defaults('section', 'failed-jobs')->name('failed-jobs');
Route::get('/failed-jobs/{id}', FailedJobController::class)
    ->where('id', DashboardSections::FAILED_JOB_ID_PATTERN)
    ->name('failed-job');
Route::get('/configuration', DashboardController::class)->defaults('section', 'configuration')->name('configuration');
Route::get('/api/status', DashboardStatusController::class)->name('status');
Route::post('/control/{command}', SupervisorControlController::class)
    ->whereIn('command', ['pause', 'continue', 'terminate'])
    ->name('control');
