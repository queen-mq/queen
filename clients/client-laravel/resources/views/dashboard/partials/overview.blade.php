<section id="overview" class="card" aria-labelledby="overview-title">
    <div class="card-header">
        <h2 id="overview-title">Overview</h2>
        <span class="header-meta">Generated <time datetime="{{ $snapshot['generated_at'] }}">{{ $snapshot['generated_at'] }}</time></span>
    </div>
    <dl class="metrics">
        <div class="metric">
            <dt>Worker processes</dt>
            <dd>{{ number_format($supervisor['workers']) }}</dd>
            <small>Running across all pools</small>
        </div>
        <div class="metric">
            <dt>Queued jobs</dt>
            <dd>{{ $depthLabel }}</dd>
            <small>{{ $unknownDepths > 0 ? $unknownDepths . ' queue depth' . ($unknownDepths === 1 ? '' : 's') . ' unavailable' : 'Latest sampled depth' }}</small>
        </div>
        <div class="metric">
            <dt>Failed jobs</dt>
            <dd>{{ $failedLabel }}</dd>
            <small>{{ ($failedJobs['available'] ?? false) ? 'Laravel failed-job store' : 'Backend unavailable' }}</small>
        </div>
        <div class="metric">
            <dt>Liveness</dt>
            <dd>
                <span class="operational-state tone-{{ $livenessTone }}">
                    <span class="state-mark" aria-hidden="true"></span>{{ $livenessLabel }}
                </span>
            </dd>
            <small>Heartbeat and owner lock</small>
        </div>
        <div class="metric">
            <dt>Readiness</dt>
            <dd>
                <span class="operational-state tone-{{ $readinessTone }}">
                    <span class="state-mark" aria-hidden="true"></span>{{ $readinessLabel }}
                </span>
            </dd>
            <small>At least one safe worker per active pool</small>
        </div>
        <div class="metric">
            <dt>Desired capacity</dt>
            <dd><span class="badge {{ $capacityTone }}">{{ $capacityLabel }}</span></dd>
            <small>{{ $supervisor['processing_healthy'] ? 'Processing healthy' : 'Processing health degraded' }}</small>
        </div>
        <div class="metric">
            <dt>Process budget</dt>
            <dd>{{ $budgetLabel }}</dd>
            <small>
                @if ($processBudget['valid'])
                    {{ number_format($processBudget['available']) }} available
                @else
                    Report unavailable
                @endif
            </small>
        </div>
        <div class="metric">
            <dt>Renewal helpers</dt>
            <dd>{{ $processBudget['valid'] ? number_format($processBudget['renewal_helpers_reserved']) : '—' }}</dd>
            <small>Reserved inside the process budget</small>
        </div>
        <div class="metric">
            <dt>Engine</dt>
            <dd>{{ $supervisor['engine'] ?? '—' }}</dd>
            <small>Local master process</small>
        </div>
        <div class="metric">
            <dt>Worker pools</dt>
            <dd>{{ number_format($poolCount) }}</dd>
            <small>Configured queue allocations</small>
        </div>
        <div class="metric">
            <dt>Draining</dt>
            <dd>{{ number_format($supervisor['draining']) }}</dd>
            <small>Processes exiting gracefully</small>
        </div>
        <div class="metric">
            <dt>Heartbeat</dt>
            <dd>{{ $supervisor['age_seconds'] === null ? '—' : $supervisor['age_seconds'] . 's' }}</dd>
            <small>Age of local state</small>
        </div>
    </dl>
</section>
