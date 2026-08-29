<section id="overview" class="card" aria-labelledby="overview-title">
    <div class="card-header">
        <h2 id="overview-title">Overview</h2>
        <span class="header-meta">Generated <time datetime="{{ $snapshot['generated_at'] }}">{{ $snapshot['generated_at'] }}</time></span>
    </div>
    <dl class="metrics">
        <div class="metric">
            <dt>Processes</dt>
            <dd>{{ number_format($supervisor['workers']) }}</dd>
            <small>Across all worker pools</small>
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
            <dt>Status</dt>
            <dd>
                <span class="operational-state tone-{{ $stateTone }}">
                    <span class="state-mark" aria-hidden="true"></span>{{ $stateLabel }}
                </span>
            </dd>
            <small>{{ $supervisor['availability'] }}</small>
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
