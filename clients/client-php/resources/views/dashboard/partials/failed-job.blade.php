<section id="failed-job" class="card" aria-labelledby="failed-job-title">
    <div class="card-header">
        <div>
            <h2 id="failed-job-title">{{ $failedJob['exception_class'] ?? 'Failure' }}</h2>
            <p>{{ $failedJob['job'] ?? 'Unknown job' }} · failed {{ $failedJob['failed_at'] ?? 'at an unknown time' }}</p>
        </div>
        <a class="button" href="{{ $sectionUrls['failed-jobs'] }}">All failed jobs</a>
    </div>
    <dl class="detail-list">
        <div><dt>ID</dt><dd><code>{{ $failedJob['id'] }}</code></dd></div>
        <div><dt>Connection</dt><dd>{{ $failedJob['connection'] ?? '—' }}</dd></div>
        <div><dt>Queue</dt><dd>{{ $failedJob['queue'] ?? '—' }}</dd></div>
        <div><dt>Attempts</dt><dd>{{ $failedJob['attempts'] ?? '—' }}@if ($failedJob['max_tries'] !== null) of {{ $failedJob['max_tries'] }}@endif</dd></div>
        <div><dt>Timeout</dt><dd>{{ $failedJob['timeout'] !== null ? $failedJob['timeout'] . 's' : '—' }}</dd></div>
        <div><dt>Index policy</dt><dd><span class="badge muted">{{ $failedJob['lifecycle_policy'] }}</span></dd></div>
    </dl>
    <div class="detail-block">
        <h3>Why it failed</h3>
        <pre class="exception-summary">{{ $failedJob['exception_summary'] ?? 'No exception was recorded.' }}</pre>
    </div>
    @if ($failedJob['exception_trace'] !== null)
        <details class="detail-block">
            <summary>Stack trace</summary>
            <pre class="exception-trace">{{ $failedJob['exception_trace'] }}</pre>
        </details>
    @endif
    <div class="detail-block">
        <h3>Retry</h3>
        <pre><code>php artisan queue:retry {{ $failedJob['id'] }}</code></pre>
    </div>
</section>
