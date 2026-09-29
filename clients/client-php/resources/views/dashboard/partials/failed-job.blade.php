<section id="failed-job" class="card failed-job" aria-labelledby="failed-job-title">
    <div class="card-header">
        <div>
            <p class="failure-kicker"><span class="badge danger">Failed</span></p>
            <h2 id="failed-job-title" class="failure-title">{{ $failedJob['exception_class'] ?? 'Failure' }}</h2>
            <p>{{ $failedJob['job'] ?? 'Unknown job' }} · failed {{ $failedJob['failed_at'] ?? 'at an unknown time' }}</p>
        </div>
        <a class="button" href="{{ $sectionUrls['failed-jobs'] }}" data-page-only>All failed jobs</a>
    </div>
    <dl class="detail-list">
        <div><dt>ID</dt><dd><code>{{ $failedJob['id'] }}</code></dd></div>
        <div><dt>Connection</dt><dd>{{ $failedJob['connection'] ?? '—' }}</dd></div>
        <div><dt>Queue</dt><dd>{{ $failedJob['queue'] ?? '—' }}</dd></div>
        <div><dt>Max tries</dt><dd>{{ $failedJob['max_tries'] ?? '—' }}</dd></div>
        <div><dt>Timeout</dt><dd>{{ $failedJob['timeout'] !== null ? $failedJob['timeout'] . 's' : '—' }}</dd></div>
        <div><dt>Index policy</dt><dd><span class="badge muted">{{ $failedJob['lifecycle_policy'] }}</span></dd></div>
    </dl>
    <div class="detail-block failure-reason">
        <div class="detail-heading">
            <h3>Why it failed</h3>
            @if ($failedJob['exception_summary'] !== null)
                <button type="button" class="button copy-button" data-copy aria-label="Copy the error message">Copy</button>
            @endif
        </div>
        <pre class="exception-summary">{{ $failedJob['exception_summary'] ?? 'No exception was recorded.' }}</pre>
    </div>
    @if ($failedJob['exception_trace'] !== null)
        <details class="detail-block">
            <summary>Stack trace</summary>
            <div class="detail-heading detail-heading-end">
                <button type="button" class="button copy-button" data-copy aria-label="Copy the stack trace">Copy</button>
            </div>
            <pre class="exception-trace">{{ $failedJob['exception_trace'] }}</pre>
        </details>
    @endif
    <div class="detail-block">
        <div class="detail-heading">
            <h3>Retry</h3>
            <button type="button" class="button copy-button" data-copy aria-label="Copy the retry command">Copy</button>
        </div>
        <pre><code>php artisan queue:retry {{ $failedJob['id'] }}</code></pre>
    </div>
    <p class="sr-only" role="status" data-copy-status></p>
</section>
