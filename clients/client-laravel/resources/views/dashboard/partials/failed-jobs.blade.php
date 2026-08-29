<section id="failed-jobs" class="card" aria-labelledby="failed-jobs-title">
    <div class="card-header">
        <div>
            <h2 id="failed-jobs-title">Failed jobs</h2>
            <p>Bounded metadata from Laravel's configured failed-job store.</p>
        </div>
        <span class="header-meta">{{ ($failedJobs['available'] ?? false) ? $failedLabel . ' total' : 'Backend unavailable' }}</span>
    </div>
    <p class="failed-help">Use Laravel's queue commands to retry, forget, flush or prune jobs. This dashboard never displays payloads or exception bodies.</p>
    @if (!($failedJobs['available'] ?? false))
        <div class="empty"><span class="badge warning">Unavailable</span> Failed-job metadata could not be read safely.</div>
    @elseif ($failedJobs['items'] === [])
        <div class="empty">No failed jobs.</div>
    @else
        <div class="table-wrap" role="region" aria-label="Failed jobs table" tabindex="0">
            <table>
                <thead><tr><th scope="col">ID</th><th scope="col">Connection</th><th scope="col">Queue</th><th scope="col">Index policy</th><th scope="col">Failed at</th></tr></thead>
                <tbody>
                @foreach ($failedJobs['items'] as $failed)
                    <tr><td><code>{{ $failed['id'] }}</code></td><td>{{ $failed['connection'] ?? '—' }}</td><td>{{ $failed['queue'] ?? '—' }}</td><td><span class="badge muted">{{ $failed['lifecycle_policy'] }}</span></td><td class="technical">{{ $failed['failed_at'] ?? '—' }}</td></tr>
                @endforeach
                </tbody>
            </table>
        </div>
    @endif
</section>
