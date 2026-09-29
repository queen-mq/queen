<section id="failed-jobs" class="card" aria-labelledby="failed-jobs-title">
    <div class="card-header">
        <div>
            <h2 id="failed-jobs-title">Failed jobs</h2>
            <p>Laravel's configured failed-job store, {{ $failedJobs['limit'] }} per page, newest first.</p>
        </div>
        <span class="header-meta">@if (!($failedJobs['available'] ?? false))Backend unavailable@elseif ($failedJobs['cursor'] === null){{ $failedLabel }} total@else Older page @endif</span>
    </div>
    <p class="failed-help">Open a job to see why it failed; payloads are never displayed. Use Laravel's queue commands to retry, forget, flush or prune jobs.</p>
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
                    @php($detailUrl = \Queen\Laravel\Dashboard\DashboardSections::failedJobUrl($failed['id']))
                    <tr><td>@if ($detailUrl !== null)<a href="{{ $detailUrl }}"><code>{{ $failed['id'] }}</code></a>@else<code>{{ $failed['id'] }}</code>@endif</td><td>{{ $failed['connection'] ?? '—' }}</td><td>{{ $failed['queue'] ?? '—' }}</td><td><span class="badge muted">{{ $failed['lifecycle_policy'] }}</span></td><td class="technical">{{ $failed['failed_at'] ?? '—' }}</td></tr>
                @endforeach
                </tbody>
            </table>
        </div>
    @endif
    @if (($failedJobs['available'] ?? false) && ($failedJobs['cursor'] !== null || $failedJobs['next_cursor'] !== null))
        <nav class="pager" aria-label="Failed jobs pages">
            @if ($failedJobs['cursor'] !== null)
                <a class="button" href="{{ $sectionUrls['failed-jobs'] }}">Newest</a>
            @endif
            @if ($failedJobs['next_cursor'] !== null)
                <a class="button" href="{{ $sectionUrls['failed-jobs'] }}?cursor={{ $failedJobs['next_cursor'] }}" rel="next">Older</a>
            @endif
        </nav>
    @endif
</section>
