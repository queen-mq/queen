<section id="failed-jobs" class="card" aria-labelledby="failed-jobs-title">
    <div class="card-header">
        <div>
            <h2 id="failed-jobs-title">Failed jobs</h2>
            <p>Laravel's configured failed-job store, {{ $failedJobs['limit'] }} per page, newest first.</p>
        </div>
        <span class="header-meta">@if (!($failedJobs['available'] ?? false))Backend unavailable@elseif ($failedJobs['cursor'] === null){{ $failedLabel }} total@else Older page @endif</span>
    </div>
    <p class="failed-help">Select a job to see why it failed; payloads are never displayed. Use Laravel's queue commands to retry, forget, flush or prune jobs.</p>
    @if (!($failedJobs['available'] ?? false))
        <div class="empty"><span class="badge warning">Unavailable</span> Failed-job metadata could not be read safely.</div>
    @elseif ($failedJobs['items'] === [])
        <div class="empty">{{ $failedJobs['cursor'] === null ? 'No failed jobs.' : 'No older failed jobs.' }}</div>
    @else
        <div class="table-wrap" role="region" aria-label="Failed jobs table" tabindex="0">
            <table class="failed-table">
                <thead><tr><th scope="col">ID</th><th scope="col">Connection</th><th scope="col">Queue</th><th scope="col">Index policy</th><th scope="col">Failed at</th><th scope="col" class="row-action"><span class="sr-only">Details</span></th></tr></thead>
                <tbody>
                @foreach ($failedJobs['items'] as $failed)
                    @php($detailUrl = \Queen\Laravel\Dashboard\DashboardSections::failedJobUrl($failed['id']))
                    <tr @if ($detailUrl !== null) class="has-detail" @endif><td><code>{{ $failed['id'] }}</code></td><td>{{ $failed['connection'] ?? '—' }}</td><td>{{ $failed['queue'] ?? '—' }}</td><td><span class="badge muted">{{ $failed['lifecycle_policy'] }}</span></td><td class="technical">{{ $failed['failed_at'] ?? '—' }}</td><td class="row-action">@if ($detailUrl !== null)<a class="row-link" href="{{ $detailUrl }}" data-drawer aria-label="Why job {{ $failed['id'] }} failed"><svg class="chevron" viewBox="0 0 20 20" aria-hidden="true" focusable="false"><path d="M7.5 4.5 13 10l-5.5 5.5"></path></svg></a>@endif</td></tr>
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
