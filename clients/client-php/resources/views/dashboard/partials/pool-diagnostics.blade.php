@php
    $diagnostics = \Queen\Laravel\Dashboard\PoolDiagnostics::forInstance($instance);
    $diagnosticLive = $instance['availability'] === 'live' && $instance['state'] === 'running';
    $attentionCount = count(array_filter($diagnostics, static fn ($row) => $row['tone'] !== ''));
    $budget = $instance['process_budget'];
@endphp
<div class="pool-diagnostics">
    <div class="pool-diagnostics-heading">
        <div>
            <h3>Pool diagnostics</h3>
            <p>{{ $diagnosticLive ? 'Capacity and worker health · highest priority first' : 'Last reported values · current processing health is not confirmed' }}</p>
        </div>
        @if ($diagnosticLive && $diagnostics !== [])
            <span class="badge {{ $attentionCount ? 'warning' : '' }}">{{ $attentionCount }} {{ $attentionCount === 1 ? 'pool needs' : 'pools need' }} attention</span>
        @endif
    </div>
    <div class="pool-budget">
        <span><strong>Process budget</strong>
        @if ($budget['valid'])
            {{ $budget['used'] }} / {{ $budget['limit'] }} reserved · <strong>{{ $budget['available'] }} available</strong>
            <span class="pool-muted">including {{ $budget['draining_worker_processes'] }} draining workers and {{ $budget['renewal_helpers_reserved'] }} renewal helpers</span>
        @else
            <span class="pool-muted">Unavailable · capacity limits cannot be diagnosed</span>
        @endif
        </span>
        <a href="{{ $sectionUrls['configuration'] }}">Review configuration →</a>
    </div>
    @if ($diagnostics === [])
        <div class="empty">No pool state is available.</div>
    @else
    <div class="table-wrap" role="region" aria-label="Pool diagnostics{{ $instanceCount > 1 ? ' of instance ' . $instanceNumber : '' }}" tabindex="0">
        <table class="pool-diagnostic-table">
            <thead><tr><th scope="col">Pool / queue</th><th scope="col" class="number">Pending</th><th scope="col" class="number">Running / desired</th><th scope="col">Observation</th><th scope="col">Next check</th></tr></thead>
            <tbody>
            @foreach ($diagnostics as $row)
                <tr>
                    <td><strong>{{ $row['queue'] }}</strong><span class="pool-muted">{{ $row['supervisor'] }}</span></td>
                    <td class="number">{{ $row['depth_available'] ? number_format($row['depth']) : '—' }}@if (!$row['depth_available'])<span class="pool-muted">Unavailable</span>@endif</td>
                    <td class="number">
                        @if ($row['counts_available'])
                            {{ $row['processes'] }} / {{ $row['desired'] }}
                            @if ($row['processes'] < $row['desired'])<span class="pool-muted">{{ $row['desired'] - $row['processes'] }} below target</span>@endif
                        @else
                            —<span class="pool-muted">Not reported</span>
                        @endif
                        @if ($row['draining'] > 0)<span class="pool-muted">{{ $row['draining'] }} draining</span>@endif
                    </td>
                    <td>
                        <span class="badge {{ $row['tone'] }}">{{ $row['label'] }}</span>
                        @if ($row['restart_failures'] > 0)
                            <span class="pool-muted">
                                {{ $row['restart_failures'] }} restart failures
                                @if ($row['restart_in_seconds'] !== null)
                                    · retry in {{ $row['restart_in_seconds'] }}s
                                @endif
                            </span>
                        @endif
                    </td>
                    <td class="pool-next">{{ $row['next'] }}@if ($diagnosticLive && $row['label'] === 'At desired capacity' && $row['depth'] > 0) <a href="{{ $sectionUrls['workload'] }}">Open Workload →</a>@endif</td>
                </tr>
            @endforeach
            </tbody>
        </table>
    </div>
    @endif
    <p class="pool-diagnostic-note">Pending is a point-in-time count, not a delay measurement. Capacity observations use this instance’s reported worker target and shared process budget.</p>
</div>
