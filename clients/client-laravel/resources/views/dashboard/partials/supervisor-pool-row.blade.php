@php
    $visiblePids = array_slice($pool['pids'], 0, 4);
    $hiddenPids = max(0, count($pool['pids']) - count($visiblePids));
    $restartTone = match ($pool['restart_state']) {
        'open' => 'danger',
        'backoff', 'probe' => 'warning',
        default => $pool['healthy'] ? 'success' : 'warning',
    };
    $restartLabel = $pool['healthy']
        ? 'Healthy'
        : ($pool['restart_state'] === 'closed' ? 'Unknown' : ucfirst($pool['restart_state']));
@endphp
<tr>
    <td><strong>{{ $pool['supervisor'] }}</strong></td>
    <td>{{ $pool['queue'] }}</td>
    <td class="number">{{ $pool['processes'] }} / {{ $pool['desired'] }}</td>
    <td>
        <span class="health-badges">
            <span class="badge {{ $pool['ready'] ? 'success' : 'warning' }}">{{ $pool['ready'] ? 'Ready' : 'Not ready' }}</span>
            <span class="badge {{ $pool['capacity_satisfied'] ? 'success' : 'warning' }}">{{ $pool['capacity_satisfied'] ? 'Full' : 'Scaling' }}</span>
        </span>
    </td>
    <td class="number budget-cell">
        @if ($pool['reserved_processes'] === null)
            —
        @else
            {{ $pool['reserved_processes'] }} / {{ $pool['renewal_helpers_reserved'] }}
        @endif
    </td>
    <td class="number">{{ $pool['draining'] }}</td>
    <td>
        <span class="badge {{ $restartTone }}">{{ $restartLabel }}</span>
        @if ($pool['restart_in_seconds'] !== null)
            <span class="header-meta">in {{ $pool['restart_in_seconds'] }}s</span>
        @endif
    </td>
    <td class="pid-list"><code>{{ $visiblePids === [] ? '—' : implode(', ', $visiblePids) }}@if($hiddenPids > 0) +{{ $hiddenPids }}@endif</code></td>
</tr>
