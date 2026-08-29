@php
    $visiblePids = array_slice($pool['pids'], 0, 4);
    $hiddenPids = max(0, count($pool['pids']) - count($visiblePids));
    $restartTone = match ($pool['restart_state']) {
        'open' => 'danger',
        'backoff', 'probe' => 'warning',
        default => 'success',
    };
    $restartLabel = $pool['restart_state'] === 'closed' && $pool['restart_failures'] === 0
        ? 'Healthy'
        : ucfirst($pool['restart_state']);
@endphp
<tr>
    <td><strong>{{ $pool['supervisor'] }}</strong></td>
    <td>{{ $pool['queue'] }}</td>
    <td class="number">{{ $pool['processes'] }} / {{ $pool['desired'] }}</td>
    <td class="number">{{ $pool['draining'] }}</td>
    <td>
        <span class="badge {{ $restartTone }}">{{ $restartLabel }}</span>
        @if ($pool['restart_in_seconds'] !== null)
            <span class="header-meta">in {{ $pool['restart_in_seconds'] }}s</span>
        @endif
    </td>
    <td class="pid-list"><code>{{ $visiblePids === [] ? '—' : implode(', ', $visiblePids) }}@if($hiddenPids > 0) +{{ $hiddenPids }}@endif</code></td>
</tr>
