@php
    // Whole units, like the broker's lag: seconds, minutes, then hours and minutes.
    $contentsAge = static fn (int $seconds): string => match (true) {
        $seconds < 60 => $seconds . ' s',
        $seconds < 3600 => intdiv($seconds, 60) . ' min',
        default => intdiv($seconds, 3600) . ' h ' . sprintf('%02d', intdiv($seconds % 3600, 60)) . ' min',
    };
    $contentsTone = static fn (int $seconds): ?string => match (true) {
        $seconds >= 3600 => 'danger',
        $seconds >= 600 => 'warning',
        default => null,
    };
    $contentsRows = $queueContents['queues'];
    $contentsHidden = count($queues) - count($contentsRows);
@endphp
<section id="queue-contents" class="card" aria-labelledby="queue-contents-title">
    <div class="card-header">
        <div>
            <h2 id="queue-contents-title">In the queue now</h2>
            <p>Oldest unfinished is how long the oldest job not yet acknowledged has been in the queue, waiting or running; the broker reports it from one minute.</p>
        </div>
        <span class="header-meta">Read from the broker</span>
    </div>
    @if ($contentsRows === [])
        <div class="empty">No supervised queues.</div>
    @elseif (!$queueContents['available'])
        <div class="empty"><span class="badge warning">Unavailable</span> The broker could not be read for these queues.</div>
    @else
        <div class="table-wrap" role="region" aria-label="Jobs in each queue now" tabindex="0">
            <table>
                <thead><tr><th scope="col">Queue</th><th scope="col" class="number">Waiting</th><th scope="col" class="number">Running</th><th scope="col">Oldest unfinished</th>@if ($consoleLinks !== null)<th scope="col"><span class="sr-only">Queen console</span></th>@endif</tr></thead>
                <tbody>
                @foreach ($contentsRows as $row)
                    <tr>
                        <td><strong>{{ $row['queue'] }}</strong> <span class="queue-group technical">{{ $row['consumer_group'] }}</span></td>
                        @if (!$row['available'])
                            <td class="number">—</td>
                            <td class="number">—</td>
                            <td><span class="badge warning">Unavailable</span></td>
                        @else
                            @foreach (['waiting', 'running'] as $field)
                                <td class="number">
                                    @if ($row[$field] === null)
                                        —
                                    @elseif ($consoleLinks !== null)
                                        <a class="table-link" href="{{ $field === 'waiting' ? $consoleLinks->waiting($row['queue']) : $consoleLinks->running($row['queue']) }}" target="_blank" rel="noopener noreferrer" aria-label="{{ number_format($row[$field]) }} {{ $row[$field] === 1 ? 'job' : 'jobs' }} {{ $field }} in queue {{ $row['queue'] }}, in the Queen console (new tab)">{{ number_format($row[$field]) }}</a>
                                    @else
                                        {{ number_format($row[$field]) }}
                                    @endif
                                </td>
                            @endforeach
                            <td>
                                @if ($row['oldest_seconds'] !== null)
                                    @php($tone = $contentsTone($row['oldest_seconds']))
                                    @if ($tone !== null)
                                        <span class="badge {{ $tone }}">{{ $contentsAge($row['oldest_seconds']) }}</span>
                                    @else
                                        {{ $contentsAge($row['oldest_seconds']) }}
                                    @endif
                                {{-- Unknown when the lag could not be read; nothing to age in an empty queue. --}}
                                @elseif (!$row['oldest_available'] || ($row['waiting'] === 0 && $row['running'] === 0))
                                    —
                                @else
                                    under a minute
                                @endif
                            </td>
                        @endif
                        @if ($consoleLinks !== null)
                            <td><a class="table-link" href="{{ $consoleLinks->queue($row['queue']) }}" target="_blank" rel="noopener noreferrer" aria-label="Open queue {{ $row['queue'] }} in the Queen console (new tab)">Open in console</a></td>
                        @endif
                    </tr>
                @endforeach
                </tbody>
            </table>
        </div>
        @if ($contentsHidden > 0)
            <p class="card-note">The first {{ count($contentsRows) }} queues are shown; {{ $contentsHidden }} more are supervised.</p>
        @endif
        @if ($consoleLinksLeftOut)
            <p class="card-note">No console links: these queues use more than one connection, and one console address serves one broker.</p>
        @endif
        @if ($consoleLinksInvalid ?? false)
            <p class="card-note">No console links: QUEEN_DASHBOARD_CONSOLE_URL is not an http or https URL without user info, query or fragment.</p>
        @endif
    @endif
</section>
