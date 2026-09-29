@php
    $rangeLabels = ['1h' => 'Last hour', '6h' => 'Last 6 hours', '24h' => 'Last 24 hours', '7d' => 'Last 7 days'];
    $bucketLabel = $throughput['bucket_minutes'] === 1 ? 'minute' : $throughput['bucket_minutes'] . ' minutes';
    $timeFormat = $throughput['range'] === '7d' ? 'D H:i' : 'H:i';
    $buckets = $throughput['buckets'];
    $bucketCount = max(1, count($buckets));
    $chartWidth = 720;
    $chartHeight = 140;
    $slot = $chartWidth / $bucketCount;
    $gap = $bucketCount <= 90 ? $slot * 0.2 : 0;
    $scale = $throughput['peak'] > 0 ? $chartHeight / $throughput['peak'] : 0;
    $failedQueues = count(array_filter($throughput['queues'], fn (array $row): bool => !$row['available']));
@endphp
<section id="throughput" class="card" aria-labelledby="throughput-title">
    <div class="card-header">
        <div>
            <h2 id="throughput-title">Jobs processed</h2>
            <p>Completed and failed jobs per {{ $bucketLabel }}, counted by the broker for these queues across every consumer group.</p>
        </div>
        <nav class="range-nav" aria-label="Time range">
            @foreach (\Queen\Laravel\Dashboard\ThroughputReader::RANGES as $range => $seconds)
                <a class="button" href="{{ $sectionUrls['workload'] }}{{ $range === \Queen\Laravel\Dashboard\ThroughputReader::DEFAULT_RANGE ? '' : '?range=' . $range }}" @if ($range === $throughput['range']) aria-current="page" @endif>{{ $range }}</a>
            @endforeach
        </nav>
    </div>
    @if (!$throughput['available'])
        <div class="empty"><span class="badge warning">Unavailable</span> The broker's queue counters could not be read.</div>
    @else
        <dl class="metrics">
            <div class="metric">
                <dt>Completed</dt>
                <dd>{{ number_format($throughput['totals']['completed']) }}</dd>
                <small>{{ $rangeLabels[$throughput['range']] }}</small>
            </div>
            <div class="metric">
                <dt>Failed</dt>
                <dd>{{ number_format($throughput['totals']['failed']) }}</dd>
                <small>Moved to the dead-letter queue</small>
            </div>
            <div class="metric">
                <dt>Dispatched</dt>
                <dd>{{ number_format($throughput['totals']['pushed']) }}</dd>
                <small>Jobs pushed to these queues</small>
            </div>
            <div class="metric">
                <dt>Peak</dt>
                <dd>{{ number_format($throughput['peak']) }}</dd>
                <small>Jobs in one {{ $bucketLabel }}</small>
            </div>
        </dl>
        <figure class="throughput-chart">
            <svg viewBox="0 0 {{ $chartWidth }} {{ $chartHeight }}" preserveAspectRatio="none" role="img" aria-labelledby="throughput-chart-title">
                <title id="throughput-chart-title">Completed and failed jobs per {{ $bucketLabel }}, {{ strtolower($rangeLabels[$throughput['range']]) }}</title>
                @foreach ($buckets as $index => $bucket)
                    @php
                        $x = $index * $slot + $gap / 2;
                        $completedHeight = $bucket['completed'] * $scale;
                        $failedHeight = $bucket['failed'] * $scale;
                    @endphp
                    <g>
                        <title>{{ gmdate($timeFormat, $bucket['start']) }} UTC: {{ number_format($bucket['completed']) }} completed, {{ number_format($bucket['failed']) }} failed, {{ number_format($bucket['pushed']) }} dispatched</title>
                        <rect class="slot" x="{{ round($index * $slot, 2) }}" y="0" width="{{ round($slot, 2) }}" height="{{ $chartHeight }}"></rect>
                        @if ($completedHeight > 0)
                            <rect class="completed" x="{{ round($x, 2) }}" y="{{ round($chartHeight - $completedHeight, 2) }}" width="{{ round($slot - $gap, 2) }}" height="{{ round($completedHeight, 2) }}"></rect>
                        @endif
                        @if ($failedHeight > 0)
                            <rect class="failed" x="{{ round($x, 2) }}" y="{{ round($chartHeight - $completedHeight - $failedHeight, 2) }}" width="{{ round($slot - $gap, 2) }}" height="{{ round($failedHeight, 2) }}"></rect>
                        @endif
                    </g>
                @endforeach
            </svg>
            <figcaption>
                <span>{{ gmdate($timeFormat, $buckets[0]['start'] ?? $throughput['from']) }} UTC</span>
                <span class="legend"><span class="swatch completed"></span>Completed <span class="swatch failed"></span>Failed</span>
                <span>{{ gmdate($timeFormat, $throughput['to']) }} UTC</span>
            </figcaption>
        </figure>
        @if ($failedQueues > 0)
            <p class="throughput-note"><span class="badge warning">Partial</span> Counters for {{ $failedQueues }} {{ $failedQueues === 1 ? 'queue' : 'queues' }} could not be read.</p>
        @endif
        <div class="table-wrap" role="region" aria-label="Jobs processed per queue" tabindex="0">
            <table>
                <thead><tr><th scope="col">Queue</th><th scope="col">Connection</th><th scope="col" class="number">Completed</th><th scope="col" class="number">Failed</th><th scope="col" class="number">Dispatched</th></tr></thead>
                <tbody>
                @foreach ($throughput['queues'] as $row)
                    <tr>
                        <td><strong>{{ $row['queue'] }}</strong></td>
                        <td>{{ $row['connection'] }}</td>
                        @if ($row['available'])
                            <td class="number">{{ number_format($row['completed']) }}</td>
                            <td class="number">{{ number_format($row['failed']) }}</td>
                            <td class="number">{{ number_format($row['pushed']) }}</td>
                        @else
                            <td class="number" colspan="3"><span class="badge warning">Unavailable</span></td>
                        @endif
                    </tr>
                @endforeach
                </tbody>
            </table>
        </div>
    @endif
</section>
<section id="workload" class="card" aria-labelledby="workload-title">
    <div class="card-header">
        <div>
            <h2 id="workload-title">Current workload</h2>
            <p>Depth is scoped by connection, consumer group and queue.</p>
        </div>
        <span class="header-meta">{{ count($queues) }} {{ count($queues) === 1 ? 'queue' : 'queues' }}</span>
    </div>
    @if ($queues === [])
        <div class="empty">No configured Queen queues.</div>
    @else
        <div class="table-wrap" role="region" aria-label="Current workload table" tabindex="0">
            <table>
                <thead><tr><th scope="col">Queue</th><th scope="col">Consumer group</th><th scope="col">Connection</th><th scope="col" class="number">Queued jobs</th><th scope="col">Backend</th></tr></thead>
                <tbody>
                @foreach ($queues as $queue)
                    <tr>
                        <td><strong>{{ $queue['queue'] }}</strong></td>
                        <td class="technical">{{ $queue['consumer_group'] }}</td>
                        <td>{{ $queue['connection'] }}</td>
                        <td class="number">{{ $queue['depth'] ?? '—' }}</td>
                        <td><span class="badge {{ $queue['available'] ? 'success' : 'warning' }}">{{ $queue['available'] ? 'Available' : 'Unavailable' }}</span></td>
                    </tr>
                @endforeach
                </tbody>
            </table>
        </div>
    @endif
</section>
