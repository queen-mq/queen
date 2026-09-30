@php
    $rangeLabels = ['1h' => 'Last hour', '6h' => 'Last 6 hours', '24h' => 'Last 24 hours'];
    $metrics = $jobMetrics;
    $runs = $metrics['totals']['processed'] + $metrics['totals']['failed'];
@endphp
<section id="jobs" class="card" aria-labelledby="jobs-title">
    <div class="card-header">
        <div>
            <h2 id="jobs-title">Jobs by class</h2>
            <p>{{ $rangeLabels[$metrics['range']] }}, from every worker of every host. Runtime covers processed and failed attempts.</p>
        </div>
        <nav class="range-nav" aria-label="Time range">
            @foreach (\Queen\Laravel\Dashboard\JobMetricsReader::RANGES as $range => $seconds)
                <a class="button" href="{{ $sectionUrls['jobs'] }}{{ $range === \Queen\Laravel\Dashboard\JobMetricsReader::DEFAULT_RANGE ? '' : '?range=' . $range }}" @if ($range === $metrics['range']) aria-current="page" @endif>{{ $range }}</a>
            @endforeach
        </nav>
    </div>
    @if (!$metrics['available'])
        <div class="empty"><span class="badge warning">Unavailable</span> The job metrics could not be read from the broker.</div>
    @elseif ($metrics['classes'] === [])
        <div class="empty">No job ran in this window, or job metrics are disabled (queen.job_metrics).</div>
    @else
        @if ($metrics['truncated'] ?? false)
            <div class="empty"><span class="badge warning">Partial</span> This window holds more records than one read takes, so the table covers only its oldest part. Choose a shorter window.</div>
        @endif
        <dl class="metrics">
            <div class="metric">
                <dt>Processed</dt>
                <dd>{{ number_format($metrics['totals']['processed']) }}</dd>
                <small>{{ $rangeLabels[$metrics['range']] }}</small>
            </div>
            <div class="metric">
                <dt>Failed attempts</dt>
                <dd>{{ number_format($metrics['totals']['failed']) }}</dd>
                <small>{{ $runs > 0 ? round(100 * $metrics['totals']['failed'] / $runs, 1) . '% of attempts' : '—' }}</small>
            </div>
            <div class="metric">
                <dt>Job classes</dt>
                <dd>{{ number_format(count($metrics['classes'])) }}</dd>
                <small>Seen in this window</small>
            </div>
        </dl>
        <div class="table-wrap" role="region" aria-label="Jobs by class" tabindex="0">
            <table>
                <thead><tr><th scope="col">Job</th><th scope="col" class="number">Processed</th><th scope="col" class="number">Failed</th><th scope="col" class="number">Per minute</th><th scope="col" class="number">Average runtime</th></tr></thead>
                <tbody>
                @foreach ($metrics['classes'] as $row)
                    <tr>
                        <td><code>{{ $row['class'] }}</code></td>
                        <td class="number">{{ number_format($row['processed']) }}</td>
                        <td class="number">@if ($row['failed'] > 0)<span class="badge danger">{{ number_format($row['failed']) }}</span>@else 0 @endif</td>
                        <td class="number">{{ $row['per_minute'] }}</td>
                        <td class="number">{{ $row['average_ms'] === null ? '—' : number_format($row['average_ms']) . ' ms' }}</td>
                    </tr>
                @endforeach
                </tbody>
            </table>
        </div>
    @endif
</section>
