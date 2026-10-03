@php
    $severityTones = ['critical' => 'danger', 'warning' => 'warning', 'info' => 'info'];
    $limit = static fn (?int $value, string $unit = ''): string => match (true) {
        $value === null => '—',
        $value === 0 => 'no limit',
        default => number_format($value) . $unit,
    };
@endphp
<section id="advice" class="card" aria-labelledby="advice-title">
    <div class="card-header">
        <div>
            <h2 id="advice-title">Tuning advice</h2>
            <p>Read from these settings, the running supervisor, the failed jobs and the last hour of jobs.</p>
        </div>
        <span class="header-meta">{{ count($advice) }} {{ count($advice) === 1 ? 'item' : 'items' }}</span>
    </div>
    @if ($advice === [])
        <div class="empty">No advice: nothing in these settings, the running supervisor or the last hour of jobs calls for a change.</div>
    @else
        <ol class="advice-list">
            @foreach ($advice as $item)
                <li class="advice advice-{{ $item['severity'] }}">
                    <div class="advice-heading">
                        <span class="badge {{ $severityTones[$item['severity']] }}">{{ ucfirst($item['severity']) }}</span>
                        <h3>{{ $item['title'] }}</h3>
                    </div>
                    <p class="advice-evidence">{{ $item['evidence'] }}</p>
                    <p class="advice-action"><strong>What to do:</strong> {{ $item['action'] }}</p>
                    <p class="advice-links">
                        @if ($item['page'] !== null && isset($sectionUrls[$item['page']]))
                            <a class="table-link" href="{{ $sectionUrls[$item['page']] }}">Open {{ \Queen\Laravel\Dashboard\DashboardSections::title($item['page']) }}</a>
                        @endif
                        <a class="table-link" href="{{ $item['doc'] }}" target="_blank" rel="noopener noreferrer">Read more<span class="sr-only"> about this in the Queen documentation (new tab)</span></a>
                    </p>
                </li>
            @endforeach
        </ol>
    @endif
</section>
<section id="configuration" class="card" aria-labelledby="configuration-title">
    <div class="card-header">
        <div>
            <h2 id="configuration-title">Settings</h2>
            <p>Every Queen setting of this application as this host resolves it. A supervisor started with other environment variables runs with its own values.</p>
        </div>
        <span class="header-meta">Credentials are never shown</span>
    </div>
    @foreach (['connection' => 'Connection', 'supervisor' => 'Supervisor'] as $group => $label)
        <h3 class="subsection-title">{{ $label }}</h3>
        <div class="table-wrap" role="region" aria-label="{{ $label }} settings" tabindex="0">
            <table class="settings-table">
                <thead><tr><th scope="col">Setting</th><th scope="col">Value</th><th scope="col">Default</th><th scope="col">Meaning</th></tr></thead>
                <tbody>
                @foreach ($settings[$group] as $row)
                    <tr>
                        <td><code>{{ $row['name'] }}</code> @if ($row['env'] !== null)<span class="queue-group technical">{{ $row['env'] }}</span>@endif</td>
                        <td>
                            @if ($row['invalid'])
                                <span class="badge warning">{{ $row['value'] }}</span>
                            @elseif ($row['changed'])
                                <strong>{{ $row['value'] }}</strong>
                            @else
                                {{ $row['value'] }}
                            @endif
                        </td>
                        <td>{{ $row['default'] }}</td>
                        <td>{{ $row['meaning'] }}</td>
                    </tr>
                @endforeach
                </tbody>
            </table>
        </div>
    @endforeach
    <h3 class="subsection-title">Pools</h3>
    <p class="settings-note">
        @if ($pools['source'] === 'published')
            As the running supervisor published them. Supervisors do not publish backoff, max jobs, max time and sleep: those come from this application's pool of the same name.
        @else
            No running supervisor has published its pools, so these are this application's.
        @endif
    </p>
    @if ($pools['pools'] === [])
        <div class="empty">No configured pools.</div>
    @else
        <div class="table-wrap" role="region" aria-label="Worker pools" tabindex="0">
            <table>
                <thead><tr><th scope="col">Name</th><th scope="col">Connection / group</th><th scope="col">Queues</th><th scope="col">Balance</th><th scope="col">Processes</th><th scope="col">Runtime limits</th><th scope="col">Worker loop</th></tr></thead>
                <tbody>
                @foreach ($pools['pools'] as $configured)
                    <tr>
                        <td><strong>{{ $configured['name'] }}</strong></td>
                        <td>{{ $configured['connection'] ?? '—' }} / <span class="technical">{{ $configured['consumer_group'] ?? '—' }}</span></td>
                        <td>{{ implode(', ', $configured['queues']) }}</td>
                        <td>{{ $configured['balance'] ?? '—' }} / {{ $configured['strategy'] ?? '—' }}</td>
                        <td>
                            @if (($configured['refused'] ?? null) !== null)
                                <span class="badge warning">{{ $configured['refused'] }}</span>
                            @else
                                {{ ($configured['balance'] ?? null) === 'simple' ? ($configured['processes'] ?? '—') : (($configured['min_processes'] ?? '—') . '–' . ($configured['max_processes'] ?? '—')) }}
                            @endif
                        </td>
                        <td>timeout {{ $configured['timeout'] ?? '—' }}s · retry {{ $configured['retry_after'] ?? '—' }}s · tries {{ $configured['tries'] ?? '—' }} · memory {{ $configured['memory'] ?? '—' }} MB</td>
                        <td>backoff {{ ($configured['backoff'] ?? null) === null ? '—' : $configured['backoff'] . ' s' }} · max jobs {{ $limit($configured['max_jobs'] ?? null) }} · max time {{ $limit($configured['max_time'] ?? null, ' s') }} · sleep {{ ($configured['sleep'] ?? null) === null ? '—' : $configured['sleep'] . ' s' }}</td>
                    </tr>
                @endforeach
                </tbody>
            </table>
        </div>
    @endif
    <dl class="settings-legend">
        <div><dt>Balance</dt><dd>auto follows the backlog, simple keeps a fixed count, off gives every worker the queues in order. Strategy size reads the depth; time also weighs the runtime. Default auto / size.</dd></div>
        <div><dt>Processes</dt><dd>The fixed count of a simple pool, otherwise the minimum and maximum. Default 1–10.</dd></div>
        <div><dt>Timeout</dt><dd>How long one job may run before its worker is stopped. Default 60 s.</dd></div>
        <div><dt>Retry</dt><dd>retry_after: how long a lease lasts. Must be longer than the timeout. Default 90 s.</dd></div>
        <div><dt>Tries</dt><dd>Attempts before a job fails for good. Default 3.</dd></div>
        <div><dt>Memory</dt><dd>Megabytes a worker may use before it restarts. Default 128.</dd></div>
        <div><dt>Backoff</dt><dd>How long a failed attempt waits before the next one. Default 0 s.</dd></div>
        <div><dt>Max jobs</dt><dd>Jobs a worker runs before it restarts. Default no limit.</dd></div>
        <div><dt>Max time</dt><dd>How long a worker runs before it restarts. Default no limit.</dd></div>
        <div><dt>Sleep</dt><dd>How long an idle worker waits before it asks for a job again. Default 1 s.</dd></div>
    </dl>
</section>
