<section id="configuration" class="card" aria-labelledby="configuration-title">
    <div class="card-header">
        <div>
            <h2 id="configuration-title">Resolved configuration</h2>
            <p>Safe worker settings published by the active supervisor generation.</p>
        </div>
        <span class="header-meta">Troubleshooting</span>
    </div>
    @if ($snapshot['configuration']['supervisors'] === [])
        <div class="empty">No configured supervisors.</div>
    @else
        <details class="configuration-disclosure">
            <summary>Show worker configuration</summary>
            <div class="table-wrap" role="region" aria-label="Resolved worker configuration table" tabindex="0">
                <table>
                    <thead><tr><th scope="col">Name</th><th scope="col">Connection / group</th><th scope="col">Queues</th><th scope="col">Balance</th><th scope="col">Processes</th><th scope="col">Runtime limits</th></tr></thead>
                    <tbody>
                    @foreach ($snapshot['configuration']['supervisors'] as $configured)
                        <tr>
                            <td><strong>{{ $configured['name'] }}</strong></td>
                            <td>{{ $configured['connection'] }} / <span class="technical">{{ $configured['consumer_group'] }}</span></td>
                            <td>{{ implode(', ', $configured['queues']) }}</td>
                            <td>{{ $configured['balance'] ?? '—' }} / {{ $configured['strategy'] ?? '—' }}</td>
                            <td>{{ $configured['processes'] ?? (($configured['min_processes'] ?? '—') . '–' . ($configured['max_processes'] ?? '—')) }}</td>
                            <td>timeout {{ $configured['timeout'] ?? '—' }}s · retry {{ $configured['retry_after'] ?? '—' }}s · tries {{ $configured['tries'] ?? '—' }} · memory {{ $configured['memory'] ?? '—' }} MB</td>
                        </tr>
                    @endforeach
                    </tbody>
                </table>
            </div>
        </details>
    @endif
</section>
