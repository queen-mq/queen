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
