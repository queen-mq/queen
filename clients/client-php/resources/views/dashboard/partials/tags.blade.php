<section id="tags" class="card" aria-labelledby="tags-title">
    <div class="card-header">
        <div>
            <h2 id="tags-title">Monitored tags</h2>
            <p>Jobs are tagged when pushed: their <code>tags()</code> method, or one <code>Model:key</code> tag per Eloquent model they carry.</p>
        </div>
        <form class="tag-form" method="post" action="{{ route('queen.dashboard.tags.monitor', [], false) }}">
            @csrf
            <label class="visually-hidden" for="tag-input">Tag to monitor</label>
            <input id="tag-input" name="tag" type="text" maxlength="128" required placeholder="App\Models\User:42">
            <button class="button primary" type="submit">Monitor</button>
        </form>
    </div>
    @if (!$tagMonitor['available'])
        <div class="empty"><span class="badge warning">Unavailable</span> The monitored tags could not be read from the broker.</div>
    @elseif ($tagMonitor['monitored'] === [])
        <div class="empty">No tag is monitored yet. Add one above; workers record matching jobs within 30 seconds.</div>
    @else
        <nav class="tag-list" aria-label="Monitored tags">
            @foreach ($tagMonitor['monitored'] as $tag)
                <a class="badge {{ $tag === $tagMonitor['selected'] ? 'success' : '' }}" href="{{ $sectionUrls['tags'] }}?tag={{ rawurlencode($tag) }}" @if ($tag === $tagMonitor['selected']) aria-current="page" @endif>{{ $tag }}</a>
            @endforeach
        </nav>
        <div class="card-header">
            <div>
                <h3 class="subsection-title">Recent jobs tagged <code>{{ $tagMonitor['selected'] }}</code></h3>
            </div>
            <form method="post" action="{{ route('queen.dashboard.tags.stop', [], false) }}">
                @csrf
                <input type="hidden" name="tag" value="{{ $tagMonitor['selected'] }}">
                <button class="button danger" type="submit">Stop monitoring</button>
            </form>
        </div>
        @if ($tagMonitor['jobs'] === [])
            <div class="empty">No job with this tag ran since it is monitored.</div>
        @else
            <div class="table-wrap" role="region" aria-label="Recent jobs with this tag" tabindex="0">
                <table>
                    <thead><tr><th scope="col">Job</th><th scope="col">Queue</th><th scope="col">Status</th><th scope="col" class="number">Attempts</th><th scope="col" class="number">Runtime</th><th scope="col">Finished</th></tr></thead>
                    <tbody>
                    @foreach ($tagMonitor['jobs'] as $job)
                        <tr>
                            <td><code>{{ $job['class'] }}</code></td>
                            <td>{{ $job['queue'] ?? '—' }}</td>
                            <td><span class="badge {{ $job['status'] === 'completed' ? 'success' : 'danger' }}">{{ ucfirst($job['status']) }}</span></td>
                            <td class="number">{{ $job['attempts'] ?? '—' }}</td>
                            <td class="number">{{ $job['runtime_ms'] === null ? '—' : number_format($job['runtime_ms']) . ' ms' }}</td>
                            <td>{{ $job['at'] ?? '—' }}</td>
                        </tr>
                    @endforeach
                    </tbody>
                </table>
            </div>
        @endif
    @endif
</section>
