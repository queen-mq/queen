<section id="supervisors" class="card" aria-labelledby="supervisors-title">
    <div class="card-header">
        <div>
            <h2 id="supervisors-title">Supervisor instance</h2>
            <p>{{ ucfirst($supervisor['engine'] ?? 'unknown') }} master · {{ $stateLabel }} · {{ $readinessLabel }}@if (($supervisor['source'] ?? null) === 'remote') · published through the broker @endif</p>
        </div>
        @if (($supervisor['source'] ?? null) === 'remote')
        <p class="controls-note">Read-only: this supervisor runs on another host. Pause, continue or terminate it there with <code>php artisan queen:supervisor</code>.</p>
        @else
        <div class="controls" aria-label="Supervisor controls">
            <form method="post" action="{{ route('queen.dashboard.control', ['command' => 'pause'], false) }}">
                @csrf
                <input type="hidden" name="instance_id" value="{{ $supervisor['instance_id'] }}">
                <button class="button" type="submit" @disabled($supervisor['availability'] !== 'live' || $supervisor['state'] === 'paused')>Pause</button>
            </form>
            <form method="post" action="{{ route('queen.dashboard.control', ['command' => 'continue'], false) }}">
                @csrf
                <input type="hidden" name="instance_id" value="{{ $supervisor['instance_id'] }}">
                <button class="button primary" type="submit" @disabled($supervisor['availability'] !== 'live' || $supervisor['state'] !== 'paused')>Continue</button>
            </form>
            <form method="post" action="{{ route('queen.dashboard.control', ['command' => 'terminate'], false) }}">
                @csrf
                <input type="hidden" name="instance_id" value="{{ $supervisor['instance_id'] }}">
                <button class="button danger" type="submit" @disabled($supervisor['availability'] !== 'live')>Terminate</button>
            </form>
        </div>
        @endif
    </div>
    <div class="instance-meta">
        <div><span class="meta-label">Instance ID</span><code>{{ $supervisor['instance_id'] ?? 'No active instance' }}</code></div>
        <div><span class="meta-label">Master PID</span><span class="meta-value">{{ $supervisor['pid'] ?? '—' }}</span></div>
        <div><span class="meta-label">Last heartbeat</span><span class="meta-value">{{ $supervisor['updated_at'] ?? 'Unavailable' }}</span></div>
    </div>

    <h3 class="subsection-title">Worker pools</h3>
    @if ($supervisor['pools'] === [])
        <div class="empty">No pool state is available.</div>
    @else
        <div class="table-wrap" role="region" aria-label="Worker pools table" tabindex="0">
            <table>
                <thead><tr><th scope="col">Supervisor</th><th scope="col">Queue</th><th scope="col" class="number">Running / desired</th><th scope="col">Readiness / capacity</th><th scope="col" class="number">Reserved / helpers</th><th scope="col" class="number">Draining</th><th scope="col">Restart</th><th scope="col">PIDs</th></tr></thead>
                <tbody>
                @foreach ($supervisor['pools'] as $pool)
                    @include('queen::dashboard.partials.supervisor-pool-row')
                @endforeach
                </tbody>
            </table>
        </div>
    @endif
</section>
