@php
    $instanceStateLabel = $stateLabelFor($instance);
    $instanceReadinessLabel = $instance['ready'] ? 'Ready' : 'Not ready';
    $instanceTitle = 'Supervisor instance' . ($instanceCount > 1 ? " {$instanceNumber} of {$instanceCount}" : '');
    $instanceTitleId = $instanceNumber === 1 ? 'supervisors-title' : 'supervisors-title-' . $instanceNumber;
@endphp
<section @if ($instanceNumber === 1) id="supervisors" @endif class="card" aria-labelledby="{{ $instanceTitleId }}">
    <div class="card-header">
        <div>
            <h2 id="{{ $instanceTitleId }}">{{ $instanceTitle }}@if ($instance['hostname'] ?? null) <span class="instance-host">· {{ $instance['hostname'] }}</span>@endif</h2>
            <p>{{ ucfirst($instance['engine'] ?? 'unknown') }} master · {{ $instanceStateLabel }} · {{ $instanceReadinessLabel }}@if (($instance['source'] ?? null) === 'remote') · published through the broker @endif</p>
        </div>
        @if (($instance['source'] ?? null) === 'remote')
        <p class="controls-note">Read-only: this supervisor runs on another host. Pause, continue or terminate it there with <code>php artisan queen:supervisor</code>.</p>
        @else
        <div class="controls" aria-label="Supervisor controls">
            <form method="post" action="{{ route('queen.dashboard.control', ['command' => 'pause'], false) }}">
                @csrf
                <input type="hidden" name="instance_id" value="{{ $instance['instance_id'] }}">
                <button class="button" type="submit" @disabled($instance['availability'] !== 'live' || $instance['state'] === 'paused')>Pause</button>
            </form>
            <form method="post" action="{{ route('queen.dashboard.control', ['command' => 'continue'], false) }}">
                @csrf
                <input type="hidden" name="instance_id" value="{{ $instance['instance_id'] }}">
                <button class="button primary" type="submit" @disabled($instance['availability'] !== 'live' || $instance['state'] !== 'paused')>Continue</button>
            </form>
            <form method="post" action="{{ route('queen.dashboard.control', ['command' => 'terminate'], false) }}">
                @csrf
                <input type="hidden" name="instance_id" value="{{ $instance['instance_id'] }}">
                <button class="button danger" type="submit" @disabled($instance['availability'] !== 'live')>Terminate</button>
            </form>
        </div>
        @endif
    </div>
    <div class="instance-meta">
        <div><span class="meta-label">Instance ID</span><code>{{ $instance['instance_id'] ?? 'No active instance' }}</code></div>
        <div><span class="meta-label">Master PID</span><span class="meta-value">{{ $instance['pid'] ?? '—' }}</span></div>
        <div><span class="meta-label">Last heartbeat</span><span class="meta-value">{{ $instance['updated_at'] ?? 'Unavailable' }}</span></div>
    </div>

    <h3 class="subsection-title">Worker pools</h3>
    @if ($instance['pools'] === [])
        <div class="empty">No pool state is available.</div>
    @else
        <div class="table-wrap" role="region" aria-label="Worker pools table{{ $instanceCount > 1 ? ' of instance ' . $instanceNumber : '' }}" tabindex="0">
            <table>
                <thead><tr><th scope="col">Supervisor</th><th scope="col">Queue</th><th scope="col" class="number">Running / desired</th><th scope="col">Readiness / capacity</th><th scope="col" class="number">Reserved / helpers</th><th scope="col" class="number">Draining</th><th scope="col">Restart</th><th scope="col">PIDs</th></tr></thead>
                <tbody>
                @foreach ($instance['pools'] as $pool)
                    @include('queen::dashboard.partials.supervisor-pool-row')
                @endforeach
                </tbody>
            </table>
        </div>
    @endif
</section>
