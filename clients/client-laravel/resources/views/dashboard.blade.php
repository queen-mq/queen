<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="refresh" content="{{ $refreshSeconds }};url={{ $refreshUrl }}">
    <title>Queen supervisor</title>
    <style nonce="{{ $cspNonce }}">
        :root { color-scheme: dark; --bg:#090d15; --panel:#111827; --line:#263248; --muted:#93a4bd; --ink:#f6f7fb; --purple:#9b87f5; --green:#4ade80; --amber:#fbbf24; --red:#fb7185; }
        * { box-sizing:border-box; }
        body { margin:0; background:radial-gradient(circle at top right,#1a1540 0,transparent 32rem),var(--bg); color:var(--ink); font:14px/1.5 ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
        main { width:min(1180px,calc(100% - 32px)); margin:0 auto; padding:32px 0 64px; }
        header { display:flex; align-items:flex-end; justify-content:space-between; gap:16px; margin-bottom:24px; }
        h1 { margin:0; font-size:30px; letter-spacing:-.04em; }
        h2 { margin:0 0 16px; font-size:16px; }
        .brand { color:var(--purple); font-weight:800; letter-spacing:.08em; text-transform:uppercase; font-size:12px; }
        .muted { color:var(--muted); }
        .grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin-bottom:20px; }
        .card,.panel { background:color-mix(in srgb,var(--panel) 94%,transparent); border:1px solid var(--line); border-radius:12px; box-shadow:0 16px 40px rgba(0,0,0,.18); }
        .card { padding:17px; }
        .card strong { display:block; font-size:24px; margin-top:5px; }
        .panel { padding:20px; margin-top:16px; overflow:auto; }
        .badge { display:inline-flex; align-items:center; gap:7px; border:1px solid var(--line); border-radius:99px; padding:5px 9px; font-weight:700; text-transform:uppercase; font-size:11px; letter-spacing:.07em; }
        .badge:before { content:""; width:7px; height:7px; border-radius:50%; background:var(--muted); }
        .live:before { background:var(--green); box-shadow:0 0 10px var(--green); }
        .stale:before { background:var(--amber); }
        .unavailable:before { background:var(--red); }
        table { width:100%; border-collapse:collapse; min-width:680px; }
        th { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.08em; text-align:left; }
        th,td { padding:11px 10px; border-bottom:1px solid var(--line); vertical-align:top; }
        tbody tr:last-child td { border-bottom:0; }
        code { color:#c4b5fd; font:12px/1.5 ui-monospace,SFMono-Regular,Consolas,monospace; overflow-wrap:anywhere; }
        .controls { display:flex; gap:8px; flex-wrap:wrap; }
        form { margin:0; }
        button { border:1px solid #5646a9; background:#302665; color:white; border-radius:8px; padding:8px 12px; font-weight:700; cursor:pointer; }
        button.danger { border-color:#733244; background:#4a1f2c; }
        button:disabled { opacity:.4; cursor:not-allowed; }
        .notice { border:1px solid #316a49; background:#10271a; border-radius:9px; padding:11px 14px; margin-bottom:14px; }
        .notice.error { border-color:#7f3349; background:#31131c; }
        .split { display:flex; align-items:center; justify-content:space-between; gap:20px; }
        .empty { color:var(--muted); padding:10px 0; }
        @media (max-width:800px) { .grid { grid-template-columns:repeat(2,minmax(0,1fr)); } header,.split { align-items:flex-start; flex-direction:column; } }
    </style>
</head>
<body>
@php($supervisor = $snapshot['supervisor'])
<main>
    <header>
        <div>
            <div class="brand">Queen MQ</div>
            <h1>Laravel supervisor</h1>
            <div class="muted">Local control plane · generated {{ $snapshot['generated_at'] }}</div>
        </div>
        <span class="badge {{ $supervisor['availability'] }}">{{ $supervisor['availability'] }}</span>
    </header>

    @if ($controlStatus)
        <div class="notice" role="status">{{ $controlStatus }}</div>
    @endif
    @if ($controlError)
        <div class="notice error" role="alert">{{ $controlError }}</div>
    @endif

    <section class="grid" aria-label="Supervisor summary">
        <div class="card"><span class="muted">Engine</span><strong>{{ $supervisor['engine'] ?? '—' }}</strong></div>
        <div class="card"><span class="muted">State</span><strong>{{ $supervisor['state'] ?? '—' }}</strong></div>
        <div class="card"><span class="muted">Workers</span><strong>{{ $supervisor['workers'] }}</strong></div>
        <div class="card"><span class="muted">Draining</span><strong>{{ $supervisor['draining'] }}</strong></div>
    </section>

    <section class="panel">
        <div class="split">
            <div>
                <h2>Supervisor instance</h2>
                <div class="muted">PID {{ $supervisor['pid'] ?? '—' }} · heartbeat {{ $supervisor['updated_at'] ?? 'unavailable' }} · age {{ $supervisor['age_seconds'] ?? '—' }}s</div>
                <code>{{ $supervisor['instance_id'] ?? 'No active instance' }}</code>
            </div>
            <div class="controls" aria-label="Supervisor controls">
                @foreach (['pause' => false, 'continue' => false, 'terminate' => true] as $command => $danger)
                    <form method="post" action="{{ route('queen.dashboard.control', ['command' => $command], false) }}">
                        @csrf
                        <input type="hidden" name="instance_id" value="{{ $supervisor['instance_id'] }}">
                        <button type="submit" @class(['danger' => $danger]) @disabled($supervisor['availability'] !== 'live')>{{ ucfirst($command) }}</button>
                    </form>
                @endforeach
            </div>
        </div>
    </section>

    <section class="panel">
        <h2>Worker pools</h2>
        @if ($supervisor['pools'] === [])
            <div class="empty">No pool state is available.</div>
        @else
            <table>
                <thead><tr><th>Supervisor</th><th>Queue</th><th>Running / desired</th><th>Draining</th><th>PIDs</th><th>Restart</th></tr></thead>
                <tbody>
                @foreach ($supervisor['pools'] as $pool)
                    <tr>
                        <td>{{ $pool['supervisor'] }}</td><td><code>{{ $pool['queue'] }}</code></td>
                        <td>{{ $pool['processes'] }} / {{ $pool['desired'] }}</td><td>{{ $pool['draining'] }} @if($pool['draining_pids'] !== [])<code>({{ implode(', ', $pool['draining_pids']) }})</code>@endif</td>
                        <td><code>{{ $pool['pids'] === [] ? '—' : implode(', ', $pool['pids']) }}</code></td>
                        <td>{{ $pool['restart_state'] }} · {{ $pool['restart_failures'] }} failures @if($pool['restart_in_seconds'] !== null) · {{ $pool['restart_in_seconds'] }}s @endif</td>
                    </tr>
                @endforeach
                </tbody>
            </table>
        @endif
    </section>

    <section class="panel">
        <h2>Queue depths</h2>
        @if ($snapshot['queues'] === [])
            <div class="empty">No configured Queen queues.</div>
        @else
            <table>
                <thead><tr><th>Connection</th><th>Consumer group</th><th>Queue</th><th>Depth</th><th>Backend</th></tr></thead>
                <tbody>
                @foreach ($snapshot['queues'] as $queue)
                    <tr><td>{{ $queue['connection'] }}</td><td><code>{{ $queue['consumer_group'] }}</code></td><td><code>{{ $queue['queue'] }}</code></td><td>{{ $queue['depth'] ?? '—' }}</td><td>{{ $queue['available'] ? 'available' : 'unavailable' }}</td></tr>
                @endforeach
                </tbody>
            </table>
        @endif
    </section>

    <section class="panel">
        <h2>Resolved worker configuration</h2>
        @if ($snapshot['configuration']['supervisors'] === [])
            <div class="empty">No configured supervisors.</div>
        @else
            <table>
                <thead><tr><th>Name</th><th>Connection / group</th><th>Queues</th><th>Balance</th><th>Processes</th><th>Runtime limits</th></tr></thead>
                <tbody>
                @foreach ($snapshot['configuration']['supervisors'] as $configured)
                    <tr>
                        <td>{{ $configured['name'] }}</td>
                        <td>{{ $configured['connection'] }} / {{ $configured['consumer_group'] }}</td>
                        <td><code>{{ implode(', ', $configured['queues']) }}</code></td>
                        <td>{{ $configured['balance'] ?? '—' }} / {{ $configured['strategy'] ?? '—' }}</td>
                        <td>{{ $configured['processes'] ?? (($configured['min_processes'] ?? '—').'–'.($configured['max_processes'] ?? '—')) }}</td>
                        <td>timeout {{ $configured['timeout'] ?? '—' }}s · retry {{ $configured['retry_after'] ?? '—' }}s · tries {{ $configured['tries'] ?? '—' }} · memory {{ $configured['memory'] ?? '—' }} MB</td>
                    </tr>
                @endforeach
                </tbody>
            </table>
        @endif
    </section>

    <section class="panel">
        <div class="split"><h2>Failed jobs</h2><span class="muted">{{ $snapshot['failed_jobs']['available'] ? $snapshot['failed_jobs']['total'].($snapshot['failed_jobs']['total_exact'] ? '' : '+').' total' : 'backend unavailable' }}</span></div>
        <p class="muted">Use Laravel's <code>queue:retry</code>, <code>queue:forget</code>, flush and prune commands for mutations. The synchronized Queen provider updates the matching broker DLQ index; global DLQ inspection remains in the broker dashboard. The policy label below is inferred from configuration and is not a live assertion that a matching DLQ row exists.</p>
        @if ($snapshot['failed_jobs']['items'] === [])
            <div class="empty">{{ $snapshot['failed_jobs']['available'] ? 'No failed jobs.' : 'Failed-job metadata is temporarily unavailable.' }}</div>
        @else
            <table>
                <thead><tr><th>ID</th><th>Connection</th><th>Queue</th><th>Expected index policy</th><th>Failed at</th></tr></thead>
                <tbody>
                @foreach ($snapshot['failed_jobs']['items'] as $failed)
                    <tr><td><code>{{ $failed['id'] }}</code></td><td>{{ $failed['connection'] ?? '—' }}</td><td>{{ $failed['queue'] ?? '—' }}</td><td>{{ $failed['lifecycle_policy'] }}</td><td>{{ $failed['failed_at'] ?? '—' }}</td></tr>
                @endforeach
                </tbody>
            </table>
        @endif
    </section>
</main>
</body>
</html>
