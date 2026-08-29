@if ($controlStatus)
    <div class="notice" role="status">{{ $controlStatus }}</div>
@endif
@if ($controlError)
    <div class="notice error" role="alert">{{ $controlError }}</div>
@endif
@if ($supervisor['availability'] !== 'live')
    <div class="availability-notice" role="status">
        <span class="state-mark" aria-hidden="true"></span>
        <div>
            <strong>Supervisor {{ strtolower($stateLabel) }}</strong>
            <span>
                @if ($supervisor['availability'] === 'stale')
                    The last published generation is no longer live. Controls remain disabled until a current heartbeat is available.
                @else
                    Start a Queen supervisor to publish local worker state and enable controls.
                @endif
            </span>
        </div>
    </div>
@endif
