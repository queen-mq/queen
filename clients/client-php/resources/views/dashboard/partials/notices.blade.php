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
@elseif (!$supervisor['ready'])
    <div class="availability-notice" role="status">
        <span class="state-mark" aria-hidden="true"></span>
        <div>
            <strong>Supervisor live, but not ready</strong>
            <span>The master heartbeat is current, but one or more worker pools cannot safely serve jobs yet.</span>
        </div>
    </div>
@elseif (!$supervisor['processing_healthy'])
    <div class="availability-notice" role="status">
        <span class="state-mark" aria-hidden="true"></span>
        <div>
            <strong>Processing health is degraded</strong>
            <span>Jobs can be processed, but desired capacity or a worker restart circuit has not recovered yet.</span>
        </div>
    </div>
@endif
