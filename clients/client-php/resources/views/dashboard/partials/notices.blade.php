@if ($controlStatus)
    <div class="notice" role="status">{{ $controlStatus }}</div>
@endif
@if ($controlError)
    <div class="notice error" role="alert">{{ $controlError }}</div>
@endif
@if ($snapshot['shared_queues'] !== [])
    @php($sharedQueue = $snapshot['shared_queues'][0])
    @php($otherSharedQueues = count($snapshot['shared_queues']) - 1)
    <div class="availability-notice" role="status">
        <span class="state-mark" aria-hidden="true"></span>
        <div>
            <strong>{{ $sharedQueue['instances'] }} running supervisors share queue {{ $sharedQueue['queue'] }} of consumer group {{ $sharedQueue['consumer_group'] }}</strong>
            <span>
                Each master sizes its workers from the whole backlog, so together they can exceed max_processes. Enable QUEEN_SUPERVISOR_COORDINATION on every replica so they share one target.
                @if ($otherSharedQueues > 0) {{ $otherSharedQueues }} other {{ $otherSharedQueues === 1 ? 'queue is' : 'queues are' }} shared too. @endif
            </span>
        </div>
    </div>
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
