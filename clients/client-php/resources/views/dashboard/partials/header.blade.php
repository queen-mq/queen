<header class="topbar">
    <a class="brand" href="{{ $sectionUrls['overview'] }}" aria-label="Queen Supervisor dashboard">
        @include('queen::dashboard.partials.mark')
        <span>
            <span class="brand-name"><strong>Queen</strong> <span>Supervisor</span></span>
            <span class="brand-context">Laravel control plane</span>
        </span>
    </a>

    <div class="topbar-meta">
        {{-- Enabled by dashboard.js; without scripts the <noscript> meta refresh applies. --}}
        <button type="button" class="refresh-toggle" data-refresh-toggle aria-pressed="false" hidden>Pause auto-refresh</button>
        <span>
            Updated
            @if ($supervisor['updated_at'])
                <time datetime="{{ $supervisor['updated_at'] }}">{{ $supervisor['age_seconds'] }}s ago</time>
            @else
                unavailable
            @endif
        </span>
        <span class="operational-state tone-{{ $livenessTone }}">
            <span class="state-mark" aria-hidden="true"></span>{{ $livenessLabel }}
        </span>
        <span class="operational-state tone-{{ $readinessTone }}">
            <span class="state-mark" aria-hidden="true"></span>{{ $readinessLabel }}
        </span>
    </div>
</header>
