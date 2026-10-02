<header class="topbar">
    <a class="brand" href="{{ $refreshUrl }}" aria-label="Queen Supervisor dashboard">
        @include('queen::dashboard.partials.mark')
        <span>
            <span class="brand-name"><strong>Queen</strong> <span>Supervisor</span></span>
            <span class="brand-context">Laravel control plane</span>
        </span>
    </a>

    <div class="topbar-meta">
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
