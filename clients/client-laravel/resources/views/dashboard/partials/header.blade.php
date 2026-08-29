<header class="topbar">
    <a class="brand" href="{{ $refreshUrl }}" aria-label="Queen Supervisor dashboard">
        <svg class="brand-mark" viewBox="2.01 2.01 255.99 255.99" aria-hidden="true" focusable="false">
            <g fill="currentColor">
                <path d="M166.295 209.709A105 105 0 1 1 209.709 166.295l-34.434-18.31a66 66 0 1 0-27.29 27.29Z"/>
                <g transform="translate(43.134 43.134) translate(177.458 177.458) scale(.85) translate(-177.458 -177.458)">
                    <path d="M209.709 166.295a105 105 0 0 1-43.414 43.414l-18.31-34.434a66 66 0 0 0 27.29-27.29Z"/>
                </g>
            </g>
        </svg>
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
        <span class="operational-state tone-{{ $stateTone }}">
            <span class="state-mark" aria-hidden="true"></span>{{ $stateLabel }}
        </span>
    </div>
</header>
