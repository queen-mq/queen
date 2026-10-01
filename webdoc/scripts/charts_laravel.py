"""The Laravel figures: Horizon against the Queen supervisors, and the
supervisor features. Rendered by `charts.py`."""

from __future__ import annotations

from pathlib import Path


from chartlib import plt, BENCH, Theme, finish, read_csv, save, style

LARAVEL = BENCH / "2026-09-30-laravel-supervisor-features" / "raw"


def fig_laravel_control_plane(out: Path, theme: Theme) -> str:
    """Orchestrator memory of Horizon and both Queen engines, read from the
    qualification summary's median table (the row is the claim; the chart only
    draws it)."""
    summary = BENCH / "laravel-supervisors" / "QUALIFICATION_SUMMARY_20260829.md"
    row = next(
        line for line in summary.read_text().splitlines() if line.startswith("| Orchestrator PSS")
    )
    values = [float(cell.split()[0]) for cell in row.strip("|").split("|")[1:]]
    engines = ["Horizon", "Queen PHP", "Queen Rust"]

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 2.1))
    bars = ax.barh(engines[::-1], values[::-1], color=[theme.series[0], theme.series[2], theme.series[1]][::-1], height=0.55)
    finish(ax, theme, "")
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Orchestrator PSS (MiB), median of 3 runs", color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, max(values) * 1.18)
    for bar, value in zip(bars, values[::-1]):
        ax.annotate(f"{value:g} MiB", xy=(bar.get_width(), bar.get_y() + bar.get_height() / 2),
                    xytext=(6, 0), textcoords="offset points", va="center", fontsize=8.5,
                    color=theme.ink_strong)

    save(fig, out, "laravel-control-plane", theme)
    return "laravel-control-plane"


def fig_laravel_prefork(out: Path, theme: Theme) -> str:
    """Worker memory, started one by one against forked from one booted
    Laravel: the median of three runs per case."""
    from statistics import median

    rows = read_csv(LARAVEL / "prefork-memory.csv")
    cases = [("4", "0", "4 idle workers"), ("8", "600", "8 workers, 600 jobs")]
    groups, spawned, forked = [], [], []
    for workers, jobs, label in cases:
        for opcache, flag in (("0", "opcache off"), ("1", "opcache on")):
            pick = lambda mode: median(
                float(r["pss_mib"]) for r in rows
                if r["workers"] == workers and r["jobs"] == jobs and r["opcache"] == opcache and r["mode"] == mode
            )
            groups.append(f"{label}\n{flag}")
            spawned.append(pick("spawned"))
            forked.append(pick("forked"))

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 3.2))
    x = range(len(groups))
    width = 0.36
    left = ax.bar([i - width / 2 for i in x], spawned, width, color=theme.series[1], label="Each worker boots Laravel")
    right = ax.bar([i + width / 2 for i in x], forked, width, color=theme.series[0], label="Forked from one boot (prefork)")
    finish(ax, theme, "Total PSS (MiB)")
    ax.set_xticks(list(x), groups)
    ax.set_ylim(0, max(spawned) * 1.22)
    ax.legend(loc="upper left", ncol=2)
    for bars in (left, right):
        for bar in bars:
            ax.annotate(f"{bar.get_height():.0f}", xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        xytext=(0, 3), textcoords="offset points", ha="center", fontsize=8,
                        color=theme.ink_strong)

    save(fig, out, "laravel-prefork-memory", theme)
    return "laravel-prefork-memory"


def fig_laravel_replicas(out: Path, theme: Theme) -> str:
    """Two supervisor pods on one queue: the workers they run together against
    the target one supervisor would size for the same backlog."""
    rows = read_csv(LARAVEL / "replicas.csv")

    style(theme)
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.9), sharey=True, gridspec_kw={"wspace": 0.12})
    for ax, (mode, title) in zip(axes, (("uncoordinated", "Without coordination"), ("coordinated", "With coordination"))):
        series = [r for r in rows if r["mode"] == mode and r["target"]]
        t = [int(r["t"]) for r in series]
        total = [int(r["workers_a"]) + int(r["workers_b"]) for r in series]
        target = [int(r["target"]) for r in series]
        ax.step(t, target, where="post", color=theme.ink, linestyle="--", linewidth=1.2, label="Fleet target")
        ax.plot(t, total, color=theme.series[1] if mode == "uncoordinated" else theme.series[0], label="Workers, both pods")
        finish(ax, theme, "Worker processes" if ax is axes[0] else "")
        ax.set_title(title, color=theme.ink_strong, fontsize=9, loc="left")
        ax.set_xlabel("Seconds", color=theme.ink, fontsize=8.5)
        ax.set_ylim(0, 18)
        ax.legend(loc="lower left")

    save(fig, out, "laravel-replicas", theme)
    return "laravel-replicas"


def fig_laravel_scale_up(out: Path, theme: Theme) -> str:
    """Time to reach twenty workers after a burst: one balance_max_shift step
    per cycle against fast_scale_up, which closes half the gap per cycle."""
    rows = read_csv(LARAVEL / "scale-up.csv")

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 2.8))
    for mode, color, label in (("fast", theme.series[0], "fast_scale_up"), ("step", theme.series[1], "One step per cycle")):
        series = [r for r in rows if r["mode"] == mode]
        t = [float(r["t"]) for r in series]
        workers = [int(r["workers"]) for r in series]
        # Both lines end at max_processes: mark when each got there instead.
        full = next(x for x, w in zip(t, workers) if w == max(workers))
        ax.plot(t, workers, color=color, label=f"{label}: {max(workers)} workers after {full:.1f} s")
        ax.plot([full], [max(workers)], marker="o", markersize=4, color=color)
    finish(ax, theme, "Worker processes")
    ax.set_xlabel("Seconds after the burst", color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, 25)
    ax.set_ylim(0, 22)
    ax.set_yticks([0, 5, 10, 15, 20])
    ax.legend(loc="lower right")

    save(fig, out, "laravel-fast-scale-up", theme)
    return "laravel-fast-scale-up"


def fig_laravel_event_driven(out: Path, theme: Theme) -> str:
    """Workers after a burst at the production cadence, polled against woken
    by the broker, every run drawn; the legend carries the median time to the
    full twenty."""
    rows = read_csv(LARAVEL / "event-driven.csv")

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 2.8))
    for mode, color, label in (("event", theme.series[0], "event_driven"), ("poll", theme.series[1], "Polling")):
        runs = []
        for run in sorted({r["run"] for r in rows if r["mode"] == mode}):
            series = [r for r in rows if r["mode"] == mode and r["run"] == run]
            runs.append(([float(r["t"]) for r in series], [int(r["workers"]) for r in series]))
        peak = max(max(workers) for _, workers in runs)
        full = sorted(next(x for x, w in zip(t, workers) if w == peak) for t, workers in runs)
        for index, (t, workers) in enumerate(runs):
            ax.step(
                t,
                workers,
                where="post",
                color=color,
                alpha=0.85,
                linewidth=1.2,
                label=f"{label}: {peak} workers after {full[len(full) // 2]:.1f} s, median of {len(runs)}"
                if index == 0
                else None,
            )
    finish(ax, theme, "Worker processes")
    ax.set_xlabel("Seconds after the burst started", color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 22)
    ax.set_yticks([0, 5, 10, 15, 20])
    ax.legend(loc="lower right")

    save(fig, out, "laravel-event-driven", theme)
    return "laravel-event-driven"


HORIZON_RAFT = BENCH / "2026-10-01-laravel-horizon-raft" / "raw" / "runs.csv"


def horizon_raft_median(rows, campaign: str, engine: str, field: str) -> float:
    """The median of one field over a lane's correct runs."""
    from statistics import median

    return median(
        float(r[field]) for r in rows
        if r["campaign"] == campaign and r["engine"] == engine and r["correct"] == "True"
    )


def fig_laravel_horizon_throughput(out: Path, theme: Theme) -> str:
    """Completed jobs per second, Horizon against Queen, per scenario: the
    median of five runs each."""
    rows = read_csv(HORIZON_RAFT)
    scenarios = [
        ("strict-all", "10 ms jobs, both fsync every write"),
        ("everysec-all", "10 ms jobs, Redis fsync once a second"),
        ("noop-all", "Empty jobs"),
        ("cpu", "CPU-bound jobs, about 20 ms"),
        ("burst-all", "Burst, autoscaling 1 to 16 workers"),
    ]
    labels = [label for _, label in scenarios][::-1]
    horizon = [horizon_raft_median(rows, c, "horizon", "jobs_per_second") for c, _ in scenarios][::-1]
    queen = [horizon_raft_median(rows, c, "queen-rust", "jobs_per_second") for c, _ in scenarios][::-1]

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    y = range(len(labels))
    height = 0.38
    bars_h = ax.barh([i + height / 2 for i in y], horizon, height, color=theme.series[1], label="Horizon, Redis")
    bars_q = ax.barh([i - height / 2 for i in y], queen, height, color=theme.series[0], label="Queen, Raft broker")
    finish(ax, theme, "")
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    ax.set_yticks(list(y), labels)
    ax.set_xlabel("Completed jobs per second, median of 5 runs", color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, max(queen + horizon) * 1.16)
    for bars in (bars_h, bars_q):
        for bar in bars:
            ax.annotate(f"{bar.get_width():.0f}", xy=(bar.get_width(), bar.get_y() + bar.get_height() / 2),
                        xytext=(4, 0), textcoords="offset points", va="center", fontsize=8,
                        color=theme.ink_strong)
    ax.legend(loc="lower right")

    save(fig, out, "laravel-horizon-throughput", theme)
    return "laravel-horizon-throughput"


def fig_laravel_horizon_memory(out: Path, theme: Theme) -> str:
    """Proportional set size of the orchestrator, the workers and the lease
    helpers with eight workers, stacked: Horizon, Queen with one helper per
    worker, and Queen renewing leases in the supervisor."""
    rows = read_csv(HORIZON_RAFT)
    stacks = [
        ("Horizon", "strict-all", "horizon"),
        ("Queen, a lease helper\nper worker", "ablation-none", "queen-rust"),
        ("Queen, leases renewed\nby the supervisor", "strict-all", "queen-rust"),
    ][::-1]
    parts = [
        ("orchestrator_pss_mib", "Orchestrator", theme.series[2]),
        ("workers_pss_mib", "Workers", theme.series[0]),
        ("renewers_pss_mib", "Lease helpers", theme.series[1]),
    ]

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 2.6))
    left = [0.0] * len(stacks)
    for field, label, color in parts:
        values = [horizon_raft_median(rows, campaign, engine, field) for _, campaign, engine in stacks]
        ax.barh([name for name, _, _ in stacks], values, left=left, height=0.55, color=color, label=label)
        left = [a + b for a, b in zip(left, values)]
    for index, total in enumerate(left):
        ax.annotate(f"{total:.0f} MiB", xy=(total, index), xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=8.5, color=theme.ink_strong)
    finish(ax, theme, "")
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("PSS with 8 workers (MiB), median of 5 runs", color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, max(left) * 1.18)
    ax.legend(loc="lower right", ncol=3)

    save(fig, out, "laravel-horizon-memory", theme)
    return "laravel-horizon-memory"


def fig_laravel_queen_optimizations(out: Path, theme: Theme) -> str:
    """What each worker-side change adds, one at a time, against Horizon at
    the same strict durability."""
    rows = read_csv(HORIZON_RAFT)
    steps = [
        ("Queen 0.5: helpers, synchronous ACK", "ablation-none"),
        ("Leases renewed by the supervisor", "ablation-lease"),
        ("Asynchronous ACK only", "ablation-ack"),
        ("Supervisor renewal and asynchronous ACK", "strict"),
        ("Both, and the next batch popped ahead", "strict-all"),
    ][::-1]
    values = [horizon_raft_median(rows, c, "queen-rust", "jobs_per_second") for _, c in steps]
    horizon = horizon_raft_median(rows, "strict-all", "horizon", "jobs_per_second")

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 2.9))
    bars = ax.barh([label for label, _ in steps], values, height=0.55, color=theme.series[0])
    ax.axvline(horizon, color=theme.series[1], linestyle="--", linewidth=1.3,
               label=f"Horizon at the same durability: {horizon:.0f}")
    ax.legend(loc="upper left", bbox_to_anchor=(0, -0.2))
    for bar in bars:
        ax.annotate(f"{bar.get_width():.0f}", xy=(bar.get_width(), bar.get_y() + bar.get_height() / 2),
                    xytext=(4, 0), textcoords="offset points", va="center", fontsize=8,
                    color=theme.ink_strong)
    finish(ax, theme, "")
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Completed jobs per second, 10 ms jobs, fsync every write, median of 5 runs",
                  color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, max(values + [horizon]) * 1.12)
    ax.set_xticks([0, 100, 200, 300, 400, 500, 600])

    save(fig, out, "laravel-queen-optimizations", theme)
    return "laravel-queen-optimizations"


def fig_laravel_horizon_latency(out: Path, theme: Theme) -> str:
    """Dispatch-to-completion latency at a steady arrival rate below capacity:
    p50 and p95 per engine and rate."""
    rows = read_csv(HORIZON_RAFT)
    rates = [("latency-100", "100 jobs/s"), ("latency-300", "300 jobs/s")]

    style(theme)
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.6), sharey=True, gridspec_kw={"wspace": 0.1})
    for ax, (campaign, title) in zip(axes, rates):
        groups = ["p50", "p95"]
        horizon = [horizon_raft_median(rows, campaign, "horizon", f"end_to_end_{p}_ms") for p in groups]
        queen = [horizon_raft_median(rows, campaign, "queen-rust", f"end_to_end_{p}_ms") for p in groups]
        x = range(len(groups))
        width = 0.36
        bars_h = ax.bar([i - width / 2 for i in x], horizon, width, color=theme.series[1], label="Horizon")
        bars_q = ax.bar([i + width / 2 for i in x], queen, width, color=theme.series[0], label="Queen")
        finish(ax, theme, "End-to-end latency (ms)" if ax is axes[0] else "")
        ax.set_title(title, color=theme.ink_strong, fontsize=9, loc="left")
        ax.set_xticks(list(x), groups)
        for bars in (bars_h, bars_q):
            for bar in bars:
                ax.annotate(f"{bar.get_height():.0f}", xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                            xytext=(0, 3), textcoords="offset points", ha="center", fontsize=8,
                            color=theme.ink_strong)
        ax.legend(loc="upper left")
    top = max(
        horizon_raft_median(rows, c, e, "end_to_end_p95_ms")
        for c, _ in rates for e in ("horizon", "queen-rust")
    )
    axes[0].set_ylim(0, top * 1.2)

    save(fig, out, "laravel-horizon-latency", theme)
    return "laravel-horizon-latency"


LINUX_VM = BENCH / "2026-10-01-linux-vm-horizon-raft" / "raw" / "runs.csv"


def vm_median(rows, group: str, lane: str, engine: str, field: str) -> float:
    """The median of one field over a server lane's correct runs."""
    from statistics import median

    return median(
        float(r[field]) for r in rows
        if r["group"] == group and r["lane"] == lane and r["engine"] == engine and r["correct"] == "True"
    )


def paired_bars(ax, theme: Theme, labels, horizon, queen, xlabel: str) -> None:
    """Horizontal bar pairs, Horizon above Queen, each bar labelled with its value."""
    y = range(len(labels))
    height = 0.38
    bars_h = ax.barh([i + height / 2 for i in y], horizon, height, color=theme.series[1], label="Horizon, Redis")
    bars_q = ax.barh([i - height / 2 for i in y], queen, height, color=theme.series[0], label="Queen, Raft broker")
    finish(ax, theme, "")
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    ax.set_yticks(list(y), labels)
    ax.set_xlabel(xlabel, color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, max(queen + horizon) * 1.16)
    for bars in (bars_h, bars_q):
        for bar in bars:
            ax.annotate(f"{bar.get_width():,.0f}", xy=(bar.get_width(), bar.get_y() + bar.get_height() / 2),
                        xytext=(4, 0), textcoords="offset points", va="center", fontsize=8,
                        color=theme.ink_strong)
    # The top pair is the shortest in every caller: its right side is free.
    ax.legend(loc="upper right")


def fig_laravel_vm_capacity(out: Path, theme: Theme) -> str:
    """Worker capacity on the Linux server: every job enqueued before the
    workers start, so the producer does not set the rate."""
    rows = read_csv(LINUX_VM)
    lanes = [
        ("drain", "drain-32", "10 ms jobs, 32 workers, both fsync every write"),
        ("drain", "drain-everysec-32", "10 ms jobs, 32 workers, Redis fsync once a second"),
        ("nofsync", "drain-nofsync-32", "10 ms jobs, 32 workers, Redis never fsyncs"),
        ("drain", "drain-noop-32", "Empty jobs, 32 workers"),
        ("drain", "drain-64", "10 ms jobs, 64 workers"),
    ][::-1]
    labels = [label for _, _, label in lanes]
    horizon = [vm_median(rows, g, lane, "horizon", "jobs_per_second") for g, lane, _ in lanes]
    queen = [vm_median(rows, g, lane, "queen-rust", "jobs_per_second") for g, lane, _ in lanes]

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    paired_bars(ax, theme, labels, horizon, queen,
                "Completed jobs per second, queue full before the workers start, median of 3 runs")
    save(fig, out, "laravel-vm-capacity", theme)
    return "laravel-vm-capacity"


def fig_laravel_vm_memory(out: Path, theme: Theme) -> str:
    """Memory of the application container as the pool grows."""
    rows = read_csv(LINUX_VM)
    pools = [
        ("load", "throughput-16", "16 workers"),
        ("load", "throughput-32", "32 workers"),
        ("drain", "drain-64", "64 workers"),
    ][::-1]
    labels = [label for _, _, label in pools]
    horizon = [vm_median(rows, g, lane, "horizon", "app_memory_mib") for g, lane, _ in pools]
    queen = [vm_median(rows, g, lane, "queen-rust", "app_memory_mib") for g, lane, _ in pools]

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 2.6))
    paired_bars(ax, theme, labels, horizon, queen,
                "Application container memory (MiB), master and workers, median of 3 runs")
    save(fig, out, "laravel-vm-memory", theme)
    return "laravel-vm-memory"


def fig_laravel_vm_latency(out: Path, theme: Theme) -> str:
    """Dispatch-to-completion latency with jobs arriving one by one: p50 and
    p95 per engine, at 500 jobs/s and in the 15-minute soak at 400 jobs/s."""
    rows = read_csv(LINUX_VM)
    lanes = [("paced-500", "500 jobs/s asked, 16 workers"), ("soak", "400 jobs/s for 15 minutes")]

    style(theme)
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.6), sharey=True, gridspec_kw={"wspace": 0.1})
    top = 0.0
    for ax, (lane, title) in zip(axes, lanes):
        groups = ["p50", "p95"]
        horizon = [vm_median(rows, "load", lane, "horizon", f"end_to_end_{p}_ms") for p in groups]
        queen = [vm_median(rows, "load", lane, "queen-rust", f"end_to_end_{p}_ms") for p in groups]
        top = max(top, *horizon, *queen)
        x = range(len(groups))
        width = 0.36
        bars_h = ax.bar([i - width / 2 for i in x], horizon, width, color=theme.series[1], label="Horizon")
        bars_q = ax.bar([i + width / 2 for i in x], queen, width, color=theme.series[0], label="Queen")
        finish(ax, theme, "End-to-end latency (ms)" if ax is axes[0] else "")
        ax.set_title(title, color=theme.ink_strong, fontsize=9, loc="left")
        ax.set_xticks(list(x), groups)
        for bars in (bars_h, bars_q):
            for bar in bars:
                ax.annotate(f"{bar.get_height():.0f}", xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                            xytext=(0, 3), textcoords="offset points", ha="center", fontsize=8,
                            color=theme.ink_strong)
    axes[0].legend(loc="upper left")
    axes[0].set_ylim(0, top * 1.2)

    save(fig, out, "laravel-vm-latency", theme)
    return "laravel-vm-latency"


def fig_laravel_headline(out: Path, theme: Theme) -> str:
    """The three Linux-server results a reader weighing a move from Horizon
    asks about first: capacity, memory and latency, one small panel each.

    Three units, so three scales: each panel has its own, and none shows a y
    axis, because a shared-looking axis would invite comparing a MiB bar with
    a jobs/s bar. Every bar starts at zero and carries its value; the panel
    title names the unit and which direction is better.
    """
    rows = read_csv(LINUX_VM)
    # (title, which way is better, value suffix, field, [(tick, group, lane)])
    panels = [
        ("Jobs per second, queue full", "higher is better", "", "jobs_per_second",
         [("32 workers", "drain", "drain-32"), ("64 workers", "drain", "drain-64")]),
        ("App memory", "lower is better", " MiB", "app_memory_mib",
         [("64 workers", "drain", "drain-64")]),
        ("p95 latency", "lower is better", " ms", "end_to_end_p95_ms",
         [("500 jobs/s asked", "load", "paced-500")]),
    ]
    engines = [("horizon", "Horizon, Redis", theme.series[1]),
               ("queen-rust", "Queen, Raft broker", theme.series[0])]

    style(theme)
    left = 0.01
    fig, axes = plt.subplots(
        1, 3, figsize=(7.2, 2.3), gridspec_kw={"width_ratios": [2, 1, 1], "wspace": 0.22}
    )
    fig.subplots_adjust(left=left, right=0.99)
    width = 0.38
    for ax, (title, better, unit, field, groups) in zip(axes, panels):
        top = 0.0
        for offset, (engine, label, color) in zip((-width / 2, width / 2), engines):
            values = [vm_median(rows, group, lane, engine, field) for _, group, lane in groups]
            top = max(top, *values)
            bars = ax.bar([i + offset for i in range(len(groups))], values, width * 0.92,
                          color=color, label=label)
            for bar, value in zip(bars, values):
                ax.annotate(f"{value:,.0f}{unit}", xy=(bar.get_x() + bar.get_width() / 2, value),
                            xytext=(0, 3), textcoords="offset points", ha="center", va="bottom",
                            fontsize=8.5, color=theme.ink_strong)
        finish(ax, theme, "")
        ax.grid(False)
        ax.spines["left"].set_visible(False)
        ax.set_yticks([])
        ax.set_ylim(0, top * 1.22)
        # The same width per group in every panel, so every bar is as wide.
        ax.set_xlim(-0.55, len(groups) - 0.45)
        ax.set_xticks(range(len(groups)), [tick for tick, _, _ in groups])
        ax.set_title(f"{title}\n", color=theme.ink_strong, fontsize=9, loc="left", pad=2)
        ax.text(0, 1.0, better, transform=ax.transAxes, ha="left", va="bottom",
                fontsize=8, color=theme.ink)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower left", bbox_to_anchor=(left, 1.03), ncol=2,
               handlelength=1.0, columnspacing=1.6, borderaxespad=0, borderpad=0)
    # How many runs each median is of, read from the same rows: one count if
    # every bar has the same, as the campaign intends.
    counts = {
        sum(1 for r in rows if r["group"] == group and r["lane"] == lane
            and r["engine"] == engine and r["correct"] == "True")
        for *_, groups in panels for _, group, lane in groups for engine, _, _ in engines
    }
    medians = f"Medians of {counts.pop()} runs" if len(counts) == 1 else "Medians of each lane's runs"
    # The latency lane asked for 500 jobs/s and Horizon's producer could not
    # send that many: say so on the figure, from the same rows.
    reached = vm_median(rows, "load", "paced-500", "horizon", "jobs_per_second")
    fig.text(left, -0.04, f"{medians} on one 16-vCPU Linux server, 10 ms jobs, fsync on every "
             f"write.\nAt 500 jobs/s asked, Horizon's producer reached {reached:.0f} jobs/s.",
             ha="left", va="top", fontsize=8, color=theme.ink, linespacing=1.5)

    save(fig, out, "laravel-headline", theme)
    return "laravel-headline"
