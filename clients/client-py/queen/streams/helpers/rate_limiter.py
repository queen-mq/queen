"""
Rate-limiter helpers — composable token-bucket / sliding-window gate
factories that return a callable suitable for ``Stream.gate(fn)``.

Mirror of the JS rateLimiter.js helpers. The semantics (per-key state in
``queen_streams.state``, partial-ack on deny, FIFO order on lease expiry)
all come from the underlying Stream gate runtime — these factories only
decide HOW each request consumes from the bucket.

All four common rate-limit shapes are expressible by varying ``cost_fn``:

  A) "100 req/sec, batches arbitrary":     cost_fn = lambda m: 1     on a queue of REQUESTS
  B) "100 msg/sec, in any number of req":  cost_fn = lambda m: 1     on a queue of MESSAGES
  C) "100 req/sec, exactly 1 msg/req":     cost_fn = lambda m: 1     same as B (degenerate)
  D) "100 weight/sec, cost varies":        cost_fn = lambda m: m["weight"]

Sizing rule: max sustainable rate per partition =
    min(refill_per_sec, capacity / lease_sec)
Pick capacity and lease_sec so the second term doesn't artificially cap you
below refill_per_sec — usually capacity ≈ refill_per_sec × lease_sec.
"""

from __future__ import annotations

import math
from typing import Any, Callable


def _is_finite_positive(x: Any) -> bool:
    return (
        isinstance(x, (int, float))
        and not isinstance(x, bool)
        and math.isfinite(x)
        and x > 0
    )


def token_bucket_gate(
    capacity: float,
    refill_per_sec: float,
    cost_fn: Callable[[Any], float] = lambda _msg: 1,
    allow_zero_cost: bool = True,
) -> Callable[[Any, Any], bool]:
    """
    Token-bucket gate factory.

    Returns a ``(msg, ctx) -> bool`` callable suitable for ``Stream.gate(fn)``.
    The bucket state lives in ``ctx.state`` (which the runtime persists per-key
    in ``queen_streams.state`` on every ALLOWED message).
    """
    if not _is_finite_positive(capacity):
        raise ValueError("token_bucket_gate: capacity must be a positive number")
    if not _is_finite_positive(refill_per_sec):
        raise ValueError("token_bucket_gate: refill_per_sec must be a positive number")
    if not callable(cost_fn):
        raise TypeError("token_bucket_gate: cost_fn must be callable")

    def token_bucket(msg: Any, ctx: Any) -> bool:
        now = ctx.stream_time_ms
        state = ctx.state
        if not isinstance(state.get("tokens"), (int, float)):
            state["tokens"] = capacity
        if not isinstance(state.get("lastRefillAt"), (int, float)):
            state["lastRefillAt"] = now

        elapsed_sec = max(0.0, (now - state["lastRefillAt"]) / 1000.0)
        state["tokens"] = min(capacity, state["tokens"] + elapsed_sec * refill_per_sec)
        state["lastRefillAt"] = now

        cost = cost_fn(msg)
        if not isinstance(cost, (int, float)) or isinstance(cost, bool) or not math.isfinite(cost) or cost < 0:
            cost = 1
        if cost == 0:
            return allow_zero_cost

        if state["tokens"] >= cost:
            state["tokens"] -= cost
            state["allowedTotal"] = state.get("allowedTotal", 0) + 1
            state["consumedTotal"] = state.get("consumedTotal", 0) + cost
            return True
        return False

    return token_bucket


def sliding_window_gate(
    limit: float,
    window_sec: float,
    cost_fn: Callable[[Any], float] = lambda _msg: 1,
) -> Callable[[Any, Any], bool]:
    """
    Sliding-window approximation gate (for "max N events in last W seconds").

    NOT a precise sliding window. 2-bucket approximation (current +
    previous) accurate within ±1× rate at the boundary. For exact sliding
    window keep per-event timestamps in state.
    """
    if not _is_finite_positive(limit):
        raise ValueError("sliding_window_gate: limit must be a positive number")
    if not _is_finite_positive(window_sec):
        raise ValueError("sliding_window_gate: window_sec must be a positive number")

    window_ms = window_sec * 1000

    def sliding_window(msg: Any, ctx: Any) -> bool:
        now = ctx.stream_time_ms
        state = ctx.state
        current_window = math.floor(now / window_ms)
        elapsed_in_window = (now % window_ms) / window_ms

        if state.get("window") != current_window:
            state["previousCount"] = (
                state.get("currentCount", 0) if state.get("window") == current_window - 1 else 0
            )
            state["currentCount"] = 0
            state["window"] = current_window

        estimated = (state.get("previousCount", 0) or 0) * (1 - elapsed_in_window) + (
            state.get("currentCount", 0) or 0
        )
        cost = cost_fn(msg)
        if not isinstance(cost, (int, float)) or isinstance(cost, bool) or not math.isfinite(cost) or cost < 0:
            cost = 1

        if estimated + cost <= limit:
            state["currentCount"] = state.get("currentCount", 0) + cost
            return True
        return False

    return sliding_window
