"""
Typed errors for the surfaces whose failure taxonomy is a CLOSED SET OF CODES.

PLAN_KV_TIMERS.md §13.5 and §10.3: a client may branch on the code and never on
the message -- string matching on error prose is forbidden everywhere in this
codebase, and these two surfaces are the first ones where the taxonomy is rich
enough that somebody would be tempted.

The two envelopes a caller can meet, and why one attribute reads both:

    broker (handlers/kv.rs, handlers/timers.rs)
        {"error": "<code>", "reason": "<stable identifier>", "detail": "<prose>"}
    proxy  (PLAN_QUEEN_PROXY_CLOUD.md §4/§9)
        {"error": "<prose>", "code": "<code>"}  + Retry-After on 429

`code` below is `body["code"]` when the proxy answered and `body["error"]` when
the broker did, so `except KvError as e: if e.code == "storage_quota_exceeded"`
works regardless of which one refused, which is the whole point.

These subclass ``httpx.HTTPStatusError`` deliberately: everything that already
catches that keeps catching it, and nothing that inspects ``e.response`` breaks.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import httpx


class QueenError(Exception):
    """Base class for errors this SDK raises on its own behalf."""


class ConflationUnsupportedError(QueenError):
    """`conflation=true` was sent and the broker did not apply it.

    PLAN_CONFLATION §4, the degrade-loudly rule. No SDK does version or
    capability negotiation, so an old broker answers a *successful* pop that
    simply ignored the unknown query parameter -- and the consumer would then
    drain the entire backlog message by message, quietly, which is the one
    outcome the feature exists to prevent. The broker echoes `"conflation":true`
    on every conflating response INCLUDING empty ones, so this fires on the
    first round trip, before any message is handled.

    Not an ``httpx.HTTPStatusError``: nothing failed at the HTTP layer. This is
    the SDK refusing to run under a contract the peer does not honour.
    """


class QueenHttpError(httpx.HTTPStatusError, QueenError):
    """An HTTP failure carrying the server's machine-readable verdict."""

    def __init__(
        self,
        message: str,
        *,
        request: httpx.Request,
        response: httpx.Response,
        code: Optional[str] = None,
        reason: Optional[str] = None,
        detail: Optional[str] = None,
        retry_after_seconds: Optional[float] = None,
    ) -> None:
        super().__init__(message, request=request, response=response)
        self.status = response.status_code
        self.code = code
        self.reason = reason
        self.detail = detail
        self.retry_after_seconds = retry_after_seconds

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        bits = [f"HTTP {self.status}"]
        if self.code:
            bits.append(self.code)
        base = super().__str__()
        if base:
            bits.append(base)
        return " ".join(bits)


class KvError(QueenHttpError):
    """A refusal from the key/value surface."""


class TimerError(QueenHttpError):
    """A refusal from the timer surface."""


class EphemeralError(QueenHttpError):
    """A refusal from the ephemeral surface (EPHEMERAL_QUEUES.md §3.1, §4).

    One code deserves naming here because it is not a refusal at all but a
    version verdict: ``ephemeral_unsupported``. No SDK negotiates capabilities,
    so a broker or proxy older than 1.1 answers 404 on the WHOLE family -- the
    broker because the routes were never registered, the proxy because an
    unknown API path is ``route_blocked`` and it fails closed. Both are mapped
    to this type with that code and the original kept as ``__cause__``.

    Exactly one 404 on this family means something else, which is why the
    mapping reads the body's CODE and not the status the two share: see
    ``EphemeralQueueNotFoundError``.
    """


class EphemeralQueueNotFoundError(EphemeralError):
    """``depth`` named an ephemeral queue that is not there.

    The ONLY verb of the family that can say this, and that is worth knowing
    rather than discovering: push and pop create a queue by naming it, ``reset``
    answers ``dropped:0`` and ``delete`` answers ``deleted:false``. So this is a
    real DATA fact -- a queue name typo, or a ring that was empty and idle long
    enough to be collected -- and not the DEPLOYMENT fact
    ``ephemeral_unsupported`` states. Collapsing the two would send somebody
    chasing a broker version over a queue name.

    A subclass of ``EphemeralError`` on purpose: every existing
    ``except EphemeralError`` keeps catching it, and what the distinct type buys
    is branching without string-matching the prose. ``code`` is
    ``ephemeral_queue_not_found`` -- the broker's own code string, byte-identical
    across every SDK (Go ``ErrEphemeralQueueNotFound``,
    ``queen_protocol::EPHEMERAL_QUEUE_NOT_FOUND_CODE``) so a code seen in one
    language's logs means the same thing in the next. ``queue`` names the missing
    queue, ``response`` is the 404 as it arrived, and ``__cause__`` is the
    original.
    """

    def __init__(self, message: str, *, queue: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)
        self.queue = queue


def _body_of(response: httpx.Response) -> Dict[str, Any]:
    try:
        body = response.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


def code_of(response: httpx.Response) -> Optional[str]:
    """The machine-readable verdict in a refusal body, whoever answered.

    The broker puts the stable identifier in ``error``; the proxy puts it in
    ``code`` and prose in ``error``. ``code`` wins when both are present, because
    the only responses carrying both are the proxy's.

    One definition, used by ``wrap_http_error`` and by any surface that has to
    branch on the code BEFORE the wrapping (the ephemeral family, whose two 404s
    are told apart by this field and by nothing else) -- two parsers would be two
    answers to "which field is the code", which is the question this whole
    module exists to settle.
    """
    body = _body_of(response)
    code = body.get("code") or body.get("error")
    return code if isinstance(code, str) else None


def wrap_http_error(error: Exception, kind: type) -> Exception:
    """Re-raise an ``httpx.HTTPStatusError`` as `kind`, keeping the verdict.

    Anything else is returned untouched -- a timeout, a connection reset and a
    ValueError raised by this SDK's own validation are not the server's
    verdicts and must not be dressed up as one.
    """
    if not isinstance(error, httpx.HTTPStatusError):
        return error
    if isinstance(error, QueenHttpError):
        return error

    body = _body_of(error.response)
    code = code_of(error.response)
    retry_after = getattr(error, "retry_after_seconds", None)
    if retry_after is None:
        raw = error.response.headers.get("retry-after")
        if raw is not None:
            try:
                retry_after = float(raw)
            except (TypeError, ValueError):
                retry_after = None

    return kind(
        str(body.get("detail") or body.get("error") or error),
        request=error.request,
        response=error.response,
        code=code,
        reason=body.get("reason") if isinstance(body.get("reason"), str) else None,
        detail=body.get("detail") if isinstance(body.get("detail"), str) else None,
        retry_after_seconds=retry_after,
    )
