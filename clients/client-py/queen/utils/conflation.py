"""
Client-side half of the conflation contract (PLAN_CONFLATION §3.3, §4).

Two rules live here, and both exist because the broker's answer is the only
channel: there is no capability negotiation anywhere in this SDK.

* **Degrade loudly.** A broker that does not know the `conflation` query
  parameter ignores it and answers an ordinary, successful pop. Nothing raises,
  nothing logs, and the consumer drains the whole backlog one message at a time
  -- the exact failure conflation exists to prevent. So: a response that does
  not carry ``"conflation": true`` after we asked for it is an ERROR, raised on
  the first such response. It is detectable that early because the broker
  emits the key on empty pops too.

* **Warn once on a declaration conflict.** The stored consumer-group policy
  always wins (§3.3): rejecting would break rolling deploys, where half the
  fleet already sends the flag and half does not. But a per-response warning
  would flood at pop rate, so the SDK warns once per (queue, group) per process.

The warn registry is deliberately process-wide and mutable from tests via
``reset_conflict_warnings`` -- "once per process" is not observable otherwise.
"""

from __future__ import annotations

import sys
import threading
from typing import Any, Dict, Optional, Set, Tuple

from . import logger
from ..errors import ConflationUnsupportedError

# The minimum broker that implements PLAN_CONFLATION. Named once so the SDK and
# its error message cannot drift apart.
MIN_BROKER_VERSION = "1.1.0"

# Verbatim across every SDK: operators grep this string, and a per-language
# paraphrase would make that impossible.
UNSUPPORTED_MESSAGE = (
    "conflation was requested but this broker did not apply it "
    "— requires broker >= " + MIN_BROKER_VERSION
)

_conflict_warned: Set[Tuple[str, str]] = set()
_conflict_lock = threading.Lock()


def reset_conflict_warnings() -> None:
    """Forget every (queue, group) that has already warned. Test hook."""
    with _conflict_lock:
        _conflict_warned.clear()


def scope_of(
    queue: Optional[str] = None,
    namespace: Optional[str] = None,
    task: Optional[str] = None,
) -> str:
    """The pop target, as one string, for the warn-once key.

    Mirrors the affinity-key precedence already used for backend routing: a
    named queue when there is one, otherwise namespace:task.
    """
    if queue:
        return queue
    if namespace or task:
        return f"{namespace or '*'}:{task or '*'}"
    return "*"


def warn_conflict_once(queue: Optional[str], group: Optional[str]) -> bool:
    """Emit the declaration-conflict warning at most once per (queue, group).

    Returns True when this call is the one that warned.
    """
    key = (queue or "*", group or "__QUEUE_MODE__")
    with _conflict_lock:
        if key in _conflict_warned:
            return False
        _conflict_warned.add(key)

    logger.warn(
        "Queen.conflation",
        {"queue": key[0], "group": key[1], "status": "declaration-mismatch"},
    )
    # Unconditional, unlike logger.warn (which is off unless QUEEN_CLIENT_LOG=
    # true): a policy this consumer asked for and did not get has to be visible
    # without opting into debug logging. Same channel as the other
    # operator-facing notices in this SDK ("Pop failed", "Network error").
    print(
        f"[queen] conflation conflict on queue '{key[0]}' group '{key[1]}': "
        "the consumer group's stored setting wins and this consumer's "
        "declaration was ignored. Align the declaration, or delete and "
        "recreate the group to change its policy.",
        file=sys.stderr,
    )
    return True


def check_pop_response(
    response: Any,
    *,
    requested: bool,
    queue: Optional[str] = None,
    group: Optional[str] = None,
) -> None:
    """Enforce the two rules above on one pop response.

    Raises ``ConflationUnsupportedError`` when conflation was requested and the
    response says nothing about conflation at all. A response that is not a dict
    (empty body, 204) is treated the same way on purpose: absence of proof is
    not proof of application, and guessing here is what silence costs.

    THE ORDER OF THE CHECKS IS THE CONTRACT, and it is the one every other SDK
    keeps (Go ``checkConflationEcho``, PHP ``ConflationGuard``, JS
    ``checkConflationResponse``, Rust ``check_conflation``): the CONFLICT key is
    read first, because a conflict is a 1.1.0 broker answering "the group is
    already registered the other way, my value wins" -- the opposite of an old
    broker. The body a 1.1.0 broker sends for requested=true / stored=false is
    ``{"messages": [...], "conflationConflict": true}`` with NO ``conflation``
    key (it is emitted only when the effective policy IS conflating, §3.1), so
    raising on the missing echo first would kill exactly the consumer §3.3 Q3
    and §7.3 E2E-4 require to keep running -- and it would do it on that
    consumer's first poll of an idle queue.

    ``paused`` is likewise not a verdict: the broker refuses pops during pop
    maintenance before the request ever reaches the claim path, so there is no
    policy to echo and nothing to conclude from the absence of one.

    A no-op when ``requested`` is falsy -- the check keys off what THIS consumer
    asked for, never off the response alone, so a consumer that never opted in
    is byte-for-byte unaffected.
    """
    if not requested:
        return

    body: Dict[str, Any] = response if isinstance(response, dict) else {}

    if body.get("conflationConflict") is True:
        warn_conflict_once(queue, group)
        return

    if body.get("conflation") is True:
        return

    if body.get("paused") is True:
        return

    raise ConflationUnsupportedError(UNSUPPORTED_MESSAGE)
