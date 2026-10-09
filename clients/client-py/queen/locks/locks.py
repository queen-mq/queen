"""
Locks: a lock and a semaphore, as leases with a fencing token.

    lock = client.lock("daily-report", ttl_seconds=30)
    if not await lock.acquire():
        return                                  # somebody else has it
    try:
        await (
            client.transaction()
            .guard(lock)                        # commits only while the lock is ours
            .queue("reports").push([{"data": report}])
            .commit()
        )
    finally:
        await lock.release()

WHAT IT IS. A permit is one KV row in the namespace ``queen-locks``, written
with a lifetime: ``acquire`` is a ``putIfAbsent``, ``renew`` a ``put`` with
``expect``, ``release`` a ``delete`` with ``expect``. The broker's
``POST /api/v1/locks`` does that turning, so there is one implementation of it
for every client. A lock is the semaphore of one permit;
``client.semaphore(name, n, ...)`` is the same thing with n.

WHAT IT IS NOT: A MUTEX. A permit EXPIRES, and nobody tells its holder. A
process that is paused, partitioned or slow keeps running past its lifetime
while somebody else acquires. So the lock alone never makes two holders
impossible; what makes their WORK exclusive is the token:

  * inside Queen, ``.guard(lock)`` on a transaction: the acks, pushes, KV
    writes and timers of the step commit only if the permit is still this
    holder's, in the same log entry. A holder that was replaced commits
    nothing.
  * outside Queen, ``lock.token``: a number that only rises on a lock. A
    resource that remembers the highest token it has accepted and refuses a
    lower one refuses the holder that was replaced. Accept an EQUAL one: a
    holder writes many times with one token.

THE TOKEN CHANGES AT EVERY RENEW. A renew rewrites the row, so the broker
answers a new token and the one before stops working. The handle keeps the
current one: read ``lock.token`` and ``lock.guard()`` when you use them, never
hold a copy across an ``await``.

THE OWNER is the holder's identity, minted per handle. It is what makes a call
safe to send again when its answer was lost: the broker answers the permit the
first attempt took. Two handles with one owner are one holder -- pass your own
only if that is what you mean.
"""

from __future__ import annotations

import asyncio
import math
import os
import random
import secrets
import socket
import time
from datetime import timedelta
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Union

from ..errors import LockError, LockNotHeldError, wrap_http_error
from ..kv.ops import UNSET, is_unset
from ..utils import logger

_PATH = "/api/v1/locks"
_NAME_MAX_BYTES = 256
_OWNER_MAX_BYTES = 256
_LIMIT_MAX = 1024

Seconds = Union[int, float, timedelta]


def _is_control(ch: str) -> bool:
    code = ord(ch)
    return code < 0x20 or 0x7F <= code <= 0x9F


def _name(name: Any) -> str:
    # The broker's rule, checked here so the mistake surfaces at the call: no
    # '#', which sits between a name and its slot in the row's key.
    if (
        not isinstance(name, str)
        or not name
        or len(name.encode("utf-8")) > _NAME_MAX_BYTES
        or any(_is_control(c) or c == "#" for c in name)
    ):
        raise ValueError(
            f"a lock's name is a non-empty string of at most {_NAME_MAX_BYTES} bytes, "
            f"without control characters and without '#' -- got {name!r}"
        )
    return name


def _owner(owner: Any) -> str:
    if (
        not isinstance(owner, str)
        or not owner
        or len(owner.encode("utf-8")) > _OWNER_MAX_BYTES
        or any(_is_control(c) for c in owner)
    ):
        raise ValueError(
            f"owner is a non-empty string of at most {_OWNER_MAX_BYTES} bytes, without control characters"
        )
    return owner


def _limit(limit: Any) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= _LIMIT_MAX:
        raise ValueError(f"limit is a whole number from 1 (a lock) to {_LIMIT_MAX}")
    return limit


def _token(token: Any, what: str) -> int:
    if isinstance(token, bool) or not isinstance(token, int) or token <= 0:
        raise ValueError(
            f"{what} needs the token of the permit (the one the last acquire or renew answered)"
        )
    return token


def lock_ttl_seconds(ttl_seconds: Any = UNSET, ttl: Any = UNSET) -> int:
    """The lifetime in whole seconds, rounded UP.

    ``ttl_seconds`` is an int, ``ttl`` a ``timedelta``. Exactly one, and there
    is no ``forever`` and no default: a lock that never expires is one nobody
    can take back from a holder that died.
    """
    if not is_unset(ttl_seconds) and not is_unset(ttl):
        raise ValueError("lifetime declared twice (ttl_seconds and ttl) -- pick one")
    if not is_unset(ttl):
        if not isinstance(ttl, timedelta):
            raise TypeError("ttl must be a timedelta; for a plain number use ttl_seconds")
        ttl_seconds = math.ceil(ttl.total_seconds())
    if is_unset(ttl_seconds) or isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
        raise ValueError(
            "a lock needs a lifetime: ttl_seconds=<int above zero> or ttl=<timedelta>. "
            "There is no forever; a holder that needs longer renews"
        )
    return ttl_seconds


def _seconds(value: Seconds) -> float:
    return value.total_seconds() if isinstance(value, timedelta) else float(value)


def _mint_owner() -> str:
    return f"{socket.gethostname()[:128]}:{os.getpid()}:{secrets.token_hex(6)}"


class LockResult(dict):
    """One result of a locks call: a plain dict whose ``bool`` is the verdict.

    ``acquired`` for an acquire, ``renewed`` for a renew, ``released`` for a
    release, ``held`` for a get -- so ``if await client.locks.acquire(...)``
    means what it reads as, which a bare dict would not (it is truthy whatever
    it says).
    """

    __slots__ = ()

    def __bool__(self) -> bool:
        for field in ("acquired", "renewed", "released", "held"):
            if field in self:
                return bool(self[field])
        return len(self) > 0


class Locks:
    """The four lock operations as the broker speaks them, with no state kept:
    the caller carries the token. ``client.lock()`` is what most code wants;
    this is for ``get`` (who holds it?) and for a caller with its own loop.

    As on the KV routes, the HTTP status says how the CALL went and never what
    an operation answered: a lock held by somebody else is a 200 with
    ``acquired: False``.
    """

    def __init__(self, http_client: Any) -> None:
        self._http_client = http_client
        self._held: "set[Lock]" = set()

    async def batch(self, operations: Iterable[Dict[str, Any]]) -> List[LockResult]:
        """Several operations in one call, each on a different lock: one result
        per operation, in order. They are independent -- nothing here is
        all-or-nothing."""
        op_list = list(operations)
        if not op_list:
            return []
        logger.log("Locks.batch", {"count": len(op_list), "ops": [o.get("op") for o in op_list]})
        try:
            response = await self._http_client.post(_PATH, {"operations": op_list})
        except Exception as error:  # noqa: BLE001 - re-raised, possibly re-typed
            raise wrap_http_error(error, LockError) from None
        results = (response or {}).get("results")
        if not isinstance(results, list) or len(results) != len(op_list):
            raise RuntimeError(
                f'locks: expected {{"results": [...]}} with {len(op_list)} element(s), got {response!r}'
            )
        return [LockResult(r) for r in results]

    async def _one(self, op: Dict[str, Any]) -> LockResult:
        return (await self.batch([op]))[0]

    async def acquire(
        self,
        name: str,
        *,
        ttl_seconds: Any = UNSET,
        ttl: Any = UNSET,
        owner: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> LockResult:
        """Take a permit: ``{acquired, slot, token, owner, guard, already?}``,
        or ``{acquired: False, reason: 'held' | 'contended', holders}``.

        ``limit`` above 1 makes it a semaphore of that many permits. Every
        caller of one name passes the same limit; it is stored nowhere.
        """
        op: Dict[str, Any] = {
            "op": "acquire",
            "name": _name(name),
            "ttlSeconds": lock_ttl_seconds(ttl_seconds, ttl),
        }
        if owner is not None:
            op["owner"] = _owner(owner)
        if limit is not None:
            op["limit"] = _limit(limit)
        return await self._one(op)

    async def renew(
        self,
        name: str,
        *,
        token: int,
        ttl_seconds: Any = UNSET,
        ttl: Any = UNSET,
        slot: Optional[int] = None,
        owner: Optional[str] = None,
    ) -> LockResult:
        """Extend a permit: ``{renewed, slot, token, guard}`` with a NEW token,
        or ``{renewed: False, reason: 'lost', holders}``."""
        op: Dict[str, Any] = {
            "op": "renew",
            "name": _name(name),
            "token": _token(token, "renew"),
            "ttlSeconds": lock_ttl_seconds(ttl_seconds, ttl),
        }
        if slot is not None:
            op["slot"] = slot
        if owner is not None:
            op["owner"] = _owner(owner)
        return await self._one(op)

    async def release(self, name: str, *, token: int, slot: Optional[int] = None) -> LockResult:
        """Give a permit back: ``{released}``; ``False`` with ``reason: 'lost'``
        when the token is no longer the row's."""
        op: Dict[str, Any] = {"op": "release", "name": _name(name), "token": _token(token, "release")}
        if slot is not None:
            op["slot"] = slot
        return await self._one(op)

    async def get(self, name: str) -> LockResult:
        """Who holds it: ``{held, holders: [{slot, owner, token, since, expiresAt, renewedAt}]}``."""
        return await self._one({"op": "get", "name": _name(name)})

    # ---- the handles of this client, for close() ------------------------------

    def _track(self, lock: "Lock", held: bool) -> None:
        if held:
            self._held.add(lock)
        else:
            self._held.discard(lock)

    async def release_all(self) -> int:
        """Give back every permit this client's handles hold, best effort.
        Called by ``Queen.close()``: a permit left behind is only a wait of one
        lifetime for the next holder, never a leak."""
        held = list(self._held)
        for lock in held:
            try:
                await lock.release()
            except Exception as error:  # noqa: BLE001 - it expires by itself
                logger.warn("Locks.release_all", {"lock": lock.name, "error": str(error)})
        return len(held)


class Lock:
    """One holder's hold on one lock (or one permit of a semaphore).

    It keeps the token current, renews in the background (every third of the
    lifetime, unless ``auto_renew=False``), and says when the permit is gone:
    ``lock.lost`` is set and ``on_lost`` handlers run. "Gone" is the broker
    saying so, or the lifetime passing on THIS machine's clock with no renew
    having succeeded -- a client that cannot reach the broker must assume the
    worst.

    ``async with lock:`` acquires (waiting ``wait`` seconds, as given here) and
    releases; it raises :class:`LockNotHeldError` when the permit could not be
    had, because a block that ran without the lock is the one thing it must
    not do.
    """

    def __init__(
        self,
        locks: Locks,
        name: str,
        *,
        ttl_seconds: Any = UNSET,
        ttl: Any = UNSET,
        limit: int = 1,
        owner: Optional[str] = None,
        auto_renew: bool = True,
        renew_every: Optional[Seconds] = None,
        wait: Seconds = 0,
        retry_min: float = 0.1,
        retry_max: float = 1.0,
    ) -> None:
        self._locks = locks
        self._name = _name(name)
        self._ttl_seconds = lock_ttl_seconds(ttl_seconds, ttl)
        self._limit = _limit(limit)
        self._owner = _owner(owner) if owner is not None else _mint_owner()
        self._auto_renew = auto_renew
        every = _seconds(renew_every) if renew_every is not None else self._ttl_seconds / 3
        if not 0 < every < self._ttl_seconds:
            raise ValueError("renew_every must be shorter than the lifetime, or the permit expires between renews")
        self._renew_every = every
        self._wait = _seconds(wait)
        # How a waiting acquire comes back: first after retry_min, then 1.5x
        # each time up to retry_max, each wait shortened by a random quarter so
        # a crowd spreads.
        self._retry_min = retry_min
        self._retry_max = retry_max

        self._token: Optional[int] = None
        self._slot: Optional[int] = None
        # The broker's own guard of the current lease period, kept as answered:
        # where a permit's row lives is the broker's rule, written once, there.
        self._guard: Optional[Dict[str, Any]] = None
        self._valid_until = 0.0
        self._task: Optional["asyncio.Task[None]"] = None
        self._renewing: Optional["asyncio.Future[bool]"] = None
        self._lost_handlers: List[Callable[[str], Any]] = []
        self.lost = asyncio.Event()

    # ---- what it is -----------------------------------------------------------

    @property
    def name(self) -> str:
        return self._name

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def ttl_seconds(self) -> int:
        return self._ttl_seconds

    @property
    def held(self) -> bool:
        """Whether this handle holds a permit, as far as it can know: the
        broker granted or renewed it, and its lifetime has not run out on this
        machine's clock. A belief with a deadline, not a proof -- the proof is
        the guard on the transaction."""
        return self._token is not None and time.monotonic() < self._valid_until

    @property
    def token(self) -> Optional[int]:
        """The fencing token of the current lease period; None when not held."""
        return self._token if self.held else None

    @property
    def slot(self) -> Optional[int]:
        """The semaphore slot this handle holds (0 for a lock); None when not held."""
        return self._slot if self.held else None

    def on_lost(self, fn: Callable[[str], Any]) -> "Lock":
        """Run ``fn(reason)`` when the permit is lost: 'renew', 'guard',
        'expired'. A release is not a loss."""
        self._lost_handlers.append(fn)
        return self

    def guard(self) -> Dict[str, Any]:
        """The KV operation that holds while the permit is this handle's: a
        ``check`` of the permit's row at the current token, ``required``.
        ``.guard(lock)`` on a transaction adds it for you and follows a renew;
        use this one to put it in a KV batch yourself, at the moment you send."""
        if not self.held or self._guard is None:
            raise LockNotHeldError(f"lock {self._name!r} is not held; there is nothing to guard with")
        return dict(self._guard)

    # ---- the operations -------------------------------------------------------

    async def acquire(self, *, wait: Optional[Seconds] = None) -> bool:
        """Take the permit. ``True`` or ``False``.

        With ``wait`` (seconds, or a timedelta) it keeps trying until the
        permit is free or the wait is over, coming back every 100 ms to 1 s
        with jitter. Without it, the ``wait`` the handle was created with.
        """
        if self.held:
            return True
        budget = self._wait if wait is None else _seconds(wait)
        deadline = time.monotonic() + budget
        pause = self._retry_min
        while True:
            sent_at = time.monotonic()
            result = await self._locks.acquire(
                self._name,
                ttl_seconds=self._ttl_seconds,
                owner=self._owner,
                limit=self._limit if self._limit > 1 else None,
            )
            if result.get("acquired"):
                self._take(result, sent_at)
                return True
            left = deadline - time.monotonic()
            if left <= 0:
                return False
            await asyncio.sleep(min(pause * (0.75 + random.random() * 0.25), left))
            pause = min(pause * 1.5, self._retry_max)

    async def renew(self) -> bool:
        """Extend the lease now. ``True`` with a new token in place, or
        ``False``: the permit is gone and the handle says so. Raises when the
        broker could not be asked -- the permit is then neither renewed nor
        known lost, and its deadline stands."""
        if self._renewing is not None:
            return await asyncio.shield(self._renewing)
        if self._token is None:
            return False
        renewing = asyncio.ensure_future(self._renew_once())
        self._renewing = renewing
        renewing.add_done_callback(self._renew_done)
        return await asyncio.shield(renewing)

    def _renew_done(self, renewing: "asyncio.Future[bool]") -> None:
        # Cleared when the renew itself ends, not when whoever awaited it does:
        # a cancelled waiter must not make a renew in flight invisible to the
        # release that has to wait for it.
        if self._renewing is renewing:
            self._renewing = None
        if not renewing.cancelled():
            renewing.exception()  # retrieved here; the awaiter saw it too

    async def _renew_once(self) -> bool:
        token = self._token
        sent_at = time.monotonic()
        result = await self._locks.renew(
            self._name,
            token=token,  # type: ignore[arg-type]
            slot=self._slot,
            ttl_seconds=self._ttl_seconds,
            owner=self._owner,
        )
        # Released, or lost, while the renew was in flight: its answer is about
        # a permit this handle no longer has.
        if self._token != token:
            return False
        if not result.get("renewed"):
            self._lose("renew")
            return False
        self._token = result["token"]
        self._guard = result["guard"]
        self._valid_until = sent_at + self._ttl_seconds
        return True

    async def release(self) -> bool:
        """Give the permit back. ``True`` when the broker removed it; ``False``
        when it was not this handle's any more or was never held. Either way
        the handle holds nothing afterwards and can acquire again."""
        if self._renewing is not None:
            # A renew in flight owns the token until it answers.
            try:
                await asyncio.shield(self._renewing)
            except Exception:  # noqa: BLE001 - the release below is what matters
                pass
        if self._token is None:
            return False
        token, slot = self._token, self._slot
        self._drop()
        result = await self._locks.release(self._name, token=token, slot=slot)
        return bool(result.get("released"))

    async def run(self, fn: Callable[["Lock"], Awaitable[Any]], *, wait: Optional[Seconds] = None) -> Dict[str, Any]:
        """Acquire, await ``fn(lock)``, release -- whatever ``fn`` does.
        ``{"acquired": False}`` when the permit could not be had, else
        ``{"acquired": True, "value": ...}``.

        It does not stop ``fn`` when the permit is lost; nothing can. Watch
        ``lock.lost`` in ``fn``, and guard what it commits."""
        if not await self.acquire(wait=wait):
            return {"acquired": False}
        try:
            return {"acquired": True, "value": await fn(self)}
        finally:
            try:
                await self.release()
            except Exception as error:  # noqa: BLE001 - the permit expires by itself
                logger.warn("Lock.run", {"lock": self._name, "error": str(error)})

    async def __aenter__(self) -> "Lock":
        if not await self.acquire():
            raise LockNotHeldError(f"lock {self._name!r} is held by somebody else")
        return self

    async def __aexit__(self, *exc: Any) -> None:
        try:
            await self.release()
        except Exception as error:  # noqa: BLE001 - never mask the block's own error
            logger.warn("Lock.release", {"lock": self._name, "error": str(error)})

    # ---- used by TransactionBuilder.guard -------------------------------------

    async def _settled(self) -> None:
        """Returns once no renew is in flight: the token is then current."""
        if self._renewing is not None:
            try:
                await asyncio.shield(self._renewing)
            except Exception:  # noqa: BLE001
                pass

    def _mark_lost(self, reason: str) -> None:
        if self._token is not None:
            self._lose(reason)

    # ---- state ----------------------------------------------------------------

    def _take(self, result: Dict[str, Any], sent_at: float) -> None:
        self._token = result["token"]
        self._slot = result["slot"]
        self._guard = result["guard"]
        # Counted from when the request was SENT, so it is never later than the
        # broker's own deadline.
        self._valid_until = sent_at + self._ttl_seconds
        # A handle that lost a permit earlier starts clean with the new one.
        self.lost = asyncio.Event()
        self._locks._track(self, True)
        if self._task is not None:
            self._task.cancel()
        self._task = asyncio.ensure_future(self._keep())

    def _drop(self) -> None:
        self._token = None
        self._slot = None
        self._guard = None
        self._valid_until = 0.0
        task, self._task = self._task, None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
        self._locks._track(self, False)

    def _lose(self, reason: str) -> None:
        logger.warn("Lock.lost", {"lock": self._name, "owner": self._owner, "reason": reason})
        lost = self.lost
        self._drop()
        lost.set()
        for fn in self._lost_handlers:
            try:
                fn(reason)
            except Exception as error:  # noqa: BLE001 - a handler must not stop the others
                logger.error("Lock.on_lost", {"error": str(error)})

    async def _keep(self) -> None:
        """The background of one hold: renew when due, and call the permit lost
        when its lifetime runs out here."""
        try:
            pause = self._renew_every
            while self._token is not None:
                left = self._valid_until - time.monotonic()
                await asyncio.sleep(max(min(pause, left) if self._auto_renew else left, 0))
                if self._token is None:
                    return
                if time.monotonic() >= self._valid_until:
                    self._lose("expired")
                    return
                if not self._auto_renew:
                    continue
                try:
                    await self.renew()
                    pause = self._renew_every
                except Exception as error:  # noqa: BLE001
                    # Could not ask. Not a loss yet: come back sooner, until the
                    # deadline above calls it.
                    logger.warn("Lock.renew", {"lock": self._name, "error": str(error)})
                    pause = max(min((self._valid_until - time.monotonic()) / 4, self._renew_every), 0.05)
        except asyncio.CancelledError:
            pass
