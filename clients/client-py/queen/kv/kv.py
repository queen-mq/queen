"""
The key/value API -- ``client.kv``.

PLAN_KV_TIMERS.md §5 (semantics), §8.1 (routes), §18.8 (why the safe form has
to be the SHORT one).

EVERYTHING GOES THROUGH ``POST /api/v1/kv``, and that is a decision.
§8.1 declares the batch route "la superficie completa": it is the only one that
accepts ``getPrefix`` and ``incr``, the only one whose answer is a uniform
index-aligned element, and the only one on which a DELETE can carry an
``expect`` at all. The three path routes are sugar for what people write by
hand with curl. An SDK that used them would have two body shapes, two error
shapes and two places to drift, in exchange for an ETag it cannot use -- the
ETag saves bandwidth, never the round trip to the database (§8.5: nothing in
front of this is cached, and nothing will be).

THE RULE THAT DECIDES HOW TO USE THIS, WORTH READING ONCE (§5.2):

    read-modify-write across two calls is safe ONLY when the KV key derives
    from the partition key. Then the lanes serialise and the key has no other
    writer inside that consumer group. When it does not derive, use the atomics
    (``incr``) or the precondition (``expect``).

And the hierarchy, in this order: THE ACK TRANSACTION IS THE PRIMARY FENCE,
``expect`` is the secondary assertion. A state write that shares the
transaction with its ack is undone when the ack raises on an expired lease --
something a CAS cannot do, because an ``expect`` on a still-matching version
succeeds from a zombie too.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import httpx

from ..errors import KvError, wrap_http_error
from ..utils import logger
from . import ops as _ops

_PATH = "/api/v1/kv"


class KvResult(dict):
    """A KV result element, which is a plain dict plus one thing.

    §10.4 lists "objects are always truthy" as the JavaScript trap with no
    structural defence: ``if (await kv.delete(ns, key))`` is always true there,
    for all five writes. Python has a defence, so it is used --
    ``__bool__`` reads the VERDICT:

        writes  -> ``applied``   ("did it apply?")
        reads   -> ``found``     ("is there a row?")
        timers-shaped answers embedded in a batch -> ``ok``

    Note that this follows ``found`` and never the value, because
    ``{found: true, value: null}`` and ``{found: false}`` are different things
    that no SDK may collapse (§5.5).
    """

    __slots__ = ()

    def __bool__(self) -> bool:
        for field in ("applied", "found", "ok"):
            if field in self:
                return bool(self[field])
        return len(self) > 0


class KV:
    """Key/value operations against one Queen deployment."""

    #: The op builders, exposed so a caller can assemble a batch by hand and
    #: still go through the one place an op is shaped.
    op = _ops

    def __init__(self, http_client: Any) -> None:
        self._http_client = http_client

    # -----------------------------------------------------------------
    # The one path to the server.
    # -----------------------------------------------------------------

    async def batch(self, operations: Iterable[Dict[str, Any]]) -> List[KvResult]:
        """Apply a list of ops in one transaction, in order.

        The answer is INDEX-ALIGNED to the input (§6.4): ``results[i]`` belongs
        to ``operations[i]``, always, and the server raises rather than
        returning a short array.
        """
        op_list = list(operations)
        if not op_list:
            return []
        logger.log("KV.batch", {"count": len(op_list), "ops": [o.get("op") for o in op_list]})
        try:
            response = await self._http_client.post(_PATH, {"operations": op_list})
        except Exception as error:  # noqa: BLE001 - re-raised, possibly re-typed
            raise wrap_http_error(error, KvError) from None
        results = (response or {}).get("results") or []
        return [KvResult(r) if isinstance(r, dict) else r for r in results]

    async def _one(self, op: Dict[str, Any]) -> KvResult:
        results = await self.batch([op])
        if not results:
            raise KvError(
                "the server returned no result for a single-op call",
                request=httpx.Request("POST", _PATH),
                response=httpx.Response(500),
                code="kv_result_missing",
            )
        return results[0]

    # -----------------------------------------------------------------
    # Reads.
    # -----------------------------------------------------------------

    async def get(self, ns: str, key: str) -> KvResult:
        """``{found, key, value, version, expiresAt, updatedAt}``.

        A key past its expiry is NEVER returned and never counts as existing,
        even when the sweeper has not pruned it yet (§5.7). The truth is the
        predicate, not the presence of the row.
        """
        return await self._one(_ops.get(ns, key))

    async def get_many(self, ns: str, keys: Iterable[str]) -> KvResult:
        """``{rows, missing}`` -- rows, never a key->value map.

        ``missing`` is explicit because absence must be a DATUM and not a hole
        the caller computes by difference (§5.5). Keys dropped by the server's
        byte budget appear in neither list, and ``truncated`` says so: calling
        them absent would be a lie.
        """
        return await self._one(_ops.get_many(ns, keys))

    async def get_prefix(
        self,
        ns: str,
        prefix: str,
        *,
        after: Optional[str] = None,
        limit: Optional[int] = None,
        keys_only: bool = False,
    ) -> KvResult:
        """``{rows, truncated, nextAfter}``.

        Only expressible on this route, never as a query string: ``?prefix=
        quota:acme:`` would be recorded by the broker's access log, the proxy's,
        the meter sample, the per-request-id tracing and any ingress in front,
        and a mitigation living in one component out of four is not a
        mitigation (§5.5).

        EVERY PAGE IS ITS OWN SNAPSHOT. With ``after`` it may miss a key
        inserted behind the cursor. Good for compacting state, not for an exact
        count.
        """
        return await self._one(_ops.get_prefix(ns, prefix, after=after, limit=limit, keys_only=keys_only))

    async def list_all(
        self,
        ns: str,
        prefix: str,
        *,
        limit: Optional[int] = None,
        keys_only: bool = False,
        max_rows: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Follow ``nextAfter`` until the prefix is exhausted, and return the rows.

        This is a convenience over a series of independent snapshots, NOT a
        consistent listing -- the caveat of ``get_prefix`` applies to the whole
        walk and more so. ``max_rows`` is there because the caller is the only
        one who knows how much of somebody else's namespace it is willing to
        pull into memory.
        """
        rows: List[Dict[str, Any]] = []
        after: Optional[str] = None
        while True:
            page = await self.get_prefix(ns, prefix, after=after, limit=limit, keys_only=keys_only)
            rows.extend(page.get("rows") or [])
            if max_rows is not None and len(rows) >= max_rows:
                return rows[:max_rows]
            if not page.get("truncated"):
                return rows
            after = page.get("nextAfter")
            if not after:
                # `truncated` without a cursor cannot be walked further; stop
                # rather than loop forever on the same page.
                return rows

    # -----------------------------------------------------------------
    # Writes.
    # -----------------------------------------------------------------

    async def put(
        self,
        ns: str,
        key: str,
        value: Any,
        *,
        ttl_seconds: Any = _ops.UNSET,
        forever: Any = _ops.UNSET,
        ttl: Any = _ops.UNSET,
        until: Any = _ops.UNSET,
        expect: Any = _ops.UNSET,
        required: bool = False,
    ) -> KvResult:
        """Upsert, or a fenced write when ``expect`` is given.

        ``expect=0`` means "must not exist" and wins even against an expired
        row that has not been pruned. ``expect=N>0`` is a pure update that
        creates NOTHING when it matches no row.

        The returned element carries the CURRENT value and version even when it
        did not apply, so the loser needs no second round trip. That version is
        ADVISORY -- never a fencing token to reuse blindly (§5.3).
        """
        return await self._one(
            _ops.put(ns, key, value, ttl_seconds=ttl_seconds, forever=forever, ttl=ttl, until=until, expect=expect, required=required)
        )

    async def put_if_absent(
        self,
        ns: str,
        key: str,
        value: Any,
        *,
        ttl_seconds: Any = _ops.UNSET,
        forever: Any = _ops.UNSET,
        ttl: Any = _ops.UNSET,
        until: Any = _ops.UNSET,
        required: bool = False,
    ) -> KvResult:
        """"Must not exist", with ``applied`` answering "did I win?".

        Exactly one of N concurrent callers applies: Postgres takes the row
        lock BEFORE evaluating the condition, so the second re-evaluates
        against the new row.

        And the sentence that has to travel with this method: **putIfAbsent
        plus a TTL is not a distributed lock.** A lock that expires is not
        revoked -- the old holder keeps working, it simply no longer has the
        row. The defence is fencing: carry your ``version`` as ``expect`` on
        every later write, so a lapsed holder's writes fail with
        ``reason:"version"`` instead of overwriting the new holder's. That
        limits the damage; it does not remove it (§5.7).
        """
        return await self._one(
            _ops.put_if_absent(ns, key, value, ttl_seconds=ttl_seconds, forever=forever, ttl=ttl, until=until, required=required)
        )

    async def delete(
        self,
        ns: str,
        key: str,
        *,
        expect: Any = _ops.UNSET,
        required: bool = False,
    ) -> KvResult:
        """Delete, idempotent: a key that was not there answers
        ``applied:false, reason:'absent'`` with HTTP 200."""
        return await self._one(_ops.delete(ns, key, expect=expect, required=required))

    async def incr(
        self,
        ns: str,
        key: str,
        *,
        delta: Any = 1,
        min: Any = None,
        max: Any = None,
        ttl_seconds: Any = _ops.UNSET,
        forever: Any = _ops.UNSET,
        ttl: Any = _ops.UNSET,
        until: Any = _ops.UNSET,
        required: bool = False,
    ) -> KvResult:
        """Atomic add, with optional floor and ceiling. See ``kv/ops.py``.

        With ``max``, ``applied`` IS the admission decision: nothing saturates
        and nothing truncates, so a refused increment has spent no budget.
        """
        return await self._one(
            _ops.incr(ns, key, delta=delta, min=min, max=max, ttl_seconds=ttl_seconds, forever=forever, ttl=ttl, until=until, required=required)
        )

    # -----------------------------------------------------------------
    # The gate (§18.8).
    # -----------------------------------------------------------------

    async def once(
        self,
        ns: str,
        key: str,
        *,
        ttl_seconds: Any = _ops.UNSET,
        forever: Any = _ops.UNSET,
        ttl: Any = _ops.UNSET,
        until: Any = _ops.UNSET,
        value: Any = True,
    ) -> bool:
        """``True`` if this caller is the one that claimed ``key``.

        The whole feature stands on the safe form being shorter to write than
        the unsafe one (§18.8), and this is that form: one call, one boolean,
        no version to carry and no CAS loop to get wrong.

        For an effect that must be atomic with an ack or a push, use
        ``tx.once(...)`` instead: the transaction is the fence, and this
        standalone call is not part of it.
        """
        res = await self.put_if_absent(ns, key, value, ttl_seconds=ttl_seconds, forever=forever, ttl=ttl, until=until)
        return bool(res.get("applied"))
