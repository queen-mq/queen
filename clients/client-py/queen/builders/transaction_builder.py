"""
Transaction builder for atomic operations
"""

from typing import Any, Dict, List, Optional, Union

from ..kv import ops as kv_ops
from ..timers import ops as timer_ops
from ..utils import logger
from ..utils.uuid_gen import generate_uuid
from ..utils.validation import is_valid_uuid


class TransactionResult(dict):
    """The transaction's answer, which is a plain dict plus a truthful ``bool``.

    ``commit()`` now has an outcome that is NOT an exception and NOT a success
    (see :meth:`TransactionBuilder.commit`), so ``if await tx.commit():`` had to
    stop meaning "an object came back". It reads ``success``.
    """

    __slots__ = ()

    def __bool__(self) -> bool:
        if "success" in self:
            return bool(self["success"])
        return len(self) > 0


class TransactionBuilder:
    """Transaction builder for atomic operations"""

    def __init__(self, http_client: Any):  # Type: HttpClient
        """
        Initialize transaction builder

        Args:
            http_client: HttpClient instance
        """
        self._http_client = http_client
        self._operations: List[Dict[str, Any]] = []
        self._required_leases: List[str] = []
        # PLAN_KV_TIMERS.md §6.3, §8.2, §10.4: these two ride as TOP-LEVEL
        # fields of the request, beside `operations` and never inside it. The
        # reason is a Go failure -- two struct fields carrying the same JSON key
        # at the same level are BOTH dropped by encoding/json, with no error and
        # no warning, so a body would go out with zero kv ops while the broker
        # committed a transaction whose gate never existed. The shape is one
        # shape for all seven clients, so this client holds to it as well.
        #
        # The server maps them into the flat `results[]` space with an
        # APPEND-ONLY layout: `[0, ops)` is `operations` exactly as today,
        # then kv, then timers. A push or an ack never changes index because a
        # rider is present, and a bundle with neither array produces exactly
        # today's request and exactly today's results.
        self._kv_ops: List[Dict[str, Any]] = []
        self._timer_ops: List[Dict[str, Any]] = []
        # index -> thunk, for the kv ops whose expiry was given as an INSTANT
        # (`until=`). See _materialize_kv().
        self._kv_deferred: Dict[int, Any] = {}

    def ack(
        self,
        messages: Union[Any, List[Any]],
        status: str = "completed",
        context: Optional[Dict[str, Any]] = None,
    ) -> "TransactionBuilder":
        """
        Add ack operation

        Args:
            messages: Single message or list of messages
            status: Status to ack with ('completed', 'failed', etc.)
            context: Optional context dict with 'consumer_group' key for consumer group acking

        Returns:
            Self for chaining
        """
        msgs = messages if isinstance(messages, list) else [messages]
        context = context or {}

        logger.log(
            "TransactionBuilder.ack",
            {"count": len(msgs), "status": status, "consumer_group": context.get("consumer_group")},
        )

        for msg in msgs:
            transaction_id = msg if isinstance(msg, str) else (msg.get("transactionId") or msg.get("id"))
            partition_id = msg.get("partitionId") if isinstance(msg, dict) else None
            lease_id = msg.get("leaseId") if isinstance(msg, dict) else None

            if not transaction_id:
                raise ValueError("Message must have transactionId or id property")

            # CRITICAL: partitionId is now MANDATORY to prevent acking wrong message
            if not partition_id:
                raise ValueError(
                    "Message must have partitionId property to ensure message uniqueness"
                )

            operation: Dict[str, Any] = {
                "type": "ack",
                "transactionId": transaction_id,
                "partitionId": partition_id,
                "status": status,
            }

            # Add consumerGroup if provided in context
            if context.get("consumer_group"):
                operation["consumerGroup"] = context["consumer_group"]

            self._operations.append(operation)

            if lease_id:
                self._required_leases.append(lease_id)

        return self

    def queue(self, queue_name: str) -> "TransactionQueueBuilder":
        """
        Add push operation to queue

        Args:
            queue_name: Queue name

        Returns:
            TransactionQueueBuilder for push operations
        """
        return TransactionQueueBuilder(self, queue_name)

    # ===========================
    # KV and timer riders (PLAN_KV_TIMERS.md §6.3, §8.2)
    # ===========================

    @property
    def kv(self) -> "TransactionKvBuilder":
        """KV operations inside this transaction.

        THE HIERARCHY, WHICH IS THE REASON TO PUT THEM HERE AT ALL (§5.2): the
        ack transaction is the PRIMARY fence and ``expect`` is the secondary
        assertion. A state write that shares the transaction with its ack is
        undone when an expired lease makes the ack raise -- something a CAS
        cannot do, because an ``expect`` on a still-matching version succeeds
        from a zombie too.
        """
        return TransactionKvBuilder(self)

    def once(
        self,
        ns: str,
        key: str,
        *,
        ttl_seconds: Any = kv_ops.UNSET,
        forever: Any = kv_ops.UNSET,
        ttl: Any = kv_ops.UNSET,
        until: Any = kv_ops.UNSET,
        value: Any = True,
    ) -> "TransactionBuilder":
        """The gate: this bundle applies only if nobody claimed ``key`` first.

        It is a ``putIfAbsent`` with ``required=True``, and ``required`` is the
        whole point -- without it the marker loses its race and the bundle
        commits anyway, i.e. the gate was decoration. With it the transaction
        rolls back in SQL and ``commit()`` RETURNS
        ``{success: False, reason: "kv_precondition", ...}``.

        Put it FIRST in the bundle. It costs one row lock held for the length of
        the transaction (accepted risk §18.2), so gate on a key derived from the
        message, never on a shared one.
        """
        return _append_kv(
            self,
            kv_ops.put_if_absent(ns, key, value, ttl_seconds=ttl_seconds, forever=forever, ttl=ttl, until=until, required=True),
            _rebuilder(kv_ops.put_if_absent, ns, key, value, ttl_seconds=ttl_seconds, forever=forever, ttl=ttl, until=until, required=True),
        )

    def timer(self, queue: str) -> "TransactionTimerBuilder":
        """Schedule or cancel a timer as part of this transaction."""
        return TransactionTimerBuilder(self, queue)

    def _materialize_kv(self) -> List[Dict[str, Any]]:
        """Re-resolve the ops whose expiry was expressed as an INSTANT.

        ``until=<datetime>`` is sugar for a delta, and the delta has to be
        measured at SEND time. Freezing it when the op was queued would ship a
        TTL already stale by however long the bundle took to assemble -- which
        for a bundle assembled around a pop is not a rounding error.

        Only ops that used ``until`` are rebuilt, and they are rebuilt by the
        same ``queen.kv.ops`` function that built them the first time, so the
        eager build still does all the validation at the CALL SITE (where the
        traceback is useful) and nothing here can produce an op shape the
        standalone surface could not.
        """
        out: List[Dict[str, Any]] = []
        for index, op in enumerate(self._kv_ops):
            rebuild = self._kv_deferred.get(index)
            out.append(rebuild() if rebuild is not None else op)
        return out

    async def commit(self) -> TransactionResult:
        """
        Commit transaction

        Returns:
            Transaction response. **This can be a FAILURE that is returned and
            not raised**: a lost KV precondition answers HTTP 200 with
            ``{"success": false, "reason": "kv_precondition", "failedIndex",
            "kvReason", "version", "value"}`` and is handed straight back
            (PLAN_KV_TIMERS.md §8.3, §10.2).

            That is not a special case grudgingly made -- it is the expected
            outcome of every legitimate redelivery. Raising would put the
            product's single most frequent outcome into the caller's error path,
            its retry policy and its error metrics. Everything the loser needs
            is in the body, so no second round trip is required.

            Every OTHER failure still raises.

        Raises:
            Exception: If the transaction failed for any reason other than a
                lost KV precondition.
        """
        if not self._operations and not self._kv_ops and not self._timer_ops:
            logger.error("TransactionBuilder.commit", "No operations to commit")
            raise Exception("Transaction has no operations to commit")

        logger.log(
            "TransactionBuilder.commit",
            {
                "operation_count": len(self._operations),
                "required_leases": len(self._required_leases),
                "kv_count": len(self._kv_ops),
                "timer_count": len(self._timer_ops),
            },
        )

        body: Dict[str, Any] = {
            "operations": self._operations,
            "requiredLeases": list(set(self._required_leases)),  # Unique leases
        }
        # Omitted entirely when unused, not sent as [] or null: a bundle that
        # carries neither rider must put exactly today's bytes on the wire, or
        # the compatibility argument of §6.3 is only half made.
        if self._kv_ops:
            body["kv"] = self._materialize_kv()
        if self._timer_ops:
            body["timers"] = self._timer_ops

        try:
            result = await self._http_client.post("/api/v1/transaction", body)

            if not result.get("success"):
                # §8.3: the ONE outcome that comes back instead of being
                # thrown. Keyed on the machine-readable `reason`, never on the
                # message -- string matching on error prose is forbidden
                # everywhere in this codebase.
                if result.get("reason") == "kv_precondition":
                    logger.log(
                        "TransactionBuilder.commit",
                        {
                            "status": "kv_precondition",
                            "failed_index": result.get("failedIndex"),
                            "kv_reason": result.get("kvReason"),
                        },
                    )
                    return TransactionResult(result)
                logger.error("TransactionBuilder.commit", {"error": result.get("error")})
                raise Exception(result.get("error") or "Transaction failed")

            logger.log("TransactionBuilder.commit", {"status": "success"})
            return TransactionResult(result)
        except Exception as error:
            logger.error("TransactionBuilder.commit", {"error": str(error)})
            raise


class TransactionKvBuilder:
    """KV leg of a transaction. Every method returns the parent builder, the
    same way ``TransactionQueueBuilder.push`` does.

    The ops are built by ``queen.kv.ops``, i.e. by the same functions the
    standalone ``client.kv`` surface uses, so the two wires cannot drift.
    """

    def __init__(self, transaction_builder: TransactionBuilder) -> None:
        self._tx = transaction_builder

    def _add(self, op: Dict[str, Any], rebuild: Any = None) -> TransactionBuilder:
        return _append_kv(self._tx, op, rebuild)

    def get(self, ns: str, key: str) -> TransactionBuilder:
        return self._add(kv_ops.get(ns, key))

    def get_many(self, ns: str, keys: Any) -> TransactionBuilder:
        return self._add(kv_ops.get_many(ns, keys))

    def get_prefix(self, *args: Any, **kwargs: Any) -> TransactionBuilder:
        """Not expressible here, on purpose (§5.5).

        A prefix read is read work whose cost the CALLER does not bound, inside
        the transaction that holds the outermost lock space of the product and,
        downstream, the partition locks. ``get`` and ``getMany`` are allowed
        because the caller fixes their cost. The boundary is COST, not the kind
        of operation. The broker raises 22023 on it; refusing here is the same
        rule, one round trip earlier.
        """
        raise ValueError(
            "getPrefix is not allowed inside a transaction: its cost is not bounded by the "
            "caller, and the bundle holds the outermost lock space of the product. Use "
            "client.kv.get_prefix(...) outside the transaction, or tx.kv.get_many(...) with "
            "explicit keys"
        )

    def put(self, ns: str, key: str, value: Any, **kwargs: Any) -> TransactionBuilder:
        return self._add(kv_ops.put(ns, key, value, **kwargs), _rebuilder(kv_ops.put, ns, key, value, **kwargs))

    def put_if_absent(self, ns: str, key: str, value: Any, **kwargs: Any) -> TransactionBuilder:
        return self._add(
            kv_ops.put_if_absent(ns, key, value, **kwargs),
            _rebuilder(kv_ops.put_if_absent, ns, key, value, **kwargs),
        )

    def delete(self, ns: str, key: str, **kwargs: Any) -> TransactionBuilder:
        return self._add(kv_ops.delete(ns, key, **kwargs))

    def incr(self, ns: str, key: str, **kwargs: Any) -> TransactionBuilder:
        return self._add(kv_ops.incr(ns, key, **kwargs), _rebuilder(kv_ops.incr, ns, key, **kwargs))


def _rebuilder(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """A thunk that rebuilds the op at send time, but ONLY when the expiry was
    an instant. Everything else is already a delta and does not age."""
    if kv_ops.is_unset(kwargs.get("until", kv_ops.UNSET)):
        return None
    return lambda: fn(*args, **kwargs)


def _append_kv(tx: TransactionBuilder, op: Dict[str, Any], rebuild: Any = None) -> TransactionBuilder:
    if rebuild is not None:
        tx._kv_deferred[len(tx._kv_ops)] = rebuild
    tx._kv_ops.append(op)
    return tx


class TransactionTimerBuilder:
    """Timer leg of a transaction. The terminals are SYNCHRONOUS and return the
    parent builder -- the work happens at ``commit()``, which is the point.
    """

    def __init__(self, transaction_builder: TransactionBuilder, queue: str) -> None:
        self._tx = transaction_builder
        self._queue = queue
        self._key: Optional[str] = None
        self._payload: Any = None
        self._has_payload = False
        self._delay_ms: Optional[Any] = None
        self._delay: Optional[Any] = None
        self._partition: Optional[str] = None
        self._txn: Optional[str] = None
        self._payload_zstd = False

    def key(self, timer_key: str) -> "TransactionTimerBuilder":
        self._key = timer_key
        return self

    def payload(self, payload: Any, *, zstd: bool = False) -> "TransactionTimerBuilder":
        self._payload = payload
        self._has_payload = True
        self._payload_zstd = zstd
        return self

    def after_ms(self, delay_ms: Any) -> "TransactionTimerBuilder":
        self._delay_ms = delay_ms
        return self

    def after(self, delay: Any) -> "TransactionTimerBuilder":
        self._delay = delay
        return self

    def partition(self, partition: str) -> "TransactionTimerBuilder":
        self._partition = partition
        return self

    def txn(self, txn: str) -> "TransactionTimerBuilder":
        self._txn = txn
        return self

    def _require_key(self) -> str:
        if not self._key:
            raise ValueError("timer needs .key(<timerKey>)")
        return self._key

    def schedule(self) -> TransactionBuilder:
        if not self._has_payload:
            raise ValueError("timer needs .payload(<data>)")
        self._tx._timer_ops.append(
            timer_ops.schedule(
                self._queue, self._require_key(), self._payload,
                delay_ms=self._delay_ms, delay=self._delay, partition=self._partition,
                txn=self._txn, payload_zstd=self._payload_zstd,
            )
        )
        return self._tx

    def reschedule(self) -> TransactionBuilder:
        if not self._has_payload:
            raise ValueError("timer needs .payload(<data>)")
        self._tx._timer_ops.append(
            timer_ops.schedule(
                self._queue, self._require_key(), self._payload,
                delay_ms=self._delay_ms, delay=self._delay, partition=self._partition,
                txn=self._txn, payload_zstd=self._payload_zstd, reschedule=True,
            )
        )
        return self._tx

    def cancel(self) -> TransactionBuilder:
        """Cancel as part of this transaction.

        Inside a bundle there is no separate route to take, and being part of
        the transaction is exactly why it is here. The STANDALONE cancel
        (``client.timers.cancel``) uses the DELETE route that is never
        blockable (§9.6) -- if that property is what you need, do not put the
        cancel in a bundle.
        """
        self._tx._timer_ops.append(timer_ops.cancel(self._queue, self._require_key(), txn=self._txn))
        return self._tx


class TransactionQueueBuilder:
    """Sub-builder for push operations in transaction"""

    def __init__(self, transaction_builder: TransactionBuilder, queue_name: str):
        """
        Initialize transaction queue builder

        Args:
            transaction_builder: Parent TransactionBuilder
            queue_name: Queue name
        """
        self._transaction_builder = transaction_builder
        self._queue_name = queue_name
        self._partition: Optional[str] = None

    def partition(self, partition_key: str) -> "TransactionQueueBuilder":
        """
        Set partition for push

        Args:
            partition_key: Partition key

        Returns:
            Self for chaining
        """
        self._partition = partition_key
        return self

    def push(self, items: Union[Dict[str, Any], List[Dict[str, Any]]]) -> TransactionBuilder:
        """
        Add messages to transaction

        Args:
            items: Single item or list of items

        Returns:
            Parent TransactionBuilder for chaining
        """
        item_array = items if isinstance(items, list) else [items]

        logger.log(
            "TransactionBuilder.queue.push",
            {"queue": self._queue_name, "partition": self._partition, "count": len(item_array)},
        )

        formatted_items = []
        for item in item_array:
            # Check if property exists, not just truthy (to support null values)
            if "data" in item:
                payload_value = item["data"]
            elif "payload" in item:
                payload_value = item["payload"]
            else:
                payload_value = item

            # Same contract as QueueBuilder.push: the caller's transactionId is
            # what makes a retried transaction idempotent inside the dedup
            # window, so it has to reach the wire. Absent, mint one here rather
            # than leaving the broker to do it, so the id is knowable client
            # side either way.
            result: Dict[str, Any] = {
                "queue": self._queue_name,
                "payload": payload_value,
                "transactionId": item.get("transactionId") or generate_uuid(),
            }

            # Add partition if set
            if self._partition is not None:
                result["partition"] = self._partition

            trace_id = item.get("traceId")
            if trace_id and is_valid_uuid(trace_id):
                result["traceId"] = trace_id

            formatted_items.append(result)

        self._transaction_builder._operations.append({"type": "push", "items": formatted_items})

        return self._transaction_builder

