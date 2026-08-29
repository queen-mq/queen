"""
Consumer manager for handling concurrent workers
"""

import asyncio
import time
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlencode

from ..errors import ConflationUnsupportedError
from ..utils import logger
from ..utils.autopilot import (
    empty_poll_delay_seconds,
    parse_autopilot_decision,
    pop_sizing,
)
from ..utils.conflation import check_pop_response as check_conflation
from ..utils.conflation import scope_of as conflation_scope
from ..utils.defaults import CONSUME_DEFAULTS


class ConsumerManager:
    """Consumer manager for handling concurrent workers"""

    def __init__(self, http_client: Any, queen: Any):
        """
        Initialize consumer manager

        Args:
            http_client: HttpClient instance
            queen: Queen instance
        """
        self._http_client = http_client
        self._queen = queen

    async def start(self, handler: Callable[[Any], Any], options: Dict[str, Any]) -> None:
        """
        Start consumer workers

        Args:
            handler: Message handler
            options: Consume options
        """
        queue = options.get("queue")
        partition = options.get("partition")
        namespace = options.get("namespace")
        task = options.get("task")
        group = options.get("group")
        concurrency = options.get("concurrency", 1)
        batch = options.get("batch")
        limit = options.get("limit")
        idle_millis = options.get("idle_millis")
        auto_ack = options.get("auto_ack", True)
        wait = options.get("wait", True)
        timeout_millis = options.get("timeout_millis", 30000)
        renew_lease = options.get("renew_lease", False)
        renew_lease_interval_millis = options.get("renew_lease_interval_millis")
        subscription_mode = options.get("subscription_mode")
        subscription_from = options.get("subscription_from")
        conflation = bool(options.get("conflation", False))
        each = options.get("each", False)
        # None means the user said nothing about this dimension, which is what
        # pop autopilot acts on. The historical default of 1 is applied at
        # emission time instead (utils/autopilot.py), and only when autopilot is
        # off, so that "never called partitions()" and "called partitions(1)"
        # stay distinguishable all the way to the wire.
        max_partitions = options.get("max_partitions")
        autopilot = self._autopilot_enabled(options.get("autopilot"))
        signal = options.get("signal")

        logger.log(
            "ConsumerManager.start",
            {
                "queue": queue,
                "partition": partition,
                "namespace": namespace,
                "task": task,
                "group": group,
                "concurrency": concurrency,
                "batch": batch,
                "limit": limit,
                "auto_ack": auto_ack,
                "wait": wait,
                "each": each,
            },
        )

        # Build the path and params for pop requests
        path = self._build_path(queue, partition, namespace, task)
        base_params = self._build_params(
            batch, wait, timeout_millis, group, subscription_mode, subscription_from, namespace, task, max_partitions,
            conflation, autopilot,
        )

        # Generate affinity key for consistent routing to same backend
        affinity_key = self._get_affinity_key(queue, partition, namespace, task, group)

        # Start workers
        worker_options = {
            "batch": batch,
            "limit": limit,
            "idle_millis": idle_millis,
            "auto_ack": auto_ack,
            "wait": wait,
            "timeout_millis": timeout_millis,
            "renew_lease": renew_lease,
            "renew_lease_interval_millis": renew_lease_interval_millis,
            "each": each,
            "signal": signal,
            "group": group,
            "affinity_key": affinity_key,
            "conflation": conflation,
            "conflation_scope": conflation_scope(queue, namespace, task),
        }

        workers = [
            self._worker(i, handler, path, base_params, worker_options) for i in range(concurrency)
        ]

        logger.log("ConsumerManager.start", {"status": "workers-started", "count": concurrency})

        # Wait for all workers to complete
        await asyncio.gather(*workers)

        logger.log("ConsumerManager.start", {"status": "completed"})

    async def _worker(
        self,
        worker_id: int,
        handler: Callable[[Any], Any],
        path: str,
        base_params: str,
        options: Dict[str, Any],
    ) -> None:
        """Worker loop"""
        batch = options["batch"]
        limit = options["limit"]
        idle_millis = options["idle_millis"]
        auto_ack = options["auto_ack"]
        wait = options["wait"]
        timeout_millis = options["timeout_millis"]
        renew_lease = options["renew_lease"]
        renew_lease_interval_millis = options["renew_lease_interval_millis"]
        each = options["each"]
        signal = options["signal"]
        group = options["group"]
        affinity_key = options["affinity_key"]
        conflation = options.get("conflation", False)
        conflation_scope_name = options.get("conflation_scope")

        logger.log(
            "ConsumerManager.worker",
            {"worker_id": worker_id, "status": "started", "limit": limit, "idle_millis": idle_millis},
        )

        processed_count = 0
        last_message_time = time.time() if idle_millis else None

        while True:
            # Check abort signal
            if signal and signal.is_set():
                logger.log(
                    "ConsumerManager.worker",
                    {"worker_id": worker_id, "status": "aborted", "processed_count": processed_count},
                )
                break

            # Check limit
            if limit and processed_count >= limit:
                logger.log(
                    "ConsumerManager.worker",
                    {
                        "worker_id": worker_id,
                        "status": "limit-reached",
                        "processed_count": processed_count,
                        "limit": limit,
                    },
                )
                break

            # Check idle timeout
            if idle_millis and last_message_time:
                idle_time = (time.time() - last_message_time) * 1000
                if idle_time >= idle_millis:
                    logger.log(
                        "ConsumerManager.worker",
                        {
                            "worker_id": worker_id,
                            "status": "idle-timeout",
                            "processed_count": processed_count,
                            "idle_time": idle_time,
                        },
                    )
                    break

            try:
                # Pop messages with affinity key for consistent routing.
                # wait=True is a long-poll: mark it "pop" so a 429 backs off
                # and keeps waiting instead of giving up after the bounded
                # push-like attempt budget.
                client_timeout = timeout_millis + 5000 if wait else timeout_millis
                result = await self._http_client.get(
                    f"{path}?{base_params}", client_timeout, affinity_key,
                    retry_kind="pop" if wait else None,
                )

                # PLAN_CONFLATION §4: prove the broker applied the policy this
                # consumer declared, BEFORE the empty-response branch below --
                # the echo rides empty pops too, so an old broker is caught on
                # the very first round trip rather than after the loop has
                # quietly drained a backlog one message at a time.
                check_conflation(
                    result,
                    requested=conflation,
                    queue=conflation_scope_name,
                    group=group,
                )

                # Handle empty response
                if not result or not result.get("messages") or not result["messages"]:
                    if wait:
                        continue  # Long polling timeout, retry
                    else:
                        # Short delay before retry -- the broker's advised
                        # pacing when this pop engaged autopilot and the broker
                        # had an opinion (it knows the arrival rate on this
                        # queue and this client does not), otherwise the
                        # historical 100ms.
                        await asyncio.sleep(empty_poll_delay_seconds(parse_autopilot_decision(result)))
                        continue

                messages = [msg for msg in result["messages"] if msg is not None]

                if not messages:
                    continue

                logger.log(
                    "ConsumerManager.worker",
                    {"worker_id": worker_id, "status": "messages-received", "count": len(messages)},
                )

                # Enhance messages with trace() method
                self._enhance_messages_with_trace(messages, group)

                # Update last message time
                if idle_millis:
                    last_message_time = time.time()

                # Set up lease renewal if enabled
                renewal_task = None
                if renew_lease and renew_lease_interval_millis:
                    renewal_task = self._setup_lease_renewal(messages, renew_lease_interval_millis)

                try:
                    # Process messages
                    if each:
                        # Process one at a time
                        for idx, message in enumerate(messages):
                            if signal and signal.is_set():
                                break

                            ok = await self._process_message(message, handler, auto_ack, group)
                            processed_count += 1

                            # A nack releases the lease and clamps the server
                            # cursor at the failed message: everything after it
                            # in this popped batch WILL be redelivered.
                            # Processing it now would only produce duplicates
                            # and rejected acks — abandon the rest of the batch.
                            if auto_ack and not ok:
                                logger.warn(
                                    "ConsumerManager.worker",
                                    {
                                        "worker_id": worker_id,
                                        "status": "batch-abandoned-after-nack",
                                        "remaining": len(messages) - idx - 1,
                                    },
                                )
                                break

                            if limit and processed_count >= limit:
                                break
                    else:
                        # Process as batch (or single message if batch=1)
                        if batch == 1 and len(messages) == 1:
                            # For batch=1, pass single message (not array)
                            await self._process_message(messages[0], handler, auto_ack, group)
                            processed_count += 1
                        else:
                            # For batch>1, pass array of messages
                            await self._process_batch(messages, handler, auto_ack, group)
                            processed_count += len(messages)

                    logger.log(
                        "ConsumerManager.worker",
                        {
                            "worker_id": worker_id,
                            "status": "messages-processed",
                            "count": len(messages),
                            "total": processed_count,
                        },
                    )
                finally:
                    # Clear renewal task
                    if renewal_task:
                        renewal_task.cancel()
                        try:
                            await renewal_task
                        except asyncio.CancelledError:
                            pass

            except ConflationUnsupportedError:
                # Terminal, and deliberately ahead of every retry branch below.
                # The broker will answer exactly the same way on the next poll,
                # so retrying is an infinite loop that silently processes the
                # backlog message by message -- the failure §4 exists to make
                # impossible. Stop the loop and surface it to the caller.
                logger.error(
                    "ConsumerManager.worker",
                    {"worker_id": worker_id, "status": "conflation-unsupported"},
                )
                raise
            except Exception as error:
                # Check if this is a timeout error (expected for long polling)
                error_str = str(error)
                is_timeout_error = "timeout" in error_str.lower() or "timed out" in error_str.lower()

                if is_timeout_error and wait:
                    continue  # Retry on timeout

                status_code = getattr(getattr(error, "response", None), "status_code", None)

                # 429 (rate limited): HttpClient already retries this
                # internally with backoff (unbounded for wait=True pop, per
                # the retry_429 policy) -- this branch is a defensive
                # fallback for the case where an explicit
                # retry_429["max_attempts"] override got exhausted. Back off
                # and keep polling instead of hot-looping.
                if status_code == 429:
                    retry_after = getattr(error, "retry_after_seconds", None)
                    delay = retry_after if isinstance(retry_after, (int, float)) and retry_after >= 0 else 1.0
                    logger.warn(
                        "ConsumerManager.worker",
                        {
                            "worker_id": worker_id,
                            "status": "rate-limited",
                            "code": getattr(error, "code", None),
                            "retry_delay_s": delay,
                        },
                    )
                    await asyncio.sleep(delay)
                    continue

                # Check if network error
                is_network_error = (
                    "fetch failed" in error_str
                    or "ECONNREFUSED" in error_str
                    or "connection" in error_str.lower()
                )

                if is_network_error:
                    logger.warn(
                        "ConsumerManager.worker",
                        {"worker_id": worker_id, "error": "network", "message": str(error)},
                    )
                    print(f"Worker {worker_id}: Network error - {error}")
                    # Wait before retry
                    await asyncio.sleep(1)
                    continue

                # 403 (forbidden): terminal. cluster_suspended in particular
                # can never resolve itself, and none of the other proxy
                # codes (storage_quota_exceeded / feature_gated / forbidden)
                # are worth hot-looping either -- stop this worker and
                # surface the error (with .code) to the caller instead of
                # retrying.
                if status_code == 403:
                    logger.error(
                        "ConsumerManager.worker",
                        {"worker_id": worker_id, "status": "forbidden", "code": getattr(error, "code", None)},
                    )
                    raise

                # Other errors - rethrow
                logger.error("ConsumerManager.worker", {"worker_id": worker_id, "error": str(error)})
                raise

        logger.log(
            "ConsumerManager.worker",
            {"worker_id": worker_id, "status": "stopped", "processed_count": processed_count},
        )

    async def _process_message(
        self, message: Dict[str, Any], handler: Callable[[Any], Any], auto_ack: bool, group: Optional[str]
    ) -> bool:
        """Process single message.

        Returns True when the message was handled (and acked) successfully,
        False when it was nacked — the caller must abandon the rest of the
        popped batch (the nack released the lease server-side).
        """
        try:
            await handler(message)

            # Auto-ack on success if enabled
            if auto_ack:
                context = {"group": group} if group else {}
                res = await self._queen.ack(message, True, context)
                if isinstance(res, dict) and res.get("success") is False:
                    logger.error(
                        "ConsumerManager.processMessage",
                        {
                            "transaction_id": message.get("transactionId"),
                            "status": "ack-rejected",
                            "error": res.get("error"),
                        },
                    )
                else:
                    logger.log(
                        "ConsumerManager.processMessage",
                        {"transaction_id": message.get("transactionId"), "status": "acked"},
                    )
            return True
        except Exception as error:
            # Auto-nack on error if enabled
            if auto_ack:
                context = {"group": group, "error": str(error)} if group else {"error": str(error)}
                res = await self._queen.ack(message, False, context)
                if isinstance(res, dict) and res.get("success") is False:
                    logger.error(
                        "ConsumerManager.processMessage",
                        {
                            "transaction_id": message.get("transactionId"),
                            "status": "nack-rejected",
                            "error": res.get("error"),
                        },
                    )
                logger.error(
                    "ConsumerManager.processMessage",
                    {
                        "transaction_id": message.get("transactionId"),
                        "error": str(error),
                        "status": "nacked",
                    },
                )
                # Don't rethrow when autoAck is enabled - NACK was already sent
                # This allows the consumer to continue and retry
                return False
            logger.error(
                "ConsumerManager.processMessage",
                {"transaction_id": message.get("transactionId"), "error": str(error)},
            )
            raise

    async def _process_batch(
        self,
        messages: List[Dict[str, Any]],
        handler: Callable[[Any], Any],
        auto_ack: bool,
        group: Optional[str],
    ) -> None:
        """Process batch of messages"""
        try:
            await handler(messages)

            # Auto-ack on success if enabled
            if auto_ack:
                context = {"group": group} if group else {}
                res = await self._queen.ack(messages, True, context)
                if isinstance(res, dict) and res.get("success") is False:
                    logger.error(
                        "ConsumerManager.processBatch",
                        {"count": len(messages), "status": "ack-rejected", "error": res.get("error")},
                    )
                else:
                    logger.log("ConsumerManager.processBatch", {"count": len(messages), "status": "acked"})
        except Exception as error:
            # Auto-nack on error if enabled
            if auto_ack:
                context = {"group": group, "error": str(error)} if group else {"error": str(error)}
                await self._queen.ack(messages, False, context)
                logger.error(
                    "ConsumerManager.processBatch",
                    {"count": len(messages), "error": str(error), "status": "nacked"},
                )
                # Don't rethrow when autoAck is enabled - NACK was already sent
                # This allows the consumer to continue and retry
                return
            logger.error("ConsumerManager.processBatch", {"count": len(messages), "error": str(error)})
            raise

    def _setup_lease_renewal(
        self, messages: List[Dict[str, Any]], interval_millis: int
    ) -> asyncio.Task[None]:
        """Setup lease renewal task"""
        lease_ids = [m.get("leaseId") for m in messages if m.get("leaseId")]

        if not lease_ids:
            return None  # type: ignore

        async def renew_loop() -> None:
            while True:
                await asyncio.sleep(interval_millis / 1000.0)
                try:
                    await self._queen.renew(messages)
                except Exception as error:
                    print(f"Lease renewal failed: {error}")

        return asyncio.create_task(renew_loop())

    def _enhance_messages_with_trace(
        self, messages: List[Dict[str, Any]], group: Optional[str]
    ) -> None:
        """Add trace() method to messages"""
        http_client = self._http_client
        consumer_group = group or "__QUEUE_MODE__"

        for message in messages:

            async def trace(trace_config: Dict[str, Any]) -> Dict[str, Any]:
                try:
                    # Validate required structure
                    if not isinstance(trace_config, dict) or "data" not in trace_config:
                        logger.warn(
                            "ConsumerManager.trace",
                            {
                                "error": "Invalid trace config: requires { data: {...} }",
                                "transaction_id": message.get("transactionId"),
                            },
                        )
                        return {
                            "success": False,
                            "error": "Invalid trace config: requires { data: {...} }",
                        }

                    # Normalize traceName to array
                    trace_names = None
                    trace_name = trace_config.get("traceName")
                    if trace_name:
                        if isinstance(trace_name, list):
                            trace_names = [n for n in trace_name if isinstance(n, str) and n]
                            if not trace_names:
                                trace_names = None
                        elif isinstance(trace_name, str):
                            trace_names = [trace_name]

                    response = await http_client.post(
                        "/api/v1/traces",
                        {
                            "transactionId": message.get("transactionId"),
                            "partitionId": message.get("partitionId"),
                            "consumerGroup": consumer_group,
                            "traceNames": trace_names,
                            "eventType": trace_config.get("eventType", "info"),
                            "data": trace_config["data"],
                        },
                    )

                    logger.log(
                        "ConsumerManager.trace",
                        {
                            "transaction_id": message.get("transactionId"),
                            "success": True,
                            "trace_names": trace_names,
                        },
                    )
                    return {"success": True, **response} if response else {"success": True}
                except Exception as error:
                    # CRITICAL: NEVER CRASH - just log and return gracefully
                    logger.error(
                        "ConsumerManager.trace",
                        {
                            "transaction_id": message.get("transactionId"),
                            "error": str(error),
                            "phase": "trace-failed",
                        },
                    )
                    print(f"[TRACE FAILED] {message.get('transactionId')}: {error}")

                    return {"success": False, "error": str(error)}

            message["trace"] = trace

    def _get_affinity_key(
        self,
        queue: Optional[str],
        partition: Optional[str],
        namespace: Optional[str],
        task: Optional[str],
        group: Optional[str],
    ) -> Optional[str]:
        """Generate affinity key"""
        if queue:
            # Queue-based routing: queue:partition:consumerGroup
            part = partition or "*"
            grp = group or "__QUEUE_MODE__"
            return f"{queue}:{part}:{grp}"
        elif namespace or task:
            # Namespace/task-based routing: namespace:task:consumerGroup
            ns = namespace or "*"
            tsk = task or "*"
            grp = group or "__QUEUE_MODE__"
            return f"{ns}:{tsk}:{grp}"
        return None

    def _build_path(
        self,
        queue: Optional[str],
        partition: Optional[str],
        namespace: Optional[str],
        task: Optional[str],
    ) -> str:
        """Build pop path"""
        if queue:
            if partition:
                return f"/api/v1/pop/queue/{queue}/partition/{partition}"
            return f"/api/v1/pop/queue/{queue}"

        if namespace or task:
            return "/api/v1/pop"

        raise ValueError("Must specify queue, namespace, or task")

    def _autopilot_enabled(self, autopilot: Optional[bool]) -> bool:
        """
        The autopilot decision for one consume: the caller's explicit option if
        there is one, otherwise the client-wide default settled in the Queen
        constructor. The builder path has already resolved it; the None case is
        for callers that drive ConsumerManager with options of their own.
        """
        if autopilot is not None:
            return bool(autopilot)
        return not getattr(self._queen, "autopilot_off", False)

    def _build_params(
        self,
        batch: int,
        wait: bool,
        timeout_millis: int,
        group: Optional[str],
        subscription_mode: Optional[str],
        subscription_from: Optional[str],
        namespace: Optional[str],
        task: Optional[str],
        max_partitions: Optional[int] = None,
        conflation: bool = False,
        autopilot: bool = True,
    ) -> str:
        """Build query parameters"""
        # Batch, partitions and with them the autopilot flag. None/0 means the
        # user set nothing (QueueBuilder leaves it that way on purpose), which
        # is the dimension the broker gets to choose. THE RULE lives in one
        # place (utils/autopilot.py) precisely because this is the SECOND
        # parameter builder -- PLAN_CONFLATION §4 opens on that hazard by name;
        # only the placement of the keys is here, and it is the pre-autopilot
        # placement so an autopilot-off request is byte-identical.
        sizing = pop_sizing(
            batch,
            max_partitions,
            fallback_batch=CONSUME_DEFAULTS["batch"],
            autopilot=autopilot,
        )

        params: Dict[str, str] = {}
        if sizing.autopilot:
            params["autopilot"] = "true"
        if sizing.batch is not None:
            params["batch"] = sizing.batch
        params["wait"] = str(wait).lower()
        params["timeout"] = str(timeout_millis)  # Server expects 'timeout', not 'timeoutMillis'

        if group:
            params["consumerGroup"] = group
        if subscription_mode:
            params["subscriptionMode"] = subscription_mode
        if subscription_from:
            params["subscriptionFrom"] = subscription_from
        if namespace:
            params["namespace"] = namespace
        if task:
            params["task"] = task
        # v4 multi-partition pop: drain up to N sparse partitions per call.
        # Under autopilot a pinned width travels even when it is 1, because 1 is
        # then a decision and not the absence of one.
        if sizing.partitions is not None:
            params["partitions"] = sizing.partitions
        # Last-value delivery for this group (PLAN_CONFLATION §3.1). Sent only
        # when true so a non-conflating consumer's query string is unchanged.
        # This builder is SEPARATE code from QueueBuilder.pop's inline params --
        # §4 opens on that hazard, and the wire tests assert both sides.
        if conflation:
            params["conflation"] = "true"
        # NEVER send autoAck for consume - client always manages acking
        # autoAck is only for pop() where server auto-acks immediately

        return urlencode(params)

