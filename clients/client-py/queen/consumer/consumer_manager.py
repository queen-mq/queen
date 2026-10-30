"""
Consumer manager for handling concurrent workers
"""

import asyncio
import math
import random
import time
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlencode

import httpx

from .supervision import Supervision
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

# After a pop that failed for a reason waiting can fix, the next one waits: the
# broker's Retry-After when it sent one (1 s on its 503s), else 1 s doubling
# with each failure in a row. Never more than 30 s, so a consumer resumes
# within 30 s of the broker answering again; +-20 % so a fleet of workers does
# not come back in step.
_TRANSIENT_BASE_SECONDS = 1.0
_TRANSIENT_CAP_SECONDS = 30.0


def is_transient_pop_error(error: BaseException) -> bool:
    """Whether a failed pop is worth polling again.

    True when the broker, or the way to it, could not answer for a while and
    waiting changes that:

    - HTTP 408, 429 or any 5xx. The broker's own retryable answers are all a
      503 with Retry-After: no leader (``no_leader``, ``retry``), an elapsed
      deadline (``timeout``, "the request deadline elapsed"), a leader change
      mid-commit (``outcome_unknown``), a standby cluster (``standby``). A 500
      on a pop comes from a node shutting down or a roll between versions; a
      502 or 504 from a gateway in front of a node that is restarting.
    - No HTTP answer at all: a timeout, a connection that could not be made,
      or one the server dropped before or while it answered.

    False, and the consumer stops as before, for a 503
    ``raft_phase1_unsupported`` (the broker cannot serve the route at all),
    every other 4xx (a request or a credential refused the same way next time)
    and any other error (a bug, a URL httpx cannot use).
    """
    if isinstance(error, httpx.HTTPStatusError):
        if getattr(error, "code", None) == "raft_phase1_unsupported":
            return False
        status = error.response.status_code
        return status in (408, 429) or status >= 500
    return isinstance(
        error,
        (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError, ConnectionError),
    )


def transient_delay_seconds(
    failures: int,
    retry_after_seconds: Optional[float],
    rand: Callable[[], float] = random.random,
) -> float:
    """The wait before the next pop, after ``failures`` transient failures in a row."""
    if (
        isinstance(retry_after_seconds, (int, float))
        and not isinstance(retry_after_seconds, bool)
        and math.isfinite(retry_after_seconds)
        and retry_after_seconds >= 0
    ):
        base = float(retry_after_seconds)
    else:
        # The exponent is bounded only so the float stays finite: 2**16 s is
        # past the cap already.
        base = _TRANSIENT_BASE_SECONDS * 2 ** min(max(0, failures - 1), 16)
    jittered = min(base, _TRANSIENT_CAP_SECONDS) * (0.8 + 0.4 * rand())
    return min(jittered, _TRANSIENT_CAP_SECONDS)


async def _pause(seconds: float, signal: Optional[Any]) -> None:
    """Wait ``seconds``, or less if the consumer is stopped meanwhile."""
    if isinstance(signal, asyncio.Event):
        try:
            await asyncio.wait_for(signal.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
        return
    await asyncio.sleep(seconds)


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

        config = options.get("supervision")
        supervision = Supervision(self._http_client, config, options) if config is not None and config is not False else None
        if supervision:
            handler = supervision.wrap(handler)
            supervision.start()

        async def observed_worker(i):
            try:
                return await self._worker(i, handler, path, base_params, worker_options)
            finally:
                supervision.running -= 1
                if supervision.running == 0:
                    await supervision.stop()

        workers = [
            observed_worker(i) if supervision else self._worker(i, handler, path, base_params, worker_options)
            for i in range(concurrency)
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
        # Pops that failed in a row for a reason waiting can fix, and since when.
        transient_failures = 0
        failing_since: Optional[float] = None

        while True:
            # True while a handler runs under auto_ack(False): then nothing in
            # the processing block talks to the broker, so whatever it raises
            # is the handler's error.
            handler_owns_errors = False

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

                if transient_failures > 0:
                    logger.warn(
                        "ConsumerManager.worker",
                        {
                            "worker_id": worker_id,
                            "status": "pop-recovered",
                            "failed_pops": transient_failures,
                            "failing_seconds": round(time.monotonic() - (failing_since or time.monotonic()), 3),
                        },
                    )
                    transient_failures = 0
                    failing_since = None

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

                handler_owns_errors = not auto_ack
                try:
                    # Process messages
                    if each:
                        # Process one at a time. A nack releases the failed
                        # message's partition and clamps that partition's
                        # cursor at it: the later messages of THAT partition
                        # will be redelivered, so handling them now would only
                        # produce duplicates and rejected acks. The other
                        # partitions of a multi-partition pop are still leased
                        # to this worker, so their messages are handled now,
                        # not after the lease.
                        nacked_partitions = set()
                        for message in messages:
                            if signal and signal.is_set():
                                break

                            partition = message.get("partitionId") or message.get("partition")
                            if partition in nacked_partitions:
                                continue

                            ok = await self._process_message(message, handler, auto_ack, group)
                            processed_count += 1

                            if not ok:
                                nacked_partitions.add(partition)
                                logger.warn(
                                    "ConsumerManager.worker",
                                    {
                                        "worker_id": worker_id,
                                        "status": "partition-abandoned-after-nack",
                                        "partition": partition,
                                    },
                                )

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
                # A handler error under auto_ack(False) is not nacked, by
                # design, and stops the consumer. Its text can say "timeout"
                # or "connection": it is still the handler's error, not the
                # broker's, so the triage below never sees it.
                if handler_owns_errors:
                    raise

                status_code = getattr(getattr(error, "response", None), "status_code", None)

                # This client's own timeout on a long poll: the poll already
                # waited its whole time, so poll again at once. Checked on the
                # error's type, not its text: a 4xx whose message says
                # "timeout" is a refusal, and an httpx timeout can carry no
                # text at all.
                if wait and isinstance(error, httpx.TimeoutException):
                    continue

                # The broker, or the way to it, could not answer for a while:
                # a leader election, a node restarting, a gateway timing out,
                # a lost connection, a 429 the HttpClient stopped retrying.
                # Wait, then poll again, for as long as it takes (see
                # is_transient_pop_error). Before this only a 429 and errors
                # whose text said "connection" were retried here: on 2026-10-29
                # the 503s of a leader election stopped consumers whose
                # process stayed up, and nothing restarted them.
                if is_transient_pop_error(error):
                    transient_failures += 1
                    if failing_since is None:
                        failing_since = time.monotonic()
                    delay = transient_delay_seconds(transient_failures, getattr(error, "retry_after_seconds", None))
                    logger.warn(
                        "ConsumerManager.worker",
                        {
                            "worker_id": worker_id,
                            "status": "pop-failed-retrying",
                            "http_status": status_code,
                            "code": getattr(error, "code", None),
                            "error": str(error) or type(error).__name__,
                            "failed_pops": transient_failures,
                            "retry_in_seconds": round(delay, 3),
                        },
                    )
                    await _pause(delay, signal)
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

                # Anything else stops this worker and fails consume(): another
                # 4xx (refused the same way next time), a 503
                # raft_phase1_unsupported, or an error that is not a transport
                # fault.
                logger.error(
                    "ConsumerManager.worker",
                    {"worker_id": worker_id, "error": str(error), "http_status": status_code, "code": getattr(error, "code", None)},
                )
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
        False when it was nacked — the caller skips the later messages of its
        partition (the nack released that partition server-side).
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

