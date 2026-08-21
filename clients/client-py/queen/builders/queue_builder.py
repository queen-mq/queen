"""
Queue builder for fluent API
"""

from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import urlencode

from ..errors import ConflationUnsupportedError
from ..types import Message
from ..utils import logger
from ..utils.conflation import check_pop_response as check_conflation
from ..utils.conflation import scope_of as conflation_scope
from ..utils.defaults import QUEUE_DEFAULTS, CONSUME_DEFAULTS, POP_DEFAULTS
from ..utils.uuid_gen import generate_uuid
from ..utils.validation import is_valid_uuid
from .consume_builder import ConsumeBuilder
from .dlq_builder import DLQBuilder
from .operation_builder import OperationBuilder
from .push_builder import PushBuilder


class QueueBuilder:
    """Queue builder for fluent API"""

    def __init__(
        self,
        queen: Any,
        http_client: Any,
        buffer_manager: Any,
        queue_name: Optional[str] = None,
    ):
        """
        Initialize queue builder

        Args:
            queen: Queen instance
            http_client: HttpClient instance
            buffer_manager: BufferManager instance
            queue_name: Optional queue name
        """
        self._queen = queen
        self._http_client = http_client
        self._buffer_manager = buffer_manager
        self._queue_name = queue_name
        self._partition = "Default"
        self._namespace: Optional[str] = None
        self._task: Optional[str] = None
        self._group: Optional[str] = None
        self._config: Dict[str, Any] = {}

        # Consume options
        self._concurrency = CONSUME_DEFAULTS["concurrency"]
        self._batch = CONSUME_DEFAULTS["batch"]
        self._limit = CONSUME_DEFAULTS["limit"]
        self._idle_millis = CONSUME_DEFAULTS["idle_millis"]
        self._auto_ack = CONSUME_DEFAULTS["auto_ack"]
        self._wait = CONSUME_DEFAULTS["wait"]
        self._timeout_millis = CONSUME_DEFAULTS["timeout_millis"]
        self._renew_lease = CONSUME_DEFAULTS["renew_lease"]
        self._renew_lease_interval_millis = CONSUME_DEFAULTS["renew_lease_interval_millis"]
        self._subscription_mode = CONSUME_DEFAULTS["subscription_mode"]
        self._subscription_from = CONSUME_DEFAULTS["subscription_from"]
        self._conflation = CONSUME_DEFAULTS["conflation"]
        self._each = False
        self._max_partitions = 1

        # Buffer options
        self._buffer_options: Optional[Dict[str, Any]] = None

    # ===========================
    # Queue Configuration Methods
    # ===========================

    def namespace(self, name: str) -> "QueueBuilder":
        """Set namespace"""
        self._namespace = name
        return self

    def task(self, name: str) -> "QueueBuilder":
        """Set task"""
        self._task = name
        return self

    def config(self, options: Dict[str, Any]) -> "QueueBuilder":
        """Set queue configuration"""
        self._config = {**QUEUE_DEFAULTS, **options}
        return self

    def create(self) -> OperationBuilder:
        """Create queue"""
        # Always merge with QUEUE_DEFAULTS to ensure all options are sent
        full_config = self._config if self._config else QUEUE_DEFAULTS
        
        # Convert snake_case keys to camelCase for server
        server_config = self._convert_config_to_camel_case(full_config)

        payload = {
            "queue": self._queue_name,
            "namespace": self._namespace,
            "task": self._task,
            "options": server_config,
        }

        logger.log(
            "QueueBuilder.create",
            {"queue": self._queue_name, "namespace": self._namespace, "task": self._task},
        )
        return OperationBuilder(self._http_client, "POST", "/api/v1/configure", payload)
    
    def _convert_config_to_camel_case(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Convert snake_case config keys to camelCase for server"""
        result = {}
        for key, value in config.items():
            # Convert snake_case to camelCase
            parts = key.split("_")
            camel_key = parts[0] + "".join(word.capitalize() for word in parts[1:])
            result[camel_key] = value
        return result

    def delete(self) -> OperationBuilder:
        """Delete queue"""
        if not self._queue_name:
            raise ValueError("Queue name is required for delete operation")

        logger.log("QueueBuilder.delete", {"queue": self._queue_name})
        return OperationBuilder(
            self._http_client,
            "DELETE",
            f"/api/v1/resources/queues/{self._queue_name}",
            None,
        )

    # ===========================
    # Push Methods
    # ===========================

    def partition(self, name: str) -> "QueueBuilder":
        """Set partition"""
        self._partition = name
        return self

    def buffer(self, options: Dict[str, Any]) -> "QueueBuilder":
        """Enable client-side buffering"""
        self._buffer_options = options
        return self

    def push(self, payload: Union[Dict[str, Any], List[Dict[str, Any]]]) -> PushBuilder:
        """Push messages"""
        if not self._queue_name:
            raise ValueError("Queue name is required for push operation")

        logger.log(
            "QueueBuilder.push",
            {
                "queue": self._queue_name,
                "partition": self._partition,
                "count": len(payload) if isinstance(payload, list) else 1,
                "buffered": self._buffer_options is not None,
            },
        )

        # Format items
        items = payload if isinstance(payload, list) else [payload]
        formatted_items = []

        for item in items:
            # Determine the payload - check if property exists, not just truthy
            if "data" in item:
                payload_value = item["data"]
            elif "payload" in item:
                payload_value = item["payload"]
            else:
                payload_value = item

            result = {
                "queue": self._queue_name,
                "partition": self._partition,
                "payload": payload_value,
                "transactionId": item.get("transactionId") or generate_uuid(),
            }

            # Include traceId if provided and valid UUID
            trace_id = item.get("traceId")
            if trace_id and is_valid_uuid(trace_id):
                result["traceId"] = trace_id

            formatted_items.append(result)

        # Return a PushBuilder for chaining callbacks
        return PushBuilder(
            self._http_client,
            self._buffer_manager,
            self._queue_name,
            self._partition,
            formatted_items,
            self._buffer_options,
        )

    # ===========================
    # Consume Configuration Methods
    # ===========================

    def group(self, name: str) -> "QueueBuilder":
        """Set consumer group"""
        self._group = name
        return self

    def concurrency(self, count: int) -> "QueueBuilder":
        """Set concurrency"""
        self._concurrency = max(1, count)
        return self

    def batch(self, size: int) -> "QueueBuilder":
        """Set batch size"""
        self._batch = max(1, size)
        return self

    def partitions(self, n: int) -> "QueueBuilder":
        """
        Pop messages from up to N partitions in a single call (v4 multi-partition pop).

        Use this to drain many sparsely-loaded partitions efficiently. With
        partitions(N), the global batch(B) budget is shared across all
        claimed partitions: at most B total messages, drawn from up to N
        partitions, in a single network round-trip. All N partitions share
        a single leaseId — renewing once extends them all.

        Default 1 = legacy single-partition behavior.
        """
        self._max_partitions = max(1, n)
        return self

    def limit(self, count: int) -> "QueueBuilder":
        """Set message limit"""
        self._limit = count
        return self

    def idle_millis(self, millis: int) -> "QueueBuilder":
        """Set idle timeout"""
        self._idle_millis = millis
        return self

    def timeout_millis(self, millis: int) -> "QueueBuilder":
        """Set the long-poll timeout in milliseconds (mirrors JS .timeoutMillis())."""
        self._timeout_millis = millis
        return self

    def auto_ack(self, enabled: bool) -> "QueueBuilder":
        """Set auto-ack"""
        self._auto_ack = enabled
        return self

    def renew_lease(self, enabled: bool, interval_millis: Optional[int] = None) -> "QueueBuilder":
        """Enable lease renewal"""
        self._renew_lease = enabled
        if interval_millis:
            self._renew_lease_interval_millis = interval_millis
        return self

    def subscription_mode(self, mode: str) -> "QueueBuilder":
        """Set subscription mode"""
        self._subscription_mode = mode
        return self

    def subscription_from(self, from_: str) -> "QueueBuilder":
        """Set subscription start point"""
        self._subscription_from = from_
        return self

    def conflation(self, enabled: bool = True) -> "QueueBuilder":
        """
        Last-value delivery: a pop of a partition delivers only the NEWEST
        visible message and retires everything behind it.

        For command-style queues where one partition is one logical task key
        ("recompute entity X") and only the freshest request matters. Under
        backlog the handler runs once per partition on the newest message
        instead of once per message.

        This is a property of the CONSUMER GROUP on this queue, sitting beside
        subscription_mode: it is persisted when the group first registers, and
        from then on the stored value wins for every consumer of that group.
        A consumer that declares the opposite keeps working -- the stored
        policy applies and the SDK warns once (PLAN_CONFLATION §3.3).

        Requires broker >= 1.1.0. An older broker ignores the flag, so the SDK
        raises ConflationUnsupportedError on the first pop rather than silently
        draining the backlog message by message (§4).

        Applies to both consume() and pop(); needs a consumer group, and is
        refused by the broker together with server-side autoAck.
        """
        self._conflation = bool(enabled)
        return self

    def each(self) -> "QueueBuilder":
        """Process messages one at a time"""
        self._each = True
        return self

    # ===========================
    # Consume Method
    # ===========================

    def consume(
        self,
        handler: Callable[[Union[Message, List[Message]]], Any],
        *,
        signal: Optional[Any] = None,  # asyncio.Event
    ) -> ConsumeBuilder:
        """
        Start consuming messages

        Args:
            handler: Message handler
            signal: Optional abort signal (asyncio.Event)

        Returns:
            ConsumeBuilder for chaining
        """
        consume_options = {
            "queue": self._queue_name,
            "partition": self._partition if self._partition != "Default" else None,
            "namespace": self._namespace,
            "task": self._task,
            "group": self._group,
            "concurrency": self._concurrency,
            "batch": self._batch,
            "limit": self._limit,
            "idle_millis": self._idle_millis,
            "auto_ack": self._auto_ack,
            "wait": self._wait,
            "timeout_millis": self._timeout_millis,
            "renew_lease": self._renew_lease,
            "renew_lease_interval_millis": self._renew_lease_interval_millis,
            "subscription_mode": self._subscription_mode,
            "subscription_from": self._subscription_from,
            "conflation": self._conflation,
            "each": self._each,
            "max_partitions": self._max_partitions,
            "signal": signal,
        }

        return ConsumeBuilder(self._http_client, self._queen, handler, consume_options)

    # ===========================
    # Pop Methods
    # ===========================

    def wait(self, enabled: bool) -> "QueueBuilder":
        """Enable/disable long polling"""
        self._wait = enabled
        return self

    async def pop(self) -> List[Dict[str, Any]]:
        """Pop messages"""
        logger.log(
            "QueueBuilder.pop",
            {
                "queue": self._queue_name,
                "partition": self._partition,
                "namespace": self._namespace,
                "task": self._task,
                "batch": self._batch,
                "wait": self._wait,
                "group": self._group,
            },
        )

        try:
            path = self._build_pop_path()

            # For pop(), use POP defaults (not CONSUME defaults)
            # Override autoAck to false unless explicitly set
            effective_auto_ack = (
                self._auto_ack
                if self._auto_ack != CONSUME_DEFAULTS["auto_ack"]
                else POP_DEFAULTS["auto_ack"]
            )

            # Build params with correct autoAck for pop
            params: Dict[str, str] = {
                "batch": str(self._batch),
                "wait": str(self._wait).lower(),
                "timeout": str(self._timeout_millis),
            }

            if self._group:
                params["consumerGroup"] = self._group
            if self._namespace:
                params["namespace"] = self._namespace
            if self._task:
                params["task"] = self._task
            if effective_auto_ack:
                params["autoAck"] = "true"
            if self._subscription_mode:
                params["subscriptionMode"] = self._subscription_mode
            if self._subscription_from:
                params["subscriptionFrom"] = self._subscription_from
            if self._max_partitions > 1:
                params["partitions"] = str(self._max_partitions)
            # Only ever emitted when true, mirroring autoAck: an absent
            # parameter is what keeps every non-conflating pop byte-identical
            # to a pre-1.1.0 one (PLAN_CONFLATION §3.1).
            if self._conflation:
                params["conflation"] = "true"

            # Generate affinity key for consistent routing to same backend
            affinity_key = self._get_affinity_key()

            # wait=True is a long-poll: on 429 it should back off and keep
            # waiting rather than give up after a handful of tries.
            query_string = urlencode(params)
            result = await self._http_client.get(
                f"{path}?{query_string}", self._timeout_millis + 5000, affinity_key,
                retry_kind="pop" if self._wait else None,
            )

            # Before anything is returned to the caller: did the broker
            # actually apply the policy we asked for? (PLAN_CONFLATION §4.)
            # This runs ahead of the empty-response branch on purpose -- the
            # echo rides empty pops too, so an old broker is caught on the
            # first round trip instead of after a backlog has been drained.
            check_conflation(
                result,
                requested=self._conflation,
                queue=conflation_scope(self._queue_name, self._namespace, self._task),
                group=self._group,
            )

            if not result or not result.get("messages"):
                logger.log("QueueBuilder.pop", {"status": "no-messages"})
                return []

            messages = [msg for msg in result["messages"] if msg is not None]
            logger.log("QueueBuilder.pop", {"status": "success", "count": len(messages)})
            return messages
        except ConflationUnsupportedError:
            # NOT swallowed into [], unlike every other failure below. An empty
            # list here would hide the one thing this error exists to say, and
            # the caller would go on polling a broker that silently ignores the
            # policy it asked for.
            raise
        except Exception as error:
            # Return empty array on error instead of throwing. This also
            # covers a 429 whose retry_429 policy was exhausted (bounded
            # pop, or an explicit max_attempts override) and a terminal 403
            # (e.g. cluster_suspended) -- both are logged with their status
            # code/`.code` rather than raising, matching this method's
            # existing swallow-to-[] contract.
            status_code = getattr(getattr(error, "response", None), "status_code", None)
            logger.error(
                "QueueBuilder.pop",
                {"error": str(error), "status_code": status_code, "code": getattr(error, "code", None)},
            )
            print(f"Pop failed: {error}")
            return []

    def _build_pop_path(self) -> str:
        """Build pop path"""
        if self._queue_name:
            if self._partition and self._partition != "Default":
                return f"/api/v1/pop/queue/{self._queue_name}/partition/{self._partition}"
            return f"/api/v1/pop/queue/{self._queue_name}"

        if self._namespace or self._task:
            return "/api/v1/pop"

        raise ValueError("Must specify queue, namespace, or task for pop operation")

    # ===========================
    # Affinity Key Generation
    # ===========================

    def _get_affinity_key(self) -> Optional[str]:
        """
        Generate affinity key for consistent routing
        Matches server's PollIntention::grouping_key() format
        Format: queue:partition:consumerGroup or namespace:task:consumerGroup
        """
        if self._queue_name:
            # Queue-based routing: queue:partition:consumerGroup
            partition = self._partition or "*"
            group = self._group or "__QUEUE_MODE__"
            return f"{self._queue_name}:{partition}:{group}"
        elif self._namespace or self._task:
            # Namespace/task-based routing: namespace:task:consumerGroup
            namespace = self._namespace or "*"
            task = self._task or "*"
            group = self._group or "__QUEUE_MODE__"
            return f"{namespace}:{task}:{group}"
        return None

    # ===========================
    # Buffer Management Methods
    # ===========================

    async def flush_buffer(self) -> None:
        """Flush buffer for this queue"""
        if not self._queue_name:
            raise ValueError("Queue name is required for buffer flush")
        queue_address = f"{self._queue_name}/{self._partition}"
        logger.log("QueueBuilder.flushBuffer", {"queue_address": queue_address})
        await self._buffer_manager.flush_buffer(queue_address)

    # ===========================
    # Dead Letter Queue Methods
    # ===========================

    def dlq(self, consumer_group: Optional[str] = None) -> DLQBuilder:
        """Query dead letter queue"""
        if not self._queue_name:
            raise ValueError("Queue name is required for DLQ operations")
        logger.log(
            "QueueBuilder.dlq",
            {"queue": self._queue_name, "consumer_group": consumer_group, "partition": self._partition},
        )
        return DLQBuilder(self._http_client, self._queue_name, consumer_group, self._partition)

