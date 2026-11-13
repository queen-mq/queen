"""
Queue builder for fluent API
"""

import json
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Mapping, NamedTuple, Optional, Union
from urllib.parse import urlencode

from ..errors import ConflationUnsupportedError, FilterUnsupportedError
from ..feed.feed import normalize_where, where_condition, where_value
from ..reads.offset_reads import (
    DEFAULT_MAX_BYTES,
    ChangedPartitions,
    FetchedPartition,
    fetch_partitions,
    partitions_changed_since,
)
from ..streams.util.js import UNDEFINED
from ..types import Message
from ..utils import logger
from ..utils.autopilot import AutopilotDecision, parse_autopilot_decision, pop_sizing
from ..utils.conflation import check_pop_response as check_conflation
from ..utils.conflation import scope_of as conflation_scope
from ..utils.defaults import QUEUE_DEFAULTS, CONSUME_DEFAULTS, POP_DEFAULTS
from ..utils.routing import check_filtered_response as check_filtered
from ..utils.routing import check_route_response
from ..utils.uuid_gen import generate_uuid
from ..utils.validation import is_valid_uuid, push_item_headers
from .consume_builder import ConsumeBuilder
from .dlq_builder import DLQBuilder
from .operation_builder import OperationBuilder
from .push_builder import PushBuilder

# The options a reading queue takes from its sources (PLAN_ROUTING.md §5.2):
# the broker refuses each of them on one (400 reading_queue).
READER_KEEPS = (
    "retentionEnabled",
    "retentionSeconds",
    "completedRetentionSeconds",
    "maxWaitTimeSeconds",
    "dedupWindowSeconds",
    "encryptionEnabled",
    "maxSize",
    "retentionSinkHold",
    "retentionSinkHoldMaxSeconds",
)


def _camel(key: str) -> str:
    """A config key as the broker names it (``partition_by`` -> ``partitionBy``)."""
    parts = key.split("_")
    return parts[0] + "".join(word.capitalize() for word in parts[1:])


def _reads_queues(options: Mapping[str, Any]) -> bool:
    """Whether a configuration is a reading queue's: it names its sources
    (``from``, not None, which stops a reader), or sets one of the options
    only a reading queue takes."""
    for key, value in options.items():
        name = _camel(key)
        if name == "from" and value is not None:
            return True
        if name in ("where", "partitionBy", "start"):
            return True
    return False


def _route_options(options: Mapping[str, Any]) -> Dict[str, Any]:
    """options with every ``where`` (the queue's, and each ``from`` source's)
    as the broker reads it."""
    out = dict(options)
    for key, value in options.items():
        name = _camel(key)
        if name == "where":
            out[key] = where_value(value)
        elif name == "from" and isinstance(value, (list, tuple)):
            out[key] = [
                {**s, "where": where_value(s["where"])}
                if isinstance(s, Mapping) and s.get("where") is not None
                else s
                for s in value
            ]
    return out


class PopResult(NamedTuple):
    """
    What one pop returned, plus what the broker chose to make it that size.

    `autopilot` is None when this pop did not engage autopilot, when the broker
    is older than 1.2, or when the answer was a bodiless 204 (an empty
    non-conflating pop) with nowhere to carry the echo.
    """

    messages: List[Dict[str, Any]]
    autopilot: Optional[AutopilotDecision]


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
        self._supervision = None
        self._concurrency = CONSUME_DEFAULTS["concurrency"]
        self._batch: Optional[int] = None
        self._limit = CONSUME_DEFAULTS["limit"]
        self._idle_millis = CONSUME_DEFAULTS["idle_millis"]
        self._auto_ack = CONSUME_DEFAULTS["auto_ack"]
        self._commit_on_delivery = False
        self._wait = CONSUME_DEFAULTS["wait"]
        self._timeout_millis = CONSUME_DEFAULTS["timeout_millis"]
        self._renew_lease = CONSUME_DEFAULTS["renew_lease"]
        self._renew_lease_interval_millis = CONSUME_DEFAULTS["renew_lease_interval_millis"]
        self._subscription_mode = CONSUME_DEFAULTS["subscription_mode"]
        self._subscription_from = CONSUME_DEFAULTS["subscription_from"]
        self._conflation = CONSUME_DEFAULTS["conflation"]
        self._each = False
        # _batch and _max_partitions hold the USER's value, and None means the
        # setter was never called -- which is the dimension pop autopilot gets
        # to choose. The client-side defaults are applied at emission time
        # (utils/autopilot.py), not here, because filling them in here would
        # erase the difference between "never called batch()" and "called
        # batch(1)". _autopilot is the per-builder override: None = the client
        # default (on unless QUEEN_SDK_POP_AUTOPILOT turned it off).
        self._max_partitions: Optional[int] = None
        self._autopilot: Optional[bool] = None
        # A filtered group's where (PLAN_ROUTING.md §4): the conditions of
        # where(field, op, value), ANDed, or the strings of where(text). Sent
        # on every pop of the group; the broker fixes it at the first one.
        self._where_conditions: List[Dict[str, Any]] = []
        self._where_texts: List[str] = []

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
        """
        Set queue configuration. Every option left out is sent with its
        default (QUEUE_DEFAULTS).

        A reading queue (PLAN_ROUTING.md §5) reads what its sources keep::

            await queen.queue("orders-it").config({
                "from": "orders",                  # or ["orders", {"queue": "refunds", "where": ...}]
                "where": "data.country = 'IT'",    # or conditions, as where() takes them
                "partition_by": "same",            # or "data.<path>", "headers.<name>"
                "start": "all",                    # or "now" (the default)
                "lease_time": 30,
                "retry_limit": 5,
            }).create()

        It keeps what its sources keep, so the defaults of the options the
        broker refuses on a reading queue (retention, dedup window, encryption,
        max size: READER_KEEPS) are not sent; set one and the broker answers
        400 ``reading_queue``. ``from: None`` makes it an ordinary queue again.
        The answer carries the queue's ``route``; without one the broker does
        not read other queues, and create() raises ReadingQueueUnsupportedError.
        """
        defaults = QUEUE_DEFAULTS
        if _reads_queues(options):
            defaults = {k: v for k, v in QUEUE_DEFAULTS.items() if _camel(k) not in READER_KEEPS}
        self._config = {**defaults, **_route_options(options)}
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
        # A reading queue's answer carries its route: without one, the broker
        # does not read other queues and made an ordinary queue (raised).
        reads = server_config.get("from") is not None
        return OperationBuilder(
            self._http_client,
            "POST",
            "/api/v1/configure",
            payload,
            check=(lambda answer: check_route_response(answer, requested=True)) if reads else None,
        )
    
    def _convert_config_to_camel_case(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Convert snake_case config keys to camelCase for server"""
        result = {}
        for key, value in config.items():
            # Convert snake_case to camelCase (from_ -> from)
            result[_camel(key)] = value
        return result

    def delete(self, force: bool = False) -> OperationBuilder:
        """
        Delete queue.

        A queue that reading queues read is refused (409 ``has_readers``,
        which names them) unless ``force=True``: their links to it then expire.
        """
        if not self._queue_name:
            raise ValueError("Queue name is required for delete operation")

        logger.log("QueueBuilder.delete", {"queue": self._queue_name, "force": force})
        return OperationBuilder(
            self._http_client,
            "DELETE",
            f"/api/v1/resources/queues/{self._queue_name}" + ("?force=true" if force else ""),
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

            # Headers ride beside data/payload and only when there are some, so
            # a push without them is the request it always was.
            headers = push_item_headers(item)
            if headers:
                result["headers"] = headers

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

    def supervision(self, config: Optional[Union[Dict[str, Any], bool]] = None) -> "QueueBuilder":
        """Opt into consumer dashboard reporting; None/False (default) disables it."""
        self._supervision = dict(config) if isinstance(config, dict) else config
        return self

    def concurrency(self, count: int) -> "QueueBuilder":
        """Set concurrency"""
        self._concurrency = max(1, count)
        return self

    def batch(self, size: int) -> "QueueBuilder":
        """
        Pin the message budget for one pop.

        Leave it unset and the broker sizes it (see autopilot()), where it used
        to mean the client-side default of 1. batch(0) is not "a batch of zero"
        and never was: it is the absence of an opinion, so it reads as unset.
        """
        self._batch = size if size > 0 else None
        return self

    def partitions(self, n: int) -> "QueueBuilder":
        """
        Pop messages from up to N partitions in a single call (v4 multi-partition pop).

        Use this to drain many sparsely-loaded partitions efficiently. With
        partitions(N), the global batch(B) budget is shared across all
        claimed partitions: at most B total messages, drawn from up to N
        partitions, in a single network round-trip. All N partitions share
        a single leaseId — renewing once extends them all.

        Leave it unset and the broker chooses the sweep width (see
        autopilot()); partitions(1) pins the legacy single-partition behaviour,
        which is a decision the broker is told about and never overrides.
        """
        self._max_partitions = n if n > 0 else None
        return self

    def autopilot(self, enabled: bool = True) -> "QueueBuilder":
        """
        Turn broker-side pop sizing on or off for this builder.

        On (the default) the broker chooses `batch` and `partitions` for the
        pops of this builder. Even then, a batch or partitions set explicitly
        travels on the wire as it always did and is never second-guessed:
        autopilot only ever fills the knobs left unset.

        autopilot(False) restores this SDK's pre-1.2 behaviour byte for byte:
        the client-side defaults come back (batch 1, partitions 1) and no
        autopilot parameter is sent. QUEEN_SDK_POP_AUTOPILOT=off does the same
        for a whole process; an explicit call here outranks the environment in
        both directions.

        Setting BOTH batch and partitions leaves autopilot nothing to decide, so
        no autopilot parameter is sent in that case either, whatever this flag
        says.
        """
        self._autopilot = bool(enabled)
        return self

    def _autopilot_enabled(self) -> bool:
        """
        This builder's resolved autopilot decision: its own flag when set,
        otherwise the client-wide default settled in the Queen constructor.
        """
        if self._autopilot is not None:
            return self._autopilot
        return not getattr(self._queen, "autopilot_off", False)

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
        """
        Ack after the consume() handler returns (default True): each message
        with each(), otherwise the batch. Client side and consume() only: it has
        no effect on pop().
        """
        self._auto_ack = enabled
        return self

    def commit_on_delivery(self, enabled: bool = True) -> "QueueBuilder":
        """
        Commit at delivery on pop(): the broker moves the consumer group's
        cursor past the messages as it hands them out. No lease, nothing to
        ack, at-most-once: a message lost after the pop is not delivered again.

        pop() and pop_result() only: consume() always leases its messages and
        raises ValueError. The broker refuses it together with conflation()
        (a 400).
        """
        self._commit_on_delivery = bool(enabled)
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
        refused by the broker together with commit_on_delivery().
        """
        self._conflation = bool(enabled)
        return self

    def each(self) -> "QueueBuilder":
        """Process messages one at a time"""
        self._each = True
        return self

    def where(self, field: Any, op: Any = UNDEFINED, value: Any = UNDEFINED) -> "QueueBuilder":
        """
        A filtered group: the group gets only the messages where the condition
        holds (PLAN_ROUTING.md §4). Several calls are ANDed::

            await (queen.queue("order-events").group("billing")
                .subscription_mode("all")
                .where("data.kind", "in", ["order.paid", "order.refunded"])
                .where("headers.source", "=", "web")
                .consume(handler))

            queen.queue("order-events").group("billing").where("data.kind in ('order.paid', 'order.refunded')")

        The same conditions as Stream.where() and the feed: fields queue,
        partition, transactionId, data, data.<path>, headers.<name>; ops = !=
        < <= > >= in exists prefix !prefix. Give conditions or one string, not
        both; a condition may also be ``{field, op, value}`` or a list of them.

        A setting of the CONSUMER GROUP, like subscription_mode: sent on every
        pop, fixed at the group's first one. A different where for that group
        is refused (400 ``where_mismatch``): change it with
        ``admin.update_consumer_group_where(group, queue, where)``. A group that
        already reads the queue without one cannot start filtering: name a new
        group. Needs a consumer group and a queue (not a namespace/task pop), on
        a broker at catalogue version 8; pop() raises the broker's 400 here
        rather than answering []. Every answer to a pop with a where says
        ``"filtered": true``; one without it (a broker that does not filter,
        which would hand the group the whole queue) raises
        FilterUnsupportedError from pop() and consume() alike.
        """
        if op is UNDEFINED and value is UNDEFINED and isinstance(field, str):
            text = field.strip()
            if not text:
                raise ValueError("where: the condition is empty")
            if self._where_conditions:
                raise ValueError("where: give conditions or one string, not both")
            self._where_texts.append(text)
            return self
        if self._where_texts:
            raise ValueError("where: give conditions or one string, not both")
        if op is UNDEFINED and value is UNDEFINED:
            conditions = normalize_where(field)
        else:
            conditions = [where_condition(field, op, value)]
        # Serialized once here so a value JSON does not carry fails at the call
        # site, not inside a pop that answers [].
        json.dumps(conditions)
        self._where_conditions.extend(conditions)
        return self

    def _where_param(self) -> Optional[str]:
        """The pop's ``where`` parameter: the strings ANDed, or the conditions
        as one JSON list; None without a where."""
        if self._where_texts:
            return " and ".join(self._where_texts)
        if self._where_conditions:
            return json.dumps(self._where_conditions, separators=(",", ":"), ensure_ascii=False)
        return None

    def _check_where(self) -> Optional[str]:
        """The where to send, refused here when the broker would ignore or refuse it."""
        where = self._where_param()
        if where is None:
            return None
        if not self._queue_name:
            raise ValueError("where() filters a group on one queue: a namespace/task pop does not take it")
        if not self._group:
            raise ValueError("where() filters a consumer group: name one with group()")
        return where

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

        Raises:
            ValueError: commit_on_delivery() is set, which is a pop() option
        """
        if self._commit_on_delivery:
            raise ValueError(
                "commit_on_delivery() is a pop() option; consume() always leases its messages"
            )
        where = self._check_where()

        consume_options = {
            "queue": self._queue_name,
            "partition": self._partition if self._partition != "Default" else None,
            "namespace": self._namespace,
            "task": self._task,
            "group": self._group,
            "concurrency": self._concurrency,
            "supervision": self._supervision,
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
            "where": where,
            "each": self._each,
            "max_partitions": self._max_partitions,
            # Resolved here so ConsumerManager sees a decision and not a None.
            # batch and max_partitions keep their None when autopilot is on, and
            # that None has to survive all the way to _build_params: it is the
            # ONLY record that the user said nothing about that dimension.
            "autopilot": self._autopilot_enabled(),
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

    async def pop_result(self) -> PopResult:
        """
        Claim messages and report what the broker chose for this pop.

        Same call as pop() -- this is the shape that also carries the additive
        `autopilot` echo, which is None when this pop did not engage autopilot
        or the broker is older than 1.2.

            res = await client.queue("events").group("workers").pop_result()
            if res.autopilot:
                print(res.autopilot.partitions, res.autopilot.batch,
                      res.autopilot.wait_millis)
        """
        return await self._pop_with_decision()

    async def pop(self) -> List[Dict[str, Any]]:
        """Pop messages"""
        return (await self._pop_with_decision()).messages

    async def _pop_with_decision(self) -> PopResult:
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
        # Raised, never answered as []: a where the broker would refuse or
        # ignore reads as "no messages" on every call otherwise.
        where = self._check_where()

        try:
            path = self._build_pop_path()

            # Batch, partitions and with them the autopilot flag. The RULE for
            # which of the three travel lives in one place
            # (utils/autopilot.py) because consume() builds its query string
            # separately; only the PLACEMENT is here, and it is the
            # pre-autopilot placement so an autopilot-off request is
            # byte-identical to the one this SDK used to send.
            sizing = pop_sizing(
                self._batch,
                self._max_partitions,
                fallback_batch=POP_DEFAULTS["batch"],
                autopilot=self._autopilot_enabled(),
            )

            params: Dict[str, str] = {}
            if sizing.autopilot:
                params["autopilot"] = "true"
            if sizing.batch is not None:
                params["batch"] = sizing.batch
            params["wait"] = str(self._wait).lower()
            params["timeout"] = str(self._timeout_millis)

            if self._group:
                params["consumerGroup"] = self._group
            if self._namespace:
                params["namespace"] = self._namespace
            if self._task:
                params["task"] = self._task
            # Commit at delivery: no lease, nothing to ack, at-most-once.
            # `autoAck` is the name every 2.x broker reads.
            if self._commit_on_delivery:
                params["autoAck"] = "true"
            if self._subscription_mode:
                params["subscriptionMode"] = self._subscription_mode
            if self._subscription_from:
                params["subscriptionFrom"] = self._subscription_from
            # Under autopilot a pinned width travels even when it is 1, because
            # 1 is then a decision and not the absence of one.
            if sizing.partitions is not None:
                params["partitions"] = sizing.partitions
            # Only ever emitted when true, mirroring autoAck: an absent
            # parameter is what keeps every non-conflating pop byte-identical
            # to a pre-1.1.0 one (PLAN_CONFLATION §3.1).
            if self._conflation:
                params["conflation"] = "true"
            # Last, and only with a where: every other pop is the request it was.
            if where is not None:
                params["where"] = where

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
            # The same rule for a where: a broker that did not filter would
            # hand this group the whole queue (utils/routing.py).
            check_filtered(result, requested=where is not None)

            # The broker's own account of how it sized this pop, when the
            # request engaged autopilot and the answer had a body to carry it (a
            # bodiless 204 cannot, so an empty short pop reports None).
            autopilot = parse_autopilot_decision(result)

            if not result or not result.get("messages"):
                logger.log("QueueBuilder.pop", {"status": "no-messages"})
                return PopResult(messages=[], autopilot=autopilot)

            messages = [msg for msg in result["messages"] if msg is not None]
            logger.log("QueueBuilder.pop", {"status": "success", "count": len(messages)})
            return PopResult(messages=messages, autopilot=autopilot)
        except (ConflationUnsupportedError, FilterUnsupportedError):
            # NOT swallowed into [], unlike every other failure below. An empty
            # list here would hide the one thing this error exists to say, and
            # the caller would go on polling a broker that silently ignores the
            # policy it asked for.
            raise
        except Exception as error:
            status_code = getattr(getattr(error, "response", None), "status_code", None)
            # The broker's 400 refusals of a conflating pop (no consumer group,
            # or commit_on_delivery() as well) are permanent config faults: []
            # would read as "no messages right now" on every call, the silent
            # version §4 rules out. Raised, as in the JS client. So are those
            # of a filtered group's pop (where_mismatch, bad_where,
            # cluster_version, a source queue that does not exist).
            if (self._conflation or where is not None) and status_code == 400:
                logger.error(
                    "QueueBuilder.pop",
                    {
                        "error": str(error),
                        "status_code": status_code,
                        "code": getattr(error, "code", None),
                        "conflation": self._conflation,
                        "where": where is not None,
                    },
                )
                raise
            # Return empty array on error instead of throwing. This also
            # covers a 429 whose retry_429 policy was exhausted (bounded
            # pop, or an explicit max_attempts override) and a terminal 403
            # (e.g. cluster_suspended) -- both are logged with their status
            # code/`.code` rather than raising, matching this method's
            # existing swallow-to-[] contract.
            logger.error(
                "QueueBuilder.pop",
                {"error": str(error), "status_code": status_code, "code": getattr(error, "code", None)},
            )
            print(f"Pop failed: {error}")
            return PopResult(messages=[], autopilot=None)

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
    # Reads by offset
    # ===========================

    async def fetch(
        self,
        partitions: Union[str, Iterable[str]],
        *,
        offsets: Optional[Mapping[str, int]] = None,
        max_bytes: int = DEFAULT_MAX_BYTES,
    ) -> Dict[str, FetchedPartition]:
        """Every record of these partitions, read by offset: no lease, no
        cursor, nothing to ack. Answers a dict of partition name to
        ``{"partition", "records", "highWatermark", "logStartOffset", "nextOffset"}``::

            read = await queen.queue("orders").fetch(["customer-42", "customer-7"])
            [r["payload"] for r in read["customer-42"]["records"]]

        ``offsets`` ({name: offset}) starts a partition later than its oldest
        kept record, e.g. at the ``nextOffset`` of an earlier read.
        ``max_bytes`` caps the payload bytes of one call (default 8 MiB).
        See queen/reads/offset_reads.py.
        """
        return await fetch_partitions(
            self._http_client, self._queue_name, partitions, offsets=offsets, max_bytes=max_bytes
        )

    async def changed_since(self, since: Union[None, str, datetime] = None) -> ChangedPartitions:
        """The partitions written at or after ``since`` (a datetime, naive =
        UTC, or an ISO-8601 string; None lists them all):
        ``{"partitions": [...], "safeTime": ...}``. Pass ``safeTime`` as the
        next ``since``::

            changed = await queen.queue("orders").changed_since(datetime.now(timezone.utc) - timedelta(minutes=10))
            [p["name"] for p in changed["partitions"]]
        """
        return await partitions_changed_since(self._http_client, self._queue_name, since)

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

