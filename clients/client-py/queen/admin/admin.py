"""
Admin API for Queen - Administrative and observability endpoints
These APIs are typically used by dashboards and admin tools, not regular applications
"""

from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode

from ..utils import logger


class Admin:
    """Admin API for Queen - Administrative and observability operations"""

    def __init__(self, http_client: Any) -> None:
        self._http_client = http_client

    # ===========================
    # Resources API
    # ===========================

    async def get_overview(self) -> Dict[str, Any]:
        """
        Get system overview with queue counts, message stats, etc.

        Returns:
            Overview data
        """
        logger.log("Admin.get_overview", {})
        return await self._http_client.get("/api/v1/resources/overview")

    async def get_namespaces(self) -> Dict[str, Any]:
        """
        Get all namespaces

        Returns:
            List of namespaces
        """
        logger.log("Admin.get_namespaces", {})
        return await self._http_client.get("/api/v1/resources/namespaces")

    async def get_tasks(self) -> Dict[str, Any]:
        """
        Get all tasks

        Returns:
            List of tasks
        """
        logger.log("Admin.get_tasks", {})
        return await self._http_client.get("/api/v1/resources/tasks")

    # ===========================
    # Queues API
    # ===========================

    async def list_queues(self, **params: Any) -> Dict[str, Any]:
        """
        List all queues

        Args:
            **params: Query parameters (limit, offset, search, etc.)

        Returns:
            List of queues
        """
        logger.log("Admin.list_queues", {"params": params})
        query_string = self._build_query_string(params)
        return await self._http_client.get(f"/api/v1/resources/queues{query_string}")

    async def get_queue(self, name: str) -> Dict[str, Any]:
        """
        Get queue details

        Args:
            name: Queue name

        Returns:
            Queue details
        """
        logger.log("Admin.get_queue", {"name": name})
        return await self._http_client.get(f"/api/v1/resources/queues/{quote(name)}")

    async def get_queue_depth(self, name: str, group: Optional[str] = None) -> Dict[str, Any]:
        """
        Per-partition backlog for a queue - the cheap sibling of get_queue:
        watermark arithmetic only, no segments, no timestamps.

        Shape: {queue, group, pending, partitionsPending, conflation,
        effectivePending, partitions: [{partition, pending}]}.

        The last three arrive with broker >= 1.1.0 (PLAN_CONFLATION §5.3) and
        exist because one number stops being enough once a group conflates:

            pending           log depth - positions left to retire
            partitionsPending partitions that owe work (pending > 0)
            conflation        this group's stored delivery policy
            effectivePending  WORK depth - handler invocations remaining;
                              partitionsPending when the group conflates,
                              pending when it does not

        Read them together or the reading is wrong: a conflating queue at
        pending 4 000 000 / effectivePending 12 is healthy, while the same two
        numbers on a non-conflating group are an incident.

        Args:
            name: Queue name
            group: Consumer group; None = queue-level pending under the same
                worst-cursor precedence the dashboard publishes, a named group
                = that group's own backlog per partition. `conflation` is a
                per-group policy, so it reads false when no group is named.

        Returns:
            Depth report. Requires broker >= 1.0.4 - an older broker answers
            404 no_such_route, so fall back to get_queue there. A broker
            between 1.0.4 and 1.1.0 answers without the three fields above.
        """
        logger.log("Admin.get_queue_depth", {"name": name, "group": group})
        query_string = f"?group={quote(group)}" if group else ""
        return await self._http_client.get(
            f"/api/v1/resources/queues/{quote(name)}/depth{query_string}"
        )

    async def clear_queue(self, name: str, partition: Optional[str] = None) -> Dict[str, Any]:
        """
        Clear all messages from a queue

        Args:
            name: Queue name
            partition: Optional partition to clear

        Returns:
            Result
        """
        logger.log("Admin.clear_queue", {"name": name, "partition": partition})
        query_string = f"?partition={quote(partition)}" if partition else ""
        return await self._http_client.delete(f"/api/v1/queues/{quote(name)}/clear{query_string}")

    async def get_partitions(self, **params: Any) -> Dict[str, Any]:
        """
        Get all partitions

        Args:
            **params: Query parameters (queue, limit, etc.)

        Returns:
            List of partitions
        """
        logger.log("Admin.get_partitions", {"params": params})
        query_string = self._build_query_string(params)
        return await self._http_client.get(f"/api/v1/resources/partitions{query_string}")

    # ===========================
    # Messages API
    # ===========================

    async def list_messages(self, **params: Any) -> Dict[str, Any]:
        """
        List messages with filters

        Args:
            **params: Query parameters (queue, partition, status, limit, offset, etc.)

        Returns:
            List of messages
        """
        logger.log("Admin.list_messages", {"params": params})
        query_string = self._build_query_string(params)
        return await self._http_client.get(f"/api/v1/messages{query_string}")

    async def get_message(self, partition_id: str, transaction_id: str) -> Dict[str, Any]:
        """
        Get a specific message

        Args:
            partition_id: Partition ID
            transaction_id: Transaction ID

        Returns:
            Message details
        """
        logger.log("Admin.get_message", {"partition_id": partition_id, "transaction_id": transaction_id})
        return await self._http_client.get(f"/api/v1/messages/{partition_id}/{transaction_id}")

    async def delete_message(self, partition_id: str, transaction_id: str) -> Dict[str, Any]:
        """
        Delete a specific message

        Args:
            partition_id: Partition ID
            transaction_id: Transaction ID

        Returns:
            Result
        """
        logger.log("Admin.delete_message", {"partition_id": partition_id, "transaction_id": transaction_id})
        return await self._http_client.delete(f"/api/v1/messages/{partition_id}/{transaction_id}")

    async def retry_message(self, partition_id: str, transaction_id: str) -> Dict[str, Any]:
        """
        Retry a failed message

        Args:
            partition_id: Partition ID
            transaction_id: Transaction ID

        Returns:
            Result
        """
        logger.log("Admin.retry_message", {"partition_id": partition_id, "transaction_id": transaction_id})
        return await self._http_client.post(f"/api/v1/messages/{partition_id}/{transaction_id}/retry", {})

    async def move_message_to_dlq(self, partition_id: str, transaction_id: str) -> Dict[str, Any]:
        """
        Move a message to the Dead Letter Queue

        Args:
            partition_id: Partition ID
            transaction_id: Transaction ID

        Returns:
            Result
        """
        logger.log("Admin.move_message_to_dlq", {"partition_id": partition_id, "transaction_id": transaction_id})
        return await self._http_client.post(f"/api/v1/messages/{partition_id}/{transaction_id}/dlq", {})

    # ===========================
    # Traces API
    # ===========================

    async def get_traces_by_name(self, trace_name: str, **params: Any) -> Dict[str, Any]:
        """
        Get traces by name

        Args:
            trace_name: Trace name to search for
            **params: Query parameters (limit, offset, from, to, etc.)

        Returns:
            List of traces
        """
        logger.log("Admin.get_traces_by_name", {"trace_name": trace_name, "params": params})
        query_string = self._build_query_string(params)
        return await self._http_client.get(f"/api/v1/traces/by-name/{quote(trace_name)}{query_string}")

    async def get_trace_names(self, **params: Any) -> Dict[str, Any]:
        """
        Get available trace names

        Args:
            **params: Query parameters (limit, search, etc.)

        Returns:
            List of trace names
        """
        logger.log("Admin.get_trace_names", {"params": params})
        query_string = self._build_query_string(params)
        return await self._http_client.get(f"/api/v1/traces/names{query_string}")

    async def get_traces_for_message(self, partition_id: str, transaction_id: str) -> Dict[str, Any]:
        """
        Get traces for a specific message

        Args:
            partition_id: Partition ID
            transaction_id: Transaction ID

        Returns:
            List of traces
        """
        logger.log("Admin.get_traces_for_message", {"partition_id": partition_id, "transaction_id": transaction_id})
        return await self._http_client.get(f"/api/v1/traces/{partition_id}/{transaction_id}")

    # ===========================
    # Analytics/Status API
    # ===========================

    async def get_status(self, **params: Any) -> Dict[str, Any]:
        """
        Get system status

        Args:
            **params: Query parameters

        Returns:
            System status
        """
        logger.log("Admin.get_status", {"params": params})
        query_string = self._build_query_string(params)
        return await self._http_client.get(f"/api/v1/status{query_string}")

    async def get_queue_stats(self, **params: Any) -> Dict[str, Any]:
        """
        Get queue statistics

        Args:
            **params: Query parameters (limit, offset, etc.)

        Returns:
            Queue statistics
        """
        logger.log("Admin.get_queue_stats", {"params": params})
        query_string = self._build_query_string(params)
        return await self._http_client.get(f"/api/v1/status/queues{query_string}")

    async def get_queue_detail(self, name: str, **params: Any) -> Dict[str, Any]:
        """
        Get detailed statistics for a specific queue

        Args:
            name: Queue name
            **params: Query parameters

        Returns:
            Detailed queue statistics
        """
        logger.log("Admin.get_queue_detail", {"name": name, "params": params})
        query_string = self._build_query_string(params)
        return await self._http_client.get(f"/api/v1/status/queues/{quote(name)}{query_string}")

    async def get_analytics(self, **params: Any) -> Dict[str, Any]:
        """
        Get analytics data

        Args:
            **params: Query parameters (from, to, interval, etc.)

        Returns:
            Analytics data
        """
        logger.log("Admin.get_analytics", {"params": params})
        query_string = self._build_query_string(params)
        return await self._http_client.get(f"/api/v1/status/analytics{query_string}")

    # ===========================
    # Consumer Groups API
    # ===========================

    async def list_consumer_groups(self) -> Dict[str, Any]:
        """
        List all consumer groups

        Returns:
            List of consumer groups
        """
        logger.log("Admin.list_consumer_groups", {})
        return await self._http_client.get("/api/v1/consumer-groups")

    async def refresh_consumer_stats(self) -> Dict[str, Any]:
        """
        Refresh consumer group statistics

        Returns:
            Result
        """
        logger.log("Admin.refresh_consumer_stats", {})
        return await self._http_client.post("/api/v1/stats/refresh", {})

    async def get_consumer_group(self, name: str) -> Dict[str, Any]:
        """
        Get consumer group details

        Args:
            name: Consumer group name

        Returns:
            Consumer group details
        """
        logger.log("Admin.get_consumer_group", {"name": name})
        return await self._http_client.get(f"/api/v1/consumer-groups/{quote(name)}")

    async def get_lagging_consumers(self, min_lag_seconds: int = 60) -> Dict[str, Any]:
        """
        Get lagging consumer groups

        Args:
            min_lag_seconds: Minimum lag in seconds to be considered lagging

        Returns:
            List of lagging consumer groups
        """
        logger.log("Admin.get_lagging_consumers", {"min_lag_seconds": min_lag_seconds})
        return await self._http_client.get(f"/api/v1/consumer-groups/lagging?minLagSeconds={min_lag_seconds}")

    async def delete_consumer_group_for_queue(
        self, consumer_group: str, queue_name: str, delete_metadata: bool = True
    ) -> Dict[str, Any]:
        """
        Delete a consumer group for a specific queue

        Args:
            consumer_group: Consumer group name
            queue_name: Queue name
            delete_metadata: Whether to delete subscription metadata

        Returns:
            Result
        """
        logger.log(
            "Admin.delete_consumer_group_for_queue",
            {"consumer_group": consumer_group, "queue_name": queue_name, "delete_metadata": delete_metadata},
        )
        url = f"/api/v1/consumer-groups/{quote(consumer_group)}/queues/{quote(queue_name)}?deleteMetadata={str(delete_metadata).lower()}"
        return await self._http_client.delete(url)

    async def seek_consumer_group(
        self, consumer_group: str, queue_name: str, options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Seek consumer group offset for a queue

        Args:
            consumer_group: Consumer group name
            queue_name: Queue name
            options: Seek options (timestamp, position, etc.)

        Returns:
            Result
        """
        logger.log(
            "Admin.seek_consumer_group",
            {"consumer_group": consumer_group, "queue_name": queue_name, "options": options},
        )
        url = f"/api/v1/consumer-groups/{quote(consumer_group)}/queues/{quote(queue_name)}/seek"
        return await self._http_client.post(url, options or {})

    # ===========================
    # System API
    # ===========================

    async def health(self) -> Dict[str, Any]:
        """
        Health check

        Returns:
            Health status
        """
        logger.log("Admin.health", {})
        return await self._http_client.get("/health")

    async def metrics(self) -> str:
        """
        Get Prometheus metrics

        Returns:
            Raw metrics text
        """
        logger.log("Admin.metrics", {})
        return await self._http_client.get("/metrics")

    async def get_maintenance_mode(self) -> Dict[str, Any]:
        """
        Get push maintenance mode status

        Returns:
            Maintenance status
        """
        logger.log("Admin.get_maintenance_mode", {})
        return await self._http_client.get("/api/v1/system/maintenance")

    async def set_maintenance_mode(self, enabled: bool) -> Dict[str, Any]:
        """
        Set push maintenance mode

        Args:
            enabled: Enable or disable maintenance mode

        Returns:
            Result
        """
        logger.log("Admin.set_maintenance_mode", {"enabled": enabled})
        return await self._http_client.post("/api/v1/system/maintenance", {"enabled": enabled})

    async def get_pop_maintenance_mode(self) -> Dict[str, Any]:
        """
        Get pop maintenance mode status

        Returns:
            Maintenance status
        """
        logger.log("Admin.get_pop_maintenance_mode", {})
        return await self._http_client.get("/api/v1/system/maintenance/pop")

    async def set_pop_maintenance_mode(self, enabled: bool) -> Dict[str, Any]:
        """
        Set pop maintenance mode

        Args:
            enabled: Enable or disable pop maintenance mode

        Returns:
            Result
        """
        logger.log("Admin.set_pop_maintenance_mode", {"enabled": enabled})
        return await self._http_client.post("/api/v1/system/maintenance/pop", {"enabled": enabled})

    async def get_system_metrics(self, **params: Any) -> Dict[str, Any]:
        """
        Get system metrics (CPU, memory, connections, etc.)

        Args:
            **params: Query parameters (from, to, etc.)

        Returns:
            System metrics
        """
        logger.log("Admin.get_system_metrics", {"params": params})
        query_string = self._build_query_string(params)
        return await self._http_client.get(f"/api/v1/analytics/system-metrics{query_string}")

    async def get_worker_metrics(self, **params: Any) -> Dict[str, Any]:
        """
        Get worker metrics

        Args:
            **params: Query parameters

        Returns:
            Worker metrics
        """
        logger.log("Admin.get_worker_metrics", {"params": params})
        query_string = self._build_query_string(params)
        return await self._http_client.get(f"/api/v1/analytics/worker-metrics{query_string}")

    async def get_postgres_stats(self) -> Dict[str, Any]:
        """
        Get PostgreSQL statistics

        Returns:
            PostgreSQL stats
        """
        logger.log("Admin.get_postgres_stats", {})
        return await self._http_client.get("/api/v1/analytics/postgres-stats")

    # ===========================
    # Helper Methods
    # ===========================

    def _build_query_string(self, params: Dict[str, Any]) -> str:
        """Build query string from parameters"""
        filtered = {k: v for k, v in params.items() if v is not None}
        if not filtered:
            return ""
        return "?" + urlencode(filtered)

