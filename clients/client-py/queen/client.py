"""
Queen Message Queue Client - Version 2
Clean, fluent API with smart defaults
"""

import asyncio
import signal
import sys
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

from .admin.admin import Admin
from .buffer.buffer_manager import BufferManager
from .builders.queue_builder import QueueBuilder
from .builders.transaction_builder import TransactionBuilder
from .http.http_client import HttpClient
from .http.load_balancer import LoadBalancer
from .utils import logger
from .utils.defaults import CLIENT_DEFAULTS
from .utils.validation import validate_url, validate_urls


class Queen:
    """Queen Message Queue Client - Version 2"""

    def __init__(
        self,
        config: Union[str, List[str], Dict[str, Any], None] = None,
        *,
        urls: Optional[List[str]] = None,
        url: Optional[str] = None,
        timeout_millis: Optional[int] = None,
        retry_attempts: Optional[int] = None,
        retry_delay_millis: Optional[int] = None,
        load_balancing_strategy: Optional[str] = None,
        affinity_hash_ring: Optional[int] = None,
        enable_failover: Optional[bool] = None,
        health_retry_after_millis: Optional[int] = None,
        bearer_token: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize Queen client

        Args:
            config: Single URL, list of URLs, or config dict
            urls: List of server URLs (keyword-only)
            url: Single server URL (keyword-only)
            timeout_millis: Request timeout in milliseconds
            retry_attempts: Number of retry attempts
            retry_delay_millis: Initial retry delay (exponential backoff)
            load_balancing_strategy: 'affinity', 'round-robin', or 'session'
            affinity_hash_ring: Virtual nodes per server for affinity
            enable_failover: Enable automatic failover
            health_retry_after_millis: Retry unhealthy backends after N ms
            bearer_token: Bearer token for proxy authentication
            headers: Custom headers to include in every request
        """
        logger.log(
            "Queen.constructor",
            {
                "config": (
                    {**config, "urls": len(config.get("urls", []))}
                    if isinstance(config, dict)
                    else {"type": type(config).__name__}
                )
            },
        )

        # Normalize config
        self._config = self._normalize_config(
            config,
            urls=urls,
            url=url,
            timeout_millis=timeout_millis,
            retry_attempts=retry_attempts,
            retry_delay_millis=retry_delay_millis,
            load_balancing_strategy=load_balancing_strategy,
            affinity_hash_ring=affinity_hash_ring,
            enable_failover=enable_failover,
            health_retry_after_millis=health_retry_after_millis,
            bearer_token=bearer_token,
            headers=headers,
        )

        # Create HTTP client
        self._http_client = self._create_http_client()

        # Create buffer manager
        self._buffer_manager = BufferManager(self._http_client)

        # Admin API (lazily initialized)
        self._admin: Optional[Admin] = None

        # Setup graceful shutdown
        self._shutdown_handlers: List[Any] = []
        self._setup_graceful_shutdown()

        logger.log("Queen.constructor", {"status": "initialized", "urls": len(self._config["urls"])})

    def _normalize_config(
        self,
        config: Union[str, List[str], Dict[str, Any], None],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Normalize configuration"""
        # Handle different input formats
        if isinstance(config, str):
            # Single URL string
            return {**CLIENT_DEFAULTS, "urls": [validate_url(config)]}

        if isinstance(config, list):
            # Array of URLs
            return {**CLIENT_DEFAULTS, "urls": validate_urls(config)}

        # Object config
        normalized = {**CLIENT_DEFAULTS}

        if isinstance(config, dict):
            normalized.update(config)

        # Override with keyword arguments
        for key, value in kwargs.items():
            if value is not None:
                normalized[key] = value

        # Ensure URLs are validated
        if "urls" in normalized:
            normalized["urls"] = validate_urls(normalized["urls"])
        elif "url" in normalized:
            normalized["urls"] = [validate_url(normalized["url"])]
        else:
            raise ValueError("Must provide urls or url in configuration")

        return normalized

    def _create_http_client(self) -> HttpClient:
        """Create HTTP client with optional load balancer"""
        urls = self._config["urls"]
        timeout_millis = self._config["timeout_millis"]
        retry_attempts = self._config["retry_attempts"]
        retry_delay_millis = self._config["retry_delay_millis"]
        load_balancing_strategy = self._config["load_balancing_strategy"]
        affinity_hash_ring = self._config["affinity_hash_ring"]
        health_retry_after_millis = self._config["health_retry_after_millis"]
        enable_failover = self._config["enable_failover"]
        bearer_token = self._config.get("bearer_token")
        custom_headers = self._config.get("headers") or {}

        if len(urls) == 1:
            # Single server
            return HttpClient(
                base_url=urls[0],
                timeout_millis=timeout_millis,
                retry_attempts=retry_attempts,
                retry_delay_millis=retry_delay_millis,
                bearer_token=bearer_token,
                headers=custom_headers,
            )

        # Multiple servers with load balancing
        load_balancer = LoadBalancer(
            urls,
            load_balancing_strategy,
            affinity_hash_ring=affinity_hash_ring,
            health_retry_after_millis=health_retry_after_millis,
        )
        return HttpClient(
            load_balancer=load_balancer,
            timeout_millis=timeout_millis,
            retry_attempts=retry_attempts,
            retry_delay_millis=retry_delay_millis,
            enable_failover=enable_failover,
            bearer_token=bearer_token,
            headers=custom_headers,
        )

    def _setup_graceful_shutdown(self) -> None:
        """Setup signal handlers for graceful shutdown"""
        signal_count = 0

        def shutdown_handler(signum: int, frame: Any) -> None:
            nonlocal signal_count
            signal_count += 1
            signal_name = signal.Signals(signum).name
            print(f"\nReceived {signal_name}, shutting down gracefully...")

            if signal_count > 1:
                print("Received multiple shutdown signals, exiting immediately")
                sys.exit(1)

            # Schedule coroutine in event loop
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(self._async_shutdown())
                else:
                    loop.run_until_complete(self.close())
                    sys.exit(0)
            except Exception as e:
                print(f"Error during shutdown: {e}")
                sys.exit(1)

        # Register signal handlers
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        # Store handlers for cleanup
        self._shutdown_handlers = [
            lambda: signal.signal(signal.SIGINT, signal.SIG_DFL),
            lambda: signal.signal(signal.SIGTERM, signal.SIG_DFL),
        ]

    async def _async_shutdown(self) -> None:
        """Async shutdown helper"""
        try:
            await self.close()
            sys.exit(0)
        except Exception as e:
            print(f"Error during shutdown: {e}")
            sys.exit(1)

    # ===========================
    # Queue Builder Entry Point
    # ===========================

    def queue(self, name: Optional[str] = None) -> QueueBuilder:
        """
        Get a queue builder for fluent API

        Args:
            name: Queue name

        Returns:
            QueueBuilder instance
        """
        return QueueBuilder(self, self._http_client, self._buffer_manager, name)

    # ===========================
    # Admin API Entry Point
    # ===========================

    @property
    def admin(self) -> Admin:
        """
        Get the Admin API for administrative and observability operations

        Returns:
            Admin API instance (lazily initialized, singleton)
        """
        if self._admin is None:
            self._admin = Admin(self._http_client)
        return self._admin

    # ===========================
    # Transaction API
    # ===========================

    def transaction(self) -> TransactionBuilder:
        """
        Start a new transaction builder

        Returns:
            TransactionBuilder instance
        """
        return TransactionBuilder(self._http_client)

    # ===========================
    # Direct ACK API
    # ===========================

    async def ack(
        self,
        message: Union[Any, List[Any]],
        status: Union[bool, str] = True,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Acknowledge one or more messages

        Args:
            message: Single message or list of messages
            status: True for success, False for retry, or status string
            context: Optional context (e.g., {'group': 'consumer-group'})

        Returns:
            Acknowledgment response
        """
        is_batch = isinstance(message, list)
        ctx = context or {}
        logger.log("Queen.ack", {"is_batch": is_batch, "count": len(message) if is_batch else 1, "status": status, "context": ctx})

        # Handle batch acknowledgment
        if isinstance(message, list):
            if not message:
                return {"processed": 0, "results": []}

            # Check if messages have individual status
            has_individual_status = any(
                isinstance(msg, dict) and ("_status" in msg or "_error" in msg) for msg in message
            )

            acknowledgments = []

            if has_individual_status:
                # Each message has its own status
                for msg in message:
                    transaction_id = msg if isinstance(msg, str) else (msg.get("transactionId") or msg.get("id"))
                    partition_id = msg.get("partitionId") if isinstance(msg, dict) else None
                    lease_id = msg.get("leaseId") if isinstance(msg, dict) else None

                    if not transaction_id:
                        raise ValueError("Message must have transactionId or id property")

                    # CRITICAL: partitionId is now MANDATORY
                    if not partition_id:
                        raise ValueError(
                            "Message must have partitionId property to ensure message uniqueness"
                        )

                    msg_status = msg.get("_status", status)
                    status_str = "completed" if msg_status is True else ("failed" if msg_status is False else msg_status)

                    ack = {
                        "transactionId": transaction_id,
                        "partitionId": partition_id,
                        "status": status_str,
                        "error": msg.get("_error") or ctx.get("error"),
                    }

                    if lease_id:
                        ack["leaseId"] = lease_id

                    acknowledgments.append(ack)
            else:
                # Same status for all messages
                status_str = "completed" if status is True else ("failed" if status is False else status)

                for msg in message:
                    transaction_id = msg if isinstance(msg, str) else (msg.get("transactionId") or msg.get("id"))
                    partition_id = msg.get("partitionId") if isinstance(msg, dict) else None
                    lease_id = msg.get("leaseId") if isinstance(msg, dict) else None

                    if not transaction_id:
                        raise ValueError("Message must have transactionId or id property")

                    # CRITICAL: partitionId is now MANDATORY
                    if not partition_id:
                        raise ValueError(
                            "Message must have partitionId property to ensure message uniqueness"
                        )

                    ack = {
                        "transactionId": transaction_id,
                        "partitionId": partition_id,
                        "status": status_str,
                        "error": ctx.get("error"),
                    }

                    if lease_id:
                        ack["leaseId"] = lease_id

                    acknowledgments.append(ack)

            # Call batch ack endpoint
            try:
                result = await self._http_client.post(
                    "/api/v1/ack/batch",
                    {"acknowledgments": acknowledgments, "consumerGroup": ctx.get("group")},
                )

                # Server returns an array of results for batch ack
                # Check if any result has an error
                if isinstance(result, dict) and result.get("error"):
                    logger.error("Queen.ack", {"type": "batch", "error": result["error"]})
                    return {"success": False, "error": result["error"]}

                logger.log("Queen.ack", {"type": "batch", "success": True, "count": len(acknowledgments)})
                # Return the array of results
                return {"success": True, "results": result} if result else {"success": True}
            except Exception as error:
                logger.error("Queen.ack", {"type": "batch", "error": str(error)})
                return {"success": False, "error": str(error)}

        # Handle single message acknowledgment
        transaction_id = message if isinstance(message, str) else (message.get("transactionId") or message.get("id"))
        partition_id = message.get("partitionId") if isinstance(message, dict) else None
        lease_id = message.get("leaseId") if isinstance(message, dict) else None

        if not transaction_id:
            return {"success": False, "error": "Message must have transactionId or id property"}

        # CRITICAL: partitionId is now MANDATORY
        if not partition_id:
            return {
                "success": False,
                "error": "Message must have partitionId property to ensure message uniqueness",
            }

        status_str = "completed" if status is True else ("failed" if status is False else status)

        body = {
            "transactionId": transaction_id,
            "partitionId": partition_id,
            "status": status_str,
            "error": ctx.get("error"),
            "consumerGroup": ctx.get("group"),
        }

        if lease_id:
            body["leaseId"] = lease_id

        try:
            result = await self._http_client.post("/api/v1/ack", body)

            # Server returns an array with single element for single ACK
            if isinstance(result, list):
                if result and len(result) > 0:
                    first_result = result[0]
                    if first_result.get("success"):
                        logger.log("Queen.ack", {"type": "single", "transaction_id": transaction_id, "success": True})
                        return {"success": True, **first_result}
                    else:
                        error = first_result.get("error", "ACK failed")
                        logger.error("Queen.ack", {"type": "single", "transaction_id": transaction_id, "error": error})
                        return {"success": False, "error": error}
                return {"success": True}
            
            # Handle dict response (error case)
            if isinstance(result, dict) and result.get("error"):
                logger.error("Queen.ack", {"type": "single", "transaction_id": transaction_id, "error": result["error"]})
                return {"success": False, "error": result["error"]}

            logger.log("Queen.ack", {"type": "single", "transaction_id": transaction_id, "success": True})
            return {"success": True, **result} if result else {"success": True}
        except Exception as error:
            logger.error("Queen.ack", {"type": "single", "transaction_id": transaction_id, "error": str(error)})
            return {"success": False, "error": str(error)}

    # ===========================
    # Lease Renewal API
    # ===========================

    async def renew(self, message_or_lease_id: Union[str, Any, List[Any]]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Renew lease for one or more messages

        Args:
            message_or_lease_id: Lease ID, message, or list of messages

        Returns:
            Renewal response or list of responses
        """
        lease_ids: List[str] = []

        if isinstance(message_or_lease_id, str):
            lease_ids = [message_or_lease_id]
        elif isinstance(message_or_lease_id, list):
            lease_ids = [
                item if isinstance(item, str) else item.get("leaseId")
                for item in message_or_lease_id
                if isinstance(item, str) or item.get("leaseId")
            ]
        elif isinstance(message_or_lease_id, dict):
            lease_id = message_or_lease_id.get("leaseId")
            if lease_id:
                lease_ids = [lease_id]

        # Dedupe: with v4 multi-partition pop, all messages in one batch share
        # the same leaseId (one renew_lease_v2 call extends every claimed
        # partition_consumers row). dict.fromkeys preserves insertion order.
        lease_ids = list(dict.fromkeys(lease_ids))

        if not lease_ids:
            logger.warn("Queen.renew", "No valid lease IDs found for renewal")
            return {"success": False, "error": "No valid lease IDs found for renewal"}

        logger.log("Queen.renew", {"count": len(lease_ids)})

        results = []
        for lease_id in lease_ids:
            try:
                result = await self._http_client.post(f"/api/v1/lease/{lease_id}/extend", {})
                results.append(
                    {
                        "leaseId": lease_id,
                        "success": True,
                        "newExpiresAt": result.get("newExpiresAt") or result.get("lease_expires_at"),
                    }
                )
                logger.log("Queen.renew", {"lease_id": lease_id, "success": True})
            except Exception as error:
                results.append({"leaseId": lease_id, "success": False, "error": str(error)})
                logger.error("Queen.renew", {"lease_id": lease_id, "error": str(error)})

        logger.log("Queen.renew", {"total": len(results), "successful": sum(1 for r in results if r["success"])})
        return results if isinstance(message_or_lease_id, list) else results[0]

    # ===========================
    # Buffer Management API
    # ===========================

    async def flush_all_buffers(self) -> None:
        """Flush all buffered messages"""
        logger.log("Queen.flushAllBuffers", "Starting flush of all buffers")
        await self._buffer_manager.flush_all_buffers()
        logger.log("Queen.flushAllBuffers", "Completed")

    def get_buffer_stats(self) -> Dict[str, Any]:
        """
        Get buffer statistics

        Returns:
            Buffer stats
        """
        stats = self._buffer_manager.get_stats()
        logger.log("Queen.getBufferStats", stats)
        return stats

    # ===========================
    # Consumer Group Management
    # ===========================

    async def delete_consumer_group(self, consumer_group: str, delete_metadata: bool = True) -> Dict[str, Any]:
        """
        Delete a consumer group and optionally its subscription metadata

        Args:
            consumer_group: Consumer group name
            delete_metadata: Whether to delete subscription metadata (default: True)

        Returns:
            Server response
        """
        logger.log("Queen.deleteConsumerGroup", {"consumer_group": consumer_group, "delete_metadata": delete_metadata})

        url = f"/api/v1/consumer-groups/{quote(consumer_group)}?deleteMetadata={str(delete_metadata).lower()}"
        response = await self._http_client.delete(url)

        logger.log("Queen.deleteConsumerGroup", {"success": True, "consumer_group": consumer_group})
        return response or {"success": True}

    async def update_consumer_group_timestamp(
        self, consumer_group: str, timestamp: str
    ) -> Dict[str, Any]:
        """
        Update subscription timestamp for a consumer group

        Args:
            consumer_group: Consumer group name
            timestamp: New subscription timestamp (ISO 8601)

        Returns:
            Server response
        """
        logger.log("Queen.updateConsumerGroupTimestamp", {"consumer_group": consumer_group, "timestamp": timestamp})

        url = f"/api/v1/consumer-groups/{quote(consumer_group)}/subscription"
        response = await self._http_client.post(url, {"subscriptionTimestamp": timestamp})

        logger.log("Queen.updateConsumerGroupTimestamp", {"success": True, "consumer_group": consumer_group})
        return response or {"success": True}


    # ===========================
    # Graceful Shutdown
    # ===========================

    async def close(self) -> None:
        """Gracefully shutdown the client"""
        logger.log("Queen.close", "Starting shutdown")
        print("Closing Queen client...")

        # Flush all buffers
        try:
            await self._buffer_manager.flush_all_buffers()
            logger.log("Queen.close", "All buffers flushed")
            print("All buffers flushed")
        except Exception as error:
            logger.error("Queen.close", {"error": str(error), "phase": "buffer-flush"})
            print(f"Error flushing buffers: {error}")

        # Cleanup buffer manager
        self._buffer_manager.cleanup()

        # Close HTTP client
        await self._http_client.close()

        # Remove shutdown handlers
        for cleanup in self._shutdown_handlers:
            cleanup()
        self._shutdown_handlers = []

        logger.log("Queen.close", "Client closed successfully")
        print("Queen client closed")

    # ===========================
    # Context Manager Support
    # ===========================

    async def __aenter__(self) -> "Queen":
        """Support async context manager"""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Cleanup on context exit"""
        await self.close()

