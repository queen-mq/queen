"""
Transaction builder for atomic operations
"""

from typing import Any, Dict, List, Optional, Union

from ..utils import logger
from ..utils.uuid_gen import generate_uuid
from ..utils.validation import is_valid_uuid


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

    async def commit(self) -> Dict[str, Any]:
        """
        Commit transaction

        Returns:
            Transaction response

        Raises:
            Exception: If transaction fails
        """
        if not self._operations:
            logger.error("TransactionBuilder.commit", "No operations to commit")
            raise Exception("Transaction has no operations to commit")

        logger.log(
            "TransactionBuilder.commit",
            {"operation_count": len(self._operations), "required_leases": len(self._required_leases)},
        )

        try:
            result = await self._http_client.post(
                "/api/v1/transaction",
                {
                    "operations": self._operations,
                    "requiredLeases": list(set(self._required_leases)),  # Unique leases
                },
            )

            if not result.get("success"):
                logger.error("TransactionBuilder.commit", {"error": result.get("error")})
                raise Exception(result.get("error") or "Transaction failed")

            logger.log("TransactionBuilder.commit", {"status": "success"})
            return result
        except Exception as error:
            logger.error("TransactionBuilder.commit", {"error": str(error)})
            raise


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

