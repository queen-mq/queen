"""
Push builder for chaining callbacks
"""

from typing import Any, Callable, Dict, List, Optional

from ..utils import logger


class PushBuilder:
    """Push builder for chaining callbacks"""

    def __init__(
        self,
        http_client: Any,
        buffer_manager: Any,
        queue_name: str,
        partition: str,
        formatted_items: List[Dict[str, Any]],
        buffer_options: Optional[Dict[str, Any]],
    ):
        """
        Initialize push builder

        Args:
            http_client: HttpClient instance
            buffer_manager: BufferManager instance
            queue_name: Queue name
            partition: Partition name
            formatted_items: Formatted message items
            buffer_options: Buffer options (None if not buffering)
        """
        self._http_client = http_client
        self._buffer_manager = buffer_manager
        self._queue_name = queue_name
        self._partition = partition
        self._formatted_items = formatted_items
        self._buffer_options = buffer_options
        self._on_success_callback: Optional[Callable[[Any], Any]] = None
        self._on_error_callback: Optional[Callable[[Any, Exception], Any]] = None
        self._on_duplicate_callback: Optional[Callable[[Any, Exception], Any]] = None
        self._executed = False

    def on_success(self, callback: Callable[[Any], Any]) -> "PushBuilder":
        """Set success callback"""
        self._on_success_callback = callback
        return self

    def on_error(self, callback: Callable[[Any, Exception], Any]) -> "PushBuilder":
        """Set error callback"""
        self._on_error_callback = callback
        return self

    def on_duplicate(self, callback: Callable[[Any, Exception], Any]) -> "PushBuilder":
        """Set duplicate callback"""
        self._on_duplicate_callback = callback
        return self

    def __await__(self):
        """Make awaitable"""
        return self._execute().__await__()

    async def _execute(self) -> Any:
        """Execute push"""
        if self._executed:
            return None
        self._executed = True

        logger.log(
            "PushBuilder.execute",
            {
                "queue": self._queue_name,
                "partition": self._partition,
                "count": len(self._formatted_items),
                "buffered": self._buffer_options is not None,
            },
        )

        # Client-side buffering.
        #
        # add_message is AWAITED, and must be: it blocks while the buffer sits
        # at its max_size bound, which is the only thing standing between a
        # producer that outruns the flush pipeline and a process that grows
        # until it dies with every unflushed message inside it. Dropping the
        # coroutine here instead of awaiting it would be a silent no-push.
        #
        # A cancelled or timed-out push therefore RAISES rather than reporting
        # {"buffered": True}; because the items are handed over one at a time,
        # an earlier prefix of this push may already be buffered when that
        # happens (the Go reference appends a whole push atomically, this SDK's
        # add_message is per message).
        if self._buffer_options:
            queue_address = f"{self._queue_name}/{self._partition}"
            accepted: List[Dict[str, Any]] = []
            try:
                for item in self._formatted_items:
                    await self._buffer_manager.add_message(queue_address, item, self._buffer_options)
                    accepted.append(item)
            except Exception as error:
                # Report the items the buffer did NOT take, the same way the
                # immediate push below reports rejected ones. Counting them as
                # buffered would be the false success this bound removes.
                unbuffered = self._formatted_items[len(accepted):]
                if self._on_error_callback:
                    await self._on_error_callback(unbuffered, error)
                    return None
                raise

            result = {"buffered": True, "count": len(accepted)}

            logger.log("PushBuilder.execute", {"status": "buffered", "count": len(accepted)})

            if self._on_success_callback:
                await self._on_success_callback(accepted)

            return result

        # Immediate push
        try:
            results = await self._http_client.post("/api/v1/push", {"items": self._formatted_items})

            # Server returns an array of results with status for each item
            if isinstance(results, list):
                # Separate results by status
                successful = []
                duplicates = []
                failed = []

                for i, result in enumerate(results):
                    original_item = self._formatted_items[i]

                    if result.get("status") == "duplicate":
                        duplicates.append({**original_item, "result": result})
                    elif result.get("status") == "failed":
                        failed.append({**original_item, "result": result, "error": result.get("error")})
                    elif result.get("status") == "queued":
                        successful.append({**original_item, "result": result})

                # Call appropriate callbacks
                if duplicates and self._on_duplicate_callback:
                    await self._on_duplicate_callback(
                        duplicates, Exception("Duplicate transaction IDs detected")
                    )

                if failed and self._on_error_callback:
                    error = Exception(failed[0].get("error") or "Push failed")
                    await self._on_error_callback(failed, error)

                if successful and self._on_success_callback:
                    await self._on_success_callback(successful)

                # Only raise if no error callback is defined
                if failed and not self._on_error_callback:
                    logger.error("PushBuilder.execute", {"status": "failed", "count": len(failed)})
                    raise Exception(failed[0].get("error") or "Push failed")

                logger.log(
                    "PushBuilder.execute",
                    {
                        "status": "success",
                        "successful": len(successful),
                        "duplicates": len(duplicates),
                        "failed": len(failed),
                    },
                )
                return results

            # Fallback for non-array responses
            if results and isinstance(results, dict) and results.get("error"):
                error = Exception(results["error"])
                if self._on_error_callback:
                    await self._on_error_callback(self._formatted_items, error)
                    return results  # Don't raise if callback is defined
                raise error

            if self._on_success_callback:
                await self._on_success_callback(self._formatted_items)

            return results

        except Exception as error:
            # Network or HTTP errors
            if self._on_error_callback:
                await self._on_error_callback(self._formatted_items, error)
                return None  # Don't raise if callback is defined
            raise

