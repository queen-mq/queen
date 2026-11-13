"""
Operation builder for create/delete operations with callbacks
"""

from typing import Any, Callable, Dict, Optional

from ..utils import logger


class OperationBuilder:
    """Operation builder for create/delete operations with callbacks"""

    def __init__(
        self,
        http_client: Any,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]],
        check: Optional[Callable[[Any], None]] = None,
    ):
        """
        Initialize operation builder

        Args:
            http_client: HttpClient instance
            method: HTTP method
            path: Request path
            body: Request body
            check: Optional check of a successful answer; what it raises is
                the operation's error (on_error, or raised)
        """
        self._http_client = http_client
        self._method = method
        self._path = path
        self._body = body
        self._check = check
        self._on_success_callback: Optional[Callable[[Any], Any]] = None
        self._on_error_callback: Optional[Callable[[Exception], Any]] = None
        self._executed = False

    def on_success(self, callback: Callable[[Any], Any]) -> "OperationBuilder":
        """
        Set success callback

        Args:
            callback: Success callback

        Returns:
            Self for chaining
        """
        self._on_success_callback = callback
        return self

    def on_error(self, callback: Callable[[Exception], Any]) -> "OperationBuilder":
        """
        Set error callback

        Args:
            callback: Error callback

        Returns:
            Self for chaining
        """
        self._on_error_callback = callback
        return self

    def __await__(self):
        """Make awaitable"""
        return self._execute().__await__()

    async def _execute(self) -> Any:
        """Execute operation"""
        if self._executed:
            return None
        self._executed = True

        logger.log("OperationBuilder.execute", {"method": self._method, "path": self._path})

        try:
            if self._method == "GET":
                result = await self._http_client.get(self._path)
            elif self._method == "POST":
                result = await self._http_client.post(self._path, self._body)
            elif self._method == "PUT":
                result = await self._http_client.put(self._path, self._body)
            elif self._method == "DELETE":
                result = await self._http_client.delete(self._path)
            else:
                raise ValueError(f"Unsupported method: {self._method}")

            if result and isinstance(result, dict) and result.get("error"):
                error = Exception(result["error"])
                logger.error(
                    "OperationBuilder.execute",
                    {"method": self._method, "path": self._path, "error": result["error"]},
                )
                if self._on_error_callback:
                    await self._on_error_callback(error)
                    return {"success": False, "error": result["error"]}
                raise error

            if self._check is not None:
                self._check(result)

            logger.log("OperationBuilder.execute", {"method": self._method, "path": self._path, "status": "success"})
            if self._on_success_callback:
                await self._on_success_callback(result)

            return result

        except Exception as error:
            logger.error("OperationBuilder.execute", {"method": self._method, "path": self._path, "error": str(error)})
            if self._on_error_callback:
                await self._on_error_callback(error)
                return {"success": False, "error": str(error)}
            raise

