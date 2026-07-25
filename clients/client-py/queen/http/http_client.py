"""
HTTP client with retry, load balancing, and failover support
"""

import asyncio
import random
from typing import Any, Dict, Optional, Tuple, Union
import httpx

from ..utils import logger
from .load_balancer import LoadBalancer


class HttpClient:
    """HTTP client with retry, load balancing, and failover support"""

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        load_balancer: Optional[LoadBalancer] = None,
        timeout_millis: int = 30000,
        retry_attempts: int = 3,
        retry_delay_millis: int = 1000,
        enable_failover: bool = True,
        bearer_token: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        retry_429: Optional[Dict[str, Any]] = None,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        """
        Initialize HTTP client

        Args:
            base_url: Base URL for single server
            load_balancer: LoadBalancer instance for multiple servers
            timeout_millis: Request timeout in milliseconds
            retry_attempts: Number of retry attempts
            retry_delay_millis: Initial retry delay (exponential backoff)
            enable_failover: Enable automatic failover
            bearer_token: Bearer token for proxy authentication
            headers: Custom headers to include in every request
            retry_429: Backoff policy for HTTP 429 (rate limited) responses,
                separate from retry_attempts/retry_delay_millis above. See
                queen.types.Retry429Config for the shape and defaults.
                max_attempts defaults to 10 for ordinary requests; a
                long-poll pop (retry_kind="pop") retries unboundedly unless
                max_attempts is set here, in which case it applies to both.
                PLAN_QUEEN_PROXY_CLOUD.md §4/§9 (client 429 backoff, B4).
            transport: Optional httpx transport override (e.g.
                httpx.MockTransport) for tests -- no broker/network required.
                None uses httpx's normal transport.
        """
        self._base_url = base_url
        self._load_balancer = load_balancer
        self._timeout_millis = timeout_millis
        self._retry_attempts = retry_attempts
        self._retry_delay_millis = retry_delay_millis
        self._enable_failover = enable_failover
        self._bearer_token = bearer_token
        self._retry_429: Dict[str, Any] = dict(retry_429) if retry_429 else {}

        # Build headers with optional auth
        merged_headers: Dict[str, str] = {}
        if bearer_token:
            merged_headers["Authorization"] = f"Bearer {bearer_token}"
        if headers:
            merged_headers.update(headers)

        # Create httpx.AsyncClient (persistent connection pool)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_millis / 1000.0),
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=100),
            headers=merged_headers,
            transport=transport,
        )

        logger.log(
            "HttpClient.constructor",
            {
                "has_load_balancer": load_balancer is not None,
                "base_url": base_url or "load-balanced",
                "timeout_millis": timeout_millis,
                "retry_attempts": retry_attempts,
                "enable_failover": enable_failover,
                "has_auth": bearer_token is not None,
                "custom_headers": len(headers) if headers else 0,
                "retry_429": self._retry_429,
            },
        )

    def _retry429_policy_for(self, retry_kind: Optional[str]) -> Tuple[Optional[int], int, int]:
        """Resolve the effective 429 retry policy for a request kind.

        - "pop": long-poll pop (wait=True). Unbounded attempts (None) by
          default -- the long-poll loop is meant to keep waiting through
          transient rate limiting -- unless retry_429["max_attempts"] was
          explicitly configured.
        - anything else (push, admin calls, non-waiting pop, ...): bounded,
          defaults to 10 attempts.

        base_ms/cap_ms always default to 500/30000 and apply to both kinds.
        Returns (max_attempts_or_None, base_ms, cap_ms).
        """
        cfg = self._retry_429 or {}
        base_ms = cfg.get("base_ms", 500)
        cap_ms = cfg.get("cap_ms", 30000)
        configured_max = cfg.get("max_attempts")
        if configured_max is not None:
            max_attempts = configured_max
        else:
            max_attempts = None if retry_kind == "pop" else 10
        return max_attempts, base_ms, cap_ms

    @staticmethod
    def _compute_retry429_delay(
        attempt_index: int,
        retry_after_seconds: Optional[float],
        base_ms: int,
        cap_ms: int,
    ) -> float:
        """Delay (seconds, for asyncio.sleep) before the next 429 retry.

        Honors Retry-After (seconds) when the server sent one, with +-20%
        jitter to avoid a synchronized thundering herd; otherwise falls back
        to exponential backoff (base_ms * 2^attempt, capped at cap_ms), also
        jittered +-20%.
        """
        if retry_after_seconds is not None and retry_after_seconds >= 0:
            delay_ms = retry_after_seconds * 1000.0
        else:
            delay_ms = min(cap_ms, base_ms * (2 ** attempt_index))
        jitter_multiplier = 1 + random.uniform(-0.2, 0.2)
        return max(0.0, (delay_ms * jitter_multiplier) / 1000.0)

    async def _execute_with_retry429(
        self,
        url: str,
        method: str,
        body: Optional[Dict[str, Any]],
        request_timeout_millis: Optional[int],
        retry_kind: Optional[str],
    ) -> Any:
        """Run a single logical request against one URL, transparently
        retrying HTTP 429 responses with backoff until the policy for
        `retry_kind` is exhausted (or never, for an unbounded pop policy).
        Any other exception is raised straight through -- 429 is the only
        status this layer treats as retryable; 5xx/network retry and
        cross-backend failover are the caller's job.
        """
        max_attempts, base_ms, cap_ms = self._retry429_policy_for(retry_kind)
        tries = 0
        while True:
            tries += 1
            try:
                return await self._execute_request(url, method, body, request_timeout_millis)
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 429:
                    raise

                if max_attempts is not None and tries >= max_attempts:
                    logger.error(
                        "HttpClient.retry429",
                        {
                            "method": method,
                            "url": url,
                            "error": "max 429 attempts exhausted",
                            "attempts": tries,
                            "code": getattr(error, "code", None),
                        },
                    )
                    raise

                delay = self._compute_retry429_delay(
                    tries - 1, getattr(error, "retry_after_seconds", None), base_ms, cap_ms
                )
                logger.warn(
                    "HttpClient.retry429",
                    {
                        "method": method,
                        "url": url,
                        "attempt": tries,
                        "retry_kind": retry_kind or "default",
                        "next_delay_s": delay,
                        "retry_after_seconds": getattr(error, "retry_after_seconds", None),
                        "code": getattr(error, "code", None),
                    },
                )
                await asyncio.sleep(delay)

    async def get(
        self,
        path: str,
        request_timeout_millis: Optional[int] = None,
        affinity_key: Optional[str] = None,
        *,
        retry_kind: Optional[str] = None,
    ) -> Any:
        """GET request.

        retry_kind: pass "pop" for long-poll (wait=True) pop requests to get
        the unbounded-with-backoff 429 policy; omit for everything else
        (push, admin calls, non-waiting pop), which get the bounded default
        (10 attempts).
        """
        return await self._request_with_failover(
            "GET", path, None, request_timeout_millis, affinity_key, retry_kind
        )

    async def post(
        self,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        request_timeout_millis: Optional[int] = None,
        affinity_key: Optional[str] = None,
        *,
        retry_kind: Optional[str] = None,
    ) -> Any:
        """POST request"""
        return await self._request_with_failover(
            "POST", path, body, request_timeout_millis, affinity_key, retry_kind
        )

    async def put(
        self,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        request_timeout_millis: Optional[int] = None,
        affinity_key: Optional[str] = None,
        *,
        retry_kind: Optional[str] = None,
    ) -> Any:
        """PUT request"""
        return await self._request_with_failover(
            "PUT", path, body, request_timeout_millis, affinity_key, retry_kind
        )

    async def delete(
        self,
        path: str,
        request_timeout_millis: Optional[int] = None,
        affinity_key: Optional[str] = None,
        *,
        retry_kind: Optional[str] = None,
    ) -> Any:
        """DELETE request"""
        return await self._request_with_failover(
            "DELETE", path, None, request_timeout_millis, affinity_key, retry_kind
        )

    async def _execute_request(
        self,
        url: str,
        method: str,
        body: Optional[Dict[str, Any]],
        request_timeout_millis: Optional[int],
    ) -> Any:
        """Execute single HTTP request"""
        effective_timeout = request_timeout_millis or self._timeout_millis
        logger.log(
            "HttpClient.request",
            {"method": method, "url": url, "has_body": body is not None, "timeout": effective_timeout},
        )

        try:
            # Set timeout for this specific request
            timeout = httpx.Timeout(effective_timeout / 1000.0)

            # Prepare request
            kwargs: Dict[str, Any] = {
                "method": method,
                "url": url,
                "timeout": timeout,
            }

            if body:
                kwargs["json"] = body

            response = await self._client.request(**kwargs)

            logger.log("HttpClient.response", {"method": method, "url": url, "status": response.status_code})

            # Handle 204 No Content
            if response.status_code == 204:
                return None

            # Handle errors
            if not response.is_success:
                error_msg = f"HTTP {response.status_code}: {response.reason_phrase}"
                code: Optional[str] = None
                try:
                    text = response.text
                    if text:
                        body_data = response.json()
                        error_msg = body_data.get("error", error_msg)
                        code = body_data.get("code")
                except Exception:
                    pass

                logger.error(
                    "HttpClient.request",
                    {"method": method, "url": url, "status": response.status_code, "error": error_msg, "code": code},
                )
                error = httpx.HTTPStatusError(error_msg, request=response.request, response=response)
                # Proxy error contract: 429 {"error", "code": "rate_limited" |
                # "quota_exceeded"} with Retry-After (seconds); 403 {"error",
                # "code": "cluster_suspended" | "storage_quota_exceeded" |
                # "feature_gated" | "forbidden"}. These attributes are always
                # set (code may be None) so callers can branch without
                # string-matching the message.
                error.code = code  # type: ignore[attr-defined]
                error.retry_after_seconds = None  # type: ignore[attr-defined]
                if response.status_code == 429:
                    retry_after = response.headers.get("retry-after")
                    if retry_after is not None:
                        try:
                            error.retry_after_seconds = float(retry_after)  # type: ignore[attr-defined]
                        except (TypeError, ValueError):
                            error.retry_after_seconds = None  # type: ignore[attr-defined]
                raise error

            # Parse successful response
            content_type = response.headers.get("content-type", "")
            content_length = response.headers.get("content-length", "")

            if (
                not content_type
                or "application/json" not in content_type
                or content_length == "0"
            ):
                text = response.text
                if not text:
                    return None
                try:
                    return response.json()
                except Exception:
                    return None

            return response.json()

        except httpx.TimeoutException as e:
            logger.error(
                "HttpClient.request",
                {"method": method, "url": url, "error": "timeout", "timeout": effective_timeout},
            )
            raise
        except Exception as e:
            logger.error("HttpClient.request", {"method": method, "url": url, "error": str(e)})
            raise

    async def _request_with_retry(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]],
        request_timeout_millis: Optional[int],
        retry_kind: Optional[str] = None,
    ) -> Any:
        """Execute request with retry logic"""
        last_error: Optional[Exception] = None

        for attempt in range(self._retry_attempts):
            try:
                url = self._get_url() + path
                return await self._execute_with_retry429(url, method, body, request_timeout_millis, retry_kind)
            except Exception as error:
                last_error = error

                # Don't retry on client errors (4xx)
                if isinstance(error, httpx.HTTPStatusError):
                    status = error.response.status_code
                    if 400 <= status < 500:
                        raise error

                # Wait before retry (except on last attempt)
                if attempt < self._retry_attempts - 1:
                    delay = (self._retry_delay_millis / 1000.0) * (2**attempt)
                    logger.warn(
                        "HttpClient.retry",
                        {
                            "method": method,
                            "path": path,
                            "attempt": attempt + 1,
                            "delay": delay,
                            "error": str(error),
                        },
                    )
                    await asyncio.sleep(delay)

        logger.error(
            "HttpClient.retry",
            {"method": method, "path": path, "error": "Max retries exceeded", "attempts": self._retry_attempts},
        )
        if last_error:
            raise last_error
        raise Exception("Max retries exceeded")

    async def _request_with_failover(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]],
        request_timeout_millis: Optional[int],
        affinity_key: Optional[str],
        retry_kind: Optional[str] = None,
    ) -> Any:
        """Execute request with failover logic"""
        if not self._load_balancer or not self._enable_failover:
            return await self._request_with_retry(method, path, body, request_timeout_millis, retry_kind)

        urls = self._load_balancer.get_all_urls()
        attempted_urls = set()
        last_error: Optional[Exception] = None

        logger.log(
            "HttpClient.failover",
            {"method": method, "path": path, "total_servers": len(urls), "affinity_key": affinity_key},
        )

        for _ in range(len(urls)):
            # Pass affinity key to load balancer for consistent routing
            url = self._load_balancer.get_next_url(affinity_key)

            if url in attempted_urls:
                continue

            attempted_urls.add(url)

            try:
                # 429s are retried in place (same backend, backoff-paced)
                # inside _execute_with_retry429 -- they are not a
                # backend-health signal, so they must not trigger failover
                # to a different server.
                result = await self._execute_with_retry429(
                    url + path, method, body, request_timeout_millis, retry_kind
                )

                # Mark backend as healthy on success
                self._load_balancer.mark_healthy(url)

                return result
            except Exception as error:
                last_error = error

                # Mark backend as unhealthy on failure (5xx or network errors)
                if isinstance(error, httpx.HTTPStatusError):
                    status = error.response.status_code
                    if status >= 500:
                        self._load_balancer.mark_unhealthy(url)
                elif isinstance(error, (httpx.NetworkError, httpx.TimeoutException)):
                    self._load_balancer.mark_unhealthy(url)

                logger.warn(
                    "HttpClient.failover", {"url": url, "method": method, "path": path, "error": str(error)}
                )
                print(f"Request failed for {url}: {method} {path} - {error}")

                # Don't retry on client errors (4xx)
                if isinstance(error, httpx.HTTPStatusError):
                    status = error.response.status_code
                    if 400 <= status < 500:
                        raise error

                # Continue to next server for server errors or network issues

        logger.error(
            "HttpClient.failover",
            {"method": method, "path": path, "error": "All servers failed", "attempted": len(attempted_urls)},
        )
        if last_error:
            raise last_error
        raise Exception("All servers failed")

    def _get_url(self) -> str:
        """Get URL from load balancer or base URL"""
        if self._load_balancer:
            return self._load_balancer.get_next_url()
        return self._base_url or ""

    def get_load_balancer(self) -> Optional[LoadBalancer]:
        """Get load balancer instance"""
        return self._load_balancer

    async def close(self) -> None:
        """Close HTTP client and connection pool"""
        await self._client.aclose()

