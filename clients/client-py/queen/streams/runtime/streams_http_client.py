"""
Minimal HTTP client for /streams/v1/* endpoints. 1:1 port of the JS
StreamsHttpClient with httpx.AsyncClient.
"""

from __future__ import annotations

from typing import Any, Optional

import httpx

from ..util.backoff import make_backoff, sleep


class StreamsHttpError(Exception):
    """Raised on non-2xx responses or transport failures."""

    def __init__(self, message: str, status: Optional[int] = None, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


class StreamsHttpClient:
    """
    Async HTTP client scoped to /streams/v1/* on a Queen base URL.

    Args:
        url: Base URL of Queen, e.g. ``http://localhost:6632``.
        bearer_token: Optional JWT for auth-enabled brokers.
        timeout_ms: Per-request timeout (ms). Default 30 000.
        retry_attempts: Retries on 5xx/network errors. Default 3.
    """

    def __init__(
        self,
        url: str,
        bearer_token: Optional[str] = None,
        timeout_ms: int = 30_000,
        retry_attempts: int = 3,
    ):
        if not url:
            raise ValueError("StreamsHttpClient requires url")
        self.url = url.rstrip("/")
        self.bearer_token = bearer_token
        self.timeout_ms = timeout_ms
        self.retry_attempts = retry_attempts
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout_ms / 1000.0))

    async def close(self) -> None:
        await self._client.aclose()

    async def post(self, path: str, body: Any) -> Any:
        """POST a JSON body, return the parsed JSON response, raise StreamsHttpError on failure."""
        full = self.url + path
        backoff = make_backoff(initial_ms=100, max_ms=5000)

        headers = {"Content-Type": "application/json"}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"

        last_err: Optional[Exception] = None
        for attempt in range(self.retry_attempts + 1):
            try:
                resp = await self._client.post(full, json=body, headers=headers)
                try:
                    parsed = resp.json() if resp.content else None
                except ValueError:
                    parsed = {"raw": resp.text}
                if resp.is_success:
                    return parsed
                err = StreamsHttpError(
                    parsed.get("error") if isinstance(parsed, dict) and parsed.get("error") else f"HTTP {resp.status_code}",
                    status=resp.status_code,
                    body=parsed,
                )
                # 4xx (other than 408/429) is not retriable.
                if 400 <= resp.status_code < 500 and resp.status_code not in (408, 429):
                    raise err
                last_err = err
            except httpx.HTTPError as ex:
                last_err = ex

            if attempt >= self.retry_attempts:
                break
            await sleep(backoff.next())

        if isinstance(last_err, StreamsHttpError):
            raise last_err
        raise StreamsHttpError(str(last_err) if last_err else "request failed")
