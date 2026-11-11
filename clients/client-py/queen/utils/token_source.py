"""
The credential a client presents (``bearer_token``): a fixed string -- an API
key, or a token it was handed -- or a FUNCTION that fetches one, typically a
client token the application's own backend minted for this device or user
(``queen.tokens.mint()`` there, ``POST /api/v1/tokens``).

With a function, the client calls it before its first request and keeps the
token it answers. It calls it again:

  - shortly before a client token expires (its ``exp``, read from the token
    itself, REFRESH_AHEAD_SECONDS ahead), so a request rarely meets an expired
    token at all;
  - after an answer of 401, and then sends that request once more with the new
    token. A second 401 is the caller's.

One call at a time: requests that need a token while one is being fetched wait
for that call, and a 401 met by many requests at once refreshes once.

The function may be a plain function or a coroutine function: an answer that
is awaitable is awaited. A plain one runs on the event loop, so a call to a
backend belongs in a coroutine function.
"""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import math
import time
from typing import Awaitable, Callable, Optional, Union

from ..errors import TokenSourceError

#: How long before a client token's ``exp`` it is fetched again, in seconds.
REFRESH_AHEAD_SECONDS = 30.0

#: What every client token starts with (the proxy's ``clients::TOKEN_PREFIX``).
CLIENT_TOKEN_PREFIX = "qt_"

#: A function that fetches a token: plain, or a coroutine function.
TokenFunction = Callable[[], Union[str, Awaitable[str]]]


class TokenSource:
    """Where a client's bearer token comes from: a fixed string, a function
    that fetches one, or nothing.

    One source can serve several clients: give the same ``TokenSource`` as
    ``bearer_token`` to each (a ``Queen`` and a stream's ``run``, say), and
    they share its token and its calls.
    """

    def __init__(self, bearer_token: Optional[Union[str, TokenFunction]] = None) -> None:
        self._fixed: Optional[str] = None
        self._fetch: Optional[TokenFunction] = None
        if callable(bearer_token):
            self._fetch = bearer_token
        elif bearer_token is not None and bearer_token != "":
            self._fixed = str(bearer_token)
        self._token: Optional[str] = None
        self._expires_at: Optional[float] = None
        # Made at the first fetch, inside the running loop: on Python 3.9 a
        # Lock made at construction binds to whatever loop was current then,
        # and a client is often built before any loop runs.
        self._lock: Optional[asyncio.Lock] = None

    @property
    def configured(self) -> bool:
        """Is there any credential at all?"""
        return self._fixed is not None or self._fetch is not None

    @property
    def refreshable(self) -> bool:
        """Can a new token be fetched (a function was given)?"""
        return self._fetch is not None

    async def get(self, failed: Optional[str] = None) -> Optional[str]:
        """The token to send, None without a credential.

        ``failed``: the token a request was just refused with (401). A new one
        is fetched unless another request already replaced it.

        Raises TokenSourceError when the function fails or answers no token.
        """
        if self._fixed is not None:
            return self._fixed
        if self._fetch is None:
            return None
        if self._kept(failed):
            return self._token
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            # Another request may have fetched while this one waited.
            if self._kept(failed):
                return self._token
            token = await self._call(self._fetch)
            self._token = token
            self._expires_at = expires_at(token)
            return token

    def _kept(self, failed: Optional[str]) -> bool:
        """Is the token kept the one to send?"""
        if self._token is None:
            return False
        if failed is not None:
            return self._token != failed
        return self._expires_at is None or self._expires_at - time.time() >= REFRESH_AHEAD_SECONDS

    @staticmethod
    async def _call(fetch: TokenFunction) -> str:
        try:
            answer = fetch()
            if inspect.isawaitable(answer):
                answer = await answer
        except Exception as error:
            raise TokenSourceError(f"token source: the token function failed: {error!r}") from error
        token = answer.strip() if isinstance(answer, str) else ""
        if not token:
            raise TokenSourceError(
                "token source: the token function must answer a non-empty string"
            )
        # It goes into a header: a non-ASCII character or a newline there
        # fails only when the request is built or sent, as an error the
        # retries would take for the network's.
        if not all(0x20 <= ord(c) < 0x7F for c in token):
            raise TokenSourceError(
                "token source: the token function answered a token that is not printable ASCII"
            )
        return token


#: What ``bearer_token`` accepts.
BearerToken = Union[str, TokenFunction, TokenSource]


def as_token_source(bearer_token: Optional[BearerToken]) -> TokenSource:
    """The source of a ``bearer_token`` option: a TokenSource given is shared
    as it is."""
    if isinstance(bearer_token, TokenSource):
        return bearer_token
    return TokenSource(bearer_token)


def expires_at(token: str) -> Optional[float]:
    """A client token's expiry in unix seconds, read from its ``exp`` claim;
    None for anything else (an API key, an opaque token). Read, never
    verified: the broker is the one that verifies."""
    if not isinstance(token, str) or not token.startswith(CLIENT_TOKEN_PREFIX):
        return None
    parts = token[len(CLIENT_TOKEN_PREFIX) :].split(".")
    if len(parts) != 3:
        return None
    try:
        # The JWT segment is base64url without its padding.
        claims = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
    except ValueError:
        return None
    exp = claims.get("exp") if isinstance(claims, dict) else None
    if isinstance(exp, bool) or not isinstance(exp, (int, float)) or not math.isfinite(exp):
        return None
    return float(exp)
