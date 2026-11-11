"""
Client tokens (``POST /api/v1/tokens``): a short-lived credential for ONE
entity -- a device, a browser user -- minted from a scoped key.

The key is the role: its grants say which queues, partitions, KV keys and
timers it reaches, and may name the entity as ``{sub}`` (``telemetry`` /
``{sub}``: each device its own partition). The token adds the entity, its
``sub``, and never reaches more than its key. It is minted on the
application's backend, which holds the key, and handed to the device::

    # backend, with the scoped key `devices`
    queen = Queen(url, bearer_token=os.environ["QUEEN_DEVICES_KEY"])
    minted = await queen.tokens.mint(device_id, ttl_seconds=3600)
    minted.token, minted.expires_at

    # device: a function, so the client fetches a new token before this one
    # expires and after a 401
    queen = Queen(url, bearer_token=fetch_token_from_my_backend)

Revoking or narrowing the key reaches every token it minted within about a
second; blocking one ``sub`` refuses that entity's tokens on every node.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from typing_extensions import TypedDict

from ..errors import QueenError

_PATH = "/api/v1/tokens"


class Grant(TypedDict, total=False):
    """One grant, for ``allow``: exactly one of ``queue``, ``kv`` (a namespace)
    and ``timers`` (a queue), the fields that narrow it, and ``ops``::

        names    narrowed by          ops
        queue    partition, group     produce, consume
        kv       key                  read, write
        timers   key, partition       read, write

    Every field is a pattern: a name, ``prefix*`` or ``*``, and may hold
    ``{sub}`` once, the token's entity. An absent field is ``*``.
    """

    queue: str
    kv: str
    timers: str
    partition: str
    group: str
    key: str
    ops: List[str]


@dataclass(frozen=True)
class MintTokenResponse:
    """A minted token, and what it is for."""

    #: The bearer credential, for the entity to send.
    token: str
    #: The entity it was minted for.
    sub: str
    #: The id of the scoped key it belongs to.
    key: str
    #: When it stops working, RFC 3339 (wire name: expiresAt).
    expires_at: str
    #: Its lifetime in seconds, from when it was minted (wire name: expiresIn).
    expires_in: int


class Tokens:
    """``POST /api/v1/tokens``, with this client's credential: a scoped key
    mints for itself, an ``admin`` key for the scoped key ``key`` names."""

    def __init__(self, http_client: Any) -> None:
        self._http_client = http_client

    async def mint(
        self,
        sub: str,
        *,
        ttl_seconds: Optional[int] = None,
        key: Optional[str] = None,
        allow: Optional[Iterable[Grant]] = None,
    ) -> MintTokenResponse:
        """Mint a token for ``sub``.

        Args:
            sub: the entity: 1 to 128 letters, digits and ``. _ - @ +``
            ttl_seconds: its lifetime; 3600 by default, at most the key's
                own ceiling
            key: the scoped key to mint for, by id or name: only when this
                client holds an ``admin`` key (a scoped key mints for itself)
            allow: grants that narrow the key's for this token; they never
                widen it

        Raises:
            ValueError: ``sub`` is empty; nothing is sent.
            httpx.HTTPStatusError: the refusal, with ``.code``:
                ``invalid_request`` (400), ``forbidden`` or ``client_blocked``
                (403), ``key_unknown`` (404), ``key_ambiguous`` (409),
                ``not_configured`` (503).
        """
        if not isinstance(sub, str) or sub == "":
            raise ValueError("tokens.mint: sub is required (the entity the token is for)")
        body: Dict[str, Any] = {"sub": sub}
        if ttl_seconds is not None:
            body["ttlSeconds"] = ttl_seconds
        if key is not None:
            body["key"] = key
        if allow is not None:
            body["allow"] = list(allow)
        answer = await self._http_client.post(_PATH, body)
        if not isinstance(answer, dict) or not isinstance(answer.get("token"), str):
            raise QueenError(f"POST {_PATH} answered no token")
        return MintTokenResponse(
            token=answer["token"],
            sub=str(answer.get("sub", "")),
            key=str(answer.get("key", "")),
            expires_at=str(answer.get("expiresAt", "")),
            expires_in=int(answer.get("expiresIn") or 0),
        )
