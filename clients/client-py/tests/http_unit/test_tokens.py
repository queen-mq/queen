"""
Client tokens on the Python client: ``bearer_token`` as a function (a
TokenSource), refreshed before a client token expires and after a 401, and
``queen.tokens.mint()`` (POST /api/v1/tokens).

Against the scripted PlanServer transport (tests/kv_unit/plan_server.py), the
seam of test_retry_429.py: no broker, no sockets.
"""

from __future__ import annotations

import asyncio
import base64
import json
import time

import httpx
import pytest

from queen import MintTokenResponse, Queen, TokenSource, TokenSourceError
from queen.http.http_client import HttpClient
from queen.streams.runtime.streams_http_client import StreamsHttpClient
from queen.tokens import expires_at

from ..kv_unit.plan_server import PlanServer

EXPIRED = {"status": 401, "json": {"error": "client token expired", "code": "token_expired"}}
RATE_LIMITED = {"status": 429, "json": {"error": "slow down", "code": "rate_limited"}}
OK = {"status": 200, "json": {"ok": 1}}


def client_token(seconds_from_now, sub="dev1"):
    """A client token whose ``exp`` is ``seconds_from_now`` away (signature fake)."""

    def b64(claims):
        return base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()

    exp = int(time.time()) + seconds_from_now
    return f"qt_{b64({'alg': 'HS256'})}.{b64({'sub': sub, 'exp': exp, 'typ': 'client'})}.sig"


def counting(*tokens):
    """A token function answering ``tokens`` in turn (the last one again once
    they run out), and the list of what it answered."""
    calls = []

    def fetch():
        calls.append(tokens[min(len(calls), len(tokens) - 1)])
        return calls[-1]

    return fetch, calls


def answering(value):
    return lambda: value


def minted(token, sub="dev-8f3a", expires_in=600):
    return {
        "status": 201,
        "json": {
            "token": token,
            "sub": sub,
            "key": "k1",
            "expiresAt": "2026-11-11T12:00:00+00:00",
            "expiresIn": expires_in,
        },
    }


def auth_of(server):
    return [r.headers.get("authorization") for r in server.requests]


# ---------------------------------------------------------------------------
# TokenSource
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_fixed_credential_is_answered_as_it_is_and_nothing_without_one():
    assert await TokenSource("qk_live_x").get() == "qk_live_x"
    assert TokenSource("qk_live_x").refreshable is False
    assert await TokenSource(None).get() is None
    assert TokenSource("").configured is False


@pytest.mark.asyncio
async def test_the_function_is_called_once_and_again_only_near_the_exp():
    fetch, calls = counting(client_token(3600))
    source = TokenSource(fetch)
    for _ in range(3):
        await source.get()
    assert len(calls) == 1, "a token with an hour left is kept"

    async def short_lived():  # a coroutine function works the same
        calls.append("short")
        return client_token(10)

    calls.clear()
    source = TokenSource(short_lived)
    await source.get()
    await source.get()
    assert len(calls) == 2, "a token 10 s from its exp is fetched again"


@pytest.mark.asyncio
async def test_concurrent_requests_wait_for_one_fetch_and_a_401_met_by_many_refreshes_once():
    calls = []

    async def fetch():
        calls.append(None)
        await asyncio.sleep(0.01)
        return client_token(3600, f"dev{len(calls)}")

    source = TokenSource(fetch)
    first = await asyncio.gather(*(source.get() for _ in range(4)))
    assert len(calls) == 1 and len(set(first)) == 1, "four requests, one fetch"

    again = await asyncio.gather(*(source.get(failed=first[0]) for _ in range(4)))
    assert len(calls) == 2, "one refresh for four 401s"
    assert len(set(again)) == 1 and again[0] != first[0]
    # A request refused with a token already replaced takes the new one.
    assert await source.get(failed=first[0]) == again[0]
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_a_function_that_fails_or_answers_no_token_is_a_token_source_error():
    def down():
        raise RuntimeError("backend down")

    with pytest.raises(TokenSourceError, match="token source") as raised:
        await TokenSource(down).get()
    assert isinstance(raised.value.__cause__, RuntimeError)
    for answer in ("", "  ", None, 42):
        with pytest.raises(TokenSourceError, match="non-empty string"):
            await TokenSource(answering(answer)).get()
    with pytest.raises(TokenSourceError, match="printable ASCII"):
        await TokenSource(answering("qt_a\nb")).get()


def test_a_client_token_expiry_is_read_and_nothing_else():
    at = expires_at(client_token(100))
    assert at is not None and time.time() + 90 < at <= time.time() + 100
    assert expires_at("qk_live_abc") is None
    assert expires_at("qt_not.a") is None
    assert expires_at("qt_a.b.c.d") is None
    assert expires_at("qt_a.!!!.c") is None


# ---------------------------------------------------------------------------
# HttpClient: the token of every attempt, and one refresh on a 401
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_401_fetches_a_new_token_and_sends_the_request_once_more():
    old, new = client_token(3600, "old"), client_token(3600, "new")
    fetch, calls = counting(old, new)
    server = PlanServer(EXPIRED, OK)
    http = HttpClient(base_url="http://plan.local", transport=server, bearer_token=fetch)
    try:
        assert await http.get("/api/v1/resources/queues") == {"ok": 1}
        # The retry carries the new token.
        assert auth_of(server) == [f"Bearer {old}", f"Bearer {new}"]
        # The new token is kept: the next request fetches nothing.
        await http.get("/api/v1/resources/queues")
        assert calls == [old, new]
    finally:
        await http.close()


@pytest.mark.asyncio
async def test_a_second_401_is_the_callers():
    fetch, _ = counting(*(client_token(3600, f"t{n}") for n in range(3)))
    server = PlanServer(default=EXPIRED)
    http = HttpClient(base_url="http://plan.local", transport=server, bearer_token=fetch)
    try:
        with pytest.raises(httpx.HTTPStatusError) as raised:
            await http.get("/api/v1/resources/queues")
        assert raised.value.response.status_code == 401
        assert raised.value.code == "token_expired"  # type: ignore[attr-defined]
        assert len(server.requests) == 2, "one refresh, one retry"
    finally:
        await http.close()


@pytest.mark.asyncio
async def test_a_fixed_credential_is_not_retried_on_a_401():
    server = PlanServer(default=EXPIRED)
    http = HttpClient(base_url="http://plan.local", transport=server, bearer_token="qk_live_static")
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await http.get("/api/v1/resources/queues")
        assert auth_of(server) == ["Bearer qk_live_static"]
    finally:
        await http.close()


@pytest.mark.asyncio
async def test_the_refresh_spends_no_attempt():
    # Two tries for a 429 and one for anything else: had the retry after the
    # 401 been counted, the 429 behind it would have been the last try.
    fetch, _ = counting(client_token(3600, "old"), client_token(3600, "new"))
    server = PlanServer(EXPIRED, RATE_LIMITED, OK)
    http = HttpClient(
        base_url="http://plan.local",
        transport=server,
        bearer_token=fetch,
        retry_attempts=1,
        retry_429={"max_attempts": 2, "base_ms": 1, "cap_ms": 2},
    )
    try:
        assert await http.get("/api/v1/resources/queues") == {"ok": 1}
        assert len(server.requests) == 3
    finally:
        await http.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("urls", [["http://plan.local"], ["http://a.local", "http://b.local"]])
async def test_a_function_that_fails_sends_nothing_and_is_not_retried(urls):
    calls = []

    async def down():
        calls.append(None)
        raise RuntimeError("backend down")

    server = PlanServer(default=OK)
    queen = Queen(urls, transport=server, bearer_token=down, retry_delay_millis=1)
    try:
        with pytest.raises(TokenSourceError, match="backend down"):
            await queen.tokens.mint("dev1")
    finally:
        await queen.close()
    assert server.requests == []
    assert len(calls) == 1, "neither retried nor failed over as if a broker had failed"


def test_a_token_function_and_an_authorization_header_are_refused():
    with pytest.raises(ValueError, match="set one"):
        HttpClient(
            base_url="http://plan.local",
            bearer_token=answering("t"),
            headers={"authorization": "Bearer x"},
        )


# ---------------------------------------------------------------------------
# Queen: one source for every client it makes, and tokens.mint()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_token_function_serves_the_whole_client():
    fetch, calls = counting(client_token(3600))
    server = PlanServer(default=minted(client_token(60, "a")))
    queen = Queen("http://plan.local", transport=server, bearer_token=fetch)
    node = queen._node_client("http://follower.local")  # what the feed opens per follower
    try:
        await queen.tokens.mint("a")
        await queen.tokens.mint("b")
        await node.get("/api/v1/resources/queues")
        assert len(calls) == 1
        assert auth_of(server) == [f"Bearer {calls[0]}"] * 3
    finally:
        await node.close()
        await queen.close()


@pytest.mark.asyncio
async def test_mint_posts_the_entity_and_reads_the_answer():
    token = client_token(600, "dev-8f3a")
    server = PlanServer(minted(token))
    queen = Queen("http://plan.local", transport=server, bearer_token="qk_live_role")
    try:
        out = await queen.tokens.mint("dev-8f3a", ttl_seconds=600)
        assert out == MintTokenResponse(
            token=token,
            sub="dev-8f3a",
            key="k1",
            expires_at="2026-11-11T12:00:00+00:00",
            expires_in=600,
        )
        hit = server.only
        assert hit.route == "POST /api/v1/tokens"
        assert hit.body == {"sub": "dev-8f3a", "ttlSeconds": 600}
        assert hit.headers["authorization"] == "Bearer qk_live_role"

        with pytest.raises(ValueError, match="sub is required"):
            await queen.tokens.mint("")
        assert len(server.requests) == 1, "an empty sub sends nothing"
    finally:
        await queen.close()


@pytest.mark.asyncio
async def test_mint_sends_the_key_and_the_grants_it_was_given():
    server = PlanServer(minted(client_token(60, "dev1"), sub="dev1"))
    queen = Queen("http://plan.local", transport=server, bearer_token="qk_live_admin")
    grants = [{"queue": "telemetry", "partition": "{sub}", "ops": ["produce"]}]
    try:
        await queen.tokens.mint("dev1", key="devices", allow=grants)
        assert server.only.body == {"sub": "dev1", "key": "devices", "allow": grants}
    finally:
        await queen.close()


# ---------------------------------------------------------------------------
# The streams runtime's client
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_streams_client_refreshes_on_a_401_and_shares_a_source_with_queen():
    old, new = client_token(3600, "old"), client_token(3600, "new")
    fetch, calls = counting(old, new)
    source = TokenSource(fetch)
    server = PlanServer(EXPIRED, {"status": 200, "json": {"success": True}})
    queen = Queen("http://plan.local", transport=server, bearer_token=source)
    streams = StreamsHttpClient("http://plan.local", bearer_token=source, transport=server)
    try:
        assert await streams.post("/streams/v1/queries", {"name": "q"}) == {"success": True}
        assert auth_of(server) == [f"Bearer {old}", f"Bearer {new}"]
        # The Queen client sends the token the streams client fetched.
        await queen._http_client.get("/api/v1/resources/queues")
        assert auth_of(server)[-1] == f"Bearer {new}"
        assert calls == [old, new]
    finally:
        await streams.close()
        await queen.close()
