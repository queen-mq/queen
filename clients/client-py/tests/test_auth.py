"""
Tests for server-stamped producer identity (issue #23, feature A).

Same semantics as client-js/test-v2/auth.js:

  - SP-level tests (always run, talk to Postgres directly) verify the schema +
    stored-procedure contract independently of the HTTP layer.
  - HTTP-level tests exercise the anti-impersonation invariant when JWT auth
    is enabled on the server.

Tests gate themselves on the ``JWT_SECRET`` env var so this suite can run
against a server in either configuration. Run with a JWT-enabled server as:

    JWT_SECRET=<server-secret> python -m pytest clients/client-py/tests/test_auth.py
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict, List, Optional

import httpx
import pytest

from queen import Queen


SERVER_URL = os.environ.get("QUEEN_URL") or os.environ.get("QUEEN_SERVER_URL", "http://localhost:6632")
JWT_SECRET = os.environ.get("JWT_SECRET", "")
JWT_ENABLED = len(JWT_SECRET) > 0


# ---------------------------------------------------------------------------
# Minimal HS256 JWT signer (avoids pulling a dependency just for tests).
# ---------------------------------------------------------------------------
def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _sign_hs256_jwt(payload: Dict[str, Any], secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    enc_header = _b64url(json.dumps(header, separators=(",", ":")).encode())
    enc_payload = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{enc_header}.{enc_payload}".encode()
    sig = hmac.new(secret.encode(), signing_input, hashlib.sha256).digest()
    return f"{enc_header}.{enc_payload}.{_b64url(sig)}"


def _make_token(sub: str, role: str = "read-write") -> str:
    now = int(time.time())
    return _sign_hs256_jwt(
        {"sub": sub, "username": sub, "role": role, "iat": now, "exp": now + 3600},
        JWT_SECRET,
    )


# ---------------------------------------------------------------------------
# Black-box observation helper: pop the message back over HTTP and read its
# producerSub. The segments engine stores payloads in queen.seg_segments (never
# queen.messages) and stamps producer_sub in the broker, so a push is only
# observable via pop — the seg-native ground truth. Optional bearer authorizes
# the pop when JWT is enabled.
# ---------------------------------------------------------------------------
async def _popped_producer_sub(
    queue: str, transaction_id: str, bearer_token: Optional[str] = None
) -> tuple:
    headers = {}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    url = (
        f"{SERVER_URL}/api/v1/pop/queue/{queue}"
        "?batch=20&wait=true&timeout=5000&autoAck=true"
    )
    deadline = time.time() + 6.0
    async with httpx.AsyncClient(timeout=10.0) as http:
        while time.time() < deadline:
            res = await http.get(url, headers=headers)
            if res.status_code == 204:
                await asyncio.sleep(0.15)
                continue
            res.raise_for_status()
            for m in (res.json().get("messages") or []):
                if m and m.get("transactionId") == transaction_id:
                    return True, m.get("producerSub")
            await asyncio.sleep(0.15)
    return False, None


async def _http_push(
    queue: str,
    transaction_id: str,
    data: Dict[str, Any],
    bearer_token: Optional[str] = None,
    extra_item_fields: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    item = {
        "queue": queue,
        "partition": "Default",
        "transactionId": transaction_id,
        "payload": data,
    }
    if extra_item_fields:
        item.update(extra_item_fields)

    async with httpx.AsyncClient(timeout=10.0) as http:
        res = await http.post(
            f"{SERVER_URL}/api/v1/push",
            headers=headers,
            json={"items": [item]},
        )
    res.raise_for_status()
    return res.json()


# ===========================================================================
# (Retired) SP-level producer_sub tests — they drove the ROWS engine directly
# (queen.push_messages_v2 + queen.pop_specific_batch + a queen.messages read) to
# verify the SP's producer_sub NULLIF handling. The rows engine is retired
# (segments-only): payloads live in queen.seg_segments and producer_sub is
# stamped in the broker, never in queen.messages. The shipping producer_sub
# behavior is covered black-box (HTTP push -> pop) by the tests below.
# ===========================================================================


# ===========================================================================
# HTTP-LEVEL TESTS — observe producer_sub black-box via the HTTP pop path
# (queen.seg_segments is the seg-native ground truth; queen.messages is gone).
# ===========================================================================
@pytest.mark.asyncio
@pytest.mark.skipif(JWT_ENABLED, reason="JWT_SECRET set - see HTTP-JWT test")
async def test_producer_sub_ignored_from_body_without_auth():
    """Body-supplied producerSub must be ignored when auth is disabled."""
    q = "test-auth-http-no-jwt-py"
    tx = f"tx-noauth-py-{int(time.time() * 1000)}"

    client = Queen(SERVER_URL)
    try:
        await client.queue(q).create()
    finally:
        await client.close()

    await _http_push(
        queue=q,
        transaction_id=tx,
        data={"hello": "world"},
        extra_item_fields={"producerSub": "attacker-no-jwt"},
    )

    found, producer_sub = await _popped_producer_sub(q, tx)
    assert found, f"did not observe tx {tx} via pop"
    assert producer_sub is None, (
        f"expected null (auth disabled), got {producer_sub!r} - client was able to set it!"
    )


@pytest.mark.asyncio
@pytest.mark.skipif(not JWT_ENABLED, reason="set JWT_SECRET to run (server must have JWT enabled)")
async def test_producer_sub_stamped_from_jwt():
    """producer_sub must equal the validated JWT sub claim."""
    q = "test-auth-http-jwt-stamp-py"
    tx = f"tx-jwt-py-{int(time.time() * 1000)}"
    token = _make_token("alice-producer")

    # Queue creation requires auth too when JWT is enabled.
    async with httpx.AsyncClient(timeout=10.0) as http:
        await http.post(
            f"{SERVER_URL}/api/v1/configure",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            json={"queue": q, "options": {}},
        )

    await _http_push(
        queue=q,
        transaction_id=tx,
        data={"hello": "world"},
        bearer_token=token,
    )

    found, producer_sub = await _popped_producer_sub(q, tx, token)
    assert found, f"did not observe tx {tx} via pop"
    assert producer_sub == "alice-producer"


@pytest.mark.asyncio
@pytest.mark.skipif(not JWT_ENABLED, reason="set JWT_SECRET to run (server must have JWT enabled)")
async def test_producer_sub_spoofing_ignored_with_jwt():
    """Body-supplied producerSub must be ignored even with a valid JWT."""
    q = "test-auth-http-jwt-spoof-py"
    tx = f"tx-spoof-py-{int(time.time() * 1000)}"
    token = _make_token("legit-producer")

    async with httpx.AsyncClient(timeout=10.0) as http:
        await http.post(
            f"{SERVER_URL}/api/v1/configure",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            json={"queue": q, "options": {}},
        )

    await _http_push(
        queue=q,
        transaction_id=tx,
        data={"hello": "world"},
        bearer_token=token,
        extra_item_fields={"producerSub": "attacker"},
    )

    found, producer_sub = await _popped_producer_sub(q, tx, token)
    assert found, f"did not observe tx {tx} via pop"
    assert producer_sub == "legit-producer", (
        f"impersonation not prevented: stored={producer_sub!r}, expected 'legit-producer'"
    )
