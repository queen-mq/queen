"""
Regression tests for Queen._normalize_config keyword handling.

Bug (found 2026-07-25 while wiring retry_429, PLAN_QUEEN_PROXY_CLOUD.md Track
C): the string/list config branches returned early, so keyword overrides were
silently dropped -- Queen("http://x", bearer_token="secret") behaved as if no
token had been passed, with no error or warning. All three documented config
forms (URL string, URL list, dict) must apply keyword arguments identically.

Broker-free: constructs clients without issuing requests, or serves canned
responses through HttpClient's `transport=` seam (see test_retry_429.py).
"""

from __future__ import annotations

import pytest

from queen import Queen
from queen.utils.defaults import CLIENT_DEFAULTS

from .test_retry_429 import PlanTransport, rate_limited


class TestKwargsAppliedToEveryConfigForm:
    @pytest.mark.asyncio
    async def test_string_config_applies_kwargs(self):
        queen = Queen(
            "http://fake.local",
            bearer_token="secret",
            retry_429={"max_attempts": 5},
            timeout_millis=1234,
        )
        try:
            assert queen._config["urls"] == ["http://fake.local"]
            assert queen._config["bearer_token"] == "secret"
            assert queen._config["retry_429"] == {"max_attempts": 5}
            assert queen._config["timeout_millis"] == 1234
        finally:
            await queen.close()

    @pytest.mark.asyncio
    async def test_list_config_applies_kwargs(self):
        queen = Queen(["http://a.local", "http://b.local"], timeout_millis=5000)
        try:
            assert queen._config["urls"] == ["http://a.local", "http://b.local"]
            assert queen._config["timeout_millis"] == 5000
        finally:
            await queen.close()

    @pytest.mark.asyncio
    async def test_dict_config_applies_kwargs(self):
        queen = Queen({"url": "http://fake.local"}, bearer_token="secret")
        try:
            assert queen._config["urls"] == ["http://fake.local"]
            assert queen._config["bearer_token"] == "secret"
        finally:
            await queen.close()

    @pytest.mark.asyncio
    async def test_omitted_kwargs_keep_defaults(self):
        queen = Queen("http://fake.local")
        try:
            assert queen._config["timeout_millis"] == CLIENT_DEFAULTS["timeout_millis"]
            assert queen._config["bearer_token"] is None
        finally:
            await queen.close()

    @pytest.mark.asyncio
    async def test_kwargs_override_positional_config(self):
        # Same precedence as the dict branch: explicit keywords win.
        queen = Queen("http://a.local", urls=["http://b.local"])
        try:
            assert queen._config["urls"] == ["http://b.local"]
        finally:
            await queen.close()

    def test_invalid_string_url_still_rejected(self):
        with pytest.raises(ValueError):
            Queen("not-a-url")


class TestStringConfigWiring:
    @pytest.mark.asyncio
    async def test_transport_and_retry_429_reach_http_client(self):
        """End-to-end proof that keywords survive the string form: before the
        fix this test dialed a real socket to fake.local (transport dropped)
        and never retried the 429 (retry_429 dropped)."""
        transport = PlanTransport(
            plan=[rate_limited(retry_after="0")],
            default={"status": 200, "json": [{"status": "queued", "transactionId": "tx-1"}]},
        )
        queen = Queen("http://fake.local", transport=transport, retry_429={"base_ms": 5, "cap_ms": 50})
        try:
            result = await queen.queue("q1").push({"hello": "world"})
            assert len(transport.hits) == 2, "one 429 then one success"
            assert result[0]["status"] == "queued"
        finally:
            await queen.close()
