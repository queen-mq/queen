"""
Minimal fake of the /streams/v1/* endpoints, plus a fake QueueBuilder that
mimics queen-mq's pop() semantics. Used by the cycle/runner tests.

Mirror of fakeServer.js but implemented with ``httpx.MockTransport`` so we
don't need to spawn a real HTTP server (no aiohttp dep, no port allocation,
faster).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

# Import the StreamsHttpClient we need to monkey-patch onto.
from queen.streams.runtime import streams_http_client as _shc_module


class FakeStreamsServer:
    """In-memory stand-in for /streams/v1/queries|cycle|state/get."""

    def __init__(self, state_seeds: Optional[Any] = None):
        self.recorded: dict = {"registers": [], "cycles": [], "stateGets": []}
        self.queries: dict = {}  # name -> {"id": str, "config_hash": str}
        self.state: dict = dict(state_seeds) if isinstance(state_seeds, dict) else {}
        self.url = "http://fake-streams.local"  # any URL — MockTransport intercepts all
        self._transport = httpx.MockTransport(self._handler)
        # Save the original AsyncClient ctor and patch it for the lifetime
        # of this fake server (until close() is called).
        self._patched = False
        self._orig_init = _shc_module.StreamsHttpClient.__init__

    async def start(self) -> "FakeStreamsServer":
        """Patch the StreamsHttpClient to use our MockTransport."""
        transport = self._transport

        def patched_init(client_self, url, bearer_token=None, timeout_ms=30_000, retry_attempts=3):
            client_self.url = url.rstrip("/")
            client_self.bearer_token = bearer_token
            client_self.timeout_ms = timeout_ms
            client_self.retry_attempts = retry_attempts
            client_self._client = httpx.AsyncClient(
                transport=transport, timeout=httpx.Timeout(timeout_ms / 1000.0)
            )

        _shc_module.StreamsHttpClient.__init__ = patched_init
        self._patched = True
        return self

    async def close(self) -> None:
        if self._patched:
            _shc_module.StreamsHttpClient.__init__ = self._orig_init
            self._patched = False

    # -------- request handler --------

    def _handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        try:
            parsed = json.loads(request.content) if request.content else {}
        except Exception:
            parsed = {}

        if request.method == "POST" and path == "/streams/v1/queries":
            self.recorded["registers"].append(parsed)
            existing = self.queries.get(parsed.get("name"))
            if existing is not None:
                if existing["config_hash"] != parsed.get("config_hash") and not parsed.get("reset"):
                    return httpx.Response(409, json={"success": False, "error": "config_hash mismatch"})
                if parsed.get("reset"):
                    pid_prefix = existing["id"] + "|"
                    for k in list(self.state.keys()):
                        if k.startswith(pid_prefix):
                            del self.state[k]
                    existing["config_hash"] = parsed.get("config_hash")
                return httpx.Response(200, json={
                    "success": True,
                    "query_id": existing["id"],
                    "name": parsed.get("name"),
                    "config_hash": parsed.get("config_hash"),
                    "fresh": False,
                    "reset": bool(parsed.get("reset")),
                })
            new_id = str(uuid.uuid4())
            self.queries[parsed.get("name")] = {"id": new_id, "config_hash": parsed.get("config_hash")}
            return httpx.Response(200, json={
                "success": True,
                "query_id": new_id,
                "name": parsed.get("name"),
                "config_hash": parsed.get("config_hash"),
                "fresh": True,
                "reset": False,
            })

        if request.method == "POST" and path == "/streams/v1/state/get":
            self.recorded["stateGets"].append(parsed)
            rows = []
            prefix = f"{parsed.get('query_id')}|{parsed.get('partition_id')}|"
            keys = parsed.get("keys") or []
            keys_set = set(keys) if keys else None
            for k, v in self.state.items():
                if not k.startswith(prefix):
                    continue
                sk = k[len(prefix):]
                if keys_set is not None and sk not in keys_set:
                    continue
                rows.append({
                    "key": sk,
                    "value": v,
                    "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                })
            return httpx.Response(200, json={"success": True, "rows": rows})

        if request.method == "POST" and path == "/streams/v1/cycle":
            self.recorded["cycles"].append(parsed)
            query_id = parsed.get("query_id")
            partition_id = parsed.get("partition_id")
            for op in parsed.get("state_ops") or []:
                k = f"{query_id}|{partition_id}|{op.get('key')}"
                if op.get("type") == "upsert":
                    self.state[k] = op.get("value")
                elif op.get("type") == "delete":
                    self.state.pop(k, None)
            return httpx.Response(200, json={
                "success": True,
                "query_id": query_id,
                "partition_id": partition_id,
                "state_ops_applied": len(parsed.get("state_ops") or []),
                "push_results": [
                    {
                        "queueName": p.get("queue"),
                        "messageId": str(uuid.uuid4()),
                        "transactionId": p.get("transactionId") or str(uuid.uuid4()),
                    }
                    for p in (parsed.get("push_items") or [])
                ],
                "ack_result": (
                    {"success": True, "lease_released": True, "dlq": False}
                    if parsed.get("ack")
                    else None
                ),
            })

        return httpx.Response(404, json={"error": "not found"})


async def create_fake_streams_server(state_seeds: Optional[Any] = None) -> FakeStreamsServer:
    s = FakeStreamsServer(state_seeds)
    await s.start()
    return s


# ---------------------------------------------------------------------------
# Fake QueueBuilder
# ---------------------------------------------------------------------------


class FakeSource:
    def __init__(self, name: str, rounds: list):
        self._queue_name = name
        self.queue_name = name
        self._rounds = list(rounds)

    def batch(self, n): return self
    def wait(self, b): return self
    def timeout_millis(self, ms): return self
    def timeoutMillis(self, ms): return self
    def group(self, g): return self
    def partitions(self, n): return self
    def subscription_mode(self, s): return self
    def subscriptionMode(self, s): return self
    def subscription_from(self, t): return self
    def subscriptionFrom(self, t): return self

    async def pop(self):
        if not self._rounds:
            return []
        return self._rounds.pop(0)


def create_fake_source(name: str, rounds: list) -> FakeSource:
    return FakeSource(name, rounds)


_next_msg_idx = [0]


def fake_message(partition_id: str, partition_name: str = "Default", data=None,
                 created_at: Optional[str] = None, transaction_id: Optional[str] = None) -> dict:
    _next_msg_idx[0] += 1
    n = _next_msg_idx[0]
    return {
        "id": f"msg-{n}",
        "transactionId": transaction_id or f"tx-{n}",
        "partitionId": partition_id,
        "partition": partition_name,
        "leaseId": "fake-lease",
        "consumerGroup": "streams.test",
        "data": data,
        "createdAt": created_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
