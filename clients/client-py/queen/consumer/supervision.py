"""Opt-in observations for one consume invocation; handler calls are not ACKs."""

import asyncio
import base64
import json
import os
import re
import socket
import time
import uuid

from ..utils import logger


class Supervision:
    def __init__(self, http, config, options):
        group = config.get("group") if isinstance(config, dict) else None
        if not isinstance(group, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}", group) is None or group == "coordination":
            raise ValueError("supervision group must be a valid application/deployment name")
        concurrency = options.get("concurrency", 1)
        if type(concurrency) is not int or not 1 <= concurrency <= 4096:
            raise ValueError("supervision requires concurrency between 1 and 4096")
        self.http, self.options, self.group = http, options, group
        self.id = uuid.uuid4().hex
        self.started, self.monotonic = int(time.time()), time.monotonic()
        self.running, self.completed, self.failed = concurrency, 0, 0
        self.last = None
        self.active = {}
        self.sequence = 0
        self.stop_event = asyncio.Event()
        self.publisher = None
        self.warned = False

    def wrap(self, handler):
        async def observed(*args):
            key = self.sequence
            self.sequence += 1
            self.active[key] = time.monotonic()
            try:
                result = await handler(*args)
                self.completed += 1
                return result
            except BaseException:
                self.failed += 1
                raise
            finally:
                del self.active[key]
                self.last = int(time.time())
        return observed

    def document(self, state):
        now = time.monotonic()
        return {
            "schema": "queen.consumer.status/v1", "instance_id": self.id,
            "engine": "python", "execution_model": "async-tasks",
            "hostname": socket.gethostname(), "pid": os.getpid(), "state": state,
            "updated_at_epoch": int(time.time()), "started_at_epoch": self.started,
            "uptime_seconds": int(now - self.monotonic),
            "configuration": {"heartbeat_timeout": 30},
            "pool_status": [{
                "name": "consumer", "queue": self.options.get("queue") or None,
                "namespace": self.options.get("namespace") or None,
                "task": self.options.get("task") or None,
                "consumer_group": self.options.get("group") or "__QUEUE_MODE__",
                "desired": self.options.get("concurrency", 1), "running": self.running,
                "busy": len(self.active), "completed": self.completed, "failed": self.failed,
                "last_completed_at_epoch": self.last,
                "oldest_inflight_seconds": int(now - min(self.active.values())) if self.active else None,
            }],
        }

    async def publish(self, state):
        try:
            raw = json.dumps(self.document(state), ensure_ascii=False).encode("utf-8")
            if len(raw) > 45000:
                raise ValueError("consumer status exceeds one chunk")
            write, slot = uuid.uuid4().hex, f"{self.group}/{self.id}"
            operations = [
                {"op": "put", "ns": "queen-supervisor", "key": f"{slot}/head", "ttlSeconds": 60,
                 "value": {"format": "queen.supervisor.remote-status/v1", "write": write, "chunks": 1, "bytes": len(raw)}},
                {"op": "put", "ns": "queen-supervisor", "key": f"{slot}/chunk/0000", "ttlSeconds": 60,
                 "value": {"write": write, "index": 0, "data": base64.b64encode(raw).decode("ascii")}},
            ]
            response = await asyncio.wait_for(self.http.post("/api/v1/kv", {"operations": operations}, 2000), 2)
            results = response.get("results") if isinstance(response, dict) else None
            if not isinstance(results, list) or len(results) != 2 or any(r.get("applied") is not True for r in results):
                raise ValueError("consumer status publication was not applied")
            self.warned = False
        except Exception:
            if not self.warned:
                logger.warn("Consumer.supervision", "Status publication failed; consumption continues")
            self.warned = True

    def start(self):
        self.publisher = asyncio.create_task(self.run())

    async def run(self):
        await self.publish("running")
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), 10)
            except asyncio.TimeoutError:
                await self.publish("running")
        await self.publish("stopped")

    async def stop(self):
        self.stop_event.set()
        if self.publisher:
            await asyncio.shield(self.publisher)
