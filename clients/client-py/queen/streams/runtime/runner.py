"""
Runner — the streaming engine's pop/process/cycle loop, plus the
per-window idle-flush timer. 1:1 port of Runner.js.

Lifecycle:
    start():
      - register the query (idempotent, returns query_id)
      - kick off the pop loop in the background
      - if a window operator has idle_flush_ms > 0, also start a flush timer

    loop:
      pop -> group by partition -> for each partition group:
        - apply pre-reducer ops -> envelopes
        - if window+reduce: read state (incl. watermark), reduce, build
          state ops + emits
        - if gate: serial allow/deny eval, possibly partial-ack
        - apply post-reducer ops to each emit
        - build push_items / run foreach effects
        - commit cycle (state ops + push items + ack) atomically

    flush timer:
      periodically wake -> for partitions touched recently, query state for
      ripe windows -> apply post-reducer ops + foreach + sink push, commit
      no-ack cycle (state deletes + sink push, no source ack).
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import time as _time
from datetime import datetime, timezone
from typing import Any, Optional

from ..util.logger import make_logger
from ..util.backoff import sleep
from ..operators.reduce_op import state_key_for, parse_state_key
from .streams_http_client import StreamsHttpClient
from .register import register_query
from .state import get_state
from .cycle import commit_cycle


_DEFAULT_BATCH_SIZE = 200
_DEFAULT_MAX_PARTITIONS = 4
_DEFAULT_IDLE_SLEEP_MS = 100
_PARTITION_RECENCY_MS = 5 * 60 * 1000

_WATERMARK_STATE_KEY = "__wm__"


class _GateContext:
    """Lightweight context object passed to user gate fn."""

    __slots__ = ("state", "stream_time_ms", "partition_id", "partition", "key", "streamTimeMs", "partitionId")

    def __init__(self, state: dict, stream_time_ms: int, partition_id: str, partition: str, key: str):
        self.state = state
        self.stream_time_ms = stream_time_ms
        self.partition_id = partition_id
        self.partition = partition
        self.key = key
        # JS-compatible aliases for users porting from queen-mq JS:
        self.streamTimeMs = stream_time_ms
        self.partitionId = partition_id


class Runner:
    def __init__(self, **opts):
        self.options = opts
        self.stream = opts["stream"]
        self.query_id = opts["queryId"]
        self.batch_size = opts.get("batchSize") or _DEFAULT_BATCH_SIZE
        self.max_partitions = opts.get("maxPartitions") or _DEFAULT_MAX_PARTITIONS
        self.max_wait_millis = opts["maxWaitMillis"] if opts.get("maxWaitMillis") is not None else 1000
        self.subscription_mode = opts.get("subscriptionMode")
        self.subscription_from = opts.get("subscriptionFrom")
        self.reset = bool(opts.get("reset"))
        self.on_error = opts.get("onError")
        self.logger = make_logger(opts.get("logger"))
        self.consumer_group = opts.get("consumerGroup") or f"streams.{self.query_id}"

        if not opts.get("url"):
            raise ValueError(
                "Stream.run requires url='<queen-url>' so the SDK can call /streams/v1/* endpoints"
            )
        self.http = StreamsHttpClient(
            url=opts["url"],
            bearer_token=opts.get("bearerToken"),
        )

        self._stopped = False
        self._loop_task: Optional[asyncio.Task] = None
        self._flush_task: Optional[asyncio.Task] = None
        self._flush_in_flight = False
        self._recent_partitions: dict = {}
        self._partition_watermarks: dict = {}
        self._stats = {
            "cyclesTotal": 0,
            "flushCyclesTotal": 0,
            "messagesTotal": 0,
            "pushItemsTotal": 0,
            "stateOpsTotal": 0,
            "lateEventsTotal": 0,
            "errorsTotal": 0,
            "lastCycleAt": None,
            "lastFlushAt": None,
            "lastError": None,
        }
        self._server_query_id: Optional[str] = None

    # ---------------------------------------------------------------- public API

    async def start(self) -> "Runner":
        stages = self.stream.stages
        source_queue_name = self._extract_queue_name(self.stream.source)
        sink_queue_name = stages.sink.queue_name if stages.sink and stages.sink.kind == "sink" else None

        reg = await register_query(
            self.http,
            name=self.query_id,
            source_queue=source_queue_name,
            sink_queue=sink_queue_name,
            config_hash=self.stream.config_hash,
            reset=self.reset,
        )
        self._server_query_id = reg["queryId"]
        self.logger.info(
            "query registered",
            {"queryId": self.query_id, "server_id": reg["queryId"], "fresh": reg["fresh"], "didReset": reg["didReset"]},
        )

        if stages.key_by:
            self.logger.warn(
                ".key_by() is set — if the derived key consistently differs from "
                "the source partition_id, multiple workers may contend on the "
                "same (query_id, partition_id, key) state rows."
            )

        # Spawn idle-flush timer if any window has it.
        if stages.window and getattr(stages.window, "idle_flush_ms", 0) > 0:
            self._flush_task = asyncio.create_task(self._flush_loop(stages.window.idle_flush_ms))
            self.logger.info(
                "idle-flush timer armed",
                {"intervalMs": stages.window.idle_flush_ms, "operator": stages.window.operator_tag},
            )

        self._loop_task = asyncio.create_task(self._loop())
        return self

    async def stop(self) -> None:
        self._stopped = True
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._loop_task:
            try:
                await self._loop_task
            except (asyncio.CancelledError, Exception):
                pass
        # Drain any in-flight flush.
        for _ in range(50):
            if not self._flush_in_flight:
                break
            await sleep(20)
        await self.http.close()

    def metrics(self) -> dict:
        return {**self._stats, "queryId": self.query_id, "serverQueryId": self._server_query_id}

    # ----------------------------------------------------------------- main loop

    async def _loop(self) -> None:
        while not self._stopped:
            try:
                messages = await self._pop_messages()
                if self._stopped:
                    break
                if not messages:
                    await sleep(_DEFAULT_IDLE_SLEEP_MS)
                    continue
                groups = self._group_by_partition(messages)
                for group in groups:
                    if self._stopped:
                        break
                    self._touch_partition(group["partitionId"], group["partitionName"])
                    try:
                        await self._process_partition_cycle(group)
                        self._stats["cyclesTotal"] += 1
                        self._stats["messagesTotal"] += len(group["messages"])
                        self._stats["lastCycleAt"] = _now_iso()
                    except Exception as err:
                        self._report_error(err, {"phase": "cycle", "partition": group["partitionId"]})
            except Exception as err:
                self._report_error(err, {"phase": "pop"})
                await sleep(500)
        self.logger.info("loop stopped", {"queryId": self.query_id, "stats": self._stats})

    def _report_error(self, err: Exception, ctx: dict) -> None:
        self._stats["errorsTotal"] += 1
        msg = str(err) if err else "unknown"
        self._stats["lastError"] = msg
        self.logger.error(f"{ctx.get('phase')} failed", {"queryId": self.query_id, **ctx, "error": msg})
        if self.on_error:
            try:
                self.on_error(err, ctx)
            except Exception:
                pass

    # ----------------------------------------------------------------- pop

    async def _pop_messages(self) -> list:
        qb = (
            self.stream.source
            .batch(self.batch_size)
            .wait(True)
            .timeout_millis(self.max_wait_millis)
            .group(self.consumer_group)
        )
        if self.max_partitions > 1 and hasattr(qb, "partitions"):
            qb = qb.partitions(self.max_partitions)
        if self.subscription_mode and hasattr(qb, "subscription_mode"):
            qb = qb.subscription_mode(self.subscription_mode)
        if self.subscription_from and hasattr(qb, "subscription_from"):
            qb = qb.subscription_from(self.subscription_from)
        result = qb.pop()
        if inspect.isawaitable(result):
            result = await result
        return list(result) if result else []

    def _group_by_partition(self, messages: list) -> list:
        groups: dict = {}
        for m in messages:
            pid = (m.get("partitionId") if isinstance(m, dict) else getattr(m, "partitionId", None)) or "unknown"
            g = groups.get(pid)
            if g is None:
                pname = m.get("partition") if isinstance(m, dict) else getattr(m, "partition", None)
                lease = m.get("leaseId") if isinstance(m, dict) else getattr(m, "leaseId", None)
                cg = m.get("consumerGroup") if isinstance(m, dict) else getattr(m, "consumerGroup", None)
                g = {
                    "partitionId": pid,
                    "partitionName": pname,
                    "leaseId": lease,
                    "consumerGroup": cg or self.consumer_group,
                    "messages": [],
                }
                groups[pid] = g
            g["messages"].append(_as_dict(m))
        return list(groups.values())

    def _touch_partition(self, partition_id: str, partition_name: str) -> None:
        if not partition_id:
            return
        now = int(_time.time() * 1000)
        self._recent_partitions[partition_id] = {"partitionName": partition_name, "touchedAt": now}
        cutoff = now - _PARTITION_RECENCY_MS
        for pid, e in list(self._recent_partitions.items()):
            if e["touchedAt"] < cutoff:
                del self._recent_partitions[pid]

    # ----------------------------------------------------------- cycle

    async def _process_partition_cycle(self, group: dict) -> None:
        stages = self.stream.stages
        partition_id = group["partitionId"]
        partition_name = group["partitionName"]

        ordered = sorted(
            group["messages"],
            key=lambda m: (_parse_iso_ms(m.get("createdAt")) or 0, m.get("id") or ""),
        )

        # 1. pre-stages
        envelopes = [
            {"msg": msg, "key": msg.get("partitionId") or partition_id, "value": msg.get("data")}
            for msg in ordered
        ]
        for op in stages.pre:
            nxt = []
            for env in envelopes:
                outs = await op.apply(env)
                for e in outs:
                    nxt.append(e)
            envelopes = nxt

        # 2. keyBy
        if stages.key_by:
            nxt = []
            for env in envelopes:
                outs = await stages.key_by.apply(env)
                for e in outs:
                    nxt.append(e)
            envelopes = nxt

        # 3. gate path
        if stages.gate:
            await self._process_gate_cycle(stages, envelopes, ordered, group, partition_id, partition_name)
            return

        # 3. window + reducer
        state_ops: list = []
        emits: list = []
        if stages.window:
            if getattr(stages.window, "window_kind", None) == "session":
                result = await self._process_session_window(stages, envelopes, partition_id)
            else:
                result = await self._process_stateless_window(stages, envelopes, partition_id, ordered)
            state_ops = result["stateOps"]
            emits = result["emits"]
        else:
            emits = [{"key": env["key"], "value": env.get("value"), "sourceMsg": env["msg"]} for env in envelopes]

        # 4. post-stages
        emits = await self._apply_post_stages(stages.post, emits, partition_id, partition_name)

        # 5. sink / foreach
        push_items: list = []
        if stages.sink and stages.sink.kind == "sink":
            push_items = stages.sink.build_push_items(
                [{"key": e["key"], "value": e["value"], "sourceMsg": e.get("sourceMsg")} for e in emits],
                partition_name,
            )
        elif stages.sink and stages.sink.kind == "foreach":
            await stages.sink.run_effects(
                [
                    {"value": e["value"], "ctx": self._build_emit_ctx(e, partition_name, partition_id)}
                    for e in emits
                ]
            )

        # 6. ack
        last = ordered[-1] if ordered else None
        ack = (
            {
                "transactionId": last.get("transactionId"),
                "leaseId": last.get("leaseId") or group.get("leaseId"),
                "status": "completed",
                "count": len(ordered),
            }
            if last
            else None
        )

        # 7. commit
        self._stats["stateOpsTotal"] += len(state_ops)
        self._stats["pushItemsTotal"] += len(push_items)
        await commit_cycle(
            self.http,
            query_id=self._server_query_id,
            partition_id=partition_id,
            consumer_group=group["consumerGroup"],
            state_ops=state_ops,
            push_items=push_items,
            ack=ack,
        )

    # --------------------------------------------------------------- gate path

    async def _process_gate_cycle(
        self,
        stages,
        envelopes: list,
        ordered: list,
        group: dict,
        partition_id: str,
        partition_name: str,
    ) -> None:
        gate = stages.gate

        loaded_all = await get_state(
            self.http,
            query_id=self._server_query_id,
            partition_id=partition_id,
            keys=[],
        )

        live_state: dict = {}
        touched_keys: set = set()

        def ensure_state(key: str) -> dict:
            if key in live_state:
                return live_state[key]
            initial = self._clone_state(loaded_all.get(key)) if key in loaded_all else {}
            live_state[key] = initial
            return initial

        stream_time_ms = int(_time.time() * 1000)
        allowed_count = 0
        first_deny_idx = -1
        allowed_envelopes: list = []

        for i, env in enumerate(envelopes):
            key = env["key"]
            state = ensure_state(key)
            ctx = _GateContext(state, stream_time_ms, partition_id, partition_name, key)
            try:
                decision = await gate.evaluate(env, ctx)
            except Exception as err:
                self._report_error(err, {"phase": "gate-eval", "partition": partition_id})
                return
            if decision.get("allow"):
                touched_keys.add(key)
                allowed_count += 1
                allowed_envelopes.append(env)
            else:
                first_deny_idx = i
                break

        if allowed_count == 0:
            self._stats["gateDenialsTotal"] = self._stats.get("gateDenialsTotal", 0) + len(envelopes)
            return

        state_ops = [{"type": "upsert", "key": k, "value": live_state[k]} for k in touched_keys]

        emits = [{"key": e["key"], "value": e.get("value"), "sourceMsg": e["msg"]} for e in allowed_envelopes]
        emits = await self._apply_post_stages(stages.post, emits, partition_id, partition_name)

        push_items: list = []
        if stages.sink and stages.sink.kind == "sink":
            push_items = stages.sink.build_push_items(
                [{"key": e["key"], "value": e["value"], "sourceMsg": e.get("sourceMsg")} for e in emits],
                partition_name,
            )
        elif stages.sink and stages.sink.kind == "foreach":
            await stages.sink.run_effects(
                [
                    {"value": e["value"], "ctx": self._build_emit_ctx(e, partition_name, partition_id)}
                    for e in emits
                ]
            )

        last_allowed_src = allowed_envelopes[-1]["msg"]
        ack = {
            "transactionId": last_allowed_src.get("transactionId"),
            "leaseId": last_allowed_src.get("leaseId") or group.get("leaseId"),
            "status": "completed",
            "count": allowed_count,
        }

        partial = first_deny_idx >= 0
        release_lease = not partial

        if first_deny_idx >= 0:
            tail = len(envelopes) - allowed_count
            self._stats["gateDenialsTotal"] = self._stats.get("gateDenialsTotal", 0) + tail
        self._stats["gateAllowsTotal"] = self._stats.get("gateAllowsTotal", 0) + allowed_count
        self._stats["stateOpsTotal"] += len(state_ops)
        self._stats["pushItemsTotal"] += len(push_items)

        await commit_cycle(
            self.http,
            query_id=self._server_query_id,
            partition_id=partition_id,
            consumer_group=group["consumerGroup"],
            state_ops=state_ops,
            push_items=push_items,
            ack=ack,
            release_lease=release_lease,
        )

    @staticmethod
    def _clone_state(v: Any) -> Any:
        if v is None:
            return {}
        if not isinstance(v, (dict, list)):
            return v
        try:
            return json.loads(json.dumps(v))
        except Exception:
            return copy.deepcopy(v)

    # --------------------- stateless window path (tumbling / sliding / cron)

    async def _process_stateless_window(self, stages, envelopes, partition_id, ordered):
        window = stages.window
        reducer = stages.reducer

        loaded_all = (
            await get_state(self.http, query_id=self._server_query_id, partition_id=partition_id, keys=[])
            if reducer
            else {}
        )

        if window.event_time_fn:
            wm_row = loaded_all.get(_WATERMARK_STATE_KEY)
            if isinstance(wm_row, dict) and isinstance(wm_row.get("eventTimeMs"), (int, float)):
                self._partition_watermarks[partition_id] = wm_row["eventTimeMs"]

        annotated: list = []
        watermark_ms = self._partition_watermarks.get(partition_id)
        dropped_late = 0
        max_observed_event_time = float("-inf")

        for env in envelopes:
            try:
                outs = await window.apply(env)
            except Exception as err:
                self._report_error(err, {"phase": "window-apply", "partition": partition_id})
                continue
            for annot in outs:
                if window.event_time_fn and isinstance(annot.get("eventTimeMs"), (int, float)):
                    if annot["eventTimeMs"] > max_observed_event_time:
                        max_observed_event_time = annot["eventTimeMs"]
                    if isinstance(watermark_ms, (int, float)) and annot["eventTimeMs"] < watermark_ms:
                        if window.on_late_policy == "drop":
                            dropped_late += 1
                            continue
                annotated.append(annot)
        if dropped_late > 0:
            self._stats["lateEventsTotal"] += dropped_late

        if not reducer:
            return {
                "stateOps": [],
                "emits": [{"key": env["key"], "value": env.get("value"), "sourceMsg": env["msg"]} for env in annotated],
            }

        # Compute close-trigger clock.
        if window.event_time_fn:
            existing_wm = self._partition_watermarks.get(partition_id, float("-inf"))
            advanced = max(
                existing_wm,
                max_observed_event_time - window.allowed_lateness_ms if max_observed_event_time > float("-inf") else float("-inf"),
            )
            self._partition_watermarks[partition_id] = advanced
            clock_ms = advanced
        else:
            pt = float("-inf")
            for m in ordered:
                t = _parse_iso_ms(m.get("createdAt")) or 0
                if t > pt:
                    pt = t
            clock_ms = pt

        result = await reducer.run(
            envelopes=annotated,
            loaded_state=loaded_all,
            stream_time_ms=clock_ms,
            operator_tag=window.operator_tag,
            grace_period_ms=window.grace_period_ms,
        )

        if window.event_time_fn:
            wm = self._partition_watermarks.get(partition_id)
            if isinstance(wm, (int, float)) and wm > float("-inf"):
                result["stateOps"].append({
                    "type": "upsert",
                    "key": _WATERMARK_STATE_KEY,
                    "value": {"eventTimeMs": wm},
                })

        return result

    # ----------------------------------------------------- session window path

    async def _process_session_window(self, stages, envelopes, partition_id):
        window = stages.window
        reducer = stages.reducer
        if not reducer:
            raise RuntimeError("window_session requires a downstream .reduce() or .aggregate()")

        loaded_all = await get_state(
            self.http, query_id=self._server_query_id, partition_id=partition_id, keys=[]
        )
        session_state = {
            k: v for k, v in loaded_all.items() if k.startswith(window.operator_tag + "\u001fopen\u001f")
        }
        if window.event_time_fn:
            wm_row = loaded_all.get(_WATERMARK_STATE_KEY)
            if isinstance(wm_row, dict) and isinstance(wm_row.get("eventTimeMs"), (int, float)):
                self._partition_watermarks[partition_id] = wm_row["eventTimeMs"]

        dropped_late = 0
        max_observed_event_time = float("-inf")
        watermark_ms = self._partition_watermarks.get(partition_id)
        enriched: list = []
        for env in envelopes:
            try:
                annots = await window.apply(env)
            except Exception as err:
                self._report_error(err, {"phase": "window-apply", "partition": partition_id})
                continue
            annot = annots[0] if annots else None
            if annot is None:
                continue
            if window.event_time_fn and isinstance(annot.get("eventTimeMs"), (int, float)):
                if annot["eventTimeMs"] > max_observed_event_time:
                    max_observed_event_time = annot["eventTimeMs"]
                if isinstance(watermark_ms, (int, float)) and annot["eventTimeMs"] < watermark_ms:
                    if window.on_late_policy == "drop":
                        dropped_late += 1
                        continue
            enriched.append(annot)
        if dropped_late > 0:
            self._stats["lateEventsTotal"] += dropped_late

        if window.event_time_fn:
            existing_wm = self._partition_watermarks.get(partition_id, float("-inf"))
            advanced = max(
                existing_wm,
                max_observed_event_time - window.allowed_lateness_ms if max_observed_event_time > float("-inf") else float("-inf"),
            )
            self._partition_watermarks[partition_id] = advanced
            now_ms = advanced
        else:
            now_ms = max_observed_event_time if max_observed_event_time > float("-inf") else int(_time.time() * 1000)

        reducer_fn = reducer.fn

        def reducer_initial():
            init = reducer.initial
            if init is None:
                return 0
            if isinstance(init, (int, float, str, bool)):
                return init
            return copy.deepcopy(init)

        result = await window.run_session(
            envelopes=enriched,
            loaded_state=session_state,
            reducer_fn=reducer_fn,
            reducer_initial=reducer_initial,
            now_ms=now_ms,
        )

        if window.event_time_fn:
            wm = self._partition_watermarks.get(partition_id)
            if isinstance(wm, (int, float)) and wm > float("-inf"):
                result["stateOps"].append({
                    "type": "upsert",
                    "key": _WATERMARK_STATE_KEY,
                    "value": {"eventTimeMs": wm},
                })

        return result

    # -------------------------------------------------------- post-reducer ops

    async def _apply_post_stages(self, post_ops, emits, partition_id, partition_name):
        if not post_ops or not emits:
            return emits
        working = emits
        for op in post_ops:
            nxt = []
            for e in working:
                ctx = self._build_emit_ctx(e, partition_name, partition_id)
                envelope = {"msg": e.get("value"), "key": e.get("key"), "value": e.get("value"), "ctx": ctx}
                outs = await op.apply(envelope)
                for o in outs:
                    new_value = o.get("value") if o.get("value") is not None else o.get("msg")
                    nxt.append({**e, "value": new_value})
            working = nxt
        return working

    def _build_emit_ctx(self, e: dict, partition_name: str, partition_id: str) -> dict:
        return {
            "partition": partition_name,
            "partitionId": partition_id,
            "key": e.get("key"),
            "windowKey": e.get("windowKey"),
            "windowStart": e.get("windowStart"),
            "windowEnd": e.get("windowEnd"),
        }

    # ------------------------------------------------ idle flush (per-window)

    async def _flush_loop(self, interval_ms: int) -> None:
        try:
            while not self._stopped:
                await asyncio.sleep(interval_ms / 1000.0)
                if self._stopped:
                    break
                try:
                    await self._flush_tick()
                except Exception as err:
                    self._report_error(err, {"phase": "flush-tick"})
        except asyncio.CancelledError:
            pass

    async def _flush_tick(self) -> None:
        if self._stopped or self._flush_in_flight:
            return
        stages = self.stream.stages
        if not stages.window:
            return
        self._flush_in_flight = True
        try:
            for partition_id, info in list(self._recent_partitions.items()):
                if self._stopped:
                    break
                try:
                    await self._flush_partition(partition_id, info["partitionName"])
                except Exception as err:
                    self._report_error(err, {"phase": "flush", "partition": partition_id})
            self._stats["lastFlushAt"] = _now_iso()
        finally:
            self._flush_in_flight = False

    async def _flush_partition(self, partition_id: str, partition_name: str) -> None:
        stages = self.stream.stages
        window = stages.window

        if window.event_time_fn:
            clock_ms = self._partition_watermarks.get(partition_id)
        else:
            clock_ms = int(_time.time() * 1000)
        if not isinstance(clock_ms, (int, float)):
            return

        if getattr(window, "window_kind", None) == "session":
            return await self._flush_session_partition(partition_id, partition_name, clock_ms)

        ripe_at = clock_ms - window.grace_period_ms
        if ripe_at < 0:
            return
        rows = await self._fetch_ripe_state_rows(partition_id, window.operator_tag + "\u001f", int(ripe_at))
        if not rows:
            return

        emits: list = []
        state_ops: list = []
        for state_key, value in rows.items():
            parts = parse_state_key(state_key)
            if not parts or parts["operatorTag"] != window.operator_tag:
                continue
            acc = value.get("acc") if isinstance(value, dict) and value.get("acc") is not None else value
            ws = value.get("windowStart") if isinstance(value, dict) else None
            we = value.get("windowEnd") if isinstance(value, dict) else None
            emits.append({
                "key": parts["userKey"],
                "windowStart": ws if ws is not None else _parse_iso_ms(parts["windowKey"]) or 0,
                "windowEnd": we,
                "windowKey": parts["windowKey"],
                "value": acc,
                "sourceMsg": None,
            })
            state_ops.append({"type": "delete", "key": state_key})
        if not emits:
            return

        emits = await self._apply_post_stages(stages.post, emits, partition_id, partition_name)

        push_items: list = []
        if stages.sink and stages.sink.kind == "sink":
            push_items = stages.sink.build_push_items(
                [{"key": e["key"], "value": e["value"], "sourceMsg": e.get("sourceMsg")} for e in emits],
                partition_name,
            )
        elif stages.sink and stages.sink.kind == "foreach":
            await stages.sink.run_effects(
                [
                    {"value": e["value"], "ctx": self._build_emit_ctx(e, partition_name, partition_id)}
                    for e in emits
                ]
            )

        self._stats["flushCyclesTotal"] += 1
        self._stats["stateOpsTotal"] += len(state_ops)
        self._stats["pushItemsTotal"] += len(push_items)
        await commit_cycle(
            self.http,
            query_id=self._server_query_id,
            partition_id=partition_id,
            consumer_group=self.consumer_group,
            state_ops=state_ops,
            push_items=push_items,
            ack=None,
        )

    async def _flush_session_partition(self, partition_id, partition_name, clock_ms):
        stages = self.stream.stages
        window = stages.window
        all_state = await get_state(self.http, query_id=self._server_query_id, partition_id=partition_id, keys=[])
        session_state = {
            k: v for k, v in all_state.items() if k.startswith(window.operator_tag + "\u001fopen\u001f")
        }
        if not session_state:
            return

        reducer = stages.reducer

        def reducer_initial():
            init = reducer.initial
            if init is None:
                return 0
            if isinstance(init, (int, float, str, bool)):
                return init
            return copy.deepcopy(init)

        result = await window.run_session(
            envelopes=[],
            loaded_state=session_state,
            reducer_fn=reducer.fn,
            reducer_initial=reducer_initial,
            now_ms=clock_ms,
        )
        if not result["emits"]:
            return

        emits = await self._apply_post_stages(stages.post, result["emits"], partition_id, partition_name)

        push_items: list = []
        if stages.sink and stages.sink.kind == "sink":
            push_items = stages.sink.build_push_items(
                [{"key": e["key"], "value": e["value"], "sourceMsg": None} for e in emits],
                partition_name,
            )
        elif stages.sink and stages.sink.kind == "foreach":
            await stages.sink.run_effects(
                [
                    {"value": e["value"], "ctx": self._build_emit_ctx(e, partition_name, partition_id)}
                    for e in emits
                ]
            )

        self._stats["flushCyclesTotal"] += 1
        self._stats["stateOpsTotal"] += len(result["stateOps"])
        self._stats["pushItemsTotal"] += len(push_items)
        await commit_cycle(
            self.http,
            query_id=self._server_query_id,
            partition_id=partition_id,
            consumer_group=self.consumer_group,
            state_ops=result["stateOps"],
            push_items=push_items,
            ack=None,
        )

    async def _fetch_ripe_state_rows(self, partition_id: str, key_prefix: str, ripe_at_ms: int) -> dict:
        body = {
            "query_id": self._server_query_id,
            "partition_id": partition_id,
            "keys": [],
            "key_prefix": key_prefix,
            "ripe_at_or_before": ripe_at_ms,
        }
        res = await self.http.post("/streams/v1/state/get", body)
        out: dict = {}
        if isinstance(res, dict) and isinstance(res.get("rows"), list):
            for row in res["rows"]:
                if isinstance(row, dict) and isinstance(row.get("key"), str):
                    out[row["key"]] = row.get("value")
        return out

    # --------------------------------------------------------------- helpers

    @staticmethod
    def _extract_queue_name(qb: Any) -> Optional[str]:
        if qb is None:
            return None
        for attr in ("_queue_name", "queue_name"):
            v = getattr(qb, attr, None)
            if isinstance(v, str) and v:
                return v
        name = getattr(qb, "name", None)
        if isinstance(name, str) and name:
            return name
        if isinstance(qb, dict):
            for key in ("queueName", "queue_name", "name"):
                if isinstance(qb.get(key), str) and qb[key]:
                    return qb[key]
        return ""


def _parse_iso_ms(s: Any) -> Optional[int]:
    if not isinstance(s, str):
        return None
    s2 = s.strip()
    if s2.endswith("Z"):
        s2 = s2[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _as_dict(m: Any) -> dict:
    """Coerce a Queen Message (object or dict) to a plain dict the runtime can index."""
    if isinstance(m, dict):
        return m
    out = {}
    for attr in (
        "id",
        "data",
        "createdAt",
        "transactionId",
        "leaseId",
        "partition",
        "partitionId",
        "consumerGroup",
        "retryCount",
        "producerSub",
        "traceId",
    ):
        v = getattr(m, attr, None)
        if v is not None:
            out[attr] = v
    return out
