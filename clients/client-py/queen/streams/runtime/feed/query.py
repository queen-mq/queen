"""
Live state of a running feed stream, read from outside its process. Port of
the JS SDK's runtime/feed/query.js: the same queue names and the same
question/answer JSON, so a view from any SDK asks a runner of any SDK.

Each active runner (each shard) answers questions on an ephemeral queue of its
own, ``queen-feed-q.<sha16 of the queryId>.<shard>``; the asker mints an inbox
(``queen-feed-r.<hex>``), sends the question with it as ``replyTo``, and waits
there. Nothing is stored, no extra port is opened.

    view = Stream.view(client, "reply-time")
    await view.get("perTenant", "tenant-42")   # {found, value, shard, position}
    await view.list("perTenant", limit=50)
    await view.status()                        # per shard: role, position, lag, open
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
from typing import Any, Callable, Dict, List, Optional

from ....errors import QueenError
from ....utils import logger
from ...util.js import js_is_integer, js_normalize, js_str, now_ms
from ...util.tasks import spawn

DEFAULT_TIMEOUT_MILLIS = 5000
SERVE_WAIT_MILLIS = 10000


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", "surrogatepass")).hexdigest()


def query_queue_name(query_id: str, index: int = 0) -> str:
    """The ephemeral queue the runner of ``query_id`` (shard ``index``) answers on."""
    return f"queen-feed-q.{_sha('queen-feed' + chr(10) + query_id)[:16]}.{index}"


class FeedViewError(QueenError):
    def __init__(self, message: str, *, code: Optional[str] = None) -> None:
        super().__init__(message)
        self.code = code


class StateServer:
    """The loop that answers questions for one active runner."""

    def __init__(self, queen: Any, queue: str, answer: Callable[[Dict[str, Any]], Dict[str, Any]], *, wait_millis: Optional[int] = None) -> None:
        self._queen = queen
        self._queue = queue
        self._answer = answer
        self._wait_millis = wait_millis if wait_millis else SERVE_WAIT_MILLIS
        self._stopped = False
        self._loop: Optional["asyncio.Task[None]"] = None
        self._warned = False

    @property
    def running(self) -> bool:
        return self._loop is not None and not self._stopped

    def start(self) -> None:
        if self._loop is not None or getattr(self._queen, "ephemeral", None) is None:
            return
        self._stopped = False
        self._loop = spawn(self._run())  # type: ignore[assignment]
        self._loop.add_done_callback(self._ended)

    def _ended(self, task: "asyncio.Task[None]") -> None:
        if self._loop is task:
            self._loop = None
        if not task.cancelled():
            task.exception()

    def stop(self) -> Optional["asyncio.Task[None]"]:
        """Stops after the long poll in hand answers (at most ``wait_millis``); not awaited by the runner.
        Answers the serving task (None when it is not running), for a caller that wants to wait for it."""
        self._stopped = True
        return self._loop

    async def _run(self) -> None:
        backoff = 250
        eph = self._queen.ephemeral
        while not self._stopped:
            try:
                got = await eph.pop(self._queue, wait=True, timeout=self._wait_millis, batch=32, commit_on_delivery=True)
                backoff = 250
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - judged below
                if self._stopped:
                    return
                if getattr(err, "code", None) == "ephemeral_unsupported" or getattr(err, "status", None) == 404:
                    logger.warn("FeedStateServer.unsupported", {"queue": self._queue, "error": str(err)})
                    return
                if not self._warned:
                    self._warned = True
                    logger.warn("FeedStateServer.pop", {"queue": self._queue, "error": str(err)})
                await asyncio.sleep(backoff / 1000)
                backoff = min(backoff * 2, 5000)
                continue
            for m in (got.get("messages") if isinstance(got, dict) else None) or []:
                req = m.get("payload") if isinstance(m, dict) else None
                if not isinstance(req, dict) or not isinstance(req.get("replyTo"), str):
                    continue
                try:
                    reply = self._answer(req)
                except Exception as err:  # noqa: BLE001 - the answer is the error
                    reply = {"error": str(err) or type(err).__name__}
                try:
                    payload = js_normalize({**reply, "id": req.get("id")})
                    await eph.push(req["replyTo"], [{"payload": payload}])
                except asyncio.CancelledError:
                    raise
                except Exception as err:  # noqa: BLE001
                    logger.warn("FeedStateServer.reply", {"queue": self._queue, "error": str(err)})


class FeedView:
    """Ask the runners of a stream about their live state."""

    def __init__(self, queen: Any, query_id: str, *, shards: Optional[int] = None, timeout_millis: Optional[int] = None) -> None:
        if queen is None or getattr(queen, "ephemeral", None) is None:
            raise ValueError("Stream.view(queen, query_id): pass the Queen client (it asks over ephemeral queues)")
        if not isinstance(query_id, str) or len(query_id) == 0:
            raise ValueError("Stream.view: query_id is the stream's queryId")
        self._queen = queen
        self._query_id = query_id
        self._shards = shards
        self._timeout = DEFAULT_TIMEOUT_MILLIS if timeout_millis is None else timeout_millis

    async def get(self, step: str, key: Any) -> Dict[str, Any]:
        """The value of one key in one step: ``{found, value?, shard?, position?}``."""
        answers = await self._ask({"op": "get", "step": step, "key": js_str(key)})
        hit = next((a for a in answers if a.get("found")), None)
        if hit is not None:
            return {"found": True, "value": hit.get("value"), "shard": hit.get("shard"), "position": hit.get("position")}
        err = next((a for a in answers if a.get("error")), None)
        if err is not None and not any(not a.get("error") for a in answers):
            raise FeedViewError(f"Stream.view: {err['error']}")
        return {"found": False, "answered": len(answers)}

    async def list(self, step: str, *, limit: int = 100) -> Dict[str, Any]:
        """Up to ``limit`` entries of one step, from every shard: ``{items, truncated}``."""
        answers = await self._ask({"op": "list", "step": step, "limit": limit})
        items: List[Any] = []
        truncated = False
        for a in answers:
            if a.get("error"):
                raise FeedViewError(f"Stream.view: {a['error']}")
            items.extend(a.get("items") or [])
            truncated = truncated or bool(a.get("truncated"))
        return {"items": items[:limit], "truncated": truncated or len(items) > limit, "answered": len(answers)}

    async def status(self) -> List[Dict[str, Any]]:
        """Every shard's runner: role, position, lag, open state, per step."""
        return await self._ask({"op": "status"})

    async def _shard_count(self) -> int:
        if js_is_integer(self._shards) and self._shards > 0:  # type: ignore[operator]
            return int(self._shards)  # type: ignore[arg-type]
        try:
            row = await self._queen.kv.get("queen-feed", f"{self._query_id}#layout")
            value = row.get("value") if isinstance(row, dict) and row.get("found") else None
            if isinstance(value, dict) and js_is_integer(value.get("shards")):
                return int(value["shards"])
        except Exception:  # noqa: BLE001 - no layout: one shard
            pass
        return 1

    async def _ask(self, question: Dict[str, Any]) -> List[Dict[str, Any]]:
        count = await self._shard_count()
        inbox = f"queen-feed-r.{secrets.token_hex(8)}"
        qid = secrets.token_hex(6)
        eph = self._queen.ephemeral
        await asyncio.gather(*[
            eph.push(query_queue_name(self._query_id, i if count > 1 else 0), [{"payload": {**question, "id": qid, "replyTo": inbox}}])
            for i in range(count)
        ])
        answers: List[Dict[str, Any]] = []
        deadline = now_ms() + self._timeout
        while len(answers) < count:
            left = deadline - now_ms()
            if left <= 0:
                break
            r = await eph.pop(inbox, wait=True, timeout=int(min(left, 30000)), batch=count, commit_on_delivery=True)
            for m in (r.get("messages") if isinstance(r, dict) else None) or []:
                payload = m.get("payload") if isinstance(m, dict) else None
                if isinstance(payload, dict) and payload.get("id") == qid:
                    answers.append(payload)
        # The inbox goes away with the broker's own cleanup too; this is a courtesy.
        if callable(getattr(eph, "delete", None)):
            spawn(_quiet(eph.delete(inbox)))
        if not answers:
            raise FeedViewError(
                f"Stream.view: no runner of {self._query_id} answered within {self._timeout} ms (is it running, with serveState on?)",
                code="feed_view_timeout",
            )
        return answers


async def _quiet(aw: Any) -> None:
    try:
        await aw
    except Exception:  # noqa: BLE001
        pass
