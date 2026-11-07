#!/usr/bin/env python3
"""
The Python SDK's conformance runner (clients/conformance/feed/README.md,
sections 1 and 3): builds a feed stream from a case file with the Python SDK
and runs it, speaking JSON lines on stdout and taking commands on stdin.
Port of clients/conformance/feed/runners/js.mjs.

    .venv/bin/python tests/feed_conformance/runner.py run  --url <url> --case <case.json> [--run '<json>']
    .venv/bin/python tests/feed_conformance/runner.py view --url <url> --query-id <id> --op get|list|status [--step s] [--key k] [--limit n] [--shards n] [--timeout-ms n]

Run from clients/client-py. Only JSON lines go to stdout; everything else
(the SDK's own prints and logs) goes to stderr.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any, Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# Every print of the SDK goes to stderr; the JSON lines go to the real stdout.
_OUT = sys.stdout
sys.stdout = sys.stderr

from queen import Queen, Stream  # noqa: E402
from queen.streams.util.js import UNDEFINED, js_json_dumps  # noqa: E402
from queen.streams.util.tasks import spawn  # noqa: E402

from expressions import evaluate, fn  # noqa: E402


def say(obj: Dict[str, Any]) -> None:
    _OUT.write(js_json_dumps(obj) + "\n")
    _OUT.flush()


def arg(name: str, fallback: Any = None) -> Any:
    argv = sys.argv
    flag = f"--{name}"
    if flag in argv:
        i = argv.index(flag)
        if i + 1 < len(argv):
            return argv[i + 1]
    return fallback


def with_fns(opts: Dict[str, Any], names: List[str]) -> Dict[str, Any]:
    out = dict(opts)
    for n in names:
        if n not in out:
            continue
        if n == "eventTime" and out[n] is False:
            continue
        out[n] = fn(out[n])
    return out


def parts(spec: Any) -> Any:
    """sides / steps: a queue name stays a name; anything else is a test."""
    if isinstance(spec, list):
        return spec
    return {k: (v if isinstance(v, str) else fn(v)) for k, v in spec.items()}


def build(queen: Any, c: Dict[str, Any]) -> Any:
    s = Stream.from_feed(queen, dict(c.get("source") or {}))
    for st in c["chain"]:
        op = st["op"]
        if op == "where":
            a = st["args"]
            s = s.where(a[0], a[1]) if len(a) == 2 else s.where(a[0], a[1], a[2])
        elif op == "filter":
            s = s.filter(fn(st["fn"]))
        elif op == "map":
            s = s.map(fn(st["fn"]))
        elif op == "flatMap":
            s = s.flat_map(fn(st["fn"]))
        elif op == "keyBy":
            s = s.key_by(fn(st["fn"]))
        elif op in ("windowTumbling", "windowSliding", "windowSession", "windowCron"):
            opts = with_fns(st["opts"], ["eventTime"])
            s = getattr(s, op)(**opts)
        elif op == "reduce":
            s = s.reduce(fn(st["fn"]), st["initial"]) if "initial" in st else s.reduce(fn(st["fn"]))
        elif op == "aggregate":
            s = s.aggregate({k: fn(e) for k, e in st["fields"].items()})
        elif op == "pattern":
            s = s.pattern(with_fns(st["opts"], ["start", "then", "key", "onMatch", "onTimeout", "keep", "eventTime"]))
        elif op == "join":
            if "sides" in st["opts"]:
                o = with_fns(st["opts"], ["key", "select", "onTimeout", "keep", "eventTime"])
                o["sides"] = parts(st["opts"]["sides"])
                s = s.join(o)
            else:
                s = s.join(with_fns(st["opts"], ["left", "right", "key", "select", "onTimeout", "keep", "eventTime"]))
        elif op == "gather":
            o = with_fns(st["opts"], ["count", "key", "onComplete", "onTimeout", "keep", "eventTime"])
            if "steps" in st["opts"]:
                o["steps"] = parts(st["opts"]["steps"])
            s = s.gather(o)
        elif op == "joinTable":
            s = s.join_table(with_fns(st["opts"], ["on", "key", "value", "select"]))
        elif op == "name":
            s = s.name(st["name"])
        elif op == "to":
            t = st["target"]
            if isinstance(t, str):
                target: Any = queen.queue(t)
            elif isinstance(t, list):
                target = [queen.queue(q) for q in t]
            else:
                target = fn(t["fn"])
            partition = None
            p = st.get("partition")
            if isinstance(p, str):
                partition = p
            elif isinstance(p, dict) and "fn" in p:
                partition = fn(p["fn"])
            s = s.to(target, partition=partition)
        elif op == "foreach":
            e = st.get("fn", UNDEFINED)

            def effect(value: Any, ctx: Any, e: Any = e) -> Any:
                say({"event": "foreach", "value": value, "ctx": ctx})
                return UNDEFINED if e is UNDEFINED else evaluate(e, [value, ctx])

            s = s.foreach(effect)
        else:
            raise ValueError(f"case: unknown chain op {op}")
    return s


def run_options(c: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    o = {**(c.get("run") or {}), **(overrides or {})}
    if "shardBy" in o and o["shardBy"] != "partition":
        o["shardBy"] = fn(o["shardBy"])

    def on_error(err: BaseException, ctx: Any) -> None:
        ctx = ctx if isinstance(ctx, dict) else {}
        say({
            "event": "error",
            "message": str(err),
            "phase": ctx.get("phase", UNDEFINED),
            "action": ctx.get("action", UNDEFINED),
            "code": getattr(err, "code", UNDEFINED),
        })

    o["onError"] = on_error
    return o


async def _stdin_lines(queue: "asyncio.Queue[Optional[str]]") -> None:
    loop = asyncio.get_running_loop()
    try:
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)
        while True:
            line = await reader.readline()
            if not line:
                break
            await queue.put(line.decode("utf-8", "replace"))
    except (ValueError, OSError):
        # Not a pipe (a file, a terminal): read it on a thread.
        while True:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            await queue.put(line)
    await queue.put(None)


def _exit(code: int) -> None:
    try:
        _OUT.flush()
        sys.stderr.flush()
    finally:
        os._exit(code)


async def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else None
    url = arg("url")
    if not url:
        raise ValueError("--url is required")
    queen = Queen(url)

    if mode == "view":
        shards = arg("shards")
        timeout = arg("timeout-ms")
        view = Stream.view(queen, arg("query-id"), shards=int(shards) if shards else None, timeout_millis=int(timeout) if timeout else 3000)
        op = arg("op")
        try:
            if op == "get":
                answer: Any = await view.get(arg("step"), arg("key"))
            elif op == "list":
                answer = await view.list(arg("step"), limit=int(arg("limit", "100")))
            else:
                answer = await view.status()
            say({"event": "view", "answer": answer})
        except Exception as err:  # noqa: BLE001
            say({"event": "failed", "message": str(err), "code": getattr(err, "code", UNDEFINED)})
        _exit(0)

    if mode != "run":
        raise ValueError("mode is run or view")
    with open(arg("case"), encoding="utf-8") as f:
        c = json.load(f)
    overrides = json.loads(arg("run")) if arg("run") else {}
    try:
        runner = await build(queen, c).run(**run_options(c, overrides))
    except Exception as err:  # noqa: BLE001
        say({"event": "failed", "message": str(err), "code": getattr(err, "code", UNDEFINED)})
        _exit(2)
        return
    m = runner.metrics()
    role = m.get("role")
    if role is None:
        role = "active" if m.get("running") else "standby"
    say({"event": "started", "role": role})

    ending = {"on": False}

    async def end(event: str) -> None:
        if ending["on"]:
            return
        ending["on"] = True
        if event == "stopped":
            await runner.stop()
        say({"event": event, "metrics": runner.metrics()})
        _exit(0)

    async def watch_done() -> None:
        try:
            await runner.done
        except BaseException:  # noqa: BLE001
            pass
        await end("done")

    spawn(watch_done())

    lines: "asyncio.Queue[Optional[str]]" = asyncio.Queue()
    spawn(_stdin_lines(lines))
    while True:
        line = await lines.get()
        if line is None:
            await end("stopped")
            return
        words = line.strip().split()
        if not words:
            continue
        cmd, rest = words[0], words[1:]
        if cmd == "stop":
            await end("stopped")
            return
        if cmd == "metrics":
            say({"event": "metrics", "metrics": runner.metrics()})
        elif cmd == "state":
            say({"event": "state", "answer": runner.state(rest[0] if rest else UNDEFINED).get(" ".join(rest[1:]))})
        elif cmd == "list":
            say({"event": "state", "answer": runner.state(rest[0] if rest else UNDEFINED).list(int(rest[1]) if len(rest) > 1 else 100)})


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except SystemExit:
        raise
    except BaseException as err:  # noqa: BLE001
        say({"event": "failed", "message": str(err) or type(err).__name__})
        _exit(2)
